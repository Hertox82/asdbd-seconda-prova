"""Acquisizione e preparazione riproducibile, senza DB e senza DataFrame completi.

Le scritture avvengono in una directory temporanea. Solo dopo la validazione
pubblichiamo l'intero bundle con un rename nella stessa directory del filesystem.
"""

import csv
import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from typing import Iterator

import psutil

from src.schema import (
    DATASET_HANDLE, FULL_ROWS, PIPELINE_VERSION, PREPARED_COLUMNS, SCHEMA_VERSION,
    parse_original_row, schema_manifest,
)
from src.validation import (
    OFFICIAL_CONTRACT, SourceContract, Statistics, check_source_summary,
    original_chunks, sha256_file, validate_bundle, validate_prepared,
)

LOGGER = logging.getLogger(__name__)


def _utc_now() -> str:
    """Restituisce una data UTC esplicita per provenienza e manifest."""
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, content: dict | list) -> None:
    """Salva JSON UTF-8 leggibile; è usato solo su file nuovi o in staging."""
    path.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@contextmanager
def _directory_lock(path: Path) -> Iterator[None]:
    """Esclude preparazioni concorrenti mediante mkdir atomico multipiattaforma.

    Il lock è una directory vuota locale, rimossa anche in caso di eccezione.
    Dopo un arresto forzato può restare: non rimuoverlo se un processo è attivo.
    """
    try:
        path.mkdir()
    except FileExistsError as error:
        raise RuntimeError(f"Operazione già attiva o lock residuo: {path}.") from error
    try:
        yield
    finally:
        path.rmdir()


class PreparationMonitor:
    """Campiona l'RSS dell'intero processo ogni 100 ms e misura il tempo trascorso.

    Non misura tutta la memoria Docker né la sola memoria degli oggetti Python.
    Anche il kernel Jupyter, se usato, contribuisce al valore osservato.
    """

    def __init__(self) -> None:
        self.process = psutil.Process()
        self.initial_rss = self.process.memory_info().rss
        self.peak_rss = self.initial_rss
        self.started = time.perf_counter()
        self.measurement: dict | None = None
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()

    def _sample(self) -> None:
        """Aggiorna il massimo campionato finché l'operazione non termina."""
        while not self.stop_event.wait(0.1):
            self.peak_rss = max(self.peak_rss, self.process.memory_info().rss)

    def finish(self) -> dict:
        """Ferma il thread e restituisce misure reali in secondi e byte."""
        if self.measurement is not None:
            return self.measurement
        self.stop_event.set()
        self.thread.join()
        self.peak_rss = max(self.peak_rss, self.process.memory_info().rss)
        self.measurement = {
            "elapsed_seconds": time.perf_counter() - self.started,
            "baseline_rss_bytes": self.initial_rss,
            "sampled_peak_rss_bytes": self.peak_rss,
            "rss_growth_bytes": self.peak_rss - self.initial_rss,
            "sampling_interval_seconds": 0.1,
            "memory_method": "psutil RSS del processo, massimo campionato; non memoria totale Docker",
        }
        return self.measurement


def acquire_source(
    raw_path: Path,
    *,
    offline: bool = True,
    local_source: Path | None = None,
    contract: SourceContract = OFFICIAL_CONTRACT,
) -> dict:
    """Riusa il CSV locale oppure ne copia uno senza sovrascrivere l'originale.

    raw_path è la destinazione persistente. local_source consente una copia
    iniziale da un percorso visibile al processo. Se offline=False e il CSV manca,
    scarica soltanto flights.csv con KaggleHub dalla versione 1 fissata.
    Restituisce provenienza e checksum; non importa gli script research.
    """
    if type(offline) is not bool:
        raise ValueError("offline deve essere True o False, non una stringa.")
    target = Path(raw_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with _directory_lock(target.parent / f".{target.name}.acquire-lock"):
        method = "existing_local_csv"
        if not target.exists():
            if local_source is not None:
                source = Path(local_source).resolve(strict=True)
                method = "copied_local_csv"
            elif offline:
                raise FileNotFoundError(f"Modalità offline: manca {target}. Copiare flights.csv o autorizzare il download.")
            else:
                if contract != OFFICIAL_CONTRACT:
                    raise ValueError("Il download è previsto solo per il contratto Kaggle ufficiale.")
                import kagglehub  # Nessun accesso alla rete nella modalità offline.

                source = Path(kagglehub.dataset_download(DATASET_HANDLE, path="flights.csv"))
                method = "kagglehub_versioned_download"
            before = sha256_file(source)
            if contract.expected_sha256 and before != contract.expected_sha256:
                raise ValueError("Checksum del CSV da copiare diverso dalla versione attesa.")
            # Staging nella stessa cartella: niente CSV parziali accettati come originali.
            with tempfile.TemporaryDirectory(prefix=".source-copy-", dir=target.parent) as temp:
                staged = Path(temp) / "flights.csv"
                shutil.copyfile(source, staged)
                if sha256_file(staged) != before or sha256_file(source) != before:
                    raise ValueError("La sorgente è cambiata o la copia non è integra.")
                os.replace(staged, target)
        fingerprint = sha256_file(target)
        if contract.expected_sha256 and fingerprint != contract.expected_sha256:
            raise ValueError("Il CSV locale non corrisponde al checksum della versione attesa.")
        if local_source is not None and sha256_file(Path(local_source)) != fingerprint:
            raise ValueError("Esiste già un CSV locale diverso: non verrà sovrascritto.")
        metadata_path = target.with_suffix(".source.json")
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata["sha256"] != fingerprint or metadata["contract"] != asdict(contract):
                raise ValueError("Provenienza locale non coerente con il CSV/contratto.")
            return metadata
        metadata = {
            "dataset": DATASET_HANDLE if contract == OFFICIAL_CONTRACT else contract.label,
            "url": "https://www.kaggle.com/datasets/usdot/flight-delays" if contract == OFFICIAL_CONTRACT else None,
            "file": target.name, "bytes": target.stat().st_size, "sha256": fingerprint,
            "acquisition_method": method, "recorded_at_utc": _utc_now(),
            "contract": asdict(contract),
        }
        with tempfile.TemporaryDirectory(prefix=".source-meta-", dir=target.parent) as temp:
            staged = Path(temp) / "source.json"
            _write_json(staged, metadata)
            os.replace(staged, metadata_path)
        return metadata


def _write_datasets(source: Path, staging: Path, sizes: tuple[int, ...], chunk_size: int) -> dict:
    """Assegna gli ID una volta e scrive i prefissi richiesti in un'unica scansione."""
    stats = Statistics()
    with ExitStack() as stack:
        writers = {}
        for size in sizes:
            stream = stack.enter_context((staging / f"flights_{size}.csv").open(
                "w", encoding="utf-8", newline=""))
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(PREPARED_COLUMNS)
            writers[size] = writer
        # Il contatore è esterno ai chunk: cambiare chunk_size non cambia la PK.
        for chunk in original_chunks(source, chunk_size):
            for row in chunk:
                flight_id = stats.rows + 1
                stats.add(parse_original_row(row, flight_id))
                canonical_row = [str(flight_id), *row]
                for size, writer in writers.items():
                    if flight_id <= size:
                        writer.writerow(canonical_row)
            if stats.rows % 100_000 < len(chunk):
                LOGGER.info("Preprocessing: %s record sorgente letti", f"{stats.rows:,}")
    return stats.summary()


def prepare_dataset(
    source_path: Path,
    output_dir: Path,
    *,
    sizes: tuple[int, ...] = (100_000, 1_000_000, FULL_ROWS),
    chunk_size: int = 2000,
    force: bool = False,
    contract: SourceContract = OFFICIAL_CONTRACT,
) -> dict:
    """Prepara e valida CSV annidati, pubblicando solo un bundle completo.

    La sorgente viene aperta solo in lettura e sempre scansionata interamente,
    anche se sizes contiene solo 100k. force=False riusa un bundle verificato;
    force=True rigenera e conserva la vecchia directory in un backup separato.
    Restituisce skipped, manifest e misure della chiamata (non tempi di query).
    """
    source = Path(source_path).resolve(strict=True)
    destination = Path(output_dir).resolve()
    if type(force) is not bool:
        raise ValueError("force deve essere True o False, non una stringa.")
    if source.is_relative_to(destination):
        raise ValueError("La directory preparata non può contenere la sorgente originale.")
    if type(chunk_size) is not int or chunk_size <= 0:
        raise ValueError("chunk_size deve essere un intero positivo.")
    if (not sizes or any(type(size) is not int or not 0 < size <= contract.expected_rows for size in sizes)
            or len(set(sizes)) != len(sizes)):
        raise ValueError("sizes deve contenere dimensioni positive, distinte e non superiori alla sorgente.")
    sizes = tuple(sorted(sizes))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with _directory_lock(destination.parent / f".{destination.name}.prepare-lock"):
        monitor = PreparationMonitor()
        try:
            if destination.exists() and not (destination / "manifest.json").is_file():
                raise ValueError("Destinazione esistente senza manifest: scegliere una nuova directory.")
            if destination.exists() and not force:
                LOGGER.info("Verifica checksum del checkpoint esistente, senza rigenerazione.")
                manifest = validate_bundle(destination, source, contract, deep=False, expected_sizes=sizes)
                return {"skipped": True, "manifest": manifest, "invocation_metrics": monitor.finish()}
            phase_start = time.perf_counter()
            source_sha = sha256_file(source)
            if contract.expected_sha256 and source_sha != contract.expected_sha256:
                raise ValueError("Checksum sorgente non conforme alla versione attesa.")
            phase_seconds = {"source_checksum": time.perf_counter() - phase_start}
            # Stima conservativa; include ID e serializzazione e non conta eventuali backup.
            required = int(source.stat().st_size * sum(sizes) / contract.expected_rows * 1.5) + 10_000_000
            if shutil.disk_usage(destination.parent).free < required:
                raise OSError(f"Spazio insufficiente per lo staging: stimati almeno {required} byte liberi.")
            with tempfile.TemporaryDirectory(prefix=f".{destination.name}.building-", dir=destination.parent) as temp:
                staging = Path(temp) / "bundle"
                staging.mkdir()
                phase_start = time.perf_counter()
                source_stats = _write_datasets(source, staging, sizes, chunk_size)
                check_source_summary(source_stats, contract)
                phase_seconds["streaming_write"] = time.perf_counter() - phase_start
                artifacts = []
                phase_start = time.perf_counter()
                for size in sizes:
                    LOGGER.info("Validazione integrale del CSV da %s record", f"{size:,}")
                    artifacts.append(validate_prepared(staging / f"flights_{size}.csv", size, source))
                phase_seconds["output_validation"] = time.perf_counter() - phase_start
                if sha256_file(source) != source_sha:
                    raise ValueError("Il CSV originale è cambiato durante la preparazione.")
                metrics = monitor.finish()
                metrics["phase_seconds"] = phase_seconds
                metrics["scope"] = "checksum, scrittura e validazione; esclusi acquisizione e pubblicazione"
                manifest = {
                    "status": "validated", "created_at_utc": _utc_now(),
                    "pipeline_version": PIPELINE_VERSION, "schema_version": SCHEMA_VERSION,
                    "source_contract": asdict(contract), "chunk_size": chunk_size,
                    "python_version": sys.version.split()[0], "schema": schema_manifest(),
                    "csv": {"encoding": "utf-8", "delimiter": ",", "line_terminator": "LF",
                            "null": "cella vuota -> None; mai zero", "columns": list(PREPARED_COLUMNS)},
                    "source": {"file": source.name, "bytes": source.stat().st_size,
                               "sha256": source_sha, "statistics": source_stats,
                               "dataset": DATASET_HANDLE if contract == OFFICIAL_CONTRACT else contract.label},
                    "artifacts": artifacts, "metrics": metrics,
                }
                _write_json(staging / "schema.json", schema_manifest())
                _write_json(staging / "manifest.json", manifest)
                backup = None
                if destination.exists():
                    backup = destination.with_name(f".{destination.name}.backup-{uuid4().hex}")
                    os.replace(destination, backup)
                try:
                    os.replace(staging, destination)
                except BaseException:
                    if backup is not None:
                        os.replace(backup, destination)
                    raise
                LOGGER.info("Bundle validato pubblicato in %s", destination)
                return {"skipped": False, "manifest": manifest,
                        "invocation_metrics": metrics, "previous_bundle_backup": str(backup) if backup else None}
        finally:
            monitor.finish()
