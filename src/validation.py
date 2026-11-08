"""Controlli streaming di schema, contenuti, ID e checksum dei CSV.

Le statistiche descrivono i dati, non sono tempi di query o risultati DB.
"""

import csv
import hashlib
import json
import logging
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from itertools import islice
from pathlib import Path
from typing import Iterator

from src.schema import (
    FULL_ROWS, PIPELINE_VERSION, PREPARED_COLUMNS, RAW_COLUMNS, SCHEMA_VERSION,
    SOURCE_SHA256, Cell, parse_prepared_row, schema_manifest,
)


@dataclass(frozen=True)
class SourceContract:
    """Valori attesi della sorgente; un contratto sintetico va dichiarato nei test."""

    expected_rows: int = FULL_ROWS
    expected_sha256: str | None = SOURCE_SHA256
    label: str = "kaggle-usdot-flight-delays-v1"


OFFICIAL_CONTRACT = SourceContract()
LOGGER = logging.getLogger(__name__)


def sha256_file(path: Path) -> str:
    """Calcola SHA-256 del file a blocchi di 1 MiB, senza caricarlo interamente."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def csv_records(path: Path, expected_header: tuple[str, ...]) -> Iterator[Iterator[list[str]]]:
    """Apre il CSV UTF-8 e verifica l'intestazione esatta, restituendo un iteratore.

    newline='' delega a csv il trattamento corretto di CRLF e celle multilinea.
    Non ignoriamo righe vuote, colonne extra o errori di quoting.
    """
    with Path(path).open("r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, None)
        if header != list(expected_header):
            raise ValueError(f"Schema CSV non valido in {path}: {header!r}.")
        yield reader


def original_chunks(path: Path, chunk_size: int) -> Iterator[list[list[str]]]:
    """Restituisce al massimo chunk_size record originali per volta, in ordine."""
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_size deve essere un intero positivo.")
    with csv_records(path, RAW_COLUMNS) as reader:
        while chunk := list(islice(reader, chunk_size)):
            yield chunk


class Statistics:
    """Accumulatore limitato ai conteggi e ai piccoli insiemi di categorie.

    Riceve le 31 celle originali tipizzate e conta i record, senza conservarli.
    """

    def __init__(self) -> None:
        self.rows = 0
        self.nulls = [0] * len(RAW_COLUMNS)
        self.airlines: set[str] = set()
        self.origins: set[str] = set()
        self.cancelled = 0
        self.origin_jfk = 0
        self.distance_1500_2000 = 0
        self.delay_sum = 0
        self.delay_count = 0

    def add(self, values: tuple[Cell, ...]) -> None:
        """Aggiorna le statistiche con le 31 celle già validate, senza imputazioni."""
        self.rows += 1
        for index, value in enumerate(values):
            if value is None:
                self.nulls[index] += 1
        self.airlines.add(values[4])
        self.origins.add(values[7])
        self.cancelled += values[24]
        self.origin_jfk += values[7] == "JFK"
        self.distance_1500_2000 += 1500 <= values[17] <= 2000
        if values[11] is not None:
            self.delay_sum += values[11]
            self.delay_count += 1

    def summary(self) -> dict:
        """Restituisce conteggi serializzabili e la media dei soli ritardi non nulli."""
        return {
            "rows": self.rows,
            "null_counts": dict(zip(RAW_COLUMNS, self.nulls)),
            "airlines_distinct": len(self.airlines),
            "origin_airports_distinct": len(self.origins),
            "cancelled": self.cancelled,
            "origin_jfk": self.origin_jfk,
            "distance_1500_2000": self.distance_1500_2000,
            "departure_delay_sum": self.delay_sum,
            "departure_delay_count": self.delay_count,
            "departure_delay_mean": self.delay_sum / self.delay_count if self.delay_count else None,
        }


def check_source_summary(summary: dict, contract: SourceContract) -> None:
    """Fa fallire la preparazione se righe o conteggi ufficiali non coincidono."""
    if summary["rows"] != contract.expected_rows:
        raise ValueError(f"Righe sorgente: {summary['rows']}; attese {contract.expected_rows}.")
    if contract == OFFICIAL_CONTRACT:
        expected = {
            "airlines_distinct": 14, "origin_airports_distinct": 628,
            "cancelled": 89_884, "origin_jfk": 93_811, "distance_1500_2000": 413_921,
        }
        for name, value in expected.items():
            if summary[name] != value:
                raise ValueError(f"Conteggio sorgente {name}: {summary[name]}; atteso {value}.")
        for name, value in {"ORIGIN_AIRPORT": 0, "DISTANCE": 0, "AIRLINE": 0,
                            "DEPARTURE_DELAY": 86_153}.items():
            if summary["null_counts"][name] != value:
                raise ValueError(f"Conteggio NULL sorgente non valido: {name}.")


def validate_prepared(path: Path, expected_rows: int, source_path: Path) -> dict:
    """Valida un CSV e confronta ogni record con il prefisso della sorgente.

    Parametri: CSV preparato, righe attese e CSV originale in sola lettura.
    Risultato: conteggi, dimensioni, ID estremi e SHA-256. Fallisce per perdite,
    ID alterati, valori/tipi non validi o cambiamenti alle 31 celle originali.
    """
    if type(expected_rows) is not int or expected_rows <= 0:
        raise ValueError("expected_rows deve essere un intero positivo.")
    stats = Statistics()
    with csv_records(path, PREPARED_COLUMNS) as prepared, csv_records(source_path, RAW_COLUMNS) as raw:
        for flight_id, row in enumerate(prepared, start=1):
            values = parse_prepared_row(row, flight_id)
            if next(raw, None) != row[1:]:
                raise ValueError(f"Record {flight_id}: celle diverse dalla sorgente in {path}.")
            stats.add(values[1:])
            if flight_id % 1_000_000 == 0:
                LOGGER.info("Validazione %s: %s record confrontati", Path(path).name, f"{flight_id:,}")
    if stats.rows != expected_rows:
        raise ValueError(f"Righe preparate in {path}: {stats.rows}; attese {expected_rows}.")
    return {
        "file": Path(path).name, "rows": stats.rows,
        "flight_id_min": 1 if stats.rows else None,
        "flight_id_max": stats.rows if stats.rows else None,
        "bytes": Path(path).stat().st_size, "sha256": sha256_file(path),
        "statistics": stats.summary(),
    }


def iter_prepared_chunks(path: Path, chunk_size: int = 2000) -> Iterator[list[tuple[Cell, ...]]]:
    """Legge il canonico in blocchi tipizzati, pronti per gli adapter futuri.

    Verifica schema, tipi e sequenza ID durante la lettura. I null restano None;
    non assegna nuove chiavi e non implementa alcuna ingestione.
    """
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_size deve essere un intero positivo.")
    with csv_records(path, PREPARED_COLUMNS) as reader:
        next_id = 1
        while rows := list(islice(reader, chunk_size)):
            yield [parse_prepared_row(row, next_id + offset) for offset, row in enumerate(rows)]
            next_id += len(rows)


def validate_bundle(
    output_dir: Path,
    source_path: Path,
    contract: SourceContract = OFFICIAL_CONTRACT,
    *,
    deep: bool = True,
    expected_sizes: tuple[int, ...] | None = None,
) -> dict:
    """Verifica manifest e checksum; opzionalmente ricontrolla tutte le celle.

    deep=True esegue anche schema/tipi/ID/confronto con la sorgente per ogni CSV.
    deep=False serve allo skip di un bundle già validato: rilegge comunque tutti
    i byte dei file, senza riscriverli. Restituisce il manifest, mai un falso OK.
    """
    directory, source = Path(output_dir), Path(source_path)
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if (manifest["status"] != "validated" or manifest["pipeline_version"] != PIPELINE_VERSION
                or manifest["schema_version"] != SCHEMA_VERSION
                or manifest["schema"] != schema_manifest()
                or manifest["source_contract"] != asdict(contract)):
            raise ValueError("Manifest incompatibile con versione, schema o contratto richiesto.")
        if json.loads((directory / "schema.json").read_text(encoding="utf-8")) != manifest["schema"]:
            raise ValueError("schema.json non corrisponde al contratto dichiarato.")
        source_info = manifest["source"]
        source_sha = sha256_file(source)
        if (source_sha != source_info["sha256"]
                or (contract.expected_sha256 and source_sha != contract.expected_sha256)
                or source.stat().st_size != source_info["bytes"]):
            raise ValueError("Checksum/dimensione della sorgente non corrispondenti al manifest.")
        check_source_summary(source_info["statistics"], contract)
        sizes = tuple(item["rows"] for item in manifest["artifacts"])
        if (not sizes or tuple(sorted(set(sizes))) != sizes
                or any(type(size) is not int or not 0 < size <= contract.expected_rows for size in sizes)
                or (expected_sizes is not None and sizes != expected_sizes)):
            raise ValueError("Dimensioni subset nel manifest non coerenti con la richiesta.")
        for artifact in manifest["artifacts"]:
            if artifact["file"] != f"flights_{artifact['rows']}.csv":
                raise ValueError("Nome artefatto non valido nel manifest.")
            if (artifact["flight_id_min"] != 1 or artifact["flight_id_max"] != artifact["rows"]
                    or artifact["statistics"]["rows"] != artifact["rows"]):
                raise ValueError("Intervallo ID o conteggi del manifest non coerenti.")
            if (artifact["rows"] == contract.expected_rows
                    and artifact["statistics"] != source_info["statistics"]):
                raise ValueError("Statistiche del CSV completo diverse dalla sorgente.")
            path = directory / artifact["file"]
            if path.is_symlink():
                raise ValueError("Non sono ammessi artefatti che siano link simbolici.")
            if deep:
                if validate_prepared(path, artifact["rows"], source) != artifact:
                    raise ValueError(f"Validazione dati non corrispondente: {path.name}.")
            elif path.stat().st_size != artifact["bytes"] or sha256_file(path) != artifact["sha256"]:
                raise ValueError(f"Checksum/dimensione artefatto non validi: {path.name}.")
        if sha256_file(source) != source_sha:
            raise ValueError("La sorgente è cambiata durante la validazione.")
        return manifest
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("Manifest incompleto o non valido: rigenerare consapevolmente con force=True.") from error
