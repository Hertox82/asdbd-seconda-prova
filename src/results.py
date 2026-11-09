"""Run isolati, misure raw e riepiloghi: nessuna sovrascrittura fra esperimenti."""

import csv
import hashlib
import io
import json
import os
import platform
import statistics
import sys
import tempfile
from datetime import datetime, timezone
from importlib.metadata import distributions, version
from pathlib import Path
from uuid import uuid4

import psutil


def utc_now() -> str:
    """Restituisce data/ora UTC esplicita per ogni osservazione."""
    return datetime.now(timezone.utc).isoformat()


def result_digest(value) -> str:
    """Calcola un digest del risultato completo; non sostituisce il confronto con tolleranza."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _atomic_text(path: Path, content: str) -> None:
    """Scrive un file temporaneo fratello e pubblica tramite rename sullo stesso filesystem."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", dir=path.parent,
                                         prefix=f".{path.name}.", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(content)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def summarize(records: list[dict]) -> list[dict]:
    """Aggrega solo misure ok non warm-up; deviazione campionaria, nessun outlier scartato."""
    groups = {}
    for record in records:
        if record["status"] != "ok" or record["warmup"] or record["duration_ms"] is None:
            continue
        key = (record["event"], record["query_id"], record["index_state"])
        groups.setdefault(key, []).append(record)
    result = []
    for (event, query_id, state), rows in groups.items():
        values = [row["duration_ms"] for row in rows]
        result.append({
            "event": event, "query_id": query_id, "index_state": state, "valid_repetitions": len(values),
            "mean_ms": statistics.mean(values), "median_ms": statistics.median(values),
            "sample_stddev_ms": statistics.stdev(values) if len(values) > 1 else None,
            "min_ms": min(values), "max_ms": max(values),
            "rows_per_second": rows[0].get("rows_per_second") if event == "ingestion" else None,
        })
    return result


def _resources() -> dict:
    """Raccoglie contesto del container e RSS corrente, senza accesso a Docker o segreti."""
    result = {"platform": platform.platform(), "machine": platform.machine(),
              "hostname": platform.node(), "visible_cpus": psutil.cpu_count(),
              "visible_memory_bytes": psutil.virtual_memory().total,
              "process_rss_bytes": psutil.Process().memory_info().rss}
    for key, filename in (("cgroup_memory_limit", "memory.max"), ("cgroup_cpu_limit", "cpu.max")):
        path = Path("/sys/fs/cgroup") / filename
        result[key] = path.read_text().strip() if path.exists() else None
    return result


def environment_context(database_version: str) -> dict:
    """Fotografa l'ambiente osservabile, separato dal protocollo obbligatorio.

    Registra anche dipendenze transitive effettivamente installate. I file
    Docker sono sorgenti dichiarativi, non la configurazione risolta del daemon:
    non leggiamo .env, credenziali o Docker socket. Questa copertura parziale
    viene dichiarata esplicitamente anche quando tutti i file sono disponibili.
    """
    root = Path(__file__).resolve().parent.parent
    mounted = Path("/opt/lab/environment")
    sources = {"docker-compose.yml": (root / "docker-compose.yml", mounted / "docker-compose.yml"),
               "Dockerfile": (root / "docker/jupyter/Dockerfile", mounted / "jupyter/Dockerfile"),
               "requirements.txt": (root / "requirements.txt", mounted / "requirements.txt")}
    hashes = {}
    for name, candidates in sources.items():
        path = next((p for p in candidates if p.is_file()), None)
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest() if path else None
    resources = _resources()
    return {"python_version": sys.version.split()[0], "database_version": database_version,
            "packages": {d.metadata["Name"].lower().replace("_", "-"): d.version
                         for d in distributions() if d.metadata["Name"]},
            "docker_sources": hashes,
            "runtime": {key: value for key, value in resources.items()
                        if key not in ("hostname", "process_rss_bytes")},
            "coverage_warnings": [
                "Docker: osservati sorgenti e risorse visibili a Jupyter; configurazione effettiva del DB, "
                "override Compose/.env e digest immagini non verificabili senza inventario host.",
                *(["Sorgenti Docker/requirements non tutti disponibili al processo."] if None in hashes.values() else [])]}


class ResultRun:
    """Context manager di una sola fase manuale da notebook, non runner multi-DB.

    Al fallimento conserva raw e diagnostica ma non pubblica un summary valido.
    Una directory unica per run separa ingestione e sessione query, collegate dal
    checksum del dataset. I tempi validi non sono mai ricavati da run falliti.
    """

    def __init__(self, directory: Path, configuration: dict, reference: dict, *, stage: str,
                 durable: bool = False) -> None:
        self.durable = durable
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex
        self.directory = Path(directory) / self.run_id
        self.directory.mkdir(parents=True, exist_ok=False)
        self.records: list[dict] = []
        self.common = {
            "run_id": self.run_id, "mode": configuration["mode"], "database": configuration["database"],
            "database_version": "not_connected", "dataset_sha256": reference["dataset_sha256"],
            "dataset_size": reference["dataset_size"],
            # In una sessione query il batch di caricamento non è una misura pertinente.
            "batch_size": configuration["batch_size"] if stage == "ingestion" else None,
            "ingestion_method": configuration.get("ingestion_method", "psycopg.executemany") if stage == "ingestion" else None,
            "commit_policy": configuration.get("commit_policy", "one_transaction_per_batch") if stage == "ingestion" else None,
        }
        self.manifest = {
            "run_id": self.run_id, "stage": stage, "status": "running", "started_at_utc": utc_now(),
            "configuration": configuration, "dataset_sha256": reference["dataset_sha256"],
            "q1_ids": reference["q1_ids"], "python_version": sys.version.split()[0],
            "driver_versions": {package: version(package) for package in
                                (("pymongo",) if configuration["database"] == "mongo" else ("psycopg",))},
            "resources_start": _resources(),
            "method_notes": [
                "Tempi query end-to-end client/driver/DB; connessione esclusa; risultati completi.",
                "Ingestione: inizio prima scrittura fino a ultimo commit/ack; lettura dei batch successivi inclusa.",
                "Driver write separato; download, preprocessing, checksum e indici esclusi dal tempo ingestione.",
                "Piani EXPLAIN ANALYZE / executionStats separati dalle query ordinarie; planner non forzato.",
                "Warm-up esclusi dalle statistiche; deviazione standard campionaria; nessun outlier scartato.",
                "Esperimento sequenziale PostgreSQL/MongoDB/CockroachDB; dimensione esplicita nel manifest; nessun benchmark distribuito.",
                "Cache OS/WSL2 non svuotate; letture e verifiche precedenti contribuiscono alle cache.",
            ],
        }
        if configuration["database"] == "mongo":
            self.manifest["method_notes"].extend([
                "Mongo standalone: insert_many ordered, w=1, j=true, retryWrites=false; nessuna transazione multi-documento.",
                "driver_write_ms include conversione/costruzione documenti nell'adapter, driver e ack journaled.",
                "rows_committed è il nome comune del contatore; per Mongo indica documenti confermati, non commit di transazioni batch.",
                "Un batch Mongo fallito può essere parziale; un errore di rete/write concern può lasciare l'esito incerto.",
            ])
        if configuration["database"] == "cockroach":
            self.manifest["method_notes"].extend([
                "CockroachDB single-node insecure solo locale: nessuna misura di disponibilità o scalabilità distribuita.",
                "Transazione SERIALIZABLE per batch; retry solo SQLSTATE 40001, mai errori di rete/commit ambiguo.",
                "Retry e backoff inclusi nei tempi client/adapter; contatori nei risultati di ingestione.",
                "INT4 esplicito per gli attributi numerici; INT8 PK senza ID generati; STRING per testo/HHMM.",
                "EXPLAIN ANALYZE nativo testuale; creazione indici include attesa di pubblicazione nel catalogo.",
                "Statement timeout 120s e lock timeout 10s per prove 100k; audit FULL non validati e limiti non aumentati.",
            ])
        if durable:
            self.manifest["method_notes"].append(
                "Runner: ogni osservazione raw è pubblicata fuori dal timer; fasi incomplete non sono aggregate fra tentativi.")
        self.save_json("reference.json", reference)
        self.flush()

    def __enter__(self):
        return self

    def __exit__(self, error_type, error, traceback) -> bool:
        """Finalizza anche su errore; non nasconde l'eccezione al notebook."""
        self.manifest.update(status="failed" if error is not None else "completed",
                             finished_at_utc=utc_now(), resources_end=_resources())
        if error is not None:
            self.manifest["error"] = str(error)
            self.record(event="failure", status="error", error=str(error))
        self.flush()
        return False

    def set_database_version(self, value: str) -> None:
        """Registra la versione reale una sola volta, fuori dai timer."""
        self.common["database_version"] = value
        self.manifest["database_version"] = value

    def record(self, **values) -> None:
        """Accoda una singola osservazione raw in memoria, compresi warm-up/errori."""
        row = {**self.common, "timestamp_utc": utc_now(), "event": "query", "query_id": "",
               "index_state": "", "iteration": 0, "warmup": False, "duration_ms": None,
               "result_digest": None, "status": "ok", "error": None, **values}
        self.records.append(row)
        # Nel runner ogni osservazione sopravvive alla perdita del kernel.
        # record() viene chiamato dopo il timer: nessuna I/O nel tempo query.
        if self.durable:
            self.flush()

    def save_json(self, name: str, value) -> None:
        """Salva un artefatto JSON del run in modo atomico e con nome locale."""
        if Path(name).name != name or not name.endswith(".json"):
            raise ValueError("Il nome dell'artefatto deve essere un file JSON nella directory del run.")
        _atomic_text(self.directory / name, json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")

    def flush(self) -> None:
        """Pubblica raw/manifest; summary solo quando l'intera fase è completata."""
        _atomic_text(self.directory / "raw.jsonl", "".join(
            json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n" for row in self.records))
        if self.manifest["status"] == "completed":
            rows = summarize(self.records)
            if rows:
                output = io.StringIO(newline="")
                writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
                _atomic_text(self.directory / "summary.csv", output.getvalue())
        # Il manifest completato è l'ultimo file pubblicato: funge da marcatore finale.
        self.save_json("manifest.json", self.manifest)
