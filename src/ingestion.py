"""Ingestione del CSV canonico a batch, senza preprocessing o indici secondari."""

import logging
import time
from itertools import chain
from types import MappingProxyType

from src.databases.base import BatchWriteError, DatabaseAdapter
from src.validation import iter_prepared_chunks, sha256_file
from src.progress import progress_sink


LOGGER = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    """Errore con contatori dei batch già confermati; non equivale a un run valido."""

    def __init__(self, message: str, progress: dict) -> None:
        super().__init__(message)
        self.progress = progress


def ingest_csv(adapter: DatabaseAdapter, path, expected_rows: int, expected_sha256: str, *, batch_size: int = 1000, progress=None) -> dict:
    """Inserisce in dataset vuoto e misura fino all'ultimo batch confermato.

    Prima del timer: checksum, precondizioni DB e lettura del primo batch.
    duration_ms include lettura/decodifica dei batch successivi fra le scritture;
    driver_write_ms somma le chiamate all'adapter, inclusi driver e commit/ack.
    Per Mongo include anche costruzione dei documenti e conversioni BSON.
    Nessun download, assegnazione ID, DDL o creazione indici è cronometrato.
    """
    if type(expected_rows) is not int or expected_rows <= 0:
        raise ValueError("expected_rows deve essere un intero positivo.")
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size deve essere un intero positivo.")
    if sha256_file(path) != expected_sha256:
        raise ValueError("Checksum CSV non valido: ingestione non avviata.")
    adapter.validate_schema()
    adapter.assert_index_state("primary_only")
    if adapter.count_rows() != 0:
        raise ValueError("Tabella/collezione già popolata: nessun append/reset implicito. Confermare un reset separato.")
    batches = iter_prepared_chunks(path, batch_size)
    first = next(batches, None)
    if first is None:
        raise ValueError("CSV privo di record: ingestione non avviata.")
    notify = progress_sink(progress)
    progress = {"rows_committed": 0, "batches_committed": 0, "batch_size": batch_size,
                "ingestion_method": adapter.ingestion_method, "commit_policy": adapter.commit_policy}
    # Una sola notifica PRIMA del timer. Il thread UI legge i contatori già
    # aggiornati dal ciclo originale: nessuna callback/display fra i batch.
    notify(kind="ingestion", status="running", counters=MappingProxyType(progress), total=expected_rows)
    started = last_commit = time.perf_counter_ns()
    driver_ns = 0
    try:
        for rows in chain((first,), batches):
            if rows[-1][0] > expected_rows:
                raise ValueError("Il CSV contiene più ID del dataset richiesto.")
            batch_start = time.perf_counter_ns()
            adapter.ingest_batch(rows)
            last_commit = time.perf_counter_ns()
            driver_ns += last_commit - batch_start
            progress["rows_committed"] += len(rows)
            progress["batches_committed"] += 1
            if progress["batches_committed"] % 10 == 0:
                LOGGER.info("Ingestione: %s record confermati in %s batch", progress["rows_committed"], progress["batches_committed"])
        if progress["rows_committed"] != expected_rows or sha256_file(path) != expected_sha256:
            raise ValueError("Numero record ingestiti errato o CSV cambiato durante l'ingestione.")
    except Exception as error:
        sqlstate = getattr(error, "sqlstate", None) or getattr(error.__cause__, "sqlstate", None)
        if sqlstate is not None:
            progress["sqlstate"] = sqlstate
        # Mongo può aver confermato solo parte del batch, senza rollback globale.
        if isinstance(error, BatchWriteError):
            progress["current_batch_acknowledged_rows"] = error.acknowledged_rows
            progress["write_outcome_uncertain"] = error.acknowledged_rows is None
            if error.acknowledged_rows is not None:
                progress["rows_committed"] += error.acknowledged_rows
        if hasattr(adapter, "ingestion_diagnostics"):
            progress.update(adapter.ingestion_diagnostics())
        progress["duration_ms"] = (time.perf_counter_ns() - started) / 1e6
        notify(kind="ingestion", status="failed", message=str(error))
        raise IngestionError(f"Ingestione interrotta: {error}", progress) from error
    finally:
        batches.close()
    duration_ms = (last_commit - started) / 1e6
    if hasattr(adapter, "ingestion_diagnostics"):
        progress.update(adapter.ingestion_diagnostics())
    result = {**progress, "duration_ms": duration_ms, "driver_write_ms": driver_ns / 1e6,
              "rows_per_second": expected_rows / (duration_ms / 1000), "status": "ok"}
    notify(kind="ingestion", status="completed", result=MappingProxyType(result))
    return result
