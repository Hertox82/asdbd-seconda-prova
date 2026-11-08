"""Misure delle quattro query e costruzione indici, separate dall'ingestione."""

import time

from src.databases.base import DatabaseAdapter
from src.query_validation import assert_equivalent, expected_result, verify_database
from src.results import ResultRun, result_digest
from src.progress import progress_sink


def benchmark_phase(
    adapter: DatabaseAdapter, reference: dict, run: ResultRun, *, index_state: str,
    query_ids: tuple[str, ...], warmup: int = 3, repetitions: int = 20,
    progress=None,
) -> None:
    """Verifica la correttezza e misura solo invio/acquisizione completa del risultato.

    Confronto, digest, salvataggio e visualizzazione avvengono dopo il timer.
    Un risultato scorretto interrompe subito la fase, conserva l'errore e non
    viene incluso nelle statistiche valide. La connessione è già aperta.
    """
    if type(warmup) is not int or warmup < 0 or type(repetitions) is not int or repetitions < 1:
        raise ValueError("Warm-up/ripetizioni non validi.")
    if not query_ids or any(q not in ("Q1", "Q2", "Q3", "Q4") for q in query_ids):
        raise ValueError("Query non prevista dal protocollo.")
    try:
        correctness = verify_database(adapter, reference, index_state)
    except Exception as error:
        run.save_json(f"correctness_{index_state}.json", {"status": "failed", "error": str(error)})
        raise
    run.save_json(f"correctness_{index_state}.json", correctness)
    measure_queries(adapter, reference, run, index_state=index_state, query_ids=query_ids,
                    warmup=warmup, repetitions=repetitions, progress=progress)
    # EXPLAIN esegue di nuovo le query, ma non contamina i tempi ordinari salvati.
    plans = {}
    progress_sink(progress)(kind="detail", message="Raccolta piani EXPLAIN e verifiche finali, fuori dai timer ordinari.")
    for query_id in query_ids:
        parameters = {**reference["parameters"], "flight_id": reference["q1_ids"][0]}
        plans[query_id] = adapter.explain(query_id, parameters)
    run.save_json(f"plans_{index_state}.json", plans)
    run.flush()


def measure_queries(adapter: DatabaseAdapter, reference: dict, run: ResultRun, *, index_state: str,
                    query_ids: tuple[str, ...], warmup: int, repetitions: int,
                    capture_results: bool = False, progress=None) -> dict:
    """Unico ciclo temporizzato FULL/DEMO, con gli stessi confini approvati.

    Il chiamante sceglie il controllo preliminare: audit integrale FULL oppure
    controllo minimo DEMO. Non esegue DDL o EXPLAIN. La raccolta dei valori
    per la visualizzazione DEMO avviene esclusivamente dopo il timer.
    """
    if type(warmup) is not int or warmup < 0 or type(repetitions) is not int or repetitions < 1:
        raise ValueError("Warm-up/ripetizioni non validi.")
    if not query_ids or any(q not in ("Q1", "Q2", "Q3", "Q4") for q in query_ids):
        raise ValueError("Query non prevista dal protocollo.")
    results = {query: [] for query in query_ids} if capture_results else {}
    notify = progress_sink(progress)
    for query_id in query_ids:
        notify(kind="query", status="running", query=query_id, scenario=index_state,
               warmup_done=0, warmup_total=warmup, repetitions_done=0, repetitions_total=repetitions)
        for iteration in range(warmup + repetitions):
            parameters = dict(reference["parameters"])
            if query_id == "Q1":
                parameters["flight_id"] = reference["q1_ids"][iteration % len(reference["q1_ids"])]
            started = time.perf_counter_ns()
            try:
                actual = adapter.run_query(query_id, parameters)
                duration_ms = (time.perf_counter_ns() - started) / 1e6
                assert_equivalent(query_id, actual, expected_result(reference, query_id, parameters))
            except Exception as error:
                run.record(query_id=query_id, index_state=index_state, iteration=iteration,
                           warmup=iteration < warmup, status="error", error=str(error), parameters=parameters)
                run.flush()
                notify(kind="query", status="failed", query=query_id, scenario=index_state, message=str(error),
                       warmup_done=min(iteration, warmup), warmup_total=warmup,
                       repetitions_done=max(0, iteration - warmup), repetitions_total=repetitions)
                raise
            run.record(query_id=query_id, index_state=index_state, iteration=iteration,
                       warmup=iteration < warmup, duration_ms=duration_ms,
                       result_digest=result_digest(actual), parameters=parameters)
            if capture_results:
                results[query_id].append({"parameters": parameters, "result": actual})
            # Invio/acquisizione e confronto sono già terminati. L'osservatore
            # aggrega gli eventi senza renderizzare ogni singola osservazione.
            notify(kind="query", status="completed" if iteration + 1 == warmup + repetitions else "running",
                   query=query_id, scenario=index_state, warmup_done=min(iteration + 1, warmup), warmup_total=warmup,
                   repetitions_done=max(0, iteration + 1 - warmup), repetitions_total=repetitions)
    run.flush()
    return results


def measure_secondary_indexes(adapter: DatabaseAdapter, run: ResultRun, *, progress=None) -> None:
    """Misura separatamente ogni CREATE INDEX; non elimina indici preesistenti."""
    adapter.assert_index_state("primary_only")
    notify = progress_sink(progress)
    for column in ("origin_airport", "distance"):
        notify(kind="index", status="running", column=column)
        started = time.perf_counter_ns()
        adapter.create_secondary_index(column)
        duration_ms = (time.perf_counter_ns() - started) / 1e6
        run.record(event="create_index", query_id=column, index_state="secondary",
                   duration_ms=duration_ms)
        notify(kind="index", status="completed", column=column, duration_ms=duration_ms)
    indexes = adapter.assert_index_state("secondary")
    run.save_json("indexes.json", indexes)
    run.flush()
