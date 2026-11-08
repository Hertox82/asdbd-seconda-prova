"""Riferimenti CSV e controlli PostgreSQL/MongoDB fuori dalle misure.

Non modifica i moduli di preprocessing approvati né crea dati nei database.
"""

import math
import json
from pathlib import Path

from src.databases.base import DatabaseAdapter
from src.schema import PREPARED_COLUMNS, RAW_COLUMNS
from src.validation import iter_prepared_chunks, sha256_file


ABS_TOLERANCE = 1e-9
REL_TOLERANCE = 1e-9


def build_reference(path, expected_rows: int, expected_sha256: str, q1_ids: list[int], parameters: dict) -> dict:
    """Scansiona solo il CSV selezionato e genera attesi per conteggi, null, Q1–Q4.

    Gli ID sono letti, mai assegnati. Per AVG conserva somme/conteggi interi per
    compagnia e ignora soltanto None, includendo anche gruppi interamente nulli.
    """
    if sha256_file(path) != expected_sha256:
        raise ValueError("Checksum del CSV selezionato non valido.")
    if not q1_ids or any(type(i) is not int or not 1 <= i <= expected_rows for i in q1_ids):
        raise ValueError("ID Q1 fuori dal dataset selezionato.")
    wanted = set(q1_ids)
    samples, groups = {}, {}
    nulls = [0] * len(RAW_COLUMNS)
    count, airport, distance = 0, 0, 0
    for chunk in iter_prepared_chunks(path):
        for row in chunk:
            count += 1
            if row[0] in wanted:
                samples[str(row[0])] = dict(zip(PREPARED_COLUMNS, row))
            for index, value in enumerate(row[1:]):
                nulls[index] += value is None
            airport += row[8] == parameters["origin_airport"]
            distance += parameters["distance_min"] <= row[18] <= parameters["distance_max"]
            airline, delay = row[5], row[12]
            totals = groups.setdefault(airline, [0, 0])
            if delay is not None:
                totals[0] += delay
                totals[1] += 1
    if count != expected_rows or len(samples) != len(wanted) or sha256_file(path) != expected_sha256:
        raise ValueError("CSV incompleto, campioni mancanti o sorgente cambiata durante la lettura.")
    return {
        "dataset_sha256": expected_sha256, "dataset_size": count, "parameters": parameters,
        "q1_ids": q1_ids, "q1_records": samples, "Q2": airport, "Q3": distance,
        "Q4": {airline: total / n if n else None for airline, (total, n) in sorted(groups.items())},
        "integrity": {"rows": count, "flight_id_min": 1, "flight_id_max": count,
                      "distinct_ids": count, "null_counts": dict(zip(RAW_COLUMNS, nulls))},
        "tolerance": {"absolute": ABS_TOLERANCE, "relative": REL_TOLERANCE},
    }


def expected_result(reference: dict, query_id: str, parameters: dict):
    """Restituisce l'atteso della singola operazione, riferito al dataset corrente."""
    if query_id == "Q1":
        return reference["q1_records"][str(parameters["flight_id"])]
    return reference[query_id]


def assert_equivalent(query_id: str, actual, expected) -> None:
    """Interrompe per differenze; Q4 usa tolleranza, Q1 verifica anche i tipi Python."""
    if query_id == "Q4":
        valid = isinstance(actual, dict) and actual.keys() == expected.keys()
        if valid:
            for airline, value in expected.items():
                other = actual[airline]
                if value is None:
                    valid = valid and other is None
                else:
                    valid = valid and type(other) in (int, float) and math.isclose(
                        other, value, abs_tol=ABS_TOLERANCE, rel_tol=REL_TOLERANCE)
    elif query_id == "Q1":
        valid = isinstance(actual, dict) and actual == expected and all(
            type(actual[key]) is type(value) for key, value in expected.items())
    else:
        valid = type(actual) is int and actual == expected
    if not valid:
        raise ValueError(f"Risultato {query_id} diverso dal riferimento CSV: atteso={expected!r}, ottenuto={actual!r}.")


def verify_database(adapter: DatabaseAdapter, reference: dict, index_state: str) -> dict:
    """Verifica schema, indici, righe/ID/null e risultati Q1–Q4, prima di misurare."""
    adapter.validate_schema()
    indexes = adapter.assert_index_state(index_state)
    integrity = adapter.integrity_summary()
    if integrity != reference["integrity"]:
        raise ValueError(f"Integrità DB diversa dal CSV: attesa={reference['integrity']!r}, ottenuta={integrity!r}.")
    results = {}
    for flight_id in reference["q1_ids"]:
        params = {**reference["parameters"], "flight_id": flight_id}
        actual = adapter.run_query("Q1", params)
        assert_equivalent("Q1", actual, expected_result(reference, "Q1", params))
    for query_id in ("Q2", "Q3", "Q4"):
        actual = adapter.run_query(query_id, reference["parameters"])
        assert_equivalent(query_id, actual, reference[query_id])
        results[query_id] = actual
    report = {"status": "passed", "index_state": index_state, "integrity": integrity,
            "q1_ids_verified": reference["q1_ids"], "results": results,
            "indexes": indexes, "tolerance": reference["tolerance"]}
    if getattr(adapter, "last_bson_audit", None) is not None:
        report["bson_audit"] = adapter.last_bson_audit
    return report


def capture_database_snapshot(adapter: DatabaseAdapter, reference: dict, index_state: str) -> dict:
    """Salva risultati reali verificati per un confronto sequenziale, senza DB concorrenti."""
    validation = verify_database(adapter, reference, index_state)
    records = {str(i): adapter.run_query("Q1", {**reference["parameters"], "flight_id": i})
               for i in reference["q1_ids"]}
    return {"database": adapter.name, "database_version": adapter.version(),
            "dataset_sha256": reference["dataset_sha256"], "dataset_size": reference["dataset_size"],
            "parameters": reference["parameters"], "q1_ids": reference["q1_ids"],
            "q1_records": records, "validation": validation}


def read_postgres_snapshot(reference: dict, snapshot_path) -> dict:
    """Controlla il riferimento PostgreSQL prima di scrivere dati Mongo o misurare."""
    if snapshot_path is None:
        raise ValueError("Manca comparison.postgres_snapshot: esportare prima uno snapshot PostgreSQL verificato.")
    postgres = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    if postgres.get("database") != "postgres" or postgres["validation"]["status"] != "passed":
        raise ValueError("Snapshot PostgreSQL non valido o non verificato.")
    for key in ("dataset_sha256", "dataset_size", "parameters", "q1_ids"):
        if postgres[key] != reference[key]:
            raise ValueError(f"Confronto PostgreSQL non omogeneo: {key} differente.")
    return postgres


def compare_postgres_snapshot(adapter: DatabaseAdapter, reference: dict, snapshot_path, index_state: str) -> dict:
    """Confronta i DB sequenzialmente: stessi file, filtri, ID e risultati reali.

    Il file PostgreSQL è scelto esplicitamente; nessuna connessione concorrente
    o ricerca ambigua dell'ultimo risultato. Le medie usano la tolleranza comune.
    """
    postgres = read_postgres_snapshot(reference, snapshot_path)
    current = capture_database_snapshot(adapter, reference, index_state)
    if current["validation"]["integrity"] != postgres["validation"]["integrity"]:
        raise ValueError("Integrità differente fra Mongo e snapshot PostgreSQL.")
    for key in reference["q1_ids"]:
        assert_equivalent("Q1", current["q1_records"][str(key)], postgres["q1_records"][str(key)])
    for query_id in ("Q2", "Q3", "Q4"):
        assert_equivalent(query_id, current["validation"]["results"][query_id], postgres["validation"]["results"][query_id])
    return {"status": "passed", "postgres_snapshot": str(snapshot_path),
            "postgres_version": postgres["database_version"], "current": current}


def read_mongo_snapshot(reference: dict, snapshot_path) -> dict:
    """Verifica identità e provenienza del riferimento Mongo senza aprire altri DB."""
    if snapshot_path is None:
        raise ValueError("Manca comparison.mongo_snapshot: esportare uno snapshot Mongo verificato.")
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    if snapshot.get("database") != "mongo" or snapshot["validation"]["status"] != "passed":
        raise ValueError("Snapshot Mongo non valido o non verificato.")
    for key in ("dataset_sha256", "dataset_size", "parameters", "q1_ids"):
        if snapshot[key] != reference[key]:
            raise ValueError(f"Confronto Mongo non omogeneo: {key} differente.")
    return snapshot


def compare_reference_snapshots(adapter: DatabaseAdapter, reference: dict, postgres_path,
                                mongo_path, index_state: str) -> dict:
    """Confronta Cockroach con CSV e snapshot reali dei due DB arrestati.

    L'audit del DB corrente viene eseguito una sola volta per entrambi i confronti,
    mantenendo memoria limitata e tutti i timeout dichiarati per 100k.
    """
    snapshots = {"postgres": read_postgres_snapshot(reference, postgres_path),
                 "mongo": read_mongo_snapshot(reference, mongo_path)}
    current = capture_database_snapshot(adapter, reference, index_state)
    for database, snapshot in snapshots.items():
        if current["validation"]["integrity"] != snapshot["validation"]["integrity"]:
            raise ValueError(f"Integrità differente dal riferimento {database}.")
        for key in reference["q1_ids"]:
            assert_equivalent("Q1", current["q1_records"][str(key)], snapshot["q1_records"][str(key)])
        for query_id in ("Q2", "Q3", "Q4"):
            assert_equivalent(query_id, current["validation"]["results"][query_id], snapshot["validation"]["results"][query_id])
    return {"status": "passed", "reference_versions": {name: value["database_version"] for name, value in snapshots.items()},
            "postgres_snapshot": str(postgres_path), "mongo_snapshot": str(mongo_path), "current": current}
