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


def validate_snapshot(snapshot: dict, reference: dict, database: str) -> None:
    """Verifica provenienza e contratto prima di adottare un riferimento esterno."""
    if (snapshot.get("database") != database or snapshot["validation"]["status"] != "passed"
            or snapshot["validation"]["index_state"] not in ("primary_only", "secondary")):
        raise ValueError(f"Snapshot {database} non valido o non verificato.")
    for key in ("dataset_sha256", "dataset_size", "parameters", "q1_ids"):
        if snapshot[key] != reference[key]:
            raise ValueError(f"Confronto {database} non omogeneo: {key} differente.")
    if "integrity" in reference and snapshot["validation"]["integrity"] != reference["integrity"]:
        raise ValueError(f"Integrità dello snapshot {database} diversa dal CSV.")
    for flight_id in reference["q1_ids"]:
        assert_equivalent("Q1", snapshot["q1_records"][str(flight_id)], reference["q1_records"][str(flight_id)])
    for query in ("Q2", "Q3", "Q4"):
        if query in reference:
            assert_equivalent(query, snapshot["validation"]["results"][query], reference[query])


def completion_key(manifest: dict, relative_path) -> tuple:
    """Ordine comune delle discovery: fine UTC, run_id, percorso; mai mtime."""
    from datetime import datetime, timezone
    finished = datetime.fromisoformat(manifest["finished_at_utc"].replace("Z", "+00:00"))
    if finished.tzinfo is None:
        raise ValueError("Data di completamento priva di fuso orario.")
    return finished.astimezone(timezone.utc), manifest["run_id"], Path(relative_path).as_posix()


def resolve_snapshot(reference: dict, database: str, snapshot_path=None, *, results_dir=None) -> Path:
    """Trova uno snapshot compatibile, oppure rispetta l'override esplicito.

    La discovery legge solo JSON: richiede manifest FULL completato e contenuti
    validati. Ordina per fine esecuzione UTC, run_id e percorso relativo; mai
    per mtime, che cambia copiando i file. Non scrive né interroga i database.
    Gli override possono essere snapshot esportati senza manifest adiacente.
    """
    if database not in ("postgres", "mongo"):
        raise ValueError("Riferimento previsto soltanto per postgres o mongo.")
    if snapshot_path:
        path = Path(snapshot_path)
        if not path.is_file():
            raise FileNotFoundError(f"Override snapshot {database} inesistente: {path}. "
                                    "Correggere il percorso o lasciare comparison vuoto per la ricerca automatica.")
        validate_snapshot(json.loads(path.read_text(encoding="utf-8")), reference, database)
        return path
    root = Path(results_dir) if results_dir is not None else Path(__file__).resolve().parents[1] / "results"
    candidates = []
    for path in root.rglob("database_snapshot.json"):
        try:
            if not path.resolve().is_relative_to(root.resolve()):
                continue
            snapshot = json.loads(path.read_text(encoding="utf-8"))
            validate_snapshot(snapshot, reference, database)
            manifest = json.loads((path.parent / "manifest.json").read_text(encoding="utf-8"))
            config = manifest["configuration"]
            if (manifest["status"] != "completed" or config["mode"] != "FULL"
                    or config["database"] != database or config["dataset_size"] != reference["dataset_size"]
                    or config["parameters"] != reference["parameters"] or config["q1_ids"] != reference["q1_ids"]
                    or manifest["dataset_sha256"] != reference["dataset_sha256"]
                    or manifest["run_id"] != path.parent.name):
                continue
            candidates.append((*completion_key(manifest, path.relative_to(root)), path))
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            # Un artefatto incompleto, corrotto o incompatibile non è un candidato.
            continue
    if not candidates:
        label = "PostgreSQL" if database == "postgres" else "MongoDB"
        raise ValueError(f"Nessuno snapshot {label} compatibile in {root}: "
                         f"richiesti {reference['dataset_size']} record e SHA-256 {reference['dataset_sha256']}. "
                         f"Eseguire prima {label} con 02_ingestion.ipynb (100K) oppure "
                         "04_full_runner.ipynb nella campagna e dimensione desiderate. "
                         "Per dati già caricati, 03_query_benchmarks.ipynb permette l'esportazione "
                         "dello snapshot nella cella dedicata, senza avviare le celle benchmark.")
    return max(candidates)[-1]


def read_postgres_snapshot(reference: dict, snapshot_path=None, *, results_dir=None) -> dict:
    """Controlla il riferimento PostgreSQL prima di scrivere dati Mongo o misurare."""
    snapshot_path = resolve_snapshot(reference, "postgres", snapshot_path, results_dir=results_dir)
    postgres = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    validate_snapshot(postgres, reference, "postgres")
    return postgres


def compare_postgres_snapshot(adapter: DatabaseAdapter, reference: dict, snapshot_path, index_state: str, *, results_dir=None) -> dict:
    """Confronta i DB sequenzialmente: stessi file, filtri, ID e risultati reali.

    Il file PostgreSQL è risolto e verificato prima dell'audit corrente;
    nessuna connessione concorrente. Le medie usano la tolleranza comune.
    """
    snapshot_path = resolve_snapshot(reference, "postgres", snapshot_path, results_dir=results_dir)
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


def read_mongo_snapshot(reference: dict, snapshot_path=None, *, results_dir=None) -> dict:
    """Verifica identità e provenienza del riferimento Mongo senza aprire altri DB."""
    snapshot_path = resolve_snapshot(reference, "mongo", snapshot_path, results_dir=results_dir)
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    validate_snapshot(snapshot, reference, "mongo")
    return snapshot


def compare_reference_snapshots(adapter: DatabaseAdapter, reference: dict, postgres_path,
                                mongo_path, index_state: str, *, results_dir=None) -> dict:
    """Confronta Cockroach con CSV e snapshot reali dei due DB arrestati.

    L'audit del DB corrente viene eseguito una sola volta per entrambi i confronti,
    mantenendo memoria limitata e tutti i timeout dichiarati per 100k.
    """
    postgres_path = resolve_snapshot(reference, "postgres", postgres_path, results_dir=results_dir)
    mongo_path = resolve_snapshot(reference, "mongo", mongo_path, results_dir=results_dir)
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
