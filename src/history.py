"""Consultazione in sola lettura delle campagne concluse: nessuna connessione DB.

I percorsi sono scelti esplicitamente nel catalogo; non cerchiamo l'ultimo run.
Non ricalcoliamo né riscriviamo statistiche, checkpoint o protocolli storici.
"""

import csv
import json
import math
from pathlib import Path

from src.query_validation import assert_equivalent
from src.validation import sha256_file


PHASES = ("ingestion", "baseline", "indexes", "indexed")


def read_json(path: Path):
    """Legge UTF-8; errori o file mancanti impediscono una falsa consultazione valida."""
    return json.loads(path.read_text(encoding="utf-8"))


def run_path(stored: str, results_root: Path) -> Path:
    """Ricollega un run al volume results corrente, anche dopo copia tra OS.

    Gli artefatti Jupyter usano /workspace/results: conserviamo l'identificativo
    della directory, verificato poi col manifest, anziché dipendere dall'host.
    """
    name = Path(stored.replace("\\", "/")).name
    path = (Path(results_root) / name).resolve()
    if path.parent != Path(results_root).resolve():
        raise ValueError("Percorso run esterno alla cartella results.")
    return path


def read_experiment(campaign: Path, database: str, size: int, results_root: Path) -> dict:
    """Carica i quattro tentativi conclusi e verifica coerenza e provenienza.

    La verifica riguarda gli artefatti approvati, non la versione corrente del
    codice: le campagne chiuse restano consultabili dopo l'aggiunta della DEMO.
    """
    campaign = Path(campaign).resolve()
    results_root = Path(results_root).resolve()
    if not campaign.is_relative_to(results_root) or campaign == results_root:
        raise ValueError("La campagna deve appartenere a results.")
    state_file = campaign / "experiments" / f"{database}_{size}.json"
    state = read_json(state_file)
    if (state.get("status") != "completed" or state.get("database") != database
            or state.get("dataset_size") != size or state.get("completed_phases") != list(PHASES)):
        raise ValueError("Campagna non completata per database/dimensione selezionati.")
    protocol_file = campaign / "protocol.json"
    protocol = read_json(protocol_file)
    paths, manifests, summaries = {}, {}, []
    source_files = [state_file, protocol_file]
    reference = None
    configuration = None
    for phase in PHASES:
        attempts = [a for a in state["attempts"] if a["phase"] == phase]
        if not attempts or attempts[-1]["status"] != "completed":
            raise ValueError(f"Tentativo {phase} non completato.")
        path = run_path(attempts[-1]["run_dir"], results_root)
        manifest_file = path / "manifest.json"
        manifest = read_json(manifest_file)
        config = manifest["configuration"]
        current_reference = read_json(path / "reference.json")
        if (manifest["status"] != "completed" or manifest["stage"] != phase
                or manifest["run_id"] != path.name or config["mode"] != "FULL"
                or config["database"] != database or config["dataset_size"] != size
                or manifest["dataset_sha256"] != current_reference["dataset_sha256"]
                or current_reference["dataset_size"] != size
                or current_reference["parameters"] != config["parameters"]
                or current_reference["q1_ids"] != config["q1_ids"]):
            raise ValueError(f"Manifest/riferimento incoerente per {phase}.")
        if reference is None:
            reference, configuration = current_reference, config
        elif reference != current_reference:
            raise ValueError("Riferimenti diversi fra le fasi della campagna.")
        for key in ("parameters", "seed", "warmup", "repetitions", "batch_size"):
            if config[key] != protocol[key] or config[key] != configuration[key]:
                raise ValueError(f"Protocollo storico incoerente: {key}.")
        for key in ("schema", "table", "mongo_database", "mongo_collection", "cockroach_database",
                    "cockroach_schema", "cockroach_table"):
            if config[key] != configuration[key]:
                raise ValueError("Destinazioni diverse fra le fasi storiche.")
        # I vecchi run manuali non sono selezionati: il catalogo usa il runner.
        if read_json(path / "protocol.json") != protocol:
            raise ValueError("Protocollo del run diverso dalla campagna.")
        summary_file = path / "summary.csv"
        with summary_file.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        expected = {"ingestion": [("ingestion", "", "primary_only")],
                    "baseline": [("query", q, "primary_only") for q in ("Q1", "Q2", "Q3", "Q4")],
                    "indexes": [("create_index", q, "secondary") for q in ("origin_airport", "distance")],
                    "indexed": [("query", q, "secondary") for q in ("Q2", "Q3")]}[phase]
        if [(r["event"], r["query_id"], r["index_state"]) for r in rows] != expected:
            raise ValueError("Riepilogo storico incompleto o con scenari inattesi.")
        for row in rows:
            n = int(row["valid_repetitions"])
            if n != (config["repetitions"] if row["event"] == "query" else 1):
                raise ValueError("Numero misure storico incoerente.")
            numeric = {}
            for key in ("mean_ms", "median_ms", "sample_stddev_ms", "min_ms", "max_ms", "rows_per_second"):
                value = float(row[key]) if row[key] else None
                if value is not None and (not math.isfinite(value) or value < 0):
                    raise ValueError("Statistica storica non valida.")
                numeric[key] = value
            if any(numeric[key] is None for key in ("mean_ms", "median_ms", "min_ms", "max_ms")):
                raise ValueError("Statistiche fondamentali mancanti nel riepilogo storico.")
            summaries.append({**row, **numeric, "valid_repetitions": n, "mode": "FULL",
                              "database": database, "dataset_size": size, "source_run": path.name,
                              "source_campaign": str(campaign)})
        paths[phase], manifests[phase] = path, manifest
        source_files.extend([manifest_file, path / "reference.json", path / "protocol.json", summary_file])
    snapshot_file = paths["indexed"] / "database_snapshot.json"
    snapshot = read_json(snapshot_file)
    if (snapshot["database"] != database or snapshot["dataset_size"] != size
            or snapshot["dataset_sha256"] != reference["dataset_sha256"]
            or snapshot["parameters"] != reference["parameters"] or snapshot["q1_ids"] != reference["q1_ids"]
            or snapshot["validation"]["status"] != "passed"
            or snapshot["validation"]["index_state"] != "secondary"
            or snapshot["validation"]["integrity"] != reference["integrity"]):
        raise ValueError("Snapshot finale storico non valido.")
    for flight_id in reference["q1_ids"]:
        assert_equivalent("Q1", snapshot["q1_records"][str(flight_id)], reference["q1_records"][str(flight_id)])
    for query in ("Q2", "Q3", "Q4"):
        assert_equivalent(query, snapshot["validation"]["results"][query], reference[query])
    plans = {}
    for phase, scenario in (("baseline", "primary_only"), ("indexed", "secondary")):
        file = paths[phase] / f"plans_{scenario}.json"
        plans[scenario] = read_json(file)
        source_files.append(file)
    source_files.append(snapshot_file)
    return {"campaign": campaign, "configuration": configuration, "reference": reference,
            "snapshot": snapshot, "summaries": summaries, "plans": plans, "manifests": manifests,
            "source_hashes": {str(p): sha256_file(p) for p in source_files}}


def read_catalog(campaigns: dict[int, Path], results_root: Path) -> list[dict]:
    """Legge le nove combinazioni approvate; nessun tempo DEMO viene mescolato."""
    return [row for size, campaign in sorted(campaigns.items())
            for database in ("postgres", "mongo", "cockroach")
            for row in read_experiment(campaign, database, size, results_root)["summaries"]]
