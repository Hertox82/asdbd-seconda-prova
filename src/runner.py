"""Runner FULL sequenziale da Jupyter, con checkpoint e recupero per fase.

Non avvia Docker, non prepara CSV e non completa automaticamente la matrice.
Ogni chiamata riguarda soltanto config.database e config.dataset_size.
Le misure restano quelle dei moduli ingestion e benchmark approvati.
"""

from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import platform
import socket
import warnings

from src.benchmark import benchmark_phase, measure_secondary_indexes
from src.config import ExperimentConfig, selected_dataset
from src.databases.factory import create_adapter
from src.ingestion import IngestionError, ingest_csv
from src.query_validation import (assert_equivalent, build_reference,
                                  capture_database_snapshot, verify_database)
from src.results import ResultRun, _atomic_text, environment_context, result_digest, utc_now
from src.validation import sha256_file


DATABASES = ("postgres", "mongo", "cockroach")
SIZES = (100_000, 1_000_000, 5_819_079)
PHASES = ("ingestion", "baseline", "indexes", "indexed")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _save(path: Path, value: dict) -> None:
    _atomic_text(path, json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def assert_single_database(database: str) -> None:
    """Rifiuta altri endpoint DB raggiungibili nella rete Compose.

    Non equivale all'inventario container: un processo vivo con porta chiusa
    non è rilevabile senza Docker. Gli script host restano il controllo completo;
    con Compose diretto l'operatore deve fermare esplicitamente gli altri DB.
    Errori DNS diversi da nome assente non vengono ignorati silenziosamente.
    """
    endpoints = {"postgres": ("PGHOST", "PGPORT", 5432),
                 "mongo": ("MONGO_HOST", "MONGO_PORT", 27017),
                 "cockroach": ("CRDB_HOST", "CRDB_PORT", 26257)}
    if database not in endpoints:
        raise ValueError("Database non riconosciuto.")
    for other, (host_key, port_key, port) in endpoints.items():
        if other == database:
            continue
        try:
            connection = socket.create_connection(
                (os.getenv(host_key, other), int(os.getenv(port_key, str(port)))), timeout=1)
        except socket.gaierror as error:
            if error.errno != socket.EAI_NONAME:
                raise
            # Nome assente nel DNS: non prova un container spento.
            # Il controllo host descritto sopra resta necessario.
            continue
        except ConnectionRefusedError:
            continue
        except TimeoutError as error:
            raise RuntimeError(f"Stato di {other} incerto (timeout rete): controllare Compose.") from error
        else:
            connection.close()
            raise RuntimeError(f"Altro database raggiungibile: {other}. Arrestarlo prima di continuare.")


@contextmanager
def _campaign_lock(directory: Path):
    """Lock esclusivo su filesystem condiviso, senza dipendenze da shell o OS.

    Dopo un kill il lock resta intenzionalmente presente: non possiamo sapere
    se un kernel di un altro container sia ancora attivo.
    """
    path = directory / "runner.lock"
    try:
        stream = path.open("x", encoding="utf-8")
    except FileExistsError as error:
        raise RuntimeError("Campagna bloccata: attendere il runner o verificare il kernel prima dello sblocco esplicito.") from error
    try:
        with stream:
            json.dump({"hostname": platform.node(), "pid": os.getpid(), "started_at": utc_now()}, stream)
        yield
    finally:
        path.unlink(missing_ok=True)


def unlock_campaign(directory: Path, *, confirm_no_runner: bool = False) -> None:
    """Rimuove solo il lock del runner dopo verifica manuale dell'assenza di altri kernel."""
    if confirm_no_runner is not True:
        raise PermissionError("Serve confirm_no_runner=True dopo avere arrestato/verificato il vecchio kernel.")
    path = Path(directory)
    if not (path / "protocol.json").is_file():
        raise ValueError("Directory campagna non riconosciuta.")
    (path / "runner.lock").unlink(missing_ok=True)


def _protocol(config: ExperimentConfig) -> dict:
    """Congela metodo e parametri comuni; DB, dimensione e destinazioni restano selezionabili."""
    root = Path(__file__).parent
    modules = [*root.glob("*.py"), *root.joinpath("databases").glob("*.py")]
    return {"version": 1, "mode": "FULL", "sizes": list(SIZES),
            "parameters": config.parameters, "warmup": config.warmup,
            "repetitions": config.repetitions, "seed": config.seed, "batch_size": config.batch_size,
            "code_sha256": {str(p.relative_to(root)).replace("\\", "/"): sha256_file(p)
                            for p in sorted(modules)}}


def _identity(config: ExperimentConfig) -> dict:
    """Impedisce di riprendere un esperimento su un'altra tabella o con altri parametri."""
    identity = config.manifest()
    for key in ("confirm_reset", "confirm_drop_indexes", "postgres_snapshot", "mongo_snapshot"):
        identity.pop(key, None)
    identity["connection"] = _connection_context(config.database)
    return identity


def _connection_context(database: str) -> dict:
    """Registra solo endpoint e nomi pubblici; mai password, token o URI autenticati."""
    names = {"postgres": ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER"),
             "mongo": ("MONGO_HOST", "MONGO_PORT", "MONGO_USER", "MONGO_AUTH_SOURCE"),
             "cockroach": ("CRDB_HOST", "CRDB_PORT")}[database]
    return {key: os.getenv(key) for key in names}


def _changed_paths(before: dict, after: dict, prefix: str = "") -> list[str]:
    """Elenca differenze ambientali leggibili, senza imporle come vincoli di protocollo."""
    changed = []
    for key in sorted(before.keys() | after.keys()):
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(before.get(key), dict) and isinstance(after.get(key), dict):
            changed.extend(_changed_paths(before[key], after[key], path))
        elif key not in before or key not in after or before[key] != after[key]:
            changed.append(path)
    return changed


def _observe_environment(directory: Path, state: dict, adapter) -> dict:
    """Confronta ambiente comune della campagna e versione DB del singolo esperimento.

    Le variazioni producono avvisi persistenti e visibili, non un reset né un
    cambio dei parametri obbligatori. La prima osservazione non certifica
    retroattivamente l'ambiente di tentativi storici privi di questi metadati.
    """
    current = environment_context(adapter.version())
    common = {key: value for key, value in current.items() if key != "database_version"}
    baseline_path = directory / "environment_baseline.json"
    if not baseline_path.exists():
        _save(baseline_path, common)
    changes = {"campaign": _changed_paths(_read(baseline_path), common), "experiment": []}
    messages = list(current["coverage_warnings"])
    baseline = state.get("environment_baseline")
    if baseline is None:
        if state["attempts"]:
            messages.append("Checkpoint storico senza baseline ambientale completa: confronto retroattivo non garantito.")
        state["environment_baseline"] = current
    else:
        changes["experiment"] = _changed_paths(baseline, current)
    for scope, paths in changes.items():
        if paths:
            messages.append(f"Ambiente variato ({scope}): {', '.join(paths)}. Interpretare separatamente le misure.")
    observation = {"at_utc": utc_now(), "context": current, "changes": changes, "warnings": messages}
    state.setdefault("environment_history", []).append(observation)
    return observation


def _verify_completed_attempt(previous: dict, phase: str, adapter, config: ExperimentConfig,
                              reference: dict, protocol: dict, directory: Path) -> dict:
    """Adotta solo un tentativo interamente pubblicato e coerente con CSV e DB reale.

    Non modifica artefatti storici, dati o indici. Il flag completed da solo non
    basta: verifichiamo identità, protocollo, riferimenti, osservazioni attese,
    artefatti di fase e snapshot, quindi eseguiamo un nuovo audit del database.
    """
    path = Path(previous["run_dir"])
    manifest = _read(path / "manifest.json")
    identity = dict(manifest["configuration"])
    for key in ("confirm_reset", "confirm_drop_indexes", "postgres_snapshot", "mongo_snapshot"):
        identity.pop(key, None)
    if (manifest["status"] != "completed" or manifest["stage"] != phase
            or manifest["run_id"] != path.name or not manifest.get("finished_at_utc")
            or manifest["dataset_sha256"] != reference["dataset_sha256"]
            or manifest["q1_ids"] != reference["q1_ids"] or identity != _identity(config)
            or _read(path / "protocol.json") != protocol or _read(path / "reference.json") != reference):
        raise ValueError("Tentativo completato non coerente con manifest/protocollo/riferimento del checkpoint.")
    artifacts = {"ingestion": ("ingestion.json", "correctness_primary_only.json"),
                 "baseline": ("plans_primary_only.json", "correctness_primary_only.json"),
                 "indexes": ("indexes.json",),
                 "indexed": ("plans_secondary.json", "correctness_secondary.json")}[phase]
    if any(not (path / name).is_file() for name in (*artifacts, "raw.jsonl", "summary.csv", "database_snapshot.json", "peer_comparison.json")):
        raise ValueError("Tentativo completed con artefatti incompleti: adozione rifiutata.")
    rows = [json.loads(line) for line in (path / "raw.jsonl").read_text(encoding="utf-8").splitlines()]
    if any(row["status"] != "ok" or type(row["duration_ms"]) not in (int, float)
           or not math.isfinite(row["duration_ms"]) or row["duration_ms"] < 0
           or row["run_id"] != manifest["run_id"] for row in rows):
        raise ValueError("Osservazioni incomplete/non valide: adozione rifiutata.")
    if phase in ("baseline", "indexed"):
        queries = ("Q1", "Q2", "Q3", "Q4") if phase == "baseline" else ("Q2", "Q3")
        expected = [(query, i, i < config.warmup) for query in queries
                    for i in range(config.warmup + config.repetitions)]
        if [(row["query_id"], row["iteration"], row["warmup"]) for row in rows] != expected:
            raise ValueError("Iterazioni del benchmark incomplete: adozione rifiutata.")
    elif phase == "ingestion":
        ingestion = _read(path / "ingestion.json")
        if (len(rows) != 1 or rows[0]["event"] != "ingestion" or ingestion["status"] != "ok"
                or ingestion["rows_committed"] != config.dataset_size
                or ingestion["batch_size"] != config.batch_size
                or ingestion["batches_committed"] != math.ceil(config.dataset_size / config.batch_size)):
            raise ValueError("Ingestione incompleta: adozione rifiutata.")
    elif [row["query_id"] for row in rows] != ["origin_airport", "distance"]:
        raise ValueError("Misure degli indici incomplete: adozione rifiutata.")
    index_state = "secondary" if phase in ("indexes", "indexed") else "primary_only"
    current = capture_database_snapshot(adapter, reference, index_state)
    saved = _read(path / "database_snapshot.json")
    for key in ("database", "dataset_size", "dataset_sha256", "parameters", "q1_ids"):
        if saved[key] != current[key]:
            raise ValueError(f"Snapshot del tentativo non coerente: {key}.")
    if (saved["validation"]["status"] != "passed" or saved["validation"]["index_state"] != index_state
            or saved["validation"]["integrity"] != current["validation"]["integrity"]):
        raise ValueError("Snapshot del tentativo non verificato o integrità diversa.")
    for flight_id in reference["q1_ids"]:
        assert_equivalent("Q1", saved["q1_records"][str(flight_id)], current["q1_records"][str(flight_id)])
    for query in ("Q2", "Q3", "Q4"):
        assert_equivalent(query, saved["validation"]["results"][query], current["validation"]["results"][query])
    _compare_peers(directory, current)
    return {"at_utc": utc_now(), "database_version": current["database_version"], "manifest_run_id": manifest["run_id"]}


def _error_details(error: BaseException) -> dict:
    cause = error
    sqlstate = None
    code = None
    timeout = False
    while cause is not None:
        sqlstate = sqlstate or getattr(cause, "sqlstate", None)
        code = code or getattr(cause, "code", None)
        timeout = timeout or isinstance(cause, TimeoutError) or getattr(cause, "timeout", False) is True
        cause = cause.__cause__
    return {"type": type(error).__name__, "message": str(error), "sqlstate": sqlstate,
            "driver_code": code, "timeout": timeout or sqlstate in ("57014", "55P03") or code == 50,
            "at_utc": utc_now()}


def _compare_peers(directory: Path, snapshot: dict) -> dict:
    """Confronta snapshot completati della stessa dimensione, senza accendere altri DB."""
    peers = {}
    for other in DATABASES:
        if other == snapshot["database"]:
            continue
        state_path = directory / "experiments" / f"{other}_{snapshot['dataset_size']}.json"
        if not state_path.exists() or _read(state_path)["status"] != "completed":
            peers[other] = "pending"
            continue
        state = _read(state_path)
        peer = _read(Path(state["snapshot"]))
        for key in ("dataset_sha256", "dataset_size", "parameters", "q1_ids"):
            if peer[key] != snapshot[key]:
                raise ValueError(f"Snapshot {other} non omogeneo: {key}.")
        if peer["validation"]["status"] != "passed" or peer["validation"]["integrity"] != snapshot["validation"]["integrity"]:
            raise ValueError(f"Integrità diversa dallo snapshot {other}.")
        for key in snapshot["q1_ids"]:
            assert_equivalent("Q1", snapshot["q1_records"][str(key)], peer["q1_records"][str(key)])
        for query in ("Q2", "Q3", "Q4"):
            assert_equivalent(query, snapshot["validation"]["results"][query], peer["validation"]["results"][query])
        peers[other] = "passed"
    return peers


def campaign_status(directory: Path) -> dict:
    """Mostra tutte le nove combinazioni, senza eseguire alcuna query o fase."""
    directory = Path(directory)
    rows = []
    for size in SIZES:
        for database in DATABASES:
            path = directory / "experiments" / f"{database}_{size}.json"
            state = _read(path) if path.exists() else {}
            rows.append({"database": database, "dataset_size": size,
                         "status": state.get("status", "not_started"),
                         "completed_phases": state.get("completed_phases", []),
                         "snapshot": state.get("snapshot"), "error": state.get("error")})
    comparisons = {}
    for size in SIZES:
        completed = [row for row in rows if row["dataset_size"] == size and row["status"] == "completed"]
        comparisons[str(size)] = (
            _compare_peers(directory, _read(Path(completed[-1]["snapshot"])))
            if len(completed) == 3 else "pending")
    return {"experiments": rows, "comparisons_by_size": comparisons,
            "completed": all(row["status"] == "completed" for row in rows)}


def _execute_phase(phase, adapter, config, path, artifact, reference, run):
    """Coordina le funzioni approvate: nessun timer, query o ingestione alternativi."""
    if phase == "ingestion":
        if config.confirm_reset is True:
            adapter.reset_dataset(confirm_reset=True)
        else:
            adapter.create_schema()
        try:
            result = ingest_csv(adapter, path, config.dataset_size, artifact["sha256"], batch_size=config.batch_size)
        except IngestionError as error:
            run.save_json("ingestion_failed.json", error.progress)
            raise
        run.save_json("ingestion.json", result)
        run.record(event="ingestion", index_state="primary_only",
                   result_digest=result_digest(result["rows_committed"]), **result)
        run.save_json("correctness_primary_only.json", verify_database(adapter, reference, "primary_only"))
        if adapter.name in ("postgres", "cockroach"):
            adapter.analyze()
    elif phase in ("baseline", "indexed"):
        baseline = phase == "baseline"
        if baseline and adapter.name in ("postgres", "cockroach"):
            adapter.analyze()
        benchmark_phase(adapter, reference, run, index_state="primary_only" if baseline else "secondary",
                        query_ids=("Q1", "Q2", "Q3", "Q4") if baseline else ("Q2", "Q3"),
                        warmup=config.warmup, repetitions=config.repetitions)
    elif phase == "indexes":
        # Un tentativo precedente può avere creato solo uno dei due indici.
        # Non lo cancelliamo/riutilizziamo senza una conferma distinta.
        if config.confirm_drop_indexes is True:
            adapter.drop_secondary_indexes(confirm_drop=True)
        verify_database(adapter, reference, "primary_only")
        measure_secondary_indexes(adapter, run)
    state = "secondary" if phase in ("indexes", "indexed") else "primary_only"
    run.save_json("database_snapshot.json", capture_database_snapshot(adapter, reference, state))


def run_full(config: ExperimentConfig, directory: Path, *, resume: bool = False,
             stop_after: str | None = None, allow_large_dataset: bool = False) -> dict:
    """Esegue UNA combinazione FULL e restituisce lo stato persistente.

    directory identifica una campagna condivisa dai tre motori e tre dimensioni.
    resume=True riusa solo fasi completate, mai tempi di tentativi incompleti.
    stop_after permette una pausa controllata dopo ingestion/baseline/indexes.
    Un reset richiede config.confirm_reset=True; indici parziali richiedono
    config.confirm_drop_indexes=True. Non esiste ripresa per append dei batch.
    """
    if config.mode != "FULL" or config.dataset_size not in SIZES or config.database not in DATABASES:
        raise ValueError("Combinazione FULL non supportata.")
    if config.dataset_size != 100_000 and allow_large_dataset is not True:
        raise PermissionError("Campagna oltre 100k non autorizzata: richiede allow_large_dataset=True esplicito.")
    if (type(resume) is not bool or stop_after not in (*PHASES[:-1], None)
            or type(config.confirm_reset) is not bool or type(config.confirm_drop_indexes) is not bool):
        raise ValueError("Parametri di ripresa/pausa non validi.")
    directory = Path(directory).resolve()
    if directory == config.results_dir.resolve() or not directory.is_relative_to(config.results_dir.resolve()):
        raise ValueError("La campagna deve essere una sottocartella dedicata di results_dir.")
    directory.mkdir(parents=True, exist_ok=True)
    with _campaign_lock(directory):
        protocol_path = directory / "protocol.json"
        protocol = _protocol(config)
        if protocol_path.exists():
            if _read(protocol_path) != protocol:
                raise ValueError("Protocollo/codice cambiato: usare una nuova campagna, senza mescolare le misure.")
        else:
            _save(protocol_path, protocol)
        states = directory / "experiments"
        states.mkdir(exist_ok=True)
        state_path = states / f"{config.database}_{config.dataset_size}.json"
        if state_path.exists():
            state = _read(state_path)
            if not resume:
                raise ValueError("Esperimento già presente: usare resume=True oppure una nuova campagna.")
            if state["configuration"] != _identity(config):
                raise ValueError("Configurazione/destinazione diversa dal checkpoint.")
        else:
            state = {"database": config.database, "dataset_size": config.dataset_size,
                     "configuration": _identity(config), "status": "preflight",
                     "completed_phases": [], "attempts": [], "created_at": utc_now()}
        _save(state_path, state)
        adapter = None
        try:
            assert_single_database(config.database)
            path, artifact = selected_dataset(config)
            reference = build_reference(path, config.dataset_size, artifact["sha256"], config.q1_ids, config.parameters)
            if state.get("dataset_sha256", artifact["sha256"]) != artifact["sha256"]:
                raise ValueError("Dataset cambiato rispetto al checkpoint.")
            state["dataset_sha256"] = artifact["sha256"]
            adapter = create_adapter(config)
            adapter.connect()
            environment = _observe_environment(directory, state, adapter)
            _save(state_path, state)
            for message in environment["warnings"]:
                warnings.warn(message, RuntimeWarning, stacklevel=2)
            for phase in PHASES:
                if phase in state["completed_phases"]:
                    continue
                assert_single_database(config.database)
                previous = next((a for a in reversed(state["attempts"]) if a["phase"] == phase), None)
                # Crash tra commit del manifest e commit del checkpoint: adotta solo
                # un intero tentativo completato, dopo nuova verifica del DB reale.
                adopted = False
                if previous is not None and previous["status"] in ("running", "failed", "interrupted"):
                    manifest = _read(Path(previous["run_dir"]) / "manifest.json")
                    if manifest["status"] == "completed":
                        verification = _verify_completed_attempt(
                            previous, phase, adapter, config, reference, protocol, directory)
                        previous["adopted_from_status"] = previous["status"]
                        previous["status"] = "completed"
                        previous["adopted_after_interruption"] = True
                        previous["adoption_verification"] = verification
                        adopted = True
                    elif previous["status"] == "running":
                        previous["status"] = "interrupted"
                if not adopted:
                    configuration = {**config.manifest(), "connection": _connection_context(config.database)}
                    run = ResultRun(config.results_dir, configuration, reference, stage=phase, durable=True)
                    attempt = {"phase": phase, "run_dir": str(run.directory), "status": "running"}
                    state["attempts"].append(attempt)
                    state.update(status="running", current_phase=phase)
                    _save(state_path, state)
                    try:
                        with run:
                            run.set_database_version(adapter.version())
                            run.save_json("protocol.json", protocol)
                            run.save_json("environment.json", environment)
                            run.manifest["environment"] = environment
                            run.flush()
                            _execute_phase(phase, adapter, config, path, artifact, reference, run)
                            snapshot = _read(run.directory / "database_snapshot.json")
                            run.save_json("peer_comparison.json", _compare_peers(directory, snapshot))
                    except BaseException:
                        attempt["status"] = "failed"
                        raise
                    attempt["status"] = "completed"
                state["completed_phases"].append(phase)
                state.pop("error", None)
                state["status"] = "paused"
                _save(state_path, state)
                _save(directory / "status.json", campaign_status(directory))
                if stop_after == phase:
                    return state
            # Anche una ripresa di un esperimento già completato è solo lettura:
            # mai nuove query temporizzate, nuovi indici o reset impliciti.
            snapshot = capture_database_snapshot(adapter, reference, "secondary")
            comparisons = _compare_peers(directory, snapshot)
            final_run = Path(state["attempts"][-1]["run_dir"])
            state.update(status="completed", snapshot=str(final_run / "database_snapshot.json"),
                         peers=comparisons, completed_at=utc_now())
            state.pop("error", None)
            _save(state_path, state)
            _save(directory / "status.json", campaign_status(directory))
            return state
        except BaseException as error:
            state.update(status="failed" if isinstance(error, Exception) else "interrupted",
                         error=_error_details(error))
            _save(state_path, state)
            _save(directory / "status.json", campaign_status(directory))
            raise
        finally:
            if adapter is not None:
                adapter.close()
