"""Checkpoint e recupero con fixture sintetiche: mai risultati sperimentali reali."""

from dataclasses import replace
import json
from pathlib import Path
import socket
from unittest.mock import Mock

import pytest

import src.runner as runner
from src.config import load_config
from test_queries import MemoryAdapter, query_fixture


class StatefulAdapter(MemoryAdapter):
    """Modello minimo dello stato persistente, per provocare errori controllati."""

    def __init__(self, reference):
        super().__init__(reference)
        self.rows = []
        self.secondary = []
        self.reset_calls = 0
        self.drop_calls = 0
        self.fail_insert = False
        self.fail_index = False

    def connect(self): pass
    def close(self): pass
    def create_schema(self): pass
    def analyze(self): pass
    def version(self): return "synthetic-only"
    def count_rows(self): return len(self.rows)

    def ingest_batch(self, rows):
        if self.fail_insert and self.rows:
            raise TimeoutError("timeout sintetico batch")
        self.rows.extend(rows)

    def reset_dataset(self, *, confirm_reset=False):
        assert confirm_reset is True
        self.reset_calls += 1
        self.rows.clear()
        self.secondary.clear()

    def indexes(self):
        return [{"primary": True, "columns": ["flight_id"], "valid": True}] + [
            {"primary": False, "columns": [column], "valid": True} for column in self.secondary]

    def assert_index_state(self, state):
        expected = [] if state == "primary_only" else ["distance", "origin_airport"]
        if sorted(self.secondary) != expected:
            raise ValueError("Stato indici diverso")
        return self.indexes()

    def create_secondary_index(self, column):
        if self.fail_index and self.secondary:
            raise TimeoutError("timeout sintetico indice")
        self.secondary.append(column)

    def drop_secondary_indexes(self, *, confirm_drop=False):
        assert confirm_drop is True
        self.drop_calls += 1
        self.secondary.clear()


@pytest.fixture
def experiment(query_fixture, tmp_path, monkeypatch):
    path, artifact, reference = query_fixture
    config = replace(load_config(Path("config/benchmark.yaml")), database="postgres",
                     dataset_size=7, results_dir=tmp_path / "results", batch_size=3,
                     warmup=1, repetitions=3, q1_ids=reference["q1_ids"])
    adapter = StatefulAdapter(reference)
    # Solo i test di macchina a stati ammettono 7 righe. In produzione le tre
    # dimensioni canoniche rimangono obbligatorie; nessun benchmark reale qui.
    monkeypatch.setattr(runner, "SIZES", (7,))
    monkeypatch.setattr(runner, "selected_dataset", lambda config: (path, artifact))
    monkeypatch.setattr(runner, "create_adapter", lambda config: adapter)
    monkeypatch.setattr(runner, "assert_single_database", Mock())
    monkeypatch.setattr(runner, "environment_context", lambda version: {
        "database_version": version, "python_version": "synthetic-python",
        "packages": {"psycopg": "synthetic-version"}, "docker_sources": {"docker-compose.yml": "synthetic-sha"},
        "runtime": {"cgroup_memory_limit": "synthetic-limit"}, "coverage_warnings": []})
    directory = config.results_dir / "synthetic-campaign"
    return config, adapter, directory


def execute(config, directory, **kwargs):
    return runner.run_full(config, directory, allow_large_dataset=True, **kwargs)


def test_pause_resume_and_completed_run_do_not_repeat_writes(experiment):
    config, adapter, directory = experiment
    first = execute(config, directory, stop_after="ingestion")
    assert first["completed_phases"] == ["ingestion"]
    assert adapter.count_rows() == 7
    with pytest.raises(ValueError, match="già presente"):
        execute(config, directory)
    complete = execute(config, directory, resume=True)
    assert complete["status"] == "completed"
    assert len(complete["attempts"]) == 4
    again = execute(config, directory, resume=True)
    assert again["attempts"] == complete["attempts"]
    assert adapter.reset_calls == adapter.drop_calls == 0
    assert runner.campaign_status(directory)["completed"] is False  # altri motori ancora pending


def test_partial_ingestion_is_not_appended_or_reset_without_confirmation(experiment):
    config, adapter, directory = experiment
    adapter.fail_insert = True
    with pytest.raises(runner.IngestionError):
        execute(config, directory)
    assert adapter.count_rows() == 3
    state = runner._read(directory / "experiments/postgres_7.json")
    assert state["error"]["timeout"] is True
    failed = Path(state["attempts"][-1]["run_dir"])
    assert runner._read(failed / "ingestion_failed.json")["rows_committed"] == 3
    assert not (failed / "summary.csv").exists()
    adapter.fail_insert = False
    with pytest.raises(ValueError, match="popolata"):
        execute(config, directory, resume=True)
    assert adapter.reset_calls == 0 and adapter.count_rows() == 3
    result = execute(replace(config, confirm_reset=True), directory, resume=True)
    assert result["status"] == "completed" and adapter.reset_calls == 1
    assert failed.exists()


def test_partial_indexes_require_explicit_drop_and_remeasure(experiment):
    config, adapter, directory = experiment
    adapter.fail_index = True
    with pytest.raises(TimeoutError):
        execute(config, directory)
    assert adapter.secondary == ["origin_airport"]
    adapter.fail_index = False
    with pytest.raises(ValueError, match="Stato indici"):
        execute(config, directory, resume=True)
    assert adapter.drop_calls == 0 and adapter.count_rows() == 7
    complete = execute(replace(config, confirm_drop_indexes=True), directory, resume=True)
    assert complete["status"] == "completed" and adapter.drop_calls == 1
    index_attempts = [a for a in complete["attempts"] if a["phase"] == "indexes"]
    assert len(index_attempts) == 3
    assert not (Path(index_attempts[0]["run_dir"]) / "summary.csv").exists()


def test_query_timeout_restarts_whole_phase_with_fresh_warmups(experiment, monkeypatch):
    config, adapter, directory = experiment
    execute(config, directory, stop_after="ingestion")
    original = runner.benchmark_phase

    def fail_after_raw(adapter, reference, run, **kwargs):
        run.record(query_id="Q2", duration_ms=1, warmup=True)
        assert (run.directory / "raw.jsonl").read_text()
        raise TimeoutError("timeout query sintetico")

    monkeypatch.setattr(runner, "benchmark_phase", fail_after_raw)
    with pytest.raises(TimeoutError):
        execute(config, directory, resume=True)
    monkeypatch.setattr(runner, "benchmark_phase", original)
    result = execute(config, directory, resume=True)
    attempts = [a for a in result["attempts"] if a["phase"] == "baseline"]
    assert len(attempts) == 2 and attempts[0]["status"] == "failed"
    assert not (Path(attempts[0]["run_dir"]) / "summary.csv").exists()
    records = [json.loads(line) for line in (Path(attempts[1]["run_dir"]) / "raw.jsonl").read_text().splitlines()]
    assert len(records) == 16 and sum(row["warmup"] for row in records) == 4
    assert adapter.reset_calls == 0


def test_commit_checkpoint_gap_adopts_only_completed_attempt(experiment):
    config, adapter, directory = experiment
    paused = execute(config, directory, stop_after="ingestion")
    paused["completed_phases"] = []
    paused["attempts"][0]["status"] = "running"
    runner._save(directory / "experiments/postgres_7.json", paused)
    resumed = execute(config, directory, resume=True)
    assert resumed["attempts"][0]["adopted_after_interruption"] is True
    assert len(resumed["attempts"]) == 4 and adapter.reset_calls == 0


def test_kernel_loss_keeps_partial_raw_and_starts_new_query_attempt(experiment):
    config, adapter, directory = experiment
    state = execute(config, directory, stop_after="ingestion")
    run = runner.ResultRun(config.results_dir, config.manifest(), adapter.reference,
                           stage="baseline", durable=True)
    run.save_json("protocol.json", runner._read(directory / "protocol.json"))
    run.record(query_id="Q1", warmup=True, duration_ms=1)
    # Nessuna uscita dal context: modello di processo perso con manifest running.
    state["attempts"].append({"phase": "baseline", "run_dir": str(run.directory), "status": "running"})
    runner._save(directory / "experiments/postgres_7.json", state)
    complete = execute(config, directory, resume=True)
    attempts = [a for a in complete["attempts"] if a["phase"] == "baseline"]
    assert [a["status"] for a in attempts] == ["interrupted", "completed"]
    assert len((run.directory / "raw.jsonl").read_text().splitlines()) == 1
    assert not (run.directory / "summary.csv").exists()
    assert adapter.reset_calls == 0


def test_timeout_diagnostic_preserves_wrapped_sqlstate():
    import psycopg
    from pymongo.errors import ExecutionTimeout, NetworkTimeout
    outer = RuntimeError("fase fallita")
    outer.__cause__ = psycopg.errors.QueryCanceled("statement timeout sintetico")
    details = runner._error_details(outer)
    assert details["sqlstate"] == "57014" and details["timeout"] is True
    assert runner._error_details(ExecutionTimeout("sintetico", code=50))["timeout"] is True
    assert runner._error_details(NetworkTimeout("sintetico"))["timeout"] is True


def test_protocol_and_target_changes_are_rejected(experiment):
    config, _, directory = experiment
    execute(config, directory, stop_after="ingestion")
    with pytest.raises(ValueError, match="Protocollo"):
        execute(replace(config, repetitions=4), directory, resume=True)
    with pytest.raises(ValueError, match="destinazione"):
        execute(replace(config, table="another"), directory, resume=True)


def test_stale_lock_needs_manual_confirmation(experiment):
    config, _, directory = experiment
    execute(config, directory, stop_after="ingestion")
    (directory / "runner.lock").write_text("synthetic old kernel")
    with pytest.raises(RuntimeError, match="bloccata"):
        execute(config, directory, resume=True)
    with pytest.raises(PermissionError):
        runner.unlock_campaign(directory)
    runner.unlock_campaign(directory, confirm_no_runner=True)
    assert execute(config, directory, resume=True)["status"] == "completed"


def test_large_dataset_requires_explicit_enable_before_any_io(tmp_path):
    config = replace(load_config(Path("config/benchmark.yaml")), dataset_size=1_000_000)
    with pytest.raises(PermissionError, match="oltre 100k"):
        runner.run_full(config, tmp_path / "never-created")
    assert not (tmp_path / "never-created").exists()


def test_network_guard_rejects_other_database_and_uncertain_timeout(monkeypatch):
    connection = Mock()
    monkeypatch.setattr(socket, "create_connection", lambda *args, **kwargs: connection)
    with pytest.raises(RuntimeError, match="Altro database"):
        runner.assert_single_database("cockroach")
    connection.close.assert_called_once()
    monkeypatch.setattr(socket, "create_connection", Mock(side_effect=TimeoutError()))
    with pytest.raises(RuntimeError, match="incerto"):
        runner.assert_single_database("cockroach")


def test_completed_peer_snapshot_is_compared_and_difference_stops_run(experiment, monkeypatch):
    config, adapter, directory = experiment
    postgres = execute(config, directory)
    snapshot_path = Path(postgres["snapshot"])
    snapshot = runner._read(snapshot_path)
    snapshot["validation"]["results"]["Q3"] += 1
    runner._save(snapshot_path, snapshot)
    mongo = StatefulAdapter(adapter.reference)
    mongo.name = "mongo"
    monkeypatch.setattr(runner, "create_adapter", lambda config: mongo)
    with pytest.raises(ValueError, match="Q3"):
        execute(replace(config, database="mongo"), directory)
    state = runner._read(directory / "experiments/mongo_7.json")
    assert state["status"] == "failed" and state["completed_phases"] == []
    assert not (Path(state["attempts"][0]["run_dir"]) / "summary.csv").exists()


@pytest.mark.parametrize("flag", ("confirm_reset", "confirm_drop_indexes"))
def test_non_boolean_confirmation_is_rejected_before_io(experiment, flag):
    config, adapter, directory = experiment
    with pytest.raises(ValueError, match="Parametri"):
        execute(replace(config, **{flag: "True"}), directory)
    assert adapter.reset_calls == adapter.drop_calls == 0
    assert not directory.exists()


@pytest.mark.parametrize("size", (100_000, 1_000_000, 5_819_079))
def test_config_accepts_three_dimensions_without_executing_them(tmp_path, size):
    import yaml
    data = yaml.safe_load(Path("config/benchmark.yaml").read_text(encoding="utf-8"))
    data["dataset_size"] = size
    path = tmp_path / "config/benchmark.yaml"
    path.parent.mkdir()
    path.write_text(yaml.safe_dump(data))
    config = load_config(path)
    assert config.dataset_size == size
    assert config.q1_ids[:2] == [1, size]


@pytest.mark.parametrize("phase", runner.PHASES)
def test_failed_checkpoint_adopts_completed_manifest_without_reexecution(experiment, monkeypatch, phase):
    """Errore reale del controllo dopo la pubblicazione finale, non fase simulata come valida."""
    config, adapter, directory = experiment
    original_exit = runner.ResultRun.__exit__

    def fail_after_commit(run, error_type, error, traceback):
        result = original_exit(run, error_type, error, traceback)
        if error_type is None and run.manifest["stage"] == phase:
            raise OSError("errore successivo al manifest completed")
        return result

    monkeypatch.setattr(runner.ResultRun, "__exit__", fail_after_commit)
    with pytest.raises(OSError, match="successivo"):
        execute(config, directory)
    checkpoint = runner._read(directory / "experiments/postgres_7.json")
    attempt = checkpoint["attempts"][-1]
    path = Path(attempt["run_dir"])
    assert attempt["status"] == "failed"
    assert runner._read(path / "manifest.json")["status"] == "completed"
    hashes = {p.name: runner.sha256_file(p) for p in path.iterdir()}
    monkeypatch.setattr(runner.ResultRun, "__exit__", original_exit)
    original_execute = runner._execute_phase

    def forbid_duplicate(current_phase, *args):
        assert current_phase != phase, "fase già completata rieseguita"
        return original_execute(current_phase, *args)

    monkeypatch.setattr(runner, "_execute_phase", forbid_duplicate)
    resumed = execute(config, directory, resume=True)
    adopted = next(a for a in resumed["attempts"] if a["phase"] == phase)
    assert adopted["adopted_from_status"] == "failed"
    assert adopted["adoption_verification"]["manifest_run_id"] == path.name
    assert len(resumed["attempts"]) == 4 and resumed["status"] == "completed"
    assert hashes == {p.name: runner.sha256_file(p) for p in path.iterdir()}
    assert adapter.reset_calls == adapter.drop_calls == 0 and adapter.count_rows() == 7


@pytest.mark.parametrize("damage", ("protocol", "manifest", "raw", "snapshot", "database"))
def test_completed_flag_does_not_override_incoherent_artifacts_or_database(experiment, damage):
    config, adapter, directory = experiment
    state = execute(config, directory, stop_after="ingestion")
    state["completed_phases"] = []
    state["attempts"][0]["status"] = "failed"
    path = Path(state["attempts"][0]["run_dir"])
    runner._save(directory / "experiments/postgres_7.json", state)
    if damage == "protocol":
        protocol = runner._read(path / "protocol.json")
        protocol["repetitions"] += 1
        runner._save(path / "protocol.json", protocol)
    elif damage == "manifest":
        manifest = runner._read(path / "manifest.json")
        manifest["configuration"]["table"] = "other_table"
        runner._save(path / "manifest.json", manifest)
    elif damage == "raw":
        (path / "raw.jsonl").write_text("")
    elif damage == "snapshot":
        snapshot = runner._read(path / "database_snapshot.json")
        snapshot["validation"]["results"]["Q3"] += 1
        runner._save(path / "database_snapshot.json", snapshot)
    else:
        adapter.invalid = True
    with pytest.raises(ValueError):
        execute(config, directory, resume=True)
    after = runner._read(directory / "experiments/postgres_7.json")
    assert len(after["attempts"]) == 1 and after["completed_phases"] == []
    assert not after["attempts"][0].get("adopted_after_interruption", False)
    assert adapter.reset_calls == adapter.drop_calls == 0


def test_failed_manifest_with_complete_data_is_not_adopted(experiment):
    config, adapter, directory = experiment
    state = execute(config, directory, stop_after="ingestion")
    state["completed_phases"] = []
    state["attempts"][0]["status"] = "failed"
    path = Path(state["attempts"][0]["run_dir"])
    manifest = runner._read(path / "manifest.json")
    manifest["status"] = "failed"
    runner._save(path / "manifest.json", manifest)
    runner._save(directory / "experiments/postgres_7.json", state)
    with pytest.raises(ValueError, match="popolata"):
        execute(config, directory, resume=True)
    after = runner._read(directory / "experiments/postgres_7.json")
    assert not after["attempts"][0].get("adopted_after_interruption", False)
    assert adapter.count_rows() == 7 and adapter.reset_calls == 0


@pytest.mark.parametrize("section,key,value", (
    (None, "database_version", "another-server-version"),
    (None, "python_version", "another-python-version"),
    ("packages", "psycopg", "another-driver-version"),
    ("docker_sources", "docker-compose.yml", "another-compose-sha"),
    ("runtime", "cgroup_memory_limit", "another-memory-limit")))
def test_environment_change_warns_and_is_recorded_without_repeating_measurements(
        experiment, monkeypatch, section, key, value):
    config, adapter, directory = experiment
    before = execute(config, directory)
    original_context = runner.environment_context

    def changed_context(version):
        context = original_context(version)
        (context if section is None else context[section])[key] = value
        return context

    monkeypatch.setattr(runner, "environment_context", changed_context)
    with pytest.warns(RuntimeWarning, match="Ambiente variato"):
        after = execute(config, directory, resume=True)
    assert before["attempts"] == after["attempts"]
    observation = after["environment_history"][-1]
    assert (f"{section}.{key}" if section else key) in observation["changes"]["experiment"]
    assert observation["warnings"] and after["status"] == "completed"
    assert adapter.reset_calls == adapter.drop_calls == 0


def test_untracked_old_environment_is_not_silently_certified(experiment):
    config, _, directory = experiment
    state = execute(config, directory, stop_after="ingestion")
    state.pop("environment_baseline")
    state.pop("environment_history")
    runner._save(directory / "experiments/postgres_7.json", state)
    with pytest.warns(RuntimeWarning, match="Checkpoint storico"):
        resumed = execute(config, directory, resume=True)
    assert any("retroattivo" in message for message in resumed["environment_history"][0]["warnings"])
    for attempt in resumed["attempts"][1:]:
        manifest = runner._read(Path(attempt["run_dir"]) / "manifest.json")
        assert manifest["environment"]["warnings"]


def test_environment_difference_is_reported_also_when_switching_database(experiment, monkeypatch):
    config, adapter, directory = experiment
    execute(config, directory)
    mongo = StatefulAdapter(adapter.reference)
    mongo.name = "mongo"
    mongo.version = lambda: "mongo-version-is-expected-to-differ"
    monkeypatch.setattr(runner, "create_adapter", lambda config: mongo)
    original_context = runner.environment_context

    def changed_context(version):
        context = original_context(version)
        context["packages"]["psycopg"] = "new-transitive-or-direct-version"
        return context

    monkeypatch.setattr(runner, "environment_context", changed_context)
    with pytest.warns(RuntimeWarning, match="Ambiente variato.*campaign"):
        state = execute(replace(config, database="mongo"), directory)
    observation = state["environment_history"][0]
    assert observation["changes"]["campaign"] == ["packages.psycopg"]
    assert observation["changes"]["experiment"] == []
    assert not any("database_version" in message for message in observation["warnings"])
