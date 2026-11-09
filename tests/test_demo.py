"""DEMO con fixture sintetiche: storici immutabili e nessuna API di scrittura."""

from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

import src.demo as demo
from src.benchmark import benchmark_phase
from src.config import load_config
from src.history import read_experiment, read_catalog
from src.presentation import bar_chart, demo_rows, html_table
from src.query_validation import capture_database_snapshot
from src.results import ResultRun
from src.runner import PHASES, _protocol
from src.validation import sha256_file
from test_queries import MemoryAdapter, query_fixture


class SafeFixtureAdapter(MemoryAdapter):
    """Un adapter che fallisce immediatamente se la DEMO tenta operazioni vietate."""
    def __init__(self, reference):
        super().__init__(reference)
        self.state = "secondary"
        self.db = Mock()
        self.db.execute.return_value.fetchone.return_value = ("on",)
        self.connect = Mock()
        self.close = Mock()
        self.version = Mock(return_value="synthetic")
        self.count_rows = Mock(return_value=reference["dataset_size"])
        for name in ("create_schema", "reset_dataset", "ingest_batch", "create_secondary_index",
                     "drop_secondary_indexes", "analyze", "explain", "integrity_summary"):
            setattr(self, name, Mock(side_effect=AssertionError("Operazione vietata: " + name)))


@pytest.fixture
def historical_case(query_fixture, tmp_path, monkeypatch):
    """Quattro piccoli run solo in tmp_path, mai nelle campagne sperimentali."""
    _, _, reference = query_fixture
    config = replace(load_config(Path("config/benchmark.yaml")), database="postgres", dataset_size=7,
                     results_dir=tmp_path / "results", table="synthetic_demo_fixture",
                     warmup=1, repetitions=3, q1_ids=reference["q1_ids"])
    campaign = config.results_dir / "synthetic-history"
    (campaign / "experiments").mkdir(parents=True)
    protocol = _protocol(config)
    (campaign / "protocol.json").write_text(json.dumps(protocol))
    memory = MemoryAdapter(reference)
    memory.version = lambda: "synthetic"
    attempts = []
    sources = [campaign / "protocol.json"]
    for phase in PHASES:
        memory.state = "secondary" if phase in ("indexes", "indexed") else "primary_only"
        with ResultRun(config.results_dir, config.manifest(), reference, stage=phase) as run:
            run.set_database_version("synthetic")
            run.save_json("protocol.json", protocol)
            if phase in ("baseline", "indexed"):
                benchmark_phase(memory, reference, run, index_state=memory.state,
                                query_ids=("Q1", "Q2", "Q3", "Q4") if phase == "baseline" else ("Q2", "Q3"),
                                warmup=1, repetitions=3)
            elif phase == "ingestion":
                run.record(event="ingestion", duration_ms=1, index_state="primary_only")
            else:
                for column in ("origin_airport", "distance"):
                    run.record(event="create_index", query_id=column, duration_ms=1, index_state="secondary")
            run.save_json("database_snapshot.json", capture_database_snapshot(memory, reference, memory.state))
        attempts.append({"phase": phase, "run_dir": str(run.directory), "status": "completed"})
        sources.extend(run.directory.iterdir())
    state_file = campaign / "experiments/postgres_7.json"
    state_file.write_text(json.dumps({"database": "postgres", "dataset_size": 7, "status": "completed",
                                     "completed_phases": list(PHASES), "attempts": attempts}))
    sources.append(state_file)
    adapter = SafeFixtureAdapter(reference)
    monkeypatch.setattr(demo, "SIZES", (7,))
    monkeypatch.setattr(demo, "create_adapter", lambda config: adapter)
    monkeypatch.setattr(demo, "assert_single_database", Mock())
    settings = demo.DemoSettings(replace(config, mode="DEMO", warmup=0), {7: campaign})
    return settings, adapter, {p: sha256_file(p) for p in sources}


def test_demo_has_only_twelve_reads_and_keeps_every_source_unchanged(historical_case):
    settings, adapter, before = historical_case
    # Nessun CSV reale esiste nella fixture: la DEMO usa solo riferimenti storici.
    directory = demo.run_demo(settings)
    assert directory.parent == settings.experiment.results_dir / "demo"
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["configuration"]["mode"] == "DEMO" and manifest["status"] == "completed"
    assert manifest["configuration"]["table"] == "synthetic_demo_fixture"
    rows = [json.loads(line) for line in (directory / "raw.jsonl").read_text().splitlines()]
    assert len(rows) == 12 and all(r["mode"] == "DEMO" and not r["warmup"] for r in rows)
    assert len(adapter.calls) == 15  # tre campioni di controllo + dodici misure
    assert len(json.loads((directory / "query_results.json").read_text())["Q1"]) == 3
    assert all(sha256_file(path) == digest for path, digest in before.items())
    for name in ("create_schema", "reset_dataset", "ingest_batch", "create_secondary_index",
                 "drop_secondary_indexes", "analyze", "explain", "integrity_summary"):
        getattr(adapter, name).assert_not_called()
    adapter.close.assert_called_once()
    assert all(r["mode"] == "DEMO" for r in demo_rows(directory))


@pytest.mark.parametrize("field,value", [("mode", "FULL"), ("confirm_reset", True),
                                         ("confirm_drop_indexes", True), ("confirm_reset", "False")])
def test_destructive_or_full_configuration_is_refused_before_connect(historical_case, field, value):
    settings, adapter, _ = historical_case
    with pytest.raises(PermissionError):
        demo.run_demo(replace(settings, experiment=replace(settings.experiment, **{field: value})))
    adapter.connect.assert_not_called()


@pytest.mark.parametrize("name", ("create_schema", "reset_dataset", "ingest_batch", "create_secondary_index",
                                 "drop_secondary_indexes", "analyze", "explain", "integrity_summary", "db"))
def test_facade_has_no_mutation_or_heavy_audit_api(name):
    backend = Mock(name="backend")
    backend.name = "mongo"
    with pytest.raises(PermissionError):
        getattr(demo.ReadOnlyAdapter(backend), name)


def test_empty_database_is_not_prepared_or_reingested(historical_case):
    settings, adapter, _ = historical_case
    adapter.count_rows.return_value = 0
    with pytest.raises(ValueError, match="Nessun caricamento automatico"):
        demo.run_demo(settings)
    adapter.create_schema.assert_not_called()
    adapter.ingest_batch.assert_not_called()
    directory = next((settings.experiment.results_dir / "demo").iterdir())
    assert json.loads((directory / "manifest.json").read_text())["status"] == "failed"
    assert not (directory / "summary.csv").exists()


def test_missing_secondary_indexes_are_not_rebuilt(historical_case):
    settings, adapter, _ = historical_case
    adapter.state = "primary_only"
    with pytest.raises(ValueError, match="Stato indici"):
        demo.run_demo(settings)
    adapter.create_secondary_index.assert_not_called()


def test_wrong_query_stops_demo_without_valid_summary(historical_case):
    settings, adapter, before = historical_case
    original = adapter.run_query
    adapter.run_query = lambda query, parameters: -1 if query == "Q3" else original(query, parameters)
    with pytest.raises(ValueError, match="Q3"):
        demo.run_demo(settings)
    directory = next((settings.experiment.results_dir / "demo").iterdir())
    assert not (directory / "summary.csv").exists()
    with pytest.raises(ValueError, match="DEMO completato"):
        demo_rows(directory)
    assert all(sha256_file(path) == digest for path, digest in before.items())


@pytest.mark.parametrize("damage", ("mode", "incomplete", "size", "summary"))
def test_history_refuses_incompatible_or_incomplete_sources(historical_case, damage):
    settings, _, _ = historical_case
    state_path = settings.campaign / "experiments/postgres_7.json"
    state = json.loads(state_path.read_text())
    folder = Path(state["attempts"][0]["run_dir"])
    if damage in ("mode", "size"):
        file = folder / "manifest.json"
        value = json.loads(file.read_text())
        value["configuration"]["mode" if damage == "mode" else "dataset_size"] = "DEMO" if damage == "mode" else 8
        file.write_text(json.dumps(value))
    elif damage == "summary":
        (folder / "summary.csv").write_text("event,query_id,index_state\n")
    else:
        state["status"] = "failed"
        state_path.write_text(json.dumps(state))
    with pytest.raises(ValueError):
        read_experiment(settings.campaign, "postgres", 7, settings.experiment.results_dir)


@pytest.mark.parametrize("n", (0, -1, True, 11))
def test_demo_repetitions_are_bounded_without_enabling_full(tmp_path, n):
    folder = tmp_path / "config"
    folder.mkdir()
    base = yaml.safe_load(Path("config/benchmark.yaml").read_text(encoding="utf-8"))
    base["benchmark"]["demo_repetitions"] = n
    (folder / "benchmark.yaml").write_text(yaml.safe_dump(base))
    (folder / "demo.yaml").write_text(Path("config/demo.yaml").read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="demo_repetitions"):
        demo.load_demo_settings(folder / "demo.yaml")


def test_svg_and_tables_preserve_mode_dimension_units_and_escape_text():
    row = {"label": "<script>", "value": 2.5, "database": "postgres", "mode": "FULL", "dataset_size": 100000}
    svg = bar_chart([row], "<titolo>", "ms")
    from xml.etree import ElementTree
    ElementTree.fromstring(svg)
    assert "&lt;script&gt;" in svg and "&lt;titolo&gt;" in svg and "2.500" in svg
    assert "ms" in svg and "FULL" in svg and "100,000" in svg
    assert "&lt;script&gt;" in html_table([{"x": "<script>"}], ("x",))
    for other in ({**row, "mode": "DEMO"}, {**row, "dataset_size": 1000000}):
        with pytest.raises(ValueError, match="Separare"):
            bar_chart([row, other], "non mescolare")
    with pytest.raises(ValueError, match="finite"):
        bar_chart([{**row, "value": float("nan")}], "invalid")


def test_shared_timer_stops_before_validation_and_persistence(query_fixture, monkeypatch):
    """Il refactoring non deve includere confronto, digest o I/O nel timer."""
    import src.benchmark as benchmark
    _, _, reference = query_fixture
    events = []
    ticks = iter((10, 2_000_010))

    def clock():
        events.append("timer")
        return next(ticks)

    backend = MemoryAdapter(reference)
    original_query = backend.run_query
    original_assert = benchmark.assert_equivalent

    def query(query_id, parameters):
        events.append("query")
        return original_query(query_id, parameters)

    def validate(*args):
        events.append("validation")
        return original_assert(*args)

    run = Mock()
    run.record.side_effect = lambda **kwargs: events.append("record")
    run.flush.side_effect = lambda: events.append("flush")
    backend.run_query = query
    monkeypatch.setattr(benchmark.time, "perf_counter_ns", clock)
    monkeypatch.setattr(benchmark, "assert_equivalent", validate)
    values = benchmark.measure_queries(backend, reference, run, index_state="secondary",
                                       query_ids=("Q2",), warmup=0, repetitions=1, capture_results=True)
    assert events == ["timer", "query", "timer", "validation", "record", "flush"]
    assert run.record.call_args.kwargs["duration_ms"] == 2
    assert values["Q2"][0]["result"] == reference["Q2"]


def test_other_active_database_stops_before_connection(historical_case):
    settings, adapter, _ = historical_case
    demo.assert_single_database.side_effect = RuntimeError("Altro database attivo")
    with pytest.raises(RuntimeError, match="Altro database"):
        demo.run_demo(settings)
    adapter.connect.assert_not_called()
