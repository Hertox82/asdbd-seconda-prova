"""Statistiche e persistenza dei run: gli errori non diventano risultati validi."""

import json

from src.results import ResultRun, result_digest, summarize


def test_summary_excludes_warmup_errors_and_does_not_drop_outliers():
    base = {"event": "query", "query_id": "Q2", "index_state": "primary_only", "status": "ok", "warmup": False}
    records = [{**base, "duration_ms": value} for value in (1.0, 2.0, 100.0)]
    records += [{**base, "duration_ms": 9999, "warmup": True},
                {**base, "duration_ms": 9999, "status": "error"}]
    row = summarize(records)[0]
    assert row["valid_repetitions"] == 3
    assert row["mean_ms"] == 103 / 3
    assert row["median_ms"] == 2
    assert row["min_ms"] == 1 and row["max_ms"] == 100
    assert row["sample_stddev_ms"] > 50


def test_run_directories_are_unique_and_failure_is_explicit(tmp_path):
    config = {"mode": "FULL", "database": "postgres", "batch_size": 1000}
    reference = {"dataset_sha256": "synthetic", "dataset_size": 7, "q1_ids": [1, 7]}
    with ResultRun(tmp_path, config, reference, stage="synthetic") as first:
        first.record(duration_ms=1.0, query_id="Q2", result_digest=result_digest(6))
    with ResultRun(tmp_path, config, reference, stage="synthetic") as second:
        second.record(duration_ms=2.0, query_id="Q2", result_digest=result_digest(6))
    assert first.directory != second.directory
    assert json.loads((first.directory / "manifest.json").read_text())["status"] == "completed"
    assert (first.directory / "summary.csv").exists()
    assert not list(first.directory.glob(".raw.jsonl.*"))


def test_digest_stable_for_dictionary_order():
    assert result_digest({"b": 2, "a": None}) == result_digest({"a": None, "b": 2})


def test_environment_records_installed_transitive_versions_and_public_source_hashes(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import src.results as results
    root = tmp_path / "repository"
    (root / "src").mkdir(parents=True)
    (root / "docker/jupyter").mkdir(parents=True)
    for name in ("docker-compose.yml", "docker/jupyter/Dockerfile", "requirements.txt"):
        (root / name).write_text("public configuration v1")
    (root / ".env").write_text("SECRET=must-not-be-read")
    monkeypatch.setattr(results, "__file__", str(root / "src/results.py"))
    monkeypatch.setattr(results, "distributions", lambda: [
        SimpleNamespace(metadata={"Name": "Direct_Package"}, version="1.0"),
        SimpleNamespace(metadata={"Name": "Transitive-Package"}, version="2.0")])
    monkeypatch.setattr(results, "_resources", lambda: {
        "hostname": "volatile", "process_rss_bytes": 123, "cgroup_memory_limit": "1024"})
    before = results.environment_context("database-v1")
    (root / "docker-compose.yml").write_text("public configuration v2")
    after = results.environment_context("database-v2")
    assert before["packages"] == {"direct-package": "1.0", "transitive-package": "2.0"}
    assert before["docker_sources"]["docker-compose.yml"] != after["docker_sources"]["docker-compose.yml"]
    assert after["database_version"] == "database-v2"
    assert after["runtime"] == {"cgroup_memory_limit": "1024"}
    assert "SECRET" not in json.dumps(after)
    assert "configurazione effettiva" in after["coverage_warnings"][0]
