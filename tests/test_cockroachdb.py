"""Tipi e retry CockroachDB: fixture piccole, senza simulare benchmark reali."""

import json
from unittest.mock import MagicMock, Mock

import psycopg
import pytest

from src.databases.base import BatchWriteError
from src.databases.cockroachdb import CockroachDBAdapter, column_definitions
from src.query_validation import capture_database_snapshot, compare_reference_snapshots, read_mongo_snapshot
from test_queries import MemoryAdapter, query_fixture


def mocked_adapter(errors):
    """Fornisce un client finto solo per la macchina a stati delle transazioni."""
    adapter = CockroachDBAdapter(max_retries=3)
    adapter.connection = MagicMock()
    adapter.connection.closed = False
    cursor = adapter.connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = errors
    return adapter, cursor


def test_crdb_ddl_uses_explicit_integer_widths():
    ddl = column_definitions().as_string()
    assert '"flight_id" INT8 NOT NULL PRIMARY KEY' in ddl
    assert '"distance" INT4 NOT NULL' in ddl
    assert '"departure_delay" INT4' in ddl
    assert '"scheduled_departure" STRING NOT NULL' in ddl
    assert "SERIAL" not in ddl and "IDENTITY" not in ddl


def test_serialization_failure_retries_whole_batch_and_preserves_ids(monkeypatch):
    adapter, cursor = mocked_adapter([psycopg.errors.SerializationFailure("injected 40001"), None])
    sleep = Mock()
    monkeypatch.setattr("src.databases.cockroachdb.time.sleep", sleep)
    rows = [(1,), (2,)]
    adapter.ingest_batch(rows)
    assert cursor.executemany.call_count == 2
    assert all(call.args[1] is rows for call in cursor.executemany.call_args_list)
    assert adapter.retries_total == 1 and adapter.batches_retried == 1
    sleep.assert_called_once_with(0.05)
    assert adapter.connection.transaction.call_count == 2


def test_retry_exhaustion_is_bounded(monkeypatch):
    adapter, cursor = mocked_adapter(psycopg.errors.SerializationFailure("injected 40001"))
    monkeypatch.setattr("src.databases.cockroachdb.time.sleep", Mock())
    with pytest.raises(RuntimeError, match="Limite retry"):
        adapter.ingest_batch([(1,)])
    assert cursor.executemany.call_count == 4
    assert adapter.retries_total == 3


@pytest.mark.parametrize("error", [psycopg.errors.UniqueViolation("duplicate"),
                                   psycopg.errors.QueryCanceled("statement timeout")])
def test_known_abort_and_timeout_are_not_retried(error):
    adapter, cursor = mocked_adapter(error)
    with pytest.raises(type(error)):
        adapter.ingest_batch([(1,)])
    assert cursor.executemany.call_count == 1
    assert adapter.retries_total == 0


@pytest.mark.parametrize("error", [psycopg.errors.StatementCompletionUnknown("ambiguous commit"),
                                   psycopg.OperationalError("connection lost")])
def test_uncertain_commit_is_not_retried(error):
    adapter, cursor = mocked_adapter(error)
    with pytest.raises(BatchWriteError) as caught:
        adapter.ingest_batch([(1,)])
    assert caught.value.acknowledged_rows is None
    assert cursor.executemany.call_count == 1


def test_destructive_operations_require_explicit_true():
    adapter = CockroachDBAdapter()
    with pytest.raises(PermissionError):
        adapter.reset_dataset()
    with pytest.raises(PermissionError):
        adapter.drop_secondary_indexes()
    assert adapter.connection is None


def test_both_snapshot_sources_must_match_and_be_correct(query_fixture, tmp_path):
    _, _, reference = query_fixture
    paths = {}
    for name in ("postgres", "mongo"):
        adapter = MemoryAdapter(reference)
        adapter.name = name
        adapter.version = lambda: "synthetic"
        snapshot = capture_database_snapshot(adapter, reference, "primary_only")
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(snapshot))
        paths[name] = path
    current = MemoryAdapter(reference)
    current.name = "cockroach"
    current.version = lambda: "synthetic"
    assert compare_reference_snapshots(current, reference, paths["postgres"], paths["mongo"], "primary_only")["status"] == "passed"
    mongo = json.loads(paths["mongo"].read_text())
    mongo["validation"]["results"]["Q3"] += 1
    paths["mongo"].write_text(json.dumps(mongo))
    with pytest.raises(ValueError, match="Q3"):
        compare_reference_snapshots(current, reference, paths["postgres"], paths["mongo"], "primary_only")
    mongo["dataset_sha256"] = "different"
    paths["mongo"].write_text(json.dumps(mongo))
    with pytest.raises(ValueError, match="non omogeneo"):
        read_mongo_snapshot(reference, paths["mongo"])
