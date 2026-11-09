"""Sicurezza dei reset e gestione delle scritture parziali, senza DB reale."""

from unittest.mock import Mock

import pytest

from src.databases.postgresql import PostgreSQLAdapter
from src.ingestion import IngestionError, ingest_csv
from test_queries import query_fixture


class BatchAdapter:
    name = "postgres"
    ingestion_method = "psycopg.executemany"
    commit_policy = "one_transaction_per_batch"

    def __init__(self, *, populated=False, fail_batch=None):
        self.rows = []
        self.populated = populated
        self.fail_batch = fail_batch
        self.calls = 0

    def validate_schema(self):
        pass

    def assert_index_state(self, state):
        assert state == "primary_only"

    def count_rows(self):
        return 1 if self.populated else len(self.rows)

    def ingest_batch(self, rows):
        self.calls += 1
        if self.calls == self.fail_batch:
            raise RuntimeError("batch sintetico fallito")
        self.rows.extend(rows)


def test_ingestion_preserves_ids_and_counts_batches(query_fixture):
    path, artifact, _ = query_fixture
    adapter = BatchAdapter()
    result = ingest_csv(adapter, path, 7, artifact["sha256"], batch_size=3)
    assert [row[0] for row in adapter.rows] == list(range(1, 8))
    assert result["rows_committed"] == 7
    assert result["batches_committed"] == 3
    assert result["duration_ms"] >= result["driver_write_ms"] >= 0
    assert result["rows_per_second"] > 0


def test_populated_database_is_not_reset_or_appended(query_fixture):
    path, artifact, _ = query_fixture
    adapter = BatchAdapter(populated=True)
    with pytest.raises(ValueError, match="già popolata"):
        ingest_csv(adapter, path, 7, artifact["sha256"])
    assert adapter.calls == 0


def test_failure_reports_only_acknowledged_batches(query_fixture):
    path, artifact, _ = query_fixture
    adapter = BatchAdapter(fail_batch=2)
    with pytest.raises(IngestionError) as error:
        ingest_csv(adapter, path, 7, artifact["sha256"], batch_size=2)
    assert error.value.progress["rows_committed"] == 2
    assert error.value.progress["batches_committed"] == 1
    assert len(adapter.rows) == 2


def test_invalid_checksum_does_not_write(query_fixture):
    path, _, _ = query_fixture
    adapter = BatchAdapter()
    with pytest.raises(ValueError, match="Checksum"):
        ingest_csv(adapter, path, 7, "not-a-checksum")
    assert adapter.calls == 0


@pytest.mark.parametrize("confirmation", [False, None, "True", 1])
def test_reset_and_index_drop_require_literal_true(confirmation):
    adapter = PostgreSQLAdapter()
    adapter.connection = Mock()
    with pytest.raises(PermissionError):
        adapter.reset_dataset(confirm_reset=confirmation)
    with pytest.raises(PermissionError):
        adapter.drop_secondary_indexes(confirm_drop=confirmation)
    adapter.connection.execute.assert_not_called()
