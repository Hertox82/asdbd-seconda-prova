"""CockroachDB reale, esclusivamente in schemi sintetici con UUID separati."""

import os
from uuid import uuid4

import psycopg
from psycopg import sql
import pytest

from src.benchmark import benchmark_phase, measure_secondary_indexes
from src.databases.cockroachdb import CockroachDBAdapter
from src.ingestion import ingest_csv
from src.query_validation import verify_database
from src.results import ResultRun
from test_queries import query_fixture


pytestmark = pytest.mark.skipif(os.getenv("LAB_TEST_COCKROACH") != "1",
                                reason="Richiede solo CockroachDB attivo e LAB_TEST_COCKROACH=1.")


@pytest.fixture
def cockroach_adapter():
    namespace = "task5_test_" + uuid4().hex
    adapter = CockroachDBAdapter(schema=namespace)
    adapter.connect()
    adapter.db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
    try:
        adapter.create_schema()
        yield adapter
    finally:
        # Solo lo schema appena creato dalla fixture: mai public.flights.
        adapter.db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))
        adapter.close()


def test_real_types_queries_indexes_and_plans(cockroach_adapter, query_fixture, tmp_path):
    adapter = cockroach_adapter
    path, artifact, reference = query_fixture
    result = ingest_csv(adapter, path, 7, artifact["sha256"], batch_size=3)
    assert result["batches_committed"] == 3
    assert result["transaction_retries"] >= 0
    assert verify_database(adapter, reference, "primary_only")["status"] == "passed"
    assert adapter.db.execute("SHOW transaction_isolation").fetchone()[0].lower() == "serializable"
    with ResultRun(tmp_path / "results", {"mode": "FULL", "database": "cockroach", "batch_size": 3},
                   reference, stage="synthetic-integration") as run:
        run.set_database_version(adapter.version())
        adapter.analyze()
        benchmark_phase(adapter, reference, run, index_state="primary_only",
                        query_ids=("Q1", "Q2", "Q3", "Q4"), warmup=1, repetitions=3)
        measure_secondary_indexes(adapter, run)
        benchmark_phase(adapter, reference, run, index_state="secondary",
                        query_ids=("Q2", "Q3"), warmup=1, repetitions=3)
    assert len(adapter.indexes()) == 3
    assert all(index["columns"] == ["flight_id"] for index in adapter.indexes() if index["primary"])
    adapter.drop_secondary_indexes(confirm_drop=True)
    adapter.assert_index_state("primary_only")


def test_batch_is_atomic_and_reset_is_explicit(cockroach_adapter, query_fixture):
    adapter = cockroach_adapter
    _, _, reference = query_fixture
    row = tuple(reference["q1_records"]["1"].values())
    with pytest.raises(psycopg.errors.UniqueViolation):
        adapter.ingest_batch([row, row])
    assert adapter.count_rows() == 0
    adapter.ingest_batch([row])
    with pytest.raises(PermissionError):
        adapter.reset_dataset()
    assert adapter.count_rows() == 1
    adapter.reset_dataset(confirm_reset=True)
    assert adapter.count_rows() == 0


def test_q4_preserves_all_null_groups(cockroach_adapter, query_fixture):
    adapter = cockroach_adapter
    _, _, reference = query_fixture
    first = list(reference["q1_records"]["1"].values())
    second = list(reference["q1_records"]["2"].values())
    second[5] = "BB"
    adapter.ingest_batch([tuple(first), tuple(second)])
    assert adapter.run_query("Q4", {}) == {"AA": None, "BB": None}


def test_schema_mismatch_is_not_migrated(cockroach_adapter):
    adapter = cockroach_adapter
    adapter.db.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN departure_delay SET NOT NULL").format(adapter.table_identifier))
    with pytest.raises(ValueError, match="Schema CockroachDB"):
        adapter.create_schema()


def test_real_injected_serialization_failure_is_bounded(cockroach_adapter, query_fixture):
    """Iniezione limitata alla connessione/schema sintetici, mai al run reale."""
    adapter = cockroach_adapter
    _, _, reference = query_fixture
    row = tuple(reference["q1_records"]["1"].values())
    adapter.max_retries = 1
    adapter.db.execute("SET inject_retry_errors_enabled = true")
    try:
        with pytest.raises(RuntimeError, match="Limite retry"):
            adapter.ingest_batch([row])
    finally:
        adapter.db.execute("SET inject_retry_errors_enabled = false")
    assert adapter.retries_total == 1
    assert adapter.count_rows() == 0
    adapter.ingest_batch([row])
    assert adapter.count_rows() == 1


def test_secondary_storing_is_rejected_without_changes(cockroach_adapter):
    """Una covering column non deve alterare silenziosamente il confronto."""
    adapter = cockroach_adapter
    adapter.create_secondary_index("origin_airport")
    adapter.db.execute(sql.SQL("CREATE INDEX {} ON {} (distance) STORING (airline)").format(
        sql.Identifier(f"{adapter.table}_distance_idx"), adapter.table_identifier))
    before = adapter.indexes()
    with pytest.raises(ValueError, match="STORING"):
        adapter.assert_index_state("secondary")
    assert adapter.indexes() == before
