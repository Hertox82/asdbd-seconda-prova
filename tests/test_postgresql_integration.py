"""Prove reali isolate in uno schema temporaneo; mai reset di public.flights."""

import os
from uuid import uuid4

import psycopg
from psycopg import sql
import pytest

from src.benchmark import benchmark_phase, measure_secondary_indexes
from src.databases.postgresql import PostgreSQLAdapter
from src.ingestion import ingest_csv
from src.query_validation import verify_database
from src.results import ResultRun
from test_queries import query_fixture


pytestmark = pytest.mark.skipif(os.getenv("LAB_TEST_POSTGRES") != "1",
                                reason="Richiede PostgreSQL reale e LAB_TEST_POSTGRES=1.")


@pytest.fixture
def postgres_adapter():
    namespace = "task3_test_" + uuid4().hex
    adapter = PostgreSQLAdapter(schema=namespace)
    adapter.connect()
    adapter.db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
    try:
        adapter.create_schema()
        yield adapter
    finally:
        # Solo namespace appena creato dalla fixture, con identificatore quotato.
        adapter.db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))
        adapter.close()


def test_real_roundtrip_schema_nulls_queries_and_indexes(postgres_adapter, query_fixture, tmp_path):
    adapter = postgres_adapter
    path, artifact, reference = query_fixture
    result = ingest_csv(adapter, path, 7, artifact["sha256"], batch_size=3)
    assert result["batches_committed"] == 3
    assert verify_database(adapter, reference, "primary_only")["status"] == "passed"
    types = adapter.db.execute(sql.SQL(
        "SELECT pg_typeof(flight_id)::text, pg_typeof(distance)::text FROM {} LIMIT 1"
    ).format(adapter.table_identifier)).fetchone()
    assert types == ("bigint", "integer")
    with ResultRun(tmp_path / "results", {"mode": "FULL", "database": "postgres", "batch_size": 3},
                   reference, stage="synthetic-integration") as run:
        run.set_database_version(adapter.version())
        adapter.analyze()
        benchmark_phase(adapter, reference, run, index_state="primary_only",
                        query_ids=("Q1", "Q2", "Q3", "Q4"), warmup=1, repetitions=3)
        measure_secondary_indexes(adapter, run)
        benchmark_phase(adapter, reference, run, index_state="secondary",
                        query_ids=("Q2", "Q3"), warmup=1, repetitions=3)
    assert len(adapter.indexes()) == 3
    with pytest.raises(ValueError, match="Indici"):
        adapter.assert_index_state("primary_only")
    with pytest.raises(PermissionError):
        adapter.drop_secondary_indexes()
    adapter.drop_secondary_indexes(confirm_drop=True)
    adapter.assert_index_state("primary_only")
    with pytest.raises(PermissionError):
        adapter.reset_dataset()
    assert adapter.count_rows() == 7


def test_failed_batch_rolls_back_and_explicit_reset_is_scoped(postgres_adapter, query_fixture):
    adapter = postgres_adapter
    _, _, reference = query_fixture
    row = tuple(reference["q1_records"]["1"].values())
    with pytest.raises(psycopg.errors.UniqueViolation):
        adapter.ingest_batch([row, row])
    assert adapter.count_rows() == 0
    adapter.ingest_batch([row])
    assert adapter.count_rows() == 1
    adapter.reset_dataset(confirm_reset=True)
    assert adapter.count_rows() == 0
    adapter.assert_index_state("primary_only")


def test_incompatible_existing_schema_is_not_altered(postgres_adapter):
    adapter = postgres_adapter
    adapter.db.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN departure_delay SET NOT NULL").format(
        adapter.table_identifier))
    with pytest.raises(ValueError, match="Schema PostgreSQL"):
        adapter.create_schema()


def test_q4_keeps_groups_with_only_null_delays(postgres_adapter, query_fixture):
    adapter = postgres_adapter
    _, _, reference = query_fixture
    first = list(reference["q1_records"]["1"].values())
    second = list(reference["q1_records"]["2"].values())
    second[5] = "BB"
    adapter.ingest_batch([tuple(first), tuple(second)])
    assert adapter.run_query("Q4", reference["parameters"]) == {"AA": None, "BB": None}
