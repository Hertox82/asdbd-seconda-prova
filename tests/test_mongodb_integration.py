"""Mongo reale in database sintetici isolati; nessun reset dei voli dell'utente."""

import os
from uuid import uuid4

import pytest
from bson import Int64
from pymongo.errors import WriteError

from src.benchmark import benchmark_phase, measure_secondary_indexes
from src.databases.base import BatchWriteError
from src.databases.mongodb import MongoDBAdapter, to_document
from src.ingestion import ingest_csv
from src.query_validation import verify_database
from src.results import ResultRun
from test_mongodb import canonical_row
from test_queries import query_fixture


pytestmark = pytest.mark.skipif(os.getenv("LAB_TEST_MONGO") != "1",
                                reason="Richiede Mongo reale e LAB_TEST_MONGO=1; PostgreSQL deve essere fermo.")


@pytest.fixture
def mongo_adapter():
    database = "task4_test_" + uuid4().hex
    adapter = MongoDBAdapter(database=database)
    adapter.connect()
    try:
        adapter.create_schema()
        yield adapter
    finally:
        # Solo il database sintetico appena creato dalla fixture con UUID.
        adapter.client.drop_database(database)
        adapter.close()


def test_real_bson_roundtrip_queries_and_indexes(mongo_adapter, query_fixture, tmp_path):
    path, artifact, reference = query_fixture
    adapter = mongo_adapter
    result = ingest_csv(adapter, path, 7, artifact["sha256"], batch_size=3)
    assert result["batches_committed"] == 3
    assert verify_database(adapter, reference, "primary_only")["status"] == "passed"
    assert adapter.last_bson_audit["rows_verified"] == 7
    assert adapter.last_bson_audit["invalid_documents"] == 0
    document = adapter.collection.find_one({"_id": Int64(1)})
    assert type(document["_id"]) is Int64
    assert type(document["distance"]) is int
    assert document["departure_delay"] is None
    with ResultRun(tmp_path / "results", {"mode": "FULL", "database": "mongo", "batch_size": 3},
                   reference, stage="synthetic-integration") as run:
        run.set_database_version(adapter.version())
        benchmark_phase(adapter, reference, run, index_state="primary_only",
                        query_ids=("Q1", "Q2", "Q3", "Q4"), warmup=1, repetitions=3)
        measure_secondary_indexes(adapter, run)
        benchmark_phase(adapter, reference, run, index_state="secondary",
                        query_ids=("Q2", "Q3"), warmup=1, repetitions=3)
    assert len(adapter.indexes()) == 3
    with pytest.raises(PermissionError):
        adapter.drop_secondary_indexes()
    adapter.drop_secondary_indexes(confirm_drop=True)
    adapter.assert_index_state("primary_only")


def test_ordered_failure_is_partial_not_atomic(mongo_adapter, query_fixture):
    _, _, reference = query_fixture
    first, second = canonical_row(reference, 1), canonical_row(reference, 2)
    with pytest.raises(BatchWriteError) as error:
        mongo_adapter.ingest_batch([first, first, second])
    assert error.value.acknowledged_rows == 1
    assert mongo_adapter.count_rows() == 1
    assert mongo_adapter.run_query("Q1", {"flight_id": 2}) is None
    with pytest.raises(PermissionError):
        mongo_adapter.reset_dataset()
    assert mongo_adapter.count_rows() == 1
    mongo_adapter.reset_dataset(confirm_reset=True)
    assert mongo_adapter.count_rows() == 0
    mongo_adapter.validate_schema()


@pytest.mark.parametrize("bad", ["id_int32", "regular_int64", "missing", "extra", "null_required"])
def test_server_rejects_wrong_bson_types_and_missing_fields(mongo_adapter, query_fixture, bad):
    _, _, reference = query_fixture
    doc = to_document(canonical_row(reference))
    if bad == "id_int32":
        doc["_id"] = 1
    elif bad == "regular_int64":
        doc["distance"] = Int64(1800)
    elif bad == "missing":
        del doc["departure_delay"]
    elif bad == "extra":
        doc["flight_id"] = 1
    else:
        doc["airline"] = None
    with pytest.raises(WriteError) as error:
        mongo_adapter.collection.insert_one(doc)
    assert error.value.code == 121
    assert mongo_adapter.count_rows() == 0


def test_audit_detects_validation_bypass(mongo_adapter, query_fixture):
    _, _, reference = query_fixture
    doc = to_document(canonical_row(reference))
    doc["distance"] = Int64(1800)
    mongo_adapter.collection.insert_one(doc, bypass_document_validation=True)
    with pytest.raises(ValueError, match="Audit BSON"):
        mongo_adapter.integrity_summary()


def test_all_null_groups_and_empty_counts(mongo_adapter, query_fixture):
    _, _, reference = query_fixture
    first = canonical_row(reference, 1)
    second = list(canonical_row(reference, 2))
    second[5] = "BB"
    mongo_adapter.ingest_batch([first, tuple(second)])
    assert mongo_adapter.run_query("Q4", {}) == {"AA": None, "BB": None}
    assert mongo_adapter.run_query("Q2", {"origin_airport": "absent"}) == 0
    assert mongo_adapter.run_query("Q3", {"distance_min": 99_999, "distance_max": 100_000}) == 0


def test_large_id_retains_int64_precision(mongo_adapter, query_fixture):
    _, _, reference = query_fixture
    row = list(canonical_row(reference))
    row[0] = 2**53 + 1
    mongo_adapter.ingest_batch([tuple(row)])
    assert mongo_adapter.run_query("Q1", {"flight_id": row[0]})["flight_id"] == row[0]
    assert type(mongo_adapter.collection.find_one()["_id"]) is Int64


def test_existing_collection_is_never_silently_modified(mongo_adapter):
    other = MongoDBAdapter(database=mongo_adapter.database_name, collection="foreign_data")
    other.client = mongo_adapter.client
    other.db.create_collection("foreign_data")
    other.collection.insert_one({"note": "keep"})
    with pytest.raises(ValueError, match="contratto"):
        other.create_schema()
    assert other.collection.find_one()["note"] == "keep"
