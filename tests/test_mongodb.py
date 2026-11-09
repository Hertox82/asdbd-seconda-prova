"""Conversioni BSON e gestione dei batch Mongo con sole fixture sintetiche."""

import json
from unittest.mock import MagicMock, Mock

import pytest
from bson import BSON, Int64
from pymongo.errors import BulkWriteError, ConnectionFailure

from src.databases.base import BatchWriteError
from src.databases.mongodb import MongoDBAdapter, canonical_document, collection_validator, to_document
from src.ingestion import IngestionError, ingest_csv
from src.query_validation import capture_database_snapshot, compare_postgres_snapshot
from src.schema import schema_manifest
from test_queries import MemoryAdapter, query_fixture


def canonical_row(reference, flight_id=1):
    return tuple(reference["q1_records"][str(flight_id)].values())


def test_bson_roundtrip_forces_int64_and_int32(query_fixture):
    _, _, reference = query_fixture
    row = list(canonical_row(reference))
    row[0] = 2**53 + 1
    document = to_document(tuple(row))
    decoded = BSON.encode(document).decode()
    assert type(decoded["_id"]) is Int64
    assert decoded["_id"] == 2**53 + 1
    assert "flight_id" not in decoded
    assert len(decoded) == 32
    for field in schema_manifest()[1:]:
        value = decoded[field["name"].lower()]
        if value is not None and field["mongodb"] == "Int32":
            assert type(value) is int  # Int64 decodificato avrebbe wrapper Int64.
    assert decoded["departure_delay"] is None
    assert decoded["scheduled_departure"] == "0005"
    assert canonical_document(decoded)["flight_id"] == 2**53 + 1


@pytest.mark.parametrize("position,value", [(0, 1.0), (0, True), (0, 2**63), (18, 2**31),
                                          (18, Int64(10)), (18, True), (5, None)])
def test_conversion_does_not_coerce_bad_types(query_fixture, position, value):
    _, _, reference = query_fixture
    row = list(canonical_row(reference))
    row[position] = value
    with pytest.raises(ValueError):
        to_document(tuple(row))


def test_validator_requires_all_fields_and_nullable_bson_null():
    schema = collection_validator()["$jsonSchema"]
    assert len(schema["required"]) == 32
    assert schema["additionalProperties"] is False
    assert schema["properties"]["_id"]["bsonType"] == "long"
    assert schema["properties"]["departure_delay"]["bsonType"] == ["int", "null"]
    assert schema["properties"]["distance"]["bsonType"] == "int"


def test_canonical_reader_rejects_int32_id_and_int64_regular_field(query_fixture):
    _, _, reference = query_fixture
    document = to_document(canonical_row(reference))
    document["_id"] = 1
    with pytest.raises(ValueError, match="Int64"):
        canonical_document(document)
    document["_id"] = Int64(1)
    document["distance"] = Int64(1800)
    with pytest.raises(ValueError, match="Int32"):
        canonical_document(document)


@pytest.mark.parametrize("write_concern_error,expected", [(False, 1), (True, None)])
def test_bulk_error_exposes_partial_or_uncertain_ack(query_fixture, write_concern_error, expected):
    _, _, reference = query_fixture
    adapter = MongoDBAdapter()
    adapter.client = MagicMock()
    collection = adapter.client.__getitem__.return_value.get_collection.return_value
    details = {"nInserted": 1, "writeErrors": [{"index": 1, "code": 11000, "errmsg": "duplicate"}],
               "writeConcernErrors": [{"code": 64, "errmsg": "journal timeout"}] if write_concern_error else []}
    collection.insert_many.side_effect = BulkWriteError(details)
    with pytest.raises(BatchWriteError) as error:
        adapter.ingest_batch([canonical_row(reference)])
    assert error.value.acknowledged_rows == expected


def test_network_error_is_not_retried_or_claimed_as_rollback(query_fixture):
    _, _, reference = query_fixture
    adapter = MongoDBAdapter()
    adapter.client = MagicMock()
    collection = adapter.client.__getitem__.return_value.get_collection.return_value
    collection.insert_many.side_effect = ConnectionFailure("synthetic disconnect")
    with pytest.raises(BatchWriteError) as error:
        adapter.ingest_batch([canonical_row(reference)])
    assert error.value.acknowledged_rows is None
    assert collection.insert_many.call_count == 1


@pytest.mark.parametrize("acknowledged", [1, None])
def test_shared_ingestion_records_partial_mongo_batch(query_fixture, acknowledged):
    from test_ingestion import BatchAdapter

    path, artifact, _ = query_fixture
    adapter = BatchAdapter()
    adapter.ingest_batch = Mock(side_effect=BatchWriteError("synthetic partial batch", acknowledged))
    with pytest.raises(IngestionError) as error:
        ingest_csv(adapter, path, 7, artifact["sha256"], batch_size=3)
    assert error.value.progress["rows_committed"] == (acknowledged or 0)
    assert error.value.progress["current_batch_acknowledged_rows"] == acknowledged
    assert error.value.progress["write_outcome_uncertain"] == (acknowledged is None)


def test_comparison_snapshot_checks_real_values_and_identity(query_fixture, tmp_path):
    _, _, reference = query_fixture
    postgres = MemoryAdapter(reference)
    postgres.version = lambda: "synthetic-postgres"
    postgres.name = "postgres"
    snapshot = capture_database_snapshot(postgres, reference, "primary_only")
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot))
    mongo = MemoryAdapter(reference)
    mongo.version = lambda: "synthetic-mongo"
    mongo.name = "mongo"
    assert compare_postgres_snapshot(mongo, reference, path, "primary_only")["status"] == "passed"
    snapshot["dataset_sha256"] = "different"
    path.write_text(json.dumps(snapshot))
    with pytest.raises(ValueError, match="non omogeneo"):
        compare_postgres_snapshot(mongo, reference, path, "primary_only")
