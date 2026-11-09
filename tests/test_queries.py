"""Riferimenti e protocollo query: fixture sintetiche e un adapter in memoria."""

import json
from copy import deepcopy

import pytest

from src.benchmark import benchmark_phase
from src.config import lookup_ids
from src.preprocessing import prepare_dataset
from src.query_validation import assert_equivalent, build_reference, verify_database
from src.results import ResultRun


@pytest.fixture
def query_fixture(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    manifest = prepare_dataset(source, output, sizes=(7,), contract=contract)["manifest"]
    artifact = manifest["artifacts"][0]
    parameters = {"origin_airport": "JFK", "distance_min": 1500, "distance_max": 2000}
    reference = build_reference(output / artifact["file"], 7, artifact["sha256"],
                                lookup_ids(7, 7, 42), parameters)
    return output / artifact["file"], artifact, reference


class MemoryAdapter:
    """Simula risultati del DB per testare il protocollo, non per produrre misure reali."""

    name = "postgres"
    ingestion_method = "psycopg.executemany"
    commit_policy = "one_transaction_per_batch"

    def __init__(self, reference):
        self.reference = reference
        self.state = "primary_only"
        self.calls = []
        self.invalid = False

    def validate_schema(self):
        pass

    def assert_index_state(self, state):
        if state != self.state:
            raise ValueError("Stato indici diverso")
        return [{"state": state}]

    def integrity_summary(self):
        return deepcopy(self.reference["integrity"])

    def run_query(self, query_id, parameters):
        self.calls.append((query_id, parameters.copy()))
        if self.invalid:
            return -1
        if query_id == "Q1":
            return deepcopy(self.reference["q1_records"][str(parameters["flight_id"])])
        return deepcopy(self.reference[query_id])

    def explain(self, query_id, parameters):
        return {"query": query_id, "synthetic": True}


def test_reference_uses_subset_nulls_and_inclusive_range(query_fixture):
    _, _, reference = query_fixture
    assert reference["Q2"] == 6
    assert reference["Q3"] == 6
    assert reference["Q4"] == {"AA": -2.0, "BB": 2.0}
    assert reference["integrity"]["null_counts"]["DEPARTURE_DELAY"] == 3
    assert reference["q1_records"]["4"]["ORIGIN_AIRPORT"] == "01234"
    assert reference["q1_records"]["1"]["DEPARTURE_DELAY"] is None


def test_lookup_ids_are_valid_distinct_and_repeatable():
    ids = lookup_ids(100_000, 23, 42)
    assert ids == lookup_ids(100_000, 23, 42)
    assert ids != lookup_ids(100_000, 23, 43)
    assert ids[:2] == [1, 100_000]
    assert len(ids) == len(set(ids)) == 23
    assert all(1 <= value <= 100_000 for value in ids)


@pytest.mark.parametrize("query_id,actual,expected", [
    ("Q2", True, 1), ("Q3", 5, 6), ("Q1", {"A": True}, {"A": 1}),
    ("Q4", {"AA": 1.0}, {"AA": None}), ("Q4", {"AA": float("nan")}, {"AA": 1.0}),
    ("Q4", {}, {"AA": 1.0}),
])
def test_differences_are_not_valid_measurements(query_id, actual, expected):
    with pytest.raises(ValueError, match="Risultato"):
        assert_equivalent(query_id, actual, expected)


def test_q4_tolerance_and_all_null_group():
    assert_equivalent("Q4", {"AA": 1.0 + 1e-10, "BB": None}, {"AA": 1.0, "BB": None})


def test_all_null_group_is_preserved_by_reference(query_fixture):
    _, _, reference = query_fixture
    reference["Q4"]["CC"] = None
    adapter = MemoryAdapter(reference)
    assert verify_database(adapter, reference, "primary_only")["results"]["Q4"]["CC"] is None


def test_benchmark_saves_warmups_and_different_ids(query_fixture, tmp_path):
    _, _, reference = query_fixture
    adapter = MemoryAdapter(reference)
    configuration = {"mode": "FULL", "database": "postgres", "batch_size": 2}
    with ResultRun(tmp_path / "results", configuration, reference, stage="synthetic-test") as run:
        benchmark_phase(adapter, reference, run, index_state="primary_only",
                        query_ids=("Q1", "Q2", "Q3", "Q4"), warmup=1, repetitions=3)
    records = [json.loads(line) for line in (run.directory / "raw.jsonl").read_text().splitlines()]
    assert len(records) == 16
    assert sum(row["warmup"] for row in records) == 4
    q1_ids = [row["parameters"]["flight_id"] for row in records if row["query_id"] == "Q1"]
    assert len(set(q1_ids)) == 4
    assert (run.directory / "summary.csv").exists()


def test_failed_correctness_produces_no_valid_summary(query_fixture, tmp_path):
    _, _, reference = query_fixture
    adapter = MemoryAdapter(reference)
    adapter.invalid = True
    with pytest.raises(ValueError):
        with ResultRun(tmp_path / "results", {"mode": "FULL", "database": "postgres", "batch_size": 2},
                       reference, stage="synthetic-failure") as run:
            benchmark_phase(adapter, reference, run, index_state="primary_only", query_ids=("Q2",))
    assert not (run.directory / "summary.csv").exists()
    assert json.loads((run.directory / "manifest.json").read_text())["status"] == "failed"
    assert json.loads((run.directory / "correctness_primary_only.json").read_text())["status"] == "failed"
