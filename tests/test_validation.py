"""Controlli negativi: corruzioni, null, tipi e sequenze non devono passare."""

import csv
import json

import pytest

from conftest import synthetic_row
from src.preprocessing import prepare_dataset
from src.schema import PREPARED_COLUMNS, RAW_COLUMNS, parse_original_row, parse_prepared_row, schema_manifest
from src.validation import (
    OFFICIAL_CONTRACT, check_source_summary, csv_records, iter_prepared_chunks,
    original_chunks, validate_bundle, validate_prepared,
)


def test_null_zero_text_and_hhmm_have_explicit_types():
    values = parse_original_row(synthetic_row(TAIL_NUMBER="NA", DEPARTURE_DELAY="", TAXI_OUT="0"), 1)
    assert values[6] == "NA"
    assert values[9] == "0005"
    assert values[11] is None
    assert values[12] == 0 and type(values[12]) is int
    assert values[20] == "2400"
    values = parse_original_row(synthetic_row(ORIGIN_AIRPORT="01234", DEPARTURE_DELAY="-11"), 1)
    assert values[7] == "01234"
    assert values[11] == -11


@pytest.mark.parametrize("changes", [
    {"DEPARTURE_DELAY": "3.5"}, {"DISTANCE": "NaN"}, {"AIRLINE": ""},
    {"DISTANCE": "2147483648"}, {"CANCELLED": "2"}, {"DIVERTED": ""},
    {"DEPARTURE_TIME": "2460"}, {"DEPARTURE_TIME": "25"}, {"DEPARTURE_TIME": "2401"},
])
def test_bad_values_are_not_silently_coerced(changes):
    with pytest.raises(ValueError, match="Record 4"):
        parse_original_row(synthetic_row(**changes), 4)


@pytest.mark.parametrize("bad_id", ["", "0", "2", "1.0", "01", "NULL"])
def test_bad_or_duplicate_ids_rejected(bad_id):
    with pytest.raises(ValueError, match="flight_id"):
        parse_prepared_row([bad_id, *synthetic_row()], 1)


def test_schema_mapping_is_complete_and_uniform():
    schema = schema_manifest()
    assert len(RAW_COLUMNS) == 31
    assert [field["name"] for field in schema] == list(PREPARED_COLUMNS)
    assert schema[0]["postgresql"] == "BIGINT"
    assert schema[0]["cockroachdb"] == "INT8"
    assert schema[0]["mongodb"] == "Int64"
    for field in schema[1:]:
        if field["logical_type"] in ("integer", "flag"):
            assert (field["postgresql"], field["cockroachdb"], field["mongodb"]) == ("INTEGER", "INT4", "Int32")
        else:
            assert (field["postgresql"], field["cockroachdb"], field["mongodb"]) == ("TEXT", "STRING", "string")


def test_chunk_reader_returns_typed_rows_and_real_subset_counts(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    result = prepare_dataset(source, output, sizes=(2, 7), contract=contract, chunk_size=2)
    blocks = list(iter_prepared_chunks(output / "flights_7.csv", chunk_size=3))
    assert list(map(len, blocks)) == [3, 3, 1]
    assert blocks[0][0][0] == 1
    assert blocks[0][0][12] is None
    assert [item["statistics"]["rows"] for item in result["manifest"]["artifacts"]] == [2, 7]
    assert result["manifest"]["artifacts"][0]["statistics"]["departure_delay_mean"] is None


@pytest.mark.parametrize("kind", ["id", "cell", "truncated", "extra", "header"])
def test_deep_validation_catches_corruption(small_source, tmp_path, kind):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    prepare_dataset(source, output, sizes=(7,), contract=contract)
    path = output / "flights_7.csv"
    with csv_records(path, PREPARED_COLUMNS) as reader:
        rows = list(reader)
    header = list(PREPARED_COLUMNS)
    if kind == "id":
        rows[1][0] = rows[0][0]
    elif kind == "cell":
        rows[0][12] = "0"  # Sostituire NULL con zero deve essere rilevato.
    elif kind == "truncated":
        rows.pop()
    elif kind == "extra":
        rows[0].append("colonna in più")
    else:
        header[0] = "other_id"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)
    with pytest.raises(ValueError):
        validate_prepared(path, 7, source)


def test_invalid_schema_sidecar_is_rejected(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    prepare_dataset(source, output, sizes=(7,), contract=contract)
    (output / "schema.json").write_text("[]")
    with pytest.raises(ValueError, match="schema.json"):
        validate_bundle(output, source, contract, deep=False)


def test_manifest_cannot_escape_bundle(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    prepare_dataset(source, output, sizes=(7,), contract=contract)
    path = output / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["artifacts"][0]["file"] = "../other.csv"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Nome artefatto"):
        validate_bundle(output, source, contract)


def test_inconsistent_manifest_counts_cannot_pass_skip(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    prepare_dataset(source, output, sizes=(7,), contract=contract)
    path = output / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["artifacts"][0]["statistics"]["rows"] = 6
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="conteggi"):
        validate_bundle(output, source, contract, deep=False)


def test_official_counts_do_not_accept_subset_as_full():
    with pytest.raises(ValueError, match="Righe sorgente"):
        check_source_summary({"rows": 100_000}, OFFICIAL_CONTRACT)


def test_wrong_raw_header_is_rejected(small_source):
    source, _, _ = small_source
    content = source.read_text().replace("YEAR,MONTH", "MONTH,YEAR", 1)
    source.write_text(content)
    with pytest.raises(ValueError, match="Schema CSV"):
        list(original_chunks(source, 2))
