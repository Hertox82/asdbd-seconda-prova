"""Prove su chunk, checkpoint, fallimenti e immutabilità con soli dati sintetici."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.preprocessing import acquire_source, prepare_dataset
from src.schema import DATASET_HANDLE, PREPARED_COLUMNS
from src.validation import SourceContract, csv_records, sha256_file, validate_bundle


def prepare_small(source, output, contract, **kwargs):
    """Prepara tre prefissi della fixture: nessuno rappresenta il dataset reale."""
    return prepare_dataset(source, output, sizes=(2, 4, 7), contract=contract, **kwargs)


@pytest.mark.parametrize("chunk_size", [1, 3, 20])
def test_ids_and_checksums_independent_of_chunks(small_source, tmp_path, chunk_size):
    source, original, contract = small_source
    before = source.read_bytes()
    first = prepare_small(source, tmp_path / "first", contract, chunk_size=2)
    second = prepare_small(source, tmp_path / "second", contract, chunk_size=chunk_size)
    assert [a["sha256"] for a in first["manifest"]["artifacts"]] == [
        a["sha256"] for a in second["manifest"]["artifacts"]]
    for size in (2, 4, 7):
        with csv_records(tmp_path / "first" / f"flights_{size}.csv", PREPARED_COLUMNS) as reader:
            rows = list(reader)
        assert [row[0] for row in rows] == [str(i) for i in range(1, size + 1)]
        assert [row[1:] for row in rows] == original[:size]
    assert source.read_bytes() == before
    assert first["manifest"]["source"]["sha256"] == contract.expected_sha256
    assert validate_bundle(tmp_path / "first", source, contract)["status"] == "validated"


def test_checkpoint_is_verified_and_does_not_rewrite(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    first = prepare_small(source, output, contract)
    before = {p.name: (p.stat().st_mtime_ns, p.read_bytes()) for p in output.iterdir()}
    second = prepare_small(source, output, contract, chunk_size=1)
    assert second["skipped"] is True
    assert second["manifest"] == first["manifest"]
    assert {p.name: (p.stat().st_mtime_ns, p.read_bytes()) for p in output.iterdir()} == before


def test_corrupted_output_cannot_be_skipped(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    prepare_small(source, output, contract)
    with (output / "flights_4.csv").open("ab") as stream:
        stream.write(b"corruption")
    with pytest.raises(ValueError, match="Checksum"):
        prepare_small(source, output, contract)


def test_source_changes_are_rejected_even_with_force(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    prepare_small(source, output, contract)
    source.write_bytes(source.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="Checksum"):
        prepare_small(source, output, contract)
    with pytest.raises(ValueError, match="Checksum"):
        prepare_small(source, output, contract, force=True)


def test_force_keeps_old_bundle_as_backup(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "prepared"
    first = prepare_small(source, output, contract)
    second = prepare_small(source, output, contract, force=True, chunk_size=1)
    assert not second["skipped"]
    backup = Path(second["previous_bundle_backup"])
    assert json.loads((backup / "manifest.json").read_text()) == first["manifest"]
    assert first["manifest"]["artifacts"] == second["manifest"]["artifacts"]


def test_partial_output_still_checks_complete_source(small_source, tmp_path):
    source, _, _ = small_source
    contract = SourceContract(8, sha256_file(source), "synthetic-wrong-count")
    output = tmp_path / "prepared"
    with pytest.raises(ValueError, match="Righe sorgente"):
        prepare_dataset(source, output, sizes=(2,), contract=contract)
    assert not output.exists()
    assert not list(tmp_path.glob(".prepared.building-*"))
    assert not (tmp_path / ".prepared.prepare-lock").exists()


@pytest.mark.parametrize("sizes", [(), (0,), (8,), (2, 2), (True,), (2.5,)])
def test_bad_sizes_rejected(small_source, tmp_path, sizes):
    source, _, contract = small_source
    with pytest.raises(ValueError, match="sizes"):
        prepare_dataset(source, tmp_path / "prepared", sizes=sizes, contract=contract)


@pytest.mark.parametrize("chunk_size", [0, -1, True, 1.5])
def test_bad_chunks_rejected(small_source, tmp_path, chunk_size):
    source, _, contract = small_source
    with pytest.raises(ValueError, match="chunk_size"):
        prepare_small(source, tmp_path / "prepared", contract, chunk_size=chunk_size)


def test_source_cannot_be_inside_output(small_source):
    source, _, contract = small_source
    with pytest.raises(ValueError, match="contenere la sorgente"):
        prepare_small(source, source.parent, contract)


def test_existing_unknown_directory_is_not_overwritten(small_source, tmp_path):
    source, _, contract = small_source
    output = tmp_path / "unrelated"
    output.mkdir()
    (output / "notes.txt").write_text("non cancellare")
    with pytest.raises(ValueError, match="senza manifest"):
        prepare_small(source, output, contract, force=True)
    assert (output / "notes.txt").read_text() == "non cancellare"


def test_offline_source_copy_and_reuse(small_source, tmp_path, monkeypatch):
    source, _, contract = small_source
    monkeypatch.setitem(sys.modules, "kagglehub", None)
    target = tmp_path / "copy" / "flights.csv"
    metadata = acquire_source(target, local_source=source, contract=contract)
    assert metadata["acquisition_method"] == "copied_local_csv"
    assert source.read_bytes() == target.read_bytes()
    assert acquire_source(target, contract=contract) == metadata
    assert metadata["dataset"] == "synthetic-fixture"


def test_missing_offline_source_does_not_download(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "kagglehub", None)
    with pytest.raises(FileNotFoundError, match="offline"):
        acquire_source(tmp_path / "missing.csv")


def test_existing_raw_is_never_overwritten(small_source, tmp_path):
    source, _, contract = small_source
    target = tmp_path / "existing.csv"
    target.write_text("contenuto da conservare")
    with pytest.raises(ValueError, match="checksum"):
        acquire_source(target, local_source=source, contract=contract)
    assert target.read_text() == "contenuto da conservare"


def test_download_uses_pinned_handle_and_rejects_wrong_checksum(small_source, tmp_path, monkeypatch):
    source, _, _ = small_source
    called = []

    def fake_download(handle, *, path):
        called.append((handle, path))
        return str(source)

    monkeypatch.setitem(sys.modules, "kagglehub", SimpleNamespace(dataset_download=fake_download))
    # La fixture non può fingere di avere il checksum ufficiale.
    with pytest.raises(ValueError, match="Checksum"):
        acquire_source(tmp_path / "download" / "flights.csv", offline=False)
    assert called == [(DATASET_HANDLE, "flights.csv")]
    assert not (tmp_path / "download" / "flights.csv").exists()


def test_mocked_download_is_copied_to_persistent_raw(small_source, tmp_path, monkeypatch):
    from src import preprocessing

    source, _, contract = small_source
    original = source.read_bytes()
    called = []

    def fake_download(handle, *, path):
        called.append((handle, path))
        return str(source)

    # Solo nel test sostituiamo il contratto ufficiale con la fixture sintetica.
    monkeypatch.setattr(preprocessing, "OFFICIAL_CONTRACT", contract)
    monkeypatch.setitem(sys.modules, "kagglehub", SimpleNamespace(dataset_download=fake_download))
    target = tmp_path / "download" / "flights.csv"
    metadata = acquire_source(target, offline=False, contract=contract)
    assert called == [(DATASET_HANDLE, "flights.csv")]
    assert metadata["acquisition_method"] == "kagglehub_versioned_download"
    assert target.read_bytes() == source.read_bytes() == original


def test_failed_force_does_not_publish_or_replace_old_bundle(small_source, tmp_path):
    source, _, _ = small_source
    contract = SourceContract(7, None, "synthetic-forced-failure")
    output = tmp_path / "prepared"
    prepare_small(source, output, contract)
    previous = {p.name: p.read_bytes() for p in output.iterdir()}
    source.write_text(source.read_text().replace("2015,1,1", "non-intero,1,1", 1))
    with pytest.raises(ValueError, match="YEAR"):
        prepare_small(source, output, contract, force=True)
    assert {p.name: p.read_bytes() for p in output.iterdir()} == previous
    assert not list(tmp_path.glob(".prepared.backup-*"))


def test_preparation_records_real_resource_metrics(small_source, tmp_path):
    source, _, contract = small_source
    result = prepare_small(source, tmp_path / "prepared", contract)
    metrics = result["manifest"]["metrics"]
    assert metrics["elapsed_seconds"] > 0
    assert metrics["sampled_peak_rss_bytes"] >= metrics["baseline_rss_bytes"] > 0
    assert metrics["sampling_interval_seconds"] == 0.1


def test_boolean_options_do_not_accept_strings(small_source, tmp_path):
    source, _, contract = small_source
    with pytest.raises(ValueError, match="force"):
        prepare_small(source, tmp_path / "prepared", contract, force="False")
    with pytest.raises(ValueError, match="offline"):
        acquire_source(source, offline="False", contract=contract)
