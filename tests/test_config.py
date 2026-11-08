"""Validazione configurazione: nessun reset implicito o passaggio a task futuri."""

from pathlib import Path

import pytest
import yaml

from src.config import load_config


@pytest.mark.parametrize("section,key,value", [
    (None, "mode", "DEMO"), (None, "database", "redis"), (None, "dataset_size", 999),
    ("ingestion", "confirm_reset", "False"), ("ingestion", "batch_size", 0),
    ("benchmark", "confirm_drop_indexes", "True"), ("benchmark", "warmup", -1),
    ("benchmark", "repetitions", True), ("postgres", "schema", ""),
])
def test_unsafe_or_unsupported_configuration_is_rejected(tmp_path, section, key, value):
    config = yaml.safe_load(Path("config/benchmark.yaml").read_text(encoding="utf-8"))
    (config if section is None else config[section])[key] = value
    target = tmp_path / "config" / "benchmark.yaml"
    target.parent.mkdir()
    target.write_text(yaml.safe_dump(config), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(target)


def test_default_configuration_has_no_destructive_confirmation():
    config = load_config(Path("config/benchmark.yaml"))
    assert config.confirm_reset is False
    assert config.confirm_drop_indexes is False
    assert config.dataset_size == 100_000
    assert config.manifest()["database"] in ("postgres", "mongo", "cockroach")
    assert "PGPASSWORD" not in str(config.manifest())
