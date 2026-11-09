"""Piccola sorgente sintetica: nessun accesso a Kaggle, research o CSV reale."""

import csv

import pytest

from src.schema import FIELDS, RAW_COLUMNS
from src.validation import SourceContract, sha256_file


def synthetic_row(**overrides: str) -> list[str]:
    """Crea un record minimo valido; overrides cambia solo i campi del test."""
    values = {field.name: "0" if field.kind in ("integer", "flag") else "" for field in FIELDS}
    values.update({
        "YEAR": "2015", "MONTH": "1", "DAY": "1", "DAY_OF_WEEK": "4",
        "AIRLINE": "AA", "FLIGHT_NUMBER": "98", "TAIL_NUMBER": "N001",
        "ORIGIN_AIRPORT": "JFK", "DESTINATION_AIRPORT": "SEA",
        "SCHEDULED_DEPARTURE": "0005", "SCHEDULED_ARRIVAL": "2400",
        "DEPARTURE_DELAY": "", "DISTANCE": "1800",
    })
    values.update(overrides)
    return [values[name] for name in RAW_COLUMNS]


@pytest.fixture
def small_source(tmp_path):
    """Restituisce percorso, sette record (anche duplicati) e contratto sintetico."""
    rows = [
        synthetic_row(),
        synthetic_row(),  # Due righe uguali devono restare due record distinti.
        synthetic_row(DEPARTURE_DELAY="-11", TAIL_NUMBER="N,001\nA"),
        synthetic_row(DEPARTURE_DELAY="0", ORIGIN_AIRPORT="01234", TAIL_NUMBER="NA"),
        synthetic_row(DEPARTURE_DELAY="2", DISTANCE="2000", AIRLINE="BB"),
        synthetic_row(DEPARTURE_DELAY="5", DISTANCE="1500", CANCELLED="1"),
        synthetic_row(DEPARTURE_DELAY="", DISTANCE="1499", TAIL_NUMBER=""),
    ]
    path = tmp_path / "raw" / "flights.csv"
    path.parent.mkdir()
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\r\n")
        writer.writerow(RAW_COLUMNS)
        writer.writerows(rows)
    contract = SourceContract(len(rows), sha256_file(path), "synthetic-fixture")
    return path, rows, contract
