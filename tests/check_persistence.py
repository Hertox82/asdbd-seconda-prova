"""Sonda sintetica Task 1: prepare, poi ricreare i container, check e cleanup.

Eseguire dentro Jupyter. Usa una tabella dedicata, senza toccare flights.
I file sonda consentono di controllare bind mount e volume home separatamente.
"""

import argparse
from pathlib import Path
from uuid import uuid4

import psycopg


PROBES = (
    Path("/workspace/results/task1-persistence-probe.txt"),
    Path("/workspace/notebooks/.task1-persistence-probe.txt"),
    Path("/home/lab/.task1-persistence-probe.txt"),
)


def prepare() -> None:
    """Crea una tabella e tre file sintetici; fallisce se la tabella esiste già."""
    token = uuid4().hex
    with psycopg.connect(connect_timeout=5) as connection:
        connection.execute("CREATE TABLE task1_persistence_probe (value TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO task1_persistence_probe VALUES (%s)", (token,))
    for path in PROBES:
        path.write_text(token, encoding="utf-8")
    print("Sonda creata: DB, notebook, risultati e home.")


def check() -> None:
    """Confronta la riga persistente con i file: un mismatch interrompe il test."""
    token = PROBES[0].read_text(encoding="utf-8")
    with psycopg.connect(connect_timeout=5) as connection:
        rows = connection.execute("SELECT value FROM task1_persistence_probe").fetchall()
    assert rows == [(token,)], "La riga nel volume PostgreSQL non corrisponde alla sonda."
    assert all(path.read_text(encoding="utf-8") == token for path in PROBES)
    print("Persistenza OK: PostgreSQL, notebook, risultati e home.")


def cleanup() -> None:
    """Elimina soltanto la tabella e i file sintetici creati da questa sonda."""
    with psycopg.connect(connect_timeout=5) as connection:
        connection.execute("DROP TABLE IF EXISTS task1_persistence_probe")
    for path in PROBES:
        path.unlink(missing_ok=True)
    print("Sonda sintetica rimossa; nessun dataset applicativo modificato.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "check", "cleanup"))
    arguments = parser.parse_args()
    {"prepare": prepare, "check": check, "cleanup": cleanup}[arguments.phase]()
