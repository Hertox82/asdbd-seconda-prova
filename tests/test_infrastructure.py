"""Test reali Task 1, da eseguire dentro Jupyter con PostgreSQL attivo."""

import os
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg
import pytest


def test_python_version() -> None:
    """Verifica che l'ambiente eseguito sia Python 3.12, come richiesto."""
    assert sys.version_info[:2] == (3, 12)


@pytest.mark.skipif(os.getenv("LAB_TEST_MONGO") == "1" or os.getenv("LAB_TEST_COCKROACH") == "1",
                    reason="Sessione Mongo/Cockroach: PostgreSQL deve restare fermo.")
def test_postgres_connection() -> None:
    """Verifica autenticazione e query reali usando le variabili PG del container."""
    with psycopg.connect(connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1, current_database(), current_user")
            assert cursor.fetchone() == (1, os.environ["PGDATABASE"], os.environ["PGUSER"])


def test_jupyter_authentication() -> None:
    """Richiede un token valido: il server deve rifiutare accessi anonimi all'API."""
    url = "http://127.0.0.1:8888/api/status"
    with pytest.raises(HTTPError) as error:
        urlopen(url, timeout=5)
    assert error.value.code in (401, 403)
    request = Request(url, headers={"Authorization": f"token {os.environ['JUPYTER_TOKEN']}"})
    with urlopen(request, timeout=5) as response:
        assert response.status == 200


def test_container_has_no_docker_access() -> None:
    """Verifica utente non root e assenza del socket Docker nel container."""
    assert os.getuid() != 0
    assert not Path("/var/run/docker.sock").exists()
