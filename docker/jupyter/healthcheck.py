"""Verifica HTTP autenticata del server Jupyter, senza stampare il token."""

import os
from urllib.request import Request, urlopen

request = Request(
    "http://127.0.0.1:8888/api/status",
    headers={"Authorization": f"token {os.environ['JUPYTER_TOKEN']}"},
)
with urlopen(request, timeout=3) as response:
    if response.status != 200:
        raise SystemExit(1)
