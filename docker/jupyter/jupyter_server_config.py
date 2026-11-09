"""Configurazione locale Jupyter: autenticazione obbligatoria e nessun browser nel container."""

import os

c = get_config()  # Fornito da Jupyter quando carica questo file.
c.ServerApp.ip = "0.0.0.0"
c.ServerApp.port = 8888
c.ServerApp.port_retries = 0
c.ServerApp.open_browser = False
c.ServerApp.root_dir = "/workspace"
c.IdentityProvider.token = os.environ["JUPYTER_TOKEN"]
# Il token non deve finire nei log: lo script host stampa solo l'URL senza segreti.
c.ServerApp.custom_display_url = "http://127.0.0.1:8888"
c.ServerApp.log_level = "WARN"
