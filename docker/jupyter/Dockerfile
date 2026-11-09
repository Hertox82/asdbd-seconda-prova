FROM python:3.12.12-slim-bookworm

# ID configurabili per rispettare i permessi dei bind mount su Linux nativo.
ARG LAB_UID=1000
ARG LAB_GID=1000

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /opt/lab/requirements.txt
# Riutilizziamo il gruppo se il GID scelto esiste gia nella base Debian.
RUN python -m pip install --no-cache-dir -r /opt/lab/requirements.txt \
    && python -m pip check \
    && test "${LAB_UID}" -gt 0 && test "${LAB_GID}" -gt 0 \
    && (getent group "${LAB_GID}" >/dev/null || groupadd --gid "${LAB_GID}" lab) \
    && useradd --no-log-init --create-home --uid "${LAB_UID}" --gid "${LAB_GID}" lab \
    && mkdir -p /workspace \
    && chown "${LAB_UID}:${LAB_GID}" /workspace

COPY docker/jupyter/jupyter_server_config.py /opt/lab/jupyter_server_config.py
COPY docker/jupyter/healthcheck.py /opt/lab/healthcheck.py
USER lab
WORKDIR /workspace
EXPOSE 8888
CMD ["jupyter", "lab", "--config=/opt/lab/jupyter_server_config.py"]
