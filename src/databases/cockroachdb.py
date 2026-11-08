"""CockroachDB single-node: SQL compatibile, metadati e transazioni specifici.

Si riusano soltanto le query di lettura PostgreSQL effettivamente compatibili.
DDL, tipi, catalogo, piani, indici e retry sono implementati per questo motore.
"""

import logging
import os
import time

import psycopg
from psycopg import sql
from psycopg.crdb import CrdbConnection

from src.databases.base import BatchWriteError
from src.databases.postgresql import PostgreSQLAdapter
from src.schema import PREPARED_COLUMNS, schema_manifest


LOGGER = logging.getLogger(__name__)


def column_definitions() -> sql.Composed:
    """Deriva il DDL dal contratto Cockroach: INT8, INT4 e STRING espliciti.

    INTEGER senza larghezza in Cockroach può significare int64: non riusiamo
    il DDL PostgreSQL. La PK esplicita impedisce una rowid nascosta/generata.
    """
    definitions = []
    for field in schema_manifest():
        constraints = "" if field["nullable"] else " NOT NULL"
        if field["name"] == "flight_id":
            constraints += " PRIMARY KEY CHECK (flight_id > 0)"
        elif field["logical_type"] == "flag":
            constraints += f" CHECK ({field['name'].lower()} IN (0, 1))"
        definitions.append(sql.SQL("{} {}{}").format(
            sql.Identifier(field["name"].lower()), sql.SQL(field["cockroachdb"]), sql.SQL(constraints)))
    return sql.SQL(", ").join(definitions)


class CockroachDBAdapter(PostgreSQLAdapter):
    """Un solo nodo, nessuna simulazione di disponibilità o scalabilità distribuita."""

    name = "cockroach"
    ingestion_method = "psycopg.crdb.executemany"
    commit_policy = "serializable_transaction_per_batch_retry_sqlstate_40001_only"

    def __init__(self, *, database: str = "defaultdb", schema: str = "public",
                 table: str = "flights", max_retries: int = 3) -> None:
        super().__init__(schema=schema, table=table)
        if type(max_retries) is not int or not 0 <= max_retries <= 10:
            raise ValueError("max_retries deve essere un intero tra 0 e 10.")
        self.database_name = database
        self.max_retries = max_retries
        self.retries_total = 0
        self.batches_retried = 0

    def connect(self) -> None:
        """Usa CrdbConnection e parametri propri, senza ereditare password/database PG.

        Root senza TLS/password è ammesso solo nel servizio Docker locale insecure.
        Statement timeout 120s e lock timeout 10s restano quelli delle prove 100k.
        """
        if self.connection is not None and not self.connection.closed:
            raise RuntimeError("La connessione CockroachDB è già aperta.")
        self.connection = CrdbConnection.connect(
            host=os.getenv("CRDB_HOST", "cockroach"), port=int(os.getenv("CRDB_PORT", "26257")),
            dbname=self.database_name, user="root", password="", sslmode="disable",
            autocommit=True, connect_timeout=10, prepare_threshold=None,
            application_name="asdbd-task5")
        self.db.execute("SET statement_timeout = '120s'")
        self.db.execute("SET lock_timeout = '10s'")
        self.db.execute("SET default_transaction_isolation = 'serializable'")

    def create_schema(self) -> None:
        """Crea solo una tabella assente; verifica la tabella presente senza migrazioni."""
        self.db.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            self.table_identifier, column_definitions()))
        self.validate_schema()

    def reset_dataset(self, *, confirm_reset: bool = False) -> None:
        """DROP/CREATE solo con conferma; non promette il rollback DDL di PostgreSQL."""
        if confirm_reset is not True:
            raise PermissionError("Reset CockroachDB rifiutato: serve confirm_reset=True.")
        self.db.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(self.table_identifier))
        self.create_schema()

    def validate_schema(self) -> None:
        """Verifica 32 colonne, tipi fisici, nullabilità e assenza di generatori ID."""
        actual = self.db.execute(
            "SELECT column_name, udt_name, is_nullable, column_default, is_identity, is_generated "
            "FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
            "ORDER BY ordinal_position", (self.schema, self.table)).fetchall()
        # STRING senza limite viene esposto come text nel catalogo v25.3.
        types = {"INT8": "int8", "INT4": "int4", "STRING": "text"}
        expected = [(field["name"].lower(), types[field["cockroachdb"]],
                     "YES" if field["nullable"] else "NO", None, "NO", "NEVER")
                    for field in schema_manifest()]
        if actual != expected:
            raise ValueError(f"Schema CockroachDB diverso dal contratto approvato: ottenuto={actual!r}.")
        primary = [index for index in self.indexes() if index["primary"]]
        if len(primary) != 1 or primary[0]["columns"] != ["flight_id"] or not primary[0]["valid"]:
            raise ValueError("CockroachDB richiede solo flight_id come PK esplicita valida.")

    def indexes(self) -> list[dict]:
        """Legge SHOW INDEXES distinguendo colonne chiave, implicite e STORING.

        I secondari includono la PK in modo implicito: non va confusa con un
        indice composto esplicitamente richiesto o con una colonna dati extra.
        """
        primary_names = {row[0] for row in self.db.execute(
            "SELECT constraint_name FROM information_schema.table_constraints "
            "WHERE table_schema=%s AND table_name=%s AND constraint_type='PRIMARY KEY'",
            (self.schema, self.table)).fetchall()}
        with self.db.cursor() as cursor:
            cursor.execute(sql.SQL("SHOW INDEXES FROM {}").format(self.table_identifier))
            names = [column.name for column in cursor.description]
            records = [dict(zip(names, row)) for row in cursor.fetchall()]
        grouped = {}
        for row in records:
            index = grouped.setdefault(row["index_name"], {
                "name": row["index_name"], "primary": row["index_name"] in primary_names,
                "valid": True, "columns": [], "implicit_columns": [], "stored_columns": [],
                "unique": not row["non_unique"], "directions": [],
            })
            if not row.get("visible", True):
                index["valid"] = False
            if row["storing"]:
                index["stored_columns"].append(row["column_name"])
            elif row["implicit"]:
                index["implicit_columns"].append(row["column_name"])
            else:
                index["columns"].append(row["column_name"])
                index["directions"].append(row["direction"])
        return sorted(grouped.values(), key=lambda item: item["name"])

    def assert_index_state(self, state: str) -> list[dict]:
        """Esclude secondari covering inattesi e verifica PK implicita e ordinamento.

        Le colonne STORING della primaria sono normali dati della tabella;
        quelle dei secondari cambierebbero invece il protocollo del confronto.
        Non modifica gli indici né gli artefatti delle misure già approvate.
        """
        indexes = super().assert_index_state(state)
        for index in indexes:
            if not index["primary"] and (
                    index["stored_columns"] or index["implicit_columns"] != ["flight_id"]
                    or index["unique"] or index["directions"] != ["ASC"]):
                raise ValueError("Indice CockroachDB con STORING/chiavi implicite/proprietà inattese.")
        return indexes

    def ingest_batch(self, rows: list[tuple]) -> None:
        """Scrive e conferma un batch SERIALIZABLE, riprovando solo transazioni abortite.

        40001 permette di ripetere l'intero batch, dopo rollback. Errori di rete
        o 40003 (commit ambiguo) non vengono ritentati: potrebbero duplicare scritture.
        Retry e backoff sono inclusi nei tempi dell'adapter e nei contatori.
        """
        columns = sql.SQL(", ").join(sql.Identifier(name.lower()) for name in PREPARED_COLUMNS)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in PREPARED_COLUMNS)
        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            self.table_identifier, columns, placeholders)
        for attempt in range(self.max_retries + 1):
            try:
                with self.db.transaction():
                    with self.db.cursor() as cursor:
                        cursor.executemany(statement, rows)
                return
            except psycopg.Error as error:
                if error.sqlstate == "40001":
                    if attempt >= self.max_retries:
                        raise RuntimeError(f"Limite retry CockroachDB esaurito ({self.max_retries}); batch annullato.") from error
                    self.retries_total += 1
                    self.batches_retried += attempt == 0
                    delay = 0.05 * 2**attempt
                    LOGGER.warning("CockroachDB SQLSTATE 40001: retry %s/%s dopo %.2fs", attempt + 1, self.max_retries, delay)
                    time.sleep(delay)
                elif error.sqlstate == "40003" or (isinstance(error, psycopg.OperationalError)
                        and (error.sqlstate is None or error.sqlstate.startswith("08"))):
                    raise BatchWriteError("Esito scrittura CockroachDB incerto: nessun retry automatico.", None) from error
                else:
                    raise

    def ingestion_diagnostics(self) -> dict:
        """Restituisce retry reali e limiti applicati, anche per un run fallito."""
        return {"transaction_retries": self.retries_total, "batches_retried": self.batches_retried,
                "max_retries_per_batch": self.max_retries, "retry_initial_backoff_seconds": 0.05,
                "statement_timeout_seconds": 120, "lock_timeout_seconds": 10}

    def create_secondary_index(self, column: str) -> None:
        """Misura DDL più attesa dell'indice pubblico; non forza il piano di accesso."""
        super().create_secondary_index(column)
        name = f"{self.table}_{column}_idx"
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if any(item["name"] == name and item["valid"] for item in self.indexes()):
                return
            time.sleep(0.1)
        raise TimeoutError(f"Indice CockroachDB non pubblico entro 120s: {name}.")

    def drop_secondary_indexes(self, *, confirm_drop: bool = False) -> None:
        """Elimina solo gli indici previsti della tabella, con sintassi table@index."""
        if confirm_drop is not True:
            raise PermissionError("Eliminazione indici CockroachDB rifiutata: serve confirm_drop=True.")
        owned = {item["name"] for item in self.indexes() if not item["primary"]}
        for column in ("origin_airport", "distance"):
            name = f"{self.table}_{column}_idx"
            if name in owned:
                self.db.execute(sql.SQL("DROP INDEX {}@{}").format(self.table_identifier, sql.Identifier(name)))

    def analyze(self) -> None:
        """Aggiorna le statistiche CockroachDB fuori dai timer di ingestione/query."""
        self.db.execute(sql.SQL("CREATE STATISTICS {} FROM {}").format(
            sql.Identifier(f"{self.table}_lab_stats"), self.table_identifier))

    def explain(self, query_id: str, parameters: dict) -> dict:
        """Salva il piano testuale nativo EXPLAIN ANALYZE, mai opzioni PostgreSQL JSON."""
        statement, values = self._query(query_id, parameters)
        # EXPLAIN Cockroach non accetta placeholder del protocollo esteso.
        # ClientCursor adatta e quota i valori lato client in modo sicuro;
        # le query temporizzate continuano a usare il binding ordinario.
        with psycopg.ClientCursor(self.db) as cursor:
            cursor.execute(sql.SQL("EXPLAIN ANALYZE ") + statement, values)
            rows = cursor.fetchall()
        return {"format": "cockroachdb_text", "lines": [str(row[0]) for row in rows]}
