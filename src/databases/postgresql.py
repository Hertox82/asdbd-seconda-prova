"""PostgreSQL: schema derivato dal contratto approvato e query parametrizzate.

Una connessione per sessione, autocommit per le letture; ogni batch scrive in
una transazione esplicita. Nessun COPY, ON CONFLICT o indice secondario implicito.
"""

import psycopg
from psycopg import sql

from src.schema import PREPARED_COLUMNS, RAW_COLUMNS, schema_manifest


def column_definitions() -> sql.Composed:
    """Costruisce le 32 definizioni SQL dal contratto, preservando tipi e nullabilità."""
    columns = []
    for field in schema_manifest():
        constraints = "" if field["nullable"] else " NOT NULL"
        if field["name"] == "flight_id":
            constraints += " PRIMARY KEY CHECK (flight_id > 0)"
        elif field["logical_type"] == "flag":
            constraints += f" CHECK ({field['name'].lower()} IN (0, 1))"
        columns.append(sql.SQL("{} {}{}").format(
            sql.Identifier(field["name"].lower()), sql.SQL(field["postgresql"]), sql.SQL(constraints)))
    return sql.SQL(", ").join(columns)


class PostgreSQLAdapter:
    """Adapter didattico; schema/table sono identificatori, mai SQL concatenato libero."""

    name = "postgres"
    ingestion_method = "psycopg.executemany"
    commit_policy = "one_transaction_per_batch"

    def __init__(self, *, schema: str = "public", table: str = "flights") -> None:
        self.schema = schema
        self.table = table
        self.connection = None

    @property
    def table_identifier(self) -> sql.Identifier:
        """Restituisce il nome qualificato e correttamente quotato della tabella."""
        return sql.Identifier(self.schema, self.table)

    @property
    def db(self):
        """Richiede una connessione aperta, evitando errori poco leggibili su None."""
        if self.connection is None or self.connection.closed:
            raise RuntimeError("Connessione PostgreSQL non aperta: chiamare connect().")
        return self.connection

    def connect(self) -> None:
        """Apre una sola connessione tramite PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.

        Disabilita la preparazione automatica delle query per rendere esplicito
        il protocollo misurato; non stampa né salva le credenziali.
        """
        if self.connection is not None and not self.connection.closed:
            raise RuntimeError("La connessione è già aperta.")
        self.connection = psycopg.connect(
            autocommit=True, connect_timeout=10, prepare_threshold=None,
            application_name="asdbd-task3")
        self.db.execute("SET statement_timeout = '120s'")
        self.db.execute("SET lock_timeout = '10s'")

    def close(self) -> None:
        """Chiude la sessione senza cancellare tabella, indici o volumi."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def create_schema(self) -> None:
        """Crea la tabella se assente; se presente, verifica il contratto senza ALTER."""
        self.db.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            self.table_identifier, column_definitions()))
        self.validate_schema()

    def reset_dataset(self, *, confirm_reset: bool = False) -> None:
        """Ricrea solo la tabella selezionata, esclusivamente con conferma booleana True.

        Nessun CASCADE: dipendenze estranee devono bloccare l'operazione. DROP e
        CREATE sono nella stessa transazione; un errore ripristina la vecchia tabella.
        """
        if confirm_reset is not True:
            raise PermissionError("Reset rifiutato: serve confirm_reset=True esplicito.")
        with self.db.transaction():
            self.db.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(self.table_identifier))
            self.db.execute(sql.SQL("CREATE TABLE {} ({})").format(
                self.table_identifier, column_definitions()))
        self.validate_schema()

    def validate_schema(self) -> None:
        """Controlla ordine, tipi, nullabilità e assenza di default/identity/generatori ID."""
        actual = self.db.execute(
            "SELECT column_name, udt_name, is_nullable, column_default, is_identity, is_generated "
            "FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
            "ORDER BY ordinal_position", (self.schema, self.table)).fetchall()
        types = {"BIGINT": "int8", "INTEGER": "int4", "TEXT": "text"}
        expected = [(f["name"].lower(), types[f["postgresql"]], "YES" if f["nullable"] else "NO",
                     None, "NO", "NEVER") for f in schema_manifest()]
        if actual != expected:
            raise ValueError("Schema PostgreSQL diverso dal contratto approvato (tipi/null/default/ordine).")
        primary = [index for index in self.indexes() if index["primary"]]
        if len(primary) != 1 or primary[0]["columns"] != ["flight_id"] or not primary[0]["valid"]:
            raise ValueError("Chiave primaria attesa: solo flight_id, valida.")

    def indexes(self) -> list[dict]:
        """Legge indici reali dal catalogo, inclusi colonne, validità e definizione."""
        rows = self.db.execute(
            "SELECT ix.relname, i.indisprimary, i.indisvalid, pg_get_indexdef(i.indexrelid), "
            "ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, pos) "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.pos) "
            "FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid "
            "JOIN pg_namespace n ON n.oid=t.relnamespace JOIN pg_class ix ON ix.oid=i.indexrelid "
            "WHERE n.nspname=%s AND t.relname=%s ORDER BY ix.relname",
            (self.schema, self.table)).fetchall()
        return [dict(zip(("name", "primary", "valid", "definition", "columns"), row)) for row in rows]

    def assert_index_state(self, state: str) -> list[dict]:
        """Richiede solo PK oppure PK + i due indici previsti, senza correggere lo stato."""
        if state not in ("primary_only", "secondary"):
            raise ValueError("Stato indici non riconosciuto.")
        indexes = self.indexes()
        primary = [i for i in indexes if i["primary"]]
        secondary = [i for i in indexes if not i["primary"]]
        expected = [] if state == "primary_only" else [["distance"], ["origin_airport"]]
        if (len(primary) != 1 or primary[0]["columns"] != ["flight_id"]
                or any(not i["valid"] for i in indexes)
                or sorted(i["columns"] for i in secondary) != expected):
            raise ValueError(f"Indici incompatibili con {state}; nessuna eliminazione automatica.")
        return indexes

    def count_rows(self) -> int:
        """Restituisce il numero reale dei record, senza stime del catalogo."""
        return self.db.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(self.table_identifier)).fetchone()[0]

    def ingest_batch(self, rows: list[tuple]) -> None:
        """Inserisce un batch con executemany e commit; rollback dell'intero batch su errore."""
        columns = sql.SQL(", ").join(sql.Identifier(c.lower()) for c in PREPARED_COLUMNS)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in PREPARED_COLUMNS)
        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            self.table_identifier, columns, placeholders)
        with self.db.transaction():
            with self.db.cursor() as cursor:
                cursor.executemany(statement, rows)

    def integrity_summary(self) -> dict:
        """Calcola conteggio, estremi, unicità ID e tutti i conteggi NULL, fuori dai timer."""
        nulls = sql.SQL(", ").join(sql.SQL("COUNT(*) FILTER (WHERE {} IS NULL)").format(
            sql.Identifier(c.lower())) for c in RAW_COLUMNS)
        row = self.db.execute(sql.SQL(
            "SELECT COUNT(*), MIN(flight_id), MAX(flight_id), COUNT(DISTINCT flight_id), {} FROM {}"
        ).format(nulls, self.table_identifier)).fetchone()
        return {"rows": row[0], "flight_id_min": row[1], "flight_id_max": row[2],
                "distinct_ids": row[3], "null_counts": dict(zip(RAW_COLUMNS, row[4:]))}

    def _query(self, query_id: str, parameters: dict) -> tuple:
        """Traduce le quattro operazioni canoniche in SQL e parametri separati."""
        if query_id == "Q1":
            columns = sql.SQL(", ").join(sql.Identifier(c.lower()) for c in PREPARED_COLUMNS)
            return sql.SQL("SELECT {} FROM {} WHERE flight_id=%s").format(columns, self.table_identifier), (parameters["flight_id"],)
        if query_id == "Q2":
            return sql.SQL("SELECT COUNT(*) FROM {} WHERE origin_airport=%s").format(self.table_identifier), (parameters["origin_airport"],)
        if query_id == "Q3":
            return sql.SQL("SELECT COUNT(*) FROM {} WHERE distance BETWEEN %s AND %s").format(self.table_identifier), (parameters["distance_min"], parameters["distance_max"])
        if query_id == "Q4":
            return sql.SQL("SELECT airline, AVG(departure_delay) FROM {} GROUP BY airline ORDER BY airline").format(self.table_identifier), ()
        raise ValueError(f"Query sconosciuta: {query_id}.")

    def run_query(self, query_id: str, parameters: dict):
        """Esegue e acquisisce tutto il risultato, normalizzandolo nel contratto comune."""
        statement, values = self._query(query_id, parameters)
        with self.db.cursor() as cursor:
            cursor.execute(statement, values)
            if query_id == "Q1":
                row = cursor.fetchone()
                return dict(zip(PREPARED_COLUMNS, row)) if row is not None else None
            if query_id == "Q4":
                return {airline: float(mean) if mean is not None else None for airline, mean in cursor.fetchall()}
            return cursor.fetchone()[0]

    def explain(self, query_id: str, parameters: dict) -> dict:
        """Raccoglie EXPLAIN ANALYZE in JSON; va chiamato fuori dai timer ordinari."""
        statement, values = self._query(query_id, parameters)
        return self.db.execute(sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + statement, values).fetchone()[0][0]

    def create_secondary_index(self, column: str) -> None:
        """Crea sincronicamente un solo indice ammesso; non forza il planner a usarlo."""
        if column not in ("origin_airport", "distance"):
            raise ValueError("Indice secondario non previsto dal protocollo.")
        self.db.execute(sql.SQL("CREATE INDEX {} ON {} ({})").format(
            sql.Identifier(f"{self.table}_{column}_idx"), self.table_identifier, sql.Identifier(column)))

    def drop_secondary_indexes(self, *, confirm_drop: bool = False) -> None:
        """Rimuove solo gli indici previsti appartenenti alla tabella, con conferma esplicita."""
        if confirm_drop is not True:
            raise PermissionError("Eliminazione indici rifiutata: serve confirm_drop=True.")
        owned = {i["name"] for i in self.indexes() if not i["primary"]}
        for column in ("origin_airport", "distance"):
            name = f"{self.table}_{column}_idx"
            if name in owned:
                self.db.execute(sql.SQL("DROP INDEX {}").format(sql.Identifier(self.schema, name)))

    def analyze(self) -> None:
        """Aggiorna le statistiche del planner dopo l'ingestione, fuori dai timer."""
        self.db.execute(sql.SQL("ANALYZE {}").format(self.table_identifier))

    def version(self) -> str:
        """Restituisce la versione effettiva del server, senza credenziali."""
        return self.db.execute("SELECT version()").fetchone()[0]
