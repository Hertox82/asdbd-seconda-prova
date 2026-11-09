"""Seleziona un solo adapter senza aprire connessioni o avviare servizi."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.config import ExperimentConfig


def create_adapter(config: "ExperimentConfig"):
    """Restituisce l'adapter richiesto; le credenziali restano nell'ambiente del client."""
    if config.database == "postgres":
        from src.databases.postgresql import PostgreSQLAdapter
        return PostgreSQLAdapter(schema=config.schema, table=config.table)
    if config.database == "mongo":
        from src.databases.mongodb import MongoDBAdapter
        return MongoDBAdapter(database=config.mongo_database, collection=config.mongo_collection)
    if config.database == "cockroach":
        from src.databases.cockroachdb import CockroachDBAdapter
        return CockroachDBAdapter(database=config.cockroach_database, schema=config.cockroach_schema,
                                 table=config.cockroach_table, max_retries=config.cockroach_max_retries)
    raise ValueError("Adapter non disponibile: scegliere postgres, mongo oppure cockroach.")
