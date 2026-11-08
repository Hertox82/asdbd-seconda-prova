"""MongoDB standalone: contratto BSON rigoroso e operazioni equivalenti alle query SQL.

Ogni documento è atomico; insert_many non è una transazione multi-documento.
Il client riusa un solo MongoClient e richiede ack w=1 con journal, senza retry.
"""

import os
import json

from bson import Int64, json_util
from pymongo import ASCENDING, MongoClient
from pymongo.errors import BulkWriteError, PyMongoError
from pymongo.write_concern import WriteConcern

from src.databases.base import BatchWriteError
from src.schema import PREPARED_COLUMNS, RAW_COLUMNS, schema_manifest


def collection_validator() -> dict:
    """Deriva JSON Schema dai tipi approvati: 32 campi obbligatori, null espliciti.

    Anche i campi nullable devono essere presenti: null e campo mancante non
    vengono confusi. flight_id diventa solo _id, mai una seconda chiave.
    """
    properties = {}
    for field in schema_manifest():
        name = "_id" if field["name"] == "flight_id" else field["name"].lower()
        kind = {"Int64": "long", "Int32": "int", "string": "string"}[field["mongodb"]]
        rule = {"bsonType": [kind, "null"] if field["nullable"] else kind}
        if name == "_id":
            rule["minimum"] = 1
        elif field["logical_type"] == "flag":
            rule["enum"] = [0, 1]
        elif field["logical_type"] == "hhmm":
            rule["pattern"] = r"^(?:(?:[01][0-9]|2[0-3])[0-5][0-9]|2400)$"
        properties[name] = rule
    return {"$jsonSchema": {"bsonType": "object", "required": list(properties),
                            "additionalProperties": False, "properties": properties}}


def to_document(row: tuple) -> dict:
    """Converte una riga canonica in BSON senza inferenza di float o ObjectId.

    Int64 forza il tipo BSON 0x12 per la PK. PyMongo codifica gli int Python
    verificati entro int32 come BSON 0x10; non esiste una classe bson.Int32.
    Il validatore server e l'audit $type controllano il risultato fisico.
    """
    if len(row) != len(PREPARED_COLUMNS):
        raise ValueError("Una riga Mongo deve contenere tutte le 32 colonne canoniche.")
    document = {}
    for field, value in zip(schema_manifest(), row):
        name = "_id" if field["name"] == "flight_id" else field["name"].lower()
        if value is None:
            if not field["nullable"]:
                raise ValueError(f"NULL non ammesso in {name}.")
            document[name] = None
        elif field["mongodb"] in ("Int64", "Int32"):
            if type(value) is not int:
                raise ValueError(f"{name}: atteso int Python, non bool/float/wrapper impliciti.")
            if name == "_id":
                if not 1 <= value < 2**63:
                    raise ValueError("flight_id fuori dall'intervallo Int64 positivo.")
                document[name] = Int64(value)
            else:
                if not -(2**31) <= value < 2**31:
                    raise ValueError(f"{name}: fuori dall'intervallo Int32.")
                document[name] = int(value)
        else:
            if type(value) is not str:
                raise ValueError(f"{name}: attesa stringa.")
            document[name] = value
    return document


def canonical_document(document: dict | None) -> dict | None:
    """Normalizza Q1 nel contratto comune, dopo aver verificato i tipi BSON decodificati."""
    if document is None:
        return None
    if set(document) != set(collection_validator()["$jsonSchema"]["properties"]):
        raise ValueError("Documento Mongo con campi mancanti o estranei.")
    values = {}
    for field in schema_manifest():
        name = "_id" if field["name"] == "flight_id" else field["name"].lower()
        value = document[name]
        if value is None:
            if not field["nullable"]:
                raise ValueError(f"NULL inatteso in {name}.")
        elif field["mongodb"] == "Int64":
            if type(value) is not Int64:
                raise ValueError("_id non è stato restituito come BSON Int64.")
            value = int(value)
        elif field["mongodb"] == "Int32":
            if type(value) is not int or not -(2**31) <= value < 2**31:
                raise ValueError(f"{name} non è BSON Int32.")
        elif type(value) is not str:
            raise ValueError(f"{name} non è una stringa BSON.")
        values[field["name"]] = value
    return values


class MongoDBAdapter:
    """Adapter standalone autenticato, senza replica set o transazioni simulate."""

    name = "mongo"
    ingestion_method = "pymongo.insert_many_ordered"
    commit_policy = "ordered_batch_w1_jtrue_no_multi_document_transaction_no_retry"

    def __init__(self, *, database: str = "flights", collection: str = "flights") -> None:
        if database in ("admin", "local", "config"):
            raise ValueError("Usare un database dati dedicato, non un database di sistema.")
        self.database_name = database
        self.collection_name = collection
        self.client = None
        self.last_bson_audit = None

    @property
    def db(self):
        """Richiede un client aperto e restituisce il database selezionato."""
        if self.client is None:
            raise RuntimeError("Client Mongo non aperto: chiamare connect().")
        return self.client[self.database_name]

    @property
    def collection(self):
        """Restituisce la collezione con la write concern esplicita del protocollo."""
        return self.db.get_collection(self.collection_name, write_concern=WriteConcern(w=1, j=True, wtimeout=10_000))

    def connect(self) -> None:
        """Apre un solo client; credenziali da ambiente e retry scritture disabilitato."""
        if self.client is not None:
            raise RuntimeError("Il client Mongo è già aperto.")
        if not os.getenv("MONGO_USER") or not os.getenv("MONGO_PASSWORD"):
            raise ValueError("Compilare le credenziali Mongo locali in .env e ricreare Jupyter.")
        self.client = MongoClient(
            host=os.getenv("MONGO_HOST", "mongo"), port=int(os.getenv("MONGO_PORT", "27017")),
            username=os.environ["MONGO_USER"], password=os.environ["MONGO_PASSWORD"],
            authSource=os.getenv("MONGO_AUTH_SOURCE", "admin"), maxPoolSize=1,
            retryWrites=False, serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000, socketTimeoutMS=120_000, appname="asdbd-task4")
        try:
            self.client.admin.command("ping")
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        """Chiude il client senza cancellare documenti, indici o volumi."""
        if self.client is not None:
            self.client.close()
            self.client = None

    def create_schema(self) -> None:
        """Crea solo una collezione assente; una collezione presente viene verificata."""
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name, validator=collection_validator(),
                                      validationLevel="strict", validationAction="error")
        self.validate_schema()

    def validate_schema(self) -> None:
        """Verifica validatore, required/null/tipi e policy strict/error senza collMod."""
        info = list(self.db.list_collections(filter={"name": self.collection_name}))
        if (len(info) != 1 or info[0]["type"] != "collection"
                or info[0]["options"].get("validator") != collection_validator()
                or info[0]["options"].get("validationLevel") != "strict"
                or info[0]["options"].get("validationAction") != "error"):
            raise ValueError("Schema/validatore Mongo diverso dal contratto: nessuna modifica automatica.")

    def reset_dataset(self, *, confirm_reset: bool = False) -> None:
        """Ricrea solo la collezione con conferma; DROP/CREATE non sono atomici insieme."""
        if confirm_reset is not True:
            raise PermissionError("Reset Mongo rifiutato: serve confirm_reset=True.")
        self.db.drop_collection(self.collection_name)
        self.create_schema()

    def count_rows(self) -> int:
        """Conta i documenti effettivi, senza estimated_document_count."""
        return self.collection.count_documents({})

    def ingest_batch(self, rows: list[tuple]) -> None:
        """Converte il batch e richiede ack journaled; conserva la diagnostica parziale."""
        documents = [to_document(row) for row in rows]
        try:
            result = self.collection.insert_many(documents, ordered=True)
            if not result.acknowledged:
                raise BatchWriteError("Scrittura Mongo non confermata.", None)
        except BulkWriteError as error:
            details = error.details
            acknowledged = None if details.get("writeConcernErrors") else details.get("nInserted", 0)
            raise BatchWriteError("Batch Mongo interrotto; documenti precedenti all'errore possono persistere.", acknowledged) from error
        except PyMongoError as error:
            raise BatchWriteError("Errore Mongo: esito del batch non determinabile, nessun retry automatico.", None) from error

    def indexes(self) -> list[dict]:
        """Legge i veri indici Mongo, inclusi attributi che renderebbero il confronto diverso."""
        return [{"name": item["name"], "primary": item["name"] == "_id_",
                 "valid": "buildUUID" not in item, "columns": list(item["key"]),
                 "keys": list(item["key"].items()), "partial": "partialFilterExpression" in item,
                 "sparse": item.get("sparse", False), "hidden": item.get("hidden", False),
                 "unique": item.get("unique", item["name"] == "_id_")}
                for item in self.collection.list_indexes()]

    def assert_index_state(self, state: str) -> list[dict]:
        """Richiede solo _id oppure _id + i due indici completi non nascosti previsti."""
        if state not in ("primary_only", "secondary"):
            raise ValueError("Stato indici Mongo non riconosciuto.")
        indexes = self.indexes()
        primary = [i for i in indexes if i["primary"]]
        secondary = [i for i in indexes if not i["primary"]]
        expected = [] if state == "primary_only" else [["distance"], ["origin_airport"]]
        if (len(primary) != 1 or primary[0]["keys"] != [("_id", 1)]
                or sorted(i["columns"] for i in secondary) != expected
                or any(not i["valid"] or i["partial"] or i["sparse"] or i["hidden"] for i in indexes)
                or any(i["unique"] or any(direction != 1 for _, direction in i["keys"]) for i in secondary)):
            raise ValueError(f"Indici Mongo incompatibili con {state}; nessuna eliminazione automatica.")
        return indexes

    def integrity_summary(self) -> dict:
        """Verifica tutti i tipi BSON con $type e calcola conteggi/null/estremi in streaming server.

        _id è univoco per l'indice obbligatorio: non raccogliamo un set di ID in RAM.
        I campi mancanti falliscono l'audit, non vengono contati come BSON null.
        """
        invalid = []
        expected_types = {}
        for name, rule in collection_validator()["$jsonSchema"]["properties"].items():
            allowed = rule["bsonType"] if isinstance(rule["bsonType"], list) else [rule["bsonType"]]
            expected_types[name] = allowed
            invalid.append({"$not": [{"$in": [{"$type": f"${name}"}, allowed]}]})
        group = {"_id": None, "rows": {"$sum": 1}, "minimum": {"$min": "$_id"},
                 "maximum": {"$max": "$_id"},
                 "invalid": {"$sum": {"$cond": [{"$or": invalid}, 1, 0]}}}
        for name in RAW_COLUMNS:
            group[f"null_{name}"] = {"$sum": {"$cond": [{"$eq": [{"$type": f"${name.lower()}"}, "null"]}, 1, 0]}}
        rows = list(self.collection.aggregate([{"$group": group}], maxTimeMS=120_000))
        summary = rows[0] if rows else {"rows": 0, "minimum": None, "maximum": None, "invalid": 0}
        self.last_bson_audit = {"rows_verified": int(summary["rows"]),
                                "invalid_documents": int(summary["invalid"]), "expected_bson_types": expected_types}
        if summary["invalid"]:
            raise ValueError("Audit BSON fallito: tipi errati o campi mancanti nella collezione.")
        return {"rows": int(summary["rows"]),
                "flight_id_min": int(summary["minimum"]) if summary["minimum"] is not None else None,
                "flight_id_max": int(summary["maximum"]) if summary["maximum"] is not None else None,
                "distinct_ids": int(summary["rows"]),
                "null_counts": {name: int(summary.get(f"null_{name}", 0)) for name in RAW_COLUMNS}}

    def _pipeline(self, query_id: str, parameters: dict) -> list[dict]:
        """Traduce Q2/Q3 in conteggi filtrati e Q4 in AVG per tutti i gruppi."""
        if query_id == "Q2":
            return [{"$match": {"origin_airport": parameters["origin_airport"]}}, {"$count": "count"}]
        if query_id == "Q3":
            return [{"$match": {"distance": {"$gte": parameters["distance_min"], "$lte": parameters["distance_max"]}}}, {"$count": "count"}]
        if query_id == "Q4":
            # Manteniamo anche i gruppi in cui tutti i ritardi sono null.
            return [{"$group": {"_id": "$airline", "average": {"$avg": "$departure_delay"}}}, {"$sort": {"_id": 1}}]
        raise ValueError(f"Query Mongo sconosciuta: {query_id}.")

    def run_query(self, query_id: str, parameters: dict):
        """Acquisisce il risultato completo nel formato canonico del contratto comune."""
        if query_id == "Q1":
            return canonical_document(self.collection.find_one({"_id": Int64(parameters["flight_id"])}))
        rows = list(self.collection.aggregate(self._pipeline(query_id, parameters), maxTimeMS=120_000))
        if query_id == "Q4":
            return {row["_id"]: float(row["average"]) if row["average"] is not None else None for row in rows}
        return int(rows[0]["count"]) if rows else 0

    def explain(self, query_id: str, parameters: dict) -> dict:
        """Raccoglie executionStats della stessa operazione, senza hint e fuori dai timer."""
        if query_id == "Q1":
            command = {"find": self.collection_name, "filter": {"_id": Int64(parameters["flight_id"])},
                       "limit": 1, "singleBatch": True}
        else:
            command = {"aggregate": self.collection_name, "pipeline": self._pipeline(query_id, parameters), "cursor": {}}
        response = self.db.command("explain", command, verbosity="executionStats")
        # UUID/timestamp BSON eventualmente presenti nel piano diventano Extended JSON.
        return json.loads(json_util.dumps(response))

    def create_secondary_index(self, column: str) -> None:
        """Crea un indice sincrono; rifiuta indici preesistenti invece di misurare un no-op."""
        if column not in ("origin_airport", "distance"):
            raise ValueError("Indice Mongo non previsto.")
        name = f"{self.collection_name}_{column}_idx"
        if any(item["name"] == name for item in self.indexes()):
            raise ValueError("Indice Mongo già presente: nessuna ricostruzione implicita.")
        self.collection.create_index([(column, ASCENDING)], name=name)

    def drop_secondary_indexes(self, *, confirm_drop: bool = False) -> None:
        """Elimina solo i due indici previsti e soltanto con conferma esplicita."""
        if confirm_drop is not True:
            raise PermissionError("Eliminazione indici Mongo rifiutata: serve confirm_drop=True.")
        owned = {item["name"] for item in self.indexes() if not item["primary"]}
        for column in ("origin_airport", "distance"):
            name = f"{self.collection_name}_{column}_idx"
            if name in owned:
                self.collection.drop_index(name)

    def version(self) -> str:
        """Legge la versione effettiva del server autenticato, senza credenziali."""
        return "MongoDB " + self.client.server_info()["version"]
