"""Configurazione FULL: un motore e una dimensione preparata per esecuzione."""

from dataclasses import asdict, dataclass
from pathlib import Path
import random
import re

import yaml

from src.validation import validate_bundle


def lookup_ids(dataset_size: int, count: int, seed: int) -> list[int]:
    """Sceglie ID validi riproducibili: estremi più campione senza sostituzione."""
    if type(dataset_size) is not int or dataset_size < 1 or type(count) is not int or count < 1:
        raise ValueError("Dimensione dataset e numero ID devono essere interi positivi.")
    count = min(dataset_size, max(2, count))
    if dataset_size == 1:
        return [1]
    return [1, dataset_size] + random.Random(seed).sample(range(2, dataset_size), count - 2)


@dataclass(frozen=True)
class ExperimentConfig:
    """Parametri espliciti e senza credenziali, salvabili nel manifest di run."""

    raw_csv: Path
    bundle_dir: Path
    results_dir: Path
    dataset_size: int
    batch_size: int
    warmup: int
    repetitions: int
    seed: int
    parameters: dict
    q1_ids: list[int]
    confirm_reset: bool
    confirm_drop_indexes: bool
    schema: str = "public"
    table: str = "flights"
    mode: str = "FULL"
    database: str = "postgres"
    mongo_database: str = "flights"
    mongo_collection: str = "flights"
    postgres_snapshot: Path | None = None
    mongo_snapshot: Path | None = None
    cockroach_database: str = "defaultdb"
    cockroach_schema: str = "public"
    cockroach_table: str = "flights"
    cockroach_max_retries: int = 3

    def manifest(self) -> dict:
        """Restituisce solo i parametri ammessi, convertendo i percorsi in stringhe."""
        result = asdict(self)
        for name in ("raw_csv", "bundle_dir", "results_dir"):
            result[name] = str(result[name])
        result["postgres_snapshot"] = str(self.postgres_snapshot) if self.postgres_snapshot else None
        result["mongo_snapshot"] = str(self.mongo_snapshot) if self.mongo_snapshot else None
        from src.databases.factory import create_adapter
        adapter = create_adapter(self)
        result.update(ingestion_method=adapter.ingestion_method, commit_policy=adapter.commit_policy)
        return result


def load_config(path: Path) -> ExperimentConfig:
    """Legge il YAML e rifiuta modalità/dimensioni dei task non ancora implementati."""
    path = Path(path).resolve()
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if (data["database"] not in ("postgres", "mongo", "cockroach") or data["mode"] != "FULL"
            or type(data["dataset_size"]) is not int or data["dataset_size"] not in (100_000, 1_000_000, 5_819_079)):
        raise ValueError("FULL richiede postgres/mongo/cockroach e dimensione 100000, 1000000 o 5819079.")
    for mapping, name, minimum in ((data["ingestion"], "batch_size", 1),
                                   (data["benchmark"], "warmup", 0),
                                   (data["benchmark"], "repetitions", 1)):
        if type(mapping[name]) is not int or mapping[name] < minimum:
            raise ValueError(f"Parametro non valido: {name}.")
    for mapping, name in ((data["ingestion"], "confirm_reset"), (data["benchmark"], "confirm_drop_indexes")):
        if type(mapping[name]) is not bool:
            raise ValueError(f"{name} deve essere un booleano, non una stringa.")
    if type(data["benchmark"]["seed"]) is not int:
        raise ValueError("seed deve essere un intero.")
    params = data["queries"]
    if (not isinstance(params["origin_airport"], str) or not params["origin_airport"]
            or type(params["distance_min"]) is not int or type(params["distance_max"]) is not int
            or params["distance_min"] > params["distance_max"]):
        raise ValueError("Filtri aeroporto/distanza non validi.")
    root = path.parent.parent
    postgres = data.get("postgres", {})
    mongo = data.get("mongo", {})
    mongo_database, mongo_collection = mongo.get("database", "flights"), mongo.get("collection", "flights")
    for name in (mongo_database, mongo_collection):
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Usare nomi Mongo semplici: lettere, numeri e underscore.")
    if mongo_database in ("admin", "local", "config"):
        raise ValueError("Usare un database Mongo dati dedicato.")
    snapshot = data.get("comparison", {}).get("postgres_snapshot")
    mongo_snapshot = data.get("comparison", {}).get("mongo_snapshot")
    cockroach = data.get("cockroach", {})
    for key, default in (("database", "defaultdb"), ("schema", "public"), ("table", "flights")):
        value = cockroach.get(key, default)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) or len(value) > 63:
            raise ValueError(f"Nome CockroachDB non valido: {key}.")
    retries = cockroach.get("max_retries", 3)
    if type(retries) is not int or not 0 <= retries <= 10:
        raise ValueError("cockroach.max_retries deve essere un intero tra 0 e 10.")
    for key in ("schema", "table"):
        value = postgres.get(key, "public" if key == "schema" else "flights")
        if not isinstance(value, str) or not value or "\x00" in value or len(value.encode("utf-8")) > 63:
            raise ValueError(f"Nome PostgreSQL non valido: {key}.")
    return ExperimentConfig(
        raw_csv=root / data["paths"]["raw_csv"],
        bundle_dir=root / data["paths"]["prepared"] / data["preprocessing"]["output_name"],
        results_dir=root / data["paths"]["results"], dataset_size=data["dataset_size"],
        batch_size=data["ingestion"]["batch_size"], warmup=data["benchmark"]["warmup"],
        repetitions=data["benchmark"]["repetitions"], seed=data["benchmark"]["seed"],
        parameters={key: params[key] for key in ("origin_airport", "distance_min", "distance_max")},
        q1_ids=lookup_ids(data["dataset_size"],
            data["benchmark"]["warmup"] + data["benchmark"]["repetitions"], data["benchmark"]["seed"]),
        confirm_reset=data["ingestion"]["confirm_reset"],
        confirm_drop_indexes=data["benchmark"]["confirm_drop_indexes"],
        schema=postgres.get("schema", "public"), table=postgres.get("table", "flights"),
        database=data["database"], mongo_database=mongo_database, mongo_collection=mongo_collection,
        postgres_snapshot=root / snapshot if snapshot else None,
        mongo_snapshot=root / mongo_snapshot if mongo_snapshot else None,
        cockroach_database=cockroach.get("database", "defaultdb"),
        cockroach_schema=cockroach.get("schema", "public"), cockroach_table=cockroach.get("table", "flights"),
        cockroach_max_retries=retries)


def selected_dataset(config: ExperimentConfig) -> tuple[Path, dict]:
    """Verifica il checkpoint Task 2 e seleziona il CSV esistente; non prepara dati."""
    manifest = validate_bundle(config.bundle_dir, config.raw_csv, deep=False)
    for artifact in manifest["artifacts"]:
        if artifact["rows"] == config.dataset_size:
            return config.bundle_dir / artifact["file"], artifact
    raise ValueError("Subset preparato non disponibile: usare il notebook 01 fuori dal benchmark.")
