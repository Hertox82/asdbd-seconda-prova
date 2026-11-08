"""DEMO sicura dei dati persistenti; nessun percorso di ingestione o ripristino.

Gli attesi e i piani provengono da campagne concluse e vengono solo letti.
I risultati nuovi finiscono in results/demo/<UUID>, sempre con mode=DEMO.
"""

from dataclasses import dataclass, replace
from pathlib import Path
import warnings

import yaml

from src.benchmark import measure_queries
from src.config import ExperimentConfig, load_config
from src.databases.factory import create_adapter
from src.history import read_experiment
from src.query_validation import assert_equivalent
from src.results import ResultRun, environment_context
from src.runner import assert_single_database
from src.validation import sha256_file


SIZES = (100_000, 1_000_000, 5_819_079)


@dataclass(frozen=True)
class DemoSettings:
    """Selezione didattica separata dai parametri delle campagne concluse."""
    experiment: ExperimentConfig
    campaigns: dict[int, Path]

    @property
    def campaign(self) -> Path:
        return self.campaigns[self.experiment.dataset_size]


def load_demo_settings(path: Path) -> DemoSettings:
    """Legge selezione DEMO e demo_repetitions, senza toccare YAML o dati storici."""
    path = Path(path).resolve()
    root = path.parent.parent
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    base = load_config(root / "config/benchmark.yaml")
    repetitions = yaml.safe_load((root / "config/benchmark.yaml").read_text(encoding="utf-8"))["benchmark"]["demo_repetitions"]
    if (data.get("mode") != "DEMO" or data.get("database") not in ("postgres", "mongo", "cockroach")
            or type(data.get("dataset_size")) is not int or data["dataset_size"] not in SIZES):
        raise ValueError("DEMO richiede un database e una dimensione canonica espliciti.")
    if type(repetitions) is not int or not 1 <= repetitions <= 10:
        raise ValueError("demo_repetitions deve essere un intero da 1 a 10 per una prova leggera.")
    if base.confirm_reset is not False or base.confirm_drop_indexes is not False:
        raise PermissionError("DEMO rifiutata: riportare entrambe le conferme distruttive a false.")
    catalog = data.get("campaigns", {})
    if set(catalog) != set(SIZES) or any(not isinstance(v, str) or not v for v in catalog.values()):
        raise ValueError("Specificare le tre campagne approvate nel catalogo DEMO.")
    return DemoSettings(replace(base, mode="DEMO", database=data["database"], dataset_size=data["dataset_size"],
                                warmup=0, repetitions=repetitions),
                        {size: root / value for size, value in catalog.items()})


class ReadOnlyAdapter:
    """Facciata con sola lettura e quattro query ammesse, senza delega generica.

    SQL aggiunge una difesa server di sessione read-only. Mongo usa le
    credenziali esistenti e il perimetro delle API: non crea utenti/ruoli.
    La facciata non è un sandbox contro codice Python arbitrario nel notebook.
    """

    def __init__(self, adapter):
        self._adapter = adapter
        self.name = adapter.name

    def connect(self) -> None:
        self._adapter.connect()
        if self.name in ("postgres", "cockroach"):
            self._adapter.db.execute("SET default_transaction_read_only = on")
            actual = self._adapter.db.execute("SHOW default_transaction_read_only").fetchone()[0]
            if str(actual).lower() not in ("on", "true"):
                raise RuntimeError("Sessione SQL non confermata in sola lettura.")

    def close(self) -> None:
        self._adapter.close()

    def version(self) -> str:
        return self._adapter.version()

    def validate_schema(self) -> None:
        self._adapter.validate_schema()

    def assert_index_state(self, state: str) -> list[dict]:
        return self._adapter.assert_index_state(state)

    def count_rows(self) -> int:
        return self._adapter.count_rows()

    def run_query(self, query_id: str, parameters: dict):
        if query_id not in ("Q1", "Q2", "Q3", "Q4"):
            raise PermissionError("La DEMO permette esclusivamente Q1-Q4.")
        return self._adapter.run_query(query_id, parameters)

    def __getattr__(self, name):
        if name in ("create_schema", "reset_dataset", "ingest_batch", "create_secondary_index",
                    "drop_secondary_indexes", "analyze", "explain", "integrity_summary", "db", "collection"):
            raise PermissionError(f"Operazione non disponibile nella DEMO: {name}.")
        raise AttributeError(name)


def demo_configuration(config: ExperimentConfig, history: dict) -> ExperimentConfig:
    """Deriva solo la destinazione storica corretta, senza scegliere l'ultimo DB popolato."""
    if (config.mode != "DEMO" or config.dataset_size not in SIZES
            or type(config.repetitions) is not int or not 1 <= config.repetitions <= 10
            or config.warmup != 0 or config.confirm_reset is not False or config.confirm_drop_indexes is not False):
        raise PermissionError("Configurazione DEMO non sicura: nessuna operazione eseguita.")
    source = history["configuration"]
    if (source["database"] != config.database or source["dataset_size"] != config.dataset_size
            or source["parameters"] != config.parameters or source["seed"] != config.seed):
        raise ValueError("Filtri, seed o selezione diversi dalla campagna approvata.")
    ids = history["reference"]["q1_ids"][:config.repetitions]
    if len(ids) != config.repetitions:
        raise ValueError("Campioni Q1 storici insufficienti per la DEMO.")
    names = ("schema", "table", "mongo_database", "mongo_collection", "cockroach_database",
             "cockroach_schema", "cockroach_table")
    return replace(config, q1_ids=ids, **{name: source[name] for name in names})


def run_demo(settings: DemoSettings) -> Path:
    """Verifica minima, tre query reali per tipo e nuovo run DEMO isolato.

    Nessuna lettura dei grandi CSV, audit BSON integrale o nuovo EXPLAIN.
    I piani consultabili sono quelli storici, esplicitamente etichettati.
    I conteggi NULL integrali restano certificati dalla campagna di origine.
    """
    base = settings.experiment
    # Controllare la modalità prima ancora di aprire un artefatto o una connessione.
    if base.mode != "DEMO" or base.confirm_reset is not False or base.confirm_drop_indexes is not False:
        raise PermissionError("La DEMO non accetta FULL o conferme distruttive.")
    history = read_experiment(settings.campaign, base.database, base.dataset_size, base.results_dir)
    config = demo_configuration(base, history)
    reference = {**history["reference"], "q1_ids": config.q1_ids}
    adapter = ReadOnlyAdapter(create_adapter(config))
    with ResultRun(config.results_dir / "demo", config.manifest(), reference, stage="demo", durable=True) as run:
        run.manifest["method_notes"].extend([
            "DEMO: zero warm-up, poche ripetizioni, nessuna ingestione/DDL/ANALYZE o ricostruzione.",
            "Controllo minimo: schema, indici, count esatto, campioni Q1 e risultati Q1-Q4; non audit NULL/BSON integrale.",
            "Piani e baseline senza secondari sono storici FULL, non nuove esecuzioni DEMO.",
            "Tempi DEMO separati dai benchmark FULL: campione ridotto e stato/cache differenti."])
        run.save_json("source.json", {"campaign": str(settings.campaign), "source_hashes": history["source_hashes"],
                                      "snapshot_version": history["snapshot"]["database_version"]})
        try:
            assert_single_database(config.database)
            adapter.connect()
            version = adapter.version()
            run.set_database_version(version)
            run.save_json("environment.json", environment_context(version))
            if version != history["snapshot"]["database_version"]:
                message = "Versione DB diversa dalla campagna storica; risultati nuovi solo dimostrativi."
                warnings.warn(message, RuntimeWarning, stacklevel=2)
                run.manifest["environment_warning"] = message
            try:
                adapter.validate_schema()
            except ValueError as error:
                raise RuntimeError("Destinazione DEMO assente o schema non conforme: verificare database, "
                                   "campagna e dati persistenti. Nessun setup o ripristino automatico.") from error
            index_state = history["snapshot"]["validation"]["index_state"]
            indexes = adapter.assert_index_state(index_state)
            count = adapter.count_rows()
            if count != config.dataset_size:
                raise ValueError(f"DB non pronto: attesi {config.dataset_size} record, trovati {count}. Nessun caricamento automatico.")
            for flight_id in config.q1_ids:
                actual = adapter.run_query("Q1", {**config.parameters, "flight_id": flight_id})
                assert_equivalent("Q1", actual, reference["q1_records"][str(flight_id)])
            run.save_json("readiness.json", {"status": "passed", "rows": count, "indexes": indexes,
                                             "q1_ids": config.q1_ids, "scope": "minimal_demo_not_full_audit"})
            values = measure_queries(adapter, reference, run, index_state=index_state,
                                     query_ids=("Q1", "Q2", "Q3", "Q4"), warmup=0,
                                     repetitions=config.repetitions, capture_results=True)
            run.save_json("query_results.json", values)
            run.save_json("historical_plans.json", {"origin": "FULL historical, not recomputed", "plans": history["plans"]})
            run.save_json("historical_summary.json", history["summaries"])
            if any(sha256_file(Path(path)) != digest for path, digest in history["source_hashes"].items()):
                raise ValueError("Artefatti storici cambiati durante la lettura DEMO.")
        finally:
            adapter.close()
    return run.directory
