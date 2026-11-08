"""Analisi descrittiva offline delle campagne concluse, senza accesso ai DB.

Nessun test inferenziale: le ripetizioni sequenziali non sono repliche
indipendenti dell'intero esperimento. Non eliminiamo osservazioni estreme.
Gli output derivati sono separati e non riscrivono gli artefatti approvati.
"""

import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import statistics
from uuid import uuid4

import yaml

from src.history import read_experiment, run_path
from src.query_validation import assert_equivalent, expected_result
from src.results import result_digest
from src.validation import sha256_file


SIZES = (100_000, 1_000_000, 5_819_079)
DATABASES = ("postgres", "mongo", "cockroach")


def load_analysis_catalog(path: Path) -> tuple[dict[int, Path], Path]:
    """Legge solo il catalogo e la radice risultati; non importa il percorso DEMO live."""
    path = Path(path).resolve()
    root = path.parent.parent
    catalog = yaml.safe_load(path.read_text(encoding="utf-8"))["campaigns"]
    if set(catalog) != set(SIZES):
        raise ValueError("L'analisi richiede le tre campagne approvate.")
    config = yaml.safe_load((root / "config/benchmark.yaml").read_text(encoding="utf-8"))
    return {size: root / folder for size, folder in catalog.items()}, root / config["paths"]["results"]


def quantile(values: list[float], probability: float) -> float:
    """Quantile con interpolazione lineare: posizione (n-1)*p, senza assunzioni distributive."""
    if not values or not 0 <= probability <= 1:
        raise ValueError("Quantile: dati mancanti o probabilità non valida.")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    fraction = position - lower
    return ordered[lower] + fraction * (ordered[min(lower + 1, len(ordered) - 1)] - ordered[lower])


def lag_one(values: list[float]) -> float | None:
    """Correlazione descrittiva fra x[0:n-1] e x[1:n]; non un test di indipendenza."""
    if len(values) < 3:
        return None
    left, right = values[:-1], values[1:]
    a, b = statistics.mean(left), statistics.mean(right)
    numerator = sum((x - a) * (y - b) for x, y in zip(left, right))
    denominator = math.sqrt(sum((x - a)**2 for x in left) * sum((y - b)**2 for y in right))
    return max(-1.0, min(1.0, numerator / denominator)) if denominator else None


def describe(values: list[float]) -> dict:
    """Statistiche di tutte le osservazioni: s usa n-1, nessun filtraggio.

    I flag di Tukey identificano solo punti oltre Q1-1.5*IQR/Q3+1.5*IQR.
    Non attestano errori e non causano esclusioni. Per n=1 la dispersione
    campionaria resta non stimabile, anziché diventare zero.
    """
    if not values or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Richieste osservazioni finite non negative.")
    mean, median = statistics.mean(values), statistics.median(values)
    sd = statistics.stdev(values) if len(values) > 1 else None
    q1, q3 = quantile(values, 0.25), quantile(values, 0.75)
    iqr = q3 - q1
    split = len(values) // 2
    return {"n": len(values), "mean_ms": mean, "median_ms": median, "sample_stddev_ms": sd,
            "min_ms": min(values), "max_ms": max(values), "range_ms": max(values) - min(values),
            "q1_ms": q1, "q3_ms": q3, "iqr_ms": iqr,
            "mad_ms": statistics.median(abs(v - median) for v in values),
            "cv_pct": 100 * sd / mean if sd is not None and mean else None,
            "tukey_flags": sum(v < q1 - 1.5 * iqr or v > q3 + 1.5 * iqr for v in values),
            "excluded_observations": 0, "lag1_descriptive": lag_one(values),
            "first_half_mean_ms": statistics.mean(values[:split]) if split else None,
            "last_half_mean_ms": statistics.mean(values[split:]) if split else None}


def index_benefit(before_ms: float, after_ms: float, build_ms: float) -> dict:
    """Modello descrittivo di ammortamento di UN indice per UNA query.

    n*=ceil(build/(before-after)). Presuppone latenze future uguali alle medie
    osservate; esclude aggiornamenti, spazio, manutenzione e cambi delle cache.
    Non è una previsione di workload né una stima con intervallo di confidenza.
    """
    for value in (before_ms, after_ms, build_ms):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("Costo/tempo non valido.")
    saved = before_ms - after_ms
    ratio = build_ms / saved if saved > 0 else None
    return {"before_mean_ms": before_ms, "after_mean_ms": after_ms, "build_ms_single_event": build_ms,
            "saved_ms_per_query": saved, "speedup": before_ms / after_ms if after_ms else None,
            "reduction_pct": 100 * saved / before_ms if before_ms else None,
            "build_over_saving_queries": ratio,
            "break_even_queries_hypothetical": math.ceil(ratio) if ratio is not None else None}


def full_indexed_comparison(postgres: dict, mongo: dict) -> dict:
    """Confronto descrittivo non appaiato, senza SEM, IC o p-value.

    Le iterazioni dei due DB avvengono in sessioni distinte: non abbiniamo i
    punti con lo stesso numero di iterazione. Le 400 coppie sono ricombinazioni
    degli stessi 40 punti, NON 400 nuove osservazioni indipendenti.
    """
    p, m = postgres, mongo
    delta = p["mean_ms"] - m["mean_ms"]
    spread = math.sqrt((p["sample_stddev_ms"]**2 + m["sample_stddev_ms"]**2) / 2)
    low, high = max(p["min_ms"], m["min_ms"]), min(p["max_ms"], m["max_ms"])
    pairs = [(a, b) for a in p["values_ms"] for b in m["values_ms"]]
    return {"query_id": p["query_id"], "pg_mean_ms": p["mean_ms"], "mongo_mean_ms": m["mean_ms"],
            "pg_median_ms": p["median_ms"], "mongo_median_ms": m["median_ms"],
            "pg_sd_ms": p["sample_stddev_ms"], "mongo_sd_ms": m["sample_stddev_ms"],
            "pg_minus_mongo_ms": delta, "pg_over_mongo_pct": 100 * delta / m["mean_ms"] if m["mean_ms"] else None,
            "rms_within_session_sd_ms": spread,
            "delta_over_rms_sd_descriptive": delta / spread if spread else None,
            "range_overlap_ms": max(0, high - low),
            "iqr_overlap_ms": max(0, min(p["q3_ms"], m["q3_ms"]) - max(p["q1_ms"], m["q1_ms"])),
            "recombined_pairs": len(pairs),
            "fraction_mongo_lower_descriptive": sum(b < a for a, b in pairs) / len(pairs),
            "inferential_conclusion": "non formulata: sessioni sequenziali, indipendenza non garantita"}


def query_observations(history: dict, database: str, size: int, results_root: Path) -> tuple[list[dict], dict]:
    """Legge i raw originali in ordine, esclude solo i warm-up e ricontrolla i CSV."""
    config, reference = history["configuration"], history["reference"]
    if (config["warmup"], config["repetitions"]) != (3, 20):
        raise ValueError("Le campagne approvate devono avere 3 warm-up e 20 misure.")
    groups, hashes = [], {}
    for phase, index_state, queries in (("baseline", "primary_only", ("Q1", "Q2", "Q3", "Q4")),
                                         ("indexed", "secondary", ("Q2", "Q3"))):
        manifest = history["manifests"][phase]
        path = run_path(manifest["run_id"], results_root) / "raw.jsonl"
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        hashes[str(path)] = sha256_file(path)
        expected = [(q, i, i < 3) for q in queries for i in range(23)]
        if [(r["query_id"], r["iteration"], r["warmup"]) for r in rows] != expected:
            raise ValueError("Sequenza raw incompleta, duplicata o con warm-up non coerenti.")
        for row in rows:
            if (row["event"] != "query" or row["status"] != "ok" or row["mode"] != "FULL"
                    or row["run_id"] != manifest["run_id"] or row["database"] != database
                    or row["dataset_size"] != size or row["index_state"] != index_state
                    or row["dataset_sha256"] != reference["dataset_sha256"]
                    or type(row["warmup"]) is not bool or type(row["iteration"]) is not int):
                raise ValueError("Identità o stato delle osservazioni raw non valido.")
            parameters = dict(reference["parameters"])
            if row["query_id"] == "Q1":
                parameters["flight_id"] = reference["q1_ids"][row["iteration"]]
            if row["parameters"] != parameters:
                raise ValueError("Parametri raw diversi dal riferimento.")
            digest = row["result_digest"]
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("Digest raw mancante o non valido.")
            # Q4 usa tolleranza numerica: un digest diverso non dimostra un errore.
            if row["query_id"] != "Q4" and digest != result_digest(expected_result(reference, row["query_id"], parameters)):
                raise ValueError("Digest del risultato raw non coerente con il riferimento.")
            describe([row["duration_ms"]])  # controlla anche i warm-up, senza aggregarli
        for query in queries:
            observations = [r for r in rows if r["query_id"] == query and not r["warmup"]]
            values = [r["duration_ms"] for r in observations]
            stats = describe(values)
            saved = next(r for r in history["summaries"] if r["event"] == "query"
                         and r["query_id"] == query and r["index_state"] == index_state)
            for key in ("mean_ms", "median_ms", "sample_stddev_ms", "min_ms", "max_ms"):
                if not math.isclose(stats[key], saved[key], rel_tol=1e-12, abs_tol=1e-12):
                    raise ValueError(f"Riepilogo diverso dai raw: {database}/{size}/{query}/{key}.")
            matches = reference[query] if query in ("Q2", "Q3") else None
            groups.append({"database": database, "dataset_size": size, "query_id": query,
                           "index_state": index_state, "source_run": manifest["run_id"],
                           "category": {"Q1": "lookup PK", "Q4": "aggregazione"}.get(query,
                                       "conteggio indicizzato" if index_state == "secondary" else "scansione filtrata"),
                           **stats, "matching_rows": matches,
                           "selectivity_pct": 100 * matches / size if matches is not None else None,
                           "values_ms": values, "iterations": [r["iteration"] for r in observations],
                           "timestamps_utc": [r["timestamp_utc"] for r in observations]})
    return groups, hashes


def growth_table(groups: list[dict]) -> list[dict]:
    """Rapporti osservati: N, tempo e conteggi selezionati restano distinti."""
    result = []
    lookup = {(r["database"], r["dataset_size"], r["query_id"], r["index_state"]): r for r in groups}
    for db in DATABASES:
        for query, state in (("Q1", "primary_only"), ("Q2", "primary_only"), ("Q3", "primary_only"),
                             ("Q4", "primary_only"), ("Q2", "secondary"), ("Q3", "secondary")):
            for small, large in ((SIZES[0], SIZES[1]), (SIZES[1], SIZES[2]), (SIZES[0], SIZES[2])):
                a, b = lookup[db, small, query, state], lookup[db, large, query, state]
                n_ratio = large / small
                ratio = b["mean_ms"] / a["mean_ms"] if a["mean_ms"] else None
                matches = b["matching_rows"] / a["matching_rows"] if a["matching_rows"] else None
                result.append({"database": db, "query_id": query, "index_state": state,
                               "from_size": small, "to_size": large, "row_ratio": n_ratio,
                               "mean_time_ratio": ratio, "time_ratio_over_row_ratio": ratio / n_ratio if ratio is not None else None,
                               "matching_row_ratio": matches,
                               "time_ratio_over_matching_ratio": ratio / matches if matches and ratio is not None else None,
                               "from_selectivity_pct": a["selectivity_pct"], "to_selectivity_pct": b["selectivity_pct"]})
    return result


def analyze_campaigns(campaigns: dict[int, Path], results_root: Path) -> dict:
    """Unica analisi offline delle nove combinazioni; nessun percorso di esecuzione DB."""
    if set(campaigns) != set(SIZES):
        raise ValueError("Servono le tre dimensioni approvate.")
    groups, benefits, single, selection, hashes = [], [], [], [], {}
    common = None
    for size in SIZES:
        first = None
        for database in DATABASES:
            history = read_experiment(campaigns[size], database, size, results_root)
            config, reference = history["configuration"], history["reference"]
            parameters = {k: config[k] for k in ("parameters", "seed", "warmup", "repetitions", "batch_size")}
            if common is None:
                common = parameters
            elif common != parameters:
                raise ValueError("Protocollo sperimentale non omogeneo fra campagne.")
            if first is None:
                first = reference
            else:
                if (first["dataset_sha256"] != reference["dataset_sha256"] or first["integrity"] != reference["integrity"]
                        or first["q1_ids"] != reference["q1_ids"]):
                    raise ValueError("Riferimenti non omogenei fra database.")
                for q in ("Q2", "Q3", "Q4"):
                    assert_equivalent(q, reference[q], first[q])
                for key in first["q1_ids"]:
                    assert_equivalent("Q1", reference["q1_records"][str(key)], first["q1_records"][str(key)])
            stats, raw_hashes = query_observations(history, database, size, results_root)
            groups.extend(stats)
            hashes.update(history["source_hashes"])
            hashes.update(raw_hashes)
            for row in history["summaries"]:
                if row["event"] != "query":
                    single.append({**row, "n": 1, "sample_stddev_ms": None,
                                   "uncertainty": "non stimabile: singolo evento"})
            for query, column in (("Q2", "origin_airport"), ("Q3", "distance")):
                before = next(s for s in stats if s["query_id"] == query and s["index_state"] == "primary_only")
                after = next(s for s in stats if s["query_id"] == query and s["index_state"] == "secondary")
                build = next(r["mean_ms"] for r in history["summaries"] if r["event"] == "create_index" and r["query_id"] == column)
                benefits.append({"database": database, "dataset_size": size, "query_id": query, "index_column": column,
                                 **index_benefit(before["mean_ms"], after["mean_ms"], build)})
        selection.append({"dataset_size": size, "Q2_rows": first["Q2"], "Q2_selectivity_pct": 100 * first["Q2"] / size,
                          "Q3_rows": first["Q3"], "Q3_selectivity_pct": 100 * first["Q3"] / size, "Q4_groups": len(first["Q4"])})
    compared = []
    for query in ("Q2", "Q3"):
        def find(db):
            return next(g for g in groups if g["database"] == db and g["dataset_size"] == SIZES[-1]
                        and g["query_id"] == query and g["index_state"] == "secondary")
        compared.append(full_indexed_comparison(find("postgres"), find("mongo")))
    return {"queries": groups, "index_benefits": benefits, "growth": growth_table(groups),
            "selection": selection, "single_events": single, "pg_mongo_full": compared,
            "source_hashes": hashes, "results_root": str(Path(results_root).resolve()),
            "methods": {"quantiles": "linear interpolation at (n-1)*p", "sd": "sample formula n-1",
                        "discarded_outliers": 0, "inferential_tests": False,
                        "limits": ["20 misure sequenziali per sessione, non repliche indipendenti dell'esperimento",
                                   "Ingestione e costruzione indici misurate una sola volta per combinazione",
                                   "Nessun IC/p-value/SEM: dipendenza temporale e variabilità fra sessioni non stimate",
                                   "100K preliminare funzionale approvato; 1M e FULL campagne successive",
                                   "Cache non svuotate, ordine fisso, subset prefissi con selettività variabile",
                                   "Ammortamento ipotetico: esclusi manutenzione, spazio, scritture e variazioni cache"]}}


def export_analysis(analysis: dict) -> Path:
    """Pubblica solo nuovi derivati, in directory unica separata da ogni sorgente."""
    for source, digest in analysis["source_hashes"].items():
        if sha256_file(Path(source)) != digest:
            raise ValueError("Sorgenti mutate durante l'analisi: esportazione rifiutata.")
    directory = Path(analysis["results_root"]) / "statistical_analysis" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex)
    if any(directory.resolve().is_relative_to(Path(p).parent.resolve()) for p in analysis["source_hashes"]):
        raise ValueError("I derivati non possono essere scritti dentro una campagna approvata.")
    directory.mkdir(parents=True, exist_ok=False)
    for name in ("queries", "index_benefits", "growth", "selection", "single_events", "pg_mongo_full"):
        rows = [{k: v for k, v in r.items() if k not in ("values_ms", "iterations", "timestamps_utc")} for r in analysis[name]]
        with (directory / f"{name}.csv").open("w", encoding="utf-8", newline="") as stream:
            fields = list(dict.fromkeys(k for row in rows for k in row))
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    (directory / "analysis.json").write_text(json.dumps(analysis, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return directory
