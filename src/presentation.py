"""Tabelle e grafici didattici dai risultati reali, senza librerie aggiuntive.

SVG resta testo scalabile visualizzabile da Jupyter: ogni asse ha un'unità e
ogni barra una misura. I grafici non uniscono dimensioni o modalità diverse.
"""

import csv
from html import escape
import json
import math
from pathlib import Path

from src.databases.factory import create_adapter
from src.history import read_json
from src.query_validation import assert_equivalent, expected_result


def demo_comparison_html(reference: dict, results: dict) -> str:
    """Mostra atteso/ottenuto e verifica con il contratto Q1-Q4 esistente.

    Non esegue query: riceve soltanto valori acquisiti dalla DEMO. I dettagli
    dei record e delle medie restano espandibili, senza nascondere differenze.
    Una struttura incompleta viene rifiutata anziche mostrata come corretta.
    """
    if set(results) != {"Q1", "Q2", "Q3", "Q4"} or any(not results[q] for q in results):
        raise ValueError("Servono risultati DEMO per tutte le quattro query.")
    rows, details = [], []
    for query in ("Q1", "Q2", "Q3", "Q4"):
        for iteration, observation in enumerate(results[query], 1):
            actual = observation["result"]
            expected = expected_result(reference, query, observation["parameters"])
            try:
                assert_equivalent(query, actual, expected)
                correctness = "OK"
            except (ValueError, TypeError):
                correctness = "DIFFERENTE"
            if isinstance(expected, dict):
                def brief(value):
                    if not isinstance(value, dict):
                        return str(value)
                    return (f"flight_id={value.get('flight_id')}; {len(value)} campi" if query == "Q1"
                            else f"{len(value)} medie per compagnia")
                wanted, obtained = brief(expected), brief(actual)
                keys = list(dict.fromkeys([*expected, *(actual if isinstance(actual, dict) else {})]))
                fields = [{"campo/gruppo": key, "atteso": expected.get(key, "ASSENTE"),
                           "ottenuto": actual.get(key, "ASSENTE") if isinstance(actual, dict) else actual}
                          for key in keys]
                details.append(f'<details><summary>{query}, esecuzione {iteration}: dettagli atteso/ottenuto</summary>'
                               + html_table(fields, ("campo/gruppo", "atteso", "ottenuto")) + '</details>')
            else:
                wanted, obtained = expected, actual
            rows.append({"query": query, "esecuzione": iteration, "atteso": wanted,
                         "ottenuto": obtained, "correttezza": correctness})
    return html_table(rows, ("query", "esecuzione", "atteso", "ottenuto", "correttezza")) + ''.join(details)


def html_table(rows: list[dict], columns: tuple[str, ...]) -> str:
    """Tabella semplice con escaping anche per stringhe provenienti dai risultati."""
    header = "".join(f"<th>{escape(name)}</th>" for name in columns)
    body = "".join("<tr>" + "".join(f"<td>{escape(str(row.get(name, '')))}</td>" for name in columns) + "</tr>" for row in rows)
    return f'<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>'


def query_descriptions(config) -> list[dict]:
    """Mostra il SQL/pipeline dell'adapter approvato, senza aprire connessioni."""
    adapter = create_adapter(config)
    rows = []
    for query in ("Q1", "Q2", "Q3", "Q4"):
        parameters = {**config.parameters, "flight_id": config.q1_ids[0]}
        if config.database == "mongo":
            if query == "Q1":
                text = f'find_one({{"_id": Int64({parameters["flight_id"]})}})'
            else:
                text = json.dumps(adapter._pipeline(query, parameters), indent=2)
            bound = "_id BSON Int64; interi attributi BSON Int32"
        else:
            statement, values = adapter._query(query, parameters)
            text, bound = statement.as_string(), str(values)
        rows.append({"query": query, "operazione": text, "parametri/tipi": bound})
    return rows


def bar_chart(rows: list[dict], title: str, unit: str = "ms") -> str:
    """Crea un grafico SVG per una sola dimensione e una sola modalità.

    Ogni riga richiede label, value, database, dataset_size, mode. Titolo e
    metadati sono espliciti, così tempi DEMO e FULL non diventano una classifica
    involontaria. I valori mancanti non vengono trasformati in zeri.
    """
    if not rows or unit not in ("ms", "s", "record/s"):
        raise ValueError("Grafico privo di dati o unità non riconosciuta.")
    if len({(r["mode"], r["dataset_size"]) for r in rows}) != 1:
        raise ValueError("Separare modalità e dimensioni nei grafici.")
    if any(type(r["value"]) not in (int, float) or not math.isfinite(r["value"]) or r["value"] < 0 for r in rows):
        raise ValueError("Il grafico richiede misure finite non negative.")
    width, left, plot = 980, 320, 520
    height = 100 + 32 * len(rows)
    maximum = max(r["value"] for r in rows) or 1
    mode, size = rows[0]["mode"], rows[0]["dataset_size"]
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img">',
             f'<title>{escape(title)} | {escape(mode)} | {size} record</title>',
             '<rect width="100%" height="100%" fill="white"/>',
             f'<text x="12" y="20" font-size="16">{escape(title)} | {escape(mode)} | {size:,} record</text>']
    colors = {"postgres": "#276594", "mongo": "#2c8756", "cockroach": "#7955a4"}
    for i, row in enumerate(rows):
        y = 40 + i * 32
        bar = row["value"] / maximum * plot
        parts.extend([f'<text x="12" y="{y+17}" font-size="13">{escape(row["label"])}</text>',
                      f'<rect x="{left}" y="{y}" width="{bar:.3f}" height="23" fill="{colors.get(row["database"], "#666")}"/>',
                      f'<text x="{left+bar+5:.3f}" y="{y+17}" font-size="12">{row["value"]:.3f}</text>'])
    bottom = 42 + len(rows) * 32
    parts.append(f'<line x1="{left}" y1="{bottom}" x2="{left+plot}" y2="{bottom}" stroke="black"/>')
    for tick in range(5):
        x = left + plot * tick / 4
        parts.append(f'<text x="{x:.3f}" y="{bottom+18}" font-size="11">{maximum*tick/4:.3f}</text>')
    parts.append(f'<text x="{left}" y="{bottom+37}" font-size="12">{escape(unit)} — misura osservata</text></svg>')
    return "".join(parts)


def historical_charts(rows: list[dict], size: int) -> dict[str, str]:
    """Ingestione e Q1-Q4, comprese Q2/Q3 con/senza indici, per N selezionato."""
    selected = [r for r in rows if r["dataset_size"] == size and r["mode"] == "FULL"]
    charts = {}
    for key, event, queries in (("Ingestione", "ingestion", None),
                                ("Q1", "query", ("Q1",)), ("Q2", "query", ("Q2",)),
                                ("Q3", "query", ("Q3",)), ("Q4", "query", ("Q4",))):
        bars = []
        for row in selected:
            if row["event"] != event or (queries and row["query_id"] not in queries):
                continue
            scenario = "sola PK" if row["index_state"] == "primary_only" else "secondari presenti"
            value = row["mean_ms"] / 1000 if event == "ingestion" else row["mean_ms"]
            bars.append({**row, "label": f'{row["database"]} | {scenario}', "value": value})
        charts[key] = bar_chart(bars, f"Storico {key}", "s" if event == "ingestion" else "ms")
    return charts


def demo_rows(directory: Path) -> list[dict]:
    """Legge solo run DEMO conclusi; rifiuta FULL, errori e prove incomplete."""
    directory = Path(directory)
    manifest = read_json(directory / "manifest.json")
    if manifest["status"] != "completed" or manifest["stage"] != "demo" or manifest["configuration"]["mode"] != "DEMO":
        raise ValueError("Selezionare un run DEMO completato, distinto dagli storici FULL.")
    with (directory / "summary.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    return [{**row, "mode": "DEMO", "database": manifest["configuration"]["database"],
             "dataset_size": manifest["configuration"]["dataset_size"],
             "mean_ms": float(row["mean_ms"]), "valid_repetitions": int(row["valid_repetitions"]),
             "source_run": directory.name} for row in rows]
