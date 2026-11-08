"""Grafici SVG offline: mostrano tutti i punti, senza librerie aggiuntive."""

from html import escape
import math

COLORS = ("#2563eb", "#d97706", "#059669")


def distribution_chart(groups: list[dict], *, temporal: bool = False) -> str:
    """Confronta una query/dimensione/scenario; punti estremi sempre conservati.

    Nel grafico temporale l'ascissa indica l'ordine locale nella sessione:
    osservazioni di DB diversi non sono simultanee o appaiate.
    Nel grafico distributivo baffi=min/max, rettangolo=IQR, linea=mediana.
    """
    if not groups or len({(g['dataset_size'], g['query_id'], g['index_state']) for g in groups}) != 1:
        raise ValueError("Selezionare una sola dimensione, query e scenario.")
    for group in groups:
        if len(group['values_ms']) != 20 or any(not math.isfinite(v) or v < 0 for v in group['values_ms']):
            raise ValueError("Sono necessarie tutte le 20 misure finite.")
    size, query, state = groups[0]['dataset_size'], groups[0]['query_id'], groups[0]['index_state']
    maximum = max(max(g['values_ms']) for g in groups) or 1
    y = lambda value: 320 - 250 * value / maximum
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="410" viewBox="0 0 900 410" role="img">',
             '<rect width="900" height="410" fill="white"/>',
             f'<text x="45" y="25">FULL storico | {query} | N={size:,} | {escape(state)} | n=20 per DB</text>']
    for i in range(6):
        value = maximum * i / 5
        parts.append(f'<path d="M70 {y(value)} H860" stroke="#e5e7eb"/><text x="5" y="{y(value)}">{value:.3g}</text>')
    parts.append('<text x="5" y="50">ms</text>')
    for k, group in enumerate(groups):
        color = COLORS[k % len(COLORS)]
        label = escape(group['database'])
        if temporal:
            points = ' '.join(f'{80 + i * 39},{y(v)}' for i, v in enumerate(group['values_ms']))
            parts.append(f'<polyline points="{points}" fill="none" stroke="{color}"/>')
        else:
            x = 180 + k * 250
            parts.extend([f'<path d="M{x} {y(group["min_ms"])} V{y(group["max_ms"])}" stroke="{color}"/>',
                          f'<rect x="{x-35}" y="{y(group["q3_ms"])}" width="70" height="{y(group["q1_ms"])-y(group["q3_ms"])}" fill="{color}" fill-opacity="0.15" stroke="{color}"/>',
                          f'<path d="M{x-35} {y(group["median_ms"])} H{x+35}" stroke="{color}"/>'])
        for i, value in enumerate(group['values_ms']):
            x = 80 + i * 39 if temporal else 180 + k * 250 + (i % 7 - 3) * 6
            parts.append(f'<circle cx="{x}" cy="{y(value)}" r="3" fill="{color}"><title>{label}: misura {i+1}, {value:.6f} ms</title></circle>')
        parts.append(f'<text x="{80+k*250}" y="355" fill="{color}">{label}</text>')
    caption = ('Ordine locale 1..20: sessioni distinte, punti non appaiati.' if temporal else
               'Baffi min/max; box Q1-Q3; mediana; tutti i 20 punti, nessuna esclusione.')
    parts.append(f'<text x="45" y="390">{caption}</text></svg>')
    return ''.join(parts)


def growth_chart(groups: list[dict], query: str, state: str) -> str:
    """Crescita delle medie su assi logaritmici esplicitamente dichiarati."""
    selected = [g for g in groups if g['query_id'] == query and g['index_state'] == state]
    if not selected or any(g['mean_ms'] <= 0 for g in selected):
        raise ValueError("Medie positive richieste per la scala logaritmica.")
    sizes = sorted({g['dataset_size'] for g in selected})
    if len(sizes) < 2:
        raise ValueError("Servono almeno due dimensioni.")
    low, high = min(g['mean_ms'] for g in selected)/1.2, max(g['mean_ms'] for g in selected)*1.2
    x = lambda n: 90 + 700 * math.log(n/sizes[0]) / math.log(sizes[-1]/sizes[0])
    y = lambda t: 300 - 220 * math.log(t/low) / math.log(high/low)
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="410" role="img">',
             '<rect width="900" height="410" fill="white"/>',
             f'<text x="40" y="25">Medie FULL storiche | {escape(query)} {escape(state)} | entrambi gli assi logaritmici</text>']
    for i in range(5):
        value = low * (high/low)**(i/4)
        parts.append(f'<path d="M90 {y(value)} H800" stroke="#e5e7eb"/><text x="5" y="{y(value)}">{value:.3g} ms</text>')
    for size in sizes:
        parts.append(f'<text x="{x(size)-30}" y="330">{size:,}</text>')
    for k, db in enumerate(('postgres', 'mongo', 'cockroach')):
        rows = sorted((g for g in selected if g['database'] == db), key=lambda g:g['dataset_size'])
        points = ' '.join(f'{x(g["dataset_size"])},{y(g["mean_ms"])}' for g in rows)
        color = COLORS[k]
        parts.append(f'<polyline points="{points}" fill="none" stroke="{color}"/>')
        for g in rows:
            parts.append(f'<circle cx="{x(g["dataset_size"])}" cy="{y(g["mean_ms"])}" r="4" fill="{color}"><title>{db}: {g["mean_ms"]:.6f} ms</title></circle>')
        parts.append(f'<text x="{90+k*240}" y="365" fill="{color}">{db}</text>')
    parts.append('<text x="40" y="395">N record; segmenti guida alla lettura, non un modello stimato. Dispersione nelle tabelle/boxplot.</text></svg>')
    return ''.join(parts)
