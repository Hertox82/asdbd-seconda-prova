"""Controlli dei notebook senza DB, caricamenti o misure di benchmark.

Le celle vengono eseguite nell'ordine salvato con le API operative sostituite
da sentinelle: anche una chiamata accidentale causa subito il fallimento.
"""

import ast
from dataclasses import replace
import json
from pathlib import Path
import re
import socket

import nbformat
import pytest

from src.config import load_config
from src.presentation import demo_comparison_html

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = sorted((ROOT / 'notebooks').glob('*.ipynb'))


def forbidden(*args, **kwargs):
    raise AssertionError('Operazione non ammessa nella verifica offline dei notebook')


@pytest.fixture
def offline(monkeypatch):
    """Niente rete, scritture applicative, query o scansioni del CSV."""
    import IPython.display
    import psycopg
    import src.config
    import src.preprocessing
    import src.query_validation
    import src.ingestion
    import src.benchmark
    import src.demo
    import src.runner
    import src.results
    from src.databases.postgresql import PostgreSQLAdapter
    from src.databases.mongodb import MongoDBAdapter
    from src.databases.cockroachdb import CockroachDBAdapter
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(psycopg, 'connect', forbidden)
    for adapter in (PostgreSQLAdapter, MongoDBAdapter, CockroachDBAdapter):
        monkeypatch.setattr(adapter, 'connect', forbidden)
    for module, names in (
        (src.config, ('selected_dataset',)),
        (src.preprocessing, ('acquire_source', 'prepare_dataset')),
        (src.query_validation, ('build_reference', 'capture_database_snapshot')),
        (src.ingestion, ('ingest_csv',)),
        (src.benchmark, ('benchmark_phase', 'measure_secondary_indexes')),
        (src.demo, ('run_demo',)),
        (src.runner, ('run_full', 'unlock_campaign')),
        (src.results, ('ResultRun',)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, forbidden)
    outputs = []
    monkeypatch.setattr(IPython.display, 'display', outputs.append)
    return outputs


def run_cells(path, overrides=None):
    """Ogni esecuzione usa uno spazio nuovo, come Restart Kernel + Run All."""
    namespace = {'__name__': '__main__'}
    from IPython.display import display
    namespace['display'] = display
    for index, cell in enumerate(json.loads(path.read_text(encoding='utf-8'))['cells']):
        if cell['cell_type'] == 'code':
            source = ''.join(cell['source'])
            if overrides:
                for old, new in overrides.items():
                    source = source.replace(old, new)
            exec(compile(source, f'{path.name}:cella-{index}', 'exec'), namespace)
    return namespace


@pytest.mark.parametrize('path', NOTEBOOKS, ids=lambda p:p.name)
def test_notebook_structure_kernel_and_saved_outputs(path):
    """Gli output sono documentazione intenzionale: validarli, senza cancellarli."""
    # json.loads rifiuta JSON malformato prima della lettura nbformat.
    serialized = json.loads(path.read_text(encoding='utf-8'))
    assert serialized['nbformat'] == 4
    notebook = nbformat.read(path, as_version=4)
    nbformat.validate(notebook)
    assert notebook.metadata.kernelspec.name == 'python3'
    assert notebook.metadata.kernelspec.language == 'python'
    assert notebook.metadata.kernelspec.display_name.strip()
    assert notebook.metadata.language_info.name == 'python'
    assert len({c.id for c in notebook.cells}) == len(notebook.cells)
    for cell in notebook.cells:
        if cell.cell_type == 'code':
            ast.parse(cell.source)
            # nbformat verifica anche tipi di output ed execution_count salvati.
        else:
            assert not re.search(r'\btask\s+\d|code review|non sono ancora implement|task successivi', cell.source, re.I)


@pytest.mark.parametrize('name,flag', [
    ('01_dataset_preparation.ipynb', 'ABILITA_PREPARAZIONE'),
    ('02_ingestion.ipynb', 'ABILITA_INGESTIONE'),
    ('03_query_benchmarks.ipynb', 'ABILITA_BENCHMARK'),
    ('04_full_runner.ipynb', 'ESEGUI'),
    ('05_demo.ipynb', 'ESEGUI_DEMO'),
])
def test_execution_flags_disabled_without_executing_cells(name, flag):
    """Controllo statico: nessuna cella viene eseguita per verificare i default."""
    notebook = json.loads((ROOT/'notebooks'/name).read_text(encoding='utf-8'))
    assignments = []
    for cell in notebook['cells']:
        if cell['cell_type'] != 'code':
            continue
        for node in ast.walk(ast.parse(''.join(cell['source']))):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
            if any(isinstance(target, ast.Name) and target.id == flag for target in targets):
                assignments.append(node.value)
    assert assignments, f'Flag {flag} assente in {name}'
    assert all(isinstance(value, ast.Constant) and value.value is False for value in assignments), (
        f'{flag} deve rimanere False in tutte le assegnazioni salvate in {name}')


@pytest.mark.parametrize('path', NOTEBOOKS, ids=lambda p:p.name)
def test_run_all_defaults_offline(path, offline):
    """In ordine reale: nessuna operazione vietata e nessun NameError a valle."""
    run_cells(path)


@pytest.mark.parametrize('name,flag', [
    ('02_ingestion.ipynb', 'ABILITA_INGESTIONE'),
    ('03_query_benchmarks.ipynb', 'ABILITA_BENCHMARK'),
])
@pytest.mark.parametrize('size', [1_000_000, 5_819_079])
@pytest.mark.parametrize('enabled', [False, True])
def test_manual_large_size_requires_enabled_guard(name, flag, size, enabled, offline, monkeypatch):
    import src.config
    actual = load_config(ROOT/'config/benchmark.yaml')
    monkeypatch.setattr(src.config, 'load_config', lambda path:replace(actual, dataset_size=size))
    if enabled:
        with pytest.raises(ValueError, match='100.000 record'):
            run_cells(ROOT/'notebooks'/name, {f'{flag} = False':f'{flag} = True'})
    else:
        # Run All deve terminare anche con 1M/FULL selezionati; le sentinelle
        # della fixture offline impediscono qualsiasi operazione accidentale.
        namespace = run_cells(ROOT/'notebooks'/name)
        assert namespace[flag] is False


@pytest.mark.parametrize('name', ['02_ingestion.ipynb', '03_query_benchmarks.ipynb'])
def test_module_reset_flags_cannot_bypass_notebook_gate(name, offline, monkeypatch):
    import src.config
    actual = load_config(ROOT/'config/benchmark.yaml')
    monkeypatch.setattr(src.config, 'load_config', lambda path:replace(actual, confirm_reset=True, confirm_drop_indexes=True))
    run_cells(ROOT/'notebooks'/name)


def test_all_27_historical_charts_available_offline(offline):
    run_cells(ROOT/'notebooks/06_results_analysis.ipynb',
              {'MOSTRA_APPROFONDIMENTI = False':'MOSTRA_APPROFONDIMENTI = True'})
    from IPython.display import SVG
    from xml.etree import ElementTree
    charts = [out for out in offline if isinstance(out, SVG)]
    assert len(charts) == 27
    for chart in charts:
        ElementTree.fromstring(chart.data)


def comparison_fixture():
    reference = {'q1_records': {'1': {'flight_id': 1, 'AIRLINE':'<AA>', 'NULL':None}},
                 'Q2':7, 'Q3':9, 'Q4':{'AA':1.0,'BB':None}}
    results = {q:[{'parameters':{'flight_id':1},
                   'result':reference['q1_records']['1'] if q=='Q1' else reference[q]}]
               for q in ('Q1','Q2','Q3','Q4')}
    return reference, results


def test_demo_comparison_values_nulls_details_and_escaping():
    reference, results = comparison_fixture()
    html = demo_comparison_html(reference, results)
    assert html.count('<td>OK</td>') == 4
    assert '&lt;AA&gt;' in html and '<AA>' not in html
    assert 'flight_id' in html and 'None' in html and '<td>7</td>' in html
    assert html.count('<details>') == 2


@pytest.mark.parametrize('query,actual', [('Q1',{'flight_id':True}),('Q2',8),('Q3',True),('Q4',{'AA':2.0,'BB':None})])
def test_demo_comparison_reports_difference(query, actual):
    reference, results = comparison_fixture()
    results[query][0]['result'] = actual
    assert demo_comparison_html(reference, results).count('<td>DIFFERENTE</td>') == 1


def test_demo_comparison_uses_approved_numeric_tolerance():
    reference, results = comparison_fixture()
    results['Q4'][0]['result'] = {'AA':1.0+1e-10,'BB':None}
    assert demo_comparison_html(reference, results).count('<td>OK</td>') == 4
    with pytest.raises(ValueError): demo_comparison_html(reference, {'Q1':[]})
