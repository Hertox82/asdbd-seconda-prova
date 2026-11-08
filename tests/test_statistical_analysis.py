"""Verifiche offline con dati sintetici, mai benchmark simulati pubblicati."""
import json
import math
from pathlib import Path
from xml.etree import ElementTree

import pytest

from src.statistical_analysis import (describe, quantile, lag_one, index_benefit,
    full_indexed_comparison, query_observations, export_analysis)
from src.statistical_plots import distribution_chart, growth_chart
from src.results import result_digest
from src.validation import sha256_file


def test_statistics_and_quantiles():
    s = describe([1, 2, 3, 4])
    assert s['mean_ms'] == s['median_ms'] == 2.5
    assert s['sample_stddev_ms'] == pytest.approx(math.sqrt(5/3))
    assert (s['q1_ms'], s['q3_ms'], s['iqr_ms'], s['mad_ms']) == (1.75, 3.25, 1.5, 1)
    assert describe([2])['sample_stddev_ms'] is None
    assert describe([2]*20)['sample_stddev_ms'] == 0
    s = describe([1]*19+[100])
    assert s['tukey_flags'] == 1 and s['n'] == 20 and s['excluded_observations'] == 0
    assert lag_one([1, 2, 3, 4]) == pytest.approx(1)
    assert lag_one([1]*20) is None


@pytest.mark.parametrize('values', [[], [float('nan')], [float('inf')], [-1], [True]])
def test_invalid_measurements(values):
    with pytest.raises(ValueError):
        describe(values)


def test_amortization_is_conditional():
    r = index_benefit(10, 4, 13)
    assert r['speedup'] == 2.5 and r['break_even_queries_hypothetical'] == 3
    assert index_benefit(4, 4, 10)['break_even_queries_hypothetical'] is None
    assert index_benefit(4, 5, 10)['break_even_queries_hypothetical'] is None
    assert index_benefit(4, 0, 10)['speedup'] is None


def group(values, **extra):
    return {**describe(values), 'values_ms': values, 'query_id': 'Q2',
            'index_state': 'secondary', 'dataset_size': 5819079, 'database': 'postgres', **extra}


def test_comparison_is_not_paired():
    p, m = group(list(range(1, 21))), group(list(range(2, 22)))
    a = full_indexed_comparison(p, m)
    b = full_indexed_comparison(p, group(list(reversed(m['values_ms']))))
    assert a == b
    assert a['recombined_pairs'] == 400
    assert 'indipendenza non garantita' in a['inferential_conclusion']


def synthetic_history(tmp_path):
    """Raw in ordine con warm-up volutamente molto diversi dalle misure."""
    reference = {'dataset_sha256': 'a'*64, 'parameters': {}, 'q1_ids': list(range(1,24)),
                 'q1_records': {str(i): {'flight_id': i} for i in range(1,24)},
                 'Q2': 7, 'Q3': 9, 'Q4': {'AA': None}}
    history = {'configuration': {'warmup': 3, 'repetitions': 20}, 'reference': reference,
               'manifests': {}, 'summaries': []}
    for phase, state, queries in [('baseline','primary_only',('Q1','Q2','Q3','Q4')),
                                  ('indexed','secondary',('Q2','Q3'))]:
        folder = tmp_path / phase
        folder.mkdir()
        history['manifests'][phase] = {'run_id': phase}
        rows = []
        for q in queries:
            for i in range(23):
                params = {'flight_id': i+1} if q == 'Q1' else {}
                result = reference['q1_records'][str(i+1)] if q == 'Q1' else reference[q]
                rows.append(dict(event='query', status='ok', mode='FULL', run_id=phase,
                    database='postgres', dataset_size=100000, index_state=state,
                    dataset_sha256=reference['dataset_sha256'], query_id=q, iteration=i,
                    warmup=i<3, parameters=params, result_digest=result_digest(result),
                    duration_ms=999 if i<3 else i-2, timestamp_utc='fixture'))
            history['summaries'].append(dict(event='query',query_id=q,index_state=state,
                                             **describe(list(range(1,21)))))
        (folder/'raw.jsonl').write_text('\n'.join(json.dumps(r) for r in rows),encoding='utf-8')
    return history


def test_raw_recalculation_excludes_only_warmup(tmp_path):
    history = synthetic_history(tmp_path)
    groups, hashes = query_observations(history,'postgres',100000,tmp_path)
    assert len(groups) == 6 and len(hashes) == 2
    assert all(g['n'] == 20 and g['mean_ms'] == 10.5 for g in groups)


@pytest.mark.parametrize('change', ['missing','duplicate','demo','failed','digest','parameters','nan','warmup'])
def test_corrupted_raw_refused(tmp_path, change):
    history = synthetic_history(tmp_path)
    path = tmp_path/'baseline/raw.jsonl'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if change == 'missing': rows.pop()
    elif change == 'duplicate': rows.append(rows[-1])
    elif change == 'demo': rows[0]['mode']='DEMO'
    elif change == 'failed': rows[0]['status']='failed'
    elif change == 'digest': rows[0]['result_digest']='0'*64
    elif change == 'parameters': rows[0]['parameters']={}
    elif change == 'nan': rows[0]['duration_ms']=float('nan')
    elif change == 'warmup': rows[0]['warmup']=False
    path.write_text('\n'.join(json.dumps(r) for r in rows))
    with pytest.raises(ValueError): query_observations(history,'postgres',100000,tmp_path)


def test_summary_mismatch_refused(tmp_path):
    history = synthetic_history(tmp_path)
    history['summaries'][0]['mean_ms'] = 50
    with pytest.raises(ValueError): query_observations(history,'postgres',100000,tmp_path)


def test_exports_unique_and_sources_unchanged(tmp_path):
    source = tmp_path/'approved/source.json'
    source.parent.mkdir()
    source.write_text('{}')
    digest = sha256_file(source)
    analysis = {k:[] for k in ('queries','index_benefits','growth','selection','single_events','pg_mongo_full')}
    analysis.update(results_root=str(tmp_path),source_hashes={str(source):digest})
    a,b = export_analysis(analysis),export_analysis(analysis)
    assert a != b and sha256_file(source) == digest
    assert json.loads((a/'analysis.json').read_text()) == analysis
    source.write_text('{"changed":true}')
    with pytest.raises(ValueError): export_analysis(analysis)


def test_svg_all_points_and_escaping():
    g = group([2]*20,database='<postgres>')
    for temporal in (False,True):
        svg = distribution_chart([g],temporal=temporal)
        root = ElementTree.fromstring(svg)
        assert len(root.findall('{http://www.w3.org/2000/svg}circle')) == 20
        assert '&lt;postgres&gt;' in svg
    with pytest.raises(ValueError): distribution_chart([g,{**g,'dataset_size':100000}])
    svg = growth_chart([g,{**g,'dataset_size':100000}], 'Q2','secondary')
    ElementTree.fromstring(svg)
    assert 'logaritmici' in svg
