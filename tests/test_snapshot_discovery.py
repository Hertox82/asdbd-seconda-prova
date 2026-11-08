"""Discovery su piccoli artefatti temporanei: nessun accesso a database reali."""

import json
import os

import pytest

from src.query_validation import (capture_database_snapshot, resolve_snapshot,
    read_postgres_snapshot, read_mongo_snapshot, compare_postgres_snapshot, compare_reference_snapshots)
from test_queries import MemoryAdapter, query_fixture


def snapshot_run(root, reference, name, *, database='postgres', finished='2026-01-01T12:00:00Z',
                 mutation=None, manifest_mutation=None):
    adapter = MemoryAdapter(reference)
    adapter.name = database
    adapter.version = lambda: 'fixture-version'
    snapshot = capture_database_snapshot(adapter, reference, 'primary_only')
    manifest = {'run_id':name, 'status':'completed', 'finished_at_utc':finished,
                'dataset_sha256':reference['dataset_sha256'],
                'configuration':{'database':database, 'dataset_size':reference['dataset_size'], 'mode':'FULL',
                                 'parameters':reference['parameters'], 'q1_ids':reference['q1_ids']}}
    if mutation: mutation(snapshot)
    if manifest_mutation: manifest_mutation(manifest)
    directory = root/name
    directory.mkdir(parents=True)
    path = directory/'database_snapshot.json'
    path.write_text(json.dumps(snapshot), encoding='utf-8')
    (directory/'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    return path


@pytest.mark.parametrize('database,label', [('postgres','PostgreSQL'),('mongo','MongoDB')])
def test_missing_snapshot_explains_setup(query_fixture, tmp_path, database, label):
    reference = query_fixture[2]
    with pytest.raises(ValueError, match=f'Nessuno snapshot {label}') as error:
        resolve_snapshot(reference, database, results_dir=tmp_path)
    assert '02_ingestion.ipynb' in str(error.value)
    assert '04_full_runner.ipynb' in str(error.value)


@pytest.mark.parametrize('database,reader', [('postgres',read_postgres_snapshot),('mongo',read_mongo_snapshot)])
def test_latest_completion_not_filename_or_mtime(query_fixture, tmp_path, database, reader):
    reference = query_fixture[2]
    old = snapshot_run(tmp_path, reference, 'z-old', database=database)
    recent = snapshot_run(tmp_path, reference, 'a-new', database=database, finished='2026-01-02T12:00:00Z')
    os.utime(old, (2_000_000_000,2_000_000_000))
    os.utime(recent, (1,1))
    assert resolve_snapshot(reference, database, results_dir=tmp_path) == recent
    assert reader(reference, results_dir=tmp_path)['database'] == database


def test_deterministic_tie_and_timezone_normalization(query_fixture, tmp_path):
    reference = query_fixture[2]
    snapshot_run(tmp_path, reference, 'a', finished='2026-01-01T13:00:00+01:00')
    expected = snapshot_run(tmp_path, reference, 'b')
    assert resolve_snapshot(reference, 'postgres', results_dir=tmp_path) == expected


@pytest.mark.parametrize('damage', ['database','validation','index_state','size','sha','parameters','ids','record','Q2','Q3','Q4','integrity'])
def test_incompatible_snapshot_is_never_selected(query_fixture, tmp_path, damage):
    reference = query_fixture[2]
    expected = snapshot_run(tmp_path, reference, 'valid')
    def mutate(s):
        if damage=='database':s['database']='mongo'
        elif damage=='validation':s['validation']['status']='failed'
        elif damage=='index_state':s['validation']['index_state']='unknown'
        elif damage=='size':s['dataset_size']+=1
        elif damage=='sha':s['dataset_sha256']='different'
        elif damage=='parameters':s['parameters']={}
        elif damage=='ids':s['q1_ids']=[]
        elif damage=='record':s['q1_records']={}
        elif damage=='integrity':s['validation']['integrity']={}
        else:s['validation']['results'][damage]=None
    snapshot_run(tmp_path, reference, 'new-invalid', finished='2026-01-03T12:00:00Z', mutation=mutate)
    assert resolve_snapshot(reference, 'postgres', results_dir=tmp_path)==expected
    expected.unlink()
    with pytest.raises(ValueError,match='Nessuno snapshot'):
        resolve_snapshot(reference, 'postgres', results_dir=tmp_path)


@pytest.mark.parametrize('damage', ['failed','running','DEMO','database','size','sha','parameters','ids','run_id','no_date','naive_date','corrupt','missing'])
def test_manifest_must_certify_completed_compatible_run(query_fixture, tmp_path, damage):
    reference=query_fixture[2]
    def mutate(m):
        if damage in ('failed','running'):m['status']=damage
        elif damage=='DEMO':m['configuration']['mode']='DEMO'
        elif damage=='database':m['configuration']['database']='mongo'
        elif damage=='size':m['configuration']['dataset_size']+=1
        elif damage=='sha':m['dataset_sha256']='different'
        elif damage=='parameters':m['configuration']['parameters']={}
        elif damage=='ids':m['configuration']['q1_ids']=[]
        elif damage=='run_id':m['run_id']='another-run'
        elif damage=='no_date':m.pop('finished_at_utc')
        elif damage=='naive_date':m['finished_at_utc']='2026-01-01T12:00:00'
    path=snapshot_run(tmp_path,reference,'candidate',manifest_mutation=mutate)
    if damage=='missing':(path.parent/'manifest.json').unlink()
    if damage=='corrupt':(path.parent/'manifest.json').write_text('{broken')
    with pytest.raises(ValueError,match='Nessuno snapshot'):
        resolve_snapshot(reference,'postgres',results_dir=tmp_path)


def test_explicit_override_supported_and_not_silently_replaced(query_fixture,tmp_path):
    reference=query_fixture[2]
    explicit=snapshot_run(tmp_path,reference,'old')
    snapshot_run(tmp_path,reference,'new',finished='2026-02-01T12:00:00Z')
    (explicit.parent/'manifest.json').unlink()  # Anche esportazione standalone.
    assert resolve_snapshot(reference,'postgres',explicit,results_dir=tmp_path)==explicit
    with pytest.raises(FileNotFoundError,match='Override'):
        resolve_snapshot(reference,'postgres',tmp_path/'missing.json',results_dir=tmp_path)
    s=json.loads(explicit.read_text());s['dataset_sha256']='different'
    explicit.write_text(json.dumps(s))
    with pytest.raises(ValueError,match='non omogeneo'):
        resolve_snapshot(reference,'postgres',explicit,results_dir=tmp_path)


def test_automatic_comparison_reports_selected_path(query_fixture,tmp_path):
    reference=query_fixture[2]
    path=snapshot_run(tmp_path,reference,'postgres-run')
    adapter=MemoryAdapter(reference)
    adapter.name='mongo'
    adapter.version=lambda:'fixture-version'
    result=compare_postgres_snapshot(adapter,reference,None,'primary_only',results_dir=tmp_path)
    assert result['status']=='passed' and result['postgres_snapshot']==str(path)


def test_cockroach_comparison_resolves_both_sources(query_fixture,tmp_path):
    reference=query_fixture[2]
    postgres=snapshot_run(tmp_path,reference,'pg')
    mongo=snapshot_run(tmp_path,reference,'mongo',database='mongo')
    adapter=MemoryAdapter(reference)
    adapter.name='cockroach'
    adapter.version=lambda:'fixture-version'
    result=compare_reference_snapshots(adapter,reference,None,None,'primary_only',results_dir=tmp_path)
    assert result['status']=='passed'
    assert result['postgres_snapshot']==str(postgres) and result['mongo_snapshot']==str(mongo)


def test_runner_rejects_wrong_peer_origin_and_keeps_campaign_scope(query_fixture,tmp_path):
    from src.runner import _compare_peers
    reference=query_fixture[2]
    current_path=snapshot_run(tmp_path,reference,'pg')
    current=json.loads(current_path.read_text())
    # Un run valido esterno alla campagna non sostituisce un peer pending.
    snapshot_run(tmp_path,reference,'other-mongo',database='mongo')
    campaign=tmp_path/'campaign'
    (campaign/'experiments').mkdir(parents=True)
    assert _compare_peers(campaign,current)['mongo']=='pending'
    # Un checkpoint Mongo che punta per errore a PostgreSQL non viene adottato.
    state=campaign/'experiments'/f"mongo_{reference['dataset_size']}.json"
    state.write_text(json.dumps({'status':'completed','snapshot':str(current_path)}))
    with pytest.raises(ValueError,match='non valido'):
        _compare_peers(campaign,current)
