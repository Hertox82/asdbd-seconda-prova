"""Discovery e preflight DEMO su artefatti temporanei, senza DB reali."""

from dataclasses import replace
import json
from pathlib import Path
import shutil

import pytest
import yaml

from src.demo import load_demo_settings, select_demo_history, run_demo
from src.history import resolve_campaign, prepared_dataset_sha
from test_demo import historical_case
from test_queries import query_fixture


def select(case, **overrides):
    settings, adapter, _ = case
    args = dict(dataset_sha256=adapter.reference['dataset_sha256'],
                parameters=settings.experiment.parameters, seed=settings.experiment.seed)
    args.update(overrides)
    return resolve_campaign(settings.experiment.results_dir,'postgres',7,**args)


def experiment_state(settings):
    return settings.campaign/'experiments/postgres_7.json'


def clone_campaign(settings, name, finished):
    target=settings.experiment.results_dir/name
    shutil.copytree(settings.campaign,target)
    file=target/'experiments/postgres_7.json'
    state=json.loads(file.read_text())
    for attempt in state['attempts']:
        old=Path(attempt['run_dir'])
        new=old.parent/(name+'-'+old.name)
        shutil.copytree(old,new)
        attempt['run_dir']=str(new)
        manifest=json.loads((new/'manifest.json').read_text())
        manifest['run_id']=new.name
        if attempt['phase']=='indexed':manifest['finished_at_utc']=finished
        (new/'manifest.json').write_text(json.dumps(manifest))
    file.write_text(json.dumps(state))
    return target


def test_global_incomplete_matrix_does_not_exclude_completed_experiment(historical_case):
    settings,adapter,before=historical_case
    (settings.campaign/'status.json').write_text(json.dumps({'completed':False}))
    history=select(historical_case)
    assert history['campaign']==settings.campaign
    assert 'globale' in history['selection']['reason']
    adapter.connect.assert_not_called()


def test_latest_completed_campaign_not_filename_or_global_status(historical_case):
    settings,_,_=historical_case
    expected=clone_campaign(settings,'a-new','2099-01-01T12:00:00Z')
    assert select(historical_case)['campaign']==expected
    explicit=select(historical_case,campaign=settings.campaign)
    assert explicit['campaign']==settings.campaign and 'Override' in explicit['selection']['reason']


def test_deterministic_tie_break(historical_case):
    settings,_,_=historical_case
    clone_campaign(settings,'a-tie','2099-01-01T12:00:00Z')
    expected=clone_campaign(settings,'z-tie','2099-01-01T12:00:00+00:00')
    assert select(historical_case)['campaign']==expected


def test_newer_incomplete_candidate_cannot_hide_valid_campaign(historical_case):
    settings,_,_=historical_case
    invalid=clone_campaign(settings,'new-but-failed','2099-01-01T12:00:00Z')
    state_file=invalid/'experiments/postgres_7.json'
    state=json.loads(state_file.read_text());state['status']='failed'
    state_file.write_text(json.dumps(state))
    assert select(historical_case)['campaign']==settings.campaign


@pytest.mark.parametrize('damage', ['missing','failed','phase','snapshot','correctness','date','size','sha','parameters','seed'])
def test_missing_incomplete_incompatible_or_failed_campaigns_refused(historical_case,damage):
    settings,_,_=historical_case
    file=experiment_state(settings)
    state=json.loads(file.read_text())
    kwargs={}
    if damage=='missing':file.unlink()
    elif damage in ('failed','phase'):
        if damage=='failed':state['status']='failed'
        else:state['completed_phases'].pop()
        file.write_text(json.dumps(state))
    elif damage in ('snapshot','correctness','date'):
        run=Path(state['attempts'][-1]['run_dir'])
        if damage=='snapshot':
            p=run/'database_snapshot.json';value=json.loads(p.read_text());value['validation']['status']='failed'
        elif damage=='correctness':
            p=run/'correctness_secondary.json';value=json.loads(p.read_text());value['status']='failed'
        else:
            p=run/'manifest.json';value=json.loads(p.read_text());value['finished_at_utc']='invalid-date'
        p.write_text(json.dumps(value))
    elif damage=='size':
        state['dataset_size']=8;file.write_text(json.dumps(state))
    elif damage=='sha':kwargs['dataset_sha256']='0'*64
    elif damage=='parameters':kwargs['parameters']={}
    elif damage=='seed':kwargs['seed']=999
    with pytest.raises(ValueError,match='Nessuna campagna'):
        select(historical_case,**kwargs)


def test_bad_explicit_override_does_not_fall_back(historical_case):
    settings,_,_=historical_case
    with pytest.raises(ValueError,match='Override'):
        select(historical_case,campaign=settings.experiment.results_dir/'missing')
    with pytest.raises(ValueError,match='Override'):
        select(historical_case,campaign=settings.campaign,dataset_sha256='0'*64)


def test_manual_100k_not_invented_or_adopted(tmp_path):
    (tmp_path/'manual-run').mkdir()
    (tmp_path/'manual-run/manifest.json').write_text('{"status":"completed"}')
    with pytest.raises(ValueError,match='manuali 100K'):
        resolve_campaign(tmp_path,'postgres',100000)
    with pytest.raises(ValueError,match='manuale 100K'):
        resolve_campaign(tmp_path,'postgres',100000,campaign=tmp_path/'manual-run')


def test_automatic_demo_checks_live_destination_before_measured_queries(historical_case,monkeypatch):
    import src.demo as demo
    settings,adapter,_=historical_case
    auto=replace(settings,campaigns={7:None},dataset_sha256=adapter.reference['dataset_sha256'])
    assert select_demo_history(auto)['campaign']==settings.campaign
    adapter.count_rows.return_value=6
    monkeypatch.setattr(demo,'measure_queries',lambda *a,**k:pytest.fail('Query temporizzate non ammesse'))
    with pytest.raises(ValueError,match='DB non pronto'):
        run_demo(auto)
    assert adapter.calls==[]


def test_load_null_catalog_uses_only_prepared_metadata(query_fixture,tmp_path,monkeypatch):
    import src.demo as demo
    csv,_,reference=query_fixture
    folder=tmp_path/'settings/config';folder.mkdir(parents=True)
    base=yaml.safe_load(Path('config/benchmark.yaml').read_text())
    base['dataset_sizes']=[100000,1000000,5819079]
    (folder/'benchmark.yaml').write_text(yaml.safe_dump(base))
    (folder/'demo.yaml').write_text(yaml.safe_dump({'mode':'DEMO','database':'postgres','dataset_size':7,'campaigns':{7:None}}))
    monkeypatch.setattr(demo,'SIZES',(7,))
    from src.config import load_config
    config=load_config(Path('config/benchmark.yaml'))
    monkeypatch.setattr(demo,'load_config',lambda p:replace(config,bundle_dir=csv.parent))
    csv.unlink()  # Dimostra che non si rilegge il CSV preparato.
    settings=load_demo_settings(folder/'demo.yaml')
    assert settings.campaigns=={7:None}
    assert settings.dataset_sha256==reference['dataset_sha256']
    assert prepared_dataset_sha(csv.parent,7)==reference['dataset_sha256']
