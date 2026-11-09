"""Eventi reali su fixture in memoria: nessuna campagna o database reale."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

import src.benchmark as benchmark
import src.ingestion as ingestion
from src.progress import progress_sink
from src.results import ResultRun
from src.runner_ui import RunnerProgress, compact_summary, summary_html
from test_ingestion import BatchAdapter
from test_queries import MemoryAdapter, query_fixture
from test_runner import experiment, execute


class Clock:
    """Il costo artificiale delle callback non deve entrare nei tempi salvati."""
    value = 0

    def __call__(self):
        self.value += 1_000_000
        return self.value

    def callback(self, event):
        self.value += 100_000_000


def test_ingestion_no_callback_between_first_write_and_last_commit(query_fixture, monkeypatch):
    path, artifact, _ = query_fixture
    clock = Clock()
    monkeypatch.setattr(ingestion.time,'perf_counter_ns',clock)
    observations=[]
    adapter=BatchAdapter()
    def callback(event):
        observations.append((event['status'],adapter.calls))
        clock.callback(event)
        if event['status']=='running':
            with pytest.raises(TypeError):event['counters']['rows_committed']=100
        if event['status']=='completed':
            with pytest.raises(TypeError):event['result']['duration_ms']=0
    result=ingestion.ingest_csv(adapter,path,7,artifact['sha256'],batch_size=3,progress=callback)
    assert observations==[('running',0),('completed',3)]
    assert result['duration_ms']==6 and result['driver_write_ms']==3
    assert result['rows_committed']==7


def test_query_events_do_not_change_timer_or_raw_schema(query_fixture,tmp_path,monkeypatch):
    reference=query_fixture[2]
    clock=Clock()
    monkeypatch.setattr(benchmark.time,'perf_counter_ns',clock)
    events=[]
    def callback(event):events.append(event);clock.callback(event)
    with ResultRun(tmp_path/'results',{'mode':'FULL','database':'postgres','batch_size':3},reference,stage='fixture') as run:
        benchmark.measure_queries(MemoryAdapter(reference),reference,run,index_state='primary_only',
                                  query_ids=('Q2',),warmup=2,repetitions=3,progress=callback)
    assert [r['duration_ms'] for r in run.records]==[1]*5
    assert len(events)==6 and events[-1]['status']=='completed'
    assert (events[-1]['warmup_done'],events[-1]['repetitions_done'])==(2,3)
    assert all('progress' not in row and 'kind' not in row for row in run.records)


def test_index_notifications_bracket_creation_and_exclude_callback_cost(query_fixture,tmp_path,monkeypatch):
    from test_runner import StatefulAdapter
    adapter=StatefulAdapter(query_fixture[2])
    clock=Clock()
    monkeypatch.setattr(benchmark.time,'perf_counter_ns',clock)
    events=[]
    def callback(event):
        events.append((event['column'],event['status'],list(adapter.secondary)))
        clock.callback(event)
    with ResultRun(tmp_path/'results',{'mode':'FULL','database':'postgres','batch_size':3},query_fixture[2],stage='fixture') as run:
        benchmark.measure_secondary_indexes(adapter,run,progress=callback)
    assert [e[:2] for e in events]==[('origin_airport','running'),('origin_airport','completed'),('distance','running'),('distance','completed')]
    assert events[0][2]==[] and events[1][2]==['origin_airport']
    assert [r['duration_ms'] for r in run.records]==[1,1]


def test_failed_query_reports_only_completed_observations(query_fixture,tmp_path):
    reference=query_fixture[2]
    adapter=MemoryAdapter(reference)
    original=adapter.run_query
    calls=0
    def failing_query(query,parameters):
        nonlocal calls
        calls+=1
        if calls==2:raise TimeoutError('timeout fixture')
        return original(query,parameters)
    adapter.run_query=failing_query
    events=[]
    with pytest.raises(TimeoutError):
        with ResultRun(tmp_path/'results',{'mode':'FULL','database':'postgres','batch_size':3},reference,stage='fixture') as run:
            benchmark.measure_queries(adapter,reference,run,index_state='primary_only',query_ids=('Q2',),
                                      warmup=1,repetitions=3,progress=events.append)
    assert events[-1]['status']=='failed'
    assert events[-1]['warmup_done']==1 and events[-1]['repetitions_done']==0


def test_callback_error_disabled_once_and_does_not_fail_work(query_fixture):
    callback=Mock(side_effect=RuntimeError('guasto UI'))
    path, artifact, _=query_fixture
    with pytest.warns(RuntimeWarning,match='Interfaccia progresso'):
        result=ingestion.ingest_csv(BatchAdapter(),path,7,artifact['sha256'],batch_size=3,progress=callback)
    assert result['rows_committed']==7 and callback.call_count==1


def test_runner_pause_resume_and_compact_summary(experiment):
    config,adapter,directory=experiment
    events=[]
    first=execute(config,directory,stop_after='ingestion',progress=events.append)
    assert first['status']=='paused'
    assert [(e['phase'],e['status']) for e in events if e['kind']=='phase'][-3:]==[
        ('baseline','skipped'),('indexes','skipped'),('indexed','skipped')]
    events.clear()
    result=execute(config,directory,resume=True,progress=events.append)
    assert result['status']=='completed'
    assert any(e.get('phase')=='ingestion' and e['status']=='resumed' for e in events)
    summary=compact_summary(directory,config.database,config.dataset_size)
    assert len(summary['measurements'])==9
    assert [r['query'] for r in summary['results']]==['Q1','Q2','Q3','Q4']
    html=summary_html(summary)
    assert 'packages' not in html and 'code_sha256' not in html
    assert 'completed' in html and 'validazione' in html
    count=len(adapter.rows)
    events.clear()
    execute(config,directory,resume=True,progress=events.append)
    assert len(adapter.rows)==count and adapter.reset_calls==adapter.drop_calls==0
    assert not any(e['kind'] in ('query','index','ingestion') for e in events)


def test_runner_failure_is_visible_and_does_not_publish_valid_summary(experiment):
    config,adapter,directory=experiment
    adapter.fail_insert=True
    events=[]
    with pytest.raises(ingestion.IngestionError):execute(config,directory,progress=events.append)
    assert events[-1]['kind']=='campaign' and events[-1]['status']=='failed'
    summary=compact_summary(directory,config.database,config.dataset_size)
    assert summary['status']=='failed' and summary['measurements']==[]
    assert 'Errore:' in summary_html(summary)


@pytest.mark.parametrize('phase',('ingestion','baseline','indexes','indexed'))
def test_adoption_after_interruption_emits_resumed_without_reexecution(experiment,monkeypatch,phase):
    import src.runner as runner
    config,_,directory=experiment
    original_exit=runner.ResultRun.__exit__
    def interrupted_after_manifest(run,error_type,error,traceback):
        result=original_exit(run,error_type,error,traceback)
        if error_type is None and run.manifest['stage']==phase:
            raise OSError('interruzione dopo pubblicazione')
        return result
    monkeypatch.setattr(runner.ResultRun,'__exit__',interrupted_after_manifest)
    with pytest.raises(OSError):execute(config,directory)
    monkeypatch.setattr(runner.ResultRun,'__exit__',original_exit)
    original_execute=runner._execute_phase
    def prevent_reexecution(current,*args,**kwargs):
        assert current!=phase
        return original_execute(current,*args,**kwargs)
    monkeypatch.setattr(runner,'_execute_phase',prevent_reexecution)
    events=[]
    execute(config,directory,resume=True,progress=events.append)
    assert any(e['kind']=='phase' and e.get('phase')==phase and e['status']=='resumed' for e in events)


def test_environment_warnings_remain_in_artifacts_and_are_not_errors(experiment,monkeypatch):
    import src.runner as runner
    config,adapter,directory=experiment
    original=runner.environment_context
    monkeypatch.setattr(runner,'environment_context',lambda v:{**original(v),'coverage_warnings':['Copertura Docker parziale']})
    events=[]
    state=execute(config,directory,stop_after='ingestion',progress=events.append)
    assert state['environment_history'][-1]['warnings']==['Copertura Docker parziale']
    assert any(e['kind']=='warning' for e in events)
    summary=compact_summary(directory,config.database,config.dataset_size)
    assert summary['error'] is None and 'Avvisi non bloccanti' in summary_html(summary)


def test_ui_counter_is_real_no_eta_and_html_escaped(monkeypatch,tmp_path):
    panel=RunnerProgress('<postgres>',100,tmp_path)
    counters={'rows_committed':10,'batches_committed':1}
    panel({'kind':'ingestion','status':'running','counters':counters,'total':100})
    counters.update(rows_committed=20,batches_committed=2)
    html=panel.html()
    assert '&lt;postgres&gt;' in html and '20/100' in html and '20.0%' in html
    assert '2 batch' in html and 'ETA' not in html
    with pytest.raises(ValueError):RunnerProgress('postgres',100,tmp_path,interval=0.1)


def test_ui_updates_while_busy_and_stops_thread(monkeypatch,tmp_path):
    import IPython.display
    from threading import Event
    refreshed=Event()
    handle=Mock()
    handle.update.side_effect=lambda value:refreshed.set()
    monkeypatch.setattr(IPython.display,'display',lambda *a,**k:handle)
    with RunnerProgress('postgres',100,tmp_path) as panel:
        assert refreshed.wait(3)
    assert not panel._thread.is_alive()


def test_ui_display_failure_does_not_mask_original_error(monkeypatch,tmp_path):
    import IPython.display
    monkeypatch.setattr(IPython.display,'display',Mock(side_effect=RuntimeError('display guasto')))
    with pytest.raises(ValueError,match='errore originale'):
        with RunnerProgress('postgres',100,tmp_path):
            raise ValueError('errore originale')


def test_summary_pending_and_malformed_phase_are_explicit(experiment):
    config,_,directory=experiment
    assert compact_summary(directory,config.database,config.dataset_size)['status']=='pending'
    state=execute(config,directory,stop_after='ingestion')
    path=Path(state['attempts'][-1]['run_dir'])/'manifest.json'
    path.write_text('{}')
    summary=compact_summary(directory,config.database,config.dataset_size)
    assert summary['warnings'] and not summary['measurements']
