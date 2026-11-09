"""Presentazione Jupyter opzionale e riepilogo offline del runner FULL.

Niente dipendenze widget obbligatorie: un unico display HTML aggiornabile.
Il thread di osservazione non riceve adapter e non esegue query o scritture.
"""

import csv
from html import escape
import json
from pathlib import Path
from threading import Event, RLock, Thread
import time

from src.history import run_path
from src.presentation import html_table

PHASES = ('ingestion', 'baseline', 'indexes', 'indexed')


def compact_summary(directory: Path, database: str, size: int, *, results_dir=None) -> dict:
    """Legge solo checkpoint e artefatti dei tentativi completati, senza audit DB.

    Non presenta misure parziali come valide. Le fasi mancanti restano pending
    o failed e le anomalie di lettura sono avvisi visibili. Non scrive file.
    """
    directory = Path(directory)
    checkpoint = directory/'experiments'/f'{database}_{size}.json'
    if not checkpoint.is_file():
        return {'database':database, 'dataset_size':size, 'status':'pending', 'directory':str(directory), 'phases':[], 'measurements':[],
                'results':[], 'warnings':[], 'error':None}
    state = json.loads(checkpoint.read_text(encoding='utf-8'))
    rows, phases, results, messages = [], [], [], []
    for observation in state.get('environment_history', []):
        messages.extend(observation.get('warnings', []))
    for phase in PHASES:
        attempts = [a for a in state['attempts'] if a['phase']==phase]
        status = attempts[-1]['status'] if attempts else 'pending'
        phases.append({'fase':phase, 'stato':status})
        if status!='completed' or phase not in state['completed_phases']:
            continue
        attempt = attempts[-1]
        path = run_path(attempt['run_dir'], Path(results_dir) if results_dir is not None else directory.parent)
        try:
            manifest = json.loads((path/'manifest.json').read_text(encoding='utf-8'))
            if (manifest['status']!='completed' or manifest['stage']!=phase
                    or manifest['configuration']['database']!=database
                    or manifest['configuration']['dataset_size']!=size
                    or manifest['dataset_sha256']!=state['dataset_sha256']):
                raise ValueError('Manifest non coerente con il checkpoint')
            with (path/'summary.csv').open(encoding='utf-8',newline='') as stream:
                for row in csv.DictReader(stream):
                    rows.append({'fase':phase,'evento/query':row['query_id'] or row['event'],
                                 'scenario':row['index_state'], 'n':row['valid_repetitions'],
                                 'media/durata ms':row['mean_ms'], 'record/s':row['rows_per_second'],
                                 'artefatti':str(path)})
            snapshot = json.loads((path/'database_snapshot.json').read_text(encoding='utf-8'))
            validation = snapshot['validation']
            results = [{'query':'Q1','risultato':f"{len(validation['q1_ids_verified'])} record completi verificati",'validazione':validation['status']},
                       *[{'query':q,'risultato':validation['results'][q],'validazione':validation['status']} for q in ('Q2','Q3','Q4')]]
        except (OSError, ValueError, KeyError, TypeError) as error:
            messages.append(f'Riepilogo {phase} non disponibile: {error}')
    return {'database':database,'dataset_size':size,'status':state['status'],'directory':str(directory),'phases':phases,'measurements':rows,
            'results':results,'warnings':list(dict.fromkeys(messages)), 'error':state.get('error')}


def summary_html(summary: dict) -> str:
    """Mostra soltanto metriche, validazione, percorsi ed avvisi distinti dagli errori."""
    parts = [f"<h3>Riepilogo FULL | {escape(summary['database'])} | {summary['dataset_size']:,} record | {escape(summary['status'])}</h3>",
             f"<p>Campagna: <code>{escape(summary['directory'])}</code></p>",
             html_table(summary['phases'],('fase','stato')),
             html_table(summary['measurements'],('fase','evento/query','scenario','n','media/durata ms','record/s','artefatti')),
             '<h4>Risultati verificati salvati — non nuove query LIVE</h4>',
             html_table(summary['results'],('query','risultato','validazione'))]
    if summary['error']:
        error = summary['error']
        text = error.get('message',str(error)) if isinstance(error,dict) else str(error)
        parts.append(f'<p style="color:#b91c1c">Errore: {escape(text)}</p>')
    if summary['warnings']:
        parts.append('<h4>Avvisi non bloccanti</h4><ul>'+''.join(f'<li>{escape(w)}</li>' for w in summary['warnings'])+'</ul>')
    parts.append('<p>Metadati completi, ambiente, piani e raw restano nei file della campagna. '
                 'Una validazione salvata non certifica lo stato corrente del DB.</p>')
    return ''.join(parts)


def show_summary(directory: Path, database: str, size: int, *, results_dir=None) -> None:
    """Punto di ingresso offline, senza IPython richiesto nei moduli di misura."""
    from IPython.display import HTML, display
    display(HTML(summary_html(compact_summary(directory,database,size,results_dir=results_dir))))


class RunnerProgress:
    """Callback riutilizzabile con polling massimo una volta al secondo.

    Gli eventi aggiornano solo memoria; il thread renderizza il display anche
    mentre il kernel è Busy. La velocità live è osservata nell'intervallo UI,
    non è il throughput ufficiale né una stima del tempo residuo. Il carico di
    rendering resta concorrente: non si può promettere interferenza fisica zero.
    """

    def __init__(self, database: str, size: int, directory: Path, *, interval: float = 1.0):
        if interval < 1:
            raise ValueError('Aggiornamento UI minimo: un secondo.')
        self.database, self.size, self.directory = database, size, Path(directory)
        self.interval = interval
        self.phases = {phase:'pending' for phase in PHASES}
        self.status, self.detail = 'pending', 'Attesa avvio esplicito.'
        self.warnings, self.events = [], []
        self.counters = None
        self.total = size
        self.query = None
        self.index = None
        self.live = ''
        self._previous = None
        self._lock, self._stop = RLock(), Event()
        self._thread = None

    def __call__(self, event: dict) -> None:
        # Non emettere output qui: questa callback può essere chiamata fra
        # query, ma non deve introdurre un display sincrono nel percorso dati.
        with self._lock:
            kind, status = event['kind'], event.get('status')
            if kind=='campaign':
                self.status = status
                if status!='running': self.counters=None
            elif kind=='phase':
                self.phases[event['phase']] = status
                self.query = None
                self.index = None
            elif kind=='warning':
                self.warnings.append(event['message'])
            elif kind=='ingestion':
                if status=='running':
                    self.counters, self.total = event['counters'], event['total']
                    self._previous = (time.monotonic(),0)
                else:
                    self._sample()
                    self.counters = None
            elif kind=='query':
                self.query=dict(event)
                if status in ('completed','failed') or (event.get('warmup_done')==0 and event.get('repetitions_done')==0):
                    self.events.append(f"{event['query']} {event['scenario']}: {status}; "
                                       f"warm-up {event.get('warmup_done',0)}/{event.get('warmup_total','?')}, "
                                       f"misure {event.get('repetitions_done',0)}/{event.get('repetitions_total','?')}")
            elif kind=='index': self.index=dict(event)
            if 'message' in event:self.detail=event['message']
            if kind in ('phase','index','campaign'):
                # Al massimo pochi eventi per fase/indice, nessuno per batch.
                self.events.append(f"{event.get('phase',event.get('column','campagna'))}: {status}")

    def _sample(self):
        if self.counters is not None:
            count, batches = self.counters['rows_committed'], self.counters['batches_committed']
            now = time.monotonic()
            before, rows = self._previous
            speed = (count-rows)/(now-before) if now>before else 0
            self._previous=(now,count)
            self.live=f'{count:,}/{self.total:,} record confermati; {batches:,} batch; {100*count/self.total:.1f}%; velocità osservata ~{speed:,.0f} record/s'

    def html(self) -> str:
        with self._lock:
            self._sample()
            parts=[f'<h3>FULL | {escape(self.database)} | {self.size:,} record | {escape(self.status)}</h3>',
                   f'<p>Campagna: <code>{escape(str(self.directory))}</code></p>',
                   html_table([{'fase':k,'stato':v} for k,v in self.phases.items()],('fase','stato')),
                   f'<p>{escape(self.detail)}</p><p>{escape(self.live)}</p>']
            if self.query:
                q=self.query
                parts.append(f"<p>{escape(q['query'])} | {escape(q['scenario'])} | {escape(q['status'])} | "
                             f"warm-up {q.get('warmup_done','?')}/{q.get('warmup_total','?')} | "
                             f"misure {q.get('repetitions_done','?')}/{q.get('repetitions_total','?')}</p>")
            if self.index:
                parts.append(f"<p>Indice {escape(self.index['column'])}: {escape(self.index['status'])}</p>")
            parts.append('<details><summary>Transizioni osservate</summary><ul>'+''.join(f'<li>{escape(e)}</li>' for e in self.events)+'</ul></details>')
            if self.warnings:
                parts.append('<p>Avvisi non bloccanti:</p><ul>'+''.join(f'<li>{escape(w)}</li>' for w in dict.fromkeys(self.warnings))+'</ul>')
            return ''.join(parts)

    def __enter__(self):
        from IPython.display import HTML, display
        try:
            handle=display(HTML(self.html()),display_id=True)
        except Exception as error:
            self.warnings.append(f'Visualizzazione non disponibile: {error}')
            self._handle=None
            return self
        def refresh():
            while not self._stop.wait(self.interval):
                try:
                    handle.update(HTML(self.html()))
                except Exception as error:
                    # L'interfaccia non deve interrompere l'esperimento.
                    self.warnings.append(f'Aggiornamento UI non disponibile: {error}')
                    return
        self._thread=Thread(target=refresh,name='runner-progress-ui',daemon=True)
        self._thread.start()
        self._handle=handle
        return self

    def __exit__(self, error_type, error, traceback):
        from IPython.display import HTML
        self._stop.set()
        if self._thread is not None:self._thread.join(timeout=2)
        if error is not None:
            self.status,self.detail='failed',str(error)
        if self._handle is not None:
            try:
                self._handle.update(HTML(self.html()))
            except Exception:
                pass  # Un guasto di visualizzazione non cambia l'esito del runner.
