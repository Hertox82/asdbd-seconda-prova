# Confronto tra PostgreSQL, MongoDB e CockroachDB su un dataset di voli

Questo repository contiene il laboratorio che ho realizzato per la seconda prova intermedia di **Algoritmi e Strutture Dati per i Big Data**. L'obiettivo è confrontare tre sistemi di gestione dei dati attraverso il tempo necessario a caricare un dataset e a eseguire quattro tipi di interrogazioni: ricerca per chiave, selezione su attributo non chiave, ricerca per intervallo e aggregazione.

Ho scelto **PostgreSQL** come database relazionale e **MongoDB** e **CockroachDB** come altri due sistemi da confrontare. Ho organizzato l'ambiente con **Docker Compose** e utilizzo **JupyterLab** per eseguire gli esperimenti e analizzare i risultati. I database vengono avviati **uno alla volta**, in modo da lavorare con le risorse disponibili sul mio portatile.

## 1. Ambiente utilizzato

Ho sviluppato ed eseguito le prove sul **mio portatile con Windows 11**, utilizzando Docker Desktop con backend **WSL2** e container Linux. Il portatile dispone di **16 GiB di RAM**; durante le campagne Docker aveva a disposizione circa **7,58 GiB di RAM e 12 CPU logiche**. Ho assegnato a ogni database un limite di **3 GiB di RAM e 2 CPU** e a Jupyter un limite di **1,5 GiB di RAM**.

Questi sono i valori dell'ambiente in cui ho raccolto le misure, non requisiti minimi universali. Su un altro computer i tempi possono cambiare, soprattutto per effetto di memoria, disco, cache e CPU disponibili.

Per utilizzare il laboratorio servono:

- Docker Desktop con Docker Compose (oppure Docker Engine e Compose su Linux/macOS).
- Spazio libero per immagini, volumi persistenti, CSV e risultati.
- Una connessione Internet per acquisire nuovamente `flights.csv` dal dataset pubblico, oppure una copia locale del CSV.

Non è necessario installare Python o Jupyter direttamente su Windows: li eseguo all'interno del container.

## 2. Primo avvio sul portatile

I comandi Docker riportati qui sotto si eseguono dal **terminale**, nella cartella principale del repository. Sul mio portatile utilizzo il terminale di Windows; gli stessi comandi Docker Compose si possono utilizzare anche da macOS e Linux, purché Docker sia installato e funzionante.

Prima di tutto avvio Docker Desktop e verifico che sia disponibile:

```bash
docker version
docker compose version
docker compose ps -a
```

Alla prima configurazione duplico il file `.env.example` e rinomino la copia `.env`, mantenendola nella cartella principale del repository. Posso farlo direttamente dal file manager, senza dipendere da comandi specifici del sistema operativo. Lascio nel file tutte le variabili previste dal progetto e imposto i valori richiesti, in particolare `POSTGRES_PASSWORD` e `JUPYTER_TOKEN`.

Per un laboratorio eseguito **soltanto in locale**, posso usare per esempio questi valori:

```dotenv
POSTGRES_PASSWORD=VoliLab2026_db
JUPYTER_TOKEN=VoliLab2026
```

`JUPYTER_TOKEN` non è un token da richiedere o generare su un servizio esterno: è una stringa scelta da me, che JupyterLab usa per l'accesso dal browser. Anche `POSTGRES_PASSWORD` è una password locale scelta da me. Questi sono **esempi**, non credenziali imposte dal progetto: se nel mio `.env.example` compaiono altre variabili, le mantengo e le compilo secondo i commenti presenti nel file. Non aggiungo variabili con nomi inventati. Per CockroachDB il laboratorio utilizza la modalità single-node locale prevista dalla sua configurazione, senza richiedere un token da creare.

Non inserisco `.env` nel repository Git, perché contiene credenziali. Le credenziali di esempio vanno bene soltanto per un ambiente di studio non esposto in rete: non le riutilizzo per altri sistemi o ambienti pubblici.

Per avviare **Jupyter e PostgreSQL**:

```bash
docker compose --profile postgres config --quiet
docker compose build jupyter
docker compose --profile postgres up -d --no-deps --wait --wait-timeout 120 jupyter postgres
```

Apro quindi **http://127.0.0.1:8888/lab** nel browser e inserisco il token configurato. I notebook sono nella cartella `notebooks/`.

Per controllare i container e le risorse:

```bash
docker compose ps -a
docker stats --no-stream
```

### Cambiare database

Ho previsto un solo database attivo alla volta: prima arresto quello in uso, poi avvio il successivo. **L'arresto non elimina i dati**, che rimangono nei volumi Docker.

```bash
# Da PostgreSQL a MongoDB
docker compose --profile postgres stop postgres
docker compose --profile mongo up -d --no-deps --wait --wait-timeout 120 jupyter mongo
```

```bash
# Da MongoDB a CockroachDB
docker compose --profile mongo stop mongo
docker compose --profile cockroach up -d --no-deps --wait --wait-timeout 120 jupyter cockroach
```

Per fermare un database, per esempio PostgreSQL, e poi Jupyter:

```bash
docker compose --profile postgres stop postgres
docker compose stop jupyter
```

Nella cartella `scripts/` ho anche predisposto `start-db.ps1` e `stop-db.ps1` per gestire l'avvio e l'arresto dall'host Windows.

> **Precauzione:** non utilizzo `docker compose down -v` per un normale arresto, perché `-v` può rimuovere i volumi contenenti i dati. Non cancello neppure `results/` o `data/prepared/` durante una semplice prova dei notebook.

## 3. Dataset e preparazione

Ho utilizzato il dataset pubblico [2015 Flight Delays and Cancellations (Kaggle)](https://www.kaggle.com/datasets/usdot/flight-delays), limitandomi al file `flights.csv`.

| Caratteristica | Valore |
|---|---:|
| Dimensione del CSV originale | circa 564,96 MiB |
| Record originali | 5.819.079 |
| Colonne originali | 31 |
| Colonne dopo il preprocessing | 32 |
| Dimensioni analizzate | 100.000, 1.000.000, 5.819.079 record |

Nel CSV originale non ho individuato una singola colonna già definita come identificativo univoco. Per ottenere una chiave comune ai tre database ho aggiunto nel preprocessing `flight_id`, un identificativo **deterministico e progressivo** assegnato seguendo l'ordine delle righe. Ho preservato le altre 31 colonne e i valori mancanti (`NULL` in SQL e `null` in MongoDB).

I tre dataset utilizzati negli esperimenti sono prefissi dello stesso CSV preparato, **non campioni casuali**. Questa scelta garantisce che, per ogni dimensione, i database ricevano gli stessi record; può però influire sulla distribuzione dei valori selezionati dalle query.

Per riconoscere esattamente il CSV sorgente usato nelle campagne, ho conservato questo SHA-256:

```text
afa4e42188b113dd69427a3fbd00a4b6979c6235c866f963c2899d18a3403a83
```

Se devo preparare il dataset da zero, configuro in `config/benchmark.yaml` la modalità di acquisizione: **offline disabilitato** per consentire il download, oppure **offline abilitato** solo quando `data/raw/flights.csv` è già presente. In alternativa copio manualmente `flights.csv` in `data/raw/flights.csv`. Controllo quindi i parametri di `config/benchmark.yaml` e apro **`notebooks/01_dataset_preparation.ipynb`**. La preparazione è inizialmente disabilitata. Per avviarla apro **`01_dataset_preparation.ipynb`** in JupyterLab, individuo la **cella Python iniziale di configurazione** e modifico questa riga:

```python
ABILITA_PREPARAZIONE = False
```

in:

```python
ABILITA_PREPARAZIONE = True
```

Eseguo poi le celle del notebook dall'inizio, nell'ordine previsto. Il flag è **una variabile del notebook**, non un parametro da inserire in `.env` o `config/benchmark.yaml`. Prima di attivarlo controllo che il CSV sorgente sia disponibile **oppure** che il download sia consentito dalla configurazione. Se compare `Modalità offline: manca .../data/raw/flights.csv`, la preparazione non può acquisire il CSV finché non viene fornito localmente o non si disabilita la modalità offline. I CSV preparati, lo schema e i manifest vengono salvati sotto `data/prepared/`. Terminata la preparazione, riporto il flag a `False` per evitare nuove esecuzioni accidentali.

## 4. I notebook e il loro utilizzo 

Ho separato i notebook per distinguere le operazioni che **preparano o modificano i dati** da quelle che **leggono risultati già acquisiti**.

| Notebook | A cosa mi serve | Impostazione iniziale |
|---|---|---|
| `01_dataset_preparation.ipynb` | Acquisire e preparare il CSV, controllare schema e checksum | `ABILITA_PREPARAZIONE=False` |
| `02_ingestion.ipynb` | Studiare l'ingestione manuale su 100K record | `ABILITA_INGESTIONE=False` |
| `03_query_benchmarks.ipynb` | Eseguire baseline, creazione indici e query sui 100K | `ABILITA_BENCHMARK=False` |
| `04_full_runner.ipynb` | Orchestrare una nuova campagna completa | `ESEGUI=False` |
| `05_demo.ipynb` | Eseguire una dimostrazione sui database già popolati | `ESEGUI_DEMO=False` |
| `06_results_analysis.ipynb` | Consultare e analizzare le campagne già completate | Lettura offline |

Quando i flag sono `False`, l'esecuzione di tutte le celle non deve avviare nuove ingestione o benchmark. I notebook manuali sono limitati a **100.000 record** quando li abilito. Per dimensioni superiori utilizzo il runner, senza aggirare i controlli previsti.

### Cambiare database e riavviare un notebook

Quando passo da PostgreSQL a MongoDB o CockroachDB, **non devo cancellare il notebook né resettare il database**. Prima arresto il database precedente e avvio quello nuovo con i comandi Docker Compose descritti nella sezione 2. Poi aggiorno il database selezionato in `config/benchmark.yaml` per i notebook manuali e il runner, oppure in `config/demo.yaml` per la DEMO; verifico anche che la dimensione del dataset e la destinazione corrispondano ai dati realmente presenti.

Per evitare di riutilizzare variabili, adapter o connessioni del database precedente, nel notebook aperto eseguo questi passaggi in **JupyterLab**:

1. Salvo eventuali modifiche al notebook.
2. Dal menu **Kernel** scelgo **Restart Kernel** (Riavvia kernel).
3. Se voglio eliminare anche i risultati visualizzati dalla precedente esecuzione, utilizzo l'opzione di JupyterLab per **cancellare tutti gli output**. Non è necessario per azzerare le variabili: serve a rendere più chiara la nuova esecuzione.
4. Controllo i flag nella cella iniziale: `ABILITA_PREPARAZIONE`, `ABILITA_INGESTIONE`, `ABILITA_BENCHMARK`, `ESEGUI` ed `ESEGUI_DEMO` sono disabilitati per impostazione predefinita nei rispettivi notebook.
5. Seleziono **Run → Run All Cells** per eseguire le celle dall'inizio. Attivo il flag necessario **solo** dopo aver verificato motore, dimensione, stato dei dati e operazione che voglio svolgere.

**Riavviare il kernel non equivale a resettare il database:** azzera lo stato Python del notebook, ma non elimina i volumi Docker, le tabelle, le collezioni, i CSV o gli artefatti salvati in `results/`. Il riavvio del kernel non cancella necessariamente gli output già visibili nelle celle.

`05_demo.ipynb` va riavviato dopo il cambio di database, così può leggere la nuova selezione da `config/demo.yaml` e creare una connessione corretta. `06_results_analysis.ipynb`, invece, legge soltanto gli storici e può essere utilizzato anche senza database attivi. Non rieseguo automaticamente `02_ingestion.ipynb`, `03_query_benchmarks.ipynb` o `04_full_runner.ipynb` su un database già popolato: prima verifico le precondizioni, perché questi percorsi possono scrivere dati o creare indici quando vengono abilitati.

### Percorso A — Capire preparazione, ingestione e indici

Questo è il percorso che seguo quando voglio studiare le singole fasi del laboratorio:

1. Avvio Jupyter e un solo database, verificando che i container risultino `healthy` con `docker compose --profile postgres ps` (sostituendo il profilo quando necessario).
2. Con `01_dataset_preparation.ipynb` controllo il dataset e, soltanto se necessario, preparo i CSV.
3. Imposto in `config/benchmark.yaml` il database prescelto e `dataset_size` a **100000**.
4. Apro `02_ingestion.ipynb`: prima controllo la configurazione con il flag disabilitato. Abilito `ABILITA_INGESTIONE=True` **solo su una destinazione didattica vuota e distinta da quelle delle campagne approvate**.
5. Dopo aver verificato il caricamento, apro `03_query_benchmarks.ipynb`. Abilito `ABILITA_BENCHMARK=True` soltanto se la destinazione contiene i 100K record e si trova nello stato iniziale previsto, con la sola chiave primaria.
6. Esamino tempi, risultati e piani di esecuzione salvati; arresto il database senza cancellarne il volume.

Il notebook delle query **crea indici secondari**: per questo non lo utilizzo per ripetere gli esperimenti sulle tabelle delle campagne già concluse. Se una tabella o collezione è già popolata, non forzo mai il reset senza avere prima verificato la destinazione.

### Percorso B — Eseguire le campagne automatizzate da 100K, 1M e FULL

Il notebook `04_full_runner.ipynb` coordina **una combinazione database × dimensione alla volta**, senza avviare autonomamente Docker. In ciascuna esecuzione effettua le quattro fasi `ingestion`, `baseline`, `indexes`, `indexed`, salva gli artefatti e aggiorna un pannello di progresso durante il lavoro. Al termine mostra un riepilogo compatto. Mantengo **un solo database attivo alla volta** e **la stessa directory `CAMPAGNA` per tutti e tre i motori della medesima dimensione**.

Dopo il collaudo completo ho adottato questi nomi; per una nuova esecuzione si possono scegliere directory diverse, purché non si riutilizzi una campagna incompatibile già esistente:

| Dimensione | `dataset_size` | Tabella/collection dedicata nei tre DB | `CAMPAGNA` nel notebook |
|---|---:|---|---|
| 100K | `100000` | `flights_100k` | `ROOT / 'results' / 'full-campaign-100000'` |
| 1M | `1000000` | `flights_1m` | `ROOT / 'results' / 'full-campaign-02'` |
| FULL | `5819079` | `flights_full` | `ROOT / 'results' / 'full-campaign-5819079'` |

La campagna da 1M si chiama `full-campaign-02` perché ho conservato una precedente campagna sperimentale separata: **il nome non è un requisito tecnico**. Non rinomino le directory di campagne già completate, per non invalidare eventuali riferimenti nei metadati.

Per ogni nuova dimensione seguo la stessa procedura:

1. In `config/benchmark.yaml` imposto la dimensione (`dataset_size`), `database: postgres`, la destinazione PostgreSQL dedicata e lascio `confirm_reset: false`, `confirm_drop_indexes: false`. I riferimenti `comparison.postgres_snapshot` e `comparison.mongo_snapshot` restano `null`: gli snapshot compatibili vengono ricercati automaticamente.
2. Dal terminale avvio PostgreSQL con `docker compose --profile postgres up -d --no-deps --wait --wait-timeout 120 jupyter postgres` e controllo che sia `healthy`.
3. In `04_full_runner.ipynb` imposto la directory di campagna e controllo il `config.manifest()` iniziale con `ESEGUI=False`. Solo dopo aver verificato motore, dimensione e nome della tabella abilito:

   ```python
   ESEGUI = True
   RIPRENDI = False
   FERMATI_DOPO = None
   ABILITA_DIMENSIONI_MAGGIORI = True  # per 1M e FULL; False è sufficiente per 100K
   ```

4. Riavvio il kernel (**Kernel → Restart Kernel**) ed eseguo **Run → Run All Cells**. Attendo il riepilogo `completed`, tutte le quattro fasi completate e le verifiche `passed`.
5. Arresto PostgreSQL, avvio MongoDB, seleziono `database: mongo` in `config/benchmark.yaml` e imposto la collection corrispondente (`flights_100k`, `flights_1m` o `flights_full`). Riavvio il kernel ed eseguo nuovamente il notebook **mantenendo la stessa `CAMPAGNA`**.
6. Ripeto con CockroachDB, usando la tabella dedicata e la stessa campagna. Per questo passaggio devono essere disponibili i riferimenti validati degli altri due motori: il laboratorio li cerca automaticamente.

Se la tabella o collection è già popolata, il runner non deve sovrascriverla silenziosamente. Per ripetere un esperimento uso una **destinazione nuova** oppure, soltanto dopo aver controllato il suo contenuto e deciso consapevolmente di perderlo, elimino la destinazione specifica. Non abilito un reset generale per aggirare i controlli. Le operazioni sui database non devono cancellare `results/`.

Il runner protegge anche il **protocollo immutabile**: quando cambiano codice o parametri rilevanti, una campagna esistente può rifiutare una nuova esecuzione. In questo caso creo una **nuova directory `CAMPAGNA` per tutti e tre i motori**, senza forzare checkpoint incompatibili. `RIPRENDI=True` va utilizzato soltanto per riprendere un'esecuzione interrotta con protocollo compatibile, non per ripetere una campagna già completata.

Le cartelle di campagna contengono `protocol.json`, `environment_baseline.json`, `status.json` ed `experiments/`. Lo `status.json` comprende anche combinazioni non eseguite in quella specifica directory: **un `completed: false` globale non indica un errore** se i tre esperimenti della dimensione selezionata hanno ciascuno `status: completed`, quattro fasi completate e confronti `passed`.

### Percorso C — DEMO sui dati già caricati

Questo percorso consente di verificare il funzionamento delle interrogazioni sui dati già caricati, senza ripetere le operazioni di ingestione e indicizzazione.

In `config/demo.yaml` imposto il database e la dimensione da mostrare. I riferimenti alle campagne **non richiedono percorsi scritti a mano**:

```yaml
mode: DEMO
database: cockroach
dataset_size: 5819079
campaigns:
  100000: null
  1000000: null
  5819079: null
```

Con `null` la DEMO individua automaticamente la campagna compatibile più recente per il motore e la dimensione selezionati, verificando manifest, snapshot, parametri e correttezza. Un percorso esplicito può essere impostato come override, ma se è errato il sistema non lo sostituisce silenziosamente. Per la DEMO da 100K utilizzo la campagna automatizzata `full-campaign-100000`: le prime prove manuali 100K, prive di una campagna completa, non vengono adottate automaticamente.

1. Avvio Jupyter e **solo il database scelto**. Verifico in `config/benchmark.yaml` che il nome della tabella/collection corrisponda alla dimensione selezionata: `flights_100k`, `flights_1m` o `flights_full`.
2. Apro `notebooks/05_demo.ipynb`, riavvio il kernel ed eseguo le celle iniziali con `ESEGUI_DEMO=False`. Verifico il percorso storico individuato e il motivo della selezione.
3. Imposto temporaneamente `ESEGUI_DEMO=True` ed eseguo **Run → Run All Cells**. Il notebook effettua **tre misure LIVE per query, senza warm-up**, e separa queste osservazioni dai benchmark FULL salvati in precedenza.
4. Controllo la tabella **atteso / ottenuto / correttezza** per Q1–Q4; al termine riporto `ESEGUI_DEMO=False`.

La DEMO è di sola lettura: **non carica dati, non cancella tabelle, non crea indici e non modifica le campagne**. Verifica preventivamente lo stato minimo della destinazione LIVE e dei risultati, ma non sostituisce un audit integrale del database. Una campagna storica valida non garantisce da sola che i dati siano ancora presenti nel database corrente.

### Percorso D — Studiare i risultati senza database attivi

Apro **`notebooks/06_results_analysis.ipynb`** anche con tutti e tre i database arrestati. Il notebook legge gli artefatti delle nove combinazioni storiche da `results/`, verifica la coerenza delle misure e mostra tabelle e grafici.

La vista iniziale contiene **cinque grafici**. Per mostrare gli altri **22** imposto `MOSTRA_APPROFONDIMENTI=True`. L'eventuale esportazione delle analisi derivate è disabilitata per impostazione predefinita e, quando richiesta, viene salvata in una directory separata sotto `results/statistical_analysis/`.

**Importante:** per utilizzare DEMO e analisi offline non basta il codice del repository. Devono essere disponibili anche gli artefatti delle campagne e, per la DEMO live, i dati persistenti del database scelto.

## 5. Query e metodo di misura

Ho scelto le stesse quattro operazioni logiche per tutti i database:

| Query | Operazione | Indici considerati |
|---|---|---|
| **Q1** | Recupero del record completo tramite `flight_id` | Chiave primaria |
| **Q2** | Conteggio voli in partenza da `JFK` | Senza/con indice su aeroporto di origine |
| **Q3** | Conteggio voli con distanza tra `1500` e `2000` (estremi inclusi) | Senza/con indice sulla distanza |
| **Q4** | Media del ritardo di partenza per compagnia, escludendo i valori mancanti | Scenario baseline |

Nelle campagne sperimentali ho utilizzato **batch da 1.000 record**, **seed 42**, **3 warm-up** e **20 misure** per query e scenario. Non ho incluso i warm-up nelle statistiche e non ho eliminato gli outlier. Ho misurato separatamente il tempo di costruzione degli indici. Il timer delle query comprende l'esecuzione tramite adapter e la ricezione completa del risultato, mentre verifica e salvataggio avvengono fuori dalla misurazione.

I tempi di ingestione e costruzione degli indici derivano da **una singola esecuzione** per motore e dimensione; per questi valori non posso stimare la variabilità tra esperimenti. Inoltre CockroachDB è stato usato in modalità **single-node**: non ho misurato la scalabilità distribuita o il failover.

## 6. Risultati e cartelle principali

Le campagne completate riguardano **100K, 1M e 5.819.079 record** su ciascuno dei tre database. Oltre ai riepiloghi, ho conservato le singole osservazioni, le configurazioni, i controlli di correttezza, gli snapshot e i piani delle query.

```text
config/           Configurazioni benchmark e DEMO
src/              Preprocessing, adapter, runner, verifiche e analisi
notebooks/        Notebook Jupyter del laboratorio
tests/            Test automatici
scripts/          Script per avvio e arresto dei servizi
data/raw/         CSV sorgente locale
data/prepared/    Dataset elaborati, schema e manifest
results/          Campagne, misure raw, statistiche, piani e DEMO
```

I grandi CSV, i volumi Docker e il file `.env` rimangono locali. Anche `results/` può essere ignorata da Git: se desidero che il destinatario consulti **subito** i grafici e gli storici senza rieseguire le campagne, devo consegnare gli artefatti separatamente o includerli esplicitamente nell’archivio di consegna, dopo aver verificato le dimensioni. Altrimenti il professore può rigenerarli seguendo il procedimento, ma i benchmark completi richiedono tempo. Se trasferisco il repository su un altro computer, devo predisporre nuovamente i dati necessari oppure copiare consapevolmente gli artefatti che voglio consultare. I benchmark storici non vengono ricostruiti automaticamente dal notebook di analisi.

## 7. Test e problemi frequenti

Gli output e i contatori di esecuzione salvati nei notebook sono conservati intenzionalmente come documentazione dei risultati sperimentali, per consultare grafici e tabelle senza rieseguire i benchmark. I test ne verificano la struttura senza richiederne la cancellazione. Gli output descrivono esecuzioni precedenti, non certificano lo stato corrente dei database; i flag di avvio restano disabilitati (`False`).

Eseguo i controlli dalla root del repository; i test Python possono essere avviati all'interno di Jupyter. Prima di lanciare l’intera suite distinguo i test unitari da quelli che potrebbero utilizzare servizi reali:

```bash
docker compose exec -T jupyter python -m pip check
docker compose exec -T jupyter python -m pytest -q -p no:cacheprovider tests/test_notebook_safety.py tests/test_demo.py tests/test_statistical_analysis.py
```

Se Jupyter non si apre, controllo `docker compose ps -a`, il token e la porta `8888`. Se una DEMO rifiuta il database, verifico che **motore, dimensione, schema e indici** corrispondano ai dati persistenti. Se mancano gli storici, controllo la presenza delle campagne e i messaggi della discovery automatica in `config/demo.yaml` (`null`); un percorso esplicito è un override vincolante. Se un notebook non esegue operazioni con i flag disabilitati, è il comportamento voluto.

Non elimino i dati e non aumento automaticamente timeout o risorse soltanto per far completare una prova. Prima verifico log, disponibilità del disco e stato del database.
