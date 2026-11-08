# Confronto tra PostgreSQL, MongoDB e CockroachDB su un dataset di voli

Questo repository contiene il laboratorio che ho realizzato per la seconda prova intermedia di **Algoritmi e Strutture Dati per i Big Data**. L'obiettivo è confrontare tre sistemi di gestione dei dati attraverso il tempo necessario a caricare un dataset e a eseguire quattro tipi di interrogazioni: ricerca per chiave, selezione su attributo non chiave, ricerca per intervallo e aggregazione.

Ho scelto **PostgreSQL** come database relazionale e **MongoDB** e **CockroachDB** come altri due sistemi da confrontare. Ho organizzato l'ambiente con **Docker Compose** e utilizzo **JupyterLab** per eseguire gli esperimenti e analizzare i risultati. I database vengono avviati **uno alla volta**, in modo da lavorare con le risorse disponibili sul mio portatile.

## 1. Ambiente utilizzato

Ho sviluppato ed eseguito le prove sul **mio portatile con Windows 11**, utilizzando Docker Desktop con backend **WSL2** e container Linux. Il portatile dispone di **16 GiB di RAM**; durante le campagne Docker aveva a disposizione circa **7,58 GiB di RAM e 12 CPU logiche**. Ho assegnato a ogni database un limite di **3 GiB di RAM e 2 CPU** e a Jupyter un limite di **1,5 GiB di RAM**.

Questi sono i valori dell'ambiente in cui ho raccolto le misure, non requisiti minimi universali. Su un altro computer i tempi possono cambiare, soprattutto per effetto di memoria, disco, cache e CPU disponibili.

Per utilizzare il laboratorio servono:

- Docker Desktop con Docker Compose (oppure Docker Engine e Compose su Linux/macOS).
- Spazio libero per immagini, volumi persistenti, CSV e risultati.
- Il file `flights.csv` del dataset pubblico, se si vuole preparare nuovamente il dataset.

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

Se devo preparare il dataset da zero, colloco `flights.csv` in `data/raw/flights.csv`, controllo i parametri di `config/benchmark.yaml` e apro **`notebooks/01_dataset_preparation.ipynb`**. La preparazione è inizialmente disabilitata. Per avviarla apro **`01_dataset_preparation.ipynb`** in JupyterLab, individuo la **cella Python iniziale di configurazione** e modifico questa riga:

```python
ABILITA_PREPARAZIONE = False
```

in:

```python
ABILITA_PREPARAZIONE = True
```

Eseguo poi le celle del notebook dall'inizio, nell'ordine previsto. Il flag è **una variabile del notebook**, non un parametro da inserire in `.env` o `config/benchmark.yaml`. Prima di attivarlo controllo che il CSV sorgente sia disponibile e che le impostazioni di preprocessing siano corrette. I CSV preparati, lo schema e i manifest vengono salvati sotto `data/prepared/`. Terminata la preparazione, riporto il flag a `False` per evitare nuove esecuzioni accidentali.

## 4. I notebook e il loro utilizzo

Ho separato i notebook per distinguere le operazioni che **preparano o modificano i dati** da quelle che **leggono risultati già acquisiti**.

| Notebook | A cosa mi serve | Impostazione iniziale |
|---|---|---|
| `00_infrastructure_check.ipynb` | Controllare Python, driver ed eventualmente PostgreSQL | Connessione facoltativa, disabilitata |
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

1. Avvio Jupyter e un solo database, poi apro `00_infrastructure_check.ipynb`.
2. Con `01_dataset_preparation.ipynb` controllo il dataset e, soltanto se necessario, preparo i CSV.
3. Imposto in `config/benchmark.yaml` il database prescelto e `dataset_size` a **100000**.
4. Apro `02_ingestion.ipynb`: prima controllo la configurazione con il flag disabilitato. Abilito `ABILITA_INGESTIONE=True` **solo su una destinazione didattica vuota e distinta da quelle delle campagne approvate**.
5. Dopo aver verificato il caricamento, apro `03_query_benchmarks.ipynb`. Abilito `ABILITA_BENCHMARK=True` soltanto se la destinazione contiene i 100K record e si trova nello stato iniziale previsto, con la sola chiave primaria.
6. Esamino tempi, risultati e piani di esecuzione salvati; arresto il database senza cancellarne il volume.

Il notebook delle query **crea indici secondari**: per questo non lo utilizzo per ripetere gli esperimenti sulle tabelle delle campagne già concluse. Se una tabella o collezione è già popolata, non forzo mai il reset senza avere prima verificato la destinazione.

### Percorso B — Eseguire una nuova campagna con il runner

Il notebook `04_full_runner.ipynb` coordina una combinazione **database × dimensione** alla volta. Il runner svolge quattro fasi: ingestione, query baseline, creazione degli indici e query indicizzate. Conserva manifest, checkpoint e misure in directory dedicate.

Prima di avviare una nuova campagna controllo `config/benchmark.yaml` e i parametri del notebook (`CAMPAGNA`, `ESEGUI`, `RIPRENDI`, `FERMATI_DOPO` e l'abilitazione per le dimensioni maggiori). Non riutilizzo le directory delle campagne già approvate e non avvio una nuova campagna solo per visualizzarne i risultati.

Il runner **non avvia né arresta Docker**: il database corretto deve già essere attivo.

### Percorso C — DEMO sui dati già caricati

Questo percorso consente di verificare il funzionamento delle interrogazioni sui dati già caricati, senza ripetere le operazioni di ingestione e indicizzazione.

1. Controllo `config/demo.yaml`: scelgo il motore (`postgres`, `mongo` o `cockroach`), la dimensione realmente presente nel database e i percorsi delle campagne storiche.
2. Avvio Jupyter e **solo il database scelto**.
3. Apro `notebooks/05_demo.ipynb` ed eseguo le celle iniziali. Posso già osservare la configurazione, il testo delle query e i risultati storici.
4. Imposto temporaneamente `ESEGUI_DEMO=True` per eseguire le interrogazioni reali. Con la configurazione prevista vengono effettuate **tre misure per query e nessun warm-up**.
5. Controllo la tabella **atteso / ottenuto / correttezza**, i tempi della DEMO e i piani storici. Le nuove osservazioni sono salvate separatamente in `results/demo/`.
6. Al termine riporto `ESEGUI_DEMO=False`.

La DEMO **non carica dati, non cancella tabelle e non crea indici**. Verifica schema, conteggio dei record, presenza degli indici e alcuni campioni prima di interrogare il database. Non sostituisce gli audit integrali già effettuati. Per una dimostrazione breve utilizzo preferibilmente la destinazione persistente da **100K record**, se presente e validata.

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

I grandi CSV, i volumi Docker e il file `.env` rimangono locali. Se trasferisco il repository su un altro computer, devo predisporre nuovamente i dati necessari oppure copiare consapevolmente gli artefatti che voglio consultare. I benchmark storici non vengono ricostruiti automaticamente dal notebook di analisi.

## 7. Test e problemi frequenti

Eseguo i controlli dalla root del repository; i test Python vengono avviati all'interno di Jupyter:

```bash
docker compose exec -T jupyter python -m pip check
docker compose exec -T jupyter python -m pytest -q -p no:cacheprovider tests/test_notebook_safety.py tests/test_demo.py tests/test_statistical_analysis.py
```

Se Jupyter non si apre, controllo `docker compose ps -a`, il token e la porta `8888`. Se una DEMO rifiuta il database, verifico che **motore, dimensione, schema e indici** corrispondano ai dati persistenti. Se mancano gli storici, controllo i percorsi in `config/demo.yaml`. Se un notebook non esegue operazioni con i flag disabilitati, è il comportamento voluto.

Non elimino i dati e non aumento automaticamente timeout o risorse soltanto per far completare una prova. Prima verifico log, disponibilità del disco e stato del database.
