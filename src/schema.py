"""Contratto unico dei dati: nessuna inferenza diversa fra i futuri database.

Il CSV conserva i valori originali come testo; le funzioni di lettura restituiscono
int, str o None. Gli adapter futuri applicheranno la mappatura qui dichiarata.
"""

from dataclasses import dataclass
import re


SCHEMA_VERSION = 1
PIPELINE_VERSION = 1
FULL_ROWS = 5_819_079
DATASET_HANDLE = "usdot/flight-delays/versions/1"
SOURCE_SHA256 = "afa4e42188b113dd69427a3fbd00a4b6979c6235c866f963c2899d18a3403a83"


@dataclass(frozen=True)
class Field:
    """Descrive nome, tipo logico e nullabilità di una colonna originale."""

    name: str
    kind: str
    nullable: bool = True


# I tempi HHMM sono stringhe, mentre le durate e i ritardi sono interi.
# CANCELLED/DIVERTED restano interi 0/1 su tutti i motori, non booleani.
FIELDS = (
    Field("YEAR", "integer", False),
    Field("MONTH", "integer", False),
    Field("DAY", "integer", False),
    Field("DAY_OF_WEEK", "integer", False),
    Field("AIRLINE", "text", False),
    Field("FLIGHT_NUMBER", "integer", False),
    Field("TAIL_NUMBER", "text"),
    Field("ORIGIN_AIRPORT", "text", False),
    Field("DESTINATION_AIRPORT", "text", False),
    Field("SCHEDULED_DEPARTURE", "hhmm", False),
    Field("DEPARTURE_TIME", "hhmm"),
    Field("DEPARTURE_DELAY", "integer"),
    Field("TAXI_OUT", "integer"),
    Field("WHEELS_OFF", "hhmm"),
    Field("SCHEDULED_TIME", "integer"),
    Field("ELAPSED_TIME", "integer"),
    Field("AIR_TIME", "integer"),
    Field("DISTANCE", "integer", False),
    Field("WHEELS_ON", "hhmm"),
    Field("TAXI_IN", "integer"),
    Field("SCHEDULED_ARRIVAL", "hhmm", False),
    Field("ARRIVAL_TIME", "hhmm"),
    Field("ARRIVAL_DELAY", "integer"),
    Field("DIVERTED", "flag", False),
    Field("CANCELLED", "flag", False),
    Field("CANCELLATION_REASON", "text"),
    Field("AIR_SYSTEM_DELAY", "integer"),
    Field("SECURITY_DELAY", "integer"),
    Field("AIRLINE_DELAY", "integer"),
    Field("LATE_AIRCRAFT_DELAY", "integer"),
    Field("WEATHER_DELAY", "integer"),
)
RAW_COLUMNS = tuple(field.name for field in FIELDS)
PREPARED_COLUMNS = ("flight_id",) + RAW_COLUMNS
INTEGER = re.compile(r"-?[0-9]+\Z", re.ASCII)
HHMM = re.compile(r"[0-9]{4}\Z", re.ASCII)
Cell = int | str | None


def parse_original_row(row: list[str], record_number: int) -> tuple[Cell, ...]:
    """Converte una riga secondo schema, oppure segnala record/campo non valido.

    Parametri: celle CSV e numero logico del record (intestazione esclusa).
    Risultato: valori Python tipizzati; la stringa vuota diventa soltanto None.
    Non altera le celle originali che verranno scritte nel CSV canonico.
    """
    if len(row) != len(FIELDS):
        raise ValueError(f"Record {record_number}: attese 31 celle, trovate {len(row)}.")
    converted: list[Cell] = []
    for field, value in zip(FIELDS, row):
        prefix = f"Record {record_number}, {field.name}"
        if value == "":
            if not field.nullable:
                raise ValueError(f"{prefix}: NULL non ammesso.")
            converted.append(None)
        elif field.kind in ("integer", "flag"):
            if not INTEGER.fullmatch(value):
                raise ValueError(f"{prefix}: intero non valido: {value!r}.")
            number = int(value)
            if not -(2**31) <= number < 2**31:
                raise ValueError(f"{prefix}: fuori dall'intervallo int32.")
            if field.kind == "flag" and number not in (0, 1):
                raise ValueError(f"{prefix}: flag diverso da 0/1.")
            converted.append(number)
        elif field.kind == "hhmm":
            if not HHMM.fullmatch(value) or not (
                (int(value[:2]) < 24 and int(value[2:]) < 60) or value == "2400"
            ):
                raise ValueError(f"{prefix}: HHMM non valido: {value!r}.")
            converted.append(value)
        else:
            converted.append(value)
    return tuple(converted)


def parse_prepared_row(row: list[str], expected_id: int) -> tuple[Cell, ...]:
    """Verifica l'ID atteso e restituisce le 32 celle tipizzate per il riuso.

    La sequenza esatta 1..N prova anche assenza di ID nulli/duplicati senza un
    insieme di milioni di chiavi in RAM. In Mongo l'adapter dovrà usare BSON Int64.
    """
    if len(row) != len(PREPARED_COLUMNS):
        raise ValueError(f"Record {expected_id}: attese 32 celle, trovate {len(row)}.")
    if row[0] != str(expected_id) or not 1 <= expected_id < 2**63:
        raise ValueError(f"Record {expected_id}: flight_id non consecutivo: {row[0]!r}.")
    return (expected_id,) + parse_original_row(row[1:], expected_id)


def schema_manifest() -> list[dict]:
    """Restituisce la mappatura esplicita per i tre motori, senza crearne lo schema."""
    result = [{"name": "flight_id", "logical_type": "int64", "nullable": False,
               "postgresql": "BIGINT", "cockroachdb": "INT8", "mongodb": "Int64"}]
    for field in FIELDS:
        numeric = field.kind in ("integer", "flag")
        result.append({
            "name": field.name, "logical_type": field.kind, "nullable": field.nullable,
            "postgresql": "INTEGER" if numeric else "TEXT",
            "cockroachdb": "INT4" if numeric else "STRING",
            "mongodb": "Int32" if numeric else "string",
        })
    return result
