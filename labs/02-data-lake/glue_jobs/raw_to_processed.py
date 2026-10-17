"""
Glue job: raw (JSON Lines) → processed (Parquet particionado).

Corre en AWS Glue 4.0 (Spark 3.3). Se despliega subiendo este archivo a
s3://<bucket>/scripts/raw_to_processed.py (ver README).

Para probarlo localmente con Spark:
    spark-submit raw_to_processed.py <ruta-base-con-carpeta-raw>

Al final, validate() revisa el resultado. Si falla, el job queda en estado
FAILED y el detalle aparece en los logs de CloudWatch.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType  # noqa: F401 (los usarás en los TODO)

try:  # En AWS Glue
    from awsglue.utils import getResolvedOptions

    BASE = f"s3://{getResolvedOptions(sys.argv, ['BUCKET'])['BUCKET']}"
except ImportError:  # Ejecución local
    BASE = sys.argv[1]

RAW = f"{BASE}/raw"
PROCESSED = f"{BASE}/processed"

spark = SparkSession.builder.appName("paytrack-raw-to-processed").getOrCreate()


def read_raw(dataset: str) -> DataFrame:
    """Lee una zona raw. Descarta las columnas de partición year/month/day
    que Spark infiere de las carpetas: en processed las vas a derivar tú."""
    return spark.read.json(f"{RAW}/{dataset}/").drop("year", "month", "day")


# ── TODO 1: limpiar y tipar transacciones ────────────────────────────────────
# En raw todo llega como JSON: fechas como texto y montos como double.
# Devuelve un DataFrame que:
#   - tenga la columna event_ts (timestamp) a partir de "timestamp",
#     y ya no tenga la columna "timestamp" original
#   - tenga amount_cop como decimal(15,2) (¿por qué no double para dinero?)
#   - no tenga transaction_id duplicados
#   - tenga las columnas de partición year ("YYYY") y month ("MM", con
#     cero a la izquierda) derivadas de event_ts
def transform_transactions(df: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO 1: transform_transactions")


# ── TODO 2: limpiar y tipar eventos de fraude ────────────────────────────────
# Mismo patrón: detected_at → timestamp, risk_score → decimal(5,4),
# sin event_id duplicados, y columnas de partición year/month desde detected_at.
def transform_fraud_events(df: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO 2: transform_fraud_events")


# ── TODO 3: escribir en la zona processed ────────────────────────────────────
# Escribe df en `path` en formato Parquet, particionado por year y month,
# reemplazando lo que hubiera (el job debe poder re-ejecutarse).
def write_processed(df: DataFrame, path: str) -> None:
    raise NotImplementedError("TODO 3: write_processed")


# ── Validación (dada) ────────────────────────────────────────────────────────

def validate(dataset: str, key: str, expected_rows: int, typed: dict) -> None:
    # Lee las particiones como texto para comprobar el formato de las carpetas
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    df = spark.read.parquet(f"{PROCESSED}/{dataset}/")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "true")

    dtypes = dict(df.dtypes)
    errors = []
    if df.count() != expected_rows:
        errors.append(f"se esperaban {expected_rows} filas y hay {df.count()}")
    if df.select(key).distinct().count() != df.count():
        errors.append(f"hay {key} duplicados")
    for col, dtype in typed.items():
        if dtypes.get(col) != dtype:
            errors.append(f"{col} debería ser {dtype} y es {dtypes.get(col)}")
    bad = df.filter((F.length("year") != 4) | (F.length("month") != 2)).count()
    if bad:
        errors.append(f"{bad} filas con particiones year/month mal formadas")

    if errors:
        raise AssertionError(f"[{dataset}] " + "; ".join(errors))
    print(f"✓ {dataset}: {expected_rows} filas validadas")


if __name__ == "__main__":
    raw_txn = read_raw("transactions")
    write_processed(transform_transactions(raw_txn), f"{PROCESSED}/transactions/")
    validate(
        "transactions", "transaction_id",
        raw_txn.select("transaction_id").distinct().count(),
        {"event_ts": "timestamp", "amount_cop": "decimal(15,2)"},
    )

    raw_fraud = read_raw("fraud_events")
    write_processed(transform_fraud_events(raw_fraud), f"{PROCESSED}/fraud_events/")
    validate(
        "fraud_events", "event_id",
        raw_fraud.select("event_id").distinct().count(),
        {"detected_at": "timestamp", "risk_score": "decimal(5,4)"},
    )
