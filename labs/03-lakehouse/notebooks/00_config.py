# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 · Configuración (dado)
# MAGIC
# MAGIC Los demás notebooks ejecutan este con `%run ./00_config`. No necesitas modificarlo.
# MAGIC
# MAGIC La primera vez, después de ejecutarlo, sube `data/transactions.json` al volumen
# MAGIC **Catalog → workspace → paytrack → raw → Upload to this volume**.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "paytrack"
TABLE = f"{CATALOG}.{SCHEMA}.transactions"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

# COMMAND ----------


def check(condition: bool, message: str) -> None:
    """Autoevaluación: si falla, revisa el TODO de la celda anterior."""
    if not condition:
        raise AssertionError(f"✗ {message}")
    print(f"✓ {message}")


def history():
    """Historial de versiones de la tabla, de la más reciente a la más antigua."""
    return spark.sql(f"DESCRIBE HISTORY {TABLE}")


print(f"Tabla: {TABLE}\nDatos: {VOLUME_PATH}")
