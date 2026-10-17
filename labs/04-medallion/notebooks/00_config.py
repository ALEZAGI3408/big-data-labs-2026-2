# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 · Configuración (dado)
# MAGIC
# MAGIC Los demás notebooks ejecutan este con `%run ./00_config`. No necesitas modificarlo.
# MAGIC
# MAGIC La primera vez, después de ejecutarlo, sube los 4 archivos de `data/`
# MAGIC (`transactions.json`, `fraud_events.json`, `customers.json`, `merchants.json`)
# MAGIC al volumen **Catalog → workspace → bronze → landing → Upload to this volume**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

# COMMAND ----------


def t(layer: str, name: str) -> str:
    """Nombre completo de una tabla: t('silver', 'transactions') → workspace.silver.transactions"""
    return f"{CATALOG}.{layer}.{name}"


def check(condition: bool, message: str) -> None:
    """Autoevaluación: si falla, revisa los TODO del notebook."""
    if not condition:
        raise AssertionError(f"✗ {message}")
    print(f"✓ {message}")


print(f"Landing: {LANDING}")
