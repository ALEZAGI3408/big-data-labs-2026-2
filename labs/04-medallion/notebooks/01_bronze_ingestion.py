# Databricks notebook source
# MAGIC %md
# MAGIC # 4.1 · Bronze: ingesta raw
# MAGIC
# MAGIC Bronze guarda los datos **tal como llegan**, sin limpiar ni tipar, más metadatos
# MAGIC de ingesta para poder rastrear de dónde y cuándo llegó cada registro.
# MAGIC Es una capa de solo-agregar (*append-only*): nunca se modifica lo que ya entró.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

DATASETS = ["transactions", "fraud_events", "customers", "merchants"]

# Marca de tiempo única para toda esta ejecución de la ingesta
INGESTION_TS = spark.sql("SELECT current_timestamp() AS ts").first().ts
print(f"Ingesta: {INGESTION_TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Ingesta
# MAGIC Implementa `ingest_to_bronze(dataset)`:
# MAGIC 1. Lee `{LANDING}/{dataset}.json` (es un arreglo JSON: necesitas la opción `multiLine`). **No** conviertas tipos ni limpies nada.
# MAGIC 2. Agrega dos columnas de auditoría:
# MAGIC    - `ingested_at`: el valor de `INGESTION_TS`
# MAGIC    - `source_file`: la ruta del archivo de origen (columna oculta `_metadata.file_path`)
# MAGIC 3. **Agrega** (append) las filas a la tabla `t("bronze", dataset)`.

# COMMAND ----------

def ingest_to_bronze(dataset: str) -> None:
    # TODO 1
    raise NotImplementedError("TODO 1: ingest_to_bronze")

# COMMAND ----------

for dataset in DATASETS:
    ingest_to_bronze(dataset)
    print(f"→ {dataset}")

# COMMAND ----------

for dataset in DATASETS:
    df = spark.table(t("bronze", dataset))
    landed = spark.read.option("multiLine", True).json(f"{LANDING}/{dataset}.json").count()
    check({"ingested_at", "source_file"} <= set(df.columns), f"{dataset}: tiene las columnas de auditoría")
    check(df.filter(F.col("ingested_at") == F.lit(INGESTION_TS)).count() == landed,
          f"{dataset}: esta ingesta cargó los {landed} registros del archivo")
    check(df.filter(~F.col("source_file").contains(f"{dataset}.json")).count() == 0,
          f"{dataset}: source_file apunta al archivo de origen")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. Revisa el esquema de `bronze.transactions`. ¿De qué tipo quedó `amount_cop` y por qué? (Pista: mira algunos registros.)
# MAGIC 2. Ejecuta este notebook dos veces. ¿Qué pasa con los conteos en Bronze? ¿Por qué eso está bien en esta capa y cuál capa debe resolverlo?
# MAGIC 3. ¿Para qué sirven `ingested_at` y `source_file` cuando aparece un dato malo en un dashboard?
