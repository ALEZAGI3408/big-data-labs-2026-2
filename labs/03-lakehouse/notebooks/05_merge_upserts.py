# Databricks notebook source
# MAGIC %md
# MAGIC # 3.5 · MERGE (upserts) y mantenimiento
# MAGIC
# MAGIC Cada noche los bancos envían a PayTrack un lote de conciliación con transacciones
# MAGIC nuevas y cambios de estado de transacciones existentes (por ejemplo, contracargos).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

current = spark.table(TABLE)
updates = (
    current.filter("status = 'approved'").orderBy("transaction_id").limit(100)
    .withColumn("status", F.lit("chargeback"))
)
inserts = (
    current.orderBy("transaction_id").limit(50)
    .withColumn("transaction_id", F.concat(F.lit("NEW"), F.monotonically_increasing_id()))
    .withColumn("event_ts", F.current_timestamp())
)
# Se materializa el lote para que no dependa del estado futuro de la tabla
batch = spark.createDataFrame(updates.unionByName(inserts).collect(), current.schema)
batch.createOrReplaceTempView("daily_batch")
update_ids = [r.transaction_id for r in updates.select("transaction_id").collect()]
print(f"Lote: {batch.count()} filas (100 contracargos + 50 nuevas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Upsert
# MAGIC Implementa `upsert(source_view)` con un `MERGE INTO` sobre `TABLE`:
# MAGIC - si la transacción ya existe, actualiza su `status`
# MAGIC - si no existe, insértala completa

# COMMAND ----------

def upsert(source_view: str) -> None:
    # TODO 1
    raise NotImplementedError("TODO 1: upsert")

# COMMAND ----------

before = spark.table(TABLE).count()
upsert("daily_batch")

df = spark.table(TABLE)
check(df.count() == before + 50, "se insertaron las 50 transacciones nuevas")
check(df.filter(F.col("transaction_id").isin(update_ids) & (F.col("status") == "chargeback")).count() == 100,
      "las 100 transacciones existentes quedaron en chargeback")
metrics = history().first().operationMetrics
check(history().first().operation == "MERGE", "la última versión es un MERGE")
check(metrics["numTargetRowsUpdated"] == "100" and metrics["numTargetRowsInserted"] == "50",
      "el MERGE actualizó 100 filas e insertó 50")

# COMMAND ----------

# MAGIC %md
# MAGIC Los bancos a veces **reenvían** el mismo lote. Un buen proceso de carga debe
# MAGIC poder ejecutarse dos veces sin duplicar datos:

# COMMAND ----------

upsert("daily_batch")
check(spark.table(TABLE).count() == before + 50, "re-ejecutar el upsert no duplica filas (idempotencia)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · Compactación
# MAGIC Tantas operaciones pequeñas dejaron la tabla con muchos archivos. Compáctala con
# MAGIC `OPTIMIZE`, ordenando los datos por `customer_id` para acelerar las consultas por cliente.

# COMMAND ----------

files_before = spark.sql(f"DESCRIBE DETAIL {TABLE}").first().numFiles

# COMMAND ----------

# TODO 2


# COMMAND ----------

files_after = spark.sql(f"DESCRIBE DETAIL {TABLE}").first().numFiles
check(history().first().operation == "OPTIMIZE", "la última versión es un OPTIMIZE")
print(f"Archivos: {files_before} → {files_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. ¿Qué habría pasado en la re-ejecución si en lugar de MERGE hubieras usado `INSERT INTO`?
# MAGIC 2. ¿Cómo implementarías esta misma conciliación en el Data Lake del Lab 2 (solo Parquet, sin Delta)?
# MAGIC 3. ¿Cuántos archivos tenía la tabla antes y después de `OPTIMIZE`? ¿Por qué el *small files problem* afecta el rendimiento?
# MAGIC 4. ¿Qué es *liquid clustering* y en qué se diferencia de `ZORDER BY`?
