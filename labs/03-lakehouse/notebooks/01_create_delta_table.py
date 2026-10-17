# Databricks notebook source
# MAGIC %md
# MAGIC # 3.1 · Crear la tabla Delta
# MAGIC
# MAGIC Un archivo Parquet es inmutable y no sabe nada de los demás archivos de la tabla.
# MAGIC Delta Lake agrega un **log de transacciones** (`_delta_log/`) que registra qué archivos
# MAGIC forman cada versión de la tabla. De ahí salen ACID, time travel y schema enforcement.
# MAGIC
# MAGIC ⚠️ Ejecutar este notebook **reinicia el lab**: borra y recrea la tabla.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

raw_df = (
    spark.read.option("multiLine", True)
    .json(f"{VOLUME_PATH}/transactions.json")
    .withColumn("event_ts", F.to_timestamp("timestamp"))
    .drop("timestamp")
    .withColumn("amount_cop", F.col("amount_cop").cast("decimal(15,2)"))
)
display(raw_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Escribir la tabla
# MAGIC Guarda `raw_df` como una **tabla Delta administrada** llamada `TABLE`.
# MAGIC Puedes usar la API de DataFrames o SQL (`CREATE TABLE ... AS SELECT`).

# COMMAND ----------

# TODO 1


# COMMAND ----------

check(spark.catalog.tableExists(TABLE), "la tabla existe")
detail = spark.sql(f"DESCRIBE DETAIL {TABLE}").first()
check(detail.format == "delta", "la tabla está en formato Delta")
check(spark.table(TABLE).count() == raw_df.count(), "la tabla tiene todas las filas")
check(history().count() == 1, "la tabla tiene exactamente una versión")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · Explorar los metadatos
# MAGIC Ejecuta `DESCRIBE DETAIL` y `DESCRIBE HISTORY` sobre la tabla y responde en tu informe:
# MAGIC 1. ¿Cuántos archivos tiene la tabla y cuánto pesa? ¿Por qué ese número de archivos?
# MAGIC 2. ¿Qué operación registró la versión 0 y qué métricas guarda (`operationMetrics`)?
# MAGIC 3. ¿Dónde está físicamente la tabla (`location`)? ¿Qué pasa con los archivos si haces `DROP TABLE` de una tabla administrada?

# COMMAND ----------

# TODO 2
