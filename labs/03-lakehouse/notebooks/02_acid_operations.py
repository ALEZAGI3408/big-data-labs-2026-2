# Databricks notebook source
# MAGIC %md
# MAGIC # 3.2 · Operaciones ACID: UPDATE, DELETE y constraints
# MAGIC
# MAGIC En un data lake de solo Parquet, cambiar una fila obliga a reescribir a mano
# MAGIC particiones completas, y si el proceso falla a mitad de camino la tabla queda corrupta.
# MAGIC En Delta cada operación es una transacción: o se aplica completa, o no se aplica.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · UPDATE
# MAGIC El cliente **C000042** reportó que le robaron su cuenta Nequi. El equipo de riesgo
# MAGIC decide marcar como fraude (`is_fraud = TRUE`) **todas** sus transacciones con `payment_method = 'nequi'`.

# COMMAND ----------

stolen = (F.col("customer_id") == "C000042") & (F.col("payment_method") == "nequi")
n_stolen = spark.table(TABLE).filter(stolen).count()
print(f"Transacciones afectadas: {n_stolen}")

# COMMAND ----------

# TODO 1


# COMMAND ----------

check(n_stolen > 0, "hay transacciones del cliente con nequi")
check(
    spark.table(TABLE).filter(stolen & F.col("is_fraud")).count() == n_stolen,
    "todas las transacciones del cliente con nequi están marcadas como fraude",
)
check(history().first().operation == "UPDATE", "la última versión es un UPDATE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · DELETE
# MAGIC Alguien del equipo de QA cargó por error transacciones de prueba a producción
# MAGIC (la celda siguiente lo simula). Elimínalas: son las del cliente `TEST_CUSTOMER`.

# COMMAND ----------

(
    spark.table(TABLE).limit(3)
    .withColumn("customer_id", F.lit("TEST_CUSTOMER"))
    .withColumn("transaction_id", F.concat(F.lit("TEST"), F.monotonically_increasing_id()))
    .write.mode("append").saveAsTable(TABLE)
)
print("Filas de prueba:", spark.table(TABLE).filter("customer_id = 'TEST_CUSTOMER'").count())

# COMMAND ----------

# TODO 2


# COMMAND ----------

check(
    spark.table(TABLE).filter("customer_id = 'TEST_CUSTOMER'").count() == 0,
    "no quedan transacciones de prueba",
)
check(history().first().operation == "DELETE", "la última versión es un DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3 · Constraint y atomicidad
# MAGIC Agrega a la tabla un constraint `CHECK` que impida guardar transacciones con
# MAGIC `amount_cop` menor o igual a cero.
# MAGIC Referencia: [Constraints en Databricks](https://docs.databricks.com/aws/en/tables/constraints)

# COMMAND ----------

# TODO 3


# COMMAND ----------

# MAGIC %md
# MAGIC La celda siguiente intenta cargar un lote de 100 transacciones en el que **solo una**
# MAGIC tiene monto negativo. Observa qué pasa con las otras 99.

# COMMAND ----------

before = spark.table(TABLE).count()
sample = spark.table(TABLE).limit(100).withColumn(
    "transaction_id", F.concat(F.lit("BATCH"), F.monotonically_increasing_id())
)
batch = sample.limit(99).unionByName(sample.limit(1).withColumn("amount_cop", F.lit(-1).cast("decimal(15,2)")))

try:
    batch.write.mode("append").saveAsTable(TABLE)
    print("⚠️ El lote se insertó completo")
except Exception as e:
    print(f"Lote rechazado: {type(e).__name__}")

check(spark.table(TABLE).count() == before, "el lote se rechazó completo: no entró ninguna de las 100 filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. ¿Cuántos archivos reescribió el UPDATE (`operationMetrics` en el historial)? ¿Por qué tantos, si solo cambiaron unas pocas filas?
# MAGIC 2. ¿Qué letra de ACID demuestra la última celda? ¿Qué habría pasado en un data lake de solo Parquet?
# MAGIC 3. ¿Por qué el lote rechazado no creó una versión nueva en el historial?
