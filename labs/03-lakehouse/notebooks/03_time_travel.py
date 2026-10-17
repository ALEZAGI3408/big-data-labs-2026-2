# Databricks notebook source
# MAGIC %md
# MAGIC # 3.3 · Time Travel
# MAGIC
# MAGIC Cada versión del log apunta a un conjunto de archivos. Mientras esos archivos
# MAGIC existan, puedes consultar la tabla tal como estaba en cualquier versión anterior.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

display(history().select("version", "timestamp", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Encontrar la versión anterior al UPDATE
# MAGIC Obtén **desde el historial** (no escribas el número a mano) la versión justo
# MAGIC anterior al UPDATE del notebook 02 y guárdala en `v_before_update`.

# COMMAND ----------

# TODO 1
v_before_update = None

# COMMAND ----------

ops = {r.version: r.operation for r in history().collect()}
check(v_before_update is not None, "v_before_update está definida")
check(ops.get(v_before_update + 1) == "UPDATE", "la versión siguiente a v_before_update es el UPDATE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · Consultar por versión
# MAGIC Calcula cuántas transacciones fraudulentas había **antes** del UPDATE (`fraud_before`)
# MAGIC y cuántas hay **ahora** (`fraud_now`).

# COMMAND ----------

# TODO 2
fraud_before = None
fraud_now = None

# COMMAND ----------

check(None not in (fraud_before, fraud_now), "fraud_before y fraud_now están definidos")
check(fraud_now > fraud_before, "el UPDATE aumentó el número de fraudes")
print(f"Antes: {fraud_before} · Ahora: {fraud_now}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3 · Consultar por fecha
# MAGIC Time travel también funciona con fechas. Usa el `timestamp` de la versión
# MAGIC `v_before_update` (tómalo del historial) para calcular `fraud_by_timestamp`.

# COMMAND ----------

# TODO 3
fraud_by_timestamp = None

# COMMAND ----------

check(fraud_by_timestamp == fraud_before, "la consulta por fecha da lo mismo que la consulta por versión")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4 · Deshacer solo el UPDATE
# MAGIC Riesgo se equivocó: el cliente C000042 no fue víctima de fraude. Hay que devolver
# MAGIC `is_fraud` de **esas** transacciones a su valor anterior al UPDATE.
# MAGIC
# MAGIC `RESTORE TABLE ... TO VERSION AS OF` no sirve: devuelve la tabla **completa** a esa
# MAGIC versión y desharía también el DELETE y el constraint del notebook 02.
# MAGIC Usa la versión anterior como fuente de un `MERGE INTO` (o un `UPDATE` con subconsulta)
# MAGIC para corregir únicamente las filas afectadas.

# COMMAND ----------

# TODO 4


# COMMAND ----------

stolen = (F.col("customer_id") == "C000042") & (F.col("payment_method") == "nequi")
check(spark.table(TABLE).filter("is_fraud").count() == fraud_before, "los fraudes volvieron al valor anterior al UPDATE")
check(spark.table(TABLE).filter(stolen & F.col("is_fraud")).count() < spark.table(TABLE).filter(stolen).count(),
      "las transacciones de C000042 con nequi ya no están todas marcadas como fraude")
check(spark.table(TABLE).filter("customer_id = 'TEST_CUSTOMER'").count() == 0, "el DELETE sigue vigente")
props = {r.key for r in spark.sql(f"SHOW TBLPROPERTIES {TABLE}").collect()}
check(any(k.startswith("delta.constraints.") for k in props), "el constraint sigue vigente")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. ¿Qué habría deshecho exactamente un `RESTORE` a `v_before_update`? Verifícalo leyendo el historial.
# MAGIC 2. `VACUUM` borra los archivos que ya no usa la versión actual. ¿Qué le pasa al time travel después de un `VACUUM`? ¿Por qué existe un período de retención por defecto de 7 días?
# MAGIC 3. Da dos casos de uso reales de time travel en PayTrack (piensa en auditoría y en ML).
