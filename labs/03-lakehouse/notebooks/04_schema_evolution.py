# Databricks notebook source
# MAGIC %md
# MAGIC # 3.4 · Schema enforcement y schema evolution
# MAGIC
# MAGIC Delta valida el esquema de cada escritura (*enforcement*) y solo lo cambia
# MAGIC cuando se lo pides explícitamente (*evolution*).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC La app de PayTrack empezó a enviar el tipo de dispositivo (`device_type`) en cada transacción.
# MAGIC Este es el primer lote con la columna nueva:

# COMMAND ----------

new_batch = (
    spark.table(TABLE).limit(200)
    .withColumn("transaction_id", F.concat(F.lit("EVO"), F.monotonically_increasing_id()))
    .withColumn("device_type", F.element_at(F.array(F.lit("ios"), F.lit("android"), F.lit("web")),
                                            (F.rand(seed=7) * 3).cast("int") + 1))
)
display(new_batch.select("transaction_id", "amount_cop", "device_type").limit(5))

# COMMAND ----------

# Intento de escritura sin pedir evolución de esquema
try:
    new_batch.write.mode("append").saveAsTable(TABLE)
    print("⚠️ Se escribió el lote")
except Exception as e:
    print(f"Escritura rechazada: {type(e).__name__}")
    print(str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Schema evolution
# MAGIC Escribe `new_batch` en la tabla permitiendo que el esquema evolucione.

# COMMAND ----------

before = spark.table(TABLE).count()

# COMMAND ----------

# TODO 1


# COMMAND ----------

df = spark.table(TABLE)
check("device_type" in df.columns, "la tabla tiene la columna device_type")
check(df.count() == before + 200, "se agregaron las 200 filas del lote")
check(df.filter("device_type IS NULL").count() == before, "las filas anteriores tienen device_type NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · Columna derivada
# MAGIC El equipo de riesgo necesita una columna `risk_band` (texto) en la tabla:
# MAGIC `'high'` si `amount_cop > 5.000.000`, `'medium'` si es mayor a `1.000.000`, `'low'` en otro caso.
# MAGIC Agrégala con `ALTER TABLE ... ADD COLUMNS` y llénala para **todas** las filas.

# COMMAND ----------

# TODO 2


# COMMAND ----------

df = spark.table(TABLE)
check(df.filter("risk_band IS NULL").count() == 0, "todas las filas tienen risk_band")
check(df.filter("amount_cop > 5000000 AND risk_band <> 'high'").count() == 0, "los montos altos están en 'high'")
check(df.filter("amount_cop <= 1000000 AND risk_band <> 'low'").count() == 0, "los montos bajos están en 'low'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. Copia el error de la escritura rechazada. ¿Por qué es bueno que Delta rechace por defecto?
# MAGIC 2. ¿Qué pasaría si un lote trajera `amount_cop` como texto? ¿Lo resuelve `mergeSchema`? Pruébalo.
# MAGIC 3. ¿Cuántas versiones y cuántos archivos nuevos generó el TODO 2? ¿Qué alternativa evitaría reescribir toda la tabla para llenar una columna nueva?
