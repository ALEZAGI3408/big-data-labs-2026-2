# Databricks notebook source
# MAGIC %md
# MAGIC # 4.3 · Gold: agregados de negocio
# MAGIC
# MAGIC Gold contiene tablas diseñadas para **una pregunta de negocio** cada una, listas para
# MAGIC dashboards y modelos. Se construyen solo a partir de Silver.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

txn = spark.table(t("silver", "transactions"))
merchants = spark.table(t("silver", "merchants"))
customers = spark.table(t("silver", "customers"))
fraud = spark.table(t("silver", "fraud_events"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · `gold.daily_merchant_summary`
# MAGIC Una fila por **día y comercio**, para el dashboard de operaciones. Columnas:
# MAGIC
# MAGIC | Columna | Definición |
# MAGIC |---------|-----------|
# MAGIC | `day` | fecha de la transacción (date) |
# MAGIC | `merchant_id`, `merchant_name`, `merchant_category` | datos del comercio |
# MAGIC | `num_transactions` | número de transacciones (todos los estados) |
# MAGIC | `approved_cop` | suma de `amount_cop` de las transacciones aprobadas |
# MAGIC | `approval_rate_pct` | % de transacciones aprobadas, con 2 decimales |

# COMMAND ----------

# TODO 1


# COMMAND ----------

daily = spark.table(t("gold", "daily_merchant_summary"))
check(daily.count() == daily.select("day", "merchant_id").distinct().count(), "daily: una fila por día y comercio")
check(daily.agg(F.sum("num_transactions")).first()[0] == txn.count(), "daily: cuadra el número de transacciones con silver")
check(daily.agg(F.sum("approved_cop")).first()[0] == txn.filter("status = 'approved'").agg(F.sum("amount_cop")).first()[0],
      "daily: cuadra el monto aprobado con silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · `gold.fraud_weekly_report`
# MAGIC Una fila por **semana, patrón de fraude y segmento de cliente**, para el equipo de riesgo:
# MAGIC
# MAGIC | Columna | Definición |
# MAGIC |---------|-----------|
# MAGIC | `week_start` | lunes de la semana del evento (date) |
# MAGIC | `pattern` | patrón de fraude |
# MAGIC | `segment` | segmento del cliente |
# MAGIC | `num_events` | número de eventos de fraude |
# MAGIC | `avg_risk_score` | risk score promedio |
# MAGIC | `total_cop` | monto total de las transacciones involucradas |
# MAGIC
# MAGIC Solo cuentan los eventos cuya transacción está en Silver.

# COMMAND ----------

# TODO 2


# COMMAND ----------

weekly = spark.table(t("gold", "fraud_weekly_report"))
expected_events = fraud.join(txn, "transaction_id").count()
check(weekly.count() == weekly.select("week_start", "pattern", "segment").distinct().count(),
      "weekly: una fila por semana, patrón y segmento")
check(weekly.agg(F.sum("num_events")).first()[0] == expected_events, "weekly: cuadra el número de eventos")
check(weekly.filter(F.dayofweek("week_start") != 2).count() == 0, "weekly: todas las semanas empiezan en lunes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3 · `gold.hourly_volume`
# MAGIC Una fila por **hora del día** (0–23) con el volumen de transacciones **aprobadas**:
# MAGIC `hour`, `num_transactions`, `total_cop`, `avg_cop` (2 decimales).

# COMMAND ----------

# TODO 3


# COMMAND ----------

hourly = spark.table(t("gold", "hourly_volume"))
approved = txn.filter("status = 'approved'")
check(hourly.count() == 24 and hourly.agg(F.min("hour"), F.max("hour")).first() == (0, 23), "hourly: 24 horas, de 0 a 23")
check(hourly.agg(F.sum("num_transactions")).first()[0] == approved.count(), "hourly: cuadra con las aprobadas de silver")
check(hourly.agg(F.sum("total_cop")).first()[0] == approved.agg(F.sum("amount_cop")).first()[0], "hourly: cuadra el monto")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard
# MAGIC Crea un dashboard (**New → Dashboard**) con al menos una visualización por tabla Gold.
# MAGIC
# MAGIC ### Preguntas para el informe
# MAGIC 1. ¿Por qué `daily_merchant_summary` guarda `merchant_name` y `merchant_category` repetidos en cada fila en lugar de hacer JOIN con `silver.merchants` en el dashboard?
# MAGIC 2. ¿Se puede calcular la tasa de aprobación mensual de un comercio a partir de `approval_rate_pct` diaria? ¿Qué columnas agregarías para que sí se pueda?
# MAGIC 3. ¿Qué hallazgos ves en los datos? (¿Horas con más fraude? ¿Segmentos más afectados?)
