# Databricks notebook source
# MAGIC %md
# MAGIC # 4.2 · Silver: limpieza, tipado y calidad
# MAGIC
# MAGIC Silver es la versión **confiable** de cada entidad: una fila por llave, tipos correctos,
# MAGIC reglas de calidad aplicadas y datos personales protegidos. Lo que no pasa las reglas
# MAGIC no se borra: va a una tabla de **cuarentena** con el motivo del rechazo.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

AMOUNT_MAX = 100_000_000  # COP: montos iguales o mayores son errores de captura

bronze_txn = spark.table(t("bronze", "transactions"))
print(f"Bronze: {bronze_txn.count()} filas, {bronze_txn.select('transaction_id').distinct().count()} transaction_id distintos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Deduplicar
# MAGIC Bronze tiene duplicados (reenvíos de la fuente y re-ejecuciones de la ingesta).
# MAGIC Implementa `deduplicate(df, key)`: deja **una fila por `key`**, la de `ingested_at` más reciente.
# MAGIC Usa una función de ventana (`Window` + `row_number`); `dropDuplicates` no te deja elegir cuál fila conservar.

# COMMAND ----------

def deduplicate(df, key: str):
    # TODO 1
    raise NotImplementedError("TODO 1: deduplicate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2 · Tipar
# MAGIC Implementa `standardize(df)` para transacciones:
# MAGIC - `amount_cop` → `decimal(15,2)` (si no se puede convertir, queda `NULL`)
# MAGIC - `timestamp` → columna `event_ts` de tipo timestamp (y elimina `timestamp`)
# MAGIC - `is_fraud` → boolean

# COMMAND ----------

def standardize(df):
    # TODO 2
    raise NotImplementedError("TODO 2: standardize")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3 · Reglas de calidad
# MAGIC Implementa `add_quality_flags(df)`: agrega la columna `dq_reason` (texto) con el
# MAGIC **primer** problema que encuentres en la fila, o `NULL` si la fila está bien:
# MAGIC
# MAGIC | Regla | `dq_reason` |
# MAGIC |-------|-------------|
# MAGIC | `customer_id` es nulo | `'missing_customer'` |
# MAGIC | `amount_cop` nulo, `<= 0` o `>= AMOUNT_MAX` | `'invalid_amount'` |

# COMMAND ----------

def add_quality_flags(df):
    # TODO 3
    raise NotImplementedError("TODO 3: add_quality_flags")

# COMMAND ----------

# Dado: separa las filas válidas de la cuarentena y escribe Silver
checked = (
    add_quality_flags(standardize(deduplicate(bronze_txn, "transaction_id")))
    .drop("ingested_at", "source_file")
    .withColumn("silver_loaded_at", F.current_timestamp())
)
(checked.filter("dq_reason IS NULL").drop("dq_reason")
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver", "transactions")))
(checked.filter("dq_reason IS NOT NULL")
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver", "transactions_quarantine")))

display(spark.table(t("silver", "transactions_quarantine")).groupBy("dq_reason").count())

# COMMAND ----------

silver = spark.table(t("silver", "transactions"))
quarantine = spark.table(t("silver", "transactions_quarantine"))
dtypes = dict(silver.dtypes)
check(silver.count() == silver.select("transaction_id").distinct().count(), "silver: sin transaction_id duplicados")
check(silver.count() + quarantine.count() == bronze_txn.select("transaction_id").distinct().count(),
      "silver + cuarentena = transacciones distintas de bronze (no se perdió nada)")
check(dtypes.get("amount_cop") == "decimal(15,2)" and dtypes.get("event_ts") == "timestamp"
      and dtypes.get("is_fraud") == "boolean", "silver: tipos correctos")
check(silver.filter(f"amount_cop <= 0 OR amount_cop >= {AMOUNT_MAX} OR amount_cop IS NULL").count() == 0,
      "silver: todos los montos en rango")
check(silver.filter("customer_id IS NULL").count() == 0, "silver: todas las transacciones tienen cliente")
check(quarantine.count() > 0 and quarantine.filter(
          "dq_reason NOT IN ('missing_customer', 'invalid_amount') OR dq_reason IS NULL").count() == 0,
      "cuarentena: cada fila tiene un dq_reason válido")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4 · Clientes: proteger datos personales
# MAGIC Crea `silver.customers` a partir de `bronze.customers`:
# MAGIC - una fila por `customer_id` (reutiliza `deduplicate`)
# MAGIC - **sin** la columna `name`
# MAGIC - `email` enmascarado: primeros 3 caracteres del usuario + `***` + el dominio
# MAGIC   (`carlos.perez@gmail.com` → `car***@gmail.com`). Pista: `F.regexp_replace`.
# MAGIC - sin las columnas de auditoría de Bronze, con `silver_loaded_at`

# COMMAND ----------

# TODO 4


# COMMAND ----------

customers = spark.table(t("silver", "customers"))
check("name" not in customers.columns, "silver.customers: sin nombres")
check(customers.count() == customers.select("customer_id").distinct().count(), "silver.customers: un registro por cliente")
check(customers.filter(~F.col("email").rlike(r"^[^@]{1,3}\*\*\*@[^@]+$")).count() == 0,
      "silver.customers: todos los emails enmascarados")

# COMMAND ----------

# Dado: comercios y eventos de fraude (mismo patrón, sin reglas de calidad)
(deduplicate(spark.table(t("bronze", "merchants")), "id")
    .select(F.col("id").alias("merchant_id"), F.col("name").alias("merchant_name"),
            F.col("category").alias("merchant_category"), "city")
    .withColumn("silver_loaded_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver", "merchants")))

(deduplicate(spark.table(t("bronze", "fraud_events")), "event_id")
    .withColumn("risk_score", F.col("risk_score").cast("decimal(5,4)"))
    .withColumn("detected_at", F.to_timestamp("detected_at"))
    .drop("ingested_at", "source_file")
    .withColumn("silver_loaded_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver", "fraud_events")))

check(spark.table(t("silver", "merchants")).count() > 0, "silver.merchants cargada")
check(spark.table(t("silver", "fraud_events")).count() > 0, "silver.fraud_events cargada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. ¿Cuántas filas quedaron en cuarentena por cada motivo? ¿Por qué es mejor una cuarentena que borrar esas filas?
# MAGIC 2. ¿Por qué se deduplica **antes** de aplicar las reglas de calidad? ¿Qué podría salir mal en el orden inverso?
# MAGIC 3. Silver se reescribe completa en cada ejecución (`overwrite`). ¿Qué problema tendría esto con 500K transacciones diarias durante años? ¿Cómo lo resolverías con lo que viste en el Lab 3?
# MAGIC 4. ¿El email enmascarado sigue siendo un dato personal? ¿Qué otra técnica usarías si analítica necesitara contar clientes únicos por email?
