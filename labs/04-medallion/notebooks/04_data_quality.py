# Databricks notebook source
# MAGIC %md
# MAGIC # 4.4 · Reporte de calidad de datos
# MAGIC
# MAGIC Las verificaciones de los notebooks anteriores son de *este* lab. En producción, las
# MAGIC reglas de calidad se declaran como **expectativas** que se evalúan en cada ejecución
# MAGIC del pipeline y alimentan un reporte. Aquí construyes ese reporte.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Dado: tres tipos de expectativa. Cada una devuelve cuántas filas la violan.
results = []


def expect_rows(name: str, table: str, condition: str) -> None:
    """Toda fila de `table` cumple `condition` (expresión SQL)."""
    failed = spark.table(table).filter(f"NOT ({condition}) OR ({condition}) IS NULL").count()
    results.append(("rows", name, table, failed))


def expect_unique(name: str, table: str, columns: str) -> None:
    """No hay dos filas de `table` con el mismo valor en `columns` (separadas por coma)."""
    df = spark.table(table)
    failed = df.count() - df.select(*[c.strip() for c in columns.split(",")]).distinct().count()
    results.append(("unique", name, table, failed))


def expect_match(name: str, sql_a: str, sql_b: str) -> None:
    """Dos consultas que devuelven un solo valor dan el mismo resultado (conciliación)."""
    a, b = spark.sql(sql_a).first()[0], spark.sql(sql_b).first()[0]
    results.append(("match", name, f"{a} vs {b}", int(a != b)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1 · Expectativas
# MAGIC Declara **al menos 8 expectativas** sobre las tablas Silver y Gold, usando los tres tipos.
# MAGIC Deben cubrir:
# MAGIC - unicidad de llaves
# MAGIC - campos obligatorios no nulos
# MAGIC - rangos y dominios válidos (montos, `status`, `risk_score`...)
# MAGIC - integridad referencial (por ejemplo, todo `merchant_id` de `silver.transactions` existe en `silver.merchants`)
# MAGIC - conciliación entre capas (por ejemplo, totales de Gold vs Silver)
# MAGIC
# MAGIC Ejemplo de uso: `expect_rows("montos positivos", t("silver", "transactions"), "amount_cop > 0")`

# COMMAND ----------

# TODO 1


# COMMAND ----------

report = spark.createDataFrame(results, "type STRING, expectation STRING, target STRING, failed_rows LONG") \
    .withColumn("passed", F.col("failed_rows") == 0) \
    .withColumn("checked_at", F.current_timestamp())
report.write.mode("append").saveAsTable(t("gold", "dq_report"))
display(report)

# COMMAND ----------

check(len(results) >= 8, "al menos 8 expectativas")
check({r[0] for r in results} == {"rows", "unique", "match"}, "se usan los tres tipos de expectativa")
check(all(r[3] == 0 for r in results), "todas las expectativas se cumplen")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el informe
# MAGIC 1. Escribe una expectativa sobre `bronze.transactions` (por ejemplo, montos positivos). ¿Se cumple? ¿Por qué no tiene sentido exigirla en Bronze?
# MAGIC 2. ¿Qué debería pasar con el pipeline si una expectativa de Gold falla: detenerse, alertar o seguir? Da un ejemplo de cada caso.
# MAGIC 3. `gold.dq_report` es *append*. ¿Qué análisis te permite hacer a lo largo del tiempo?
