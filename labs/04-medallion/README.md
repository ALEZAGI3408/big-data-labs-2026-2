# Lab 4 · Arquitectura Medallion con Spark

**Semana 4 · Entrega:** domingo 9 de noviembre de 2026

## Objetivo

Implementar la arquitectura Medallion completa (Bronze → Silver → Gold) para PayTrack con Apache Spark en Databricks Free Edition, usando Delta Lake en todas las capas, sobre datos **con problemas de calidad reales**: duplicados, montos inválidos, llaves faltantes y tipos inconsistentes.

## Arquitectura

```
landing (volumen)        Bronze                  Silver                         Gold
───────────────────      ───────────────────     ───────────────────────────    ──────────────────────────
transactions.json   →    bronze.transactions  →  silver.transactions         →  gold.daily_merchant_summary
                                              ↘  silver.transactions_quarantine
fraud_events.json   →    bronze.fraud_events  →  silver.fraud_events         →  gold.fraud_weekly_report
customers.json      →    bronze.customers     →  silver.customers (PII enmascarada)
merchants.json      →    bronze.merchants     →  silver.merchants            →  gold.hourly_volume
                                                                                 gold.dq_report
```

## Definición de capas para PayTrack

| Capa | Descripción | Escritura | Retención sugerida |
|------|-------------|-----------|--------------------|
| **Bronze** | Datos tal como llegan + metadatos de ingesta. Esquema inferido. | Append | 2 años |
| **Silver** | Una fila por llave, tipada, validada, con PII protegida. Lo inválido va a cuarentena. | Overwrite / Merge | 1 año |
| **Gold** | Agregados de negocio, uno por pregunta, listos para dashboards y ML. | Overwrite | 90 días |

## Preparación

1. Genera datos **con errores de calidad**:
   ```bash
   uv run python labs/shared/generate_data.py --transactions 50000 --dirty-rate 0.02 --output data/
   ```
2. En Databricks Free Edition, trae los notebooks con **Workspace → Create → Git folder** (URL de tu fork).
3. Ejecuta `00_config` y sube los 4 archivos de `data/` al volumen **workspace → bronze → landing**.
4. Ejecuta los notebooks **en orden**. Completa los `TODO`; las celdas de verificación (`✓` / `✗`) no se modifican.

## Notebooks

| Notebook | Tema | TODO |
|----------|------|------|
| `00_config` | Configuración y utilidades (dado) | — |
| `01_bronze_ingestion` | Ingesta append-only con metadatos de auditoría | 1 |
| `02_silver_cleaning` | Deduplicación, tipado, reglas de calidad, cuarentena y enmascaramiento de PII | 4 |
| `03_gold_aggregations` | Tres tablas de negocio con conciliación contra Silver | 3 |
| `04_data_quality` | Expectativas declarativas y reporte de calidad | 1 |

Cada notebook termina con **preguntas para el informe**.

> 💡 Ejecuta el pipeline completo dos veces. Bronze debe crecer; Silver y Gold deben quedar idénticas. Esa es la prueba de que tu pipeline es idempotente.

## Entregables

- [ ] Los 4 notebooks exportados como `.html` con todas las celdas ejecutadas y todas las verificaciones en `✓`
- [ ] Captura de la tabla `gold.dq_report`
- [ ] Dashboard en Databricks con al menos una visualización por tabla Gold (captura)
- [ ] Diagrama del pipeline Medallion hecho en [draw.io](https://app.diagrams.net/): tablas por capa, transformaciones y reglas de calidad
- [ ] Informe en PDF (máx. 3 páginas) con las respuestas a las preguntas de los notebooks y las decisiones de diseño de cada capa

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Bronze y Silver correctas (verificaciones en `✓`) | 30% |
| Gold correctas y conciliadas (verificaciones en `✓`) | 20% |
| Expectativas de calidad y reporte | 15% |
| Respuestas y decisiones de diseño | 20% |
| Dashboard, diagrama y calidad del informe | 15% |
