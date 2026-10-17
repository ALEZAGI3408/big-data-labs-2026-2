# Lab 3 · Lakehouse con Delta Lake

**Semana 3 · Entrega:** domingo 2 de noviembre de 2026

## Objetivo

Convertir los datos de PayTrack en una tabla **Delta Lake** y experimentar con lo que un data lake de solo Parquet no puede hacer: transacciones ACID, constraints, time travel, evolución de esquema y upserts con `MERGE`.

> **¿Por qué Delta Lake?**
> Delta Lake agrega un log de transacciones sobre archivos Parquet. Ese log es la única fuente de verdad sobre qué archivos forman cada versión de la tabla, y de ahí salen la atomicidad, el aislamiento entre lectores y escritores, y la posibilidad de viajar en el tiempo.

## Pre-requisitos

1. Cuenta en [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (gratuita).
2. Datos generados en el Lab 1 (`data/transactions.json`).

## Preparación

1. **Trae los notebooks a Databricks.** En tu workspace: **Workspace → Create → Git folder** y pega la URL de tu fork del repositorio. Los archivos de `labs/03-lakehouse/notebooks/` aparecen como notebooks.
2. Abre y ejecuta `00_config`. Crea el esquema `workspace.paytrack` y el volumen `raw`.
3. Sube `data/transactions.json` al volumen: **Catalog → workspace → paytrack → raw → Upload to this volume**.
4. Ejecuta los notebooks **en orden**. Cada uno tiene celdas `TODO` que completas tú, seguidas de celdas de verificación (`✓` / `✗`) que no debes modificar.

> Si algo se daña, vuelve a ejecutar `01_create_delta_table`: reinicia la tabla y puedes empezar de nuevo.

## Notebooks

| Notebook | Tema | TODO |
|----------|------|------|
| `00_config` | Configuración y utilidades (dado) | — |
| `01_create_delta_table` | Crear la tabla Delta y explorar sus metadatos | 2 |
| `02_acid_operations` | `UPDATE`, `DELETE`, constraints y atomicidad | 3 |
| `03_time_travel` | Consultas por versión y por fecha; deshacer un cambio puntual | 4 |
| `04_schema_evolution` | Schema enforcement vs. schema evolution | 2 |
| `05_merge_upserts` | Upserts idempotentes con `MERGE` y compactación con `OPTIMIZE` | 2 |

Cada notebook termina con **preguntas para el informe**.

## Referencias

- [Delta Lake: documentación de Databricks](https://docs.databricks.com/aws/en/delta/)
- [Time travel y `DESCRIBE HISTORY`](https://docs.databricks.com/aws/en/delta/history)
- [`MERGE INTO`](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)
- [Schema evolution](https://docs.databricks.com/aws/en/delta/update-schema)

## Entregables

- [ ] Los 5 notebooks exportados como `.html` (**File → Export → HTML**) con todas las celdas ejecutadas y todas las verificaciones en `✓`
- [ ] Captura del `DESCRIBE HISTORY` final de la tabla
- [ ] Diagrama hecho en [draw.io](https://app.diagrams.net/) que muestre cómo el `_delta_log` relaciona versiones y archivos Parquet a lo largo de las operaciones del lab
- [ ] Informe en PDF (máx. 3 páginas) con las respuestas a las preguntas de cada notebook y una comparación entre el Data Lake del Lab 2 y este lakehouse

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Notebooks completos con todas las verificaciones en `✓` | 50% |
| Respuestas a las preguntas de los notebooks | 30% |
| Diagrama del log de transacciones | 10% |
| Calidad del informe | 10% |
