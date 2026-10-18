# Lab 2 · Lakehouse end-to-end en AWS

**Entrega:** viernes 6 de noviembre de 2026 · **Peso:** 30%

## Objetivo

Construir el lakehouse de PayTrack de punta a punta: ingestar lotes de datos en S3, procesarlos con la arquitectura Medallion (Bronze → Silver → Gold) en tablas **Delta Lake** con **AWS Glue**, auditar los cambios con **time travel** y consultar el resultado con **Amazon Athena**.

Es la arquitectura que vimos en clase: *S3 + Delta Lake + Glue + Athena*.

## Arquitectura

```
data/batchN/*.json
      │  ingest.py
      ▼
S3 raw/<dataset>/ingest_date=YYYY-MM-DD/        ← cada lote tal como llegó (JSON Lines)
      │  job bronze (Glue)
      ▼
lakehouse/bronze/<dataset>        Delta, todo texto + metadatos, partición ingest_date
      │  job silver (Glue)
      ▼
lakehouse/silver/transactions     Delta, tipada, deduplicada, MERGE por lote
lakehouse/silver/transactions_quarantine   filas que no pasan las reglas de calidad
lakehouse/silver/customers        PII enmascarada
      │  job gold (Glue)                       job audit (Glue, time travel)
      ▼                                              ▼
lakehouse/gold/daily_merchant_summary         lakehouse/gold/status_changes
lakehouse/gold/fraud_weekly_report
lakehouse/gold/hourly_volume
      │
      ▼
Amazon Athena (sql/athena.sql)
```

## El escenario: dos lotes

PayTrack recibe datos en lotes diarios con problemas reales de calidad: duplicados, montos inválidos, clientes faltantes y números que llegan como texto.

- **Lote 1** (`ingest_date = 2026-09-29`): 60 días de historia.
- **Lote 2** (`ingest_date = 2026-09-30`): transacciones nuevas, **contracargos** de transacciones del lote 1 (cambian a `status = 'chargeback'`) y clientes que se mudaron o cambiaron de segmento.

Tu pipeline debe integrar el lote 2 **sin duplicar nada**, actualizando lo que cambió, y debe poder re-ejecutarse sin alterar el resultado.

## Qué está dado y qué construyes tú

| Archivo | Estado | Qué hace |
|---------|--------|----------|
| `ingest.py` | ✏️ **TODO 1–2** | Partición de llegada y formato JSON Lines |
| `jobs/bronze.py` | ✏️ **TODO 1–2** | Metadatos de ingesta y escritura idempotente (`replaceWhere`) |
| `jobs/silver.py` | ✏️ **TODO 1–5** | Deduplicación, tipado, reglas de calidad, `MERGE` y enmascaramiento de PII |
| `jobs/gold.py` | ✏️ **TODO 1–3** | Tres tablas de negocio |
| `jobs/audit.py` | ✏️ **TODO 1–2** | Time travel: qué transacciones cambiaron con el último lote |
| `sql/athena.sql` | ✏️ **TODO 1–4** | Consultas de negocio sobre las tablas Delta |
| `infra/serverless.yml` | ✏️ **TODO** | Todo dado salvo el ciclo de vida de la zona raw |
| `jobs/common.py`, `run_local.sh`, `run_aws.sh`, `tests/` | ✅ Dado | Utilidades, ejecución y autoevaluación |

Cada job termina con **validaciones** (`✓` / `✗`) que no debes modificar: si una falla, el job termina con error y te dice qué revisar.

## Paso 0 · Generar los lotes

```bash
uv sync
uv run python labs/shared/generate_data.py --start-date 2026-08-01 --days 60 \
    --transactions 30000 --dirty-rate 0.02 --output data/batch1
uv run python labs/shared/generate_data.py --start-date 2026-09-30 --days 1 \
    --transactions 2000 --dirty-rate 0.02 --from-batch data/batch1 --id-prefix B2 --output data/batch2
```

## Paso 1 · Desarrollar en local (gratis)

Desarrolla y prueba todo en tu computador antes de tocar AWS. Solo necesitas Docker: el script usa una imagen de Spark con Delta Lake, y los datos quedan en `lakehouse-local/`.

```bash
# Ingesta: valida tus TODO sin Spark
uv run pytest labs/02-lakehouse/tests

# Pipeline completo del lote 1, y luego solo un paso mientras desarrollas
bash labs/02-lakehouse/run_local.sh data/batch1 2026-09-29
bash labs/02-lakehouse/run_local.sh data/batch1 2026-09-29 silver

# Lote 2
bash labs/02-lakehouse/run_local.sh data/batch2 2026-09-30
```

> ✅ **Prueba de idempotencia:** ejecuta cada lote **dos veces**. La segunda vez todas las validaciones deben seguir en verde y la auditoría debe reportar 0 insertadas y 0 actualizadas. Algunos errores (por ejemplo, escribir Bronze con `append`) solo se notan en la segunda ejecución.

Para empezar de cero: `rm -rf lakehouse-local`.

## Paso 2 · Desplegar en AWS

Completa el TODO de `infra/serverless.yml` y despliega:

```bash
set -a && source .envs/aws.env && set +a
cd labs/02-lakehouse/infra && serverless deploy --stage dev && cd -
```

## Paso 3 · Ejecutar el pipeline en AWS

```bash
bash labs/02-lakehouse/run_aws.sh data/batch1 2026-09-29
bash labs/02-lakehouse/run_aws.sh data/batch2 2026-09-30
```

El script sube los jobs a S3, ingesta el lote y ejecuta los 4 jobs de Glue en orden. Si alguno falla, muestra el error; lo que imprime cada job (incluidas las validaciones) queda en CloudWatch: `/aws-glue/jobs/output`.

## Paso 4 · Consultar con Athena

Abre la consola de Athena, elige el workgroup `paytrack-lakehouse-wg` y ejecuta `sql/athena.sql`: primero el DDL dado y luego tus 4 consultas. Anota los MB escaneados.

## Paso 5 · Limpieza (obligatoria)

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev \
  --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucket'].OutputValue" --output text)
aws s3 rm s3://$BUCKET --recursive
cd labs/02-lakehouse/infra && serverless remove --stage dev
```

## Preguntas para el informe

1. **Bronze.** ¿Por qué Bronze lee todo como texto? ¿Qué problema evita `replaceWhere` frente a `append`? Muéstralo con lo que pasó en tu prueba de idempotencia.
2. **Silver.** ¿Cuántas filas quedaron en cuarentena por cada motivo en cada lote? ¿Por qué se deduplica **antes** de aplicar las reglas de calidad?
3. **MERGE.** ¿Por qué la actualización solo debe ocurrir cuando el `status` cambió? ¿Qué reportaría la auditoría si quitaras esa condición y re-ejecutaras un lote?
4. **Time travel.** ¿Cuántos contracargos encontró la auditoría y qué monto revierten? ¿Qué pasaría con esta auditoría después de un `VACUUM`?
5. **PII.** `silver.customers` se actualiza sobrescribiendo los cambios (tipo 1). ¿Qué análisis de negocio se pierde? ¿Cómo lo resolverías con lo que hiciste en el Lab 1?
6. **Consumo.** Compara los MB escaneados del TODO 1 y del TODO 2 de Athena. ¿Por qué existe Gold si Silver ya responde la pregunta? ¿Qué pierdes?
7. **Costos y operación.** Estima el costo mensual de este pipeline con 500K transacciones diarias (S3, Glue y Athena). ¿Por qué no se debe poner una regla de expiración de S3 sobre `lakehouse/`?

## Entregables

- [ ] Tu código: `ingest.py`, los 4 jobs, `athena.sql` y `serverless.yml`
- [ ] Captura de `pytest` en verde
- [ ] Salida de `run_local.sh` para los dos lotes, incluida la segunda ejecución del lote 2 (idempotencia)
- [ ] Captura de los 4 jobs de Glue en `Succeeded` para los dos lotes
- [ ] Resultados y MB escaneados de las 4 consultas de Athena
- [ ] Diagrama de la arquitectura hecho en [draw.io](https://app.diagrams.net/): zonas, tablas por capa, jobs y servicios de AWS
- [ ] Informe en PDF (máx. 5 páginas) con las respuestas a las preguntas
- [ ] **Recursos eliminados** (paso 5)

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Ingesta y Bronze correctos e idempotentes | 15% |
| Silver: calidad, cuarentena, MERGE y PII (validaciones en verde) | 25% |
| Gold y auditoría con time travel (validaciones en verde) | 20% |
| Pipeline ejecutado en AWS y consultas de Athena | 15% |
| Respuestas y análisis del informe | 15% |
| Diagrama y calidad del informe | 10% |

## ⚠️ Costos estimados

Si desarrollas en local y solo usas AWS para la ejecución final: **~USD 1**.

| Servicio | Costo estimado |
|----------|---------------|
| S3 (< 1 GB) | Gratis (free tier) |
| Glue 5.0 (2 workers, ~1–2 min por job; 4 jobs × 2 lotes) | ~USD 0.03 por job |
| Athena (< 100 MB escaneados) | < USD 0.01 |

Cada ejecución de un job de Glue cobra mínimo 1 minuto. Depurar directamente en Glue es lento y cuesta: para eso está `run_local.sh`.
