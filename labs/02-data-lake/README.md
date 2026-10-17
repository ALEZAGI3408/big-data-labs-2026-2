# Lab 2 · Data Lake en AWS S3

**Semana 2 · Entrega:** domingo 26 de octubre de 2026

## Objetivo

Construir el Data Lake de PayTrack en AWS S3 con tres zonas (raw, processed, curated), catalogarlo con AWS Glue y consultarlo con Amazon Athena sin mover los datos. Medir con datos reales cuánto ahorran el particionamiento y el formato columnar.

## Arquitectura

```
S3 Bucket: paytrack-datalake-{student_id}-{account_id}
├── raw/                     ← JSON Lines tal como llega, particionado por día
│   ├── transactions/year=2026/month=10/day=03/
│   └── fraud_events/year=2026/month=10/day=03/
├── processed/               ← Parquet tipado y deduplicado, particionado por mes
│   ├── transactions/year=2026/month=10/
│   └── fraud_events/year=2026/month=10/
├── curated/                 ← agregados de negocio (CTAS de Athena)
│   └── daily_summary/
├── scripts/                 ← código del Glue job
└── athena-results/          ← resultados temporales de Athena
```

## Qué está dado y qué construyes tú

| Archivo | Estado | Qué hace |
|---------|--------|----------|
| `infra/serverless.yml` | ✏️ **TODO** | Bucket, Glue y Athena están dados; tú defines el ciclo de vida de cada zona |
| `upload_raw.py` | ✏️ **TODO** | Particionado Hive y conversión a JSON Lines |
| `tests/test_upload_raw.py` | ✅ Dado | Autoevaluación de `upload_raw.py` sin AWS |
| `glue_jobs/raw_to_processed.py` | ✏️ **TODO** | Tipado, deduplicación y escritura en Parquet particionado |
| `sql/athena_queries.sql` | ✏️ **TODO** | Consultas comparativas y CTAS de la zona curated |
| `query_athena.py` | ✅ Dado | Ejecuta las consultas y reporta MB escaneados |

## Requisitos

- Cuenta de AWS con la CLI configurada y tu `.envs/aws.env` completo (incluye `STUDENT_ID`)
- Serverless Framework v3: `npm install -g serverless@3`
- Datos generados en el Lab 1 (`data/`)

Carga tus variables de entorno en la terminal antes de empezar:
```bash
set -a && source .envs/aws.env && set +a
```

## Pasos

### 1. Políticas de ciclo de vida (TODO en `infra/serverless.yml`)

Define cuánto vive cada zona y en qué clase de almacenamiento. Luego despliega:

```bash
cd labs/02-data-lake/infra
serverless deploy --stage dev
cd -
export DATALAKE_BUCKET=$(aws cloudformation describe-stacks --stack-name paytrack-datalake-dev \
  --query "Stacks[0].Outputs[?OutputKey=='DataLakeBucket'].OutputValue" --output text)
```

### 2. Zona raw (TODO en `upload_raw.py`)

Completa `partition_prefix` y `to_json_lines`, valida sin AWS y sube los datos:

```bash
uv run pytest labs/02-data-lake/tests
uv run python labs/02-data-lake/upload_raw.py
```

Cataloga la zona raw:
```bash
aws glue start-crawler --name paytrack-raw-crawler
# espera ~2 min hasta que el estado vuelva a READY
aws glue get-crawler --name paytrack-raw-crawler --query Crawler.State
```

### 3. Zona processed (TODO en `glue_jobs/raw_to_processed.py`)

Completa las 3 funciones del job, súbelo y ejecútalo:

```bash
aws s3 cp labs/02-data-lake/glue_jobs/raw_to_processed.py s3://$DATALAKE_BUCKET/scripts/
aws glue start-job-run --job-name paytrack-raw-to-processed
# sigue el estado (RUNNING → SUCCEEDED / FAILED):
aws glue get-job-runs --job-name paytrack-raw-to-processed --max-items 1 \
  --query "JobRuns[0].[JobRunState,ErrorMessage]"
```

El job valida su propia salida: si termina en `FAILED`, revisa el mensaje y los logs en CloudWatch (`/aws-glue/jobs/error`). Cuando termine bien, cataloga la zona:

```bash
aws glue start-crawler --name paytrack-processed-crawler
```

### 4. Consultas y zona curated (TODO en `sql/athena_queries.sql`)

```bash
uv run python labs/02-data-lake/query_athena.py
# o solo algunas consultas:
uv run python labs/02-data-lake/query_athena.py --only q1_raw,q1_processed
```

El script muestra los MB escaneados de cada consulta y guarda los resultados en `results/`.

### 5. Limpieza (obligatoria)

```bash
aws s3 rm s3://$DATALAKE_BUCKET --recursive
cd labs/02-data-lake/infra && serverless remove --stage dev
```

## Preguntas para el informe

1. ¿Cuántos MB escanean `q1_raw` vs `q1_processed` y `q2_raw` vs `q2_processed`? Explica qué parte de la diferencia se debe a la **poda de particiones** y qué parte al **formato columnar**.
2. En raw particionamos por día y en processed por mes. ¿Qué pasaría con la consulta q1 y con el número de archivos si particionaras processed por día? ¿Y por `merchant_category`?
3. ¿Por qué el job lee raw pero re-deriva `year` y `month` desde `event_ts` en lugar de reutilizar las carpetas?
4. Justifica tus políticas de ciclo de vida y estima el costo mensual de almacenamiento de cada zona con 500K transacciones diarias durante un año.
5. ¿Qué ganaste al crear la zona curated? ¿Qué pierdes?

## Entregables

- [ ] Tu `serverless.yml`, `upload_raw.py`, `raw_to_processed.py` y `athena_queries.sql`
- [ ] Captura de `pytest` en verde
- [ ] Captura de la estructura de carpetas del bucket (consola S3) y del catálogo de tablas en Glue
- [ ] Captura del job de Glue en estado `SUCCEEDED`
- [ ] Tabla de `query_athena.py` con los MB escaneados de todas las consultas
- [ ] Diagrama de la arquitectura del Data Lake hecho en [draw.io](https://app.diagrams.net/)
- [ ] Informe en PDF (máx. 3 páginas) respondiendo las preguntas
- [ ] **Recursos eliminados** al terminar (paso 5)

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Zona raw: particionado y formato correctos (`pytest` en verde) | 15% |
| Zona processed: job de Glue correcto (validación en verde) | 25% |
| Consultas Athena y zona curated | 25% |
| Análisis de costo/rendimiento y políticas de ciclo de vida | 25% |
| Calidad del informe y del diagrama | 10% |

## ⚠️ Costos estimados

Con free tier activo y limpiando al terminar el lab: **< USD 0.50**

| Servicio | Costo estimado |
|----------|---------------|
| S3 (< 1 GB) | Gratis (free tier) |
| Glue Crawler (≈ 4 ejecuciones) | ~USD 0.30 |
| Glue job (2 workers, ~2 min por ejecución) | ~USD 0.03 por ejecución |
| Athena (< 100 MB escaneados) | < USD 0.01 |
