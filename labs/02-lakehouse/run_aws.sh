#!/usr/bin/env bash
# ================================================================
# Ejecuta el pipeline de un lote en AWS: sube los jobs, ingesta el lote en S3
# y corre los 4 jobs de Glue en orden, esperando a que cada uno termine.
#
# Uso (desde la raíz del repositorio, con el stack ya desplegado):
#   bash labs/02-lakehouse/run_aws.sh <carpeta-del-lote> <ingest-date>
#   bash labs/02-lakehouse/run_aws.sh data/batch1 2026-09-30
# ================================================================
set -euo pipefail

BATCH_DIR=${1:?Falta la carpeta del lote, ej. data/batch1}
INGEST_DATE=${2:?Falta la fecha de ingesta, ej. 2026-09-30}
LAB=labs/02-lakehouse

BUCKET=$(aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev \
  --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucket'].OutputValue" --output text)
echo "Bucket: $BUCKET"

echo "━━━ subiendo jobs ━━━"
aws s3 cp "$LAB/jobs/" "s3://$BUCKET/scripts/" --recursive --exclude "*" --include "*.py" --quiet

echo "━━━ ingest ━━━"
uv run python "$LAB/ingest.py" --batch-dir "$BATCH_DIR" --ingest-date "$INGEST_DATE" --base "s3://$BUCKET"

for job in bronze silver gold audit; do
  echo -e "\n━━━ $job ━━━"
  run_id=$(aws glue start-job-run --job-name "paytrack-$job" \
    --arguments "{\"--INGEST_DATE\":\"$INGEST_DATE\"}" --query JobRunId --output text)
  while true; do
    read -r state error < <(aws glue get-job-run --job-name "paytrack-$job" --run-id "$run_id" \
      --query "JobRun.[JobRunState,ErrorMessage]" --output text)
    case "$state" in
      STARTING|RUNNING|WAITING|STOPPING) printf "."; sleep 15 ;;
      SUCCEEDED) echo " ✓ $state"; break ;;
      *)
        echo -e " ✗ $state\n$error"
        echo "Revisa los logs en CloudWatch: /aws-glue/jobs/error y /aws-glue/jobs/output (run $run_id)"
        exit 1 ;;
    esac
  done
done

echo -e "\nPipeline completo para el lote $INGEST_DATE"
