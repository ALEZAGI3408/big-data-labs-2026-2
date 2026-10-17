# Lab 5 · Streaming de eventos de fraude con Kinesis

**Semana 5 · Entrega:** domingo 16 de noviembre de 2026

## Objetivo

Construir un pipeline de streaming que detecte fraude en las transacciones de PayTrack **segundos** después de que ocurren, con **Amazon Kinesis Data Streams**, **AWS Lambda**, **DynamoDB**, **SNS** y un archivo histórico en **S3** consultable con Athena.

> ⚠️ Kinesis Data Streams **no** está en la capa gratuita de AWS.
> Costo estimado si eliminas los recursos el mismo día: **< USD 0.50**. Si los olvidas encendidos, el stream cuesta ~USD 22 al mes.

## Arquitectura

```
simulate_stream.py        Kinesis Data Streams        Lambda                 Destinos
──────────────────   →    paytrack-fraud-stream  →    fraud-detector    →    DynamoDB  fraud-alerts   (alertas)
 ~10 txn/s                 2 shards                    reglas + score    →    S3        archive/...    (todo, para Athena)
 + fraudes inyectados      partition key = ?           lotes de ≤ 5 s    →    SNS       email          (riesgo ≥ 0.9)
                                                                                    ↑
                                                              monitor_alerts.py ────┘ (latencia de detección)
```

## Qué está dado y qué construyes tú

| Archivo | Estado | Qué hace |
|---------|--------|----------|
| `serverless.yml` | ✅ Dado | Stream, Lambda, DynamoDB, S3 y SNS |
| `functions/fraud_detector.py` | ✏️ **TODO** | Las 3 reglas de fraude y el puntaje de riesgo |
| `simulate_stream.py` | ✏️ **TODO** | Partition key y reintentos de `put_records` |
| `tests/test_streaming.py` | ✅ Dado | Autoevaluación sin AWS |
| `monitor_alerts.py` | ✅ Dado | Muestra alertas y latencia en vivo |
| `sql/archive_queries.sql` | ✏️ **TODO** | Análisis del histórico con Athena |

## Pasos

### 5.1 Lógica de detección (sin AWS)

Completa los TODO de `functions/fraud_detector.py` y `simulate_stream.py` hasta que todos los tests pasen:

```bash
uv run pytest labs/05-streaming/tests -v
```

**No despliegues hasta tener los tests en verde**: cada minuto con el stream encendido cuesta.

### 5.2 Desplegar la infraestructura

```bash
set -a && source .envs/aws.env && set +a
cd labs/05-streaming
serverless deploy --stage dev --param="alertEmail=tu@correo.com"
cd -
```

Confirma la suscripción en el correo que te llega de AWS SNS.

### 5.3 Simular eventos en tiempo real

```bash
# Terminal 1: alertas en vivo
uv run python labs/05-streaming/monitor_alerts.py

# Terminal 2: ~10 transacciones/s durante 5 minutos
uv run python labs/05-streaming/simulate_stream.py --rate 10 --duration 300
```

Revisa también los logs de la Lambda (**CloudWatch → Log groups → /aws/lambda/paytrack-fraud-detector-dev**): cada invocación imprime cuántos registros procesó y cuántas alertas generó por patrón.

### 5.4 Consultar el histórico en S3

Completa y ejecuta `sql/archive_queries.sql` en la consola de Athena.

### 5.5 Experimentos

1. Ejecuta el simulador con `--rate 50`. ¿Cambia la latencia? ¿Aparecen más alertas de `rapid_sequence`? ¿Son fraudes reales o falsos positivos?
2. Cambia temporalmente tu `partition_key` para que devuelva `transaction_id`, ejecuta de nuevo y compara cuántas ráfagas detecta la Lambda. Explica el resultado.

### 5.6 Limpieza (obligatoria)

```bash
ARCHIVE=$(aws cloudformation describe-stacks --stack-name paytrack-streaming-dev \
  --query "Stacks[0].Outputs[?OutputKey=='ArchiveBucket'].OutputValue" --output text)
aws s3 rm s3://$ARCHIVE --recursive
cd labs/05-streaming && serverless remove --stage dev
```

## Preguntas para el informe

1. ¿Qué partition key elegiste y por qué? ¿Qué pasaría con la regla `rapid_sequence` si los eventos de un cliente se repartieran entre shards?
2. ¿Por qué `put_records` puede fallar parcialmente? ¿Qué garantía de entrega ofrece tu simulador: *at-most-once*, *at-least-once* o *exactly-once*? ¿Y la Lambda, si falla a mitad de un lote?
3. ¿Qué limitación tiene detectar ráfagas mirando solo el lote actual de la Lambda? ¿Cómo la resolverías (piensa en estado compartido entre invocaciones)?
4. Reporta la latencia promedio y p95 de detección. ¿Qué componentes la explican? ¿Qué parámetro de `serverless.yml` la afecta más?
5. Compara este pipeline con el batch de los Labs 2–4: ¿qué preguntas de negocio de PayTrack solo se pueden responder en streaming?

## Entregables

- [ ] Tu `fraud_detector.py`, `simulate_stream.py` y `archive_queries.sql`
- [ ] Captura de `pytest -v` con todos los tests en verde
- [ ] Captura de CloudWatch Logs con invocaciones de la Lambda
- [ ] Captura de `monitor_alerts.py` con al menos 10 alertas y la latencia
- [ ] Captura de un email de alerta de SNS
- [ ] Resultados de las 3 consultas Athena
- [ ] Diagrama de la arquitectura hecho en [draw.io](https://app.diagrams.net/)
- [ ] Informe en PDF (máx. 3 páginas) con las respuestas y los experimentos
- [ ] **Recursos eliminados** (paso 5.6)

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Reglas de detección y simulador correctos (tests en verde) | 30% |
| Pipeline desplegado y funcionando (evidencias) | 20% |
| Consultas sobre el histórico | 15% |
| Experimentos y respuestas | 25% |
| Diagrama y calidad del informe | 10% |
