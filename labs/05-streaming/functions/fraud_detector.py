"""
Lambda de detección de fraude en tiempo real.

Recibe lotes de transacciones desde Kinesis, aplica las reglas de fraude y:
  - guarda una alerta en DynamoDB por cada transacción sospechosa
  - publica en SNS las alertas de riesgo alto
  - archiva todas las transacciones (enriquecidas) en S3 para análisis con Athena

Completa los TODO (las reglas de detección) y valida tu lógica sin AWS con:
    uv run pytest labs/05-streaming/tests
"""

import base64
import json
import os
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

ALERTS_TABLE = os.environ.get("ALERTS_TABLE", "fraud-alerts-dev")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

# Parámetros de las reglas
AMOUNT_SPIKE_COP = 5_000_000        # monto a partir del cual una transacción es sospechosa
HOME_COUNTRY = "CO"
RAPID_SEQUENCE_MAX = 5              # más de 5 transacciones...
RAPID_SEQUENCE_WINDOW = timedelta(seconds=60)  # ...del mismo cliente dentro de 60 segundos

# Probabilidad de fraude que aporta cada patrón por sí solo
PATTERN_RISK = {
    "amount_spike": 0.6,
    "foreign_country": 0.5,
    "rapid_sequence": 0.7,
}
SNS_MIN_RISK = 0.9


# ── TODO 1: monto inusual ────────────────────────────────────────────────────
# True si amount_cop es mayor o igual a AMOUNT_SPIKE_COP.
def is_amount_spike(txn: dict) -> bool:
    raise NotImplementedError("TODO 1: is_amount_spike")


# ── TODO 2: país extranjero ──────────────────────────────────────────────────
# True si la transacción se hizo en un país distinto de HOME_COUNTRY.
def is_foreign_country(txn: dict) -> bool:
    raise NotImplementedError("TODO 2: is_foreign_country")


# ── TODO 3: secuencia rápida ─────────────────────────────────────────────────
# Recibe TODAS las transacciones del lote y devuelve el conjunto de
# transaction_id que hacen parte de una ráfaga: una transacción se marca si,
# contando desde RAPID_SEQUENCE_WINDOW antes de su timestamp hasta su timestamp
# (incluida ella misma), el mismo cliente tiene MÁS de RAPID_SEQUENCE_MAX transacciones.
#   - El campo "timestamp" es un texto ISO 8601 (usa datetime.fromisoformat).
#   - Las transacciones del lote pueden venir desordenadas.
# Pregunta para el informe: ¿por qué esta regla solo funciona si todas las
# transacciones de un cliente llegan a la misma invocación de la Lambda?
def find_rapid_sequences(txns: list[dict]) -> set[str]:
    raise NotImplementedError("TODO 3: find_rapid_sequences")


# ── TODO 4: puntaje de riesgo ────────────────────────────────────────────────
# Combina los patrones detectados en un risk score entre 0 y 1, suponiendo que
# cada patrón es una evidencia independiente:
#     risk = 1 - Π (1 - PATTERN_RISK[p])   para cada patrón p detectado
# Sin patrones el riesgo es 0. Redondea a 4 decimales.
# Ejemplo: ["amount_spike", "foreign_country"] → 1 - (0.4 * 0.5) = 0.8
def risk_score(patterns: list[str]) -> float:
    raise NotImplementedError("TODO 4: risk_score")


# ── Dado ─────────────────────────────────────────────────────────────────────

def decode_records(event: dict) -> list[dict]:
    """Kinesis entrega cada registro en base64."""
    return [json.loads(base64.b64decode(r["kinesis"]["data"])) for r in event["Records"]]


def detect_fraud(txns: list[dict], now: datetime) -> list[dict]:
    rapid = find_rapid_sequences(txns)
    enriched = []
    for txn in txns:
        patterns = []
        if is_amount_spike(txn):
            patterns.append("amount_spike")
        if is_foreign_country(txn):
            patterns.append("foreign_country")
        if txn["transaction_id"] in rapid:
            patterns.append("rapid_sequence")
        latency = now - datetime.fromisoformat(txn["timestamp"])
        enriched.append({
            **txn,
            "patterns": patterns,
            "risk_score": risk_score(patterns),
            "detected_at": now.isoformat(),
            "latency_ms": int(latency.total_seconds() * 1000),
        })
    return enriched


_clients: dict = {}


def client(service: str):
    if service not in _clients:
        _clients[service] = boto3.client(service) if service != "dynamodb" else boto3.resource(service)
    return _clients[service]


def archive(enriched: list[dict], now: datetime) -> None:
    key = f"archive/year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/{uuid.uuid4()}.json"
    body = "\n".join(json.dumps(t, ensure_ascii=False) for t in enriched)
    client("s3").put_object(Bucket=ARCHIVE_BUCKET, Key=key, Body=body.encode("utf-8"))


def save_alerts(alerts: list[dict]) -> None:
    with client("dynamodb").Table(ALERTS_TABLE).batch_writer() as batch:
        for a in alerts:
            batch.put_item(Item={
                "alert_id": str(uuid.uuid4()),
                "transaction_id": a["transaction_id"],
                "customer_id": a["customer_id"],
                "patterns": a["patterns"],
                "risk_score": Decimal(str(a["risk_score"])),
                "amount_cop": Decimal(str(a["amount_cop"])),
                "event_time": a["timestamp"],
                "detected_at": a["detected_at"],
                "latency_ms": a["latency_ms"],
            })


def notify(alerts: list[dict]) -> None:
    for a in alerts:
        if a["risk_score"] >= SNS_MIN_RISK and ALERT_TOPIC_ARN:
            client("sns").publish(
                TopicArn=ALERT_TOPIC_ARN,
                Subject=f"PayTrack: posible fraude ({a['risk_score']:.0%})",
                Message=json.dumps(a, ensure_ascii=False, indent=2),
            )


def handler(event, context):
    now = datetime.now(timezone.utc)
    enriched = detect_fraud(decode_records(event), now)
    alerts = [t for t in enriched if t["patterns"]]

    archive(enriched, now)
    save_alerts(alerts)
    notify(alerts)

    by_pattern = defaultdict(int)
    for a in alerts:
        for p in a["patterns"]:
            by_pattern[p] += 1
    summary = {"records": len(enriched), "alerts": len(alerts), "by_pattern": dict(by_pattern)}
    print(json.dumps(summary))
    return summary
