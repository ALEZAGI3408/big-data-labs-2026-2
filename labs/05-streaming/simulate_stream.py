"""
Simulador de transacciones de PayTrack en tiempo real hacia Kinesis.

Uso:
    python labs/05-streaming/simulate_stream.py --rate 10 --duration 300

Genera transacciones normales e inyecta tres patrones de fraude:
  - amount_spike:    montos de más de 5M COP
  - foreign_country: transacciones fuera de Colombia
  - rapid_sequence:  ráfagas de 6 a 8 transacciones del mismo cliente en segundos

Completa los TODO y valida tu lógica sin AWS con:
    uv run pytest labs/05-streaming/tests
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3
from rich.console import Console

console = Console()

CUSTOMERS = [f"C{i:06d}" for i in range(1, 501)]
MERCHANTS = ["M001", "M002", "M003", "M004", "M005", "M006", "M007", "M008"]
PAYMENT_METHODS = ["credit_card", "debit_card", "nequi", "daviplata", "pse"]
FOREIGN_COUNTRIES = ["US", "PA", "BR", "RU", "NG"]


def make_txn(customer_id: str | None = None, **overrides) -> dict:
    txn = {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12]}",
        "customer_id": customer_id or random.choice(CUSTOMERS),
        "merchant_id": random.choice(MERCHANTS),
        "amount_cop": round(random.lognormvariate(10.5, 1.0), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "country": "CO",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return {**txn, **overrides}


def next_events() -> list[dict]:
    """Devuelve una transacción normal o, a veces, un caso de fraude."""
    r = random.random()
    if r < 0.01:
        return [make_txn(amount_cop=round(random.uniform(5e6, 2e7), 2))]
    if r < 0.02:
        return [make_txn(country=random.choice(FOREIGN_COUNTRIES))]
    if r < 0.025:
        customer = random.choice(CUSTOMERS)
        return [make_txn(customer) for _ in range(random.randint(6, 8))]
    return [make_txn()]


# ── TODO 1: partition key ────────────────────────────────────────────────────
# Kinesis asigna cada registro a un shard según el hash de su partition key, y
# solo garantiza el orden DENTRO de un shard. Elige la partition key de modo que
# la regla rapid_sequence de la Lambda funcione (piensa qué transacciones deben
# llegar juntas y en orden) y que la carga se reparta entre los shards.
def partition_key(txn: dict) -> str:
    raise NotImplementedError("TODO 1: partition_key")


# ── TODO 2: reintentos ───────────────────────────────────────────────────────
# put_records NO es todo o nada: puede aceptar unos registros y rechazar otros
# (por ejemplo, si un shard supera su límite de escritura). La respuesta trae
# FailedRecordCount y, en "Records", un resultado por registro EN EL MISMO ORDEN
# del envío; los fallidos tienen la llave "ErrorCode".
#
# Envía `records` y reintenta SOLO los fallidos, hasta `max_attempts` intentos
# en total, esperando entre intentos con backoff exponencial
# (base_delay, 2*base_delay, 4*base_delay...). Devuelve cuántos registros
# quedaron sin enviar después del último intento.
def put_with_retries(kinesis, stream: str, records: list[dict],
                     max_attempts: int = 4, base_delay: float = 0.2) -> int:
    raise NotImplementedError("TODO 2: put_with_retries")


# ── Dado ─────────────────────────────────────────────────────────────────────

def simulate(stream: str, rate: int, duration: int) -> None:
    kinesis = boto3.client("kinesis")
    sent = failed = 0
    end = time.time() + duration
    console.print(f"Enviando ~{rate} eventos/s a [bold]{stream}[/bold] durante {duration}s (Ctrl+C para parar)")

    while time.time() < end:
        tick = time.time()
        events = [e for _ in range(rate) for e in next_events()]
        records = [{"Data": json.dumps(e).encode(), "PartitionKey": partition_key(e)} for e in events]
        failed += put_with_retries(kinesis, stream, records)
        sent += len(records)
        console.print(f"  enviados: {sent} · perdidos: {failed}", end="\r")
        time.sleep(max(0.0, 1 - (time.time() - tick)))

    console.print(f"\n[green]✓ {sent} eventos enviados, {failed} perdidos[/green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", default="paytrack-fraud-stream-dev")
    parser.add_argument("--rate", type=int, default=10, help="transacciones por segundo")
    parser.add_argument("--duration", type=int, default=300, help="segundos")
    args = parser.parse_args()
    simulate(args.stream, args.rate, args.duration)
