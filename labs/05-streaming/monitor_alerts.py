"""
Monitorea las alertas de fraude que la Lambda escribe en DynamoDB y mide la
latencia de detección (tiempo entre la transacción y la alerta).

Uso: python labs/05-streaming/monitor_alerts.py [--table fraud-alerts-dev]
"""

import argparse
import statistics
import time

import boto3
from rich.console import Console

console = Console()


def scan_all(table) -> list[dict]:
    items, kwargs = [], {}
    while True:
        page = table.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def monitor(table_name: str, interval: int) -> None:
    table = boto3.resource("dynamodb").Table(table_name)
    seen: set[str] = set()
    console.print(f"Monitoreando [bold]{table_name}[/bold] cada {interval}s (Ctrl+C para salir)\n")

    while True:
        items = scan_all(table)
        new = sorted((i for i in items if i["alert_id"] not in seen), key=lambda i: i["detected_at"])
        for a in new:
            seen.add(a["alert_id"])
            color = "red" if a["risk_score"] >= 0.9 else "yellow"
            console.print(
                f"[{color}]⚠ {a['detected_at'][11:19]} {a['customer_id']} "
                f"${float(a['amount_cop']):>14,.0f} riesgo {float(a['risk_score']):.2f} "
                f"{','.join(a['patterns'])} ({a['latency_ms']} ms)[/{color}]"
            )
        if items:
            latencies = sorted(int(i["latency_ms"]) for i in items)
            p95 = latencies[int(0.95 * (len(latencies) - 1))]
            console.print(
                f"[dim]{len(items)} alertas · latencia promedio {statistics.mean(latencies):,.0f} ms "
                f"· p95 {p95:,} ms[/dim]"
            )
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="fraud-alerts-dev")
    parser.add_argument("--interval", type=int, default=5)
    args = parser.parse_args()
    try:
        monitor(args.table, args.interval)
    except KeyboardInterrupt:
        pass
