"""
Sube los datos generados (data/*.json) a la zona raw del Data Lake.

Uso: python labs/02-data-lake/upload_raw.py [--data-dir data/]

La zona raw guarda los datos tal como llegan, pero organizados con
particiones estilo Hive (year=/month=/day=) para que Glue y Athena
puedan descubrirlas y podarlas.

Completa los TODO y valida tu lógica sin AWS con:
    uv run pytest labs/02-data-lake/tests
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import boto3
from datalake_utils import get_bucket
from rich.console import Console

console = Console()

# dataset → campo de fecha que define la partición
DATASETS = {
    "transactions": "timestamp",
    "fraud_events": "detected_at",
}


# ── TODO 1: prefijo de partición ─────────────────────────────────────────────
# Devuelve el prefijo S3 de la partición diaria a la que pertenece un registro.
# Formato esperado (mes y día con dos dígitos):
#   raw/<dataset>/year=YYYY/month=MM/day=DD/
# Ejemplo: partition_prefix("transactions", "2026-10-03T14:22:01")
#          → "raw/transactions/year=2026/month=10/day=03/"
def partition_prefix(dataset: str, iso_timestamp: str) -> str:
    raise NotImplementedError("TODO 1: partition_prefix")


# ── TODO 2: formato JSON Lines ───────────────────────────────────────────────
# generate_data.py produce un arreglo JSON ([{...}, {...}]), pero Glue y
# Athena esperan JSON Lines: un objeto JSON por línea, sin corchetes ni comas
# entre registros. Convierte la lista de registros a ese formato.
# Conserva las tildes (ensure_ascii=False).
def to_json_lines(records: list[dict]) -> str:
    raise NotImplementedError("TODO 2: to_json_lines")


# ── Dado ─────────────────────────────────────────────────────────────────────

def group_by_partition(dataset: str, records: list[dict]) -> dict[str, list[dict]]:
    date_field = DATASETS[dataset]
    groups: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        groups[partition_prefix(dataset, record[date_field])].append(record)
    return groups


def upload(data_dir: Path) -> None:
    s3 = boto3.client("s3")
    bucket = get_bucket()
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    console.print(f"Bucket: [bold]{bucket}[/bold]")

    for dataset in DATASETS:
        records = json.loads((data_dir / f"{dataset}.json").read_text())
        groups = group_by_partition(dataset, records)
        for prefix, items in groups.items():
            s3.put_object(
                Bucket=bucket,
                Key=f"{prefix}part-{run_id}.json",
                Body=to_json_lines(items).encode("utf-8"),
            )
        console.print(f"[green]✓ {dataset}: {len(records)} registros en {len(groups)} particiones[/green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data", type=Path)
    args = parser.parse_args()
    upload(args.data_dir)
