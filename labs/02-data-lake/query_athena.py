"""
Ejecuta en Athena las consultas de sql/athena_queries.sql y compara cuántos
datos escanea cada una (en Athena pagas por dato escaneado).

Uso: python labs/02-data-lake/query_athena.py [--only q1_raw,q1_processed]

Formato del archivo SQL: cada sentencia va precedida de un comentario
`-- name: <nombre>` y termina en `;`. Puedes usar ${BUCKET} en el SQL.
Los resultados se guardan en results/<nombre>.csv
"""

import argparse
import csv
import re
import time
from pathlib import Path

import boto3
from datalake_utils import get_bucket
from rich.console import Console
from rich.table import Table

console = Console()

SQL_FILE = Path(__file__).parent / "sql" / "athena_queries.sql"
RESULTS_DIR = Path("results")
WORKGROUP = "paytrack-wg"
DATABASE = "paytrack_db"


def parse_queries(text: str) -> list[tuple[str, str]]:
    blocks = re.split(r"^--\s*name:\s*(\S+)\s*$", text, flags=re.MULTILINE)[1:]
    queries = []
    for name, body in zip(blocks[::2], blocks[1::2]):
        sql = re.sub(r"--[^\n]*", "", body).strip().rstrip(";").strip()
        queries.append((name, sql))
    return queries


def clear_ctas_location(s3, bucket: str, sql: str) -> None:
    """Un CTAS falla si su external_location tiene datos: la vaciamos antes."""
    match = re.search(r"external_location\s*=\s*'s3://[^/]+/([^']+)'", sql)
    if match:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=match.group(1)).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])


def run(athena, sql: str) -> dict:
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(execution["Status"].get("StateChangeReason", state))

    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue", "") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    return {"stats": execution["Statistics"], "rows": rows}


def main(only: set[str] | None) -> None:
    athena, s3 = boto3.client("athena"), boto3.client("s3")
    bucket = get_bucket()
    RESULTS_DIR.mkdir(exist_ok=True)

    table = Table(title="Consultas Athena")
    for col in ("Consulta", "Filas", "MB escaneados", "Tiempo (ms)"):
        table.add_column(col)

    for name, sql in parse_queries(SQL_FILE.read_text()):
        if only and name not in only:
            continue
        if not sql:
            table.add_row(name, "[yellow]sin implementar[/yellow]", "", "")
            continue
        sql = sql.replace("${BUCKET}", bucket)
        if name.startswith("ctas_"):
            clear_ctas_location(s3, bucket, sql)
        try:
            result = run(athena, sql)
        except RuntimeError as e:
            table.add_row(name, f"[red]error: {e}[/red]", "", "")
            continue
        with open(RESULTS_DIR / f"{name}.csv", "w", newline="") as f:
            csv.writer(f).writerows(result["rows"])
        stats = result["stats"]
        table.add_row(
            name,
            str(max(len(result["rows"]) - 1, 0)),  # sin encabezado
            f"{stats['DataScannedInBytes'] / 1e6:.3f}",
            str(stats["EngineExecutionTimeInMillis"]),
        )

    console.print(table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="nombres separados por coma")
    args = parser.parse_args()
    main(set(args.only.split(",")) if args.only else None)
