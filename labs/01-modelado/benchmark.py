"""
Compara el rendimiento de las 3 consultas en OLTP vs esquema estrella y
mide el efecto de cada índice de la tabla de hechos.

Uso: python labs/01-modelado/benchmark.py [--runs 5] [--out labs/01-modelado/resultados]

Guarda el plan completo de cada consulta (EXPLAIN ANALYZE, BUFFERS) en --out
e imprime la mediana del tiempo de ejecución de --runs ejecuciones.
"""

import argparse
import os
import re
import statistics
from pathlib import Path

import psycopg2
from rich.console import Console
from rich.table import Table

console = Console()

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/paytrack")
SQL_DIR = Path(__file__).parent / "sql"

FACT_INDEXES = {
    "idx_fact_date": "CREATE INDEX idx_fact_date ON dw.fact_transactions (date_key)",
    "idx_fact_customer": "CREATE INDEX idx_fact_customer ON dw.fact_transactions (customer_key)",
    "idx_fact_merchant": "CREATE INDEX idx_fact_merchant ON dw.fact_transactions (merchant_key)",
}
SCHEMA_INDEXES = {"idx_fact_date", "idx_fact_customer"}  # los de star_schema.sql


def read_queries(path: Path) -> list[str]:
    text = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
    statements = [s.strip() for s in text.split(";")][:3]
    return [re.sub(r"^\s*EXPLAIN\s+ANALYZE\s+", "", s, flags=re.IGNORECASE) for s in statements]


def explain(cur, sql: str, runs: int) -> tuple[float, str]:
    """Devuelve la mediana del Execution Time (ms) y el último plan."""
    times, plan = [], ""
    cur.execute(sql)  # calentamiento: carga las páginas en shared_buffers
    for _ in range(runs):
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
        plan = "\n".join(r[0] for r in cur.fetchall())
        times.append(float(re.search(r"Execution Time: ([\d.]+) ms", plan).group(1)))
    return statistics.median(times), plan


def node_summary(plan: str) -> str:
    """Tipos de scan y join que aparecen en el plan."""
    kinds = re.findall(
        r"(Seq Scan|Index Only Scan|Index Scan|Bitmap Heap Scan|Hash Join|"
        r"Nested Loop|Merge Join|Sort|HashAggregate|GroupAggregate)", plan)
    return ", ".join(dict.fromkeys(kinds))


def set_indexes(cur, keep: set[str]) -> None:
    for name, ddl in FACT_INDEXES.items():
        cur.execute(f"DROP INDEX IF EXISTS dw.{name}")
        if name in keep:
            cur.execute(ddl)
    cur.execute("ANALYZE dw.fact_transactions")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out", type=Path, default=Path(__file__).parent / "resultados")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("ANALYZE")
    cur.execute("SELECT COUNT(*) FROM oltp.transactions")
    n = cur.fetchone()[0]

    oltp = read_queries(SQL_DIR / "oltp_problems.sql")
    olap = read_queries(SQL_DIR / "olap_queries.sql")

    # 1. OLTP vs OLAP (con todos los índices)
    set_indexes(cur, set(FACT_INDEXES))
    table = Table(title=f"OLTP vs OLAP — {n:,} transacciones (mediana de {args.runs}, ms)")
    for col in ("Consulta", "OLTP ms", "OLAP ms", "Nodos OLTP", "Nodos OLAP"):
        table.add_column(col)
    for i, (q_oltp, q_olap) in enumerate(zip(oltp, olap), start=1):
        t_oltp, p_oltp = explain(cur, q_oltp, args.runs)
        t_olap, p_olap = explain(cur, q_olap, args.runs)
        (args.out / f"q{i}_oltp_{n}.txt").write_text(p_oltp, encoding="utf-8")
        (args.out / f"q{i}_olap_{n}.txt").write_text(p_olap, encoding="utf-8")
        table.add_row(f"Q{i}", f"{t_oltp:.2f}", f"{t_olap:.2f}",
                      node_summary(p_oltp), node_summary(p_olap))
    console.print(table)

    # 2. Experimento de índices sobre las consultas OLAP
    scenarios = {"sin índices": set(), **{f"solo {k}": {k} for k in FACT_INDEXES},
                 "todos": set(FACT_INDEXES)}
    table = Table(title="OLAP: efecto de los índices de fact_transactions (ms)")
    table.add_column("Escenario")
    for i in range(1, 4):
        table.add_column(f"Q{i}")
    for label, keep in scenarios.items():
        set_indexes(cur, keep)
        row, plans = [], []
        for i, q in enumerate(olap, start=1):
            t, p = explain(cur, q, args.runs)
            row.append(f"{t:.2f}  ({node_summary(p)})")
            plans.append(f"===== Q{i}\n{p}")
        slug = label.replace(" ", "_").replace("í", "i")
        (args.out / f"indices_{slug}_{n}.txt").write_text("\n\n".join(plans), encoding="utf-8")
        table.add_row(label, *row)
    set_indexes(cur, SCHEMA_INDEXES)  # deja los índices de star_schema.sql
    console.print(table)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
