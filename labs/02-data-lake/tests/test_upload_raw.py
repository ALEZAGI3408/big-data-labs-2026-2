"""Autoevaluación de upload_raw.py (no necesita AWS). No modifiques este archivo."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from upload_raw import group_by_partition, partition_prefix, to_json_lines  # noqa: E402


def test_partition_prefix_formato_hive():
    assert (
        partition_prefix("transactions", "2026-10-03T14:22:01")
        == "raw/transactions/year=2026/month=10/day=03/"
    )


def test_partition_prefix_rellena_con_ceros():
    assert (
        partition_prefix("fraud_events", "2026-01-09T00:00:00.123456")
        == "raw/fraud_events/year=2026/month=01/day=09/"
    )


def test_json_lines_un_objeto_por_linea():
    records = [{"id": 1, "city": "Medellín"}, {"id": 2, "city": "Bogotá"}]
    lines = to_json_lines(records).strip().split("\n")
    assert len(lines) == 2
    assert [json.loads(line) for line in lines] == records


def test_json_lines_conserva_tildes():
    assert "Medellín" in to_json_lines([{"city": "Medellín"}])


def test_group_by_partition_no_pierde_registros():
    records = [
        {"transaction_id": "T1", "timestamp": "2026-10-01T08:00:00"},
        {"transaction_id": "T2", "timestamp": "2026-10-01T23:59:59"},
        {"transaction_id": "T3", "timestamp": "2026-10-02T00:00:00"},
    ]
    groups = group_by_partition("transactions", records)
    assert len(groups) == 2
    assert sum(len(v) for v in groups.values()) == len(records)
