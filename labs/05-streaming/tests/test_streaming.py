"""Autoevaluación del Lab 5 (no necesita AWS). No modifiques este archivo."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

LAB = Path(__file__).parents[1]
sys.path.insert(0, str(LAB))
sys.path.insert(0, str(LAB / "functions"))

import fraud_detector as fd  # noqa: E402
import simulate_stream as sim  # noqa: E402

T0 = datetime(2026, 11, 10, 15, 0, 0, tzinfo=timezone.utc)


def txn(tid="T1", customer="C000001", seconds=0, amount=50_000.0, country="CO"):
    return {
        "transaction_id": tid,
        "customer_id": customer,
        "amount_cop": amount,
        "country": country,
        "timestamp": (T0 + timedelta(seconds=seconds)).isoformat(),
    }


# ── Lambda: reglas ───────────────────────────────────────────────────────────

def test_amount_spike():
    assert fd.is_amount_spike(txn(amount=5_000_000))
    assert fd.is_amount_spike(txn(amount=12_000_000))
    assert not fd.is_amount_spike(txn(amount=4_999_999.99))


def test_foreign_country():
    assert fd.is_foreign_country(txn(country="US"))
    assert not fd.is_foreign_country(txn(country="CO"))


def test_rapid_sequence_detecta_rafaga():
    burst = [txn(f"B{i}", seconds=i * 5) for i in range(6)]  # 6 en 25 s
    assert fd.find_rapid_sequences(burst) == {"B5"}


def test_rapid_sequence_respeta_la_ventana():
    spread = [txn(f"S{i}", seconds=i * 20) for i in range(6)]  # 6 en 100 s
    assert fd.find_rapid_sequences(spread) == set()


def test_rapid_sequence_por_cliente_y_desordenado():
    burst = [txn(f"B{i}", seconds=i) for i in range(7)]
    others = [txn(f"O{i}", customer=f"OTHER{i}", seconds=i) for i in range(10)]
    batch = list(reversed(burst)) + others
    assert fd.find_rapid_sequences(batch) == {"B5", "B6"}


def test_risk_score():
    assert fd.risk_score([]) == 0
    assert fd.risk_score(["rapid_sequence"]) == 0.7
    assert fd.risk_score(["amount_spike", "foreign_country"]) == 0.8
    assert fd.risk_score(["amount_spike", "foreign_country", "rapid_sequence"]) == 0.94


def test_detect_fraud_integra_las_reglas():
    batch = [txn("OK"), txn("BIG", customer="C2", amount=9e6, country="PA")]
    result = {t["transaction_id"]: t for t in fd.detect_fraud(batch, now=T0 + timedelta(seconds=2))}
    assert result["OK"]["patterns"] == [] and result["OK"]["risk_score"] == 0
    assert result["BIG"]["patterns"] == ["amount_spike", "foreign_country"]
    assert result["BIG"]["risk_score"] == 0.8
    assert result["BIG"]["latency_ms"] == 2000


# ── Simulador ────────────────────────────────────────────────────────────────

def test_partition_key_agrupa_por_lo_correcto():
    a1, a2 = sim.make_txn("C000001"), sim.make_txn("C000001")
    b = sim.make_txn("C000002")
    assert sim.partition_key(a1) == sim.partition_key(a2), "una ráfaga debe llegar al mismo shard"
    assert sim.partition_key(a1) != sim.partition_key(b), "la carga debe repartirse entre shards"


class FakeKinesis:
    """Rechaza los registros indicados en sus primeros `fail_times` intentos."""

    def __init__(self, fail_keys, fail_times):
        self.fail_keys, self.fail_times, self.calls = set(fail_keys), fail_times, []

    def put_records(self, StreamName, Records):
        self.calls.append([r["PartitionKey"] for r in Records])
        failing = len(self.calls) <= self.fail_times
        results = [
            {"ErrorCode": "ProvisionedThroughputExceededException"}
            if failing and r["PartitionKey"] in self.fail_keys else {"SequenceNumber": "1", "ShardId": "s"}
            for r in Records
        ]
        return {"FailedRecordCount": sum("ErrorCode" in x for x in results), "Records": results}


def records(*keys):
    return [{"Data": b"{}", "PartitionKey": k} for k in keys]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(sim.time, "sleep", lambda s: None)


def test_reintenta_solo_los_fallidos():
    k = FakeKinesis(fail_keys={"b"}, fail_times=1)
    assert sim.put_with_retries(k, "s", records("a", "b", "c")) == 0
    assert k.calls == [["a", "b", "c"], ["b"]]


def test_respeta_max_attempts():
    k = FakeKinesis(fail_keys={"a"}, fail_times=99)
    assert sim.put_with_retries(k, "s", records("a", "b"), max_attempts=3) == 1
    assert len(k.calls) == 3


def test_backoff_exponencial(monkeypatch):
    delays = []
    monkeypatch.setattr(sim.time, "sleep", delays.append)
    k = FakeKinesis(fail_keys={"a"}, fail_times=99)
    sim.put_with_retries(k, "s", records("a"), max_attempts=4, base_delay=0.1)
    assert delays == pytest.approx([0.1, 0.2, 0.4])
