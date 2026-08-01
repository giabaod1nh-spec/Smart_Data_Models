from __future__ import annotations

import sys
from types import SimpleNamespace

from de.kafka_raw.consumer import ConsumerState, RawKafkaConsumer
from de.kafka_raw.metrics import Metrics
from de.kafka_raw.offset_tracker import OffsetTracker


class _TopicPartition:
    def __init__(self, topic: str, partition: int, offset: int = -1):
        self.topic = topic
        self.partition = partition
        self.offset = offset


class _FakeKafka:
    def __init__(self, high: int, committed: int):
        self.high = high
        self.committed_offset = committed

    def get_watermark_offsets(self, _tp, **_kwargs):
        return 0, self.high

    def committed(self, tps, **_kwargs):
        return [_TopicPartition(tps[0].topic, tps[0].partition, self.committed_offset)]


class _FakeLedger:
    def __init__(self, max_offset: int):
        self.max_offset = max_offset

    def max_completed_offset(self, _topic: str, _partition: int):
        return self.max_offset


class _AliveThread:
    @staticmethod
    def is_alive() -> bool:
        return True


class _Repo:
    @staticmethod
    def ping() -> bool:
        return True


def _consumer(monkeypatch, *, high: int, committed: int, ledger_max: int):
    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka",
        SimpleNamespace(TopicPartition=_TopicPartition),
    )
    c = RawKafkaConsumer.__new__(RawKafkaConsumer)
    c.settings = SimpleNamespace(
        watermark_sample_interval_sec=0,
        cutover_max_lag=0,
        commit_stale_sec=300,
        client_id="raw-test",
        group_id="raw-group",
    )
    c.metrics = Metrics()
    c.offsets = OffsetTracker()
    c.ledger = _FakeLedger(ledger_max)
    c._consumer = _FakeKafka(high, committed)
    c._assigned = {("traffic.entity-events.v2", 0)}
    c._last_watermark_sample = 0.0
    c._thread = _AliveThread()
    c.repo = _Repo()
    c.migrations_ok = True
    c.schemas_ok = True
    c.state = ConsumerState.READY
    return c


def test_cutover_ready_when_durable_position_reaches_watermark(monkeypatch):
    c = _consumer(monkeypatch, high=11, committed=11, ledger_max=10)
    c._maybe_refresh_partition_offsets()
    health = c.health()
    assert health["cutover_ready"] is True
    assert health["partition_offsets"][0]["lag"] == 0
    assert health["partition_offsets"][0]["commit_ahead_of_durable"] is False


def test_cutover_not_ready_on_lag_or_commit_ahead(monkeypatch):
    c = _consumer(monkeypatch, high=15, committed=12, ledger_max=10)
    c._maybe_refresh_partition_offsets()
    health = c.health()
    assert health["cutover_ready"] is False
    assert health["partition_offsets"][0]["lag"] == 4
    assert health["partition_offsets"][0]["commit_ahead_of_durable"] is True
