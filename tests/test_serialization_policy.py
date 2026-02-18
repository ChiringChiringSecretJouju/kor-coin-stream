from __future__ import annotations

from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic


def test_ticker_topic_uses_requested_value_when_avro_forced_off() -> None:
    resolved, reason = resolve_use_avro_for_topic("ticker-data.korea", requested_use_avro=False)
    assert resolved is False
    assert reason == "requested"


def test_json_only_topic_forces_json() -> None:
    resolved, reason = resolve_use_avro_for_topic(
        "monitoring.batch.performance", requested_use_avro=True
    )
    assert resolved is False
    assert reason == "json_only_topic"


def test_unmatched_topic_uses_requested_value() -> None:
    resolved, reason = resolve_use_avro_for_topic("custom.topic", requested_use_avro=True)
    assert resolved is True
    assert reason == "requested"
