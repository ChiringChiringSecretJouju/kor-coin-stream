from __future__ import annotations

from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic


def test_avro_required_topic_forces_avro() -> None:
    resolved, reason = resolve_use_avro_for_topic("ticker-data.korea", requested_use_avro=False)
    assert resolved is True
    assert reason == "avro_required_topic"


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
