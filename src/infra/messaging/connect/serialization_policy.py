from __future__ import annotations

from fnmatch import fnmatch

AVRO_REQUIRED_TOPIC_PATTERNS: tuple[str, ...] = (
    "ticker-data.*",
    "trade-data.*",
    "ws.connect_success.*",
    "ws.metrics.reception.*",
    "ws.metrics.processing.*",
    "ws.metrics.quality.*",
)

JSON_ONLY_TOPIC_PATTERNS: tuple[str, ...] = (
    "ws.command",
    "ws.error",
    "ws.dlq",
    "ws.backpressure.events",
    "monitoring.batch.performance",
)


def _matches(topic: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch(topic, pattern) for pattern in patterns)


def is_avro_required_topic(topic: str) -> bool:
    return _matches(topic, AVRO_REQUIRED_TOPIC_PATTERNS)


def is_json_only_topic(topic: str) -> bool:
    return _matches(topic, JSON_ONLY_TOPIC_PATTERNS)


def resolve_use_avro_for_topic(topic: str, requested_use_avro: bool) -> tuple[bool, str]:
    """Resolve serialization mode by fixed topic policy.

    Returns:
        tuple[resolved_use_avro, reason]
    """
    if is_json_only_topic(topic):
        return False, "json_only_topic"
    if is_avro_required_topic(topic):
        return True, "avro_required_topic"
    return requested_use_avro, "requested"
