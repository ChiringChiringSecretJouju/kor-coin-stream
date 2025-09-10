from __future__ import annotations

import json
from typing import Any
from dataclasses import asdict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from config.settings import kafka_settings
from core.dto.internal.mq import ConsumerConfig, ProducerConfig


def default_value_serializer(value: Any) -> bytes:
    # 기본은 JSON 직렬화
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def default_key_serializer(value: Any) -> bytes:
    # 문자열 키 우선, 아니라면 그대로 bytes라고 가정
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    # 그 외는 문자열로 변환하여 사용
    return str(value).encode("utf-8")


def default_value_deserializer(raw: bytes) -> Any:
    # 기본은 JSON 역직렬화
    return json.loads(raw.decode("utf-8"))


def default_key_deserializer(raw: bytes | None) -> Any | None:
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except Exception:
        return raw


def producer_config(**overrides: Any) -> dict[str, Any]:
    """Producer 설정을 dataclass로 구성 후 dict로 반환.

    - dataclass(slots=True)로 정의된 ProducerConfig를 사용해 기본값을 고정
    - asdict로 변환 후 overrides를 적용하여 유연성 보장
    """
    cfg_dc = ProducerConfig(
        bootstrap_servers=kafka_settings.BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        acks=kafka_settings.ACKS,
        linger_ms=kafka_settings.LINGER_MS,
        value_serializer=default_value_serializer,
        key_serializer=default_key_serializer,
    )
    cfg: dict[str, Any] = asdict(cfg_dc)
    cfg.update(overrides)  # 사용자 지정 값으로 덮어쓰기
    return cfg


def consumer_config(**overrides: Any) -> dict[str, Any]:
    """Consumer 설정을 dataclass로 구성 후 dict로 반환."""
    cfg_dc = ConsumerConfig(
        bootstrap_servers=kafka_settings.BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        group_id=kafka_settings.CONSUMER_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset=kafka_settings.AUTO_OFFSET_RESET,
        value_deserializer=default_value_deserializer,
        key_deserializer=default_key_deserializer,
    )
    cfg: dict[str, Any] = asdict(cfg_dc)
    cfg.update(overrides)
    return cfg


def create_producer(**overrides: Any) -> AIOKafkaProducer:
    cfg = producer_config(**overrides)
    return AIOKafkaProducer(**cfg)  # type: ignore[arg-type]


def create_consumer(topic: str, **overrides: Any) -> AIOKafkaConsumer:
    cfg = consumer_config(**overrides)
    return AIOKafkaConsumer(topic, **cfg)  # type: ignore[arg-type]
