"""
JSON 기반 Kafka 클라이언트

고성능 JSON 직렬화/역직렬화를 지원하는 비동기 Producer/Consumer 래퍼입니다.
orjson을 사용하여 3-5배 빠른 JSON 처리 성능을 제공합니다.
"""

import orjson
from datetime import datetime
from typing import Any
from dataclasses import dataclass

from src.infra.messaging.clients.cb.base import AsyncProducerBase, AsyncConsumerBase
from src.infra.messaging.clients.cb.config import (
    json_producer_config,
    json_consumer_config,
)
from src.infra.messaging.serializers.unified_serializer import (
    create_unified_serializers,
)


def _datetime_serializer(obj):
    """JSON 직렬화를 위한 datetime 처리"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def default_value_serializer(value: Any) -> bytes:
    return orjson.dumps(value, default=_datetime_serializer)


def default_key_serializer(value: Any) -> bytes:
    # 문자열 키 우선, 아니라면 그대로 bytes라고 가정
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    # 그 외는 문자열로 변환하여 사용
    return str(value).encode("utf-8")


def default_value_deserializer(raw: bytes) -> Any:
    return orjson.loads(raw)


def default_key_deserializer(raw: bytes | None) -> Any | None:
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except Exception:
        return raw


@dataclass(slots=True)
class AsyncProducerWrapper(AsyncProducerBase):
    """JSON 기반 고성능 비동기 Producer 래퍼 - 큐 기반 아키텍처 (부모 클래스에서 제공)"""

    # JSON 전용 직렬화기 (init=False)
    value_serializer: Any = default_value_serializer
    key_serializer: Any = default_key_serializer

    async def _initialize_serializers(self) -> None:
        """JSON 직렬화기 초기화"""
        self.value_serializer, self.key_serializer = create_unified_serializers()

    async def _serialize_message(
        self, value: Any, key: Any = None
    ) -> tuple[bytes, bytes | None]:
        """JSON 메시지 직렬화"""
        if not self.value_serializer or not self.key_serializer:
            raise RuntimeError("JSON serializers not initialized")

        # 통합 직렬화 처리 (바이트/JSON 자동 구분)
        serialized_value = self.value_serializer(value)
        serialized_key = self.key_serializer(key)

        return serialized_value, serialized_key


@dataclass(slots=True)
class AsyncConsumerWrapper(AsyncConsumerBase):
    """JSON 기반 고성능 비동기 Consumer 래퍼 - 전용 poll 스레드 기반 (부모 클래스에서 제공)"""

    # JSON 전용 역직렬화기 (init=False)
    value_deserializer: Any = default_value_deserializer
    key_deserializer: Any = default_key_deserializer

    async def _initialize_deserializers(self) -> None:
        """JSON 역직렬화기 초기화"""
        pass

    def _deserialize_message(self, raw_msg) -> dict[str, Any]:
        """JSON 메시지 역직렬화"""
        if not self.value_deserializer or not self.key_deserializer:
            raise RuntimeError("JSON deserializers not initialized")

        # 정상 메시지를 역직렬화
        deserialized_value = self.value_deserializer(raw_msg.value())
        deserialized_key = self.key_deserializer(raw_msg.key())

        return {
            "key": deserialized_key,
            "value": deserialized_value,
            "topic": raw_msg.topic(),
        }


def create_producer(**overrides: Any) -> AsyncProducerWrapper:
    """JSON 기반 비동기 Producer 래퍼 생성 - 성능 최적화 ProducerConfigDomain 기반"""
    cfg = json_producer_config(**overrides)
    return AsyncProducerWrapper(config=cfg)


def create_consumer(topic: list[str], **overrides: Any) -> AsyncConsumerWrapper:
    """JSON 기반 비동기 Consumer 래퍼 생성 - 성능 최적화 ConsumerConfigDomain 기반"""
    overrides.setdefault("auto.offset.reset", "latest")
    cfg = json_consumer_config(**overrides)
    return AsyncConsumerWrapper(topic=topic, config=cfg)
