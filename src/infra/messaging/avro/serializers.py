"""
Avro 직렬화/역직렬화 구현

confluent-kafka 공식 라이브러리를 사용한 Avro 직렬화기를 제공합니다.
Schema Registry와 연동하여 스키마 진화를 지원합니다.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka.schema_registry import (
    AsyncSchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import (
    AsyncAvroDeserializer as ConfluentAsyncAvroDeserializer,
)
from confluent_kafka.schema_registry.avro import (
    AsyncAvroSerializer as ConfluentAsyncAvroSerializer,
)

from src.common.logger import PipelineLogger
from src.config.settings import kafka_settings
from src.infra.messaging.avro.utils.serde_utiles import create_value_context

logger = PipelineLogger.get_logger("avro_serializers", "avro")


def get_registry_client() -> AsyncSchemaRegistryClient:
    """프로세스 단위 단일 AsyncSchemaRegistryClient 인스턴스 생성/재사용."""
    return AsyncSchemaRegistryClient(
        {
            "url": kafka_settings.schema_register,
            "cache.capacity": 1000,  # 스키마 캐시 용량
            "cache.latest.ttl.sec": 300,  # 최신 스키마 캐시 TTL (5분)
        }
    )


def _fixed_subject_name_strategy(subject: str):
    """Return subject strategy that always resolves to the configured subject."""

    def _strategy(_ctx, _schema_name: str) -> str:
        return subject

    return _strategy


def _serialize_dict(obj: object, _ctx: object) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    raise TypeError(f"Expected dict payload for Avro serialization, got {type(obj)!r}")


def _deserialize_dict(obj: dict[str, Any], _ctx: object) -> object:
    return obj


@dataclass(slots=True)
class AsyncBaseAvroHandler:
    """
    Avro 스키마 연결 및 관리 부모 클래스

    공통 기능:
    - Schema Registry 연결 관리
    - confluent-kafka 클라이언트 초기화
    - 스키마는 직렬화기가 자동으로 Registry에서 조회
    """

    subject: str
    _confluent_client: AsyncSchemaRegistryClient | None = field(default=None, init=False)
    _initialized: bool = field(default=False, init=False)
    _init_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    async def ensure_connection(self) -> AsyncSchemaRegistryClient:
        """Schema Registry 연결 (스키마 조회는 직렬화기가 담당)"""
        if self._initialized and self._confluent_client:
            return self._confluent_client

        # 싱글-플라이트 초기화 가드
        async with self._init_lock:
            if self._initialized and self._confluent_client:
                return self._confluent_client

            # 단일 Schema Registry 클라이언트 재사용
            self._confluent_client = get_registry_client()
            self._initialized = True

            logger.debug(f"Schema Registry 연결 완료: subject={self.subject}")

            return self._confluent_client


@dataclass(slots=True)
class AsyncAvroSerializer(AsyncBaseAvroHandler):
    """
    Avro 직렬화 전용 클래스

    부모 클래스에서 연결 관리, 자신은 직렬화만 담당
    """

    _confluent_serializer: ConfluentAsyncAvroSerializer | None = field(default=None, init=False)

    async def ensure_serializer(self) -> ConfluentAsyncAvroSerializer:
        """직렬화기 초기화 (자식 전용 로직)"""
        if self._confluent_serializer:
            return self._confluent_serializer

        # 부모에서 Schema Registry 연결만 수행
        confluent_client = await self.ensure_connection()

        # 직렬화기 생성 (await 필수 - 비동기 생성자)
        # 직렬화기 생성 (await 필수 - 비동기 생성자)
        # schema_str 없이 생성 → subject 기반으로 Registry에서 자동 조회
        # use.latest.version=True → 최신 스키마 사용
        # subject.name.strategy: 토픽 이름을 그대로 Subject로 사용 (suffix 없음)

        self._confluent_serializer = ConfluentAsyncAvroSerializer(
            schema_registry_client=confluent_client,
            to_dict=_serialize_dict,
            conf={
                "use.latest.version": True,  # 최신 스키마 자동 조회
                "auto.register.schemas": False,  # 등록된 스키마만 사용
                # 고정 subject 기반 전략 (topic과 독립적인 계약 유지)
                "subject.name.strategy": _fixed_subject_name_strategy(self.subject),
            },
        )

        logger.debug(f"Avro 직렬화기 초기화 완료: subject={self.subject}")
        return self._confluent_serializer

    async def serialize_async(self, obj: dict[str, Any]) -> bytes:
        """
        비동기적으로 객체를 Avro 바이트로 직렬화합니다.
        CPU 집약적인 작업을 별도 스레드로 오프로드합니다.

        Args:
            obj: 직렬화할 객체 (딕셔너리)

        Returns:
            Confluent Wire Format으로 인코딩된 바이트
        """
        serializer = await self.ensure_serializer()
        context = create_value_context(topic=self.subject)
        encoded = serializer(obj, context.to_confluent_context())
        if encoded is None:
            raise ValueError("Avro serialization returned None")
        return encoded


@dataclass(slots=True)
class AsyncAvroDeserializer(AsyncBaseAvroHandler):
    """
    Avro 역직렬화 전용 클래스

    부모 클래스에서 연결 관리, 자신은 역직렬화만 담당
    """

    _confluent_deserializer: ConfluentAsyncAvroDeserializer | None = field(default=None, init=False)

    async def ensure_deserializer(self) -> ConfluentAsyncAvroDeserializer:
        """역직렬화기 초기화 (자식 전용 로직)"""
        if self._confluent_deserializer:
            return self._confluent_deserializer

        # 부모에서 연결 및 스키마 로딩
        confluent_client = await self.ensure_connection()

        # 역직렬화기 생성 (await 필수 - 비동기 생성자)
        self._confluent_deserializer = ConfluentAsyncAvroDeserializer(
            schema_registry_client=confluent_client,
            from_dict=_deserialize_dict,
        )

        return self._confluent_deserializer

    async def deserialize_async(self, data: bytes) -> dict[str, Any]:
        """
        비동기적으로 Avro 바이트를 객체로 역직렬화합니다.
        CPU 집약적인 작업을 별도 스레드로 오프로드합니다.

        Args:
            data: Confluent Wire Format 바이트

        Returns:
            역직렬화된 딕셔너리 객체
        """
        deserializer = await self.ensure_deserializer()
        context = create_value_context(topic=self.subject)
        decoded = deserializer(data, context.to_confluent_context())
        if decoded is None:
            raise ValueError("Avro deserialization returned None")
        if isinstance(decoded, dict):
            return decoded
        if isinstance(decoded, Mapping):
            return dict(decoded)
        raise TypeError(f"Expected dict payload from Avro deserializer, got {type(decoded)!r}")


def create_avro_serializer(subject: str) -> AsyncAvroSerializer:
    """
    Avro 직렬화기를 생성합니다.

    Args:
        subject: 스키마 주제명
    Returns:
        AsyncAvroSerializer 인스턴스
    """
    return AsyncAvroSerializer(subject=subject)


def create_avro_deserializer(subject: str) -> AsyncAvroDeserializer:
    """
    Avro 역직렬화기를 생성합니다.

    Args:
        subject: 스키마 주제명
    Returns:
        AsyncAvroDeserializer 인스턴스
    """
    return AsyncAvroDeserializer(subject=subject)
