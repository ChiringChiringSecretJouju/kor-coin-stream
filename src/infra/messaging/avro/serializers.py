"""
Avro 직렬화/역직렬화 구현

confluent-kafka 공식 라이브러리를 사용한 Avro 직렬화기를 제공합니다.
Schema Registry와 연동하여 스키마 진화를 지원합니다.
"""

from __future__ import annotations

import asyncio
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
from src.config.settings import KafkaSettings
from src.infra.messaging.avro.utils.serde_utiles import create_value_context

logger = PipelineLogger.get_logger("avro_serializers", "avro")


async def get_registry_client() -> AsyncSchemaRegistryClient:
    """프로세스 단위 단일 AsyncSchemaRegistryClient 인스턴스 생성/재사용."""
    return AsyncSchemaRegistryClient(
        {
            "url": KafkaSettings.SCHEMA_REGISTRY_URL,
            "cache.capacity": 1000,  # 스키마 캐시 용량
            "cache.latest.ttl.sec": 300,  # 최신 스키마 캐시 TTL (5분)
        }
    )


def context_topic_from_subject(subject: str) -> str:
    if subject.endswith("-value"):
        return subject[:-6]
    if subject.endswith("-key"):
        return subject[:-4]
    return subject


@dataclass(slots=True)
class AsyncBaseAvroHandler:
    """
    Avro 스키마 연결 및 관리 부모 클래스

    공통 기능:
    - Schema Registry 연결 관리
    - 스키마 로딩 및 캐싱
    - confluent-kafka 클라이언트 초기화
    """

    subject: str
    schema_id: int | None = field(default=None, init=False)
    schema_content: str | None = field(default=None, init=False)
    _confluent_client: AsyncSchemaRegistryClient | None = field(
        default=None, init=False
    )
    _initialized: bool = field(default=False, init=False)
    _init_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)

    async def ensure_connection(self) -> AsyncSchemaRegistryClient:
        """스키마 연결 및 로딩 (공통 로직)"""
        if self._initialized and self._confluent_client:
            return self._confluent_client

        # 싱글-플라이트 초기화 가드
        if self._init_lock is None:
            self._init_lock = asyncio.Lock()

        async with self._init_lock:
            if self._initialized and self._confluent_client:
                return self._confluent_client

            # 단일 Schema Registry 클라이언트 재사용
            self._confluent_client = await get_registry_client()

            # 공식 비동기 클라이언트로 최신 스키마 조회
            schema = await self._confluent_client.get_latest_version(self.subject)
            self.schema_id = schema.schema_id
            self.schema_content = schema.schema.schema_str

            self._initialized = True
            logger.debug(
                f"Avro 스키마 로드 완료: subject={self.subject}, id={self.schema_id}"
            )

            return self._confluent_client


@dataclass(slots=True)
class AsyncAvroSerializer(AsyncBaseAvroHandler):
    """
    Avro 직렬화 전용 클래스

    부모 클래스에서 연결 관리, 자신은 직렬화만 담당
    """

    _confluent_serializer: ConfluentAsyncAvroSerializer | None = field(
        default=None, init=False
    )

    async def ensure_serializer(self) -> ConfluentAsyncAvroSerializer:
        """직렬화기 초기화 (자식 전용 로직)"""
        if self._confluent_serializer:
            return self._confluent_serializer

        # 부모에서 연결 및 스키마 로딩
        self._confluent_client = await self.ensure_connection()

        # 직렬화기만 생성
        self._confluent_serializer = ConfluentAsyncAvroSerializer(
            schema_registry_client=self._confluent_client,
            schema_str=self.schema_content,
            to_dict=lambda obj, ctx: obj,  # 이미 dict 형태이므로 그대로 반환
            conf={
                "use.latest.version": True,  # 최신 스키마 버전 사용
                "normalize.schemas": True,  # 스키마 정규화
                "use.latest.with.metadata": True,  # 메타데이터와 함께 최신 버전 사용
                "auto.register.schemas": False,  # 스키마 자동 등록
            },
        )

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
        context = create_value_context(topic=context_topic_from_subject(self.subject))
        return await serializer(obj, context.to_confluent_context())


@dataclass(slots=True)
class AsyncAvroDeserializer(AsyncBaseAvroHandler):
    """
    Avro 역직렬화 전용 클래스

    부모 클래스에서 연결 관리, 자신은 역직렬화만 담당
    """

    _confluent_deserializer: ConfluentAsyncAvroDeserializer | None = field(
        default=None, init=False
    )

    async def ensure_deserializer(self) -> ConfluentAsyncAvroDeserializer:
        """역직렬화기 초기화 (자식 전용 로직)"""
        if self._confluent_deserializer:
            return self._confluent_deserializer

        # 부모에서 연결 및 스키마 로딩
        confluent_client = await self.ensure_connection()

        # 역직렬화기만 생성
        self._confluent_deserializer = ConfluentAsyncAvroDeserializer(
            schema_registry_client=confluent_client,
            from_dict=lambda obj, ctx: obj,  # 이미 dict 형태이므로 그대로 반환
            conf={
                "use.latest.version": True,  # 최신 스키마 버전 사용
                "normalize.schemas": True,  # 스키마 정규화
                "use.latest.with.metadata": True,  # 메타데이터와 함께 최신 버전 사용
            },
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
        context = create_value_context(topic=context_topic_from_subject(self.subject))
        return await deserializer(data, context.to_confluent_context())


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
