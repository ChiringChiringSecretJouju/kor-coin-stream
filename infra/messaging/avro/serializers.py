"""
Avro 직렬화/역직렬화 구현

confluent-kafka-python과 호환되는 Avro 직렬화기를 제공합니다.
Schema Registry와 연동하여 스키마 진화를 지원합니다.
"""

from __future__ import annotations

import asyncio
import io
import struct
from typing import Any
from dataclasses import dataclass

import orjson
import fastavro
from common.logger import PipelineLogger
from infra.messaging.avro.schema_registry import SchemaRegistryClient, Schema

logger = PipelineLogger.get_logger("avro_serializers", "avro")

# Confluent Schema Registry 매직 바이트 (0x0)
MAGIC_BYTE = 0


@dataclass(slots=True)
class AvroSerializer:
    """
    Avro 직렬화기

    Confluent Schema Registry와 연동하여 메시지를 Avro 형식으로 직렬화합니다.
    """

    schema_registry_client: SchemaRegistryClient
    subject: str
    schema_id: int | None = None
    schema_str: str | None = None

    def __post_init__(self):
        # Any 사용 사유: Avro 스키마는 중첩된 복잡한 구조를 가질 수 있어 정확한 타입 정의가 어려움
        self._parsed_schema: dict[str, Any] | None = None

    async def ensure_schema(self) -> None:
        """스키마를 로드하고 초기화합니다."""
        if self._parsed_schema is not None:
            return

        if self.schema_str:
            # 직접 제공된 스키마 사용
            self._parsed_schema = orjson.loads(self.schema_str)

            # Schema Registry에 등록 (ID 획득)
            if self.schema_id is None:
                self.schema_id = await self.schema_registry_client.register_schema(
                    self.subject, self.schema_str
                )
        else:
            # Schema Registry에서 최신 스키마 조회
            schema_obj = await self.schema_registry_client.get_latest_schema(
                self.subject
            )
            self.schema_id = schema_obj.id
            self._parsed_schema = orjson.loads(schema_obj.schema)

        logger.debug(
            f"Avro 스키마 로드 완료: subject={self.subject}, id={self.schema_id}"
        )

    def __call__(self, obj: dict[str, Any], ctx: Any = None) -> bytes:
        """
        객체를 Avro 바이트로 직렬화합니다.

        Args:
            obj: 직렬화할 객체 (딕셔너리) - Any 사용 사유: Avro 메시지는 다양한 타입의 필드를 포함할 수 있음
            ctx: 컨텍스트 (사용하지 않음) - Any 사용 사유: confluent-kafka 호환성을 위한 범용 컨텍스트

        Returns:
            Confluent Wire Format으로 인코딩된 바이트

        Raises:
            RuntimeError: 스키마가 로드되지 않은 경우
        """
        if self._parsed_schema is None or self.schema_id is None:
            raise RuntimeError(
                "Schema not loaded. Call 'await serializer.ensure_schema()' before use."
            )

        return self._serialize_sync(obj)

    async def serialize_async(self, obj: dict[str, Any]) -> bytes:
        """
        비동기적으로 객체를 Avro 바이트로 직렬화합니다.
        CPU 집약적인 작업을 별도 스레드로 오프로드합니다.

        Args:
            obj: 직렬화할 객체 (딕셔너리)

        Returns:
            Confluent Wire Format으로 인코딩된 바이트

        Raises:
            RuntimeError: 스키마가 로드되지 않은 경우
        """
        if self._parsed_schema is None or self.schema_id is None:
            raise RuntimeError(
                "Schema not loaded. Call 'await serializer.ensure_schema()' before use."
            )

        # CPU 집약적인 직렬화 작업을 별도 스레드로 오프로드
        return await asyncio.to_thread(self._serialize_sync, obj)

    def _serialize_sync(self, obj: dict[str, Any]) -> bytes:
        """동기 직렬화 (스키마가 이미 로드된 상태)"""
        # fastavro를 사용한 Avro 바이너리 인코딩
        bytes_writer = io.BytesIO()
        fastavro.schemaless_writer(bytes_writer, self._parsed_schema, obj)
        avro_bytes = bytes_writer.getvalue()

        # Confluent Wire Format: [MAGIC_BYTE][SCHEMA_ID][AVRO_DATA]
        wire_format = io.BytesIO()
        wire_format.write(struct.pack("!bI", MAGIC_BYTE, self.schema_id))
        wire_format.write(avro_bytes)

        result = wire_format.getvalue()
        logger.debug(f"Avro 직렬화 완료: size={len(result)} bytes")
        return result


@dataclass(slots=True)
class AvroDeserializer:
    """
    Avro 역직렬화기

    Confluent Wire Format으로 인코딩된 메시지를 역직렬화합니다.
    """

    schema_registry_client: SchemaRegistryClient
    reader_schema_str: str | None = None

    def __post_init__(self):
        # Any 사용 사유: Avro 스키마는 중첩된 복잡한 구조를 가질 수 있어 정확한 타입 정의가 어려움
        self._schema_cache: dict[int, dict[str, Any]] = {}
        self._reader_schema: dict[str, Any] | None = None

        if self.reader_schema_str:
            self._reader_schema = orjson.loads(self.reader_schema_str)

    def __call__(self, data: bytes, ctx: Any = None) -> dict[str, Any]:
        """
        Avro 바이트를 객체로 역직렬화합니다.

        Args:
            data: Confluent Wire Format 바이트
            ctx: 컨텍스트 (사용하지 않음) - Any 사용 사유: confluent-kafka 호환성을 위한 범용 컨텍스트

        Returns:
            역직렬화된 딕셔너리 객체 - Any 사용 사유: Avro 메시지는 다양한 타입의 필드를 포함할 수 있음
        """
        if len(data) < 5:
            raise ValueError("Invalid Confluent Wire Format: too short")

        magic_byte, schema_id = struct.unpack("!bI", data[:5])

        if magic_byte != MAGIC_BYTE:
            raise ValueError(
                f"Invalid magic byte: expected {MAGIC_BYTE}, got {magic_byte}"
            )

        avro_data = data[5:]

        # 스키마 캐시 확인 (동기 버전 - 캐시된 스키마만 사용)
        if schema_id not in self._schema_cache:
            raise RuntimeError(
                f"Schema {schema_id} not in cache. Use deserialize_async() or preload_schema()."
            )

        return self._deserialize_with_schema(avro_data, schema_id)

    async def deserialize_async(self, data: bytes) -> dict[str, Any]:
        """
        비동기적으로 Avro 바이트를 객체로 역직렬화합니다.
        스키마 로드와 CPU 집약적인 작업을 별도 스레드로 오프로드합니다.

        Args:
            data: Confluent Wire Format 바이트

        Returns:
            역직렬화된 딕셔너리 객체
        """
        if len(data) < 5:
            raise ValueError("Invalid Confluent Wire Format: too short")

        magic_byte, schema_id = struct.unpack("!bI", data[:5])

        if magic_byte != MAGIC_BYTE:
            raise ValueError(
                f"Invalid magic byte: expected {MAGIC_BYTE}, got {magic_byte}"
            )

        avro_data = data[5:]

        # 스키마 캐시 확인 및 로드
        if schema_id not in self._schema_cache:
            schema_obj = await self.schema_registry_client.get_schema_by_id(schema_id)
            writer_schema = orjson.loads(schema_obj.schema)
            self._schema_cache[schema_id] = writer_schema

            logger.debug(f"스키마 캐시 추가: id={schema_id}")

        # CPU 집약적인 역직렬화 작업을 별도 스레드로 오프로드
        return await asyncio.to_thread(self._deserialize_with_schema, avro_data, schema_id)

    def _deserialize_with_schema(
        self, avro_data: bytes, schema_id: int
    ) -> dict[str, Any]:
        """fastavro를 사용하여 Avro 데이터를 역직렬화합니다."""
        writer_schema = self._schema_cache[schema_id]
        reader_schema = self._reader_schema or writer_schema

        bytes_reader = io.BytesIO(avro_data)
        result = fastavro.schemaless_reader(bytes_reader, writer_schema, reader_schema)

        logger.debug(f"Avro 역직렬화 완료: schema_id={schema_id}")
        return result

    async def preload_schema(self, schema_id: int) -> None:
        """스키마를 미리 로드합니다."""
        if schema_id not in self._schema_cache:
            schema_obj = await self.schema_registry_client.get_schema_by_id(schema_id)
            writer_schema = orjson.loads(schema_obj.schema)
            self._schema_cache[schema_id] = writer_schema

            logger.debug(f"스키마 캐시 추가: id={schema_id}")


def create_avro_serializer(
    schema_registry_client: SchemaRegistryClient,
    subject: str,
    schema_str: str | None = None,
) -> AvroSerializer:
    """
    Avro 직렬화기를 생성합니다.

    Args:
        schema_registry_client: Schema Registry 클라이언트
        subject: 스키마 주제명
        schema_str: 스키마 JSON 문자열 (선택사항)

    Returns:
        설정된 AvroSerializer 인스턴스
    """
    return AvroSerializer(
        schema_registry_client=schema_registry_client,
        subject=subject,
        schema_str=schema_str,
    )


def create_avro_deserializer(
    schema_registry_client: SchemaRegistryClient, reader_schema_str: str | None = None
) -> AvroDeserializer:
    """
    Avro 역직렬화기를 생성합니다.

    Args:
        schema_registry_client: Schema Registry 클라이언트
        reader_schema_str: Reader 스키마 JSON 문자열 (스키마 진화용)

    Returns:
        설정된 AvroDeserializer 인스턴스
    """
    return AvroDeserializer(
        schema_registry_client=schema_registry_client,
        reader_schema_str=reader_schema_str,
    )
