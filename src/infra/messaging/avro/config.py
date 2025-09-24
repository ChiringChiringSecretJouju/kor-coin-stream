"""
Avro 관련 설정 및 유틸리티

Schema Registry 연결 설정과 공통 스키마 정의를 제공합니다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

from src.infra.messaging.avro.schema_registry import (
    SchemaRegistryClient,
    CompatibilityLevel,
)


class AvroSettings:
    """Avro 관련 환경 변수 설정"""

    # Schema Registry 설정
    SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    SCHEMA_REGISTRY_USERNAME: str | None = os.getenv("SCHEMA_REGISTRY_USERNAME")
    SCHEMA_REGISTRY_PASSWORD: str | None = os.getenv("SCHEMA_REGISTRY_PASSWORD")

    # 기본 호환성 레벨
    DEFAULT_COMPATIBILITY_LEVEL: str = os.getenv(
        "SCHEMA_COMPATIBILITY_LEVEL", "BACKWARD"
    )

    # 타임아웃 설정
    SCHEMA_REGISTRY_TIMEOUT: float = float(os.getenv("SCHEMA_REGISTRY_TIMEOUT", "30.0"))


def create_schema_registry_client(
    url: str | None = None,
    auth: tuple[str, str] | None = None,
    timeout: float | None = None,
) -> SchemaRegistryClient:
    """
    Schema Registry 클라이언트를 생성합니다.

    Args:
        url: Schema Registry URL (기본값: 환경변수)
        auth: 인증 정보 (username, password)
        timeout: 타임아웃 (초)

    Returns:
        설정된 SchemaRegistryClient 인스턴스
    """
    registry_url = url or AvroSettings.SCHEMA_REGISTRY_URL

    # 인증 정보 설정
    if auth is None and AvroSettings.SCHEMA_REGISTRY_USERNAME:
        auth = (
            AvroSettings.SCHEMA_REGISTRY_USERNAME,
            AvroSettings.SCHEMA_REGISTRY_PASSWORD or "",
        )

    registry_timeout = timeout or AvroSettings.SCHEMA_REGISTRY_TIMEOUT

    return SchemaRegistryClient(
        base_url=registry_url, auth=auth, timeout=registry_timeout
    )


# 공통 스키마 정의들
class CommonSchemas:
    """자주 사용되는 Avro 스키마 정의"""

    # 기본 이벤트 스키마
    BASE_EVENT_SCHEMA = """
    {
        "type": "record",
        "name": "BaseEvent",
        "namespace": "com.chiringchiring.events",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "event_type", "type": "string"},
            {"name": "version", "type": "string", "default": "1.0"},
            {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
        ]
    }
    """

    # 거래소 연결 요청 스키마
    CONNECT_REQUEST_SCHEMA = """
    {
        "type": "record",
        "name": "ConnectRequest",
        "namespace": "com.chiringchiring.commands",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "action", "type": "string"},
            {"name": "target", "type": {
                "type": "record",
                "name": "ConnectionTarget",
                "fields": [
                    {"name": "exchange", "type": "string"},
                    {"name": "region", "type": "string"},
                    {"name": "request_type", "type": "string"}
                ]
            }},
            {"name": "symbols", "type": {"type": "array", "items": "string"}},
            {"name": "projection", "type": ["null", {"type": "array", "items": "string"}], "default": null}
        ]
    }
    """

    # 에러 이벤트 스키마
    ERROR_EVENT_SCHEMA = """
    {
        "type": "record",
        "name": "ErrorEvent",
        "namespace": "com.chiringchiring.events",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "error_code", "type": "string"},
            {"name": "error_message", "type": "string"},
            {"name": "target", "type": {
                "type": "record",
                "name": "ErrorTarget",
                "fields": [
                    {"name": "exchange", "type": "string"},
                    {"name": "region", "type": "string"},
                    {"name": "request_type", "type": "string"}
                ]
            }},
            {"name": "context", "type": {"type": "map", "values": "string"}, "default": {}},
            {"name": "stack_trace", "type": ["null", "string"], "default": null}
        ]
    }
    """

    # 메트릭 이벤트 스키마
    METRICS_EVENT_SCHEMA = """
    {
        "type": "record",
        "name": "MetricsEvent",
        "namespace": "com.chiringchiring.metrics",
        "fields": [
            {"name": "timestamp", "type": "long"},
            {"name": "scope", "type": {
                "type": "record",
                "name": "MetricsScope",
                "fields": [
                    {"name": "exchange", "type": "string"},
                    {"name": "region", "type": "string"},
                    {"name": "request_type", "type": "string"}
                ]
            }},
            {"name": "range_start", "type": "long"},
            {"name": "range_end", "type": "long"},
            {"name": "items", "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "MetricsItem",
                    "fields": [
                        {"name": "symbol", "type": ["null", "string"], "default": null},
                        {"name": "count", "type": "long"}
                    ]
                }
            }}
        ]
    }
    """

    # 티커 데이터 스키마 (예시)
    TICKER_DATA_SCHEMA = """
    {
        "type": "record",
        "name": "TickerData",
        "namespace": "com.chiringchiring.market",
        "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "price", "type": "double"},
            {"name": "volume", "type": "double"},
            {"name": "change", "type": "double"},
            {"name": "change_percent", "type": "double"},
            {"name": "high_24h", "type": ["null", "double"], "default": null},
            {"name": "low_24h", "type": ["null", "double"], "default": null},
            {"name": "exchange", "type": "string"},
            {"name": "region", "type": "string"}
        ]
    }
    """


@dataclass(slots=True, frozen=True)
class AvroTopicConfig:
    """Avro 토픽 설정"""

    topic: str
    value_subject: str
    key_subject: str | None = None
    value_schema: str | None = None
    key_schema: str | None = None
    compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD


class AvroTopics:
    """Avro 토픽 설정 정의"""

    # 연결 요청 토픽
    CONNECT_REQUESTS = AvroTopicConfig(
        topic="connect-requests",
        value_subject="connect-requests-value",
        value_schema=CommonSchemas.CONNECT_REQUEST_SCHEMA,
    )

    # 에러 이벤트 토픽
    ERROR_EVENTS = AvroTopicConfig(
        topic="error-events",
        value_subject="error-events-value",
        value_schema=CommonSchemas.ERROR_EVENT_SCHEMA,
    )

    # 메트릭 이벤트 토픽
    METRICS_EVENTS = AvroTopicConfig(
        topic="metrics-events",
        value_subject="metrics-events-value",
        value_schema=CommonSchemas.METRICS_EVENT_SCHEMA,
    )

    # 티커 데이터 토픽
    TICKER_DATA = AvroTopicConfig(
        topic="ticker-data",
        value_subject="ticker-data-value",
        key_subject="ticker-data-key",
        value_schema=CommonSchemas.TICKER_DATA_SCHEMA,
    )


async def setup_schemas(
    schema_registry_client: SchemaRegistryClient, topics: list[AvroTopicConfig]
) -> None:
    """
    토픽들의 스키마를 Schema Registry에 등록합니다.

    Args:
        schema_registry_client: Schema Registry 클라이언트
        topics: 설정할 토픽 목록
    """
    for topic_config in topics:
        try:
            # Value 스키마 등록
            if topic_config.value_schema:
                value_id = await schema_registry_client.register_schema(
                    topic_config.value_subject, topic_config.value_schema
                )
                print(
                    f"Value 스키마 등록: {topic_config.value_subject} (ID: {value_id})"
                )

            # Key 스키마 등록 (있는 경우)
            if topic_config.key_subject and topic_config.key_schema:
                key_id = await schema_registry_client.register_schema(
                    topic_config.key_subject, topic_config.key_schema
                )
                print(f"Key 스키마 등록: {topic_config.key_subject} (ID: {key_id})")

            # 호환성 레벨 설정
            await schema_registry_client.set_compatibility_level(
                topic_config.value_subject, topic_config.compatibility_level
            )

        except Exception as e:
            print(f"스키마 설정 실패: {topic_config.topic} - {e}")


async def setup_all_schemas() -> None:
    """모든 기본 스키마를 설정합니다."""
    async with create_schema_registry_client() as client:
        topics = [
            AvroTopics.CONNECT_REQUESTS,
            AvroTopics.ERROR_EVENTS,
            AvroTopics.METRICS_EVENTS,
            AvroTopics.TICKER_DATA,
        ]

        await setup_schemas(client, topics)
        print("모든 스키마 설정 완료!")


if __name__ == "__main__":
    import asyncio

    asyncio.run(setup_all_schemas())
