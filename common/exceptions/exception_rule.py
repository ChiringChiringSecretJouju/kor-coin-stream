from __future__ import annotations

import asyncio

from redis.exceptions import (
    AuthenticationError as RedisAuthenticationError,
    ConnectionError as RedisConnectionError,
    DataError as RedisDataError,
    ResponseError as RedisResponseError,
    TimeoutError as RedisTimeoutError,
)
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException
from kafka.errors import KafkaConnectionError, KafkaProtocolError, NoBrokersAvailable

from core.dto.internal.common import Rule
from core.types import ErrorCode, ErrorDomain, RuleDict


# Kafka
KafkaException = (
    NoBrokersAvailable,
    KafkaConnectionError,
    KafkaProtocolError,
)

# Redis
RedisException = (
    RedisConnectionError,
    RedisDataError,
    RedisResponseError,
    RedisTimeoutError,
    RedisAuthenticationError,
)

# Type/역직렬화 및 기타 공통 규칙 (모든 경계 공통)
DESERIALIZATION_ERRORS = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
)

# 소켓/웹소켓 등
SOCKET_EXCEPTIONS = (
    asyncio.TimeoutError,
    InvalidStatus,
    WebSocketException,
    ConnectionClosed,
    OSError,
)


# 1) asyncio 규칙 (ws/infra)
RULES_ASYNCIO: list[Rule] = [
    Rule(
        kinds=("ws", "infra"),
        exc=asyncio.CancelledError,
        result=(ErrorDomain.ORCHESTRATOR, ErrorCode.ORCHESTRATOR_ERROR, False),
    ),
    Rule(
        kinds=("ws", "infra"),
        exc=asyncio.TimeoutError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 2) Type/역직렬화 및 기타 공통 규칙 (모든 경계 공통)
RULES_TYPE: list[Rule] = [
    Rule(
        kinds=("infra", "kafka", "redis", "ws"),
        exc=DESERIALIZATION_ERRORS,
        result=(ErrorDomain.DESERIALIZATION, ErrorCode.DESERIALIZATION_ERROR, False),
    ),
]

# 4) 기타(소켓/웹소켓 등) 규칙 (ws/infra)
RULES_OTHERS: list[Rule] = [
    Rule(
        kinds=("ws", "infra"),
        exc=SOCKET_EXCEPTIONS,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 3) Kafka 규칙 (구체 -> 포괄)
RULES_KAFKA: list[Rule] = [
    Rule(
        kinds=("kafka", "infra"),
        exc=KafkaProtocolError,
        result=(ErrorDomain.PROTOCOL, ErrorCode.INVALID_SCHEMA, False),
    ),
    Rule(
        kinds=("kafka", "infra"),
        exc=KafkaConnectionError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
    Rule(
        kinds=("kafka", "infra"),
        exc=KafkaException,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 1) Redis 규칙 (구체 -> 포괄)
RULES_REDIS: list[Rule] = [
    Rule(
        kinds=("redis", "infra"),
        exc=RedisAuthenticationError,
        result=(ErrorDomain.CACHE, ErrorCode.CONNECT_FAILED, False),
    ),
    Rule(
        kinds=("redis", "infra"),
        exc=RedisConnectionError,
        result=(ErrorDomain.CACHE, ErrorCode.CONNECT_FAILED, True),
    ),
    Rule(
        kinds=("redis", "infra"),
        exc=RedisDataError,
        result=(ErrorDomain.PAYLOAD, ErrorCode.INVALID_SCHEMA, False),
    ),
    Rule(
        kinds=("redis", "infra"),
        exc=RedisException,
        result=(ErrorDomain.CACHE, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 5) 전체 규칙 (구체 -> 포괄 순서를 유지하며 결합)
# 주의: 매칭 우선순위를 보장하기 위해 선언 순서를 유지합니다.
RULES_FOR_WS: list[Rule] = [
    *RULES_ASYNCIO,
    *RULES_TYPE,
    *RULES_OTHERS,
]

RULES_FOR_KAFKA: list[Rule] = [
    *RULES_FOR_WS,
    *RULES_KAFKA,
]

RULES_FOR_REDIS: list[Rule] = [
    *RULES_FOR_WS,
    *RULES_REDIS,
]

RULES_FOR_INFRA: list[Rule] = [
    *RULES_FOR_WS,
    *RULES_KAFKA,
    *RULES_REDIS,
]

# 예상되는 예외
EXPECTED_EXCEPTIONS = (
    *SOCKET_EXCEPTIONS,
    *DESERIALIZATION_ERRORS,
    *KafkaException,
    *RedisException,
)

RULES_BY_KIND: RuleDict = {
    "kafka": RULES_FOR_KAFKA,
    "redis": RULES_FOR_REDIS,
    "ws": RULES_FOR_WS,
    "infra": RULES_FOR_INFRA,
}


def get_rules_for(kind: str) -> list[Rule]:
    """kind에 해당하는 규칙 리스트 반환. 알 수 없는 kind는 RULES_ALL로 폴백."""
    return RULES_BY_KIND.get(kind, RULES_FOR_INFRA)
