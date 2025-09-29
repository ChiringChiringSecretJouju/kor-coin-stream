from __future__ import annotations

import asyncio
from typing import TypeAlias

from kafka.errors import KafkaConnectionError, KafkaProtocolError, NoBrokersAvailable
from pydantic import ValidationError
from redis.exceptions import (
    AuthenticationError as RedisAuthenticationError,
)
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    DataError as RedisDataError,
)
from redis.exceptions import (
    ResponseError as RedisResponseError,
)
from redis.exceptions import (
    TimeoutError as RedisTimeoutError,
)
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from src.core.dto.internal.common import RuleDomain
from src.core.types import ErrorCategory, ErrorCode, ErrorDomain

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
    ValidationError,
)

# 소켓/웹소켓 등
SOCKET_EXCEPTIONS = (
    asyncio.TimeoutError,
    InvalidStatus,
    WebSocketException,
    ConnectionClosed,
    OSError,
    KeyError,
)


# 1) asyncio 규칙 (ws/infra)
RULES_ASYNCIO: list[RuleDomain] = [
    RuleDomain(
        kinds=("ws", "infra"),
        exc=asyncio.CancelledError,
        result=(ErrorDomain.ORCHESTRATOR, ErrorCode.ORCHESTRATOR_ERROR, False),
    ),
    RuleDomain(
        kinds=("ws", "infra"),
        exc=asyncio.TimeoutError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 2) Type/역직렬화 및 기타 공통 규칙 (모든 경계 공통)
RULES_TYPE: list[RuleDomain] = [
    RuleDomain(
        kinds=("infra", "kafka", "redis", "ws"),
        exc=DESERIALIZATION_ERRORS,
        result=(ErrorDomain.DESERIALIZATION, ErrorCode.DESERIALIZATION_ERROR, False),
    ),
]

# 4) 기타(소켓/웹소켓 등) 규칙 (ws/infra)
RULES_OTHERS: list[RuleDomain] = [
    RuleDomain(
        kinds=("ws", "infra", "orchestrator"),
        exc=SOCKET_EXCEPTIONS,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 3) Kafka 규칙 (구체 -> 포괄)
RULES_KAFKA: list[RuleDomain] = [
    RuleDomain(
        kinds=("kafka", "infra"),
        exc=KafkaProtocolError,
        result=(ErrorDomain.PROTOCOL, ErrorCode.INVALID_SCHEMA, False),
    ),
    RuleDomain(
        kinds=("kafka", "infra"),
        exc=KafkaConnectionError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("kafka", "infra"),
        exc=KafkaException,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 1) Redis 규칙 (구체 -> 포괄)
RULES_REDIS: list[RuleDomain] = [
    RuleDomain(
        kinds=("redis", "infra"),
        exc=RedisAuthenticationError,
        result=(ErrorDomain.CACHE, ErrorCode.CONNECT_FAILED, False),
    ),
    RuleDomain(
        kinds=("redis", "infra"),
        exc=RedisConnectionError,
        result=(ErrorDomain.CACHE, ErrorCode.CONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("redis", "infra"),
        exc=RedisDataError,
        result=(ErrorDomain.PAYLOAD, ErrorCode.INVALID_SCHEMA, False),
    ),
    RuleDomain(
        kinds=("redis", "infra"),
        exc=RedisException,
        result=(ErrorDomain.CACHE, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 5) 전체 규칙 (구체 -> 포괄 순서를 유지하며 결합)
# 주의: 매칭 우선순위를 보장하기 위해 선언 순서를 유지합니다.
RULES_FOR_WS: list[RuleDomain] = [
    *RULES_ASYNCIO,
    *RULES_TYPE,
    *RULES_OTHERS,
]

RULES_FOR_KAFKA: list[RuleDomain] = [
    *RULES_FOR_WS,
    *RULES_KAFKA,
]

RULES_FOR_REDIS: list[RuleDomain] = [
    *RULES_FOR_WS,
    *RULES_REDIS,
]

RULES_FOR_INFRA: list[RuleDomain] = [
    *RULES_FOR_WS,
    *RULES_KAFKA,
    *RULES_REDIS,
]

# 예상되는 예외
EXPECTED_EXCEPTIONS = (
    *RULES_OTHERS,
    *SOCKET_EXCEPTIONS,
    *DESERIALIZATION_ERRORS,
    *KafkaException,
    *RedisException,
)

RuleDict: TypeAlias = dict[str, list[RuleDomain]]
RULES_BY_KIND: RuleDict = {
    "kafka": RULES_FOR_KAFKA,
    "redis": RULES_FOR_REDIS,
    "ws": RULES_FOR_WS,
    "infra": RULES_FOR_INFRA,
}


def classify_exception(err: BaseException, kind: str) -> ErrorCategory:
    """예외 → (ErrorDomain, ErrorCode, retryable) 분류기 (규칙 테이블 기반)

    - if-else 분기를 제거하고, 선언적 규칙을 순서대로 평가합니다.
    - 규칙은 "구체 → 포괄" 순서로 선언되어 가장 특수한 규칙이 먼저 매칭됩니다.
    """

    # 일급 함수로 합병(사유 -> 예외 테이블에서 바로 매칭)
    def get_rules_for(kind: str) -> list[RuleDomain]:
        """kind에 해당하는 규칙 리스트 반환. 알 수 없는 kind는 RULES_ALL로 폴백."""
        return RULES_BY_KIND.get(kind, [])

    rules: list[RuleDomain] = get_rules_for(kind)
    for rule in rules:
        # 예외 타입 매칭 (단일 타입 또는 타입 튜플)
        if isinstance(err, rule.exc):
            return rule.result

    # 알 수 없는 경우 기본값
    return (ErrorDomain.UNKNOWN, ErrorCode.UNKNOWN_ERROR, False)
