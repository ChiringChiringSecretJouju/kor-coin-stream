import asyncio
from dataclasses import dataclass
from enum import Enum
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

# 네트워크 관련 OSError (세밀한 분류)
NETWORK_ERRORS = (
    ConnectionRefusedError,  # 연결 거부 (서버 다운)
    ConnectionResetError,  # 연결 리셋 (네트워크 불안정)
    ConnectionAbortedError,  # 연결 중단
    BrokenPipeError,  # 파이프 손상
    TimeoutError,  # 일반 타임아웃
)


# 1) asyncio 규칙 (ws/infra/connection)
RULES_ASYNCIO: list[RuleDomain] = [
    RuleDomain(
        kinds=("ws", "infra", "connection"),
        exc=asyncio.CancelledError,
        result=(ErrorDomain.ORCHESTRATOR, ErrorCode.ORCHESTRATOR_ERROR, False),
    ),
    RuleDomain(
        kinds=("ws", "infra", "connection"),
        exc=asyncio.TimeoutError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 1-1) 네트워크 에러 규칙 (connection 전용, 우선순위 높음)
RULES_NETWORK: list[RuleDomain] = [
    RuleDomain(
        kinds=("connection", "ws", "infra"),
        exc=ConnectionRefusedError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("connection", "ws", "infra"),
        exc=ConnectionResetError,
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("connection", "ws", "infra"),
        exc=(ConnectionAbortedError, BrokenPipeError),
        result=(ErrorDomain.CONNECTION, ErrorCode.DISCONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("connection", "ws", "infra"),
        exc=TimeoutError,  # 일반 타임아웃 (asyncio.TimeoutError와 구분)
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 2) Type/역직렬화 및 기타 공통 규칙 (모든 경계 공통)
RULES_TYPE: list[RuleDomain] = [
    RuleDomain(
        kinds=(
            "infra",
            "kafka",
            "redis",
            "ws",
            "connection",
            "subscription",
            "orchestrator",
        ),
        exc=DESERIALIZATION_ERRORS,
        result=(ErrorDomain.DESERIALIZATION, ErrorCode.DESERIALIZATION_ERROR, False),
    ),
]

# 4) WebSocket 전용 규칙 (구체적 예외 먼저)
RULES_WEBSOCKET: list[RuleDomain] = [
    RuleDomain(
        kinds=("ws", "connection", "infra"),
        exc=InvalidStatus,  # HTTP 업그레이드 실패 (401, 403, 503 등)
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("ws", "connection", "infra"),
        exc=ConnectionClosed,  # WebSocket 정상/비정상 종료
        result=(ErrorDomain.CONNECTION, ErrorCode.DISCONNECT_FAILED, True),
    ),
    RuleDomain(
        kinds=("ws", "connection", "infra"),
        exc=WebSocketException,  # 일반 WebSocket 예외
        result=(ErrorDomain.CONNECTION, ErrorCode.CONNECT_FAILED, True),
    ),
]

# 5) 기타(소켓 등) 규칙 (포괄적)
RULES_OTHERS: list[RuleDomain] = [
    RuleDomain(
        kinds=("ws", "infra", "orchestrator", "connection"),
        exc=OSError,  # 일반 OS 레벨 에러 (가장 포괄적)
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
    *RULES_ASYNCIO,  # asyncio.TimeoutError, CancelledError (최우선)
    *RULES_WEBSOCKET,  # InvalidStatus, ConnectionClosed, WebSocketException
    *RULES_TYPE,  # 역직렬화 에러
    *RULES_OTHERS,  # OSError (가장 포괄적)
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

# 6) 도메인별 전용 규칙 (명확한 의미 부여)
# Connection 관련 (WebSocket 연결/재연결)
# 우선순위: 구체적 네트워크 에러 → asyncio → WebSocket → 일반 소켓 → 역직렬화
RULES_FOR_CONNECTION: list[RuleDomain] = [
    *RULES_NETWORK,  # ConnectionRefusedError, ConnectionResetError 등 (최우선)
    *RULES_ASYNCIO,  # asyncio.TimeoutError, CancelledError
    *RULES_WEBSOCKET,  # InvalidStatus, ConnectionClosed, WebSocketException
    *RULES_OTHERS,  # OSError (가장 포괄적)
    *RULES_TYPE,  # 역직렬화 에러
]

# Subscription 관련 (구독/재구독)
RULES_FOR_SUBSCRIPTION: list[RuleDomain] = [
    *RULES_TYPE,  # 파라미터 파싱 에러 (최우선)
    *RULES_WEBSOCKET,  # WebSocket 에러
    *RULES_OTHERS,  # 일반 소켓 에러
]

# Orchestrator 관련 (연결 관리, 태스크 관리)
RULES_FOR_ORCHESTRATOR: list[RuleDomain] = [
    RuleDomain(
        kinds=("orchestrator",),
        exc=asyncio.CancelledError,
        result=(ErrorDomain.ORCHESTRATOR, ErrorCode.ORCHESTRATOR_ERROR, False),
    ),
    *RULES_TYPE,  # 설정/파라미터 에러
]

# 예상되는 모든 예외 (포괄적 리스트)
EXPECTED_EXCEPTIONS = (
    # 네트워크
    *NETWORK_ERRORS,
    # WebSocket
    InvalidStatus,
    WebSocketException,
    ConnectionClosed,
    # 소켓
    OSError,
    # asyncio
    asyncio.TimeoutError,
    asyncio.CancelledError,
    # 역직렬화
    *DESERIALIZATION_ERRORS,
    # Kafka
    *KafkaException,
    # Redis
    *RedisException,
)

RuleDict: TypeAlias = dict[str, list[RuleDomain]]
RULES_BY_KIND: RuleDict = {
    # Infrastructure 레이어
    "kafka": RULES_FOR_KAFKA,
    "redis": RULES_FOR_REDIS,
    "infra": RULES_FOR_INFRA,
    # Domain/Application 레이어
    "ws": RULES_FOR_WS,
    "connection": RULES_FOR_CONNECTION,  # WebSocket 연결 전용
    "subscription": RULES_FOR_SUBSCRIPTION,  # 구독 관리 전용
    "orchestrator": RULES_FOR_ORCHESTRATOR,  # 오케스트레이터 전용
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


# ============================================================================
# 에러 처리 전략 (EDA 환경용)
# ============================================================================


class ErrorSeverity(Enum):
    """에러 심각도 분류"""

    CRITICAL = "critical"  # 즉시 대응 필요, ws.error 필수
    WARNING = "warning"  # 모니터링 필요, ws.error 권장
    INFO = "info"  # 로깅만, 재시도 가능


@dataclass(slots=True, frozen=True)
class ErrorStrategy:
    """에러 처리 전략

    EDA 환경에서 에러 발생 시 어떻게 처리할지 정의합니다.
    """

    severity: ErrorSeverity
    send_to_ws_error: bool  # ws.error 토픽 발행 여부
    send_to_dlq: bool  # DLQ 전송 여부
    log_level: str = "error"  # 로그 레벨
    circuit_break: bool = False  # Circuit Breaker 트리거 여부
    alert: bool = False  # 알람 발송 여부 (Slack/PagerDuty)


# 에러 코드별 기본 전략
ERROR_STRATEGIES: dict[ErrorCode, ErrorStrategy] = {
    # ========== CRITICAL 에러들 (즉시 대응 필요) ==========
    ErrorCode.CONNECT_FAILED: ErrorStrategy(
        severity=ErrorSeverity.CRITICAL,
        send_to_ws_error=True,
        send_to_dlq=False,
        log_level="critical",
        circuit_break=True,  # 5회 실패 시 Circuit Open
        alert=True,  # 즉시 알람
    ),
    ErrorCode.DISCONNECT_FAILED: ErrorStrategy(
        severity=ErrorSeverity.CRITICAL,
        send_to_ws_error=True,
        send_to_dlq=False,
        log_level="critical",
        circuit_break=False,
        alert=True,
    ),
    ErrorCode.UNKNOWN_ERROR: ErrorStrategy(
        severity=ErrorSeverity.CRITICAL,
        send_to_ws_error=True,
        send_to_dlq=True,  # 원인 파악용
        log_level="critical",
        circuit_break=False,
        alert=True,
    ),
    ErrorCode.DLQ_PUBLISH_FAILED: ErrorStrategy(
        severity=ErrorSeverity.CRITICAL,
        send_to_ws_error=True,
        send_to_dlq=False,  # DLQ 자체 실패는 DLQ로 보내지 않음
        log_level="critical",
        circuit_break=False,
        alert=True,
    ),
    # ========== WARNING 에러들 (모니터링 필요) ==========
    ErrorCode.DESERIALIZATION_ERROR: ErrorStrategy(
        severity=ErrorSeverity.WARNING,
        send_to_ws_error=True,
        send_to_dlq=True,  # 파싱 실패 메시지는 DLQ로
        log_level="warning",
    ),
    ErrorCode.INVALID_SCHEMA: ErrorStrategy(
        severity=ErrorSeverity.WARNING,
        send_to_ws_error=True,
        send_to_dlq=True,  # 스키마 불일치 메시지는 DLQ로
        log_level="error",
    ),
    ErrorCode.MISSING_FIELD: ErrorStrategy(
        severity=ErrorSeverity.WARNING,
        send_to_ws_error=True,
        send_to_dlq=True,  # 필드 누락 메시지는 DLQ로
        log_level="warning",
    ),
    ErrorCode.CACHE_CONFLICT: ErrorStrategy(
        severity=ErrorSeverity.WARNING,
        send_to_ws_error=True,
        send_to_dlq=False,  # 캐시 문제는 일시적
        log_level="warning",
    ),
    # ========== INFO 에러들 (로깅만, 자동 복구 가능) ==========
    ErrorCode.ORCHESTRATOR_ERROR: ErrorStrategy(
        severity=ErrorSeverity.INFO,
        send_to_ws_error=False,  # 내부 로직, 외부 노출 불필요
        send_to_dlq=False,
        log_level="info",
    ),
    ErrorCode.ALREADY_CONNECTED: ErrorStrategy(
        severity=ErrorSeverity.INFO,
        send_to_ws_error=False,  # 정상 범위 (중복 요청)
        send_to_dlq=False,
        log_level="info",
    ),
}


def get_error_strategy(code: ErrorCode) -> ErrorStrategy:
    """에러 코드에 대한 처리 전략 조회

    등록되지 않은 에러 코드는 기본 전략(WARNING) 반환
    """
    return ERROR_STRATEGIES.get(
        code,
        ErrorStrategy(
            severity=ErrorSeverity.WARNING,
            send_to_ws_error=True,
            send_to_dlq=False,
            log_level="warning",
        ),
    )
