"""트라이/캐치 블록에서 사용할 예외 타입 정의 모듈.

광범위한 Exception 사용을 지양하고, 의도한 예외만 명시적으로 처리하기 위해 사용합니다.
"""

import asyncio
from enum import StrEnum
from typing import Any, Awaitable, Callable, Final, TypeAlias, TypeVar, Union

import orjson
import websockets

# ----------------------------------------------------------------------------
# Type Definitions & Enums
# ----------------------------------------------------------------------------
T = TypeVar("T")

# Callables
AsyncWrappedCallable = Callable[..., Awaitable[Any]]
SyncOrAsyncCallable = Union[Callable[..., Any], Callable[..., Awaitable[Any]]]
ErrorWrappedDecorator = Callable[[Callable[..., Any]], Callable[..., Any]]

# Enums
class ErrorDomain(StrEnum):
    """에러 도메인 분류"""

    CONNECTION = "connection"
    PROTOCOL = "protocol"
    PAYLOAD = "payload"
    DESERIALIZATION = "deserialization"
    ORCHESTRATOR = "orchestrator"
    CACHE = "cache"
    UNKNOWN = "unknown"


class ErrorCode(StrEnum):
    """에러 코드 분류"""

    ALREADY_CONNECTED = "already_connected"
    CONNECT_FAILED = "connect_failed"
    DISCONNECT_FAILED = "disconnect_failed"
    INVALID_SCHEMA = "invalid_schema"
    MISSING_FIELD = "missing_field"
    DESERIALIZATION_ERROR = "deserialization_error"
    ORCHESTRATOR_ERROR = "orchestrator_error"
    CACHE_CONFLICT = "cache_conflict"
    DLQ_PUBLISH_FAILED = "dlq_publish_failed"
    UNKNOWN_ERROR = "unknown_error"


# ----------------------------------------------------------------------------
# Exception Constants
# ----------------------------------------------------------------------------

# 1. 네트워크/연결 관련 예외 (재시도 대상)
# - websockets.ConnectionClosed: 정상/비정상 종료
# - asyncio.TimeoutError: 시간 초과
# - ConnectionError: OS 레벨 연결 에러
# - OSError: 소켓 레벨 에러
CONNECTION_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    websockets.ConnectionClosed,
    websockets.ConnectionClosedError,
    websockets.ConnectionClosedOK,
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
    TimeoutError,  # Python 3.10+ built-in
)

# 2. 프로토콜/구독 관련 예외 (경고 대상)
# - ValueError: 파라미터 파싱 실패 등
# - TypeError: 타입 불일치
PROTOCOL_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    ValueError,
    TypeError,
    orjson.JSONDecodeError if 'orjson' in locals() else ValueError, # fallback
)

# 3. 인프라/설정 관련 예외 (중단 대상)
# - RuntimeError: 치명적 상태
INFRA_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    RuntimeError,
    AssertionError,
)

# 4. 재구독 실패 처리용 (연결은 살아있으나 구독만 실패)
RESUBSCRIBE_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    *CONNECTION_EXCEPTIONS,
    *PROTOCOL_EXCEPTIONS,
)


ErrorCategory: TypeAlias = tuple[ErrorDomain, ErrorCode, bool]
ExceptionGroup: TypeAlias = type[BaseException] | tuple[type[BaseException], ...]
RuleKind: TypeAlias = tuple[str, ...]