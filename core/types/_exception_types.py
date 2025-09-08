from __future__ import annotations

from enum import Enum
from typing import TypeAlias, Callable, ParamSpec, TypeVar, Awaitable

from core.dto.internal.common import Rule

# 타입 파라미터 정의
P = ParamSpec("P")
R = TypeVar("R")


class ErrorDomain(str, Enum):
    """에러 도메인 분류"""

    CONNECTION = "connection"
    PROTOCOL = "protocol"
    PAYLOAD = "payload"
    DESERIALIZATION = "deserialization"
    ORCHESTRATOR = "orchestrator"
    CACHE = "cache"
    UNKNOWN = "unknown"


class ErrorCode(str, Enum):
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


# 타입 별칭
ErrorCategory: TypeAlias = tuple[ErrorDomain, ErrorCode, bool]
ExceptionGroup: TypeAlias = type[BaseException] | tuple[type[BaseException], ...]
RuleKind: TypeAlias = tuple[str, ...]
RuleDict: TypeAlias = dict[str, list[Rule]]

# 데코레이터 타입 별칭
SyncOrAsyncCallable: TypeAlias = Callable[P, R] | Callable[P, Awaitable[R]]
AsyncWrappedCallable: TypeAlias = Callable[P, Awaitable[R]]
ErrorWrappedDecorator: TypeAlias = Callable[[SyncOrAsyncCallable], AsyncWrappedCallable]
