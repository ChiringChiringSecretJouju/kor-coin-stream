from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, Awaitable, Callable, ParamSpec, TypeAlias, TypeVar, cast

from common.exceptions.base import ExchangeException
from common.exceptions.exception_rule import EXPECTED_EXCEPTIONS, get_rules_for
from core.types import ErrorCode, ErrorDomain

# 타입 파라미터 정의
P = ParamSpec("P")
R = TypeVar("R")
ErrorCategory = tuple[ErrorDomain, ErrorCode, bool]

# 데코레이터 타입 별칭
SyncOrAsyncCallable: TypeAlias = Callable[P, R] | Callable[P, Awaitable[R]]
AsyncWrappedCallable: TypeAlias = Callable[P, Awaitable[R]]
ErrorWrappedDecorator: TypeAlias = Callable[[SyncOrAsyncCallable], AsyncWrappedCallable]


def _classify_exception(err: BaseException, kind: str) -> ErrorCategory:
    """예외 → (ErrorDomain, ErrorCode, retryable) 분류기 (규칙 테이블 기반)

    - if-else 분기를 제거하고, 선언적 규칙을 순서대로 평가합니다.
    - 규칙은 "구체 → 포괄" 순서로 선언되어 가장 특수한 규칙이 먼저 매칭됩니다.
    """

    rules = get_rules_for(kind)
    for rule in rules:
        # 예외 타입 매칭 (단일 타입 또는 타입 튜플)
        if isinstance(err, rule.exc):
            return rule.result

    # 알 수 없는 경우 기본값
    return (ErrorDomain.UNKNOWN, ErrorCode.UNKNOWN_ERROR, False)


def error_wrapped(kind: str, exchange: str = "exchange_name") -> ErrorWrappedDecorator:
    """동기/비동기 공용: 예외를 ExchangeException으로 래핑하는 데코레이터

    Notes:
    - 코루틴 함수(async def)도 지원합니다. 내부에서 await 후 동일 규칙으로 래핑합니다.
    - message_builder는 선택 콜백이며, 다양한 포맷 출력을 허용합니다.
      반환 타입은 호출부 로깅/메시지 포맷에 맞춰 자유롭게 사용할 수 있으므로
      Any 허용이 합리적이지만, 본 시그니처에서는 str로 제한해 일관성을 유지합니다.
      필요 시 확장 가능.
    """

    def _decorator(fn: SyncOrAsyncCallable) -> AsyncWrappedCallable:
        @functools.wraps(fn)
        async def _sw(self, *args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[override]
            try:
                if inspect.iscoroutinefunction(fn):
                    result = await fn(self, *args, **kwargs)  # type: ignore[misc]
                    return cast(R, result)  # type: ignore[misc]
                else:
                    # 동기 함수는 쓰레드로 오프로딩하여 이벤트 루프 블로킹 방지
                    result = await asyncio.to_thread(fn, self, *args, **kwargs)
                    return cast(R, result)
            except EXPECTED_EXCEPTIONS as e:  # 포괄 그룹
                domain, code, retryable = _classify_exception(e, kind)
                exch = getattr(self, exchange, "unknown")
                raise ExchangeException(
                    exchange_name=str(exch),
                    message=str(e),
                    original_exception=e if isinstance(e, ExchangeException) else None,
                    error_domain=domain,
                    error_code=code,
                    retryable=retryable,
                ) from e

        return _sw

    return _decorator


# 레거시 별칭(프로젝트 곳곳에서 사용 중) 유지
def redis_exception_wrapped(**kwargs: Any) -> ErrorWrappedDecorator:
    return error_wrapped(kind="redis", **kwargs)


def kafka_exception_wrapped(**kwargs: Any) -> ErrorWrappedDecorator:
    return error_wrapped(kind="kafka", **kwargs)


def infra_exception_wrapped(**kwargs: Any) -> ErrorWrappedDecorator:
    return error_wrapped(kind="infra", **kwargs)


def ws_exception_wrapped(**kwargs: Any) -> ErrorWrappedDecorator:
    return error_wrapped(kind="ws", **kwargs)
