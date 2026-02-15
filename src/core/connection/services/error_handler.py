from __future__ import annotations

from typing import Any

from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.connection.utils.logging.log_phases import (
    PHASE_CONNECTION_ERROR,
    PHASE_PARSE,
    PHASE_SUBSCRIPTION_ACK,
)
from src.core.connection.utils.logging.logging_mixin import ScopedConnectionLoggingMixin
from src.core.connection.utils.logging.pydantic_filter import PydanticFilter
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.commands import ConnectionTargetDTO

logger = PipelineLogger.get_logger("connection_error_handler", "connection")


class ConnectionErrorHandler(ScopedConnectionLoggingMixin):
    """연결 에러 처리 전담 클래스

    책임:
    - 웹소켓 연결 관련 에러 처리
    - 에러 이벤트 발행 (기존 error_adapter 활용)
    """

    def __init__(self, scope: ConnectionScopeDomain) -> None:
        self.scope = scope
        self._logger = logger
        self._target = ConnectionTargetDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
        )

    async def emit_ws_error(
        self,
        err: BaseException,
        observed_key: str = "",
        raw_context: dict | None = None,
    ) -> None:
        """웹소켓 경계 에러를 통합 디스패처로 처리 (전략 기반)"""
        self._log_warning(
            "Dispatching websocket error",
            phase=PHASE_PARSE,
            observed_key=observed_key,
            error=str(err),
        )
        await dispatch_error(
            exc=err if isinstance(err, Exception) else Exception(str(err)),
            kind="ws",
            target=self._target,
            context=raw_context,
        )

    async def emit_subscription_error(
        self,
        err: BaseException,
        symbols: list[str] | None = None,
        raw_context: dict | None = None,
    ) -> None:
        """구독 관련 에러를 통합 디스패처로 처리 (전략 기반)"""
        context = PydanticFilter.filter_dict({**(raw_context or {}), "symbols": symbols})

        self._log_warning(
            "Dispatching subscription error",
            phase=PHASE_SUBSCRIPTION_ACK,
            error=str(err),
            symbol_count=len(symbols or []),
        )

        await dispatch_error(
            exc=err if isinstance(err, Exception) else Exception(str(err)),
            kind="subscription",
            target=self._target,
            context=context,
        )

    async def emit_connection_error(
        self,
        err: BaseException,
        url: str,
        attempt: int,
        backoff: float,
        **additional_context: Any,  # Any 사용 이유: 거래소별 다양한 추가 컨텍스트를 수용하기 위함
    ) -> None:
        """연결 관련 에러를 통합 디스패처로 처리 (전략 기반)

        Note: kind="connection"으로 지정하여 WebSocket 연결 전용 규칙 적용.
              TimeoutError → CONNECT_FAILED (circuit_break=True)
              WebSocketException → CONNECT_FAILED (circuit_break=True)
        """
        context = {
            "url": url,
            "attempt": attempt,
            "backoff": backoff,
            **additional_context,
        }

        self._log_warning(
            "Dispatching connection error",
            phase=PHASE_CONNECTION_ERROR,
            url=url,
            attempt=attempt,
            backoff=backoff,
            error=str(err),
        )

        await dispatch_error(
            exc=err if isinstance(err, Exception) else Exception(str(err)),
            kind="connection",  # RULES_FOR_CONNECTION 사용 (명확한 의미)
            target=self._target,
            context=context,
        )
