from __future__ import annotations

from typing import Any

from src.common.logger import PipelineLogger
from src.core.dto.adapter.error_adapter import make_ws_error_event_from_kind
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.target import ConnectionTargetDTO

logger = PipelineLogger.get_logger("error_handler", "connection")


class ConnectionErrorHandler:
    """연결 에러 처리 전담 클래스

    책임:
    - 웹소켓 연결 관련 에러 처리
    - 에러 이벤트 발행 (기존 error_adapter 활용)
    """

    def __init__(self, scope: ConnectionScopeDomain) -> None:
        self.scope = scope
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
        """웹소켓 경계 에러를 표준 에러 토픽으로 발행(kind='ws')"""
        logger.debug(
            f"{self.scope.exchange}: 웹소켓 에러 발행",
            extra={
                "error_type": type(err).__name__,
                "error_message": str(err),
                "observed_key": observed_key,
            },
        )

        await make_ws_error_event_from_kind(
            target=self._target,
            err=err,
            kind="ws",
            observed_key=observed_key,
            raw_context=raw_context,
        )

    async def emit_subscription_error(
        self,
        err: BaseException,
        symbols: list[str] | None = None,
        raw_context: dict | None = None,
    ) -> None:
        """구독 관련 에러를 표준 에러 토픽으로 발행(kind='subscription')"""
        observed_key = f"symbols:{','.join(symbols)}" if symbols else ""

        logger.debug(
            f"{self.scope.exchange}: 구독 에러 발행",
            extra={
                "error_type": type(err).__name__,
                "error_message": str(err),
                "symbols": symbols,
            },
        )

        await make_ws_error_event_from_kind(
            target=self._target,
            err=err,
            kind="subscription",
            observed_key=observed_key,
            raw_context=raw_context,
        )

    async def emit_connection_error(
        self,
        err: BaseException,
        url: str,
        attempt: int,
        backoff: float,
        **additional_context: Any,  # Any 사용 이유: 거래소별 다양한 추가 컨텍스트를 수용하기 위함
    ) -> None:
        """연결 관련 에러를 표준 에러 토픽으로 발행(kind='connection')"""
        raw_context = {
            "url": url,
            "attempt": attempt,
            "backoff": backoff,
            **additional_context,
        }

        observed_key = f"url:{url}:attempt:{attempt}"

        logger.debug(
            f"{self.scope.exchange}: 연결 에러 발행",
            extra={
                "error_type": type(err).__name__,
                "error_message": str(err),
                "url": url,
                "attempt": attempt,
                "backoff": backoff,
            },
        )

        await make_ws_error_event_from_kind(
            target=self._target,
            err=err,
            kind="connection",
            observed_key=observed_key,
            raw_context=raw_context,
        )
