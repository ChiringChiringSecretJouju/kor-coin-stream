from __future__ import annotations

import asyncio
import json
from typing import Any

from src.common.logger import PipelineLogger
from src.config.settings import websocket_settings
from src.core.connection.handlers.base import BaseWebsocketHandler
from src.core.connection.utils.parse import update_dict
from src.core.connection._utils import extract_symbol as _extract_symbol_impl
from src.core.types import (
    TickerResponseData,
    OrderbookResponseData,
    TradeResponseData,
    MessageHandler,
)

logger = PipelineLogger.get_logger("websocket_handler", "connection")


class BaseKoreaWebsocketHandler(BaseWebsocketHandler):
    """한국 거래소 웹소켓 핸들러"""

    def __init__(self, exchange_name: str, region: str, request_type: str) -> None:
        super().__init__(
            exchange_name=exchange_name,
            region=region,
            request_type=request_type,
        )
        # 한국 거래소 설정
        self.ping_interval = websocket_settings.HEARTBEAT_INTERVAL
        self.projection: list[str] | None = None  # 필드 필터링용

    def _extract_symbol(self, message: dict[str, Any]) -> str | None:
        """심볼 추출 (위임)"""
        return _extract_symbol_impl(message)

    def _parse_message(self, message: str | bytes) -> dict[str, Any]:
        """메시지 파싱 공통 로직"""
        if isinstance(message, bytes):
            message = json.loads(message.decode("utf-8"))
        if isinstance(message, str):
            message = json.loads(message)

        return message

    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """티커 메시지 처리 함수"""
        parsed_message = self._parse_message(message)

        # 구독 확인 메시지 처리 (일부 거래소에서 사용)
        rt = parsed_message.get("response_type", "")
        logger.debug(f"{self.scope.exchange}: response_type={rt}")

        if rt == "SUBSCRIBED":
            self._log_status("subscribed")
            logger.info(f"{self.scope.exchange}: Subscription confirmed (SUBSCRIBED)")
            # 구독 확정 시점에서 ACK 보장 (중복 방지는 내부 플래그로 처리)
            await self._emit_connect_success_ack()
            return None
        if rt == "CONNECTED":
            logger.debug(f"{self.scope.exchange}: Connection confirmed (CONNECTED)")
            return None

        # data_sub에 dictionary가 있으면 update_dict를 사용하여 병합
        data_sub: dict | None = parsed_message.get("data", None)
        if isinstance(data_sub, dict):
            parsed_message = update_dict(parsed_message, "data")

        # projection이 지정되면 해당 필드만 추출
        fields: list[str] | None = self.projection
        if fields:
            return {field: parsed_message.get(field) for field in fields}  # type: ignore[misc]
        return parsed_message

    async def orderbook_message(self, message: Any) -> OrderbookResponseData | None:
        """오더북 메시지 처리 함수"""
        parsed_message = self._parse_message(message)
        return parsed_message

    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """체결 메시지 처리 함수"""
        parsed_message = self._parse_message(message)
        return parsed_message

    async def _handle_message_loop(self, websocket: Any, timeout: int) -> None:
        """메시지 수신 및 처리 루프"""
        while not self.stop_requested:
            message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            # 수신 알림으로 워치독에 최신 수신 시각을 전달
            self._health_monitor.notify_receive()
            parsed_message = self._parse_message(message)

            # 심볼 추출 후 분 집계 카운트 증가
            symbol = self._extract_symbol(parsed_message)
            self._minute_batch_counter.inc(symbol=symbol)

            # 메시지 타입별 핸들러 호출
            handler_map: MessageHandler = {
                "ticker": self.ticker_message,
                "orderbook": self.orderbook_message,
                "trade": self.trade_message,
            }
            fn = handler_map.get(self.scope.request_type)
            if fn:
                await fn(parsed_message)

        logger.info(
            f"{self.scope.exchange}: message loop stopped ({self.scope.request_type})"
        )
