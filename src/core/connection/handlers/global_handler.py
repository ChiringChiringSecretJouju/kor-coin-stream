from __future__ import annotations

import asyncio
from typing import Any

import orjson

from src.common.logger import PipelineLogger
from src.config.settings import websocket_settings
from src.core.connection._utils import extract_symbol as _extract_symbol_impl
from src.core.connection.handlers.base import BaseWebsocketHandler
from src.core.connection.handlers.realtime_collection import RealtimeBatchCollector
from src.core.connection.utils.parse import update_dict
from src.core.types import (
    MessageHandler,
    OrderbookResponseData,
    TickerResponseData,
    TradeResponseData,
)
from src.infra.messaging.connect.producer_client import RealtimeDataProducer

logger = PipelineLogger.get_logger("websocket_handler", "connection")


class BaseGlobalWebsocketHandler(BaseWebsocketHandler):
    """글로벌 거래소 웹소켓 핸들러 (배치 수집 지원)"""

    def __init__(self, exchange_name: str, region: str, request_type: str) -> None:
        super().__init__(
            exchange_name=exchange_name,
            region=region,
            request_type=request_type,
        )
        # 글로벌 거래소 기본 설정
        self.ping_interval = websocket_settings.HEARTBEAT_INTERVAL
        self.projection: list[str] | None = None  # 필드 필터링용

        # 배치 수집기 및 프로듀서 (글로벌 거래소 특화 설정)
        self._batch_collector: RealtimeBatchCollector | None = None
        self._realtime_producer: RealtimeDataProducer | None = None
        self._batch_enabled = True  # 배치 수집 활성화 플래그

    def _extract_symbol(self, message: dict[str, Any]) -> str | None:
        """심볼 추출 (위임)"""
        return _extract_symbol_impl(message)

    def _parse_message(self, message: str | bytes | dict[str, Any]) -> dict[str, Any]:
        """메시지 파싱 공통 로직 (안전한 JSON 파싱)"""
        try:
            # 이미 dict인 경우 그대로 반환
            if isinstance(message, dict):
                return message

            if isinstance(message, bytes):
                decoded = message.decode("utf-8").strip()
                if not decoded:
                    logger.debug(f"{self.scope.exchange}: Received empty bytes message")
                    return {}
                message_str = decoded
            elif isinstance(message, str):
                message_str = message.strip()
                if not message_str:
                    logger.debug(
                        f"{self.scope.exchange}: Received empty string message"
                    )
                    return {}
            else:
                message_str = str(message).strip()

            # JSON 파싱 시도 (orjson 사용)
            parsed = orjson.loads(message_str)

            # dict 타입이면 그대로 반환
            if isinstance(parsed, dict):
                return parsed

            # list 타입이면 첫 번째 요소가 dict인지 확인 (Bitfinex 등)
            elif isinstance(parsed, list):
                if parsed and isinstance(parsed[0], dict):
                    # list의 첫 번째 dict를 반환하되, 원본 list 정보도 포함
                    result = parsed[0].copy()
                    result["_original_list"] = parsed  # 원본 list 보존
                    result["_is_list_message"] = True
                    return result
                else:
                    logger.debug(
                        f"{self.scope.exchange}: List message without dict elements: {parsed}"
                    )
                    return {}

            # 기타 타입은 빈 dict 반환
            else:
                logger.debug(
                    f"{self.scope.exchange}: Non-dict/list message: {type(parsed)}"
                )
                return {}

        except orjson.JSONDecodeError as e:
            # JSON이 아닌 메시지는 디버그 레벨로 로깅 (heartbeat는 health_monitor에서 처리)
            if len(str(message)) < 50:  # 짧은 메시지는 heartbeat일 가능성
                logger.debug(
                    f"{self.scope.exchange}: Non-JSON message (likely heartbeat): {message!r}"
                )
            else:
                logger.warning(
                    f"{self.scope.exchange}: JSON decode error: {e}, message: {message!r}"
                )
            return {}
        except Exception as e:
            logger.error(
                f"{self.scope.exchange}: Unexpected parsing error: {e}, message: {message!r}"
            )
            return {}

    async def _initialize_batch_system(self) -> None:
        """배치 수집 시스템 초기화 (글로벌 거래소 특화)"""
        if not self._batch_enabled:
            return
        try:
            # RealtimeDataProducer 초기화
            self._realtime_producer = RealtimeDataProducer()

            # 배치 수집기 초기화 (글로벌 거래소 특화 설정)
            async def emit_batch(batch: list[dict[str, Any]]) -> bool:
                """배치 전송 콜백 함수"""
                if self._realtime_producer:
                    return await self._realtime_producer.send_batch(
                        scope=self.scope, batch=batch
                    )
                return False

            # 글로벌 거래소 특화 배치 설정 (안정적 전송)
            self._batch_collector = RealtimeBatchCollector(
                batch_size=50,  # 글로벌: 50개 메시지마다 전송
                time_window=10.0,  # 글로벌: 10초마다 전송
                max_batch_size=100,  # 글로벌: 최대 100개
                emit_factory=emit_batch,
            )

            # 배치 수집기 시작
            await self._batch_collector.start()

            logger.info(
                f"{self.scope.exchange}: Batch collection system initialized (Global optimized)"
            )

        except Exception as e:
            logger.error(
                f"{self.scope.exchange}: Failed to initialize batch system: {e}"
            )
            self._batch_enabled = False

    async def _cleanup_batch_system(self) -> None:
        """배치 수집 시스템 정리"""
        if self._batch_collector:
            await self._batch_collector.stop()
            self._batch_collector = None

        if self._realtime_producer:
            await self._realtime_producer.stop_producer()
            self._realtime_producer = None

        logger.info(f"{self.scope.exchange}: Batch collection system cleaned up")

    def enable_batch_collection(self) -> None:
        """배치 수집 활성화"""
        self._batch_enabled = True
        logger.info(f"{self.scope.exchange}: Batch collection enabled")

    def disable_batch_collection(self) -> None:
        """배치 수집 비활성화"""
        self._batch_enabled = False
        logger.info(f"{self.scope.exchange}: Batch collection disabled")

    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """티커 메시지 처리 함수 (글로벌 거래소 배치 수집 지원)"""
        # 자식 클래스에서 전처리된 메시지인지 확인
        if isinstance(message, dict) and message.get("_preprocessed", False):
            # 이미 전처리된 메시지 - 플래그만 제거하고 바로 배치 처리
            filtered_message = message.copy()
            filtered_message.pop("_preprocessed", None)
        else:
            # 기본 처리 (자식 클래스에서 오버라이드하지 않은 경우)
            parsed_message = (
                message if isinstance(message, dict) else self._parse_message(message)
            )

            # 빈 메시지나 파싱 실패 시 무시
            if not parsed_message:
                return None

            # 구독 확인 메시지 처리 (글로벌 거래소)
            if parsed_message.get("result") is not None:
                # Binance, OKX 등의 result 기반 응답
                result = parsed_message.get("result")
                if result == "success":
                    self._log_status("subscribed")
                    logger.info(
                        f"{self.scope.exchange}: Subscription confirmed (result=success)"
                    )
                    await self._emit_connect_success_ack()
                    return None

            event = parsed_message.get("event")
            if event in ("subscribe", "subscribed"):
                # Bybit, Gate.io 등의 event 기반 응답
                logger.info(
                    f"{self.scope.exchange}: Subscription confirmed (event={event})"
                )
                self._log_status("subscribed")
                await self._emit_connect_success_ack()
                return None

            # data 필드 병합 (중첩 구조 처리)
            data_sub: dict | None = parsed_message.get("data", None)
            if isinstance(data_sub, dict):
                parsed_message = update_dict(parsed_message, "data")

            # projection 필터링 (기본 동작)
            fields: list[str] | None = self.projection
            if fields:
                filtered_message = {field: parsed_message.get(field) for field in fields}  # type: ignore[misc]
            else:
                filtered_message = parsed_message

            logger.info(
                f"{self.scope.exchange}: Using default processing (no child preprocessing)"
            )

        # 배치 수집기에 메시지 추가 (글로벌 거래소 특화)
        if self._batch_enabled and self._batch_collector:
            await self._batch_collector.add_message("ticker", filtered_message)
            logger.debug(
                f"{self.scope.exchange}: Ticker message added to batch collector"
            )
        else:
            logger.warning(
                f"""
                {self.scope.exchange}: Batch collection disabled or not initialized
                (enabled={self._batch_enabled}, collector={self._batch_collector is not None})
                """
            )

        return filtered_message

    async def orderbook_message(self, message: Any) -> OrderbookResponseData | None:
        """오더북 메시지 처리 함수 (글로벌 거래소 배치 수집 지원)"""
        parsed_message = (
            message if isinstance(message, dict) else self._parse_message(message)
        )

        # 빈 메시지나 파싱 실패 시 무시
        if not parsed_message:
            return None

        # 글로벌 거래소 공통 응답 처리
        logger.debug(
            f"""
            {self.scope.exchange}: orderbook_message received, 
            keys={list(parsed_message.keys()) if isinstance(parsed_message, dict) else 'not_dict'}
            """
        )

        if parsed_message.get("result") is not None:
            # Binance, OKX 등의 result 기반 응답
            result = parsed_message.get("result")
            logger.debug(f"{self.scope.exchange}: result field found: {result}")
            if result == "success":
                self._log_status("subscribed")
                logger.info(
                    f"{self.scope.exchange}: Subscription confirmed (result=success)"
                )
                # 구독 확정 시점에서 ACK 보장 (중복 방지는 내부 플래그로 처리)
                await self._emit_connect_success_ack()
                return None

        event = parsed_message.get("event")
        if event in ("subscribe", "subscribed"):
            # Bybit, Gate.io 등의 event 기반 응답
            logger.info(
                f"{self.scope.exchange}: Subscription confirmed (event={event})"
            )
            self._log_status("subscribed")
            # 구독 확정 시점에서 ACK 보장 (중복 방지는 내부 플래그로 처리)
            await self._emit_connect_success_ack()
            return None

        # data 필드가 있으면 병합
        data_sub: dict | None = parsed_message.get("data", None)
        if isinstance(data_sub, dict):
            parsed_message = update_dict(parsed_message, "data")

        # projection이 지정되면 해당 필드만 추출
        fields: list[str] | None = self.projection
        if fields:
            filtered_message = {field: parsed_message.get(field) for field in fields}  # type: ignore[misc]
        else:
            filtered_message = parsed_message

        # 배치 수집기에 메시지 추가 (글로벌 거래소 특화)
        if self._batch_enabled and self._batch_collector:
            await self._batch_collector.add_message("orderbook", filtered_message)

        return filtered_message

    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """체결 메시지 처리 함수 (글로벌 거래소 배치 수집 지원)"""
        parsed_message = (
            message if isinstance(message, dict) else self._parse_message(message)
        )

        # 빈 메시지나 파싱 실패 시 무시
        if not parsed_message:
            return None

        # 글로벌 거래소 공통 응답 처리
        logger.debug(
            f"""
            {self.scope.exchange}: trade_message received, 
            keys={list(parsed_message.keys()) if isinstance(parsed_message, dict) else 'not_dict'}
            """
        )

        if parsed_message.get("result") is not None:
            # Binance, OKX 등의 result 기반 응답
            result = parsed_message.get("result")
            logger.debug(f"{self.scope.exchange}: result field found: {result}")
            if result == "success":
                self._log_status("subscribed")
                logger.info(
                    f"{self.scope.exchange}: Subscription confirmed (result=success)"
                )
                # 구독 확정 시점에서 ACK 보장 (중복 방지는 내부 플래그로 처리)
                await self._emit_connect_success_ack()
                return None

        event = parsed_message.get("event")
        if event in ("subscribe", "subscribed"):
            # Bybit, Gate.io 등의 event 기반 응답
            logger.info(
                f"{self.scope.exchange}: Subscription confirmed (event={event})"
            )
            self._log_status("subscribed")
            # 구독 확정 시점에서 ACK 보장 (중복 방지는 내부 플래그로 처리)
            await self._emit_connect_success_ack()
            return None

        # data 필드가 있으면 병합
        data_sub: dict | None = parsed_message.get("data", None)
        if isinstance(data_sub, dict):
            parsed_message = update_dict(parsed_message, "data")

        # projection이 지정되면 해당 필드만 추출
        fields: list[str] | None = self.projection
        if fields:
            filtered_message = {field: parsed_message.get(field) for field in fields}  # type: ignore[misc]
        else:
            filtered_message = parsed_message

        # 배치 수집기에 메시지 추가 (글로벌 거래소 특화)
        if self._batch_enabled and self._batch_collector:
            await self._batch_collector.add_message("trade", filtered_message)

        return filtered_message

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
