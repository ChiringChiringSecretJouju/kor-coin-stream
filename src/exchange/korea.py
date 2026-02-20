from typing import Any, override

from src.common.logger import PipelineLogger
from src.core.connection.handlers.korea_handler import BaseKoreaWebsocketHandler
from src.core.types import (
    SocketParams,
    TickerResponseData,
    TradeResponseData,
)

logger = PipelineLogger.get_logger("korea_handlers", "exchange")


class UpbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """업비트 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: SocketParams, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """업비트 Ticker 메시지 처리 (StandardTickerDTO 기반)."""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """업비트 Trade 메시지 처리 (StandardTradeDTO 기반).

        업비트는 플랫 구조로 직접 파싱 가능합니다.
        """
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        trade_dto = self._trade_dispatcher.parse(parsed_message)
        preprocessed_message = trade_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().trade_message(preprocessed_message)


class BithumbWebsocketHandler(BaseKoreaWebsocketHandler):
    """빗썸 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: SocketParams, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """빗썸 Ticker 메시지 처리 (StandardTickerDTO 기반)."""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """빗썸 Trade 메시지 처리 (StandardTradeDTO 기반).

        빗썸은 업비트와 동일한 플랫 구조입니다.
        """
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        trade_dto = self._trade_dispatcher.parse(parsed_message)
        preprocessed_message = trade_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().trade_message(preprocessed_message)


class CoinoneWebsocketHandler(BaseKoreaWebsocketHandler):
    """코인원 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: SocketParams, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """코인원 Ticker 메시지 처리 (StandardTickerDTO 기반).

        코인원은 중첩 구조(data dict)이며, response_type 필터링이 필요합니다.
        원본 메시지를 그대로 파서에 전달합니다 (파서가 data 평탄화 수행).
        """
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # 코인원 구독 확인 메시지 필터링 (SUBSCRIBED, CONNECTED 등)
        rt = parsed_message.get("response_type", "")
        if rt and rt != "DATA":
            if rt == "SUBSCRIBED":
                self._log_status("subscribed")
                logger.info(f"{self.scope.exchange}: Ticker subscription confirmed (SUBSCRIBED)")
                await self._emit_connect_success_ack()
            return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """코인원 Trade 메시지 처리 (StandardTradeDTO 기반).

        코인원은 중첩 구조(data dict)로 전달됩니다.
        response_type 필터링 후, 원본 메시지를 그대로 파서에 전달합니다.
        """
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        rt = parsed_message.get("response_type", "")
        if rt and rt != "DATA":
            if rt == "SUBSCRIBED":
                self._log_status("subscribed")
                logger.info(f"{self.scope.exchange}: Trade subscription confirmed (SUBSCRIBED)")
                await self._emit_connect_success_ack()
            return None

        trade_dto = self._trade_dispatcher.parse(parsed_message)
        preprocessed_message = trade_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().trade_message(preprocessed_message)


class KorbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """코빗 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: SocketParams, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """코빗 Ticker 메시지 처리 (StandardTickerDTO 기반).

        코빗은 중첩 구조(data dict)로 전달됩니다.
        원본 메시지를 그대로 파서에 전달합니다 (파서가 data 처리 수행).
        """
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """코빗 Trade 메시지 처리 (StandardTradeDTO 기반).

        코빗은 중첩 구조(data 배열)로 전달됩니다.
        원본 메시지를 그대로 파서에 전달합니다 (파서가 data 배열 처리 수행).
        """
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        trade_dto = self._trade_dispatcher.parse(parsed_message)
        preprocessed_message = trade_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().trade_message(preprocessed_message)
