from typing import Any, override

from src.core.connection.handlers.global_handler import BaseGlobalWebsocketHandler
from src.core.types import TickerResponseData, TradeResponseData


class BinanceWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이낸스 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: dict, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """바이난스 Ticker 메시지 처리 (StandardTickerDTO 기반)."""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # 구독 응답 필터링: {"result": null, "id": 1}
        if isinstance(parsed_message, dict):
            if "result" in parsed_message and "id" in parsed_message:
                return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """Trade 메시지 처리 - Binance Spot"""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # Binance는 시스템 메시지가 별도로 없음 (depthUpdate 이벤트만 수신)
        # 만약 구독 응답이 있다면 여기서 필터링
        if isinstance(parsed_message, dict):
            # 예: {"result": null, "id": 1} 같은 구독 응답 필터링
            if "result" in parsed_message and "id" in parsed_message:
                return None

        # DI 주입된 디스패처로 Trade DTO 변환
        trade_dto = self._trade_dispatcher.parse(parsed_message)

        # Pydantic DTO를 dict로 변환 후 _preprocessed 플래그 추가
        preprocessed_message = trade_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().trade_message(preprocessed_message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()


class BybitWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이비트 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: dict, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """바이비트 Ticker 메시지 처리 (StandardTickerDTO 기반)."""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # Bybit 시스템 메시지 필터링
        if isinstance(parsed_message, dict):
            if "success" in parsed_message and "op" in parsed_message:
                op: str = parsed_message.get("op", "")
                if op in ("subscribe", "unsubscribe", "ping", "pong"):
                    return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """Trade 메시지 처리 - Bybit Spot"""

        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # Bybit 시스템 메시지 필터링
        if isinstance(parsed_message, dict):
            # 구독 응답: {"success": true, "ret_msg": "", "op": "subscribe", "conn_id": "..."}
            if "success" in parsed_message and "op" in parsed_message:
                op: str = parsed_message.get("op", "")
                if op in ("subscribe", "unsubscribe", "ping", "pong"):
                    return None

        # DI 주입된 디스패처로 Trade DTO 변환
        trade_dto = self._trade_dispatcher.parse(parsed_message)

        # Pydantic DTO를 dict로 변환 후 _preprocessed 플래그 추가
        preprocessed_message = trade_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().trade_message(preprocessed_message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()



class OKXWebsocketHandler(BaseGlobalWebsocketHandler):
    """오케이엑스 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(
        self, url: str, parameter_info: dict, correlation_id: str | None = None
    ) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info, correlation_id)

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """오케이엑스 Ticker 메시지 처리 (StandardTickerDTO 기반)."""
        if isinstance(message, dict):
            event = message.get("event", "")
            if event in ("subscribe", "unsubscribe", "error"):
                return None

        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        ticker_dto = self._ticker_dispatcher.parse(parsed_message)
        preprocessed_message = ticker_dto.model_dump()
        preprocessed_message["_preprocessed"] = True
        return await super().ticker_message(preprocessed_message)

    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """Trade 메시지 처리 - OKX Spot"""

        if isinstance(message, dict):
            event = message.get("event", "")
            if event in ("subscribe", "unsubscribe", "error"):
                if event == "error":
                    self._log_status(f"error: {message.get('msg', '')}")
                return None

        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # DI 주입된 디스패처로 Trade DTO 변환
        trade_dto = self._trade_dispatcher.parse(parsed_message)

        # Pydantic DTO를 dict로 변환
        preprocessed_message = trade_dto.model_dump()
        return await super().trade_message(preprocessed_message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()
