from typing import Any, override

from src.core.connection.handlers.global_handler import BaseGlobalWebsocketHandler
from src.core.connection.utils.parse import (
    preprocess_na_trade_message,
    preprocess_ticker_message,
)
from src.core.types import TickerResponseData, TradeResponseData


class CoinbaseWebsocketHandler(BaseGlobalWebsocketHandler):
    """코인베이스 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.

    특징:
        - snapshot 메시지 무시 (초기 데이터 불필요)
        - l2update만 심볼별로 누적
        - 주기적으로 누적된 데이터를 배치 전송
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
        parsed_message = self._parse_message(message)

        # 전처리된 메시지 생성 (Coinbase는 플랫 구조)
        if isinstance(parsed_message, dict):
            preprocessed_message = preprocess_ticker_message(
                parsed_message, self.projection
            )
            return await super().ticker_message(preprocessed_message)

        return await super().ticker_message(message)



    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """Trade 메시지 처리 - Coinbase Advanced Trade"""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # Coinbase 시스템 메시지 필터링 (orderbook_message와 동일)
        if isinstance(parsed_message, dict):
            msg_type: str = parsed_message.get("type", "")
            if msg_type in ("subscriptions", "heartbeat", "error", "last_match"):
                if msg_type == "error":
                    self._log_status(f"error: {parsed_message.get('message', '')}")
                return None

        # 북미 거래소 Trade 파서 사용 → StandardTradeDTO
        preprocessed_dto = preprocess_na_trade_message(parsed_message, self.projection)

        # Pydantic DTO를 dict로 변환
        preprocessed_message = preprocessed_dto.model_dump()
        return await super().trade_message(preprocessed_message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()


class KrakenWebsocketHandler(BaseGlobalWebsocketHandler):
    """크라켄 거래소 웹소켓 핸들러 (배치 수집 지원)

    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.

    특징:
        - update 메시지의 bids/asks를 심볼별로 누적
        - 주기적으로 누적된 데이터를 배치 전송
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
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """티커 메시지 처리 함수 - Kraken 특화 (heartbeat 필터링 + data 배열 병합)"""
        parsed_message = self._parse_message(message)

        # Kraken heartbeat 메시지 필터링
        if (
            isinstance(parsed_message, dict)
            and parsed_message.get("channel") == "heartbeat"
        ):
            return None  # heartbeat 메시지는 수집하지 않음

        # 구독 성공 응답 처리 (Kraken v2 API)
        if isinstance(parsed_message, dict):
            # method: subscribe, success: true 패턴
            if (
                parsed_message.get("method") == "subscribe"
                and parsed_message.get("success") is True
            ):
                self._log_status("subscribed")
                return None

            # result: success 패턴 (다른 거래소와 호환)
            if parsed_message.get("result") == "success":
                self._log_status("subscribed")
                return None

        # data 배열 병합 (Kraken은 data 배열 구조)
        if isinstance(parsed_message, dict):
            data_list = parsed_message.get("data", [])
            if isinstance(data_list, list) and len(data_list) > 0:
                # 첫 번째 데이터 항목을 최상위로 병합
                first_data = data_list[0]
                if isinstance(first_data, dict):
                    merged_message = {**parsed_message, **first_data}
                    merged_message.pop("data", None)  # 원본 data 필드 제거
                else:
                    merged_message = parsed_message
            else:
                merged_message = parsed_message

            # 전처리된 메시지 생성 (projection 적용)
            preprocessed_message = preprocess_ticker_message(
                merged_message, self.projection
            )
            return await super().ticker_message(preprocessed_message)

        return await super().ticker_message(message)



    @override
    async def trade_message(self, message: Any) -> TradeResponseData | None:
        """Trade 메시지 처리 - Kraken v2"""
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # Kraken 시스템 메시지 필터링 (orderbook_message와 동일)
        if isinstance(parsed_message, dict):
            # heartbeat 메시지
            if parsed_message.get("channel") == "heartbeat":
                return None

            # 구독 성공 응답
            if (
                parsed_message.get("method") == "subscribe"
                and parsed_message.get("success") is True
            ):
                self._log_status("subscribed")
                return None

            # result: success 패턴
            if parsed_message.get("result") == "success":
                self._log_status("subscribed")
                return None

            if parsed_message.get("channel") == "status":
                return None

        # snapshot 메시지 필터링 (파서 호출 전에 필터링 필수!)
        msg_type = parsed_message.get("type", "")
        if msg_type == "snapshot":
            return None

        # 북미 거래소 Trade 파서 사용 → StandardTradeDTO
        preprocessed_dto = preprocess_na_trade_message(parsed_message, self.projection)

        # Pydantic DTO를 dict로 변환
        preprocessed_message = preprocessed_dto.model_dump()
        return await super().trade_message(preprocessed_message)
