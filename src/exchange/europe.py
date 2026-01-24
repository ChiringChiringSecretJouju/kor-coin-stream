from dataclasses import dataclass
from typing import Any, override

from src.core.connection.handlers.global_handler import BaseGlobalWebsocketHandler
from src.core.types import OrderbookResponseData, TickerResponseData, TradeResponseData


@dataclass(slots=True, frozen=True)
class BitfinexTickerData:
    """Bitfinex 티커 데이터 - 성능 최적화된 불변 객체"""

    channel_id: int
    symbol: str
    bid: float
    bid_size: float
    ask: float
    ask_size: float
    daily_change: float
    daily_change_relative: float
    last_price: float
    volume: float
    high: float
    low: float

    def to_dict(self) -> dict[str, Any]:
        """dict 형태로 변환 (projection 적용 시 사용)"""
        return {
            "channel_id": self.channel_id,
            "symbol": self.symbol,
            "bid": self.bid,
            "bid_size": self.bid_size,
            "ask": self.ask,
            "ask_size": self.ask_size,
            "daily_change": self.daily_change,
            "daily_change_relative": self.daily_change_relative,
            "last_price": self.last_price,
            "volume": self.volume,
            "high": self.high,
            "low": self.low,
        }


class BitfinexWebsocketHandler(BaseGlobalWebsocketHandler):
    """비트파이넥스 거래소 웹소켓 핸들러 (배치 수집 지원)
    
    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨
        # 구독된 심볼 정보를 저장 (channel_id -> symbol 매핑)
        self._channel_symbol_map: dict[int, str] = {}
    
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

    def _convert_bitfinex_array_to_dataclass(
        self, channel_id: int, data: list
    ) -> BitfinexTickerData:
        """Bitfinex 배열 데이터를 성능 최적화된 dataclass로 변환"""
        if len(data) < 10:
            raise ValueError("Invalid data format")

        # 동적으로 심볼 정보 가져오기
        symbol = self._channel_symbol_map.get(channel_id, "UNKNOWN")

        return BitfinexTickerData(
            channel_id=channel_id,
            symbol=symbol,
            bid=float(data[0]),
            bid_size=float(data[1]),
            ask=float(data[2]),
            ask_size=float(data[3]),
            daily_change=float(data[4]),
            daily_change_relative=float(data[5]),
            last_price=float(data[6]),
            volume=float(data[7]),
            high=float(data[8]),
            low=float(data[9]),
        )

    def _apply_projection(
        self, ticker_data: BitfinexTickerData
    ) -> dict[str, Any] | BitfinexTickerData:
        """projection 필드 적용 - 시스템에서 설정된 self.projection 사용"""
        fields = self.projection
        if not fields:
            return ticker_data

        # projection이 있는 경우에만 dict로 변환하여 필드 추출
        ticker_dict = ticker_data.to_dict()

        # EDA에서 요청한 필드들을 Bitfinex 필드명으로 매핑
        field_mapping = {
            "symbol": "symbol",
            "last_price": "last_price",
            "high": "high",
            "low": "low",
            "volume": "volume",
        }

        result = {}
        for field in fields:
            mapped_field = field_mapping.get(field, field)
            if mapped_field in ticker_dict:
                result[field] = ticker_dict[mapped_field]
            else:
                result[field] = None

        return result

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        """티커 메시지 처리 함수 - Bitfinex 특화 (배열 형태 처리)"""
        parsed_message = self._parse_message(message)

        # Bitfinex 연결 정보 메시지 무시
        if isinstance(parsed_message, dict):
            if parsed_message.get("event") == "info":
                return None

            # 구독 성공 응답 처리 - channel_id와 symbol 매핑 저장
            if parsed_message.get("event") == "subscribed":
                channel_id = parsed_message.get("chanId")
                symbol = parsed_message.get("symbol", "UNKNOWN")
                if channel_id is not None:
                    self._channel_symbol_map[channel_id] = symbol
                self._log_status("subscribed")
                return None

            # 에러 메시지 처리
            if parsed_message.get("event") == "error":
                error_msg = parsed_message.get("msg", "Unknown error")
                self._log_status(f"subscription_error: {error_msg}")
                return None

        # Bitfinex 배열 형태 데이터 처리: [CHANNEL_ID, [데이터]]
        if isinstance(parsed_message, list) and len(parsed_message) >= 2:
            channel_id = parsed_message[0]
            data = parsed_message[1]

            # 실제 ticker 데이터인 경우
            if isinstance(data, list) and len(data) >= 10:
                # 배열을 성능 최적화된 dataclass로 변환
                ticker_data = self._convert_bitfinex_array_to_dataclass(
                    channel_id, data
                )
                if ticker_data is None:
                    return None

                # projection 적용 (시스템에서 설정된 self.projection 사용)
                result = self._apply_projection(ticker_data)
                return result

        # 다른 형태의 메시지는 부모 클래스로 전달하지 않음 (배열 처리 에러 방지)
        return None
