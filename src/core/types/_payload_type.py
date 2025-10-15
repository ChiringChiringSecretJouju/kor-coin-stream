from __future__ import annotations

from enum import Enum
from typing import Any, Awaitable, Callable, TypedDict


class PayloadType(str, Enum):
    """페이로드 타입 상수

    - STATUS: 제어/상태성 메시지
    - EVENT: 일반 이벤트
    - COMMAND: 명령 메시지
    """

    STATUS = "status"
    EVENT = "event"
    COMMAND = "command"


class PayloadAction(str, Enum):
    """페이로드 액션 상수"""

    CONNECT_AND_SUBSCRIBE = "connect_and_subscribe"
    DISCONNECT = "disconnect"
    UPDATE = "update"


class OrderbookItem(TypedDict):
    """호가 아이템 (price, size).
    
    **DEPRECATED**: src.core.dto.io.orderbook.OrderbookItemDTO 사용 권장
    
    표준화된 orderbook의 개별 호가 데이터입니다.
    모든 거래소에서 동일한 형식으로 변환됩니다.
    """
    
    price: str
    size: str


class StandardOrderbook(TypedDict):
    """표준화된 Orderbook 포맷.
    
    **DEPRECATED**: src.core.dto.io.orderbook.StandardOrderbookDTO 사용 권장
    
    모든 거래소의 orderbook 데이터를 동일한 형식으로 변환한 결과입니다.
    - symbol: 심볼 (BTC, ETH 등)
    - quote_currency: 기준 통화 (KRW, USDT 등)
    - timestamp: 현재 시각 (Unix timestamp milliseconds)
    - asks: 매도 호가 리스트 [{"price": str, "size": str}, ...]
    - bids: 매수 호가 리스트 [{"price": str, "size": str}, ...]
    """
    
    symbol: str
    quote_currency: str
    timestamp: int
    asks: list[OrderbookItem]
    bids: list[OrderbookItem]


class StandardTrade(TypedDict):
    """표준화된 Trade 포맷 (Upbit 기준).
    
    **DEPRECATED**: src.core.dto.io.trade.StandardTradeDTO 사용 권장
    
    모든 거래소의 trade 데이터를 Upbit 형식으로 통일한 결과입니다.
    - code: 마켓 코드 ("KRW-BTC" 형식으로 통일)
    - trade_timestamp: 체결 타임스탬프 (milliseconds)
    - trade_price: 체결 가격 (float)
    - trade_volume: 체결량 (float)
    - ask_bid: 매수/매도 구분 ("ASK" or "BID")
    - sequential_id: 체결 고유 ID (문자열)
    """
    
    code: str
    trade_timestamp: int
    trade_price: float
    trade_volume: float
    ask_bid: str
    sequential_id: str


OrderbookResponseData = dict[str, Any]
TradeResponseData = dict[str, Any]
TickerResponseData = dict[str, int | float]

AsyncTradeType = Awaitable[
    TickerResponseData | OrderbookResponseData | TradeResponseData | None
]

# NOTE: 핸들러 맵에서 dict[str, Any] 사용 이유
# - 수신 메시지의 원본 스키마가 가변적이므로, 파서 단계에서 Any를 허용하여 공통 처리 파이프라인 유지
MessageHandler = dict[str, Callable[[dict[str, Any]], AsyncTradeType]]
CountingItem = list[dict[str, int | dict[str, int]]]
