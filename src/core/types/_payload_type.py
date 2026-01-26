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




class StandardTrade(TypedDict):
    """표준화된 Trade 포맷 (Upbit 기준).
    
    **DEPRECATED**: src.core.dto.io.trade.StandardTradeDTO 사용 권장
    
    모든 거래소의 trade 데이터를 Upbit 형식으로 통일한 결과입니다.
    - code: 마켓 코드 ("KRW-BTC" 형식으로 통일)
    - trade_timestamp: 체결 타임스탬프 (Unix Seconds, float)
    - trade_price: 체결 가격 (float)
    - trade_volume: 체결량 (float)
    - ask_bid: 매수/매도 구분 (1=BID, -1=ASK, 0=UNKNOWN)
    - sequential_id: 체결 고유 ID (문자열)
    """
    
    code: str
    trade_timestamp: float
    trade_price: float
    trade_volume: float
    ask_bid: int
    sequential_id: str


TradeResponseData = dict[str, Any]
TickerResponseData = dict[str, int | float]

AsyncTradeType = Awaitable[
    TickerResponseData | TradeResponseData | None
]

# NOTE: 핸들러 맵에서 dict[str, Any] 사용 이유
# - 수신 메시지의 원본 스키마가 가변적이므로, 파서 단계에서 Any를 허용하여 공통 처리 파이프라인 유지
MessageHandler = dict[str, Callable[[dict[str, Any]], AsyncTradeType]]
CountingItem = list[dict[str, int | dict[str, int]]]
