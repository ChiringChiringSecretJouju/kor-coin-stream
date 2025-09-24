from __future__ import annotations

from enum import Enum
from typing import Any, Awaitable, Callable


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


# NOTE: Orderbook/Trade에 Any를 사용하는 이유
# - 거래소/마켓에 따라 필드 구성이 제각각이며, 런타임에 projection으로 필터링됩니다.
# - 표준화 이전의 원본 보존을 위해 dict[str, Any] 타입을 허용합니다.
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
