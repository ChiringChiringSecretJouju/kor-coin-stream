"""한국 거래소 Orderbook 파서 모듈.

Strategy Pattern으로 거래소별 메시지를 표준 포맷으로 변환합니다.
"""

from src.core.connection.utils.orderbooks.korea.dispatcher import (
    get_korea_orderbook_dispatcher,
)

__all__ = [
    "get_korea_orderbook_dispatcher",
]
