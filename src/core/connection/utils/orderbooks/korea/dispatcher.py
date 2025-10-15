"""한국 거래소 Orderbook 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.orderbooks.korea.base import OrderbookParser
from src.core.connection.utils.orderbooks.korea.bithumb import BithumbOrderbookParser
from src.core.connection.utils.orderbooks.korea.coinone import CoinoneOrderbookParser
from src.core.connection.utils.orderbooks.korea.korbit import KorbitOrderbookParser
from src.core.connection.utils.orderbooks.korea.upbit import UpbitOrderbookParser
from src.core.dto.io.realtime import StandardOrderbookDTO


class KoreaOrderbookDispatcher:
    """한국 거래소 Orderbook 파서 자동 선택.
    
    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    """
    
    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        self._parsers: list[OrderbookParser] = [
            CoinoneOrderbookParser(),  # 가장 구체적 (4개 필드 체크)
            KorbitOrderbookParser(),   # 중간 (data 내부 체크)
            UpbitOrderbookParser(),    # 일반적 (orderbook_units)
            BithumbOrderbookParser(),  # Upbit 상속
        ]
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """자동으로 파서를 선택하여 파싱 (Pydantic DTO 반환).
        
        Args:
            message: 원본 메시지
        
        Returns:
            표준 포맷 orderbook (Pydantic 검증 완료)
        
        Raises:
            ValueError: 지원하지 않는 메시지 포맷
        """
        for parser in self._parsers:
            if parser.can_parse(message):
                return parser.parse(message)
        
        raise ValueError(f"Unsupported orderbook format: {list(message.keys())}")


# 싱글톤 인스턴스
_dispatcher: KoreaOrderbookDispatcher | None = None


def get_korea_orderbook_dispatcher() -> KoreaOrderbookDispatcher:
    """한국 거래소 Orderbook 디스패처 싱글톤 인스턴스 반환.
    
    Returns:
        KoreaOrderbookDispatcher 인스턴스
    """
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = KoreaOrderbookDispatcher()
    return _dispatcher
