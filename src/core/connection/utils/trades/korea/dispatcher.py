"""한국 거래소 Trade 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TradeParser
from src.core.connection.utils.trades.korea.bithumb import BithumbTradeParser
from src.core.connection.utils.trades.korea.coinone import CoinoneTradeParser
from src.core.connection.utils.trades.korea.korbit import KorbitTradeParser
from src.core.connection.utils.trades.korea.upbit import UpbitTradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class KoreaTradeDispatcher:
    """한국 거래소 Trade 파서 자동 선택.
    
    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Trade 데이터를 Upbit 포맷(Pydantic DTO)으로 통일합니다.
    """
    
    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        self._parsers: list[TradeParser] = [
            CoinoneTradeParser(),  # 가장 구체적 (data.target_currency 등 체크)
            KorbitTradeParser(),   # 중간 (data 배열 체크)
            BithumbTradeParser(),  # Upbit와 동일 구조
            UpbitTradeParser(),    # 기준 포맷
        ]
    
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """자동으로 파서를 선택하여 Upbit 포맷(Pydantic DTO)으로 변환.
        
        Args:
            message: 원본 메시지
        
        Returns:
            Upbit 형식의 표준화된 trade (Pydantic 검증 완료)
        
        Raises:
            ValueError: 지원하지 않는 메시지 포맷
        """
        for parser in self._parsers:
            if parser.can_parse(message):
                return parser.parse(message)
        
        raise ValueError(f"Unsupported trade format: {list(message.keys())}")


# Module-level 싱글톤 인스턴스 (Thread-safe eager initialization)
_dispatcher = KoreaTradeDispatcher()


def get_korea_trade_dispatcher() -> KoreaTradeDispatcher:
    """한국 거래소 Trade 디스패처 싱글톤 인스턴스 반환.
    
    Thread-safe한 pre-initialized singleton을 반환합니다.
    
    Returns:
        KoreaTradeDispatcher 인스턴스
    """
    return _dispatcher
