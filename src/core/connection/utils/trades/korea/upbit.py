"""Upbit Trade 파서 (기준 포맷, 변환 없음)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class UpbitTradeParser(TradeParser):
    """Upbit Trade 파서.
    
    특징:
    - 이미 표준 포맷이므로 변환 없이 Pydantic DTO로 검증만 수행
    - code, trade_timestamp, trade_price, trade_volume, ask_bid, sequential_id 직접 제공
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """필수 필드로 판단.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        return (
            "code" in message
            and "trade_timestamp" in message
            and "trade_price" in message
            and "trade_volume" in message
            and "ask_bid" in message
            and "sequential_id" in message
        )
    
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Upbit → 표준 포맷 (Pydantic DTO 생성).
        
        Args:
            message: Upbit 원본 메시지
        
        Returns:
            표준화된 trade (Pydantic 검증 완료)
        """
        return StandardTradeDTO(
            code=message["code"],
            trade_timestamp=float(message["trade_timestamp"]) / 1000.0,
            trade_price=float(message["trade_price"]),
            trade_volume=float(message["trade_volume"]),
            ask_bid=1 if message["ask_bid"] == "BID" else -1,
            sequential_id=str(message["sequential_id"]),
        )
