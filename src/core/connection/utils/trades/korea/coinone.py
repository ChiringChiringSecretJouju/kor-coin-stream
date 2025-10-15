"""Coinone Trade 파서 (중첩 구조 → Upbit 포맷 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.dict_utils import update_dict
from src.core.connection.utils.trades.korea.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class CoinoneTradeParser(TradeParser):
    """Coinone Trade 파서.
    
    특징:
    - 중첩 구조: data 객체 안에 실제 체결 데이터
    - target_currency + quote_currency → code 조합
    - is_seller_maker: bool → ask_bid: "ASK" or "BID" 변환
    - update_dict 활용하여 data 평탄화
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """data 객체 및 필수 필드로 판단.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        data = message.get("data")
        if not isinstance(data, dict):
            return False
        
        return (
            "target_currency" in data
            and "quote_currency" in data
            and "timestamp" in data
            and "price" in data
            and "qty" in data
            and "is_seller_maker" in data
            and "id" in data
        )
    
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Coinone → Upbit 포맷 변환 (update_dict 활용).
        
        Args:
            message: Coinone 원본 메시지
        
        Returns:
            Upbit 형식의 표준화된 trade (Pydantic DTO)
        """
        # update_dict로 data 평탄화
        flattened = update_dict(message, "data")
        
        # 1. code 조합: target_currency + quote_currency → KRW-XRP
        target = flattened.get("target_currency", "").upper()
        quote = flattened.get("quote_currency", "").upper()
        code = f"{quote}-{target}"
        
        # 2. is_seller_maker → ask_bid 변환
        # is_seller_maker=True → 판매자가 Maker → 실제 체결은 매도(ASK)
        # is_seller_maker=False → 구매자가 Maker → 실제 체결은 매수(BID)
        is_seller_maker: bool = flattened.get("is_seller_maker", False)
        ask_bid = "ASK" if is_seller_maker else "BID"
        
        return StandardTradeDTO(
            code=code,
            trade_timestamp=int(flattened["timestamp"]),
            trade_price=float(flattened["price"]),
            trade_volume=float(flattened["qty"]),
            ask_bid=ask_bid,
            sequential_id=str(flattened["id"]),
        )
