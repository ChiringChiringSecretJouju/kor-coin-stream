"""한국 거래소 Trade 파서 기본 인터페이스.

Strategy Pattern으로 각 거래소 메시지를 Upbit 표준 포맷으로 변환합니다.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.core.dto.io.realtime import StandardTradeDTO


class TradeParser(ABC):
    """Trade 파서 인터페이스.
    
    모든 거래소의 Trade 메시지를 Upbit 포맷(Pydantic DTO)으로 통일합니다.
    """
    
    @abstractmethod
    def can_parse(self, message: dict[str, Any]) -> bool:
        """파싱 가능 여부 판단.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        pass
    
    @abstractmethod
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """메시지를 Upbit 표준 포맷으로 변환.
        
        Args:
            message: 원본 메시지
        
        Returns:
            Upbit 형식의 표준화된 trade (Pydantic DTO)
        """
        pass
