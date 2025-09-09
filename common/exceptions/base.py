from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.types import ErrorCode, ErrorDomain


@dataclass(slots=True)
class ExchangeException(Exception):
    """거래소 관련 기본 예외 클래스

    운영/관측/정책 판단을 위한 구조화 필드를 포함하며, `to_dict()`는
    이벤트/로그 직렬화 시 일관된 스키마를 제공합니다.
    """

    exchange_name: str
    message: str
    original_exception: Exception | None = None

    # 구조화 필드 (운영/관측/정책 판단용)
    error_domain: ErrorDomain = ErrorDomain.UNKNOWN
    error_code: ErrorCode = ErrorCode.UNKNOWN_ERROR
    retryable: bool = False
    correlation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """예외 정보를 이벤트 데이터로 변환"""
        result: dict[str, Any] = {
            "exchange": self.exchange_name,
            "error": self.message,
            "error_type": self.__class__.__name__,
            "error_domain": self.error_domain.value,
            "error_code": self.error_code.value,
            "retryable": self.retryable,
        }

        if self.original_exception:
            result["original_error"] = str(self.original_exception)
            result["original_error_type"] = self.original_exception.__class__.__name__

        if self.correlation_id:
            result["correlation_id"] = self.correlation_id

        return result
