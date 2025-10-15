"""숫자 포맷 변환 유틸리티 (과학적 표기법 처리)."""

from __future__ import annotations

from decimal import Decimal


def normalize_number_string(value: str | float | int) -> str:
    """과학적 표기법을 일반 숫자 형식으로 변환.
    
    Args:
        value: 변환할 값 (문자열, float, int)
    
    Returns:
        일반 숫자 형식의 문자열
    
    Examples:
        >>> normalize_number_string("5.883e-05")
        '0.00005883'
        >>> normalize_number_string("1.23e+10")
        '12300000000'
        >>> normalize_number_string("123.45")
        '123.45'
        >>> normalize_number_string(0.00005883)
        '0.00005883'
        >>> normalize_number_string("")
        '0'
        >>> normalize_number_string(None)
        '0'
    """
    # 빈 문자열이나 None 처리
    if not value or (isinstance(value, str) and value.strip() == ""):
        return "0"
    
    try:
        # Decimal을 사용하여 정확한 변환
        decimal_value = Decimal(str(value))
        
        # 정규화된 문자열로 변환 (과학적 표기법 제거)
        normalized = format(decimal_value, "f")
        
        # 불필요한 trailing zeros 제거 (소수점 이하만)
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        
        return normalized
    except (ValueError, TypeError):
        # 변환 실패 시 "0" 반환 (안전한 기본값)
        return "0"
