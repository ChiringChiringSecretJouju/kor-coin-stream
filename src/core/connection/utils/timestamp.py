"""지역 기준 타임스탬프 생성 유틸리티.

순환 import를 방지하기 위해 parse.py에서 분리되었습니다.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ========================================
# 지역 기준 타임스탬프 생성 함수
# ========================================

# 지역명 → 타임존 매핑
_REGION_TIMEZONE_MAP: dict[str, str] = {
    "korea": "Asia/Seoul",
    "asia": "Asia/Tokyo",  # 기본값: 일본 시간 (UTC+9)
    "america": "America/New_York",  # 기본값: 뉴욕 시간 (UTC-5/UTC-4)
    "europe": "Europe/London",  # 기본값: 런던 시간 (UTC+0/UTC+1)
    "utc": "UTC",
}


def get_regional_timestamp_ms(region: str = "korea") -> int:
    """지역 기준 현재 타임스탬프 생성 (milliseconds).
    
    Args:
        region: 지역명 (korea, asia, america, europe, utc)
    
    Returns:
        Unix timestamp (milliseconds)
    
    Examples:
        >>> ts = get_regional_timestamp_ms("korea")  # KST 기준
        >>> ts = get_regional_timestamp_ms("asia")   # JST 기준
        >>> ts = get_regional_timestamp_ms("utc")    # UTC 기준
    
    Note:
        - 반환값은 UTC 기준 Unix timestamp (지역 무관)
        - 단, 시간대 계산은 해당 지역의 현재 시간 기준
    """
    timezone_name = _REGION_TIMEZONE_MAP.get(region.lower(), "UTC")
    try:
        tz = ZoneInfo(timezone_name)
        now = datetime.now(tz)
        return int(now.timestamp() * 1000)
    except Exception:
        # Fallback: UTC
        return int(time.time() * 1000)


def get_regional_datetime(region: str = "korea") -> datetime:
    """지역 기준 현재 datetime 객체 생성.
    
    Args:
        region: 지역명 (korea, asia, america, europe, utc)
    
    Returns:
        지역 시간대가 적용된 datetime 객체
    
    Examples:
        >>> dt = get_regional_datetime("korea")
        >>> print(dt.tzinfo)  # Asia/Seoul
    """
    timezone_name = _REGION_TIMEZONE_MAP.get(region.lower(), "UTC")
    try:
        tz = ZoneInfo(timezone_name)
        return datetime.now(tz)
    except Exception:
        # Fallback: UTC
        return datetime.now(timezone.utc)
