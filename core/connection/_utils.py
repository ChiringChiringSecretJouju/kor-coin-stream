import asyncio
import time
from typing import Any

from websockets.exceptions import ConnectionClosed
from websockets.frames import Close


def update_dict(message: dict, key: str) -> dict:
    """
    message[key]가 dict 또는 list[dict]인 경우 해당 내용을 message에 병합하여 반환
    """
    merged = message.copy()
    data_sub = message.get(key)

    if isinstance(data_sub, dict):
        merged.update(data_sub)
    elif isinstance(data_sub, list) and data_sub and isinstance(data_sub[0], dict):
        merged.update(data_sub[0])

    return merged


def extract_symbol(message: dict[str, Any]) -> str | None:
    """메시지에서 카운터 키를 추출합니다.

    반환 형식: `{TOKEN}_COUNT`
    우선순위:
    - 최상위: code → symbol → market
    - data 블록: target_currency(coinone) → symbol → s → market → code
    정규화 규칙:
    - "KRW-BTC" 등 '-' 포함 시 마지막 세그먼트 → BTC
    - "btc_krw" 등 '_' 포함 시 첫 세그먼트 → btc
    - 그 외 전체 문자열을 토큰으로 간주
    """
    if not isinstance(message, dict):
        return None

    def token_from(s: str) -> str:
        if "-" in s:
            return s.rsplit("-", 1)[-1]
        if "_" in s:
            return s.split("_", 1)[0]
        return s

    def fmt(token: str | None) -> str | None:
        return f"{token.upper()}_COUNT" if isinstance(token, str) and token else None

    # 1) 최상위 필드 우선
    for key in ("code", "symbol", "market"):
        v = message.get(key)
        if isinstance(v, str):
            return fmt(token_from(v))

    # 2) data 블록 처리
    data = message.get("data")
    if isinstance(data, dict):
        # coinone 전용: target_currency 최우선(토큰)
        v = data.get("target_currency")
        if isinstance(v, str):
            return fmt(v)
        # 일반 필드 스캔
        for key in ("symbol", "s", "market", "code"):
            v = data.get(key)
            if isinstance(v, str):
                return fmt(token_from(v))

    return None
