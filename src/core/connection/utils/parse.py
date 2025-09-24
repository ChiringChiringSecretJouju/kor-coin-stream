from __future__ import annotations

from typing import Any


def update_dict(message: dict[str, Any], key: str) -> dict[str, Any]:
    """message[key]가 dict 또는 list[dict]인 경우 해당 내용을 message에 병합하여 반환.

    - 공용 파싱 유틸로 승격된 함수입니다.
    - 입력은 변경하지 않고 복사본을 기반으로 처리합니다.
    """
    merged = dict(message)
    data_sub = message.get(key)
    if isinstance(data_sub, dict):
        merged.update(data_sub)
    elif isinstance(data_sub, list) and data_sub and isinstance(data_sub[0], dict):
        merged.update(data_sub[0])
    return merged
