"""딕셔너리 유틸리티 함수.

순환 import를 방지하기 위해 parse.py에서 분리되었습니다.
"""

from __future__ import annotations

from typing import Any


def update_dict(message: dict[str, Any], key: str) -> dict[str, Any]:
    """message[key]가 dict 또는 list[dict]인 경우 해당 내용을 message에 병합하여 반환.

    - 공용 파싱 유틸로 승격된 함수입니다.
    - 입력은 변경하지 않고 복사본을 기반으로 처리합니다.

    Args:
        message: 원본 메시지 딕셔너리
        key: 병합할 키

    Returns:
        병합된 새로운 딕셔너리

    Examples:
        >>> msg = {"a": 1, "data": {"b": 2, "c": 3}}
        >>> result = update_dict(msg, "data")
        >>> result
        {"a": 1, "data": {"b": 2, "c": 3}, "b": 2, "c": 3}

        >>> msg = {"a": 1, "data": [{"b": 2, "c": 3}]}
        >>> result = update_dict(msg, "data")
        >>> result
        {"a": 1, "data": [{"b": 2, "c": 3}], "b": 2, "c": 3}
    """
    merged = dict(message)
    data_sub = message.get(key)
    if isinstance(data_sub, dict):
        merged.update(data_sub)
    elif isinstance(data_sub, list) and data_sub and isinstance(data_sub[0], dict):
        merged.update(data_sub[0])
    return merged
