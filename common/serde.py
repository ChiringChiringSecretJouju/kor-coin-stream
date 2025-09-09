from typing import Any, Callable
from decimal import Decimal
from collections import deque
import json

JSONDefault = Callable[[Any], Any]
Serializer = Callable[[Any], bytes]


def default_json_encoder(obj: Any) -> Any:
    """JSON 직렬화 헬퍼.

    - Decimal -> str
    - deque -> list
    - 그 외: 그대로 반환하여 json.dumps의 기본 동작에 위임
    """
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, deque):
        return list(obj)
    return obj


def to_bytes(value: Any, default: JSONDefault | None = None) -> bytes:
    """객체를 UTF-8 JSON bytes로 직렬화.

    Args:
        value: 직렬화할 값
        default: 추가 커스텀 인코더가 필요할 경우 주입
    """
    encoder = default or default_json_encoder
    return json.dumps(value, default=encoder).encode("utf-8")
