from typing import Any, Callable
from decimal import Decimal
from collections import deque
import orjson

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
    """객체를 UTF-8 JSON bytes(orjson)로 직렬화.

    - orjson.dumps 사용, 실패 시 default 인코더 시도 후 str(value)로 폴백
    """

    try:
        return orjson.dumps(value)
    except TypeError:
        if default is not None:
            return orjson.dumps(default(value))
        return orjson.dumps(str(value))
