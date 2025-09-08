from __future__ import annotations

from typing import Any, Final, Literal, TypeAlias

DEFAULT_SCHEMA_VERSION: Final[str] = "1.0"

# 공통 타입/별칭을 한곳에 모읍니다.
# - 코어 계층 어디서나 재사용 가능한 최소 단위만 정의합니다.
# - I/O 스키마(카프카 이벤트 등) 세부는 각 모듈에 두고, 여기에는 기반 타입만 둡니다.

# 지역/요청타입은 일단 자유 문자열을 허용합니다. 필요 시 Literal로 좁힐 수 있습니다.
Region: TypeAlias = Literal["korea"]
RequestType: TypeAlias = Literal["ticker", "orderbook", "trade"]
ExchangeName: TypeAlias = Literal["upbit", "bithumb", "coinone", "korbit"]
SocketParams: TypeAlias = dict[str, Any] | list[dict[str, Any]] | list[Any] | list[str]
