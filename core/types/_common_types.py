from __future__ import annotations

from typing import Any, Final, Literal, TypeAlias, assert_never
from enum import Enum

DEFAULT_SCHEMA_VERSION: Final[str] = "1.0"

# 공통 타입/별칭을 한곳에 모읍니다.
# - 코어 계층 어디서나 재사용 가능한 최소 단위만 정의합니다.
# - I/O 스키마(카프카 이벤트 등) 세부는 각 모듈에 두고, 여기에는 기반 타입만 둡니다.

# 지역/요청타입은 일단 자유 문자열을 허용합니다. 필요 시 Literal로 좁힐 수 있습니다.
Region: TypeAlias = Literal["korea"]
RequestType: TypeAlias = Literal["ticker", "orderbook", "trade"]
ExchangeName: TypeAlias = Literal["upbit", "bithumb", "coinone", "korbit"]
SocketParams: TypeAlias = dict[str, Any] | list[dict[str, Any]] | list[Any] | list[str]


# redis
class ConnectionStatus(Enum):
    """연결 상태 Enum.

    Redis에는 문자열 값(value)이 저장됩니다.
    """

    CONNECTED = "connected"
    CONNECTING = "connecting"
    DISCONNECTED = "disconnected"


CONNECTION_STATUS_CONNECTED: Final[ConnectionStatus] = ConnectionStatus.CONNECTED
CONNECTION_STATUS_CONNECTING: Final[ConnectionStatus] = ConnectionStatus.CONNECTING
CONNECTION_STATUS_DISCONNECTED: Final[ConnectionStatus] = ConnectionStatus.DISCONNECTED


def connection_status_format(status: ConnectionStatus) -> str:
    """상태 로깅 포맷터: Enum 분기 완전탐색 보장."""
    match status:
        case ConnectionStatus.CONNECTED:
            return ConnectionStatus.CONNECTED.value
        case ConnectionStatus.CONNECTING:
            return ConnectionStatus.CONNECTING.value
        case ConnectionStatus.DISCONNECTED:
            return ConnectionStatus.DISCONNECTED.value
        case _:
            # 타입 시스템 관점에서 도달 불가이나, 방어적으로 처리
            assert_never(status)
