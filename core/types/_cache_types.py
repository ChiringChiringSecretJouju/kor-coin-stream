from __future__ import annotations

# Consolidated from infra/cache/cache_types.py
from enum import Enum
from typing import TYPE_CHECKING, Final, TypedDict

# NOTE: 순환 참조 방지
# - 이 모듈은 core.types 패키지의 __init__에서 와일드카드 임포트로 불립니다.
# - 여기서 다시 core.types를 임포트하면 순환이 발생하므로,
#   타입 체크시에만 ConnectionScope를 임포트하고 런타임에는 문자열 힌트를 사용합니다.
if TYPE_CHECKING:  # pragma: no cover
    from core.dto.internal.common import ConnectionScope


class ConnectionStatus(Enum):
    """연결 상태 Enum.

    Redis에는 문자열 값(value)이 저장됩니다.
    """

    CONNECTED = "connected"
    CONNECTING = "connecting"
    DISCONNECTED = "disconnected"


class ConnectionMetaData(TypedDict):
    status: str
    created_at: int
    last_active: int
    connection_id: str
    scope: ConnectionScope


class ConnectionData(ConnectionMetaData):
    """연결 상태 데이터 구조체(메타 + 심볼 목록).

    - ConnectionMetaData(메타 해시 필드)를 상속 받아 중복 제거
    - symbols: 현재 구독 심볼 목록(Set에서 읽어 정렬 후 반환)
    """

    symbols: list[str]


CONNECTION_STATUS_CONNECTED: Final[ConnectionStatus] = ConnectionStatus.CONNECTED
CONNECTION_STATUS_CONNECTING: Final[ConnectionStatus] = ConnectionStatus.CONNECTING
CONNECTION_STATUS_DISCONNECTED: Final[ConnectionStatus] = ConnectionStatus.DISCONNECTED
