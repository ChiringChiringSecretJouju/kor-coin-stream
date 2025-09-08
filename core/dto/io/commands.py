from __future__ import annotations

from typing import Any

from pydantic import ConfigDict
from core.dto.io._base import BaseIOModel

from core.types import SocketParams

"""명령(Command) 관련 I/O DTO 집합.

- 팀 규칙: I/O 경계는 Pydantic v2 모델 사용
- 내부 도메인은 dataclass 사용
- 이 파일은 core/types/_commands_types.py의 3개 TypedDict를 Pydantic 모델로 이관한 구현입니다.
"""


class ConnectRequestEvent(BaseIOModel):
    """연결 요청 이벤트 스키마(IO 모델)."""

    socket_mode: str
    symbols: str | list[str]
    orderbook_depth: int | None = None
    realtime_only: bool
    correlation_id: str | None = None


class Connection(BaseIOModel):
    """연결 스키마(IO 모델)."""

    url: str
    socket_params: SocketParams


class CommandPayload(BaseIOModel):
    """명령 스키마(IO 모델)."""

    type: str
    action: str
    target: dict[str, Any]
    symbols: list[str]
    connection: Connection
    projection: list[str] | None = None
    schema_version: str | None = None

    model_config = ConfigDict(extra="forbid")
