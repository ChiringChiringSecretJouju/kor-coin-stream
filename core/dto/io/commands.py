from __future__ import annotations

from typing import Any

from pydantic import ConfigDict
from core.dto.io._base import BaseIOModelDTO
from core.types import SocketParams


class ConnectRequestDTO(BaseIOModelDTO):
    """연결 요청 이벤트 스키마(IO 모델)."""

    socket_mode: str
    symbols: str | list[str]
    orderbook_depth: int | None = None
    realtime_only: bool
    correlation_id: str | None = None


class ConnectionConfigDTO(BaseIOModelDTO):
    """연결 스키마(IO 모델)."""

    url: str
    socket_params: SocketParams


class CommandDTO(BaseIOModelDTO):
    """명령 스키마(IO 모델)."""

    type: str
    action: str
    target: dict[str, str]
    symbols: list[str]
    connection: ConnectionConfigDTO
    projection: list[str] | None = None
    schema_version: str | None = None
    ttl_ms: int | None = None
    routing: dict[str, Any] | None = None
    ts_issue: int | None = None
    ts_ingest: int | None = None

    model_config = ConfigDict(extra="forbid")


class DisconnectCommandDTO(BaseIOModelDTO):
    """연결 해제 명령 이벤트 스키마."""

    type: str
    action: str
    target: dict[str, str]
    reason: str | None = None
    correlation_id: str | None = None
    meta: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")
