from __future__ import annotations

from pydantic import BaseModel, field_validator

from core.types import ConnectionStatus, ExchangeName, Region, RequestType


class ConnectionMetaHash(BaseModel):
    """Redis 메타 해시 스키마 검증용 I/O 모델.

    - Redis 해시에서 읽은 원시 값을 파싱/검증한다.
    - status는 ConnectionStatus의 value만 허용.
    - created_at/last_active는 int로 정규화.
    """

    status: str
    created_at: int
    last_active: int
    connection_id: str
    exchange: ExchangeName
    region: Region
    request_type: RequestType

    @field_validator("status")
    @classmethod
    def _validate_status(cls, v: str) -> str:
        allowed = {e.value for e in ConnectionStatus}
        if v not in allowed:
            raise ValueError(f"invalid status: {v}")
        return v
