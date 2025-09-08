from pydantic import BaseModel, ConfigDict

from core.types import ExchangeName, Region, RequestType


class ConnectionTarget(BaseModel):
    """이벤트 대상(Target) Pydantic v2 모델."""

    type: str | None = None
    exchange: ExchangeName
    region: Region
    request_type: RequestType

    model_config = ConfigDict(use_enum_values=True, extra="forbid")
