from pydantic import BaseModel, ConfigDict

from core.types import ExchangeName, Region, RequestType


class TargetModel(BaseModel):
    """이벤트 대상(Target) Pydantic v2 모델.

    - 기존 TypedDict(Target) 대신 I/O 경계에서 엄격한 검증/직렬화를 제공합니다.
    - 기존 호출부가 dict를 전달해도 Pydantic이 자동으로 모델로 변환합니다.
    """

    type: str | None = None
    exchange: ExchangeName
    region: Region
    request_type: RequestType

    model_config = ConfigDict(use_enum_values=True, extra="forbid")
