from pydantic import BaseModel, ConfigDict


class BaseIOModel(BaseModel):
    """I/O 경계용 공통 Pydantic v2 베이스 모델.

    - Enum 직렬화를 값(value)로 고정
    - 알 수 없는 필드 금지(extra="forbid")
    """

    model_config = ConfigDict(use_enum_values=True, extra="forbid")
