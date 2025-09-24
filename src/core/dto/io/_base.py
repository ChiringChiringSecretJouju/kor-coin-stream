import uuid
from pydantic import BaseModel, ConfigDict, Field


class BaseIOModelDTO(BaseModel):
    """I/O 경계용 공통 Pydantic v2 베이스 모델.

    - Enum 직렬화를 값(value)로 고정
    - 알 수 없는 필드 금지(extra="forbid")
    - ticket_id는 자동으로 UUID 생성
    """

    ticket_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_config = ConfigDict(use_enum_values=True, extra="forbid")
