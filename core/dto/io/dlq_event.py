from typing import Any, Literal

from core.dto.io._base import BaseIOModelDTO
from core.dto.io.error_event import WsEventErrorMetaDTO
from core.dto.io.target import ConnectionTargetDTO


class DlqEventDTO(BaseIOModelDTO):
    """DLQ 이벤트 DTO (I/O 경계용 Pydantic v2 모델).

    - action은 "dlq"로 고정합니다.
    - unknown/extra 필드는 금지합니다.
    """

    action: Literal["dlq"]
    reason: str
    original_message: Any
    target: ConnectionTargetDTO | None = None
    meta: WsEventErrorMetaDTO
