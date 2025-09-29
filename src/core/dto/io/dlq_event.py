from datetime import datetime, timezone
from typing import Literal

from pydantic import Field

from src.core.dto.io._base import BaseIOModelDTO
from src.core.dto.io.error_event import WsEventErrorMetaDTO
from src.core.dto.io.target import ConnectionTargetDTO


class DlqEventDTO(BaseIOModelDTO):
    """DLQ 이벤트 DTO (I/O 경계용 Pydantic v2 모델).

    - action은 "dlq"로 고정합니다.
    - unknown/extra 필드는 금지합니다.
    """

    action: Literal["dlq"]
    reason: str
    # UTC 기준 시각을 추가 제공합니다(후방 호환). 기본값은 현재 UTC.
    error_timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    target: ConnectionTargetDTO | None = None
    meta: WsEventErrorMetaDTO
    detail_error: dict[str, str]
