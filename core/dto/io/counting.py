from pydantic import BaseModel, ConfigDict

from core.types import CountingItem, ExchangeName, Region, RequestType


class BatchPayload(BaseModel):
    """카운팅 배치 페이로드 (Pydantic v2 모델).

    - 카운팅 아이템 목록과 범위 메타데이터를 포함합니다.
    - unknown/extra 필드는 금지합니다.
    """

    range_start_ts_kst: int
    range_end_ts_kst: int
    bucket_size_sec: int
    items: CountingItem
    version: int

    model_config = ConfigDict(extra="forbid")


class MarketSocketCountingPayload(BaseModel):
    """마켓 소켓 카운팅 메시지 페이로드 (Pydantic v2 모델).

    - region/exchange/request_type 메타와 배치 본문을 포함합니다.
    - unknown/extra 필드는 금지합니다.
    """

    region: Region
    exchange: ExchangeName
    request_type: RequestType
    batch: BatchPayload

    model_config = ConfigDict(extra="forbid")
