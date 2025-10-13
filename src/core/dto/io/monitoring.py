"""배치 모니터링 DTO 모듈

배치 성능 모니터링 이벤트를 위한 Pydantic 모델들
"""

from pydantic import BaseModel, Field

from src.core.dto.io._base import BaseIOModelDTO
from src.core.types import ExchangeName, Region, RequestType

# ========================================
# 배치 성능 통계
# ========================================


class BatchPerformanceStatsDTO(BaseModel):
    """배치 수집 성능 통계 DTO

    Fields:
        batch_size: 배치 크기 (메시지 수)
        unique_symbols: 고유 심볼 수
        cache_efficiency: 캐시 효율성 (0.0~1.0)
        collection_timestamp_ms: 수집 타임스탬프 (밀리초)
    """

    batch_size: int = Field(..., description="배치 크기 (메시지 수)")
    unique_symbols: int = Field(..., description="고유 심볼 수")
    cache_efficiency: float = Field(..., description="캐시 효율성 (0.0~1.0)")
    collection_timestamp_ms: int = Field(
        ..., description="수집 타임스탬프 (Unix timestamp milliseconds)"
    )


class AggregatedPerformanceStatsDTO(BaseModel):
    """집계된 성능 통계 DTO

    Fields:
        total_batches: 총 배치 수
        total_messages: 총 메시지 수
        avg_batch_size: 배치당 평균 메시지 수
        avg_symbols_per_batch: 배치당 평균 심볼 수
        avg_cache_efficiency: 평균 캐시 효율성
        messages_per_second: 초당 메시지 수
        batches_per_minute: 분당 배치 수
        elapsed_seconds: 경과 시간 (초)
    """

    total_batches: int = Field(..., description="총 배치 수")
    total_messages: int = Field(..., description="총 메시지 수")
    avg_batch_size: float = Field(..., description="배치당 평균 메시지 수")
    avg_symbols_per_batch: float = Field(..., description="배치당 평균 심볼 수")
    avg_cache_efficiency: float = Field(..., description="평균 캐시 효율성")
    messages_per_second: float = Field(..., description="초당 메시지 수")
    batches_per_minute: float = Field(..., description="분당 배치 수")
    elapsed_seconds: float = Field(..., description="경과 시간 (초)")


# ========================================
# 모니터링 이벤트
# ========================================


class BatchMonitoringEventDTO(BaseIOModelDTO):
    """배치 수집 모니터링 이벤트 DTO

    토픽: monitoring.batch.performance
    이벤트 타입: batch_collected

    Fields:
        event_type: 이벤트 타입 ("batch_collected")
        region: 지역 (korea, asia, etc)
        exchange: 거래소 (upbit, bithumb, etc)
        request_type: 요청 타입 (ticker, orderbook, trade)
        stats: 배치 성능 통계
    """

    event_type: str = Field(default="batch_collected", description="이벤트 타입")
    region: Region = Field(..., description="지역")
    exchange: ExchangeName = Field(..., description="거래소")
    request_type: RequestType = Field(..., description="요청 타입")
    stats: BatchPerformanceStatsDTO = Field(..., description="배치 성능 통계")


class PerformanceSummaryEventDTO(BaseIOModelDTO):
    """성능 요약 모니터링 이벤트 DTO

    토픽: monitoring.batch.performance
    이벤트 타입: performance_summary

    Fields:
        event_type: 이벤트 타입 ("performance_summary")
        region: 지역
        exchange: 거래소
        request_type: 요청 타입
        aggregated_stats: 집계된 성능 통계
        summary_period_seconds: 요약 기간 (초)
    """

    event_type: str = Field(default="performance_summary", description="이벤트 타입")
    region: Region = Field(..., description="지역")
    exchange: ExchangeName = Field(..., description="거래소")
    request_type: RequestType = Field(..., description="요청 타입")
    aggregated_stats: AggregatedPerformanceStatsDTO = Field(
        ..., description="집계된 성능 통계"
    )
    summary_period_seconds: int = Field(..., description="요약 기간 (초)")
