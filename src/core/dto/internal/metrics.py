from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from typing import Any


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class ReceptionMetricsDomain:
    """Layer 1: 수신 메트릭 항목 (독립 전송용)."""

    minute_start_ts_kst: int
    total_received: int  # WebSocket에서 받은 모든 메시지
    total_parsed: int  # JSON 파싱 성공
    total_parse_failed: int  # JSON 파싱 실패
    bytes_received: int  # 수신 바이트 수


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class ProcessingMetricsDomain:
    """Layer 2: 처리 메트릭 항목 (독립 전송용)."""

    minute_start_ts_kst: int
    total_processed: int  # 정상 처리 (심볼 추출 성공)
    total_failed: int  # 처리 실패 (심볼 추출 실패)
    details: dict[str, int]  # 심볼별 집계


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class QualityMetricsDomain:
    """Layer 3: 품질 메트릭 항목 (독립 전송용)."""

    minute_start_ts_kst: int
    data_completeness: float  # 완전성 (0.0~1.0)
    symbol_coverage: int  # 처리된 심볼 종류 수
    avg_latency_ms: float  # 평균 처리 지연
    p95_latency_ms: float  # 95 백분위 지연
    p99_latency_ms: float  # 99 백분위 지연
    health_score: float  # 종합 건강도 점수


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class MinuteStateDomain:
    """내부 상태 캡슐화 (다층 메트릭 시스템, 슬롯으로 오버헤드 축소)."""

    kst: timezone
    current_minute_key: int
    # Layer 2: Processing Metrics
    total: int  # 정상 처리된 메시지 (심볼 추출 성공)
    symbols: dict[str, int]  # 심볼별 집계
    # Layer 1: Raw Reception Metrics
    total_received: int  # 수신한 모든 메시지
    total_failed: int  # 심볼 추출 실패
    # NOTE: buffer는 직렬화 경계 전 단계의 임시 저장소입니다.
    # - 내부 로직은 MinuteItem으로 다루지만, 프로듀서 경계에서 직렬화 용이성을 위해
    #   dict 형태로 보관합니다.
    # - dict 값 타입에 Any를 사용하는 이유: 세부 필드 확장(추가 메타) 가능성과
    #   직렬화 층(프로듀서)에서의 유연성을 허용하기 위함입니다.
    buffer: list[dict[str, Any]]
    batch_size: int = 5
