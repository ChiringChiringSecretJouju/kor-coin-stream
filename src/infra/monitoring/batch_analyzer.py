"""
배치 메시지 비동기 분석기 (Producer와 연동)

Kafka 배치 메시지의 성능 지표를 수집하고 이벤트로 전송합니다.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from src.core.dto.io.monitoring import (
    AggregatedPerformanceStatsDTO,
    BatchMonitoringEventDTO,
    BatchPerformanceStatsDTO,
    PerformanceSummaryEventDTO,
)
from src.core.types import ExchangeName, Region, RequestType


@dataclass
class BatchAnalyzer:
    """배치 메시지 성능 분석기 (비동기)
    
    배치 메시지의 크기, 심볼 다양성을 분석하여 성능 메트릭을 생성합니다.
    
    캐시 효율성 계산:
    - CPU 캐시 지역성(Locality) 기반
    - 1 / unique_symbols → 같은 심볼 연속 처리 시 L1/L2 캐시 재사용률 높음
    - 심볼 전환이 많을수록 컨텍스트 스위칭 증가 → 캐시 미스 증가
    """

    # 카운터
    total_messages: int = 0
    total_batches: int = 0

    # 배치 통계
    batch_sizes: list[int] = field(default_factory=list)
    symbols_per_batch: list[set[str]] = field(default_factory=list)

    # 시간 통계
    start_time: float = field(default_factory=time.time)

    def analyze_batch(
        self,
        batch_data: list[dict[str, Any]],
        region: Region,
        exchange: ExchangeName,
        request_type: RequestType,
    ) -> BatchMonitoringEventDTO | None:
        """배치 데이터 분석 및 DTO 반환 (실제 메시지 구조 기반)
        
        Args:
            batch_data: 배치 메시지 데이터 리스트 (data 필드의 내용)
            region: 지역
            exchange: 거래소
            request_type: 요청 타입
            
        Returns:
            BatchMonitoringEventDTO 또는 None
        """
        try:
            if not batch_data:
                return None

            # 통계 업데이트
            batch_size = len(batch_data)
            self.total_batches += 1
            self.total_messages += batch_size
            self.batch_sizes.append(batch_size)

            # 심볼 다양성 분석 (실제 메시지 구조: target_currency 필드 사용)
            symbols = set()
            for msg in batch_data:
                # 한국 거래소: target_currency (KRW-BTC, KRW-ETH 등)
                symbol = msg.get("target_currency", "UNKNOWN")
                if isinstance(symbol, str):
                    symbols.add(symbol)

            self.symbols_per_batch.append(symbols)
            unique_symbols = len(symbols)

            # 캐시 효율성 계산
            # 1/unique_symbols: 심볼이 적을수록 CPU 캐시 재사용률 높음
            cache_efficiency = 1.0 / unique_symbols if unique_symbols > 0 else 0.0

            timestamp_ms = int(time.time() * 1000)

            # DTO 생성
            stats = BatchPerformanceStatsDTO(
                batch_size=batch_size,
                unique_symbols=unique_symbols,
                cache_efficiency=cache_efficiency,
                collection_timestamp_ms=timestamp_ms,
            )

            return BatchMonitoringEventDTO(
                region=region,
                exchange=exchange,
                request_type=request_type,
                stats=stats,
            )

        except Exception:
            # 분석 실패는 무시
            return None

    def get_summary_stats(
        self,
        region: Region,
        exchange: ExchangeName,
        request_type: RequestType,
        summary_period_seconds: int,
    ) -> PerformanceSummaryEventDTO | None:
        """성능 요약 통계 DTO 반환
        
        Args:
            region: 지역
            exchange: 거래소
            request_type: 요청 타입
            summary_period_seconds: 요약 기간 (초)
            
        Returns:
            PerformanceSummaryEventDTO 또는 None
        """
        elapsed = time.time() - self.start_time

        if not self.batch_sizes:
            return None

        avg_batch_size = sum(self.batch_sizes) / len(self.batch_sizes)
        avg_symbols = sum(len(s) for s in self.symbols_per_batch) / len(
            self.symbols_per_batch
        )
        avg_cache_efficiency = 1.0 / avg_symbols if avg_symbols > 0 else 0.0
        messages_per_second = self.total_messages / elapsed if elapsed > 0 else 0.0
        batches_per_minute = (self.total_batches / elapsed) * 60 if elapsed > 0 else 0.0

        # DTO 생성
        aggregated_stats = AggregatedPerformanceStatsDTO(
            total_batches=self.total_batches,
            total_messages=self.total_messages,
            avg_batch_size=avg_batch_size,
            avg_symbols_per_batch=avg_symbols,
            avg_cache_efficiency=avg_cache_efficiency,
            messages_per_second=messages_per_second,
            batches_per_minute=batches_per_minute,
            elapsed_seconds=elapsed,
        )

        return PerformanceSummaryEventDTO(
            region=region,
            exchange=exchange,
            request_type=request_type,
            aggregated_stats=aggregated_stats,
            summary_period_seconds=summary_period_seconds,
        )

    def reset(self) -> None:
        """통계 초기화"""
        self.total_messages = 0
        self.total_batches = 0
        self.batch_sizes.clear()
        self.symbols_per_batch.clear()
        self.start_time = time.time()
