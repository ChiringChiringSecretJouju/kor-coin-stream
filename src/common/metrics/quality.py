"""Layer 3: Quality Metrics Calculator.

품질 메트릭을 계산합니다.
- 데이터 완전성
- 심볼 커버리지
- 처리 지연 통계 (평균, 백분위)
- 종합 건강도 점수
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class QualityMetrics:
    """Layer 3: 품질 메트릭 (불변).
    
    Attributes:
        data_completeness: 데이터 완전성 (처리 성공률, 0.0~1.0)
        symbol_coverage: 처리된 심볼 종류 수
        avg_latency_ms: 평균 처리 지연 (밀리초)
        p95_latency_ms: 95 백분위 처리 지연 (밀리초)
        p99_latency_ms: 99 백분위 처리 지연 (밀리초)
        health_score: 종합 건강도 점수 (0.0~100.0)
    """
    
    data_completeness: float = 1.0
    symbol_coverage: int = 0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    health_score: float = 100.0


class QualityMetricsCalculator:
    """Layer 3: 품질 메트릭 계산기.
    
    통계 및 품질 지표를 계산합니다.
    - Latency 샘플 수집 (메모리 효율적)
    - 백분위 계산
    - 건강도 점수 산출
    """
    
    __slots__ = ("_latencies", "_max_samples")
    
    def __init__(self, max_samples: int = 1000) -> None:
        """
        Args:
            max_samples: 저장할 최대 latency 샘플 수 (메모리 제한)
        """
        self._latencies: list[float] = []
        self._max_samples: int = max_samples
    
    def record_latency(self, start_time: float) -> None:
        """처리 지연 시간 기록.
        
        Args:
            start_time: 처리 시작 시각 (time.time())
        """
        latency_ms = (time.time() - start_time) * 1000.0
        
        # 메모리 제한: 최대 샘플 수 유지
        if len(self._latencies) >= self._max_samples:
            # 오래된 샘플 절반 제거 (FIFO)
            self._latencies = self._latencies[self._max_samples // 2 :]
        
        self._latencies.append(latency_ms)
    
    def calculate(
        self,
        total_processed: int,
        total_received: int,
        symbol_count: int,
    ) -> QualityMetrics:
        """품질 메트릭 계산.
        
        Args:
            total_processed: 정상 처리된 메시지 수
            total_received: 수신한 전체 메시지 수
            symbol_count: 처리된 심볼 종류 수
            
        Returns:
            계산된 품질 메트릭 (불변)
        """
        # 데이터 완전성 (처리 성공률)
        data_completeness = (
            total_processed / total_received if total_received > 0 else 1.0
        )
        
        # Latency 통계 계산
        avg_latency = sum(self._latencies) / len(self._latencies) if self._latencies else 0.0
        p95_latency = self._percentile(self._latencies, 95) if self._latencies else 0.0
        p99_latency = self._percentile(self._latencies, 99) if self._latencies else 0.0
        
        # 종합 건강도 점수 계산
        health_score = self._calculate_health_score(
            data_completeness=data_completeness,
            avg_latency_ms=avg_latency,
            symbol_count=symbol_count,
        )
        
        return QualityMetrics(
            data_completeness=round(data_completeness, 4),
            symbol_coverage=symbol_count,
            avg_latency_ms=round(avg_latency, 2),
            p95_latency_ms=round(p95_latency, 2),
            p99_latency_ms=round(p99_latency, 2),
            health_score=round(health_score, 1),
        )
    
    def reset(self) -> None:
        """메트릭 초기화 (분 롤오버 시)."""
        self._latencies.clear()
    
    @staticmethod
    def _percentile(data: list[float], percentile: int) -> float:
        """백분위 계산 (정렬 없이 효율적).
        
        Args:
            data: 데이터 샘플 리스트
            percentile: 백분위 (0-100)
            
        Returns:
            백분위 값
        """
        if not data:
            return 0.0
        
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100.0)
        index = min(index, len(sorted_data) - 1)
        return sorted_data[index]
    
    @staticmethod
    def _calculate_health_score(
        data_completeness: float,
        avg_latency_ms: float,
        symbol_count: int,
    ) -> float:
        """종합 건강도 점수 계산.
        
        점수 구성:
        - 데이터 완전성: 60% 가중치 (가장 중요)
        - Latency: 30% 가중치 (성능)
        - Symbol Coverage: 10% 가중치 (다양성)
        
        Args:
            data_completeness: 데이터 완전성 (0.0~1.0)
            avg_latency_ms: 평균 지연 (밀리초)
            symbol_count: 심볼 종류 수
            
        Returns:
            건강도 점수 (0.0~100.0)
        """
        # 1. 데이터 완전성 점수 (60점 만점)
        completeness_score = data_completeness * 60.0
        
        # 2. Latency 점수 (30점 만점)
        # - 10ms 이하: 만점
        # - 50ms 이상: 0점
        # - 중간: 선형 감소
        if avg_latency_ms <= 10.0:
            latency_score = 30.0
        elif avg_latency_ms >= 50.0:
            latency_score = 0.0
        else:
            latency_score = 30.0 * (1.0 - (avg_latency_ms - 10.0) / 40.0)
        
        # 3. Symbol Coverage 점수 (10점 만점)
        # - 3개 이상: 만점
        # - 0개: 0점
        # - 중간: 선형 증가
        if symbol_count >= 3:
            coverage_score = 10.0
        else:
            coverage_score = symbol_count * (10.0 / 3.0)
        
        total_score = completeness_score + latency_score + coverage_score
        return min(100.0, max(0.0, total_score))
