"""Layer 2: Processing Metrics Collector.

메시지 처리 단계의 메트릭을 수집합니다.
- 정상 처리 (심볼 추출 성공)
- 처리 실패 (심볼 추출 실패)
- 심볼별 집계
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ProcessingMetrics:
    """Layer 2: 처리 메트릭 (불변).
    
    Attributes:
        total_processed: 정상 처리된 메시지 수 (심볼 추출 성공)
        total_failed: 처리 실패 메시지 수 (심볼 추출 실패)
        details: 심볼별 카운트 (예: {"BTC_COUNT": 500, "ETH_COUNT": 800})
    """
    
    total_processed: int = 0
    total_failed: int = 0
    details: dict[str, int] | None = None
    
    def __post_init__(self) -> None:
        """details를 dict로 초기화 (None이면)."""
        if self.details is None:
            object.__setattr__(self, "details", {})


class ProcessingMetricsCollector:
    """Layer 2: 처리 메트릭 수집기.
    
    메시지 처리 단계에서의 메트릭을 추적합니다.
    - 심볼별 집계
    - 성공/실패 카운트
    """
    
    __slots__ = ("_total_processed", "_total_failed", "_symbols")
    
    def __init__(self) -> None:
        self._total_processed: int = 0
        self._total_failed: int = 0
        self._symbols: dict[str, int] = {}
    
    def record_success(self, symbol: str, count: int = 1) -> None:
        """처리 성공 기록 (심볼 추출 성공).
        
        Args:
            symbol: 심볼 (예: "BTC_COUNT")
            count: 카운트 증가량
        """
        self._total_processed += count
        self._symbols[symbol] = self._symbols.get(symbol, 0) + count
    
    def record_failed(self, count: int = 1) -> None:
        """처리 실패 기록 (심볼 추출 실패).
        
        Args:
            count: 카운트 증가량
        """
        self._total_failed += count
    
    def snapshot(self) -> ProcessingMetrics:
        """현재 메트릭 스냅샷 반환 (불변)."""
        return ProcessingMetrics(
            total_processed=self._total_processed,
            total_failed=self._total_failed,
            details=self._symbols.copy(),
        )
    
    def reset(self) -> None:
        """메트릭 초기화 (분 롤오버 시)."""
        self._total_processed = 0
        self._total_failed = 0
        self._symbols.clear()
