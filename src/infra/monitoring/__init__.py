"""
실시간 배치 모니터링 모듈

Kafka 토픽의 배치 메시지를 모니터링하고 성능 이벤트를 Kafka로 전송합니다.

주요 구성:
- BatchAnalyzer: 배치 데이터 분석 (DTO 반환)
- BatchPerformanceMonitor: 실시간 모니터링 루프
- BatchMonitoringProducer: src.infra.messaging.connect.producer_client에 있음
"""

from __future__ import annotations

from .batch_analyzer import BatchAnalyzer
from .kafka_monitor import BatchPerformanceMonitor

__all__ = [
    "BatchAnalyzer",
    "BatchPerformanceMonitor",
]
