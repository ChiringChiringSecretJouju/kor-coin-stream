"""완전한 3-Tier 메트릭 시스템 - Layer별 독립 전송.

3개의 전문 Collector를 조율하여 다층 메트릭을 수집하고,
각 Layer를 독립된 토픽으로 전송합니다.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, TypeAlias

from src.common.logger import PipelineLogger
from src.common.metrics.processing import ProcessingMetricsCollector
from src.common.metrics.quality import QualityMetricsCalculator
from src.common.metrics.reception import ReceptionMetricsCollector
from src.core.dto.internal.metrics import (
    ProcessingMetricsDomain,
    QualityMetricsDomain,
    ReceptionMetricsDomain,
)

# 3개의 독립된 Emit Factory
ReceptionEmitFactory: TypeAlias = Callable[
    [list[ReceptionMetricsDomain], int, int], Coroutine[Any, Any, Any]
]
ProcessingEmitFactory: TypeAlias = Callable[
    [list[ProcessingMetricsDomain], int, int], Coroutine[Any, Any, Any]
]
QualityEmitFactory: TypeAlias = Callable[
    [list[QualityMetricsDomain], int, int], Coroutine[Any, Any, Any]
]


class MinuteBatchCounter:
    """완전한 3-Tier 메트릭 시스템 - Layer별 독립 전송.

    3개의 전문 Collector와 3개의 독립 토픽:
    - Layer 1: ReceptionMetricsCollector → ws.metrics.reception.{region}
    - Layer 2: ProcessingMetricsCollector → ws.metrics.processing.{region}
    - Layer 3: QualityMetricsCalculator → ws.metrics.quality.{region}
    """

    def __init__(
        self,
        reception_emit: ReceptionEmitFactory,
        processing_emit: ProcessingEmitFactory,
        quality_emit: QualityEmitFactory,
        logger: PipelineLogger,
        batch_size: int = 1,
    ) -> None:
        """
        Args:
            reception_emit: Layer 1 전송 함수
            processing_emit: Layer 2 전송 함수
            quality_emit: Layer 3 전송 함수
            logger: 로깅 인스턴스
            batch_size: 배치 크기 (분 단위)
        """
        self._reception_emit = reception_emit
        self._processing_emit = processing_emit
        self._quality_emit = quality_emit
        self.logger = logger

        # 3개의 전문 Collector
        self._reception = ReceptionMetricsCollector()
        self._processing = ProcessingMetricsCollector()
        self._quality = QualityMetricsCalculator(max_samples=1000)

        # 내부 상태
        self._kst = timezone(timedelta(hours=9))
        self._current_minute_key: int = int(time.time() // 60)

        # 3개의 독립 버퍼
        self._reception_buffer: list[dict[str, Any]] = []
        self._processing_buffer: list[dict[str, Any]] = []
        self._quality_buffer: list[dict[str, Any]] = []

        self._batch_size: int = max(1, int(batch_size))

    def inc(
        self,
        n: int = 1,
        symbol: str | None = None,
        byte_size: int = 0,
        parse_success: bool = True,
        start_time: float | None = None,
    ) -> None:
        """메시지 카운트 증가 (완전한 3-Tier, 독립 전송)."""
        now_s: float = time.time()
        minute_key: int = int(now_s // 60)

        # 분 경계 체크
        if minute_key != self._current_minute_key:
            self._rollover_minute()
            self._current_minute_key = minute_key
            self._schedule_emit_if_ready()

        # Layer 1: Reception
        self._reception.record_received(byte_size=byte_size)
        if parse_success:
            self._reception.record_parse_success()
        else:
            self._reception.record_parse_failed()

        # Layer 2: Processing
        if symbol:
            self._processing.record_success(symbol=symbol, count=n)
        else:
            self._processing.record_failed(count=n)
            # 실패율이 높을 때만 경고 (10% 이상)
            reception_snapshot = self._reception.snapshot()
            processing_snapshot = self._processing.snapshot()
            
            # 최소 100개 이상 수신 후 실패율 체크
            if reception_snapshot.total_received >= 100:
                failure_rate = (
                    processing_snapshot.total_failed / reception_snapshot.total_received
                )
                # 실패율 10% 이상일 때만 경고
                if failure_rate >= 0.10:
                    self.logger.warning(
                        "High symbol extraction failure rate",
                        extra={
                            "minute_key": minute_key,
                            "total_processed": processing_snapshot.total_processed,
                            "total_failed": processing_snapshot.total_failed,
                            "total_received": reception_snapshot.total_received,
                            "failure_rate": round(failure_rate, 4),
                        },
                    )

        # Layer 3: Quality (Latency)
        if start_time is not None:
            self._quality.record_latency(start_time=start_time)

    def _rollover_minute(self) -> None:
        """분 경계 롤오버: 3개 독립 버퍼에 각각 적재."""
        minute_dt_kst = datetime.fromtimestamp(
            self._current_minute_key * 60, tz=self._kst
        )
        minute_start_ts_kst = int(minute_dt_kst.timestamp())

        # 3개 Collector에서 스냅샷 수집
        reception = self._reception.snapshot()
        processing = self._processing.snapshot()
        quality = self._quality.calculate(
            total_processed=processing.total_processed,
            total_received=reception.total_received,
            symbol_count=len(processing.details or {}),
        )

        # Layer 1: Reception Domain → Buffer
        reception_item = ReceptionMetricsDomain(
            minute_start_ts_kst=minute_start_ts_kst,
            total_received=reception.total_received,
            total_parsed=reception.total_parsed,
            total_parse_failed=reception.total_parse_failed,
            bytes_received=reception.bytes_received,
        )
        self._reception_buffer.append(
            {
                "minute_start_ts_kst": reception_item.minute_start_ts_kst,
                "total_received": reception_item.total_received,
                "total_parsed": reception_item.total_parsed,
                "total_parse_failed": reception_item.total_parse_failed,
                "bytes_received": reception_item.bytes_received,
            }
        )

        # Layer 2: Processing Domain → Buffer
        processing_item = ProcessingMetricsDomain(
            minute_start_ts_kst=minute_start_ts_kst,
            total_processed=processing.total_processed,
            total_failed=processing.total_failed,
            details=processing.details or {},
        )
        self._processing_buffer.append(
            {
                "minute_start_ts_kst": processing_item.minute_start_ts_kst,
                "total_processed": processing_item.total_processed,
                "total_failed": processing_item.total_failed,
                "details": processing_item.details,
            }
        )

        # Layer 3: Quality Domain → Buffer
        quality_item = QualityMetricsDomain(
            minute_start_ts_kst=minute_start_ts_kst,
            data_completeness=quality.data_completeness,
            symbol_coverage=quality.symbol_coverage,
            avg_latency_ms=quality.avg_latency_ms,
            p95_latency_ms=quality.p95_latency_ms,
            p99_latency_ms=quality.p99_latency_ms,
            health_score=quality.health_score,
        )
        self._quality_buffer.append(
            {
                "minute_start_ts_kst": quality_item.minute_start_ts_kst,
                "data_completeness": quality_item.data_completeness,
                "symbol_coverage": quality_item.symbol_coverage,
                "avg_latency_ms": quality_item.avg_latency_ms,
                "p95_latency_ms": quality_item.p95_latency_ms,
                "p99_latency_ms": quality_item.p99_latency_ms,
                "health_score": quality_item.health_score,
            }
        )

        # Collector 초기화
        self._reception.reset()
        self._processing.reset()
        self._quality.reset()

    def _schedule_emit_if_ready(self) -> None:
        """버퍼가 배치 크기 이상이면 3개 Layer 독립 전송."""
        if len(self._reception_buffer) < self._batch_size:
            return

        # Layer 1: Reception 전송
        reception_dicts = self._reception_buffer[: self._batch_size]
        self._reception_buffer = self._reception_buffer[self._batch_size :]
        reception_items: list[ReceptionMetricsDomain] = [
            ReceptionMetricsDomain(
                minute_start_ts_kst=d["minute_start_ts_kst"],
                total_received=d["total_received"],
                total_parsed=d["total_parsed"],
                total_parse_failed=d["total_parse_failed"],
                bytes_received=d["bytes_received"],
            )
            for d in reception_dicts
        ]
        range_start = reception_items[0].minute_start_ts_kst
        range_end = reception_items[-1].minute_start_ts_kst + 59
        asyncio.create_task(
            self._reception_emit(reception_items, range_start, range_end)
        )

        # Layer 2: Processing 전송
        processing_dicts = self._processing_buffer[: self._batch_size]
        self._processing_buffer = self._processing_buffer[self._batch_size :]
        processing_items: list[ProcessingMetricsDomain] = [
            ProcessingMetricsDomain(
                minute_start_ts_kst=d["minute_start_ts_kst"],
                total_processed=d["total_processed"],
                total_failed=d["total_failed"],
                details=d["details"],
            )
            for d in processing_dicts
        ]
        asyncio.create_task(
            self._processing_emit(processing_items, range_start, range_end)
        )

        # Layer 3: Quality 전송
        quality_dicts = self._quality_buffer[: self._batch_size]
        self._quality_buffer = self._quality_buffer[self._batch_size :]
        quality_items: list[QualityMetricsDomain] = [
            QualityMetricsDomain(
                minute_start_ts_kst=d["minute_start_ts_kst"],
                data_completeness=d["data_completeness"],
                symbol_coverage=d["symbol_coverage"],
                avg_latency_ms=d["avg_latency_ms"],
                p95_latency_ms=d["p95_latency_ms"],
                p99_latency_ms=d["p99_latency_ms"],
                health_score=d["health_score"],
            )
            for d in quality_dicts
        ]
        asyncio.create_task(self._quality_emit(quality_items, range_start, range_end))

    async def flush_now(self) -> None:
        """잔여 버퍼를 즉시 동기 전송 (3개 Layer 독립)."""
        # 현재 분에 누계가 있으면 버퍼로 롤오버
        reception = self._reception.snapshot()
        processing = self._processing.snapshot()

        if (
            processing.total_processed > 0
            or processing.total_failed > 0
            or reception.total_received > 0
        ):
            self._rollover_minute()

        # Layer 1
        if self._reception_buffer:
            reception_items: list[ReceptionMetricsDomain] = [
                ReceptionMetricsDomain(
                    minute_start_ts_kst=d["minute_start_ts_kst"],
                    total_received=d["total_received"],
                    total_parsed=d["total_parsed"],
                    total_parse_failed=d["total_parse_failed"],
                    bytes_received=d["bytes_received"],
                )
                for d in self._reception_buffer
            ]
            range_start = reception_items[0].minute_start_ts_kst
            range_end = reception_items[-1].minute_start_ts_kst + 59
            await self._reception_emit(reception_items, range_start, range_end)
            self._reception_buffer.clear()

        # Layer 2
        if self._processing_buffer:
            processing_items: list[ProcessingMetricsDomain] = [
                ProcessingMetricsDomain(
                    minute_start_ts_kst=d["minute_start_ts_kst"],
                    total_processed=d["total_processed"],
                    total_failed=d["total_failed"],
                    details=d["details"],
                )
                for d in self._processing_buffer
            ]
            range_start = processing_items[0].minute_start_ts_kst
            range_end = processing_items[-1].minute_start_ts_kst + 59
            await self._processing_emit(processing_items, range_start, range_end)
            self._processing_buffer.clear()

        # Layer 3
        if self._quality_buffer:
            quality_items: list[QualityMetricsDomain] = [
                QualityMetricsDomain(
                    minute_start_ts_kst=d["minute_start_ts_kst"],
                    data_completeness=d["data_completeness"],
                    symbol_coverage=d["symbol_coverage"],
                    avg_latency_ms=d["avg_latency_ms"],
                    p95_latency_ms=d["p95_latency_ms"],
                    p99_latency_ms=d["p99_latency_ms"],
                    health_score=d["health_score"],
                )
                for d in self._quality_buffer
            ]
            range_start = quality_items[0].minute_start_ts_kst
            range_end = quality_items[-1].minute_start_ts_kst + 59
            await self._quality_emit(quality_items, range_start, range_end)
            self._quality_buffer.clear()
