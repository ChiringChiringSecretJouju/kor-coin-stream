"""
Kafka 토픽 모니터 (비동기)

실시간으로 Kafka 토픽을 모니터링하고 배치 성능 이벤트를 전송합니다.
"""

import asyncio

from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.dto.io.commands import ConnectionTargetDTO
from src.core.types import ExchangeName, Region, RequestType
from src.infra.messaging.clients.json_client import (
    AsyncConsumerWrapper as JsonConsumerWrapper,
)
from src.infra.messaging.clients.json_client import create_consumer
from src.infra.messaging.connect.producers.monitoring.batch_monitoring import (
    BatchMonitoringProducer,
)

from .batch_analyzer import BatchAnalyzer

logger = PipelineLogger.get_logger(__name__)


class BatchPerformanceMonitor:
    """배치 성능 실시간 모니터 (비동기)

    실시간 데이터 토픽을 구독하여 배치 성능을 분석하고 모니터링 이벤트를 전송합니다.

    Example:
        >>> monitor = BatchPerformanceMonitor(
        ...     region="korea",
        ...     exchange="upbit",
        ...     request_type="ticker",
        ... )
        >>> await monitor.start()
        >>> await asyncio.sleep(300)  # 5분 모니터링
        >>> await monitor.stop()
    """

    def __init__(
        self,
        region: Region,
        exchange: ExchangeName,
        request_type: RequestType,
        summary_interval: int = 60,
    ):
        """
        Args:
            region: 지역
            exchange: 거래소
            request_type: 요청 타입
            summary_interval: 요약 이벤트 전송 간격 (초)
            error_dispatcher: 에러 디스패처 (선택적, None이면 생성)
        """
        self.region = region
        self.exchange = exchange
        self.request_type = request_type
        self.summary_interval = summary_interval

        # 토픽 이름 생성: ticker-data.korea, orderbook-data.asia 등
        self.topic = f"{request_type}-data.{region}"

        # 분석기 및 Producer
        self.analyzer = BatchAnalyzer()
        self.consumer: JsonConsumerWrapper | None = None
        self.producer: BatchMonitoringProducer | None = None

        # 제어 플래그
        self._running = False
        self._monitor_task: asyncio.Task | None = None

    async def start(self) -> None:
        """모니터링 시작"""
        if self._running:
            return

        # Consumer 생성
        self.consumer = create_consumer(topics=[self.topic])
        await self.consumer.start()

        # Producer 생성
        self.producer = BatchMonitoringProducer()
        await self.producer.start_producer()

        # 모니터링 태스크 시작
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

        logger.info(
            f"배치 모니터링 시작: {self.region}/{self.exchange}/{self.request_type}"
        )

    async def stop(self) -> None:
        """모니터링 중지"""
        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        if self.consumer:
            await self.consumer.stop()

        if self.producer:
            await self.producer.stop_producer()

        logger.info(
            f"배치 모니터링 중지: {self.region}/{self.exchange}/{self.request_type}"
        )

    async def _monitor_loop(self) -> None:
        """메시지 모니터링 루프 (실제 메시지 구조 기반)"""
        last_summary = asyncio.get_event_loop().time()

        try:
            async for record in self.consumer:
                if not self._running:
                    break

                try:
                    # 실제 메시지 구조:
                    # {
                    #   "exchange": "bithumb",
                    #   "region": "korea",
                    #   "request_type": "ticker",
                    #   "timestamp_ms": 1760031213693,
                    #   "batch_size": 20,
                    #   "data": [{"target_currency": "KRW-BTC", ...}, ...]
                    # }
                    value = record["value"]
                    if not isinstance(value, dict):
                        continue

                    # 배치 데이터 추출
                    batch_data = value.get("data", [])
                    if not batch_data:
                        continue

                    # 분석 수행 (DTO 반환)
                    event = self.analyzer.analyze_batch(
                        batch_data=batch_data,
                        region=self.region,
                        exchange=self.exchange,
                        request_type=self.request_type,
                    )

                    # 이벤트 전송
                    if event and self.producer:
                        await self.producer.send_batch_event(event)

                    # 주기적 요약 전송
                    current_time = asyncio.get_event_loop().time()
                    if current_time - last_summary >= self.summary_interval:
                        summary_event = self.analyzer.get_summary_stats(
                            region=self.region,
                            exchange=self.exchange,
                            request_type=self.request_type,
                            summary_period_seconds=self.summary_interval,
                        )

                        if summary_event and self.producer:
                            await self.producer.send_summary_event(summary_event)

                        last_summary = current_time

                except Exception as msg_error:
                    # 개별 메시지 처리 오류 → ErrorDispatcher로 전송
                    target = ConnectionTargetDTO(
                        exchange=self.exchange,
                        region=self.region,
                        request_type=self.request_type,
                    )
                    await dispatch_error(
                        exc=msg_error,
                        kind="batch_monitoring_message",
                        target=target,
                        context={
                            "topic": self.topic,
                            "error_source": "BatchPerformanceMonitor._monitor_loop",
                        },
                    )

        except asyncio.CancelledError:
            pass
        except Exception as e:
            # 치명적 오류 → ErrorDispatcher로 전송 + 모니터링 중지
            target = ConnectionTargetDTO(
                exchange=self.exchange,
                region=self.region,
                request_type=self.request_type,
            )
            await dispatch_error(
                exc=e,
                kind="batch_monitoring_fatal",
                target=target,
                context={
                    "topic": self.topic,
                    "error_source": "BatchPerformanceMonitor._monitor_loop",
                    "fatal": True,
                },
            )
