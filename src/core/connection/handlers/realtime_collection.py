from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Awaitable, Callable, Literal, TypeAlias

from src.common.logger import PipelineLogger
from src.core.types import (
    OrderbookResponseData,
    TickerResponseData,
    TradeResponseData,
)

logger = PipelineLogger.get_logger("realtime_collector", "connection")

# 타입 정의
EmitFactory: TypeAlias = Callable[[list[dict[str, Any]]], Awaitable[bool]]
MessageType: TypeAlias = Literal["ticker", "orderbook", "trade"]
BatchData: TypeAlias = TickerResponseData | OrderbookResponseData | TradeResponseData
MemoryStorage: TypeAlias = dict[MessageType, deque[BatchData]]

# 상수 정의
MESSAGE_TYPES: tuple[MessageType, ...] = ("ticker", "orderbook", "trade")
ERROR_RETRY_DELAY: float = 1.0


class RealtimeBatchCollector:
    """실시간 데이터를 배치로 수집하는 컬렉터 (성능 최적화 버전)

    배치 전략:
    - Count Based: 30-50개 메시지 모이면 전송
    - Time Based: 5-10초마다 전송
    - Hybrid: 둘 중 하나라도 조건 만족하면 전송
    - Auto Timer: 백그라운드 타이머로 주기적 플러시

    성능 최적화:
    - 메모리 사전 할당으로 리스트 재할당 최소화
    - 배치별 개별 타이머로 세밀한 제어
    - 비동기 큐 기반 처리로 블로킹 최소화
    """

    def __init__(
        self,
        batch_size: int = 30,  # 30개 메시지
        time_window: float = 5.0,  # 5초 타임윈도우
        max_batch_size: int = 50,  # 최대 50개
        emit_factory: EmitFactory | None = None,
    ):
        self.batch_size = batch_size
        self.time_window = time_window
        self.max_batch_size = max_batch_size
        self.emit_factory = emit_factory

        # 배치 데이터 저장소 (딕셔너리로 통합 관리)
        self._batches: MemoryStorage = {
            "ticker": deque(maxlen=self.max_batch_size),
            "orderbook": deque(maxlen=self.max_batch_size),
            "trade": deque(maxlen=self.max_batch_size),
        }

        # 배치별 개별 타이머
        current_time = time.time()
        self._batch_timers: dict[MessageType, float] = {
            msg_type: current_time for msg_type in MESSAGE_TYPES
        }

        # 타이머 관리
        self._last_flush_time = current_time
        self._flush_timer: asyncio.Task | None = None
        self._is_running = False

    async def start(self) -> None:
        """컬렉터 시작 및 백그라운드 타이머 실행"""
        self._is_running = True
        self._last_flush_time = time.time()

        # 백그라운드 타이머 시작
        self._flush_timer = asyncio.create_task(self._auto_flush_timer())

        logger.info(
            f"""
            RealtimeBatchCollector started: batch_size={self.batch_size},
            time_window={self.time_window}s, max_size={self.max_batch_size}
            """
        )

    async def stop(self) -> None:
        """컬렉터 중지 및 잔여 배치 플러시"""
        self._is_running = False

        # 타이머 취소
        if self._flush_timer and not self._flush_timer.done():
            self._flush_timer.cancel()
            try:
                await self._flush_timer
            except asyncio.CancelledError:
                pass

        # 잔여 배치 플러시
        if any(self._batches.values()):
            await self._flush_batches(force=True)

        logger.info("RealtimeBatchCollector stopped")

    async def add_message(self, message_type: str, data: dict[str, Any]) -> None:
        """메시지 배치에 추가 (성능 최적화)"""
        if not self._is_running:
            return

        # 타입 검증
        if message_type not in self._batches:
            logger.warning(f"Unknown message type: {message_type}")
            return

        # 배치에 추가
        batch = self._batches[message_type]
        batch.append(data)
        self._batch_timers[message_type] = time.time()

        # 플러시 조건 확인
        batch_len = len(batch)
        if batch_len >= self.max_batch_size or batch_len >= self.batch_size:
            await self._flush_single_batch(message_type)

    async def _auto_flush_timer(self) -> None:
        """백그라운드 타이머로 주기적 플러시"""
        while self._is_running:
            try:
                await asyncio.sleep(self.time_window)

                if not self._is_running:
                    break

                # 배치별 개별 시간 기준 플러시
                current_time = time.time()
                for msg_type in MESSAGE_TYPES:
                    if (
                        current_time - self._batch_timers[msg_type] >= self.time_window
                        and self._batches[msg_type]
                    ):
                        await self._flush_single_batch(msg_type)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto flush timer error: {e}")
                await asyncio.sleep(ERROR_RETRY_DELAY)

    async def _flush_single_batch(self, message_type: str) -> None:
        """단일 타입 배치 플러시 (성능 최적화: 참조 교체)"""
        if message_type not in self._batches:
            return

        batch = self._batches[message_type]
        if not batch or not self.emit_factory:
            return

        # 참조 교체로 copy() 오버헤드 제거
        self._batches[message_type] = deque(maxlen=self.max_batch_size)
        await self.emit_factory(batch)

    async def _flush_batches(self, force: bool = False) -> None:
        """모든 배치 플러시"""
        for msg_type in MESSAGE_TYPES:
            await self._flush_single_batch(msg_type)

        self._last_flush_time = time.time()

        if force:
            logger.debug("Realtime batches flushed (force=True)")
        else:
            logger.debug("Realtime batches flushed (auto/count trigger)")

    def get_batch_status(self) -> dict[str, int]:
        """현재 배치 상태 조회 (디버깅용)"""
        return {msg_type: len(self._batches[msg_type]) for msg_type in MESSAGE_TYPES}
