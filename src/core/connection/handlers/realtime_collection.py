from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

from src.common.logger import PipelineLogger
from src.core.types import (
    OrderbookResponseData,
    TickerResponseData,
    TradeResponseData,
)

logger = PipelineLogger.get_logger("realtime_collector", "connection")

EmitFactory: type = Callable[[str, list[dict[str, Any]], int], Awaitable[bool]]


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

        # 배치 데이터 저장소 (메모리 사전 할당)
        self._ticker_batch: list[TickerResponseData] = []
        self._orderbook_batch: list[OrderbookResponseData] = []
        self._trade_batch: list[TradeResponseData] = []

        # 배치별 개별 타이머 (세밀한 제어)
        self._batch_timers: dict[str, float] = {
            "ticker": time.time(),
            "orderbook": time.time(),
            "trade": time.time(),
        }

        # 타이머 관리
        self._last_flush_time = time.time()
        self._flush_timer: asyncio.Task | None = None
        self._is_running = False

        # 성능 최적화: 배치 크기 예약
        self._reserve_batch_capacity()

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
        if self._ticker_batch or self._orderbook_batch or self._trade_batch:
            await self._flush_batches(force=True)

        logger.info("RealtimeBatchCollector stopped")

    def _reserve_batch_capacity(self) -> None:
        """배치 리스트 용량 사전 예약 (성능 최적화)"""
        # 메모리 사전 할당으로 리스트 재할당 오버헤드 감소
        if hasattr(list, "__sizeof__"):  # CPython에서만 지원
            try:
                # 예상 최대 크기로 사전 할당
                self._ticker_batch = [None] * self.max_batch_size  # type: ignore
                self._ticker_batch.clear()
                self._orderbook_batch = [None] * self.max_batch_size  # type: ignore
                self._orderbook_batch.clear()
                self._trade_batch = [None] * self.max_batch_size  # type: ignore
                self._trade_batch.clear()
            except Exception:
                # 실패 시 기본 리스트 사용
                pass

    async def add_message(self, message_type: str, data: dict[str, Any]) -> None:
        """메시지 배치에 추가 (성능 최적화)"""
        if not self._is_running:
            return

        current_time = time.time()
        batch_updated = False

        # 해당 타입 배치에 추가
        if message_type == "ticker":
            self._ticker_batch.append(data)
            self._batch_timers["ticker"] = current_time
            batch_updated = True
            # 최대 크기 초과 방지
            if len(self._ticker_batch) >= self.max_batch_size:
                await self._flush_single_batch("ticker")
                return
        elif message_type == "orderbook":
            self._orderbook_batch.append(data)
            self._batch_timers["orderbook"] = current_time
            batch_updated = True
            if len(self._orderbook_batch) >= self.max_batch_size:
                await self._flush_single_batch("orderbook")
                return
        elif message_type == "trade":
            self._trade_batch.append(data)
            self._batch_timers["trade"] = current_time
            batch_updated = True
            if len(self._trade_batch) >= self.max_batch_size:
                await self._flush_single_batch("trade")
                return

        # 배치 조건 확인 (개수 기준) - 단일 타입만 체크
        if batch_updated:
            batch_size = (
                len(self._ticker_batch)
                if message_type == "ticker"
                else (
                    len(self._orderbook_batch)
                    if message_type == "orderbook"
                    else len(self._trade_batch)
                )
            )

            if batch_size >= self.batch_size:
                await self._flush_single_batch(message_type)

    async def _auto_flush_timer(self) -> None:
        """백그라운드 타이머로 주기적 플러시"""
        while self._is_running:
            try:
                await asyncio.sleep(self.time_window)

                if not self._is_running:
                    break

                # 배치별 개별 시간 기준 플러시 (세밀한 제어)
                current_time = time.time()

                # 각 배치 타입별로 개별 타이머 체크
                for batch_type in ["ticker", "orderbook", "trade"]:
                    if (
                        current_time - self._batch_timers[batch_type]
                    ) >= self.time_window:
                        batch_size = (
                            len(self._ticker_batch)
                            if batch_type == "ticker"
                            else (
                                len(self._orderbook_batch)
                                if batch_type == "orderbook"
                                else len(self._trade_batch)
                            )
                        )

                        if batch_size > 0:
                            await self._flush_single_batch(batch_type)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto flush timer error: {e}")
                await asyncio.sleep(1)  # 에러 시 1초 대기

    async def _flush_single_batch(self, message_type: str) -> None:
        """단일 타입 배치 플러시"""
        current_time = time.time()

        if message_type == "ticker" and self._ticker_batch:
            batch = self._ticker_batch.copy()
            self._ticker_batch.clear()
            if self.emit_factory:
                await self.emit_factory("ticker", batch, int(current_time * 1000))

        elif message_type == "orderbook" and self._orderbook_batch:
            batch = self._orderbook_batch.copy()
            self._orderbook_batch.clear()
            if self.emit_factory:
                await self.emit_factory("orderbook", batch, int(current_time * 1000))

        elif message_type == "trade" and self._trade_batch:
            batch = self._trade_batch.copy()
            self._trade_batch.clear()
            if self.emit_factory:
                await self.emit_factory("trade", batch, int(current_time * 1000))

    async def _flush_batches(self, force: bool = False) -> None:
        """모든 배치 플러시"""
        current_time = time.time()

        # 각 타입별로 배치가 있으면 전송
        if self._ticker_batch:
            batch = self._ticker_batch.copy()
            self._ticker_batch.clear()
            if self.emit_factory:
                await self.emit_factory("ticker", batch, int(current_time * 1000))

        if self._orderbook_batch:
            batch = self._orderbook_batch.copy()
            self._orderbook_batch.clear()
            if self.emit_factory:
                await self.emit_factory("orderbook", batch, int(current_time * 1000))

        if self._trade_batch:
            batch = self._trade_batch.copy()
            self._trade_batch.clear()
            if self.emit_factory:
                await self.emit_factory("trade", batch, int(current_time * 1000))

        self._last_flush_time = current_time

        if force:
            logger.debug("Realtime batches flushed (force=True)")
        else:
            logger.debug("Realtime batches flushed (auto/count trigger)")

    def get_batch_status(self) -> dict[str, int]:
        """현재 배치 상태 조회 (디버깅용)"""
        return {
            "ticker": len(self._ticker_batch),
            "orderbook": len(self._orderbook_batch),
            "trade": len(self._trade_batch),
        }
