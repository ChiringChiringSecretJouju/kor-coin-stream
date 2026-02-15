from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal, TypeAlias

from src.common.logger import PipelineLogger
from src.core.types import (
    TickerResponseData,
    TradeResponseData,
)

logger = PipelineLogger.get_logger("realtime_collector", "connection")

# 타입 정의
EmitFactory: TypeAlias = Callable[[list[dict[str, Any]]], Awaitable[bool]]
MessageType: TypeAlias = Literal["ticker", "trade"]
BatchData: TypeAlias = TickerResponseData | TradeResponseData
MemoryStorage: TypeAlias = dict[MessageType, deque[BatchData]]
SymbolGroup: TypeAlias = dict[str, list[dict[str, Any]]]

# 상수 정의
MESSAGE_TYPES: tuple[MessageType, ...] = ("ticker", "trade")
ERROR_RETRY_DELAY: float = 1.0
DEFAULT_SYMBOL_KEY: str = "UNKNOWN"  # 심볼 추출 실패 시 기본값


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

        # 배치 데이터 저장소 (심볼별 격리 관리)
        # 구조: _batches[type][symbol] = deque
        self._batches: dict[str, dict[str, deque]] = {
            "ticker": defaultdict(lambda: deque(maxlen=self.max_batch_size)),
            "trade": defaultdict(lambda: deque(maxlen=self.max_batch_size)),
        }

        # 심볼별 마지막 플러시 시간 관리 (큐에 데이터가 들어온 시점)
        # 구조: _batch_timers[type][symbol] = timestamp
        self._batch_timers: dict[str, dict[str, float]] = {
            "ticker": defaultdict(lambda: 0.0),
            "trade": defaultdict(lambda: 0.0),
        }

        self._flush_timer: asyncio.Task | None = None
        self._is_running = False

    @staticmethod
    def _extract_symbol_from_message(data: dict[str, Any]) -> str:
        """메시지에서 심볼 추출 (플러시 시 그룹화용)

        우선순위:
        1. target_currency (표준 필드)
        2. symbol (일반적인 필드)
        3. code (업비트 등)
        4. market (빗썸 등)
        5. s (바이낸스 등)
        6. instId (OKX 등)
        7. product_id

        Args:
            data: 메시지 데이터

        Returns:
            추출된 심볼 또는 DEFAULT_SYMBOL_KEY
        """
        # 표준 필드 우선
        if symbol := data.get("target_currency"):
            return str(symbol).upper()

        # 2. 거래소별 필드 확인
        for key in ["symbol", "code", "market", "s", "instId", "product_id"]:
            if val := data.get(key):
                # 코인만 추출 (예: KRW-BTC → BTC, BTCUSDT → BTC, ETH/USD → ETH)
                symbol_str = str(val).upper()

                # 구분자 처리 (하이픈 -, 슬래시 /)
                for sep in ["-", "/"]:
                    if sep in symbol_str:
                        # 통상 페어에서 뒤쪽이 quote이므로 앞쪽 혹은 뒤쪽 선택 logic
                        parts = symbol_str.split(sep)
                        for part in parts:
                            # quote 성격이 강한 심볼 제외 시도
                            if part not in ["KRW", "USDT", "USD", "BTC", "ETH"]:
                                return part
                        return parts[0]  # fallback: 앞쪽 선택 (Asia/NA 기준)

                # USDT 등 제거 (BTCUSDT → BTC)
                for suffix in ["USDT", "BUSD", "USDC", "BTC", "ETH", "KRW"]:
                    if symbol_str.endswith(suffix) and len(symbol_str) > len(suffix):
                        return symbol_str[: -len(suffix)]

                return symbol_str

        return DEFAULT_SYMBOL_KEY

    @staticmethod
    def _extract_timestamp_from_message(data: dict[str, Any]) -> float:
        """정렬을 위한 타임스탬프 추출 (숫자형 및 ISO 문자열 대응)"""
        # 1. 숫자형 및 문자열 타임스탬프 키 확인
        ts_keys = ["trade_timestamp", "timestamp", "ts", "E", "T", "time"]
        for key in ts_keys:
            val = data.get(key)
            if not val:
                continue

            # 숫자형(int/float) 처리
            try:
                ts_val = float(val)
                if ts_val > 1e12:  # ms (13자리 이상)
                    return ts_val / 1000.0
                return ts_val
            except (ValueError, TypeError):
                # ISO 8601 문자열 처리 ("2025-09-24T04:22:00.543709Z" 형식)
                if isinstance(val, str) and ("T" in val):
                    try:
                        # 'Z'를 UTC 오프셋으로 변환하여 fromisoformat 지원
                        clean_ts = val.replace("Z", "+00:00")
                        return datetime.fromisoformat(clean_ts).timestamp()
                    except ValueError:
                        continue

        # 2. 없으면 현재 시각 (Fallback) - Explicit UTC
        return datetime.now(timezone.utc).timestamp()

    async def start(self) -> None:
        """컬렉터 시작 및 백그라운드 타이머 실행"""
        self._is_running = True

        # 백그라운드 타이머 시작
        self._flush_timer = asyncio.create_task(self._auto_flush_timer())

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

        # 잔여 모든 배치 플러시
        await self._flush_all_batches()

    async def add_message(self, message_type: str, data: dict[str, Any]) -> None:
        """메시지를 심볼별 큐에 추가하고 조건 만족 시 플러시"""
        if not self._is_running:
            return

        if message_type not in self._batches:
            logger.warning(f"Unknown message type: {message_type}")
            return

        symbol = self._extract_symbol_from_message(data)
        queue = self._batches[message_type][symbol]

        # 데이터가 처음 들어온 시점에 타이머 기록
        if not queue:
            self._batch_timers[message_type][symbol] = datetime.now(timezone.utc).timestamp()

        queue.append(data)

        # 갯수 기준 플러시
        if len(queue) >= self.batch_size:
            await self._flush_symbol_batch(message_type, symbol)

    async def _auto_flush_timer(self) -> None:
        """백그라운드 루프: 시간 기준(time_window) 플러시"""
        while self._is_running:
            try:
                await asyncio.sleep(0.5)  # 0.5초 간격으로 스위핑

                if not self._is_running:
                    break

                current_time = datetime.now(timezone.utc).timestamp()
                for msg_type in MESSAGE_TYPES:
                    # 복사본으로 순회 (RuntimeError 방지)
                    active_symbols = list(self._batches[msg_type].keys())
                    for symbol in active_symbols:
                        queue = self._batches[msg_type][symbol]
                        if not queue:
                            continue

                        last_flush = self._batch_timers[msg_type][symbol]
                        if current_time - last_flush >= self.time_window:
                            await self._flush_symbol_batch(msg_type, symbol)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto flush timer error: {e}")
                await asyncio.sleep(ERROR_RETRY_DELAY)

    async def _flush_symbol_batch(self, message_type: str, symbol: str) -> None:
        """특정 심볼의 배치를 정렬하여 전송"""
        if symbol not in self._batches[message_type]:
            return

        queue = self._batches[message_type][symbol]
        if not queue:
            return

        # 1. 큐 데이터 복사 및 초기화
        batch_to_send = list(queue)
        queue.clear()
        self._batch_timers[message_type][symbol] = datetime.now(timezone.utc).timestamp()

        # 2. 거래소 타임스탬프 기준으로 정렬 (주요 요청사항)
        batch_to_send.sort(key=self._extract_timestamp_from_message)

        # 3. 전송
        if self.emit_factory:
            try:
                await self.emit_factory(batch_to_send)
            except Exception as e:
                logger.error(f"Failed to flush {message_type} batch for {symbol}: {e}")

    async def _flush_all_batches(self) -> None:
        """중지 시 모든 잔여 데이터를 플러시"""
        for msg_type in MESSAGE_TYPES:
            symbols = list(self._batches[msg_type].keys())
            for symbol in symbols:
                await self._flush_symbol_batch(msg_type, symbol)
