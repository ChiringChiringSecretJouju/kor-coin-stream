from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, TypeAlias

from common.logger import PipelineLogger
from core.dto.internal.metrics import MinuteItemDomain, MinuteStateDomain

# NOTE: emit_factory 반환 타입에 Any를 사용하는 이유
# - 호출자는 결과 값을 사용하지 않고 fire-and-forget 패턴으로
#   스케줄링만 합니다.
# - 반환 타입을 구체화할 근거가 없어, 인터페이스 유연성을 위해
#   Any 허용.
EmitFactory: TypeAlias = Callable[
    [list[MinuteItemDomain], int, int], Coroutine[Any, Any, Any]
]


class MinuteBatchCounter:
    """수신 메시지 카운트를 1분 단위로 합산하고, 1개(1분) 모아 비동기 배치를 발행합니다.

    - inc()는 초경량이며, 분 경계가 바뀌었을 때만 내부 상태를 롤오버합니다.
    - 배치 길이가 1개가 되면 `emit_factory`에서 생성된 코루틴을
      asyncio.create_task로 발행합니다.
    - emit은 비동기로 처리되어 수신 루프를 블로킹하지 않습니다.

    설계 노트:
    - 내부 버퍼는 직렬화 경계 전 단계로 dict를 저장하지만, 외부로 내보낼 때는
      `MinuteItem` 리스트로 복원하여 타입 일관성을 유지합니다.
      직렬화는 프로듀서에서 수행합니다.
    - emit_factory 시그니처:
      (items, range_start_ts_kst, range_end_ts_kst)
      -> Coroutine 생성자
    logger: 로깅 인스턴스
    """

    def __init__(
        self,
        emit_factory: EmitFactory,
        logger: PipelineLogger,
        batch_size: int = 1,
    ) -> None:
        """
        Args:
            emit_factory:
                (items, range_start_ts_kst, range_end_ts_kst)
                -> Coroutine 생성자
            logger: 로깅 인스턴스
        """
        self._emit_factory = emit_factory
        self.logger = logger
        self._state = MinuteStateDomain(
            kst=timezone(timedelta(hours=9)),
            current_minute_key=int(time.time() // 60),
            total=0,
            symbols={},
            buffer=[],
        )
        # 배치 크기(분 단위). 최소 1분 보장
        self._batch_size: int = max(1, int(batch_size))

    def inc(self, n: int = 1, symbol: str | None = None) -> None:
        """
        Args:
            n: 카운트 증가량
            symbol: 심볼(선택)
        """
        now_s: float = time.time()
        minute_key = int(now_s // 60)
        if minute_key != self._state.current_minute_key:
            self._rollover_minute()
            self._state.current_minute_key = minute_key
            self._schedule_emit_if_ready()

        # 현재 분 카운트 반영
        self._state.total += n
        if symbol:
            self._state.symbols[symbol] = self._state.symbols.get(symbol, 0) + n

    def _rollover_minute(self) -> None:
        """분 경계 롤오버 처리: 현재 분을 버퍼에 적재하고 누계 리셋."""
        minute_dt_kst = datetime.fromtimestamp(
            self._state.current_minute_key * 60, tz=self._state.kst
        )
        minute_start_ts_kst = int(minute_dt_kst.timestamp())
        item = MinuteItemDomain(
            minute_start_ts_kst=minute_start_ts_kst,
            total=self._state.total,
            details=self._state.symbols.copy(),
        )

        # 내부 버퍼 타입은 MinuteItem 기반이며, 외부 전송 시 직렬화는 프로듀서 경계에서 수행
        self._state.buffer.append(
            {
                "minute_start_ts_kst": item.minute_start_ts_kst,
                "total": item.total,
                "details": item.details,
            }
        )
        self._state.total = 0
        self._state.symbols.clear()

    def _schedule_emit_if_ready(self) -> None:
        """버퍼가 1개 이상이면 비동기 배치 전송 태스크를 스케줄합니다."""
        if len(self._state.buffer) < self._batch_size:
            return
        items_dicts = self._state.buffer[: self._batch_size]
        self._state.buffer = self._state.buffer[self._batch_size :]
        # dict를 MinuteItem으로 복원하여 타입 일관성 유지
        items: list[MinuteItemDomain] = [
            MinuteItemDomain(
                minute_start_ts_kst=d["minute_start_ts_kst"],
                total=d["total"],
                details=d["details"],
            )
            for d in items_dicts
        ]
        range_start_ts_kst = items[0].minute_start_ts_kst
        range_end_ts_kst = items[-1].minute_start_ts_kst + 59
        coro = self._emit_factory(items, range_start_ts_kst, range_end_ts_kst)
        task = asyncio.create_task(coro)

        def _done_cb(t: asyncio.Task) -> None:
            if exc := t.exception():
                self.logger.warning(
                    "MinuteBatchCounter emit task error",
                    extra={"error": str(exc)},
                )

        task.add_done_callback(_done_cb)

    async def flush_now(self) -> None:
        """잔여 버퍼를 즉시 동기 전송합니다.

        - 현재 분 누계가 남아 있으면 롤오버하여 버퍼에 적재한 뒤 전송합니다.
        - 종료/정리 시점에서 호출하여 메트릭 손실을 줄입니다.
        """
        # 현재 분에 누계가 있으면 버퍼로 롤오버
        if self._state.total > 0 or self._state.symbols:
            self._rollover_minute()

        if not self._state.buffer:
            return

        # 버퍼 전체를 한 번에 전송
        items: list[MinuteItemDomain] = [
            MinuteItemDomain(
                minute_start_ts_kst=d["minute_start_ts_kst"],
                total=d["total"],
                details=d["details"],
            )
            for d in self._state.buffer
        ]
        range_start_ts_kst = items[0].minute_start_ts_kst
        range_end_ts_kst = items[-1].minute_start_ts_kst + 59

        await self._emit_factory(items, range_start_ts_kst, range_end_ts_kst)
        self._state.buffer.clear()
