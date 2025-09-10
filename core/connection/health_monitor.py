from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from common.logger import PipelineLogger
from core.dto.internal.common import ConnectionPolicyDomain, ConnectionScopeDomain
from core.dto.io.target import ConnectionTargetDTO
from core.dto.adapter.error_adapter import make_ws_error_event_from_kind


logger = PipelineLogger.get_logger("health_monitor", "connection")


class ConnectionHealthMonitor:
    """연결 상태 감시 전담 클래스

    책임:
    - 하트비트 전송 관리
    - 워치독 타이머 관리
    - 연결 상태 추적
    """

    def __init__(
        self, scope: ConnectionScopeDomain, policy: ConnectionPolicyDomain
    ) -> None:
        self.scope = scope
        self.policy = policy

        # 상태 추적 (단순한 인스턴스 변수)
        self._last_heartbeat_ts: float = 0.0
        self._heartbeat_fail_count: int = 0
        self._is_monitoring: bool = False

        # 태스크 관리
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None

    def update_policy(
        self,
        kind: Literal["frame", "text"] = "frame",
        message: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        """하트비트 설정을 동적으로 구성"""
        # 기존 정책을 유지하면서 하트비트 관련 설정만 업데이트
        self.policy.heartbeat_kind = kind
        self.policy.heartbeat_message = message
        self.policy.heartbeat_timeout = timeout

    async def _emit_error(
        self, err: BaseException, *, phase: str, extra: dict | None = None
    ) -> None:
        """헬스 모니터 단계 오류를 표준 에러 이벤트로 발행한다."""
        observed_key = (
            f"{self.scope.exchange}/{self.scope.region}/{self.scope.request_type}"
        )
        target = ConnectionTargetDTO(
            exchange=self.scope.exchange,
            region=self.scope.region,
            request_type=self.scope.request_type,
        )
        raw_context: dict = {
            "phase": phase,
            "heartbeat_kind": self.policy.heartbeat_kind,
            "heartbeat_timeout": self.policy.heartbeat_timeout,
            "heartbeat_fail_count": self._heartbeat_fail_count,
            **(extra or {}),
        }
        await make_ws_error_event_from_kind(
            target=target,
            err=err,
            kind="ws",
            observed_key=observed_key,
            raw_context=raw_context,
        )

    async def send_heartbeat(self, websocket: Any) -> None:
        """하트비트 전송"""
        try:
            if self.policy.heartbeat_kind == "frame":
                await websocket.ping()
            else:
                if self.policy.heartbeat_message:
                    await websocket.send(self.policy.heartbeat_message)

            self._update_heartbeat_status(success=True)
            logger.debug(f"{self.scope.exchange}: 하트비트 전송 성공")

        except Exception as e:
            self._update_heartbeat_status(success=False)
            logger.warning(f"{self.scope.exchange}: 하트비트 전송 실패 - {e}")
            await self._emit_error(e, phase="send_heartbeat")
            raise

    async def start_monitoring(self, websocket: Any, ping_interval: int) -> None:
        """하트비트 모니터링 시작"""
        self._last_heartbeat_ts = time.monotonic()
        self._heartbeat_fail_count = 0
        self._is_monitoring = True

        # 하트비트 태스크 시작
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(websocket, ping_interval)
        )

        logger.info(f"{self.scope.exchange}: 연결 상태 모니터링 시작")

    async def stop_monitoring(self) -> None:
        """모니터링 중단"""
        self._is_monitoring = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None

        logger.info(f"{self.scope.exchange}: 연결 상태 모니터링 중단")

    async def _heartbeat_loop(self, websocket: Any, interval: int) -> None:
        """하트비트 루프"""
        while self._is_monitoring:
            try:
                await asyncio.sleep(interval)
                await self.send_heartbeat(websocket)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{self.scope.exchange}: 하트비트 루프 에러 - {e}")
                await self._emit_error(e, phase="heartbeat_loop")
                break

    def _update_heartbeat_status(self, success: bool) -> None:
        """하트비트 상태 업데이트"""
        if success:
            self._last_heartbeat_ts = time.monotonic()
            self._heartbeat_fail_count = 0
        else:
            self._heartbeat_fail_count += 1

    def is_healthy(self) -> bool:
        """연결 상태가 건강한지 확인"""
        return self._heartbeat_fail_count < self.policy.heartbeat_fail_limit

    @property
    def heartbeat_fail_count(self) -> int:
        """하트비트 실패 횟수 반환"""
        return self._heartbeat_fail_count

    @property
    def last_heartbeat_ts(self) -> float:
        """마지막 하트비트 시간 반환"""
        return self._last_heartbeat_ts

    @property
    def is_monitoring(self) -> bool:
        """모니터링 상태 반환"""
        return self._is_monitoring
