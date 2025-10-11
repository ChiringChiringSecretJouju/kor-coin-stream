from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.dto.internal.common import ConnectionPolicyDomain, ConnectionScopeDomain
from src.core.dto.io.target import ConnectionTargetDTO

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
        self._last_receive_ts: float = 0.0
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
        """헬스 모니터 단계 오류를 통합 에러 디스패처로 발행한다."""
        target = ConnectionTargetDTO(
            exchange=self.scope.exchange,
            region=self.scope.region,
            request_type=self.scope.request_type,
        )
        context: dict = {
            "phase": phase,
            "heartbeat_kind": self.policy.heartbeat_kind,
            "heartbeat_timeout": self.policy.heartbeat_timeout,
            "heartbeat_fail_count": self._heartbeat_fail_count,
            **(extra or {}),
        }
        await dispatch_error(
            exc=err if isinstance(err, Exception) else Exception(str(err)),
            kind="health_monitor",
            target=target,
            context=context,
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
        self._last_receive_ts = self._last_heartbeat_ts
        self._heartbeat_fail_count = 0
        self._is_monitoring = True

        # 하트비트 태스크 시작
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(websocket, ping_interval)
        )

        # 워치독 태스크 시작 (수신 유휴 시간 감시) - 타임아웃이 0 이하이면 비활성화
        if float(self.policy.receive_idle_timeout) > 0:
            self._watchdog_task = asyncio.create_task(self._watchdog_loop(websocket))

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

    def notify_receive(self) -> None:
        """메시지 수신 시점에 호출되어 마지막 수신 시각을 갱신합니다."""
        self._last_receive_ts = time.monotonic()

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

    async def _watchdog_loop(self, websocket: Any) -> None:
        """수신 유휴 감시 루프: 일정 시간 수신이 없으면 연결을 종료합니다."""
        # 체크 주기는 타임아웃의 1/3 또는 최대 10초, 최소 1초
        timeout = float(self.policy.receive_idle_timeout)
        check_interval = max(1.0, min(10.0, timeout / 3.0 if timeout > 0 else 5.0))

        while self._is_monitoring:
            try:
                await asyncio.sleep(check_interval)
                now = time.monotonic()
                idle_for = now - self._last_receive_ts
                if timeout > 0 and idle_for >= timeout:
                    msg = (
                        f"{self.scope.exchange}: receive idle timeout exceeded - "
                        f"idle={idle_for:.1f}s >= {timeout:.1f}s"
                    )
                    logger.warning(msg)
                    await self._emit_error(
                        TimeoutError("receive idle timeout exceeded"),
                        phase="watchdog_idle",
                        extra={
                            "idle_seconds": round(idle_for, 1),
                            "receive_idle_timeout": timeout,
                        },
                    )
                    # 연결 종료를 시도하여 상위 루프가 재연결 로직을 타게 한다
                    try:
                        await websocket.close()
                    except Exception:
                        pass
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{self.scope.exchange}: watchdog loop error - {e}")
                await self._emit_error(e, phase="watchdog_loop")
                break
