"""
연결 레지스트리 관리

StreamOrchestrator에서 태스크/핸들러 레지스트리 관리 책임을 분리합니다.
실행 중인 연결들의 생명주기를 추적하고 관리합니다.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TypeAlias

from src.common.logger import PipelineLogger
from src.core.connection.handlers.base import BaseWebsocketHandler
from src.core.dto.internal.common import ConnectionScopeDomain

# 거래소 핸들러 타입 (임시 - 실제로는 orchestrator.py에서 import)
ExchangeSocketHandler: TypeAlias = BaseWebsocketHandler
ConnectionKey: TypeAlias = tuple[str, str, str, str | None]
logger = PipelineLogger.get_logger("connection_registry", "app")


class ConnectionRegistry:
    """연결 레지스트리 관리자

    실행 중인 WebSocket 연결들의 태스크와 핸들러를 추적하고 관리합니다.
    중복 연결 방지와 정리 작업을 담당합니다.
    """

    def __init__(self) -> None:
        """레지스트리 초기화"""
        self._tasks: dict[ConnectionKey, asyncio.Task[None]] = {}
        self._handlers: dict[ConnectionKey, ExchangeSocketHandler] = {}

    def make_key(self, scope: ConnectionScopeDomain) -> ConnectionKey:
        """연결 스코프로 레지스트리 키 생성

        Args:
            scope: 연결 스코프

        Returns:
            (exchange, region, request_type, symbol?) 소문자 튜플
        """
        symbol = scope.symbol.lower() if isinstance(scope.symbol, str) else None
        return (
            scope.exchange.lower(),
            scope.region.lower(),
            scope.request_type.lower(),
            symbol,
        )

    def _base_key(self, scope: ConnectionScopeDomain) -> tuple[str, str, str]:
        return (
            scope.exchange.lower(),
            scope.region.lower(),
            scope.request_type.lower(),
        )

    def _matching_keys(self, scope: ConnectionScopeDomain) -> list[ConnectionKey]:
        key = self.make_key(scope)
        if key[3] is not None:
            return [key]

        base = self._base_key(scope)
        return [
            candidate
            for candidate in self._tasks
            if candidate[0] == base[0] and candidate[1] == base[1] and candidate[2] == base[2]
        ]

    def is_running(self, scope: ConnectionScopeDomain) -> bool:
        """연결이 실행 중인지 확인

        Args:
            scope: 연결 스코프

        Returns:
            실행 중이면 True
        """
        return any(
            (task := self._tasks.get(key)) is not None and not task.done()
            for key in self._matching_keys(scope)
        )

    def register_connection(
        self,
        scope: ConnectionScopeDomain,
        task: asyncio.Task[None],
        handler: ExchangeSocketHandler,
    ) -> None:
        """연결 등록

        Args:
            scope: 연결 스코프
            task: 실행 태스크
            handler: 핸들러 인스턴스
        """
        key = self.make_key(scope)
        self._tasks[key] = task
        self._handlers[key] = handler
        logger.debug(f"Connection registered: {self._format_scope(scope)}")

    def get_handler(self, scope: ConnectionScopeDomain) -> ExchangeSocketHandler | None:
        """핸들러 조회

        Args:
            scope: 연결 스코프

        Returns:
            핸들러 인스턴스 또는 None
        """
        for key in self._matching_keys(scope):
            handler = self._handlers.get(key)
            if handler is not None:
                return handler
        return None

    def get_task(self, scope: ConnectionScopeDomain) -> asyncio.Task[None] | None:
        """태스크 조회

        Args:
            scope: 연결 스코프

        Returns:
            태스크 인스턴스 또는 None
        """
        for key in self._matching_keys(scope):
            task = self._tasks.get(key)
            if task is not None:
                return task
        return None

    def unregister_connection(self, scope: ConnectionScopeDomain) -> None:
        """연결 등록 해제

        Args:
            scope: 연결 스코프
        """
        key = self.make_key(scope)
        self._tasks.pop(key, None)
        self._handlers.pop(key, None)
        logger.debug(f"Connection unregistered: {self._format_scope(scope)}")

    async def disconnect_connection(
        self,
        scope: ConnectionScopeDomain,
        reason: str | None = None,
        correlation_id: str | None = None,
    ) -> bool:
        """특정 연결 종료

        Args:
            scope: 연결 스코프
            reason: 종료 사유

        Returns:
            종료 성공 여부
        """
        matching_keys = self._matching_keys(scope)
        if not matching_keys:
            logger.info(f"Disconnect ignored (no active connection): {self._format_scope(scope)}")
            return False

        disconnected = False
        for key in matching_keys:
            task = self._tasks.get(key)
            handler = self._handlers.get(key)

            if handler and hasattr(handler, "request_disconnect"):
                await handler.request_disconnect(reason=reason)

            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                disconnected = True

        if disconnected:
            logger.info(
                f"Disconnect completed: {self._format_scope(scope)}"
                + (f" (reason: {reason})" if reason else "")
            )
        else:
            logger.info(f"Disconnect processed: already terminated {self._format_scope(scope)}")

        return True

    async def shutdown_all(self) -> None:
        """모든 연결 종료

        모든 실행 중인 태스크를 취소하고 정리합니다.
        Redis 상태 정리가 완료되도록 보장합니다.
        """
        if not self._tasks:
            logger.info("No active connections to shutdown")
            return

        logger.info(f"Shutting down {len(self._tasks)} active connections...")

        # 모든 태스크 취소 요청
        for task in list(self._tasks.values()):
            if not task.done():
                task.cancel()

        # 모든 태스크 종료 대기
        for key, task in list(self._tasks.items()):
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                self._tasks.pop(key, None)
                self._handlers.pop(key, None)

        logger.info("All connections shutdown completed")

    def get_active_connections(self) -> list[ConnectionKey]:
        """활성 연결 목록 반환

        Returns:
            활성 연결 키 목록
        """
        return [key for key, task in self._tasks.items() if not task.done()]

    def _format_scope(self, scope: ConnectionScopeDomain) -> str:
        """스코프를 읽기 쉬운 문자열로 변환"""
        if scope.symbol:
            return f"{scope.exchange}/{scope.region}/{scope.request_type}/{scope.symbol}"
        return f"{scope.exchange}/{scope.region}/{scope.request_type}"
