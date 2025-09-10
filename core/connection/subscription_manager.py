from __future__ import annotations

import asyncio
import json
from typing import Any


from common.logger import PipelineLogger
from core.dto.internal.common import ConnectionScopeDomain
from core.dto.internal.subscription import (
    SymbolMergeResultDomain,
    SubscriptionStateDomain,
)
from core.types import SocketParams


logger = PipelineLogger.get_logger("subscription_manager", "connection")


class SubscriptionManager:
    """구독 관리 전담 클래스

    책임:
    - 웹소켓 구독 메시지 생성
    - 심볼 병합 및 재구독 로직
    - 구독 상태 추적 (SubscriptionStateDomain 사용)
    """

    def __init__(self, scope: ConnectionScopeDomain) -> None:
        self.scope = scope
        self._send_lock = asyncio.Lock()
        # 구독 상태 추적 (ConnectRequestDomain과 의미적으로 분리)
        self._state = SubscriptionStateDomain()

    async def prepare_subscription_message(self, params: SocketParams) -> str:
        """구독 메시지 JSON 직렬화"""
        return json.dumps(params)

    def merge_symbols(
        self,
        current_params: SocketParams | None,
        new_symbols: list[str],
        subscribe_type: str | None = None,
    ) -> SymbolMergeResultDomain:
        """심볼 병합 로직 - 명확한 반환 타입으로 가독성 향상"""
        if not isinstance(current_params, dict):
            logger.info(
                f"{self.scope.exchange}: 리스트 파라미터는 재구독 미지원 - 건너뜀"
            )
            return SymbolMergeResultDomain(
                merged_params=None,
                has_new_symbols=False,
                total_symbols=0,
            )

        new_params = dict(current_params)
        existing_syms = []
        try:
            existing_syms = (
                list(new_params.get("symbols", []))
                if isinstance(new_params.get("symbols"), list)
                else []
            )
        except Exception:
            existing_syms = []

        # 집합 연산으로 신규만 판단
        before_set = set(s for s in existing_syms if isinstance(s, str))
        add_set = set(s for s in new_symbols if isinstance(s, str))
        merged = list(before_set | add_set)

        # 신규가 없으면 전송 생략
        if before_set >= add_set:
            logger.info(
                f"{self.scope.exchange}: 추가할 신규 심볼 없음 - 재구독 생략 ({len(before_set)} 유지)"
            )
            return SymbolMergeResultDomain(
                merged_params=None,
                has_new_symbols=False,
                total_symbols=len(before_set),
            )

        new_params["symbols"] = merged
        if subscribe_type:
            new_params["subscribe_type"] = subscribe_type

        return SymbolMergeResultDomain(
            merged_params=new_params,
            has_new_symbols=True,
            total_symbols=len(merged),
        )

    async def update_subscription(
        self, websocket: Any, symbols: list[str], subscribe_type: str | None = None
    ) -> bool:
        """실행 중 구독 심볼을 갱신합니다.

        Args:
            websocket: 현재 웹소켓 연결
            symbols: 새로 적용할 심볼 목록
            subscribe_type: 구독 타입(미지정 시 기존 파라미터 유지)

        Returns:
            bool: 성공 여부
        """
        if not symbols:
            return True

        async with self._send_lock:
            if websocket is None or self._state.current_params is None:
                logger.info(
                    f"{self.scope.exchange}: 활성 웹소켓이 없어 재구독을 건너뜁니다."
                )
                return False

            if getattr(websocket, "closed", False):
                logger.info(
                    f"{self.scope.exchange}: 웹소켓이 이미 종료되어 재구독 불가"
                )
                return False

            # 파라미터 갱신
            merge_result = self.merge_symbols(
                self._state.current_params,
                symbols,
                subscribe_type,
            )

            if not merge_result.has_new_symbols or merge_result.merged_params is None:
                return True

            # 전송 및 내부 상태 업데이트
            try:
                subscription_message = await self.prepare_subscription_message(
                    merge_result.merged_params
                )
                await websocket.send(subscription_message)

                # 상태 업데이트 (SubscriptionStateDomain으로 상태 추적)
                self._state = SubscriptionStateDomain(
                    current_params=merge_result.merged_params,
                    symbols=merge_result.merged_params.get("symbols", []),  # type: ignore[arg-type]
                    subscribe_type=subscribe_type,
                )

                logger.info(
                    f"{self.scope.exchange}: 재구독 메시지 전송 완료 -> now {merge_result.total_symbols} symbols"
                )
                return True

            except Exception as e:
                logger.warning(f"{self.scope.exchange}: 재구독 전송 실패: {e}")
                return False

    async def update_subscription_raw(
        self, websocket: Any, params: SocketParams
    ) -> bool:
        """원본 파라미터를 그대로 재전송하여 재구독합니다.

        Args:
            websocket: 현재 웹소켓 연결
            params: 거래소별 subscribe payload

        Returns:
            bool: 성공 여부
        """
        async with self._send_lock:
            if websocket is None:
                logger.info(
                    f"{self.scope.exchange}: 활성 웹소켓이 없어 재구독(raw) 건너뜁니다."
                )
                return False

            if getattr(websocket, "closed", False):
                logger.info(
                    f"{self.scope.exchange}: 웹소켓 종료 상태 - 재구독(raw) 불가"
                )
                return False

            try:
                subscription_message = await self.prepare_subscription_message(params)
                await websocket.send(subscription_message)

                # 상태 업데이트
                symbols = []
                if isinstance(params, dict):
                    symbols = params.get("symbols", [])

                self._state = SubscriptionStateDomain(
                    current_params=params,
                    symbols=symbols if isinstance(symbols, list) else [],
                    subscribe_type=None,
                )

                logger.info(f"{self.scope.exchange}: 재구독(raw) 메시지 전송 완료")
                return True

            except Exception as e:
                logger.warning(f"{self.scope.exchange}: 재구독(raw) 전송 실패: {e}")
                return False

    def update_current_params(self, params: SocketParams) -> None:
        """현재 구독 파라미터 업데이트 (연결 시 호출)"""
        symbols = []
        if isinstance(params, dict):
            symbols = params.get("symbols", [])

        self._state = SubscriptionStateDomain(
            current_params=params,
            symbols=symbols if isinstance(symbols, list) else [],
            subscribe_type=None,
        )

    @property
    def current_params(self) -> SocketParams | None:
        """현재 구독 파라미터 조회"""
        return self._state.current_params

    @property
    def current_symbols(self) -> list[str] | None:
        """현재 구독 심볼 목록 조회"""
        return self._state.symbols

    @property
    def subscribe_type(self) -> str | None:
        """현재 구독 타입 조회"""
        return self._state.subscribe_type
