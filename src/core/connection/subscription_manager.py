from __future__ import annotations

import asyncio
from typing import Any

import orjson

from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.subscription import (
    SubscriptionStateDomain,
    SymbolMergeResultDomain,
)
from src.core.dto.io.target import ConnectionTargetDTO
from src.core.types import SocketParams

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
        # 구독 상태 추적 (현재 구독 파라미터/심볼 목록/subscribe_type 관리)
        self._state = SubscriptionStateDomain()

    def _observed_key(self) -> str:
        return f"{self.scope.exchange}/{self.scope.region}/{self.scope.request_type}"

    async def _emit_error(
        self, err: BaseException, *, phase: str, extra: dict | None = None
    ) -> None:
        """구독 단계 오류를 통합 에러 디스패처로 발행한다."""
        target = ConnectionTargetDTO(
            exchange=self.scope.exchange,
            region=self.scope.region,
            request_type=self.scope.request_type,
        )
        context = {"phase": phase, **(extra or {})}
        await dispatch_error(
            exc=err if isinstance(err, Exception) else Exception(str(err)),
            kind="subscription",
            target=target,
            context=context,
        )

    async def prepare_subscription_message(self, params: SocketParams) -> str:
        """구독 메시지 JSON 직렬화 (orjson 사용)"""
        return orjson.dumps(params).decode("utf-8")

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
                f"""
                {self.scope.exchange}: 추가할 신규 심볼 없음 - 재구독 생략 ({len(before_set)} 유지)
                """
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
                await self._emit_error(
                    RuntimeError("no active websocket or missing state params"),
                    phase="update_subscription",
                    extra={"reason": "websocket_none_or_state_empty"},
                )
                return False

            if getattr(websocket, "closed", False):
                logger.info(
                    f"{self.scope.exchange}: 웹소켓이 이미 종료되어 재구독 불가"
                )
                await self._emit_error(
                    RuntimeError("websocket already closed"),
                    phase="update_subscription",
                    extra={"reason": "websocket_closed"},
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
                    f"""
                    {self.scope.exchange}: 재구독 메시지 전송 완료 -> 
                    now {merge_result.total_symbols} symbols
                    """
                )
                return True

            except Exception as e:
                logger.warning(f"{self.scope.exchange}: 재구독 전송 실패: {e}")
                await self._emit_error(
                    e,
                    phase="update_subscription_send",
                    extra={
                        "symbols": symbols,
                        "subscribe_type": subscribe_type,
                        "merged_total": merge_result.total_symbols,
                    },
                )
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
                    f"{self.scope.exchange}: 활성 웹소켓이 없어 재구독을 건너뜁니다."
                )
                await self._emit_error(
                    RuntimeError("no active websocket for raw update"),
                    phase="update_subscription_raw",
                    extra={"reason": "websocket_none"},
                )
                return False

            if getattr(websocket, "closed", False):
                logger.info(
                    f"{self.scope.exchange}: 웹소켓이 이미 종료되어 재구독 불가"
                )
                await self._emit_error(
                    RuntimeError("websocket already closed"),
                    phase="update_subscription_raw",
                    extra={"reason": "websocket_closed"},
                )
                return False

            try:
                subscription_message = await self.prepare_subscription_message(params)
                await websocket.send(subscription_message)

                # 상태 업데이트
                symbols = self._extract_symbols_from_params(params)

                self._state = SubscriptionStateDomain(
                    current_params=params,
                    symbols=symbols,
                    subscribe_type=None,
                )

                logger.info(f"{self.scope.exchange}: 재구독(raw) 메시지 전송 완료")
                return True
            except Exception as e:
                logger.warning(f"{self.scope.exchange}: 재구독 전송 실패(raw): {e}")
                await self._emit_error(
                    e,
                    phase="update_subscription_raw_send",
                    extra={"params_type": type(params).__name__},
                )
                return False

    def update_current_params(self, params: SocketParams) -> None:
        """현재 구독 파라미터 업데이트 (연결 시 호출)"""
        symbols = self._extract_symbols_from_params(params)

        self._state = SubscriptionStateDomain(
            current_params=params,
            symbols=symbols,
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

    def _extract_symbols_from_params(self, params: SocketParams) -> list[str]:
        """socket params에서 심볼 리스트를 추출한다.

        지원 형식:
        - dict: { "symbols"|"codes"|"pair"|"pairs"|"symbol"|"product_ids": [..] }
          + 중첩: { "params": { 동일 키들... } }
        - list[dict]: 각 항목의 동일 키들을 모아 평탄화
        - 그 외: 빈 리스트
        """

        def collect_from_dict(d: dict[str, Any], bag: list[str]) -> None:
            # 우선순위 없이 모든 후보 키를 수집
            for key in ("symbols", "codes", "pair", "pairs", "symbol", "product_ids"):
                v = d.get(key)
                if isinstance(v, list):
                    bag.extend([x for x in v if isinstance(x, str)])
            # Kraken v2 등 중첩 params 처리
            p = d.get("params")
            if isinstance(p, dict):
                for key in (
                    "symbols",
                    "codes",
                    "pair",
                    "pairs",
                    "symbol",
                    "product_ids",
                ):
                    v = p.get(key)
                    if isinstance(v, list):
                        bag.extend([x for x in v if isinstance(x, str)])

        collected: list[str] = []
        if isinstance(params, dict):
            collect_from_dict(params, collected)
        elif isinstance(params, list):
            for item in params:
                if isinstance(item, dict):
                    collect_from_dict(item, collected)
        # 중복 제거 및 원래 순서 유지
        seen: set[str] = set()
        result: list[str] = []
        for s in collected:
            if s not in seen:
                seen.add(s)
                result.append(s)
        return result

    def effective_symbols(self) -> list[str]:
        """현재 상태에서 사용할 수 있는 심볼 리스트를 반환합니다.

        우선순위:
        - 상태에 저장된 symbols가 있으면 그대로 반환
        - 없으면 current_params에서 추출하여 상태를 갱신 후 반환
        - 그래도 없으면 빈 리스트 반환
        """
        syms = self._state.symbols
        if isinstance(syms, list) and len(syms) > 0:
            return syms

        params = self._state.current_params
        if params is None:
            return []

        extracted = self._extract_symbols_from_params(params)
        # 상태 갱신하여 이후 참조에서도 사용 가능하도록 함
        self._state = SubscriptionStateDomain(
            current_params=params,
            symbols=extracted,
            subscribe_type=self._state.subscribe_type,
        )
        return extracted
