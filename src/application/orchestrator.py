"""
리팩토링된 StreamOrchestrator

책임을 명확히 분리하여 단일 책임 원칙을 준수합니다.
ConnectionRegistry와 ErrorCoordinator를 활용하여 복잡도를 대폭 감소시킵니다.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Final, TypeAlias

# 새로운 컴포넌트들
from src.application.connection_registry import ConnectionRegistry
from src.application.error_coordinator import ErrorCoordinator
from src.common.logger import PipelineLogger
from src.core.dto.internal.cache import WebsocketConnectionSpecDomain
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.orchestrator import StreamContextDomain
from src.core.dto.io.target import ConnectionTargetDTO
from src.core.types import (
    CONNECTION_STATUS_CONNECTED,
    CONNECTION_STATUS_DISCONNECTED,
    SocketParams,
)
from src.exchange.asia import (
    BinanceWebsocketHandler,
    BybitWebsocketHandler,
    HuobiWebsocketHandler,
    OKXWebsocketHandler,
)
from src.exchange.europe import BitfinexWebsocketHandler

# 기존 컴포넌트들 (이미 잘 분리됨)
from src.exchange.korea import (
    BithumbWebsocketHandler,
    CoinoneWebsocketHandler,
    KorbitWebsocketHandler,
    UpbitWebsocketHandler,
)
from src.exchange.na import CoinbaseWebsocketHandler, KrakenWebsocketHandler
from src.infra.cache.cache_store import WebsocketConnectionCache

logger = PipelineLogger.get_logger("orchestrator_refactored", "app")

# 타입 정의
ExchangeSocketHandler: TypeAlias = (
    UpbitWebsocketHandler
    | BithumbWebsocketHandler
    | CoinoneWebsocketHandler
    | KorbitWebsocketHandler
    | BinanceWebsocketHandler
    | BybitWebsocketHandler
    | OKXWebsocketHandler
    | HuobiWebsocketHandler
    | BitfinexWebsocketHandler
    | CoinbaseWebsocketHandler
    | KrakenWebsocketHandler
)

# 거래소별 웹소켓 핸들러 매핑
HANDLER_MAP: Final[dict[str, type[ExchangeSocketHandler]]] = {
    # 한국 거래소
    "upbit": UpbitWebsocketHandler,
    "bithumb": BithumbWebsocketHandler,
    "coinone": CoinoneWebsocketHandler,
    "korbit": KorbitWebsocketHandler,
    # 아시아 거래소
    "binance": BinanceWebsocketHandler,
    "bybit": BybitWebsocketHandler,
    "okx": OKXWebsocketHandler,
    "huobi": HuobiWebsocketHandler,
    # 유럽 거래소
    "bitfinex": BitfinexWebsocketHandler,
    # 북미 거래소
    "coinbase": CoinbaseWebsocketHandler,
    "kraken": KrakenWebsocketHandler,
}


class StreamOrchestrator:
    """리팩토링된 스트림 오케스트레이터

    책임을 명확히 분리하여 단일 책임 원칙을 준수합니다.
    - 연결 레지스트리: 태스크/핸들러 관리
    - 에러 코디네이터: 에러 처리 통합
    - 기존 컴포넌트들: 각자의 책임 유지
    """

    def __init__(self) -> None:
        """오케스트레이터 초기화

        각 컴포넌트의 책임을 명확히 분리합니다.
        """
        # 핵심 컴포넌트들 (책임 분리)
        self._registry = ConnectionRegistry()
        self._error_coordinator = ErrorCoordinator()

        # 기존 컴포넌트들 (이미 잘 분리됨)
        self._connector = WebsocketConnector(HANDLER_MAP)
        self._cache = RedisCacheCoordinator()
        self._subs = SubscriptionManager()

    async def connect_from_context(self, ctx: StreamContextDomain) -> None:
        """컨텍스트 기반 연결 생성

        리팩토링된 버전: 각 컴포넌트의 책임을 명확히 분리
        """
        start_time = datetime.now()
        logger.info(
            f"{ctx.scope.exchange} 연결 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # 1. 중복 연결 확인 (레지스트리 책임)
        if self._registry.is_running(ctx.scope):
            logger.info(f"중복 연결 감지, 재구독 처리: {ctx.scope.exchange}")
            await self._handle_resubscribe(ctx)
            return

        # 2. 핸들러 생성 (커넥터 책임)
        try:
            handler = self._connector.create_handler_with(ctx.scope, ctx.projection)
            logger.info(
                f"{ctx.scope.exchange} 핸들러 생성: {handler.__class__.__name__}"
            )
        except Exception as e:
            await self._error_coordinator.emit_connection_error(
                scope=ctx.scope, error=e, phase="handler_creation"
            )
            return

        # 3. 캐시 준비 (캐시 코디네이터 책임)
        cache = self._cache.make_cache_with(ctx)
        await self._cache.on_start_with(ctx, cache)

        # 4. 연결 실행 태스크 생성
        task = asyncio.create_task(
            self._run_connection_task(ctx, handler, cache),
            name=f"ws-{ctx.scope.exchange}-{ctx.scope.region}-{ctx.scope.request_type}",
        )

        # 5. 레지스트리에 등록
        self._registry.register_connection(ctx.scope, task, handler)

        logger.info(f"{ctx.scope.exchange} 연결 태스크 시작됨")

    async def disconnect(
        self, target: ConnectionTargetDTO, *, reason: str | None = None
    ) -> bool:
        """연결 종료 요청

        레지스트리에 위임하여 책임 분리
        """
        scope = ConnectionScopeDomain(
            exchange=target.exchange,
            region=target.region,
            request_type=target.request_type,
        )

        return await self._registry.disconnect_connection(scope, reason)

    async def shutdown(self) -> None:
        """모든 연결 종료

        레지스트리에 위임하여 책임 분리
        """
        await self._registry.shutdown_all()

    async def _run_connection_task(
        self,
        ctx: StreamContextDomain,
        handler: ExchangeSocketHandler,
        cache: WebsocketConnectionCache,
    ) -> None:
        """연결 실행 태스크 (내부 메서드)

        실제 WebSocket 연결을 실행하고 정리를 담당합니다.
        """
        try:
            # 연결 실행
            await self._connector.run_with(ctx, handler)
        except Exception as e:
            # 실행 중 에러
            await self._error_coordinator.emit_connection_error(
                scope=ctx.scope, error=e, phase="run_with"
            )
        finally:
            # 정리 작업
            await self._cache.on_stop(cache)
            self._registry.unregister_connection(ctx.scope)

    async def _handle_resubscribe(self, ctx: StreamContextDomain) -> None:
        """재구독 처리 (내부 메서드)

        중복 연결 시 재구독을 처리합니다.
        """
        try:
            handler = self._registry.get_handler(ctx.scope)
            cache = self._cache.make_cache_with(ctx)

            success = await self._subs.handle_resubscribe(
                running=handler,
                cache=cache,
                ctx=ctx,
            )

            if success:
                logger.info(f"재구독 성공: {ctx.scope.exchange}")
            else:
                logger.warning(f"재구독 실패: {ctx.scope.exchange}")

        except Exception as e:
            await self._error_coordinator.emit_resubscribe_error(
                scope=ctx.scope,
                error=e,
                socket_params=ctx.socket_params,
            )


# 기존 컴포넌트들 (이미 잘 분리되어 있음 - 그대로 유지)
class WebsocketConnector:
    """웹소켓 연결 및 핸들러 생성 책임"""

    def __init__(self, handler_map: dict[str, type[ExchangeSocketHandler]]) -> None:
        self.handler_map = handler_map or HANDLER_MAP

    def get_handler_class(self, exchange: str) -> type[ExchangeSocketHandler] | None:
        return self.handler_map.get(exchange.lower())

    def create_handler_with(
        self, scope: ConnectionScopeDomain, projection: list[str] | None = None
    ) -> ExchangeSocketHandler:
        handler_class = self.get_handler_class(scope.exchange)
        if not handler_class:
            raise ValueError(f"Unsupported exchange: {scope.exchange}")
        handler = handler_class(
            exchange_name=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
        )
        # projection 필드 설정
        if hasattr(handler, "projection"):
            handler.projection = projection
            logger.info(f"{scope.exchange}: Projection set to {projection}")
        return handler

    async def run_with(
        self, ctx: StreamContextDomain, handler: ExchangeSocketHandler
    ) -> None:
        await self.run_connection(
            handler=handler,
            url=ctx.url,
            socket_params=ctx.socket_params,
        )

    async def run_connection(
        self,
        handler: ExchangeSocketHandler,
        url: str,
        socket_params: SocketParams,
    ) -> None:
        await handler.websocket_connection(url=url, parameter_info=socket_params)


class RedisCacheCoordinator:
    """Redis 캐시 관련 책임"""

    def make_cache_with(self, ctx: StreamContextDomain) -> WebsocketConnectionCache:
        spec = WebsocketConnectionSpecDomain(scope=ctx.scope, symbols=ctx.symbols)
        return WebsocketConnectionCache(spec)

    async def on_start(
        self, cache: WebsocketConnectionCache, symbols: list[str]
    ) -> None:
        await cache.update_connection_state(CONNECTION_STATUS_CONNECTED)
        if symbols:
            await cache.replace_symbols(symbols)

    async def on_start_with(
        self, ctx: StreamContextDomain, cache: WebsocketConnectionCache
    ) -> None:
        await self.on_start(cache, list(ctx.symbols))

    async def on_stop(self, cache: WebsocketConnectionCache) -> None:
        await cache.update_connection_state(CONNECTION_STATUS_DISCONNECTED)
        await cache.remove_connection()


class SubscriptionManager:
    """구독/재구독 및 심볼 추출 책임"""

    def extract_symbols(self, socket_params: SocketParams) -> list[str]:
        symbols_field = None
        if isinstance(socket_params, dict):
            symbols_field = socket_params.get("symbols")
        elif isinstance(socket_params, list):
            return []

        if isinstance(symbols_field, list):
            return [str(s) for s in symbols_field if s]
        return []

    async def handle_resubscribe(
        self,
        running: ExchangeSocketHandler | None,
        cache: WebsocketConnectionCache,
        ctx: StreamContextDomain,
    ) -> bool:
        subscribe_type: str | None = None
        symbols: list[str] = []

        if isinstance(ctx.socket_params, dict):
            symbols = self.extract_symbols(ctx.socket_params)
            st = ctx.socket_params.get("subscribe_type")
            if isinstance(st, str):
                subscribe_type = st

        if symbols:
            await cache.replace_symbols(symbols)

        if running is not None and symbols:
            await running.update_subscription(symbols, subscribe_type)  # type: ignore[attr-defined]
            logger.info(f"재구독 완료: {ctx.scope.exchange} -> {symbols}")

        return True
