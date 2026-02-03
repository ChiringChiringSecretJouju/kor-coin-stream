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
from src.common.logger import PipelineLogger
from src.core.connection.handlers.base import BaseWebsocketHandler
from src.core.connection.utils.subscription import extract_subscription_symbols
from src.core.decorators import catch_exception
from src.core.dto.internal.cache import WebsocketConnectionSpecDomain
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.orchestrator import StreamContextDomain
from src.core.dto.io.commands import ConnectionTargetDTO
from src.core.types import (
    CONNECTION_STATUS_CONNECTED,
    CONNECTION_STATUS_DISCONNECTED,
    SocketParams,
)
from src.core.types._exception_types import (
    RESUBSCRIBE_EXCEPTIONS,
)
from src.infra.cache.cache_store import WebsocketConnectionCache
from src.infra.messaging.connect.producers.error.error_event import (
    ErrorEventProducer,
)

logger = PipelineLogger.get_logger("orchestrator_refactored", "app")

# 타입 정의: BaseWebsocketHandler (구체적인 핸들러 타입)
ExchangeSocketHandler: TypeAlias = BaseWebsocketHandler

# 단일 구독 전용 거래소 (각 심볼마다 별도 WebSocket 연결 필요)
SINGLE_SUBSCRIPTION_ONLY: Final[frozenset[str]] = frozenset(
    {
        "coinone",  # 한국 - 단일 심볼만 구독 지원
        "huobi",  # 아시아 - 단일 심볼만 구독 지원
    }
)


class StreamOrchestrator:
    """리팩토링된 스트림 오케스트레이터 (DI 적용)

    Dependency Injection으로 모든 의존성을 주입받습니다.
    
    책임:
    - 연결 레지스트리: 태스크/핸들러 관리
    - 에러 코디네이터: 에러 처리 통합
    - 기존 컴포넌트들: 각자의 책임 유지
    
    DI Features:
    - 모든 의존성을 생성자에서 주입
    - 테스트 시 Mock 주입 가능
    - Resource provider가 Producer 라이프사이클 자동 관리
    """

    def __init__(
        self,
        error_producer: ErrorEventProducer,
        registry: ConnectionRegistry,
        connector: WebsocketConnector,
    ) -> None:
        """오케스트레이터 초기화 (DI)
        
        Args:
            error_producer: 에러 이벤트 프로듀서
            registry: 연결 레지스트리
            connector: 웹소켓 커넥터
        """
        # ✅ 의존성이 자동으로 주입됨!
        self._error_producer = error_producer
        self._registry = registry
        self._connector = connector
        # self._subs는 제거됨

        # Producer 시작 상태 추적
        # Note: Resource provider가 자동으로 start_producer를 호출하므로
        # 이 플래그는 레거시 호환성을 위해 유지
        self._producer_started = False

    async def startup(self) -> None:
        """오케스트레이터 시작 (DI 모드)
        
        Note:
            DI 모드에서는 Resource provider가 자동으로 start_producer를 호출합니다.
            이 메서드는 하위 호환성을 위해 유지하지만, 실제로는 아무 작업도 하지 않습니다.
            
            레거시 모드 (DI 미사용):
                await orchestrator.startup()  # Producer 수동 시작
            
            DI 모드 (권장):
                await container.init_resources()  # 모든 Resource 자동 시작
        """
        if not self._producer_started:
            # DI 모드에서는 이미 시작되어 있으므로 플래그만 설정
            self._producer_started = True
            logger.info(
                "StreamOrchestrator.startup() called (DI mode: Producer already started)"
            )

    @catch_exception(phase="connect_from_context", level="error")
    async def connect_from_context(self, ctx: StreamContextDomain) -> None:
        """컨텍스트 기반 연결 생성

        리팩토링된 버전: 각 컴포넌트의 책임을 명확히 분리

        단일 구독 거래소(Coinone, Huobi) 처리:
        - 여러 심볼이 있으면 각 심볼마다 별도 연결 생성
        - scope에 symbol 정보 포함하여 Redis 키 충돌 방지
        """
        start_time = datetime.now()
        logger.info(
            f"{ctx.scope.exchange} 연결 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # 0. 단일 구독 거래소 자동 분리 처리
        if ctx.scope.exchange.lower() in SINGLE_SUBSCRIPTION_ONLY:
            if len(ctx.symbols) > 1:
                logger.info(
                    f"{ctx.scope.exchange}는 단일 구독만 지원 → "
                    f"{len(ctx.symbols)}개 심볼을 개별 연결로 분리"
                )
                await self._connect_multiple_symbols(ctx)
                return
            elif len(ctx.symbols) == 1:
                # 단일 심볼이면 scope에 symbol 추가
                ctx = self._add_symbol_to_context(ctx, ctx.symbols[0])
                logger.info(f"{ctx.scope.exchange} 단일 심볼 연결: {ctx.scope.symbol}")

        # 1. 중복 연결 확인 (레지스트리 책임)
        if self._registry.is_running(ctx.scope):
            logger.info(f"중복 연결 감지, 재구독 처리: {ctx.scope.exchange}")
            await self._handle_resubscribe(ctx)
            return

        # 2. 핸들러 생성 (커넥터 책임)
        handler = await self._connector.create_handler_with(ctx.scope, ctx.projection)
        logger.info(
            f"{ctx.scope.exchange} 핸들러 생성: {handler.__class__.__name__}"
        )

        # 3. 캐시 준비 (직접 관리)
        spec = WebsocketConnectionSpecDomain(scope=ctx.scope, symbols=ctx.symbols)
        cache = WebsocketConnectionCache(spec)
        # 상태 업데이트
        await cache.update_connection_state(CONNECTION_STATUS_CONNECTED)
        if ctx.symbols:
            await cache.replace_symbols(list(ctx.symbols))

        # 4. 연결 실행 태스크 생성
        task = asyncio.create_task(
            self._run_connection_task(ctx, handler, cache),
            name=f"ws-{ctx.scope.exchange}-{ctx.scope.region}-"
                 f"{ctx.scope.request_type}-{ctx.correlation_id or ''}",
        )

        # 5. 레지스트리에 등록
        self._registry.register_connection(ctx.scope, task, handler)

        logger.info(f"{ctx.scope.exchange} 연결 태스크 시작됨")

    async def disconnect(
        self,
        target: ConnectionTargetDTO,
        *,
        reason: str | None = None,
        correlation_id: str | None = None,
    ) -> bool:
        """연결 종료 요청

        레지스트리에 위임하여 책임 분리
        """
        scope = ConnectionScopeDomain(
            exchange=target.exchange,
            region=target.region,
            request_type=target.request_type,
        )

        return await self._registry.disconnect_connection(
            scope, reason, correlation_id=correlation_id
        )

    async def shutdown(self) -> None:
        """모든 연결 종료

        레지스트리에 위임하여 책임 분리
        """
        await self._registry.shutdown_all()

        # Producer 정리
        if self._producer_started:
            await self._error_producer.stop_producer()
            self._producer_started = False
            logger.info("ErrorEventProducer stopped")

    @catch_exception(phase="run_connection_task", level="error")
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
            await self._connector.run_connection(
                handler=handler,
                url=ctx.url,
                socket_params=ctx.socket_params,
                correlation_id=ctx.correlation_id,
            )
        finally:
            # 정리 작업
            await cache.update_connection_state(CONNECTION_STATUS_DISCONNECTED)
            await cache.remove_connection()
            self._registry.unregister_connection(ctx.scope)

    @catch_exception(
        exceptions=RESUBSCRIBE_EXCEPTIONS, 
        phase="handle_resubscribe", 
        level="warning"
    )
    async def _handle_resubscribe(self, ctx: StreamContextDomain) -> None:
        """재구독 처리 (내부 메서드)
        
        중복 연결 시 기존 연결에 새로운 심볼을 동적으로 추가합니다.
        """
        handler = self._registry.get_handler(ctx.scope)
        if not isinstance(handler, BaseWebsocketHandler):
            logger.warning(
                f"재구독 실패: 핸들러가 BaseWebsocketHandler 타입이 아님 ({type(handler)})"
            )
            return

        spec = WebsocketConnectionSpecDomain(scope=ctx.scope, symbols=ctx.symbols)
        cache = WebsocketConnectionCache(spec)
        
        # 1. 심볼 추출 (유틸리티 사용)
        symbols: list[str] = []
        subscribe_type: str | None = None
        
        if isinstance(ctx.socket_params, dict):
            symbols = extract_subscription_symbols(ctx.socket_params)
            st = ctx.socket_params.get("subscribe_type")
            if isinstance(st, str):
                subscribe_type = st
        elif isinstance(ctx.socket_params, list):
            # 리스트 형태 파라미터에서도 추출 시도
            symbols = extract_subscription_symbols(ctx.socket_params)

        # 2. 캐시 갱신 (누적)
        if symbols:
            # RedisCache.replace_symbols는 덮어쓰기이므로 주의 필요
            # 하지만 현재 캐시 로직상 활성 심볼 목록을 최신화하는 것이므로 replace가 적절할 수 있음
            await cache.replace_symbols(symbols)

        # 3. 핸들러에 동적 구독 요청 (Zero-Downtime)
        if symbols:
            await handler.update_subscription(symbols, subscribe_type)
            logger.info(f"재구독(동적 추가) 요청 완료: {ctx.scope.exchange} -> {symbols}")

    @catch_exception(phase="connect_multiple_symbols", level="error")
    async def _connect_multiple_symbols(self, ctx: StreamContextDomain) -> None:
        """단일 구독 거래소용: 여러 심볼을 개별 연결로 분리 (내부 메서드)

        Args:
            ctx: 원본 컨텍스트 (여러 심볼 포함)

        Note:
            단일 구독 거래소는 재구독이 불가능하므로, 중복 연결 시 스킵합니다.
        """
        for symbol in ctx.symbols:
            # 각 심볼마다 별도 컨텍스트 생성
            single_ctx = self._add_symbol_to_context(ctx, symbol)

            logger.info(f"  → {ctx.scope.exchange} 개별 연결 생성: {symbol}")

            # 재귀 호출하지 않고 직접 연결 로직 수행
            # 중복 연결 확인 (단일 구독 거래소는 재구독 불가)
            if self._registry.is_running(single_ctx.scope):
                logger.info(
                    f"  → {symbol}: 이미 연결 중 - 스킵 "
                    f"(단일 구독 거래소는 재구독 불가)"
                )
                continue

            # 핸들러 생성
            handler = await self._connector.create_handler_with(
                single_ctx.scope, single_ctx.projection
            )

            # 캐시 준비 (직접 관리)
            single_spec = WebsocketConnectionSpecDomain(
                scope=single_ctx.scope, symbols=single_ctx.symbols
            )
            cache = WebsocketConnectionCache(single_spec)
            await cache.update_connection_state(CONNECTION_STATUS_CONNECTED)
            if single_ctx.symbols:
                await cache.replace_symbols(list(single_ctx.symbols))

            # 연결 태스크 생성
            task = asyncio.create_task(
                self._run_connection_task(single_ctx, handler, cache),
                name=f"ws-{single_ctx.scope.exchange}-{single_ctx.scope.region}-"
                f"{single_ctx.scope.request_type}-{symbol}",
            )

            # 레지스트리 등록
            self._registry.register_connection(single_ctx.scope, task, handler)

            logger.info(f"  → {symbol}: 연결 태스크 시작됨")

    def _add_symbol_to_context(
        self, ctx: StreamContextDomain, symbol: str
    ) -> StreamContextDomain:
        """컨텍스트에 symbol 정보 추가 (내부 메서드)

        Args:
            ctx: 원본 컨텍스트
            symbol: 추가할 심볼

        Returns:
            symbol이 포함된 새로운 컨텍스트
        """
        new_scope = ConnectionScopeDomain(
            exchange=ctx.scope.exchange,
            region=ctx.scope.region,
            request_type=ctx.scope.request_type,
            symbol=symbol,  # 심볼 추가
        )

        return StreamContextDomain(
            scope=new_scope,
            url=ctx.url,
            socket_params=ctx.socket_params,
            symbols=(symbol,),  # 단일 심볼만
            projection=ctx.projection,
        )


# 기존 컴포넌트들 (이미 잘 분리되어 있음 - 그대로 유지)
class WebsocketConnector:
    """웹소켓 연결 및 핸들러 생성 책임 (DI 지원)
    
    Features:
    - FactoryAggregate 지원: 동적 핸들러 생성
    - 하위 호환성: dict 기반 handler_map도 지원
    - DI Container로부터 handler_factory 주입 가능
    """

    def __init__(
        self,
        handler_map: dict[str, type[ExchangeSocketHandler]] | None = None,
    ) -> None:
        """
        Args:
            handler_map: 핸들러 매핑 (dict 또는 FactoryAggregate)
                - dict: 기존 방식 (하위 호환성)
                - FactoryAggregate: DI 컨테이너에서 주입 (권장)
        """
        # handler_factory는 FactoryAggregate (DI Container에서 주입)
        # None인 경우 에러를 발생시켜야 함 (DI 없이는 동작 불가)
        if handler_map is None:
            raise ValueError(
                "handler_map is required. Use DI Container to inject FactoryAggregate."
            )
        self.handler_factory = handler_map

    def get_handler_class(self, exchange: str) -> type[ExchangeSocketHandler] | None:
        """핸들러 클래스 조회 (레거시 호환성)
        
        Note: DI 모드에서는 사용되지 않음 (FactoryAggregate는 클래스를 반환하지 않음)
        """
        return None  # DI 모드에서는 항상 None

    async def create_handler_with(
        self, scope: ConnectionScopeDomain, projection: list[str] | None = None
    ) -> ExchangeSocketHandler:
        """핸들러 생성 (FactoryAggregate 또는 dict 지원)
        
        Args:
            scope: 연결 스코프
            projection: 프로젝션 필드
            
        Returns:
            생성된 핸들러 인스턴스
            
        Raises:
            ValueError: 지원하지 않는 거래소
        """
        exchange_key = scope.exchange.lower()
        
        # FactoryAggregate인 경우: 함수처럼 호출
        # dict인 경우: 클래스를 가져와서 직접 인스턴스화
        try:
            if isinstance(self.handler_factory, dict):
                # 기존 방식 (dict)
                handler_class = self.handler_factory.get(exchange_key)
                if not handler_class:
                    raise ValueError(f"Unsupported exchange: {scope.exchange}")
                handler = handler_class(
                    exchange_name=scope.exchange,
                    region=scope.region,
                    request_type=scope.request_type,
                )
            else:
                # FactoryAggregate 방식 (DI)
                # FactoryAggregate는 callable이므로 직접 호출
                handler = self.handler_factory(
                    exchange_key,
                    request_type=scope.request_type,
                )
                
                # Check if handler is a coroutine/future (DI issue safeguard)
                if asyncio.iscoroutine(handler) or isinstance(handler, asyncio.Future):
                    handler = await handler
                    
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(
                f"Failed to create handler for {scope.exchange}: {e}"
            ) from e
        
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
        correlation_id: str | None = None,
    ) -> None:
        await handler.websocket_connection(
            url=url, parameter_info=socket_params, correlation_id=correlation_id
        )






