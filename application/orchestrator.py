from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Final, TypeAlias

from common.logger import PipelineLogger
from core.dto.internal.cache import WebsocketConnectionSpecDomain
from core.dto.internal.common import ConnectionScopeDomain
from core.dto.internal.orchestrator import StreamContextDomain
from core.types import SocketParams, CONNECTION_STATUS_DISCONNECTED
from core.dto.io.target import ConnectionTargetDTO
from core.dto.adapter.error_adapter import make_ws_error_event_from_kind
from exchange.korea import (
    BithumbWebsocketHandler,
    CoinoneWebsocketHandler,
    KorbitWebsocketHandler,
    UpbitWebsocketHandler,
)
from infra.cache.cache_store import WebsocketConnectionCache


logger = PipelineLogger.get_logger("orchestrator", "app")

ExchangeSocketHandler: TypeAlias = (
    UpbitWebsocketHandler
    | BithumbWebsocketHandler
    | CoinoneWebsocketHandler
    | KorbitWebsocketHandler
)

# 거래소별 웹소켓 핸들러 매핑
HANDLER_MAP: Final[dict[str, type[ExchangeSocketHandler]]] = {
    "upbit": UpbitWebsocketHandler,
    "bithumb": BithumbWebsocketHandler,
    "coinone": CoinoneWebsocketHandler,
    "korbit": KorbitWebsocketHandler,
}


def _log_string(ctx: ConnectionScopeDomain) -> str:
    """연결 스코프를 사람이 읽기 쉬운 문자열로 변환합니다.

    Args:
        ctx (ConnectionScopeDomain): 거래소/지역/요청유형 스코프

    Returns:
        str: "exchange/region/request_type" 포맷의 문자열
    """
    return f"{ctx.exchange}/{ctx.region}/{ctx.request_type}"


async def _emit_scope_error_event(
    scope: ConnectionScopeDomain,
    err: BaseException,
    *,
    kind: str,
    observed_key: str,
    raw_context: dict | None = None,
) -> bool:
    """주어진 scope로 표준 에러 이벤트를 발행하는 헬퍼.

    Args:
        scope: 연결 스코프
        err: 예외 객체
        kind: 분류 kind (예: "ws", "orchestrator")
        observed_key: 관측 키 (exchange/region/request_type)
        raw_context: 추가 메타 컨텍스트

    Returns:
        bool: 전송 성공 여부
    """
    target = ConnectionTargetDTO(
        exchange=scope.exchange,
        region=scope.region,
        request_type=scope.request_type,
    )
    return await make_ws_error_event_from_kind(
        target=target,
        err=err,
        kind=kind,
        observed_key=observed_key,
        raw_context=raw_context or {},
    )


class WebsocketConnector:
    """웹소켓 연결 및 핸들러 생성 책임.

    - 거래소별 핸들러 클래스를 결정하고 인스턴스를 생성
    - 웹소켓 연결 실행을 캡슐화
    """

    def __init__(self, handler_map: HANDLER_MAP) -> None:
        """핸들러 매핑으로 커넥터를 초기화합니다.

        Args:
            handler_map (HANDLER_MAP): 거래소명 → 핸들러 클래스 매핑
        """
        self.handler_map = handler_map or HANDLER_MAP

    def get_handler_class(self, exchange: str) -> type[ExchangeSocketHandler] | None:
        """거래소명으로 핸들러 클래스를 조회합니다.

        Args:
            exchange (str): 거래소명(대소문자 무시)

        Returns:
            type[ExchangeSocketHandler] | None: 매핑된 핸들러 클래스 또는 None
        """
        return self.handler_map.get(exchange.lower())

    def create_handler(self, ctx: ConnectionScopeDomain) -> ExchangeSocketHandler:
        """컨텍스트를 바탕으로 거래소 핸들러 인스턴스를 생성합니다.

        Args:
            ctx (ConnectionScopeDomain): 연결 스코프(거래소/지역/요청유형)

        Raises:
            ValueError: 지원하지 않는 거래소일 경우

        Returns:
            ExchangeSocketHandler: 생성된 거래소 핸들러 인스턴스
        """
        handler_class = self.get_handler_class(ctx.exchange)
        if not handler_class:
            raise ValueError(f"지원하지 않는 거래소입니다: {ctx}")
        return handler_class(
            exchange_name=ctx.exchange,
            region=ctx.region,
            request_type=ctx.request_type,
        )

    async def run_connection(
        self,
        handler: ExchangeSocketHandler,
        url: str,
        socket_params: SocketParams,
    ) -> None:
        """지정된 핸들러로 웹소켓 연결을 수행합니다.

        Args:
            handler (ExchangeSocketHandler): 실행할 거래소 핸들러
            url (str): 웹소켓 엔드포인트 URL
            socket_params (SocketParams): 거래소별 소켓 파라미터
        """
        await handler.websocket_connection(url=url, parameter_info=socket_params)

    # 컨텍스트 기반 헬퍼
    def create_handler_with(self, ctx: ConnectionScopeDomain) -> ExchangeSocketHandler:
        """컨텍스트 기반 핸들러 생성 헬퍼.

        Args:
            ctx (ConnectionScopeDomain): 연결 스코프

        Returns:
            ExchangeSocketHandler: 생성된 핸들러 인스턴스
        """
        return self.create_handler(ctx)

    async def run_with(
        self, ctx: StreamContextDomain, handler: ExchangeSocketHandler
    ) -> None:
        """컨텍스트를 사용해 웹소켓 연결을 실행합니다.

        Args:
            ctx (StreamContextDomain): URL/파라미터를 포함한 실행 컨텍스트
            handler (ExchangeSocketHandler): 실행할 핸들러
        """
        await self.run_connection(
            handler=handler,
            url=ctx.url,
            socket_params=ctx.socket_params,
        )


class RedisCacheCoordinator:
    """Redis 캐시 관련 책임.

    - 캐시 인스턴스 생성
    - 시작/정지 시 캐시 상태 갱신
    """

    def make_cache_with(self, ctx: StreamContextDomain) -> WebsocketConnectionCache:
        """컨텍스트로부터 캐시 인스턴스를 생성합니다.

        Args:
            ctx (StreamContextDomain): 연결 스코프/심볼 정보를 가진 컨텍스트

        Returns:
            WebsocketConnectionCache: 연결 상태/심볼 관리를 위한 캐시
        """
        spec = WebsocketConnectionSpecDomain(scope=ctx.scope, symbols=ctx.symbols)
        return WebsocketConnectionCache(spec)

    async def on_start(
        self, cache: WebsocketConnectionCache, symbols: list[str]
    ) -> None:
        """연결 시작 시 캐시를 초기화하고 상태를 CONNECTED로 설정합니다.

        Args:
            cache (WebsocketConnectionCache): 대상 캐시 인스턴스
            symbols (list[str]): 초기 구독 심볼 목록
        """
        if symbols:
            await cache.replace_symbols(symbols)
        await cache.set_connected()

    async def on_start_with(
        self,
        ctx: StreamContextDomain,
        cache: WebsocketConnectionCache,
    ) -> None:
        """컨텍스트 기반 시작 훅. 심볼을 주입한 뒤 on_start를 호출합니다.

        Args:
            ctx (StreamContextDomain): 실행 컨텍스트
            cache (WebsocketConnectionCache): 대상 캐시 인스턴스
        """
        await self.on_start(cache, list(ctx.symbols))

    async def on_stop(self, cache: WebsocketConnectionCache) -> None:
        """연결 종료 시 캐시 상태를 DISCONNECTED로 갱신하고 정리합니다.

        Args:
            cache (WebsocketConnectionCache): 대상 캐시 인스턴스
        """
        await cache.update_connection_state(CONNECTION_STATUS_DISCONNECTED)
        await cache.remove_connection()


class SubscriptionManager:
    """구독/재구독 및 심볼 추출 책임.

    - socket_params에서 심볼/구독타입 추출
    - 실행 중 핸들러의 재구독 처리
    """

    def extract_symbols(self, socket_params: SocketParams) -> list[str]:
        """소켓 파라미터에서 심볼 리스트를 추출합니다.

        Args:
            socket_params (SocketParams): 거래소별 소켓 파라미터(dict 또는 list 등)

        Returns:
            list[str]: 추출된 심볼 리스트(없으면 빈 리스트)
        """
        symbols_field = None
        if isinstance(socket_params, dict):
            symbols_field = socket_params.get("symbols")

        symbols: list[str] = (
            list(symbols_field)
            if isinstance(symbols_field, list)
            and all(isinstance(s, str) for s in symbols_field)
            else []
        )
        return symbols

    def fill_symbols(self, ctx: StreamContextDomain) -> None:
        """컨텍스트에 symbols를 추출/주입합니다.

        - tuple로 저장해 불변 컨테이너의 메모리 효율을 확보합니다.

        Args:
            ctx (StreamContextDomain): 실행 컨텍스트
        """
        syms = self.extract_symbols(ctx.socket_params)
        ctx.symbols = tuple(syms)

    async def handle_resubscribe(
        self,
        running: ExchangeSocketHandler | None,
        cache: WebsocketConnectionCache,
        ctx: StreamContextDomain,
    ) -> bool:
        """이미 실행 중인 핸들러에 대한 재구독을 처리합니다.

        - 리스트형 파라미터는 원문 그대로 재구독 전송
        - dict형 파라미터는 심볼 추출 후 캐시 갱신 및 증분 재구독

        Args:
            running (ExchangeSocketHandler | None): 실행 중인 핸들러(없으면 None)
            cache (WebsocketConnectionCache): 심볼/상태 캐시
            ctx (StreamContextDomain): 실행 컨텍스트

        Returns:
            bool: 처리 여부(True면 새 연결 생성 스킵)
        """
        logger.info(f"이미 실행 중: {_log_string(ctx.scope)} - 재구독 처리")

        # 1) Korbit 등 리스트 기반: raw 재구독 전송, 캐시 심볼 갱신은 생략
        if isinstance(ctx.socket_params, list):
            if running is not None and hasattr(running, "update_subscription_raw"):
                await running.update_subscription_raw(ctx.socket_params)  # type: ignore[attr-defined]
                logger.info(f"재구독 완료(리스트 기반): {_log_string(ctx.scope)}")
            return True

        # 2) dict 기반: 심볼 추출 후 캐시 갱신 + 증분 재구독
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
            logger.info(f"재구독 완료: {_log_string(ctx.scope)} -> {symbols}")
        return True


class StreamOrchestrator:
    """공통 스트림 오케스트레이터

    - Kafka에서 전달된 payload를 해석하여 각 거래소 핸들러를 생성 및 연결
    - projection 전달 및 websocket 연결 수행
    """

    def __init__(self) -> None:
        """오케스트레이터 구성 요소를 초기화합니다.

        - 커넥터/캐시 코디네이터/구독 매니저 컴포지션
        - 실행 중 태스크/핸들러 레지스트리 준비
        """
        # Composition
        self._connector = WebsocketConnector(HANDLER_MAP)
        self._cache = RedisCacheCoordinator()
        self._subs = SubscriptionManager()
        # 실행 상태 레지스트리들
        self._tasks: dict[tuple[str, str, str], asyncio.Task[None]] = {}
        self._handlers: dict[tuple[str, str, str], ExchangeSocketHandler] = {}

    def _make_key(self, scope: ConnectionScopeDomain) -> tuple[str, str, str]:
        """연결 스코프로 태스크/핸들러 레지스트리 키를 생성합니다.

        Args:
            scope (ConnectionScope): 연결 스코프

        Returns:
            tuple[str, str, str]: (exchange, region, request_type) 소문자 튜플
        """
        return (
            scope.exchange.lower(),
            scope.region.lower(),
            scope.request_type.lower(),
        )

    async def shutdown(self) -> None:
        """모든 실행 중인 연결 태스크를 취소하고 종료를 기다립니다.

        이 과정을 통해 각 태스크의 `finally` 블록(`connect_from_payload` 내부)이 실행되어
        Redis 상태 정리(`update_connection_state` 및 `remove_connection`)가 Redis 연결이
        닫히기 전에 완료되도록 보장합니다.
        """
        if not self._tasks:
            return
        # 취소 요청
        for task in list(self._tasks.values()):
            if not task.done():
                task.cancel()
        # 종료 대기
        for key, task in list(self._tasks.items()):
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                self._tasks.pop(key, None)

    # 분산락 제거: 동시성 제어는 Kafka 파티셔닝/캐시로 처리

    async def _handle_resubscribe(
        self,
        key: tuple[str, str, str],
        scope: ConnectionScopeDomain,
        socket_params: SocketParams,
    ) -> bool:
        """중복 실행 케이스에서 재구독 처리 흐름을 수행합니다.

        Args:
            key (tuple[str, str, str]): 실행 레지스트리 키
            scope (ConnectionScope): 연결 스코프
            socket_params (SocketParams): 재구독 대상 파라미터

        Returns:
            bool: 재구독을 수행했으면 True, 아니면 False
        """

        try:
            running = self._handlers.get(key)
            # 컨텍스트 구성 및 캐시 인스턴스 준비 (심볼은 SubscriptionManager에서 추출/갱신)
            ctx = StreamContextDomain(
                scope=scope,
                socket_params=socket_params,
            )
            cache: WebsocketConnectionCache = self._cache.make_cache_with(ctx)
            return await self._subs.handle_resubscribe(
                running=running,
                cache=cache,
                ctx=ctx,
            )
        except Exception as e:
            # 재구독 처리 중 예외를 에러 이벤트로 발행
            logger.error(f"재구독 처리 실패: {_log_string(scope)} - {e}")
            await _emit_scope_error_event(
                scope=scope,
                err=e,
                kind="ws",
                observed_key=f"{_log_string(scope)}|resubscribe",
                raw_context={
                    "phase": "resubscribe",
                    "socket_params_type": type(socket_params).__name__,
                    "socket_params": socket_params,
                },
            )
            return False

    async def connect_from_context(self, ctx: StreamContextDomain) -> None:
        """StreamContextDomain을 받아 웹소켓 연결을 생성/실행합니다.

                - projection 전달 가능
        - 중복 실행 방지는 레지스트리/캐시로 수행

                Args:
                    ctx (StreamContextDomain): 검증 및 변환 완료된 내부 컨텍스트
        """

        start_time = datetime.now()
        logger.info(
            f"{ctx.scope.exchange} 연결 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        handler: ExchangeSocketHandler = self._connector.create_handler_with(ctx.scope)

        logger.info(f"{ctx.scope.exchange} 핸들러 생성: {handler.__class__.__name__}")

        # projection이 문자열 리스트인 경우에만 핸들러에 설정
        if isinstance(ctx.projection, list) and all(
            isinstance(field, str) for field in ctx.projection
        ):
            setattr(handler, "projection", ctx.projection)

        if not ctx.url or ctx.socket_params is None:
            logger.error("connection.url 또는 connection.socket_params 누락")
            # 누락 오류를 에러 이벤트로 발행하고 종료
            await _emit_scope_error_event(
                scope=ctx.scope,
                err=ValueError("missing url or socket_params"),
                kind="ws",
                observed_key=_log_string(ctx.scope),
                raw_context={"phase": "precheck"},
            )
            return

        # 분산 락 제거: 중복 실행은 레지스트리/캐시로 제어
        key: tuple[str, str, str] = self._make_key(ctx.scope)
        existing = self._tasks.get(key)
        if existing and not existing.done():
            handled = await self._handle_resubscribe(
                key=key,
                scope=ctx.scope,
                socket_params=ctx.socket_params,
            )
            if handled:
                return

        # 태스크 레지스트리에 현재 태스크 등록
        current_task = asyncio.current_task()
        if current_task is not None:
            self._tasks[key] = current_task  # type: ignore[assignment]

        # Redis 캐시 준비
        self._subs.fill_symbols(ctx)
        cache = self._cache.make_cache_with(ctx)

        # 실행 중 핸들러 레지스트리 등록
        self._handlers[key] = handler

        try:
            await self._cache.on_start_with(ctx, cache)
            # 웹소켓 실행 (끊길 때까지 블로킹)
            await self._connector.run_with(ctx, handler)
        except Exception as e:
            # 런타임 예외를 에러 이벤트로 발행
            logger.error(f"연결 실행 중 오류: {_log_string(ctx.scope)} - {e}")
            await _emit_scope_error_event(
                scope=ctx.scope,
                err=e,
                kind="ws",
                observed_key=_log_string(ctx.scope),
                raw_context={"phase": "run_with"},
            )
        finally:
            await self._cache.on_stop(cache)
            # 태스크/핸들러 레지스트리 정리
            self._tasks.pop(key, None)
            self._handlers.pop(key, None)
