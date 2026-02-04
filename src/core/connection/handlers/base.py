from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from typing import Any, Literal, cast

import orjson
import websockets

from src.common.exceptions.exception_rule import SOCKET_EXCEPTIONS
from src.common.logger import PipelineLogger
from src.common.metrics import MinuteBatchCounter
from src.config.settings import websocket_settings
from src.core.connection.emitters.connect_success_ack_emitter import (
    ConnectSuccessAckEmitter,
    _normalize_coin_symbol,
)
from src.core.connection.error_handler import ConnectionErrorHandler
from src.core.connection.health_monitor import ConnectionHealthMonitor
from src.core.connection.services.backoff import compute_next_backoff
from src.core.connection.utils.subscription import (
    extract_subscription_symbols,
    merge_subscription_params,
)
from src.core.dto.internal.common import ConnectionPolicyDomain, ConnectionScopeDomain
from src.core.dto.internal.metrics import (
    ProcessingMetricsDomain,
    QualityMetricsDomain,
    ReceptionMetricsDomain,
)
from src.core.types import (
    ExchangeName,
    Region,
    RequestType,
)
from src.infra.messaging.connect.producers.backpressure.backpressure_event import (
    BackpressureEventProducer,
)
from src.infra.messaging.connect.producers.metrics.metrics import MetricsProducer

logger = PipelineLogger.get_logger("websocket_handler", "connection")


# Constants
DEFAULT_MESSAGE_TIMEOUT = 60
DEFAULT_PING_INTERVAL = 30


class BaseWebsocketHandler(ABC):
    """웹소켓 핸들러 추상 기본 클래스"""

    def __init__(self, exchange_name: str, region: str, request_type: str) -> None:
        """
        Args:
            exchange_name: 거래소 이름
            region: 거래소 지역
            request_type: 요청 타입
        """
        # 스코프 및 정책 캡슐화
        self.scope = ConnectionScopeDomain(
            exchange=cast(ExchangeName, exchange_name),
            region=cast(Region, region),
            request_type=cast(RequestType, request_type),
        )
        self.policy = ConnectionPolicyDomain(
            initial_backoff=1.0,
            max_backoff=30.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            heartbeat_kind="frame",
            heartbeat_message=None,
            heartbeat_timeout=float(websocket_settings.heartbeat_timeout),
            heartbeat_fail_limit=websocket_settings.heartbeat_fail_limit,
            receive_idle_timeout=websocket_settings.receive_idle_timeout,
        )

        # 연결 상태 관리
        self._current_websocket = None

        # 컴포넌트 초기화
        # self._subscription_manager 제거됨 -> 핸들러 직접 관리
        self._last_socket_params: dict[str, Any] | list[Any] | None = None
        
        self._health_monitor = ConnectionHealthMonitor(self.scope, self.policy)
        self._error_handler = ConnectionErrorHandler(self.scope)

        # 롱-리빙 메트릭 프로듀서
        self._metrics_producer = MetricsProducer()

        # 백프레셔 모니터링 Producer
        self._backpressure_producer = BackpressureEventProducer()

        # 연결 성공 ACK 방출기
        self._ack_emitter = ConnectSuccessAckEmitter(self.scope)
        # 중복 ACK 방지 플래그 (연결당 1회만 발행)
        self._ack_sent: bool = False
        # 실제 메시지에서 추출된 심볼 캐시 (최초 1회 추출용)
        self._cached_symbols: set[str] = set()

        # 실행 제어 플래그 및 재시도 정책
        self._stop_requested: bool = False
        self._max_reconnect_attempts: int = websocket_settings.reconnect_max_attempts
        self._backoff_task: asyncio.Task[None] | None = None
        self._last_backoff: float = 0.0  # Decorrelated Jitter용 이전 상태
        self._current_correlation_id: str | None = None

        # 3개의 독립 emit_factory (Layer별 독립 전송)
        async def _reception_emit_factory(
            items: list[ReceptionMetricsDomain], start_ts_kst: int, end_ts_kst: int
        ) -> None:
            await self._metrics_producer.send_reception_batch(
                scope=self.scope,
                items=items,
                range_start_ts_kst=start_ts_kst,
                range_end_ts_kst=end_ts_kst,
                key=self.scope.to_key(),
            )

        async def _processing_emit_factory(
            items: list[ProcessingMetricsDomain], start_ts_kst: int, end_ts_kst: int
        ) -> None:
            await self._metrics_producer.send_processing_batch(
                scope=self.scope,
                items=items,
                range_start_ts_kst=start_ts_kst,
                range_end_ts_kst=end_ts_kst,
                key=self.scope.to_key(),
            )

        async def _quality_emit_factory(
            items: list[QualityMetricsDomain], start_ts_kst: int, end_ts_kst: int
        ) -> None:
            await self._metrics_producer.send_quality_batch(
                scope=self.scope,
                items=items,
                range_start_ts_kst=start_ts_kst,
                range_end_ts_kst=end_ts_kst,
                key=self.scope.to_key(),
            )

        self._minute_batch_counter = MinuteBatchCounter(
            reception_emit=_reception_emit_factory,
            processing_emit=_processing_emit_factory,
            quality_emit=_quality_emit_factory,
            logger=logger,
        )

    def _log_status(self, status: str) -> None:
        """연결 상태 로깅"""
        logger.info(
            f"{self.scope.exchange} [{self.scope.region}/{self.scope.request_type}]: {status}"
        )

    def set_heartbeat(
        self,
        kind: Literal["frame", "text"] = "frame",
        message: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        """하트비트 설정을 동적으로 구성 (헬스 모니터로 위임)

        Args:
            kind: "frame"(웹소켓 ping/pong) 또는 "text"(텍스트/JSON 메시지 전송)
            message: kind=="text"일 때 보낼 메시지(JSON 문자열 등)
            timeout: frame 모드에서 pong 대기 타임아웃
        """
        self._health_monitor.update_policy(kind, message, timeout)

    async def _sending_socket_parameter(
        self, params: dict[str, Any] | list[Any]
    ) -> str | bytes:
        """구독 메시지 준비 (orjson 직렬화 직접 수행)"""
        if not params:
            return ""
        return orjson.dumps(params).decode("utf-8")

    @abstractmethod
    async def _handle_message_loop(self, websocket: Any, timeout: int) -> None:
        """메시지 수신 및 처리 루프 - 각 거래소별로 구현 필요"""
        raise NotImplementedError()

    async def update_subscription(
        self, symbols: list[str], subscribe_type: str | None = None
    ) -> None:
        """실행 중 구독 심볼을 동적으로 추가합니다 (Zero-Downtime).
        
        Algorithm:
            1. State Update: 현재 파라미터(_last_socket_params)에 새 심볼 병합.
            2. Check State: 연결된 상태라면 즉시 구독 메시지 전송.
            3. Recovery: 연결 끊긴 상태라도 1번에서 상태가 갱신되었으므로 재연결 시 반영됨.
        """
        if not symbols or not self._last_socket_params:
            return

        # 1. State Update (Merge)
        try:
            merged_params = merge_subscription_params(self._last_socket_params, symbols)
            # 만약 subscribe_type이 주어졌다면 업데이트 (일부 거래소용)
            if subscribe_type and isinstance(merged_params, dict):
                merged_params["subscribe_type"] = subscribe_type
                
            self._last_socket_params = merged_params
            logger.info(f"{self.scope.exchange}: Subscription params merged with {symbols}")
        except Exception as e:
            logger.warning(f"{self.scope.exchange}: Merge params failed - {e}")
            return

        # 2. Check State & Send
        websocket = self._current_websocket
        if websocket and not getattr(websocket, "closed", True):
            try:
                # 변경된 전체 파라미터를 다시 전송 (Overwrite Strategy)
                # 대부분의 거래소는 추가 구독 시 전체 리스트를 보내도 안전함
                msg = await self._sending_socket_parameter(self._last_socket_params)
                await websocket.send(msg)
                logger.info(f"{self.scope.exchange}: Dynamic subscription sent")
            except Exception as e:
                logger.warning(
                    f"{self.scope.exchange}: Dynamic subscription send failed - {e}"
                )
        else:
            logger.info(
                f"{self.scope.exchange}: Not connected, params updated for next connection"
            )

    async def update_subscription_raw(self, params: dict[str, Any] | list[Any]) -> None:
        """원본 파라미터를 그대로 재전송하여 재구독합니다."""
        websocket = self._current_websocket
        if websocket is None or getattr(websocket, "closed", True):
            logger.warning(f"{self.scope.exchange}: 활성 소켓 없음, 재구독(raw) 무시")
            return

        try:
            msg = await self._sending_socket_parameter(params)
            await websocket.send(msg)
            # 상태 업데이트
            self._last_socket_params = params
            logger.info(f"{self.scope.exchange}: 재구독(raw) 메시지 전송 완료")
        except Exception as e:
            logger.warning(f"{self.scope.exchange}: 재구독(raw) 전송 실패 - {e}")
            await self._error_handler.emit_subscription_error(
                e,
                symbols=None, # raw params라 심볼 목록을 바로 알기 어려움 (추출 필요 시 유틸 사용)
            )

    async def _batch_flush(self) -> None:
        try:
            await self._minute_batch_counter.flush_now()
        except Exception as flush_e:
            logger.warning(f"{self.scope.exchange}: metrics flush 실패 - {flush_e}")
            await self._error_handler.emit_ws_error(flush_e)
        finally:
            # 종료 경로에서 남아있을 수 있는 producer를 반드시 정리
            await self._metrics_producer.stop_producer()
            await self._ack_emitter.aclose()

    @property
    def stop_requested(self) -> bool:
        """외부에서 종료 요청 여부를 확인하기 위한 플래그."""
        return self._stop_requested

    async def request_disconnect(self, reason: str | None = None) -> None:
        """외부 신호에 의해 웹소켓 연결을 중단합니다."""

        if self._stop_requested:
            return

        self._stop_requested = True
        reason_suffix = f" (reason: {reason})" if reason else ""
        logger.info(f"{self.scope.exchange}: disconnect requested{reason_suffix}")

        if self._backoff_task and not self._backoff_task.done():
            self._backoff_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._backoff_task
            self._backoff_task = None

        websocket = self._current_websocket
        if websocket is not None:
            try:
                await websocket.close()
            except Exception as close_error:
                logger.warning(
                    f"""
                    {self.scope.exchange}: websocket close failed 
                    during disconnect - {close_error}"""
                )

        await self._health_monitor.stop_monitoring()

    async def websocket_connection(
        self, url: str, parameter_info: dict, correlation_id: str | None = None
    ) -> None:
        """웹소켓에 연결하고 구독/수신 루프를 실행합니다. 끊김 시 재접속을 수행합니다."""
        socket_parameters: dict | list = parameter_info
        timeout: int = DEFAULT_MESSAGE_TIMEOUT
        self._current_correlation_id = correlation_id

        if not socket_parameters:
            logger.warning(f"{self.scope.exchange}: 소켓 파라미터가 없습니다.")
            return

        attempt = 0
        while not self._stop_requested:
            self._log_status("connecting")
            logger.info(f"{self.scope.exchange}: 연결 시도 중... {url}")
            try:
                async with websockets.connect(uri=url, ping_interval=None) as websocket:
                    if self._stop_requested:
                        await websocket.close()
                        break

                    logger.info(f"{self.scope.exchange}: 연결 성공")
                    self._log_status("connected")
                    attempt = 0  # 성공적으로 연결되었으므로 백오프 시도횟수 리셋
                    self._stop_requested = False
                    self._ack_sent = False
                    self._last_backoff = 0.0  # 성공 시 백오프 상태 리셋
                    # 재연결 시 캐시 초기화
                    self._cached_symbols.clear()

                    # Producer 시작 및 백프레셔 모니터링 연결
                    await self._backpressure_producer.start_producer()
                    await self._metrics_producer.start_producer()
                    # MetricsProducer에 백프레셔 모니터링 연결 (30초마다 큐 상태 리포트)
                    self._metrics_producer.producer.set_backpressure_event_producer(
                        self._backpressure_producer, enable_periodic_monitoring=True
                    )

                    # 현재 연결 보관 및 파라미터 상태 저장
                    self._current_websocket = websocket
                    self._last_socket_params = socket_parameters

                    # 구독 파라미터 전송
                    subscription_message: str | bytes = (
                        await self._sending_socket_parameter(socket_parameters)
                    )
                    await websocket.send(subscription_message)

                    # 웹소켓 연결 및 구독 요청 완료 -> ACK 이벤트 전송
                    await self._emit_connect_success_ack()

                    # 하트비트/워치독 태스크 시작
                    ping_interval = getattr(
                        self, "ping_interval", DEFAULT_PING_INTERVAL
                    )
                    # 헬스 모니터링 시작
                    await self._health_monitor.start_monitoring(
                        websocket, ping_interval
                    )
                    await self._handle_message_loop(websocket, timeout)

                    # 정상 종료 혹은 외부 요청에 의한 종료
                    break
            except asyncio.CancelledError:
                logger.info(f"{self.scope.exchange}: 연결 작업이 취소되었습니다.")
                self._log_status("cancelled")
                raise
            except SOCKET_EXCEPTIONS as e:
                if self._stop_requested:
                    logger.info(
                        f"{self.scope.exchange}: disconnect flow stopped reconnection (reason: {e})"
                    )
                    break

                self._log_status("disconnected")
                logger.warning(
                    f"{self.scope.exchange}: 연결이 끊겼습니다. 재시도합니다. 이유: {e}"
                )
                attempt += 1
                
                # Smart Backoff (Decorrelated Jitter + Circuit Breaker)
                backoff_delay = compute_next_backoff(
                    self.policy, attempt - 1, self._last_backoff
                )
                self._last_backoff = backoff_delay  # 다음 계산을 위해 저장

                # ws 경계 예외를 에러 토픽으로 발행
                await self._error_handler.emit_connection_error(
                    e,
                    url=url,
                    attempt=attempt,
                    backoff=backoff_delay,
                )
                if attempt >= self._max_reconnect_attempts:
                    logger.error(
                        f"""
                        {self.scope.exchange}:재연결 ({self._max_reconnect_attempts}) 초과 종료
                        """
                    )
                    await self._error_handler.emit_ws_error(
                        RuntimeError("max reconnect attempts exceeded"),
                        observed_key=f"url:{url}:retry_limit",
                        raw_context={
                            "attempt": attempt,
                            "max_reconnect_attempts": self._max_reconnect_attempts,
                            "url": url,
                        },
                    )
                    self._stop_requested = True
                    break

                logger.info(
                    f"{self.scope.exchange}: {backoff_delay:.2f}s 후 재접속 (attempt={attempt})"
                )
                self._backoff_task = asyncio.create_task(asyncio.sleep(backoff_delay))
                try:
                    await self._backoff_task
                except asyncio.CancelledError:
                    logger.info(f"{self.scope.exchange}: 재접속 대기 중단")
                    if self._stop_requested:
                        break
                    raise
                finally:
                    self._backoff_task = None
            except Exception as e:
                # 예기치 못한 오류도 ws.error로 발행하고 재시도 흐름을 동일하게 적용
                if self._stop_requested:
                    logger.info(
                        f"{self.scope.exchange}: disconnect flow stopped reconnection (reason: {e})"
                    )
                    break

                self._log_status("disconnected")
                logger.error(
                    f"{self.scope.exchange}: unexpected error in connection loop - {e}"
                )
                attempt += 1

                # Smart Backoff (Decorrelated Jitter + Circuit Breaker)
                backoff_delay = compute_next_backoff(
                    self.policy, attempt - 1, self._last_backoff
                )
                self._last_backoff = backoff_delay

                await self._error_handler.emit_ws_error(
                    e,
                    observed_key=f"url:{url}:unexpected",
                    raw_context={
                        "attempt": attempt,
                        "backoff": backoff_delay,
                        "url": url,
                    },
                )
                if attempt >= self._max_reconnect_attempts:
                    logger.error(
                        f"""
                        {self.scope.exchange}: 재연결 시도 
                        재시도 한도({self._max_reconnect_attempts}) 초과로 종료
                        """
                    )
                    await self._error_handler.emit_ws_error(
                        RuntimeError("max reconnect attempts exceeded"),
                        observed_key=f"url:{url}:retry_limit",
                        raw_context={
                            "attempt": attempt,
                            "max_reconnect_attempts": self._max_reconnect_attempts,
                            "url": url,
                        },
                    )
                    self._stop_requested = True
                    break

                logger.info(
                    f"{self.scope.exchange}: {backoff_delay:.2f}s 후 재접속 (attempt={attempt})"
                )
                self._backoff_task = asyncio.create_task(asyncio.sleep(backoff_delay))
                try:
                    await self._backoff_task
                except asyncio.CancelledError:
                    logger.info(f"{self.scope.exchange}: 재접속 대기 중단")
                    if self._stop_requested:
                        break
                    raise
                finally:
                    self._backoff_task = None
            finally:
                self._current_websocket = None
                await self._batch_flush()
                await self._health_monitor.stop_monitoring()

        self._log_status("stopped")
        self._stop_requested = True

    async def _try_emit_ack_from_message(self, symbol: str | None) -> None:
        """메시지에서 추출된 심볼로 ACK 발행 시도 (심볼별 1회만).
        
        Args:
            symbol: 추출된 심볼 ("BTC_COUNT" 형식)
        """
        if not symbol or not symbol.endswith("_COUNT"):
            return
        
        # "BTC_COUNT" → "BTC" 변환
        coin = symbol[:-6]
        
        # 이미 ACK 발행한 심볼이면 스킵
        if coin in self._cached_symbols:
            return
        
        # 새로운 심볼 발견 → 캐시에 추가 및 즉시 ACK 발행
        self._cached_symbols.add(coin)
        logger.info(
            f"{self.scope.exchange}: New symbol extracted: {coin}, sending ACK immediately"
        )
        await self._emit_single_symbol_ack(coin)

    async def _emit_single_symbol_ack(self, coin: str) -> None:
        """단일 심볼에 대한 ACK 이벤트 발행.
        
        Args:
            coin: 정규화된 심볼 ("BTC" 형식)
        """
        try:
            await self._ack_emitter.emit_for_symbols(
                [coin], correlation_id=self._current_correlation_id
            )
            logger.info(f"{self.scope.exchange}: ACK sent for symbol: {coin}")
        except Exception as ack_e:
            logger.warning(
                f"{self.scope.exchange}: ACK 전송 실패 (symbol={coin}) - {ack_e}"
            )

    async def _emit_connect_success_ack(self) -> None:
        """연결 성공(구독 확정) 시 심볼별 ACK 이벤트를 발행합니다.
        
        구독 확인 메시지(SUBSCRIBED 등)에서 호출되며, socket_parameters에서 심볼 추출을 시도합니다.
        추출 실패 시 실제 데이터 메시지에서 점진적으로 추출합니다.
        """
        if self._ack_sent:
            logger.debug(f"{self.scope.exchange}: ACK process already initiated, skipping")
            return

        # socket_parameters에서 심볼 추출 시도
        try:
            # 상태로 저장된 마지막 파라미터 사용
            params = self._last_socket_params
            if params:
                symbols = extract_subscription_symbols(params)
                logger.info(
                    f"{self.scope.exchange}: Extracted symbols from params: {symbols}"
                )
            else:
                symbols = []
        except Exception as e:
            logger.warning(f"{self.scope.exchange}: 심볼 추출 실패 - {e}")
            symbols = []
        
        if not symbols:
            # 추출 실패 → 실제 데이터 메시지에서 점진적으로 추출
            logger.info(
                f"{self.scope.exchange}: No symbols from socket_parameters, "
                "will extract from data messages"
            )
            self._ack_sent = True  # 재시도 방지
            return

        # 추출 성공 → 각 심볼에 대해 ACK 발행
        logger.info(
            f"{self.scope.exchange}: Sending ACK for {len(symbols)} symbols "
            "from socket_parameters"
        )
        for sym in symbols:
            # 정규화 ("KRW-BTC" → "BTC" 등)
            coin = _normalize_coin_symbol(sym)
            
            # 중복 방지
            if coin not in self._cached_symbols:
                self._cached_symbols.add(coin)
                await self._emit_single_symbol_ack(coin)
        
        self._ack_sent = True
        logger.info(f"{self.scope.exchange}: ACK process completed")
