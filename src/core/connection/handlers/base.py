from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from typing import Any, Literal, cast

import websockets

from src.common.exceptions.exception_rule import SOCKET_EXCEPTIONS
from src.common.logger import PipelineLogger
from src.common.metrics import MinuteBatchCounter
from src.config.settings import websocket_settings
from src.core.connection.emitters.connect_success_ack_emitter import (
    ConnectSuccessAckEmitter,
)
from src.core.connection.error_handler import ConnectionErrorHandler
from src.core.connection.health_monitor import ConnectionHealthMonitor
from src.core.connection.services.backoff import compute_next_backoff
from src.core.connection.subscription_manager import SubscriptionManager
from src.core.dto.internal.common import ConnectionPolicyDomain, ConnectionScopeDomain
from src.core.dto.internal.metrics import MinuteItemDomain
from src.core.types import (
    ExchangeName,
    Region,
    RequestType,
)
from src.infra.messaging.connect.producer_client import MetricsProducer

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
            heartbeat_timeout=float(websocket_settings.HEARTBEAT_TIMEOUT),
            heartbeat_fail_limit=websocket_settings.HEARTBEAT_FAIL_LIMIT,
            receive_idle_timeout=websocket_settings.RECEIVE_IDLE_TIMEOUT,
        )

        # 연결 상태 관리
        self._current_websocket = None

        # 컴포넌트 초기화
        self._subscription_manager = SubscriptionManager(self.scope)
        self._health_monitor = ConnectionHealthMonitor(self.scope, self.policy)
        self._error_handler = ConnectionErrorHandler(self.scope)
        # 롱-리빙 메트릭 프로듀서
        self._metrics_producer = MetricsProducer()
        # 연결 성공 ACK 방출기
        self._ack_emitter = ConnectSuccessAckEmitter(self.scope)
        # 중복 ACK 방지 플래그 (연결당 1회만 발행)
        self._ack_sent: bool = False

        # 실행 제어 플래그 및 재시도 정책
        self._stop_requested: bool = False
        self._max_reconnect_attempts: int = websocket_settings.RECONNECT_MAX_ATTEMPTS
        self._backoff_task: asyncio.Task[None] | None = None

        # emit_factory는 컨텍스트(self.scope, self._metrics_producer)를 캡처한 비동기 함수일것.
        async def _emit_factory(
            items: list[MinuteItemDomain], start_ts_kst: int, end_ts_kst: int
        ) -> None:
            await self._metrics_producer.send_counting_batch(
                scope=self.scope,
                items=items,
                range_start_ts_kst=start_ts_kst,
                range_end_ts_kst=end_ts_kst,
                key=self.scope.to_key(),
            )

        self._minute_batch_counter = MinuteBatchCounter(
            emit_factory=_emit_factory, logger=logger
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
        """구독 메시지 준비 (구독 매니저로 위임)"""
        return await self._subscription_manager.prepare_subscription_message(params)

    @abstractmethod
    async def _handle_message_loop(self, websocket: Any, timeout: int) -> None:
        """메시지 수신 및 처리 루프 - 각 거래소별로 구현 필요"""
        raise NotImplementedError()

    async def update_subscription(
        self, symbols: list[str], subscribe_type: str | None = None
    ) -> None:
        """실행 중 구독 심볼을 갱신합니다 (구독 매니저로 위임)"""
        success = await self._subscription_manager.update_subscription(
            self._current_websocket, symbols, subscribe_type
        )
        if not success:
            # 실패 시 에러 발행
            await self._error_handler.emit_subscription_error(
                RuntimeError("구독 업데이트 실패"), symbols=symbols
            )

    async def update_subscription_raw(self, params: dict[str, Any] | list[Any]) -> None:
        """원본 파라미터를 그대로 재전송하여 재구독합니다 (구독 매니저로 위임)"""
        success = await self._subscription_manager.update_subscription_raw(
            self._current_websocket, params
        )
        if not success:
            # 실패 시 에러 발행
            await self._error_handler.emit_subscription_error(
                RuntimeError("원본 구독 업데이트 실패"),
                symbols=params.get("symbols") if isinstance(params, dict) else None,
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

    async def websocket_connection(self, url: str, parameter_info: dict) -> None:
        """웹소켓에 연결하고 구독/수신 루프를 실행합니다. 끊김 시 재접속을 수행합니다."""
        socket_parameters: dict | list = parameter_info
        timeout: int = DEFAULT_MESSAGE_TIMEOUT

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

                    # 현재 연결 보관 및 구독 매니저에 파라미터 등록
                    self._current_websocket = websocket
                    self._subscription_manager.update_current_params(socket_parameters)

                    # 구독 파라미터 전송
                    subscription_message: str | bytes = (
                        await self._sending_socket_parameter(socket_parameters)
                    )
                    await websocket.send(subscription_message)
                    logger.info(f"{self.scope.exchange}: 구독 파라미터 전송 완료")

                    # 웹소켓 연결 및 구독 요청 완료 -> ACK 이벤트 전송
                    logger.info(
                        f"{self.scope.exchange}: 웹소켓 연결 및 구독 완료, ACK 이벤트 전송"
                    )
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
                backoff_delay = compute_next_backoff(self.policy, attempt - 1)
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
                backoff_delay = compute_next_backoff(self.policy, attempt - 1)
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

    async def _emit_connect_success_ack(self) -> None:
        """연결 성공(구독 확정) 시 심볼별 ACK 이벤트를 1회 발행합니다."""
        logger.debug(
            f"{self.scope.exchange}: _emit_connect_success_ack called, _ack_sent={self._ack_sent}"
        )

        if self._ack_sent:
            logger.debug(f"{self.scope.exchange}: ACK already sent, skipping")
            return

        # 심볼이 아직 상태에 세팅되지 않았어도, current_params에서 추출하여 사용 (타이밍 이슈 방지)
        try:
            symbols = self._subscription_manager.effective_symbols()
        except Exception:
            # 이전 버전 호환 또는 예상치 못한 예외 시 폴백
            symbols = self._subscription_manager.current_symbols or []
        logger.debug(f"{self.scope.exchange}: symbols_for_ack={symbols}")

        if not symbols:
            # 심볼을 확인할 수 없는 경우(리스트 기반 등)는 스킵
            logger.warning(
                f"{self.scope.exchange}: No symbols available for ACK, skipping"
            )
            return

        try:
            logger.info(
                f"{self.scope.exchange}: Sending connect success ACK for symbols: {symbols}"
            )
            await self._ack_emitter.emit_for_symbols(symbols)
            self._ack_sent = True
            logger.info(f"{self.scope.exchange}: Connect success ACK sent successfully")
        except Exception as ack_e:
            logger.warning(
                f"{self.scope.exchange}: connect-success ACK 전송 실패 - {ack_e}"
            )
