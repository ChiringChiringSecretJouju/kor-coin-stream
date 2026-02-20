from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Literal, cast

import orjson

from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.config.settings import kafka_settings, websocket_settings
from src.core.connection.handlers.base import BaseWebsocketHandler
from src.core.connection.services.realtime_collection import RealtimeBatchCollector
from src.core.connection.utils.logging.log_phases import (
    PHASE_BATCH_CLEANUP,
    PHASE_BATCH_CONTROL,
    PHASE_BATCH_ENQUEUE,
    PHASE_BATCH_INIT,
    PHASE_MESSAGE_LOOP_STOP,
    PHASE_PARSE,
)
from src.core.connection.utils.logging.logging_mixin import ScopedConnectionLoggingMixin
from src.core.connection.utils.market_data.common.parse import update_dict
from src.core.connection.utils.symbols.symbol_utils import (
    extract_symbol as _extract_symbol_impl,
)
from src.core.dto.io.commands import ConnectionTargetDTO
from src.core.types import MessageHandler, TickerResponseData, TradeResponseData
from src.infra.messaging.connect.producers.realtime.ticker import TickerDataProducer
from src.infra.messaging.connect.producers.realtime.trade import TradeDataProducer

logger = PipelineLogger.get_logger("websocket_handler", "connection")


class BaseRegionalWebsocketHandler(ScopedConnectionLoggingMixin, BaseWebsocketHandler):
    """Korea/Global 핸들러 공통 기반 클래스."""

    _batch_profile_name = "Regional"
    _logger = logger

    def __init__(
        self,
        exchange_name: str,
        region: str,
        request_type: str,
        heartbeat_kind: str | None = None,
        heartbeat_message: str | None = None,
        ticker_producer: TickerDataProducer | None = None,
        trade_producer: TradeDataProducer | None = None,
        trade_dispatcher: Any = None,
        ticker_dispatcher: Any = None,
    ) -> None:
        super().__init__(
            exchange_name=exchange_name,
            region=region,
            request_type=request_type,
        )

        if heartbeat_kind:
            self.set_heartbeat(
                kind=cast(Literal["frame", "text"], heartbeat_kind),
                message=heartbeat_message,
            )

        self.ping_interval = websocket_settings.heartbeat_interval
        self.projection: list[str] | None = None

        self._batch_collector: RealtimeBatchCollector | None = None
        self._ticker_producer: TickerDataProducer | None = ticker_producer
        self._trade_producer: TradeDataProducer | None = trade_producer
        self._is_shared_producer = ticker_producer is not None and trade_producer is not None

        self._trade_dispatcher = trade_dispatcher
        self._ticker_dispatcher = ticker_dispatcher
        self._batch_enabled = True

    def _extract_symbol(self, message: dict[str, Any]) -> str | None:
        return _extract_symbol_impl(message)

    def _parse_message(self, message: str | bytes | dict[str, Any]) -> dict[str, Any]:
        try:
            match message:
                case dict() as payload:
                    return payload
                case bytes() as payload:
                    message_str = payload.decode("utf-8").strip()
                    if not message_str:
                        return {}
                case str() as payload:
                    message_str = payload.strip()
                    if not message_str:
                        return {}
                case _:
                    message_str = str(message).strip()

            parsed = orjson.loads(message_str)
            match parsed:
                case dict() as payload:
                    return payload
                case list() as payload if payload and isinstance(payload[0], dict):
                    result = payload[0].copy()
                    result["_original_list"] = payload
                    result["_is_list_message"] = True
                    return result
                case list() as payload:
                    self._log_debug(
                        "List payload without dict item",
                        phase=PHASE_PARSE,
                        size=len(payload),
                    )
                    return {}
                case _:
                    self._log_debug(
                        "Unsupported parsed payload type",
                        phase=PHASE_PARSE,
                        parsed_type=type(parsed).__name__,
                    )
                    return {}

        except orjson.JSONDecodeError as e:
            if len(str(message)) >= 50:
                self._log_warning(
                    "JSON decode error",
                    phase=PHASE_PARSE,
                    error=str(e),
                    raw_message=repr(message),
                )
            return {}
        except Exception as e:
            logger.error(
                "Unexpected parsing error",
                extra=self._scope_log_extra(
                    PHASE_PARSE,
                    error=str(e),
                    raw_message=repr(message),
                ),
            )
            asyncio.create_task(
                dispatch_error(
                    exc=e,
                    kind="ws",
                    target=ConnectionTargetDTO(
                        exchange=self.scope.exchange,
                        region=self.scope.region,
                        request_type=self.scope.request_type,
                    ),
                    context={"message": str(message)[:100], "observed_key": "parsing"},
                )
            )
            return {}

    async def _initialize_batch_system(self) -> None:
        if not self._batch_enabled:
            return

        try:
            if not self._ticker_producer:
                self._ticker_producer = TickerDataProducer(use_avro=True)
                await self._ticker_producer.start_producer()

            if not self._trade_producer:
                use_array_format = kafka_settings.use_array_format
                self._trade_producer = TradeDataProducer(
                    use_avro=True,
                    use_array_format=use_array_format,
                )
                await self._trade_producer.start_producer()

            async def emit_batch(batch: list[dict[str, Any]]) -> bool:
                if not batch:
                    return False

                request_type = self.scope.request_type
                first_msg = batch[0]
                symbol = self._extract_symbol(first_msg) or "UNKNOWN"
                kafka_key = f"{self.scope.region}-{self.scope.exchange}-{request_type}-{symbol}"

                match request_type:
                    case "ticker" if self._ticker_producer is not None:
                        return await self._ticker_producer.send_batch(
                            scope=self.scope,
                            batch=batch,
                            key=kafka_key,
                        )
                    case "trade" if self._trade_producer is not None:
                        return await self._trade_producer.send_batch(
                            scope=self.scope,
                            batch=batch,
                            key=kafka_key,
                        )
                    case "orderbook":
                        return False
                    case _:
                        return False

            self._batch_collector = RealtimeBatchCollector(
                batch_size=30,
                time_window=5.0,
                max_batch_size=100,
                emit_factory=emit_batch,
            )

            await self._batch_collector.start()
            self._log_info(
                "Batch collection system initialized",
                phase=PHASE_BATCH_INIT,
                profile=self._batch_profile_name,
                shared_producer=self._is_shared_producer,
            )

        except Exception as e:
            logger.error(
                "Failed to initialize batch system",
                extra=self._scope_log_extra(PHASE_BATCH_INIT, error=str(e)),
            )
            self._batch_enabled = False
            await self._error_handler.emit_ws_error(
                e,
                observed_key="batch_system_init",
                raw_context={"batch_enabled": False},
            )

    async def _cleanup_batch_system(self) -> None:
        if self._batch_collector:
            await self._batch_collector.stop()
            self._batch_collector = None

        if not self._is_shared_producer:
            if self._ticker_producer:
                await self._ticker_producer.stop_producer()
                self._ticker_producer = None
            if self._trade_producer:
                await self._trade_producer.stop_producer()
                self._trade_producer = None

        self._log_info("Batch collection system cleaned up", phase=PHASE_BATCH_CLEANUP)

    def enable_batch_collection(self) -> None:
        self._batch_enabled = True
        self._log_info("Batch collection enabled", phase=PHASE_BATCH_CONTROL)

    def _normalize_message(self, parsed_message: dict[str, Any]) -> dict[str, Any]:
        data_sub: dict | None = parsed_message.get("data", None)
        if isinstance(data_sub, dict):
            parsed_message = update_dict(parsed_message, "data")

        fields: list[str] | None = self.projection
        if fields:
            return {field: parsed_message.get(field) for field in fields}
        return parsed_message

    async def _enqueue_batch_message(self, kind: str, message: dict[str, Any]) -> None:
        if self._batch_enabled and self._batch_collector:
            await self._batch_collector.add_message(kind, message)
            return

        self._log_warning(
            "Batch collection disabled or not initialized",
            phase=PHASE_BATCH_ENQUEUE,
            enabled=self._batch_enabled,
            collector=self._batch_collector is not None,
        )

    def _prepare_incoming_message(self, message: Any) -> tuple[dict[str, Any] | None, bool]:
        """Normalize raw input into a handler-ready dict.

        Returns:
            tuple[prepared_message, is_preprocessed]
        """
        if isinstance(message, dict) and message.get("_preprocessed", False):
            prepared = message.copy()
            prepared.pop("_preprocessed", None)
            return prepared, True

        parsed_message = message if isinstance(message, dict) else self._parse_message(message)
        if not parsed_message:
            return None, False
        return parsed_message, False

    async def _should_skip_by_ack(
        self,
        prepared: dict[str, Any],
        is_preprocessed: bool,
        ack_handler: Callable[[dict[str, Any]], Awaitable[bool]],
    ) -> bool:
        if is_preprocessed:
            return False
        return await ack_handler(prepared)

    def _normalized_payload(
        self, prepared: dict[str, Any], is_preprocessed: bool
    ) -> dict[str, Any]:
        if is_preprocessed:
            return prepared
        return self._normalize_message(prepared)

    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        raise NotImplementedError

    async def trade_message(self, message: Any) -> TradeResponseData | None:
        raise NotImplementedError

    async def orderbook_message(self, message: Any) -> dict[str, Any] | None:
        _ = message
        error = NotImplementedError(
            "orderbook request_type is not supported by regional realtime pipeline"
        )
        await self._error_handler.emit_ws_error(
            error,
            observed_key="orderbook_not_supported",
            raw_context={
                "request_type": self.scope.request_type,
                "exchange": self.scope.exchange,
                "region": self.scope.region,
            },
        )
        await self.request_disconnect(reason="orderbook_not_supported")
        return None

    async def _handle_message_loop(self, websocket: Any, timeout: int) -> None:
        while not self.stop_requested:
            start_time = time.time()
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                self._health_monitor.notify_receive()
            except asyncio.TimeoutError:
                continue

            parsed_message = self._parse_message(message)
            symbol = self._extract_symbol(parsed_message)
            self._minute_batch_counter.inc(symbol=symbol, start_time=start_time)
            await self._try_emit_ack_from_message(symbol)

            handler_map: MessageHandler = {
                "ticker": self.ticker_message,
                "trade": self.trade_message,
                "orderbook": self.orderbook_message,
            }
            fn = handler_map.get(self.scope.request_type)
            if fn:
                await fn(parsed_message)

        self._log_info("Message loop stopped", phase=PHASE_MESSAGE_LOOP_STOP)
