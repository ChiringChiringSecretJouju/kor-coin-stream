from __future__ import annotations

from typing import Any, cast

from src.core.connection.handlers.regional_base import BaseRegionalWebsocketHandler
from src.core.connection.utils.logging.log_phases import (
    PHASE_SUBSCRIPTION_ACK,
)
from src.core.connection.utils.market_data.parsers.base import TickerParser, TradeParser
from src.core.connection.utils.subscriptions.subscription_ack import (
    decide_korea_subscription_ack,
)
from src.core.types import TickerResponseData, TradeResponseData
from src.infra.messaging.connect.producers.realtime.ticker import TickerDataProducer
from src.infra.messaging.connect.producers.realtime.trade import TradeDataProducer


class BaseKoreaWebsocketHandler(BaseRegionalWebsocketHandler):
    """한국 거래소 전용 기반 웹소켓 핸들러."""

    _batch_profile_name = "Korea"

    def __init__(
        self,
        exchange_name: str,
        region: str,
        request_type: str,
        heartbeat_kind: str | None = None,
        heartbeat_message: str | None = None,
        ticker_producer: TickerDataProducer | None = None,
        trade_producer: TradeDataProducer | None = None,
        trade_dispatcher: TradeParser | None = None,
        ticker_dispatcher: TickerParser | None = None,
    ) -> None:
        super().__init__(
            exchange_name=exchange_name,
            region=region,
            request_type=request_type,
            heartbeat_kind=heartbeat_kind,
            heartbeat_message=heartbeat_message,
            ticker_producer=ticker_producer,
            trade_producer=trade_producer,
            trade_dispatcher=trade_dispatcher,
            ticker_dispatcher=ticker_dispatcher,
        )

    async def _handle_subscription_ack(self, parsed_message: dict[str, Any]) -> bool:
        decision = decide_korea_subscription_ack(parsed_message)
        if not decision.should_skip_message:
            return False
        if decision.status is not None:
            self._log_status(decision.status)

        match decision.reason:
            case "SUBSCRIBED":
                self._log_info(
                    "Subscription confirmed",
                    phase=PHASE_SUBSCRIPTION_ACK,
                    reason="SUBSCRIBED",
                )
            case "CONNECTED":
                self._log_debug(
                    "Connection confirmed",
                    phase=PHASE_SUBSCRIPTION_ACK,
                    reason="CONNECTED",
                )
            case str() as reason:
                self._log_debug(
                    "Skipping non-data response",
                    phase=PHASE_SUBSCRIPTION_ACK,
                    reason=reason,
                )
            case _:
                pass
        if decision.should_emit_ack:
            await self._emit_connect_success_ack()
        return True

    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        prepared, is_preprocessed = self._prepare_incoming_message(message)
        if prepared is None:
            return None

        if await self._should_skip_by_ack(prepared, is_preprocessed, self._handle_subscription_ack):
            return None

        filtered_message = self._normalized_payload(prepared, is_preprocessed)

        await self._enqueue_batch_message("ticker", filtered_message)
        return cast(TickerResponseData, filtered_message)

    async def trade_message(self, message: Any) -> TradeResponseData | None:
        prepared, is_preprocessed = self._prepare_incoming_message(message)
        if prepared is None:
            return None

        if await self._should_skip_by_ack(prepared, is_preprocessed, self._handle_subscription_ack):
            return None

        if is_preprocessed:
            filtered_message = prepared
        else:
            normalized_message = self._normalized_payload(prepared, is_preprocessed)

            if self._trade_dispatcher is None:
                raise RuntimeError(
                    f"{self.scope.exchange}: trade_dispatcher가 주입되지 않았습니다. "
                    "DI Container 설정을 확인하세요."
                )
            trade_dto = self._trade_dispatcher.parse(normalized_message)
            filtered_message = trade_dto.model_dump()

        await self._enqueue_batch_message("trade", filtered_message)

        return cast(TradeResponseData, filtered_message)
