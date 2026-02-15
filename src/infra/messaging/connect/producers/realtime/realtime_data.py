from __future__ import annotations

import time
from typing import Any

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.realtime import RealtimeDataBatchDTO
from src.core.types import TickerResponseData, TradeResponseData
from src.infra.messaging.avro.subjects import REALTIME_TICKER_SUBJECT
from src.infra.messaging.connect.producer_client import AvroProducer
from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic

BatchType = list[TickerResponseData | TradeResponseData]


class RealtimeDataProducer(AvroProducer):
    """통합 실시간 데이터 배치 프로듀서.

    토픽 전략:
    - ticker-data.{region}
    - trade-data.{region}
    """

    def __init__(self, use_avro: bool = True) -> None:
        resolved_use_avro, _ = resolve_use_avro_for_topic("ticker-data.korea", use_avro)
        super().__init__(use_avro=resolved_use_avro)
        if resolved_use_avro:
            self.enable_avro(REALTIME_TICKER_SUBJECT)

    def _convert_to_dto(
        self, scope: ConnectionScopeDomain, batch: list[dict[str, Any]]
    ) -> RealtimeDataBatchDTO:
        return RealtimeDataBatchDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
            timestamp_ms=int(time.time() * 1000),
            batch_size=len(batch),
            data=batch,
        )

    async def send_batch(self, scope: ConnectionScopeDomain, batch: BatchType) -> bool:
        topic = f"{scope.request_type}-data.{scope.region}"

        key = None
        if batch and isinstance(batch[0], dict):
            first_msg = batch[0]
            raw_symbol = (
                first_msg.get("code")
                or first_msg.get("symbol")
                or first_msg.get("target_currency")
                or first_msg.get("s")
                or first_msg.get("market")
            )
            if raw_symbol:
                key = str(raw_symbol).upper()

        if key is None:
            key = f"{scope.exchange}:{scope.region}:{scope.request_type}"

        message = self._convert_to_dto(scope, batch)
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
