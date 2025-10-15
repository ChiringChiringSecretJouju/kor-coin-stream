"""Orderbook 데이터 전용 Producer (Avro 지원).

토픽: orderbook-data.{region}
스키마: orderbook-data-value
"""

from __future__ import annotations

import time
from typing import Any

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.realtime import RealtimeDataBatchDTO
from src.core.types import OrderbookResponseData
from src.infra.messaging.connect.producer_client import AvroProducer

KeyType = str | bytes | None


class OrderbookDataProducer(AvroProducer):
    """Orderbook 데이터 전용 Producer (Avro 직렬화 우선).

    토픽: orderbook-data.{region} (korea, asia, na, eu)
    키: {exchange}:{region}:orderbook

    성능 최적화:
    - use_avro=True: orderbook-data-value 스키마 사용 (기본값)
    - use_avro=False: orjson 기반 JSON 직렬화
    - 부모 클래스 기반 고성능 비동기 처리
    - Avro 직렬화로 20-40% 메시지 크기 감소
    - asyncio.to_thread() CPU 오프로드
    """

    def __init__(self, use_avro: bool = True) -> None:
        """
        Args:
            use_avro: True면 Avro 직렬화, False면 JSON 직렬화
        """
        super().__init__(use_avro=use_avro)
        if use_avro:
            self.enable_avro("orderbook-data-value")

    def _convert_to_dto(
        self, scope: ConnectionScopeDomain, batch: list[dict[str, Any]]
    ) -> RealtimeDataBatchDTO:
        """배치 데이터를 DTO로 변환 (타입 안전성 보장)."""
        return RealtimeDataBatchDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type="orderbook",  # 고정
            timestamp_ms=int(time.time() * 1000),
            batch_size=len(batch),
            batch_id=None,
            data=batch,
        )

    async def send_batch(
        self,
        scope: ConnectionScopeDomain,
        batch: list[OrderbookResponseData],
        key: KeyType = None,
    ) -> bool:
        """Orderbook 배치 전송.

        Args:
            scope: 연결 스코프 (exchange, region 포함)
            batch: Orderbook 데이터 배치
            key: Kafka 키 (None이면 자동 생성)

        Returns:
            전송 성공 여부
        """
        topic = f"orderbook-data.{scope.region}"
        if key is None:
            key = f"{scope.exchange}:{scope.region}:orderbook"

        message = self._convert_to_dto(scope, batch)
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
