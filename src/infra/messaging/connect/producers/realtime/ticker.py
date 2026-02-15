"""Ticker 데이터 전용 Producer (Avro 지원).

토픽: ticker-data.{region}
스키마: ticker-data-value
"""

from __future__ import annotations

import time
from typing import Any

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.realtime import RealtimeDataBatchDTO
from src.core.types import TickerResponseData
from src.infra.messaging.avro.subjects import TICKER_DATA_SUBJECT
from src.infra.messaging.connect.producer_client import AvroProducer
from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic

KeyType = str | bytes | None


class TickerDataProducer(AvroProducer):
    """Ticker 데이터 전용 Producer (Avro 직렬화 기본).

    토픽: ticker-data.{region} (korea, asia, na, eu)
    키: {exchange}:{region}:ticker

    성능 최적화:
    - use_avro=True: ticker-data-value 스키마 사용 (기본값)
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
        resolved_use_avro, _ = resolve_use_avro_for_topic("ticker-data.korea", use_avro)
        super().__init__(use_avro=resolved_use_avro)
        if resolved_use_avro:
            self.enable_avro(TICKER_DATA_SUBJECT)

    def _convert_to_dto(
        self, scope: ConnectionScopeDomain, batch: list[dict[str, Any]]
    ) -> RealtimeDataBatchDTO:
        """배치 데이터를 DTO로 변환 (타입 안전성 보장)."""
        return RealtimeDataBatchDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type="ticker",  # 고정
            timestamp_ms=int(time.time() * 1000),
            batch_size=len(batch),
            data=batch,
        )

    async def send_batch(
        self,
        scope: ConnectionScopeDomain,
        batch: list[TickerResponseData],
        key: KeyType = None,
    ) -> bool:
        """Ticker 배치 전송.

        Args:
            scope: 연결 스코프 (exchange, region 포함)
            batch: Ticker 데이터 배치
            key: Kafka 키 (None이면 자동 생성)

        Returns:
            전송 성공 여부
        """
        topic = f"ticker-data.{scope.region}"
        if key is None:
            # 배치 내 첫 번째 메시지에서 심볼 추출하여 Kafka Key로 사용 (순서 보장)
            # Upbit: 'code', Binance: 's', Bithumb/Coinone: 'market' 등
            if batch and isinstance(batch[0], dict):
                first_msg = batch[0]
                symbol = (
                    first_msg.get("code")
                    or first_msg.get("symbol")
                    or first_msg.get("market")
                    or first_msg.get("s")
                )
                if symbol:
                    key = str(symbol).upper()

            # 실패 시 폴백
            if key is None:
                key = f"{scope.exchange}:{scope.region}:ticker"

        message = self._convert_to_dto(scope, batch)
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
