"""Trade 데이터 전용 Producer (Avro 지원).

토픽: trade-data.{region}
스키마: trade-data-value
"""

from __future__ import annotations

import time
from typing import Any

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.realtime import RealtimeDataBatchDTO
from src.core.types import TradeResponseData
from src.infra.messaging.avro.subjects import TRADE_DATA_SUBJECT
from src.infra.messaging.connect.producer_client import AvroProducer
from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic

KeyType = str | bytes | None


class TradeDataProducer(AvroProducer):
    """Trade 데이터 전용 Producer (Avro 직렬화 기본).

    토픽: trade-data.{region} (korea, asia, na, eu)
    키: {exchange}:{region}:trade

    성능 최적화:
    - use_avro=True: trade-data-value 스키마 사용 (기본값)
    - use_avro=False: orjson 기반 JSON 직렬화
    - 부모 클래스 기반 고성능 비동기 처리
    - Avro 직렬화로 20-40% 메시지 크기 감소
    - asyncio.to_thread() CPU 오프로드
    """

    def __init__(self, use_avro: bool = True, use_array_format: bool = False) -> None:
        """
        Args:
            use_avro: True면 Avro 직렬화, False면 JSON 직렬화
            use_array_format: True면 데이터를 배열 포맷 [v, ts, p, q, s, id]으로 변환
        """
        resolved_use_avro, _ = resolve_use_avro_for_topic("trade-data.korea", use_avro)
        super().__init__(use_avro=resolved_use_avro)
        self.use_array_format = use_array_format
        if resolved_use_avro:
            self.enable_avro(TRADE_DATA_SUBJECT)

    def _transform_to_array_format(self, batch: list[dict[str, Any]]) -> list[list[Any]]:
        """딕셔너리 리스트를 배열 리스트로 변환 (프로토콜 버전 포함).

        Format v1: [ver, ts, price, vol, side, id, code]
        - ver: 1 (int)
        - ts: trade_timestamp (float)
        - price: trade_price (float)
        - vol: trade_volume (float)
        - side: ask_bid (int)
        - id: sequential_id (str)
        - code: code (str) - 배치 내 혼합 가능성 대비 포함
        """
        array_batch = []
        for item in batch:
            # 필수 필드 추출 (속도 최적화를 위해 get 대신 직접 접근 시도 가능하나 안전하게 get 사용)
            row = [
                1,  # Protocol Version
                item.get("trade_timestamp", 0.0),
                item.get("trade_price", 0.0),
                item.get("trade_volume", 0.0),
                item.get("ask_bid", 0),
                item.get("sequential_id", ""),
                item.get("code", ""),
            ]
            array_batch.append(row)
        return array_batch

    def _convert_to_dto(
        self, scope: ConnectionScopeDomain, batch: list[dict[str, Any]]
    ) -> RealtimeDataBatchDTO:
        """배치 데이터를 DTO로 변환 (타입 안전성 보장)."""
        return RealtimeDataBatchDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type="trade",  # 고정
            timestamp_ms=int(time.time() * 1000),
            batch_size=len(batch),
            data=self._transform_to_array_format(batch) if self.use_array_format else batch,
        )

    async def send_batch(
        self,
        scope: ConnectionScopeDomain,
        batch: list[TradeResponseData],
        key: KeyType = None,
    ) -> bool:
        """Trade 배치 전송.

        Args:
            scope: 연결 스코프 (exchange, region 포함)
            batch: Trade 데이터 배치
            key: Kafka 키 (None이면 자동 생성)

        Returns:
            전송 성공 여부
        """
        topic = f"trade-data.{scope.region}"
        if key is None:
            # 배치 내 첫 번째 메시지에서 심볼 추출하여 Kafka Key로 사용 (순서 보장)
            if batch and isinstance(batch[0], dict):
                first_msg = batch[0]
                # Trade DTO는 'code' (예: KRW-BTC) 필드를 가짐
                symbol = first_msg.get("code") or first_msg.get("symbol")
                if symbol:
                    key = str(symbol).upper()

            # 실패 시 폴백
            if key is None:
                key = f"{scope.exchange}:{scope.region}:trade"

        message = self._convert_to_dto(scope, batch)
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
