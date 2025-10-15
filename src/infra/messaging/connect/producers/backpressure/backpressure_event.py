"""백프레셔 이벤트 Producer.

토픽: ws.backpressure.events
"""

from src.core.dto.io.events import BackpressureEventDTO, BackpressureStatusDTO
from src.infra.messaging.connect.producer_client import AvroProducer

KeyType = str | bytes | None


class BackpressureEventProducer(AvroProducer):
    """백프레셔 이벤트 Producer (DI 주입).

    토픽: ws.backpressure.events
    키: {producer_name}

    성능 최적화:
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값
    - 부모 클래스 기반 고성능 비동기 처리

    Example:
        >>> producer = BackpressureEventProducer()
        >>> await producer.start_producer()
        >>> await producer.send_backpressure_event(
        ...     action="backpressure_activated",
        ...     producer_name="MetricsProducer",
        ...     status=status_dict
        ... )
    """

    def __init__(
        self, topic: str = "ws.backpressure.events", use_avro: bool = False
    ) -> None:
        """
        Args:
            topic: 토픽 이름 (기본: ws.backpressure.events)
            use_avro: True면 Avro 직렬화, False면 JSON 직렬화
        """
        super().__init__(use_avro=use_avro)
        self.topic = topic

    async def send_backpressure_event(
        self,
        action: str,
        producer_name: str,
        status: dict[str, int | float | bool],
        producer_type: str = "AsyncProducerBase",
        message: str | None = None,
        key: KeyType = None,
    ) -> bool:
        """백프레셔 이벤트 전송.

        Args:
            action: 백프레셔 상태 (backpressure_activated, backpressure_deactivated)
            producer_name: Producer 이름 (클래스명)
            status: 백프레셔 상태 정보 dict
            producer_type: Producer 타입
            message: 추가 메시지
            key: Kafka 키

        Returns:
            전송 성공 여부
        """
        status_dto = BackpressureStatusDTO(**status)
        event = BackpressureEventDTO(
            action=action,  # type: ignore
            producer_name=producer_name,
            producer_type=producer_type,
            status=status_dto,
            message=message,
        )

        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )
