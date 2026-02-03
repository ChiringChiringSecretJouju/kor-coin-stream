"""연결 성공 ACK 이벤트 Producer.

토픽: ws.connect_success.{region}
"""

from src.core.dto.io.commands import ConnectSuccessEventDTO
from src.infra.messaging.connect.producer_client import AvroProducer

KeyType = str | bytes | None


class ConnectSuccessEventProducer(AvroProducer):
    """연결 성공 ACK 이벤트 Producer (리팩토링됨, Avro 직렬화 우선).

    토픽: ws.connect_success.{region} (korea, asia, na, eu)
    키: {exchange}|{region}|{request_type}|{coin_symbol}

    성능 최적화:
    - use_avro=True: connect-success-events 스키마 (기본값)
    - use_avro=False: orjson 기반 JSON 직렬화
    - 부모 클래스 기반 고성능 비동기 처리
    """

    def __init__(self, use_avro: bool = False) -> None:
        """
        Args:
            use_avro: True면 Avro 직렬화, False면 JSON 직렬화
        """
        super().__init__(use_avro=use_avro)
        if use_avro:
            self.enable_avro("connect_success")

    async def send_event(self, event: ConnectSuccessEventDTO, key: KeyType) -> bool:
        """연결 성공 ACK 이벤트 전송.

        Args:
            event: 연결 성공 이벤트 DTO
            key: Kafka 키

        Returns:
            전송 성공 여부
        """
        region = event.target.region
        topic = f"ws.connect_success.{region}"

        return await self.produce_sending(
            message=event,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
