"""에러 이벤트 Producer.

토픽: ws.error
"""

from src.common.logger import PipelineLogger
from src.core.dto.io.events import WsErrorEventDTO
from src.infra.messaging.connect.producer_client import AvroProducer
from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic

KeyType = str | bytes | None
logger = PipelineLogger.get_logger(__name__)


class ErrorEventProducer(AvroProducer):
    """에러 이벤트 Producer (리팩토링됨, DI 주입).

    토픽: ws.error
    키: null (파티셔닝 없음)

    주요 기능:
    - 부모 클래스 기반 고성능 비동기 전송 및 배치 처리
    - orjson 기반 JSON 직렬화 (Avro 불필요)
    """

    def __init__(self, topic: str = "ws.error", use_avro: bool = False) -> None:
        """
        Args:
            topic: 토픽 이름 (기본: ws.error)
            use_avro: True면 Avro 직렬화, False면 JSON 직렭화
        """
        resolved_use_avro, reason = resolve_use_avro_for_topic(topic, use_avro)
        if reason == "json_only_topic" and use_avro:
            logger.warning(
                "ws.error topic has no Avro schema contract. Falling back to JSON serializer."
            )
        super().__init__(use_avro=resolved_use_avro)
        self.topic = topic

    async def send_error_event(self, event: WsErrorEventDTO, key: KeyType = None) -> bool:
        """에러 이벤트 전송.

        Args:
            event: 에러 이벤트 DTO
            key: Kafka 키 (기본: None, 파티셔닝 없음)

        Returns:
            전송 성공 여부
        """
        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )
