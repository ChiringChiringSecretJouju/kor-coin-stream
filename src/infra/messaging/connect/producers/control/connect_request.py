"""연결 요청 명령 Producer.

토픽: ws.command
"""

from src.core.dto.io.commands import CommandDTO
from src.infra.messaging.connect.producer_client import AvroProducer

KeyType = str | bytes | None


class ConnectRequestProducer(AvroProducer):
    """연결 요청 명령 Producer (리팩토링됨).
    
    토픽: ws.command
    키: {region}|{exchange}|{request_type}
    
    주요 기능:
    - 부모 클래스 기반 고성능 비동기 처리
    - orjson 기반 JSON 직렬화 (Avro 불필요)
    """
    
    def __init__(self, topic: str = "ws.command") -> None:
        super().__init__(use_avro=False)  # JSON 사용
        self.topic = topic
    
    async def send_event(self, event: CommandDTO, key: KeyType) -> bool:
        """연결 요청 명령 전송.
        
        Args:
            event: 연결 요청 명령 DTO
            key: Kafka 키
            
        Returns:
            전송 성공 여부
        """
        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )
