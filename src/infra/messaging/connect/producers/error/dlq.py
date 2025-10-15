"""Dead Letter Queue Producer.

토픽: ws.dlq
"""

from typing import Any

from src.core.dto.io.events import DlqEventDTO
from src.infra.messaging.connect.producer_client import AvroProducer

KeyType = str | bytes | None


class DlqProducer(AvroProducer):
    """Dead Letter Queue Producer (리팩토링됨, DI 주입).
    
    토픽: ws.dlq
    키: {exchange}|{region}|{request_type}
    
    주요 기능:
    - 부모 클래스 기반 고성능 비동기 처리
    - orjson 기반 JSON 직렬화 (Avro 불필요)
    - 실패한 메시지 + 오류 메타데이터 저장
    """
    
    def __init__(self, topic: str = "ws.dlq", use_avro: bool = False) -> None:
        """
        Args:
            topic: 토픽 이름 (기본: ws.dlq)
            use_avro: True면 Avro 직렬화, False면 JSON 직렬화
        """
        super().__init__(use_avro=use_avro)
        self.topic = topic
    
    async def send_failed_message(
        self,
        original_message: dict[str, Any],
        error_reason: str,
        error_context: dict[str, Any] | None = None,
        key: KeyType = None,
    ) -> bool:
        """실패한 메시지를 DLQ로 전송.
        
        Args:
            original_message: 원본 메시지
            error_reason: 실패 사유
            error_context: 추가 에러 컨텍스트
            key: Kafka 키
            
        Returns:
            전송 성공 여부
        """
        dlq_payload = {
            "original_message": original_message,
            "error_reason": error_reason,
            "error_context": error_context or {},
        }
        
        return await self.produce_sending(
            message=dlq_payload,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )
    
    async def send_dlq_event(self, event: DlqEventDTO, key: KeyType = None) -> bool:
        """DLQ 이벤트 전송 (error_dispatcher 호환성).
        
        Args:
            event: DLQ 이벤트 DTO
            key: Kafka 키 (None이면 자동 생성)
            
        Returns:
            전송 성공 여부
        """
        if key is None:
            key = f"{event.target.exchange}|{event.target.region}|{event.target.request_type}"
        
        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )
