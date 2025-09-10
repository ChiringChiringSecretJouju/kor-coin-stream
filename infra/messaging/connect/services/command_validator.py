"""
범용 DTO 검증 및 변환을 위한 Validator 모듈.

- 검증: dict -> DTO (모든 Pydantic 모델 지원)
- 예외 발생 시 DLQ 발행
- error_wrappers 사용하여 예외 처리 통일
"""

from __future__ import annotations

import traceback
import uuid
from typing import Final, TypeVar, cast

from pydantic import BaseModel

from common.exceptions.exception_rule import DESERIALIZATION_ERRORS
from common.logger import PipelineLogger
from core.types import DEFAULT_SCHEMA_VERSION, ExchangeName, RequestType
from core.dto.io.dlq_event import DlqEventDTO
from core.dto.io.target import ConnectionTargetDTO
from core.dto.io.error_event import WsEventErrorMetaDTO
from infra.messaging.connect.producer_client import DlqProducer


logger: Final = PipelineLogger.get_logger("validator", "generic")
DTOType = TypeVar("T", bound=BaseModel)


class GenericValidator:
    """범용 DTO 검증기.

    - 입력 dict를 지정된 DTO 타입으로 변환
    - 모든 Pydantic 모델 지원
    - 검증 실패 시 DLQ로 전송
    - kafka_exception_wrapped 데코레이터로 예외 처리 통일
    """

    def __init__(self, exchange_name: str, request_type: str) -> None:
        self._dlq_producer = DlqProducer()
        self.exchange_name = exchange_name
        self.request_type = request_type

    async def validate_dto(
        self,
        payload: dict,
        dto_class: type[DTOType],
        key: str | None = None,
    ) -> DTOType | None:
        """딕셔너리를 지정된 DTO 타입으로 변환하고 검증합니다.

        검증에 실패하면 DLQ로 전송하고 None을 반환합니다.

        Args:
            payload: Kafka에서 받은 원본 페이로드
            dto_class: 변환할 대상 DTO 클래스 타입
            key: 메시지 키 (DLQ 전송용)

        Returns:
            DTOType | None: 검증 성공 시 DTO 객체, 실패 시 None
        """

        try:
            # 지정된 DTO 클래스로 변환 시도 (Pydantic 검증 수행)
            dto = dto_class.model_validate(payload)
            logger.debug(f"{dto_class.__name__} 검증 성공")
            return cast(DTOType, dto)
        except DESERIALIZATION_ERRORS as e:
            # 검증 실패 시 DLQ로 전송
            await self._send_to_dlq(
                payload=payload,
                reason=f"{dto_class.__name__} 검증 실패: {str(e)}",
                key=key,
            )
            return None

    async def _send_to_dlq(
        self,
        payload: dict,
        reason: str,
        key: str | None = None,
    ) -> None:
        """실패한 메시지를 DLQ로 전송합니다.

        Args:
            payload: 원본 페이로드
            reason: 실패 이유
            key: 메시지 키
        """
        try:
            # 로깅
            logger.warning(f"명령 검증 실패: {reason}")

            # DLQ 이벤트 생성 및 전송
            dlq_event = DlqEventDTO(
                action="dlq",
                reason=reason,
                target=ConnectionTargetDTO(
                    exchange=cast(ExchangeName, self.exchange_name),
                    region="korea",
                    request_type=cast(RequestType, self.request_type),
                ),
                meta=WsEventErrorMetaDTO(
                    schema_version=DEFAULT_SCHEMA_VERSION,
                    correlation_id=uuid.uuid4().hex,
                    observed_key=key,
                    raw_context=payload,
                ),
                detail_error={
                    "validation_error": traceback.format_exc(),
                },
            )

            await self._dlq_producer.send_dlq_event(
                event=dlq_event,
                key=key,
            )
        except Exception as e:
            # DLQ 전송 실패는 최소한 로깅만이라도
            logger.error(f"DLQ 전송 실패: {e}")
