"""Avro 직렬화/역직렬화 유틸리티

confluent-kafka SerializationContext를 래핑하여 타입 안전한 컨텍스트를 제공합니다.
"""

from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka.serialization import (
    MessageField,
)
from confluent_kafka.serialization import (
    SerializationContext as BaseSerializationContext,
)


@dataclass(slots=True, frozen=True)
class AvroSerializationContext:
    """
    Avro 전용 직렬화 컨텍스트

    confluent-kafka SerializationContext의 대체제로 더 간단하고 타입 안전한 방식.
    """

    topic: str
    field: MessageField = MessageField.VALUE
    headers: dict[str, bytes] | None = None

    def to_confluent_context(self) -> BaseSerializationContext:
        """기존 confluent-kafka SerializationContext로 변환"""
        return BaseSerializationContext(self.topic, self.field, self.headers)


# 팩토리 함수들
def create_value_context(
    topic: str, headers: dict[str, bytes] | None = None
) -> AvroSerializationContext:
    """값 직렬화를 위한 컨텍스트 생성"""
    return AvroSerializationContext(topic=topic, field=MessageField.VALUE, headers=headers)


def create_key_context(
    topic: str, headers: dict[str, bytes] | None = None
) -> AvroSerializationContext:
    """키 직렬화를 위한 컨텍스트 생성"""
    return AvroSerializationContext(topic=topic, field=MessageField.KEY, headers=headers)


# 타입 에일리어스
SerdeContext = AvroSerializationContext  # 더 짧은 이름
ValueContext = AvroSerializationContext  # 의미적 이름
