"""Avro 직렬화/역직렬화 유틸리티

confluent-kafka SerializationContext를 dataclass로 상속하여 Python 3.12 스타일로 개선합니다.
"""

from __future__ import annotations

from dataclasses import dataclass
from confluent_kafka.serialization import (
    SerializationContext as BaseSerializationContext,
    MessageField,
)


@dataclass(slots=True, frozen=True)
class SerializationContext(BaseSerializationContext):
    """
    Avro 직렬화 컨텍스트 (dataclass 버전)

    confluent-kafka의 SerializationContext를 상속하여 Python 3.12 dataclass 스타일로 개선.

    slots=True: 메모리 최적화
    frozen=True: 불변성 보장 (컨텍스트는 불변이어야 함)
    """

    topic: str
    field: MessageField
    headers: dict[str, bytes] | None = None

    def __post_init__(self) -> None:
        """데이터클래스 초기화 후 부모 클래스 초기화"""
        # frozen=True로 인해 object.__setattr__ 사용
        super().__init__(self.topic, self.field, self.headers)


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
    return AvroSerializationContext(
        topic=topic, field=MessageField.VALUE, headers=headers
    )


def create_key_context(
    topic: str, headers: dict[str, bytes] | None = None
) -> AvroSerializationContext:
    """키 직렬화를 위한 컨텍스트 생성"""
    return AvroSerializationContext(
        topic=topic, field=MessageField.KEY, headers=headers
    )


# 타입 에일리어스
SerdeContext = AvroSerializationContext  # 더 짧은 이름
ValueContext = AvroSerializationContext  # 의미적 이름
