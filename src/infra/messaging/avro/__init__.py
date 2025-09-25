"""
Avro 직렬화/역직렬화 및 Schema Registry 지원 모듈

이 모듈은 Confluent Kafka와 Schema Registry를 사용한 Avro 메시지 처리를 제공합니다.

주요 기능:
- Avro 스키마 기반 직렬화/역직렬화
- Schema Registry 클라이언트
- Confluent-kafka와 통합된 Producer/Consumer
- 스키마 진화 지원 (BACKWARD, FORWARD, FULL 호환성)
"""

from src.infra.messaging.avro.serializers import (
    AsyncAvroSerializer,
    AsyncAvroDeserializer,
    create_avro_serializer,
    create_avro_deserializer,
)


__all__ = [
    # Serializers
    "AsyncAvroSerializer",
    "AsyncAvroDeserializer",
    "create_avro_serializer",
    "create_avro_deserializer",
]
