from dataclasses import dataclass
from typing import Any, Callable


@dataclass(slots=True, frozen=True, kw_only=True, repr=False, match_args=False)
class ProducerConfigDomain:
    """Kafka Producer 설정 (내부용 dataclass).

    - slots+frozen으로 메모리/안전 최적화.
    - I/O 경계로 전달 시에는 외부 클라이언트가 요구하는 kwargs로 변환하여 사용하세요.
    """

    bootstrap_servers: str
    security_protocol: str
    acks: str | int
    linger_ms: int
    # 직렬화 콜백: 입력 타입은 다양할 수 있으므로 구체 타입 표기는 생략
    value_serializer: Callable[..., bytes]
    key_serializer: Callable[..., bytes]


@dataclass(slots=True, frozen=True, kw_only=True, repr=False, match_args=False)
class ConsumerConfigDomain:
    """Kafka Consumer 설정 (내부용 dataclass)."""

    bootstrap_servers: str
    security_protocol: str
    group_id: str
    enable_auto_commit: bool
    auto_offset_reset: str
    # 역직렬화 콜백: 반환 타입은 다양한 파이썬 기본형/컨테이너가 될 수 있음
    # Any 사용 사유: 카프카 메시지 스키마가 토픽별로 상이하며 런타임 검증(Pydantic)으로 구체화
    value_deserializer: Callable[[bytes], Any]
    key_deserializer: Callable[[bytes | None], Any | None]

# Backward-compat aliases
ProducerConfig = ProducerConfigDomain
ConsumerConfig = ConsumerConfigDomain
