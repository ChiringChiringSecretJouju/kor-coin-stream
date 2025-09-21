from dataclasses import dataclass
from typing import Any, Callable


@dataclass(slots=True, frozen=True, kw_only=True, repr=False, match_args=False)
class ProducerConfigDomain:
    """Kafka Producer 설정 (confluent-kafka 최적화).

    - confluent-kafka-python 성능 최적화 설정 적용
    - 처리량(throughput) 우선 최적화
    - slots+frozen으로 메모리/안전 최적화
    """

    # 기본 연결 설정
    bootstrap_servers: str
    security_protocol: str

    # 성능 최적화 설정 (처리량 우선)
    acks: str | int  # "1" 또는 1 (처리량 최적화)
    batch_size: int  # 100000-200000 (기본값 16384보다 크게)
    linger_ms: int  # 10-100ms (기본값 0보다 크게)
    compression_type: str  # "lz4" 또는 "snappy" (압축 활성화)
    buffer_memory: int  # 64MB+ (기본값 33554432보다 크게)

    # 안정성 및 성능 설정
    enable_idempotence: bool  # True (중복 방지)
    retries: int  # 2147483647 (최대 재시도)
    max_in_flight_requests_per_connection: int  # 5 (성능 최적화)
    request_timeout_ms: int  # 30000ms (타임아웃)
    delivery_timeout_ms: int  # 120000ms (전체 전송 타임아웃)

    # 직렬화 콜백
    value_serializer: Callable[..., bytes]
    key_serializer: Callable[..., bytes]


@dataclass(slots=True, frozen=True, kw_only=True, repr=False, match_args=False)
class ConsumerConfigDomain:
    """Kafka Consumer 설정 (confluent-kafka 최적화).

    - confluent-kafka-python 성능 최적화 설정 적용
    - 처리량(throughput) 우선 최적화
    - slots+frozen으로 메모리/안전 최적화
    """

    # 기본 연결 설정
    bootstrap_servers: str
    security_protocol: str
    group_id: str

    # 오프셋 관리
    enable_auto_commit: bool
    auto_offset_reset: str  # "earliest" 또는 "latest"
    auto_commit_interval_ms: int  # 5000ms (자동 커밋 간격)

    # 성능 최적화 설정 (처리량 우선)
    fetch_min_bytes: int  # 100000+ (기본값 1보다 크게)
    fetch_max_wait_ms: int  # 500ms (최대 대기 시간)
    max_poll_records: int  # 1000+ (한 번에 가져올 레코드 수)
    max_poll_interval_ms: int  # 300000ms (폴링 간격)

    # 세션 관리
    session_timeout_ms: int  # 30000ms (세션 타임아웃)
    heartbeat_interval_ms: int  # 10000ms (하트비트 간격)

    # 파티션 할당 및 리밸런싱
    partition_assignment_strategy: str  # "RoundRobin" 또는 "Range"

    # 역직렬화 콜백
    # Any 사용 사유: 카프카 메시지 스키마가 토픽별로 상이하며 런타임 검증(Pydantic)으로 구체화
    value_deserializer: Callable[[bytes], Any]
    key_deserializer: Callable[[bytes | None], Any | None]


# Backward-compat aliases
ProducerConfig = ProducerConfigDomain
ConsumerConfig = ConsumerConfigDomain
