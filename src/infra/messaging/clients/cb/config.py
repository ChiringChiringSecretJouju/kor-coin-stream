"""
Kafka 클라이언트 설정 관리

Producer/Consumer 설정을 ProducerConfigDomain/ConsumerConfigDomain 기반으로 구성하고
confluent-kafka 형식으로 변환하는 공통 함수들을 제공합니다.
"""

from typing import Any

from src.config.settings import kafka_settings
from src.core.dto.internal.mq import ConsumerConfigDomain, ProducerConfigDomain


def producer_config(**overrides: Any) -> dict[str, Any]:
    """Producer 설정을 ProducerConfigDomain 기반으로 구성 후 confluent-kafka 형식으로 변환.

    - 성능 최적화: 처리량(throughput) 우선 설정
    - ProducerConfigDomain을 사용하여 타입 안전성 보장
    - confluent-kafka 매개변수 명으로 변환

    Args:
        **overrides: 사용자 지정 설정으로 덮어쓸 값들

    Returns:
        confluent-kafka Producer 설정 딕셔너리
    """
    # ProducerConfigDomain으로 성능 최적화 설정 구성
    cfg_domain = ProducerConfigDomain(
        # 기본 연결 설정
        bootstrap_servers=kafka_settings.BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        # 성능 최적화 설정 (처리량 우선)
        acks="all",  # 안정성 우선 (스키마 검증이 중요한 경우)
        batch_size=131072,  # 128KB (기본값 16KB보다 8배 크게)
        linger_ms=50,  # 50ms 대기 (배치 효율 극대화)
        compression_type="lz4",  # LZ4 압축 (빠른 압축/해제)
        # 안정성 및 성능 설정
        enable_idempotence=True,
        retries=2147483647,  # 최대 재시도
        max_in_flight_requests_per_connection=5,  # 성능 최적화
        request_timeout_ms=30000,  # 30초 타임아웃
        delivery_timeout_ms=120000,  # 2분 전체 타임아웃
        # 직렬화 함수는 클라이언트별로 설정
        value_serializer=None,
        key_serializer=None,
    )

    # confluent-kafka 형식으로 변환 (점 표기법 사용)
    cfg = {
        "bootstrap.servers": cfg_domain.bootstrap_servers,
        "security.protocol": cfg_domain.security_protocol,
        "acks": cfg_domain.acks,
        "batch.size": cfg_domain.batch_size,
        "linger.ms": cfg_domain.linger_ms,
        "compression.type": cfg_domain.compression_type,
        "retries": cfg_domain.retries,
        "enable.idempotence": cfg_domain.enable_idempotence,
        "max.in.flight.requests.per.connection": cfg_domain.max_in_flight_requests_per_connection,
        "request.timeout.ms": cfg_domain.request_timeout_ms,
        "delivery.timeout.ms": cfg_domain.delivery_timeout_ms,
    }

    cfg.update(overrides)  # 사용자 지정 값으로 덮어쓰기
    return cfg


def consumer_config(**overrides: Any) -> dict[str, Any]:
    """Consumer 설정을 ConsumerConfigDomain 기반으로 구성 후 confluent-kafka 형식으로 변환.

    - confluent-kafka-python 호환 설정만 사용
    - 최소한의 필수 설정으로 안정성 확보

    Args:
        **overrides: 사용자 지정 설정으로 덮어쓸 값들

    Returns:
        confluent-kafka Consumer 설정 딕셔너리
    """
    # ConsumerConfigDomain으로 기본 설정 구성 (호환성 유지)
    cfg_domain = ConsumerConfigDomain(
        # 기본 연결 설정
        bootstrap_servers=kafka_settings.BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        group_id=kafka_settings.CONSUMER_GROUP_ID,
        # 오프셋 관리
        enable_auto_commit=True,
        auto_offset_reset=kafka_settings.AUTO_OFFSET_RESET,
        auto_commit_interval_ms=5000,  # 5초 자동 커밋
        # 성능 최적화 설정 (confluent-kafka 미지원 속성들은 도메인에만 유지)
        fetch_min_bytes=102400,  # 도메인 호환성 유지
        fetch_max_wait_ms=500,  # 도메인 호환성 유지
        max_poll_records=1000,  # 도메인 호환성 유지
        max_poll_interval_ms=300000,  # 5분 폴링 간격
        # 세션 관리
        session_timeout_ms=30000,  # 30초 세션 타임아웃
        heartbeat_interval_ms=10000,  # 10초 하트비트
        # 역직렬화 함수는 클라이언트별로 설정
        value_deserializer=None,
        key_deserializer=None,
    )

    # confluent-kafka 형식으로 변환 (지원되는 속성만 포함)
    cfg = {
        # 필수 기본 설정
        "bootstrap.servers": cfg_domain.bootstrap_servers,
        "security.protocol": cfg_domain.security_protocol,
        "group.id": cfg_domain.group_id,
        # 오프셋 관리
        "enable.auto.commit": cfg_domain.enable_auto_commit,
        "auto.offset.reset": cfg_domain.auto_offset_reset,
        "auto.commit.interval.ms": cfg_domain.auto_commit_interval_ms,
        # 세션 관리 (confluent-kafka 지원)
        "session.timeout.ms": cfg_domain.session_timeout_ms,
        "heartbeat.interval.ms": cfg_domain.heartbeat_interval_ms,
        "max.poll.interval.ms": cfg_domain.max_poll_interval_ms,
    }

    cfg.update(overrides)
    return cfg


def avro_producer_config(**overrides: Any) -> dict[str, Any]:
    """Avro Producer 전용 설정 - 안정성 우선"""
    defaults = {
        "acks": "all",  # Avro는 스키마 검증이 중요하므로 안정성 우선
    }
    defaults.update(overrides)
    return producer_config(**defaults)


def avro_consumer_config(**overrides: Any) -> dict[str, Any]:
    """Avro Consumer 전용 설정"""
    return consumer_config(**overrides)


def json_producer_config(**overrides: Any) -> dict[str, Any]:
    """JSON Producer 전용 설정 - 처리량 우선"""
    defaults = {
        "acks": "1",  # JSON은 처리량 우선
    }
    defaults.update(overrides)
    return producer_config(**defaults)


def json_consumer_config(**overrides: Any) -> dict[str, Any]:
    """JSON Consumer 전용 설정"""
    return consumer_config(**overrides)
