"""
Kafka 클라이언트 설정 관리

Producer/Consumer 설정을 ProducerConfigDomain/ConsumerConfigDomain 기반으로 구성하고
confluent-kafka 형식으로 변환하는 공통 함수들을 제공합니다.
"""

from typing import Any

from src.config.settings import kafka_settings


def producer_config(**overrides: Any) -> dict:
    """Producer 설정을 ProducerConfigDomain 기반으로 구성 후 confluent-kafka 형식으로 변환.

    - 성능 최적화: 처리량(throughput) 우선 설정
    - ProducerConfigDomain을 사용하여 타입 안전성 보장
    - confluent-kafka 매개변수 명으로 변환

    Args:
        **overrides: 사용자 지정 설정으로 덮어쓸 값들

    Returns:
        confluent-kafka Producer 설정 딕셔너리
    """
    # confluent-kafka 형식으로 변환 (점 표기법 사용)
    cfg = {
        # 필수
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        # 보안(예시: SASL/SCRAM). mTLS면 security.protocol/ssl.*로 교체
        # "security.protocol": "SASL_SSL",
        # "sasl.mechanisms": "SCRAM-SHA-512",
        # "sasl.username": "<user>",
        # "sasl.password": "<pass>",
        # 안정성: Idempotent + acks=all (+ in-flight ≤5, retries>0)
        "enable.idempotence": True,  # EOS 아님, 중복 없는 producer 보장.
        "acks": "all",  # idempotence 요구조건에 포함.
        "max.in.flight.requests.per.connection": 5,  # ≤5 권장. 별칭 'max.in.flight'도 가능.
        # 재시도/타임아웃
        "retries": 1000000,  # 재시도는 크게
        "request.timeout.ms": 30000,
        "delivery.timeout.ms": 120000,  # 전체 전송 마감시간.
        # 처리량·지연 균형(배칭)
        "linger.ms": 30,  # queue.buffering.max.ms의 현대식 별칭.
        "batch.size": 331072,  # (신규 지원) 배치 바이트 상한.
        # 압축
        "compression.type": "lz4",
    }

    cfg.update(overrides)  # 사용자 지정 값으로 덮어쓰기
    return cfg


def consumer_config(**overrides: Any) -> dict:
    """Consumer 설정을 ConsumerConfigDomain 기반으로 구성 후 confluent-kafka 형식으로 변환.

    - confluent-kafka-python 호환 설정만 사용
    - 최소한의 필수 설정으로 안정성 확보
    - cooperative-sticky 전략으로 리밸런싱 최적화 (무중단)

    Args:
        **overrides: 사용자 지정 설정으로 덮어쓸 값들

    Returns:
        confluent-kafka Consumer 설정 딕셔너리
    """
    # confluent-kafka 형식으로 변환 (지원되는 속성만 포함)
    cfg = {
        # 필수 기본 설정
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        "security.protocol": "PLAINTEXT",
        "group.id": kafka_settings.consumer_group_id,
        # 오프셋 관리
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,  # 수동 저장: 처리 성공 시에만 store_offsets 호출
        "auto.offset.reset": kafka_settings.auto_offset_reset,
        "auto.commit.interval.ms": 5000,
        # 세션 관리 (confluent-kafka 지원)
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
        "max.poll.interval.ms": 300000,
        # 파티션 할당 전략 (시니어 레벨 최적화)
        # cooperative-sticky: 증분 리밸런싱으로 서비스 중단 없음 (vs stop-the-world)
        "partition.assignment.strategy": "cooperative-sticky",
    }

    cfg.update(overrides)
    return cfg


def avro_producer_config(**overrides: Any) -> dict:
    """Avro Producer 전용 설정 - 안정성 우선"""
    return producer_config(**overrides)


def avro_consumer_config(**overrides: Any) -> dict:
    """Avro Consumer 전용 설정"""
    return consumer_config(**overrides)


def json_producer_config(**overrides: Any) -> dict:
    """JSON Producer 전용 설정 - 처리량 우선"""
    return producer_config(**overrides)


def json_consumer_config(**overrides: Any) -> dict:
    """JSON Consumer 전용 설정"""
    return consumer_config(**overrides)
