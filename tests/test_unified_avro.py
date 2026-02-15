#!/usr/bin/env python3
"""
통합 Avro Producer 테스트 스크립트

KafkaProducerClient 기본 클래스의 Avro 직렬화 기능 테스트
"""

import asyncio

import pytest

from src.common.logger import PipelineLogger
from src.infra.messaging.connect.producers.control.connect_success import (
    ConnectSuccessEventProducer,
)
from src.infra.messaging.connect.producers.metrics.metrics import MetricsProducer
from src.infra.messaging.connect.producers.realtime.realtime_data import (
    RealtimeDataProducer,
)
from tests.factory_builders import build_scope_domain, build_ticker_batch_payload

logger = PipelineLogger.get_logger("unified_avro_test", "main")

pytestmark = pytest.mark.skip(
    reason="Integration smoke script requires external Kafka/Schema Registry"
)


async def test_ticker_producer():
    """티커 Producer Avro 테스트"""
    logger.info("🎯 티커 Producer Avro 테스트 시작")

    # Avro 활성화된 Producer
    producer = RealtimeDataProducer(use_avro=True)

    # Avro 상태 확인
    status = producer.get_avro_status()
    logger.info(f"Avro 상태: {status}")

    # 테스트 데이터
    test_batch = build_ticker_batch_payload()

    scope = build_scope_domain(exchange="binance", region="asia")

    try:
        success = await producer.send_ticker_batch(scope, test_batch)
        if success:
            logger.info("✅ 티커 배치 전송 성공!")
        else:
            logger.error("❌ 티커 배치 전송 실패!")
    except Exception as e:
        logger.error(f"티커 테스트 오류: {e}")


async def test_metrics_producer():
    """메트릭 Producer Avro 테스트"""
    logger.info("📊 메트릭 Producer Avro 테스트 시작")

    # Avro 활성화된 Producer (기본값)
    producer = MetricsProducer(use_avro=True)

    # Avro 상태 확인
    status = producer.get_avro_status()
    logger.info(f"메트릭 Avro 상태: {status}")

    # JSON 방식으로도 사용 가능
    json_producer = MetricsProducer(use_avro=False)
    json_status = json_producer.get_avro_status()
    logger.info(f"메트릭 JSON 상태: {json_status}")


async def test_connect_success_producer():
    """연결 성공 Producer Avro 테스트"""
    logger.info("🔗 연결 성공 Producer Avro 테스트 시작")

    # Avro 활성화된 Producer
    producer = ConnectSuccessEventProducer(use_avro=True)

    # Avro 상태 확인
    status = producer.get_avro_status()
    logger.info(f"연결 성공 Avro 상태: {status}")


async def test_avro_toggle():
    """Avro 활성화/비활성화 테스트"""
    logger.info("🔄 Avro 토글 테스트 시작")

    # JSON 방식으로 시작
    producer = RealtimeDataProducer(use_avro=False)

    # 초기 상태
    logger.info(f"초기 상태 (JSON): {producer.get_avro_status()}")

    # Avro 활성화
    producer.enable_avro("ticker-data-value")
    logger.info(f"Avro 활성화 후: {producer.get_avro_status()}")

    # Avro 비활성화
    producer.disable_avro()
    logger.info(f"비활성화 후: {producer.get_avro_status()}")


async def main():
    """메인 함수"""
    logger.info("🚀 통합 Avro Producer 테스트 시작")

    await test_ticker_producer()
    await test_metrics_producer()
    await test_connect_success_producer()
    await test_avro_toggle()

    logger.info("🎉 모든 테스트 완료!")


if __name__ == "__main__":
    asyncio.run(main())
