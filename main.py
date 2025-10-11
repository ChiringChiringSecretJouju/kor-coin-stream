"""
통합 거래소 웹소켓 스트림 파이프라인

여러 한국 암호화폐 거래소에 병렬로 웹소켓 연결하여 실시간 데이터를 수신합니다.
구성 정보는 통합 JSON 형식으로 제공되며,

사용법:
    python main.py                   # Kafka 토픽을 소비하여 연결 지시 처리
    python main.py --exchange upbit  # 특정 거래소만 연결 (기존 모드, 선택)
"""

import asyncio
import contextlib

from src.application.orchestrator import StreamOrchestrator
from src.common.logger import PipelineLogger
from src.config.settings import kafka_settings
from src.infra.cache.cache_client import RedisConnectionManager
from src.infra.messaging.connect.consumer_client import KafkaConsumerClient
from src.infra.messaging.connect.disconnection_consumer import (
    KafkaDisconnectionConsumerClient,
)

logger = PipelineLogger.get_logger("main", "app")


async def main() -> None:
    """메인 실행 함수"""
    logger.info("암호화폐 거래소 웹소켓 스트림 파이프라인 시작 (Kafka 소비 모드)")
    # Redis 초기화 (싱글톤)
    redis_mgr = RedisConnectionManager.get_instance()
    await redis_mgr.initialize()
    orchestrator = StreamOrchestrator()
    await orchestrator.startup()  # Producer 시작

    command_consumer = KafkaConsumerClient(
        orchestrator=orchestrator,
        topic=kafka_settings.STATUS_TOPIC,
    )
    disconnection_consumer = KafkaDisconnectionConsumerClient(
        orchestrator=orchestrator,
        topic=kafka_settings.DISCONNECTION_TOPIC,
    )

    tasks = [
        asyncio.create_task(
            command_consumer.run(),
            name="ws-command-consumer",
        ),
        asyncio.create_task(
            disconnection_consumer.run(),
            name="ws-disconnection-consumer",
        ),
    ]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        # 오케스트레이터의 실행 중 태스크를 먼저 정리하여 Redis 정리 로직이 실행되도록 함
        await orchestrator.shutdown()
        await redis_mgr.close()
        logger.info("프로그램 종료")


if __name__ == "__main__":
    try:
        # 비동기 실행
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램이 종료되었습니다.")

    # except Exception as e:
    #     logger.critical(f"예상치 못한 오류로 프로그램이 종료됨: {str(e)}")
