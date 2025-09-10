"""
통합 거래소 웹소켓 스트림 파이프라인

여러 한국 암호화폐 거래소에 병렬로 웹소켓 연결하여 실시간 데이터를 수신합니다.
구성 정보는 통합 JSON 형식으로 제공되며,

사용법:
    python main.py                   # Kafka 토픽을 소비하여 연결 지시 처리
    python main.py --exchange upbit  # 특정 거래소만 연결 (기존 모드, 선택)
"""

import asyncio

from common.logger import PipelineLogger
from config.settings import kafka_settings
from infra.messaging.connect.consumer_client import KafkaConsumerClient
from infra.cache.cache_client import RedisConnectionManager
from application.orchestrator import StreamOrchestrator

logger = PipelineLogger.get_logger("main", "app")


async def main():
    """메인 실행 함수"""
    logger.info("암호화폐 거래소 웹소켓 스트림 파이프라인 시작 (Kafka 소비 모드)")
    # Redis 초기화 (싱글톤)
    redis_mgr = RedisConnectionManager.get_instance()
    await redis_mgr.initialize()
    orchestrator = StreamOrchestrator()
    consumer = KafkaConsumerClient(
        orchestrator=orchestrator,
        topic=kafka_settings.STATUS_TOPIC,
    )
    try:
        await consumer.run()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다")
    finally:
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
