"""애플리케이션 진입점 (DI Container 기반)

암호화폐 거래소 웹소켓 스트림 파이프라인
- Kafka Consumer로 연결 지시 수신
- 실시간 데이터 스트림 처리
- Event Bus 기반 에러 처리 (EDA)

Usage:
    python main.py                   # 개발 환경
    ENV=prod python main.py          # 프로덕션 환경
"""

import asyncio
import contextlib
import os
from pathlib import Path

from src.common.events import ErrorEvent, EventBus
from src.common.exceptions.error_dispatcher import ErrorDispatcher
from src.common.logger import PipelineLogger
from src.config.containers import ApplicationContainer

logger = PipelineLogger.get_logger("main", "app")


class Application:
    """애플리케이션 메인 클래스

    책임:
    - DI Container 관리
    - Event Bus 리스너 등록
    - Consumer 태스크 실행
    - Graceful Shutdown
    """

    def __init__(self) -> None:
        self.container = ApplicationContainer()
        self.tasks: list[asyncio.Task] = []
        self.orchestrator = None
        self.status_consumer = None
        self.disconnection_consumer = None

    def _get_config_path(self) -> Path:
        """설정 파일 경로 결정 (환경별)"""
        env = os.getenv("ENV", "dev")
        config_file = Path(__file__).parent / f"config/{env}.yaml"

        if not config_file.exists():
            config_file = Path(__file__).parent / "config/settings.yaml"
            logger.warning(
                f"환경 설정 파일 {env}.yaml을 찾을 수 없어 settings.yaml을 사용합니다"
            )

        return config_file

    async def _setup_event_bus(self) -> None:
        """Event Bus 리스너 등록 (EDA 패턴)

        ErrorEvent → ErrorDispatcher:
        - 전략 기반 에러 처리
        - Circuit Breaker (Redis 분산)
        - DLQ 전송
        - 알람 발송
        - ws.error 토픽 발행
        """
        error_producer = await self.container.messaging.error_producer()
        error_dispatcher = ErrorDispatcher(error_producer=error_producer)

        async def handle_error_event(event: ErrorEvent) -> None:
            """에러 이벤트를 ErrorDispatcher로 처리"""
            try:
                await error_dispatcher.dispatch(
                    exc=event.exc,
                    kind=event.kind,
                    target=event.target,
                    context=event.context,
                )
            except Exception as e:
                logger.error(
                    f"ErrorDispatcher failed: {e}",
                    exc_info=True,
                    extra={"kind": event.kind, "original_error": str(event.exc)},
                )

        EventBus.on(ErrorEvent, handle_error_event)

    async def initialize(self) -> None:
        """애플리케이션 초기화

        Flow:
        1. 설정 로드
        2. Resource 초기화 (Redis, Kafka Producers)
        3. Event Bus 리스너 등록
        4. Orchestrator 및 Consumer 가져오기
        """
        # 설정 로드
        config_path = self._get_config_path()
        logger.info(f"설정 파일 로드: {config_path}")
        self.container.config.from_yaml(str(config_path))

        logger.info("암호화폐 거래소 웹소켓 스트림 파이프라인 시작 (DI 모드)")

        # Resource 초기화
        logger.info("Resource 초기화 시작...")
        await self.container.init_resources()
        logger.info("✅ 모든 Resource 초기화 완료")

        # Event Bus 리스너 등록
        await self._setup_event_bus()
        logger.info("✅ Event Bus 리스너 등록 완료")

        # Orchestrator 및 Consumer 가져오기
        self.orchestrator = await self.container.orchestrator()
        self.status_consumer = await self.container.status_consumer()
        self.disconnection_consumer = await self.container.disconnection_consumer()
        logger.info("✅ Orchestrator 및 Consumer 준비 완료")

    async def run(self) -> None:
        """Consumer 태스크 실행 (메인 루프)"""
        self.tasks = [
            asyncio.create_task(self.status_consumer.run(), name="ws-status-consumer"),
            asyncio.create_task(
                self.disconnection_consumer.run(), name="ws-disconnection-consumer"
            ),
        ]

        logger.info(f"✅ {len(self.tasks)}개 Consumer 태스크 실행 중...")
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        """Graceful Shutdown

        Flow:
        1. 태스크 취소
        2. 태스크 완료 대기
        3. Orchestrator 정리
        4. Resource 정리 (Redis, Kafka Producers)
        """
        logger.info("정리 작업 시작...")

        # 태스크 취소
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # 태스크 완료 대기
        for task in self.tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Orchestrator 정리
        if self.orchestrator:
            await self.orchestrator.shutdown()

        # Resource 정리
        logger.info("모든 Resource 종료 중...")
        await self.container.shutdown_resources()
        logger.info("✅ 프로그램 종료 완료")


async def main() -> None:
    """메인 실행 함수"""
    app = Application()

    try:
        await app.initialize()
        await app.run()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다")
    finally:
        await app.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램이 종료되었습니다.")
