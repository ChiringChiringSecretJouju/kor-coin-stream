from __future__ import annotations

import asyncio
import logging
import queue
import sys
from datetime import datetime
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
from pathlib import Path
from typing import Any


def ensure_file_exists(file_path: str) -> None:
    """
    주어진 파일 경로의 상위 폴더가 없으면 생성합니다.
    (파일 자체는 핸들러가 생성하도록 두어 불필요한 I/O를 줄입니다.)

    Args:
        file_path (str): 파일 경로
    """
    path = Path(file_path)

    # 파일이 위치할 폴더 경로
    folder_path = path.parent

    # 폴더가 존재하지 않으면 폴더 생성
    if not folder_path.exists():
        folder_path.mkdir(parents=True, exist_ok=True)
    # 파일은 핸들러가 생성합니다.


class PipelineLogger:
    """
    파이프라인 아키텍처에 최적화된 로깅 시스템
    비동기 처리, 컴포넌트별 로깅, 성능 모니터링 기능 제공
    """

    _default_level = logging.INFO

    @classmethod
    def get_logger(cls, name: str, component: str | None = None, **kwargs) -> PipelineLogger:
        """
        로거 인스턴스를 반환하는 간단한 팩토리 메서드.
        표준 logging.getLogger가 이름 단위로 사실상 싱글톤이므로
        별도 레지스트리 없이 인스턴스를 생성합니다.
        """
        return cls(name, component, **kwargs)

    def __init__(
        self,
        name: str,
        component: str | None = None,
        level: int | None = None,
        log_to_file: bool = True,
        log_to_console: bool = True,
        log_dir: str = "logs",
        rotation: str = "midnight",
    ):
        """
        로거 초기화

        Args:
            name: 로거 이름
            component: 컴포넌트 이름
            level: 로깅 레벨
            log_to_file: 파일에 로깅 여부
            log_to_console: 콘솔에 로깅 여부
            log_dir: 로그 디렉토리
            rotation: 로그 로테이션 주기
        """
        self.name = name
        self.component = component
        self.level = level or self._default_level
        self.log_to_file = log_to_file
        self.log_to_console = log_to_console
        self.log_dir = log_dir
        self.rotation = rotation

        # 로깅 큐 및 컨텍스트 초기화 (무제한 버퍼로 설정해 queue.Full 예외 방지)
        self.log_queue: queue.Queue = queue.Queue()  # unlimited buffer
        self.context: dict[str, Any] = {}

        # 로거 및 핸들러 설정
        self._setup_logger()

        # 비동기 이벤트 루프 참조 (필요시 설정)
        self._loop: asyncio.AbstractEventLoop | None = None

    def _setup_logger(self) -> None:
        """
        로거, 핸들러, 포맷터 설정
        """
        self.logger_name = f"{self.name}.{self.component}" if self.component else self.name
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(self.level)

        # 기존 핸들러 제거
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # 포맷터 설정 (간결화)
        self.formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s [%(component)s] %(message)s"
        )

        # 핸들러 설정
        handlers: list[logging.Handler] = []

        if self.log_to_console:
            console = logging.StreamHandler(sys.stdout)
            console.setFormatter(self.formatter)
            handlers.append(console)

        if self.log_to_file:
            log_filename = self._get_log_filename()
            # 디렉터리만 생성하고 파일 생성은 핸들러에 위임
            Path(log_filename).parent.mkdir(parents=True, exist_ok=True)

            file_handler = TimedRotatingFileHandler(
                filename=log_filename,
                when=self.rotation,
                backupCount=7,  # 7일치 로그 유지
            )
            file_handler.setFormatter(self.formatter)
            handlers.append(file_handler)

        # 큐 핸들러 및 리스너 설정
        self.queue_handler = QueueHandler(self.log_queue)
        self.logger.addHandler(self.queue_handler)

        self.listener = QueueListener(self.log_queue, *handlers, respect_handler_level=True)
        self.listener.start()

    def _get_log_filename(self) -> str:
        """
        로그 파일 이름 생성
        """
        today = datetime.now().strftime("%Y-%m-%d")
        component_part = f"{self.component}/" if self.component else ""
        path = f"{self.log_dir}"
        return f"{path}/{component_part}{self.name}_{today}.log"

    def set_context(self, **kwargs) -> None:
        """
        로깅 컨텍스트 설정
        """
        self.context.update(kwargs)

    def clear_context(self) -> None:
        """
        로깅 컨텍스트 초기화
        """
        self.context.clear()

    def _process_message(self, level: int, msg: str, extra: dict[str, Any] | None = None) -> None:
        """
        메시지 처리 및 로깅
        """
        log_extra = {"component": self.component or "main", "exchange": "global"}

        # exc_info, stack_info는 logger.log()의 파라미터로 추출
        exc_info_param = None
        stack_info_param = False

        if extra:
            # logging 파라미터 추출 (exc_info, stack_info)
            exc_info_param = extra.pop("exc_info", None)
            stack_info_param = bool(extra.pop("stack_info", False))

            # 'extra' 키가 있으면 그 내용을 풀어서 병합
            if "extra" in extra:
                nested_extra = extra.pop("extra")
                if isinstance(nested_extra, dict):
                    log_extra.update(nested_extra)

            # 나머지 컨텍스트 병합
            if self.context:
                log_extra.update(self.context)

            # 남은 extra 병합
            log_extra.update(extra)
        elif self.context:
            log_extra.update(self.context)

        # exc_info, stack_info를 파라미터로 전달
        self.logger.log(
            level, msg, exc_info=exc_info_param, stack_info=stack_info_param, extra=log_extra
        )

    async def alog(self, level: int, msg: str, **kwargs) -> None:
        """
        비동기적으로 로그 메시지 기록
        - 실행 중인 이벤트 루프가 있으면 스레드 풀로 위임
        - 루프가 없으면 동기 처리(로그는 QueueHandler로 빠르게 반환)
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 이벤트 루프가 없는 환경에서는 동기 처리 (큐 기반이라 지연이 매우 작음)
            self._process_message(level, msg, kwargs)
            return
        await loop.run_in_executor(None, self._process_message, level, msg, kwargs)

    def debug(self, msg: str, **kwargs) -> None:
        self._process_message(logging.DEBUG, msg, kwargs)

    def info(self, msg: str, **kwargs) -> None:
        self._process_message(logging.INFO, msg, kwargs)

    def warning(self, msg: str, **kwargs) -> None:
        self._process_message(logging.WARNING, msg, kwargs)

    def error(self, msg: str, **kwargs) -> None:
        self._process_message(logging.ERROR, msg, kwargs)

    def critical(self, msg: str, **kwargs) -> None:
        self._process_message(logging.CRITICAL, msg, kwargs)

    async def adebug(self, msg: str, **kwargs) -> None:
        await self.alog(logging.DEBUG, msg, **kwargs)

    async def ainfo(self, msg: str, **kwargs) -> None:
        await self.alog(logging.INFO, msg, **kwargs)

    async def awarning(self, msg: str, **kwargs) -> None:
        await self.alog(logging.WARNING, msg, **kwargs)

    async def aerror(self, msg: str, **kwargs) -> None:
        await self.alog(logging.ERROR, msg, **kwargs)

    async def acritical(self, msg: str, **kwargs) -> None:
        await self.alog(logging.CRITICAL, msg, **kwargs)

    def close(self) -> None:
        """
        리소스 정리
        """
        self.listener.stop()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
