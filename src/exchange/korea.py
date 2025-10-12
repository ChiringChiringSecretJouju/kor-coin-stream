from typing import Any, override

from src.common.logger import PipelineLogger
from src.core.connection.handlers.korea_handler import BaseKoreaWebsocketHandler
from src.core.connection.utils.parse import preprocess_ticker_message, update_dict
from src.core.types import (
    TickerResponseData,
)

logger = PipelineLogger.get_logger("korea_handlers", "exchange")


class UpbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """업비트 거래소 웹소켓 핸들러 (배치 수집 지원)
    
    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(self, url: str, parameter_info: dict) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # 업비트 전용 전처리 (projection 필터링)
        preprocessed_message = preprocess_ticker_message(
            parsed_message, self.projection
        )

        # 공통 처리는 부모 클래스에 위임 (단, 이미 전처리된 메시지 전달)
        return await super().ticker_message(preprocessed_message)


class BithumbWebsocketHandler(BaseKoreaWebsocketHandler):
    """빗썸 거래소 웹소켓 핸들러 (배치 수집 지원)
    
    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(self, url: str, parameter_info: dict) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        # 빗썸 전용 전처리 (projection 필터링)
        preprocessed_message = preprocess_ticker_message(
            parsed_message, self.projection
        )

        # 공통 처리는 부모 클래스에 위임 (단, 이미 전처리된 메시지 전달)
        return await super().ticker_message(preprocessed_message)


class CoinoneWebsocketHandler(BaseKoreaWebsocketHandler):
    """코인원 거래소 웹소켓 핸들러 (배치 수집 지원)
    
    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(self, url: str, parameter_info: dict) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)
        if not parsed_message:
            return None

        data_sub: dict | None = parsed_message.get("data", None)
        if isinstance(data_sub, dict):
            # data 필드를 최상위로 병합
            merged_message = update_dict(parsed_message, "data")
        else:
            merged_message = parsed_message

        # 코인원 전용 전처리 (projection 필터링)
        preprocessed_message = preprocess_ticker_message(
            merged_message, self.projection
        )

        # 공통 처리는 부모 클래스에 위임 (단, 이미 전처리된 메시지 전달)
        return await super().ticker_message(preprocessed_message)


class KorbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """코빗 거래소 웹소켓 핸들러 (배치 수집 지원)
    
    Note:
        Heartbeat 설정은 YAML (config/settings.yaml)에서 주입됩니다.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # heartbeat는 DI Container에서 주입됨

    @override
    async def websocket_connection(self, url: str, parameter_info: dict) -> None:
        """웹소켓 연결 시 배치 시스템 초기화"""
        await self._initialize_batch_system()
        await super().websocket_connection(url, parameter_info)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)

        data_sub: dict | None = parsed_message.get("data", None)
        if isinstance(data_sub, dict):
            # data 필드를 최상위로 병합
            merged_message = update_dict(parsed_message, "data")

        # 코빗 전용 전처리 (projection 필터링)
        preprocessed_message = preprocess_ticker_message(
            merged_message, self.projection
        )

        # 공통 처리는 부모 클래스에 위임 (단, 이미 전처리된 메시지 전달)
        return await super().ticker_message(preprocessed_message)


