import gzip
from typing import Any, override

import orjson

from src.core.connection.handlers.global_handler import BaseGlobalWebsocketHandler
from src.core.connection.utils.parse import preprocess_ticker_message, update_dict
from src.core.types import TickerResponseData


class BinanceWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이낸스 거래소 웹소켓 핸들러 (배치 수집 지원)
    
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
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)

        # 전처리된 메시지 생성 (Binance는 플랫 구조)
        if isinstance(parsed_message, dict):
            preprocessed_message = preprocess_ticker_message(
                parsed_message, self.projection
            )
            return await super().ticker_message(preprocessed_message)

        return await super().ticker_message(message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()


class BybitWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이비트 거래소 웹소켓 핸들러 (배치 수집 지원)
    
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
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)

        # data 필드 병합 (Bybit은 중첩 구조 + 중복 필드)
        if isinstance(parsed_message, dict):
            data_sub: dict | None = parsed_message.get("data", None)
            if isinstance(data_sub, dict):
                # data 필드를 최상위로 병합
                merged_message = update_dict(parsed_message, "data")
            else:
                merged_message = parsed_message

            preprocessed_message = preprocess_ticker_message(
                merged_message, self.projection
            )
            return await super().ticker_message(preprocessed_message)

        return await super().ticker_message(message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()


class HuobiWebsocketHandler(BaseGlobalWebsocketHandler):
    """후오비(HTX) 거래소 웹소켓 핸들러 (배치 수집 지원)
    
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
    def _parse_message(self, message: str | bytes) -> dict[str, Any]:
        """Huobi 메시지 파싱 - GZIP 압축 해제 포함"""
        try:
            # bytes인 경우 GZIP 압축 해제 시도
            if isinstance(message, bytes):
                # GZIP 매직 넘버 확인 (0x1f, 0x8b)
                if len(message) >= 2 and message[0] == 0x1F and message[1] == 0x8B:
                    # GZIP 압축된 데이터 해제
                    decompressed = gzip.decompress(message)
                    message = decompressed.decode("utf-8")
                else:
                    # 일반 bytes 데이터
                    message = message.decode("utf-8")

            # JSON 파싱
            if isinstance(message, str):
                return orjson.loads(message)

            return message
        except (gzip.BadGzipFile, UnicodeDecodeError, orjson.JSONDecodeError) as e:
            # 압축 해제 또는 파싱 실패 시 원본 메시지 반환
            self._log_status(f"message_parse_error: {e}")
            return {"error": f"Failed to parse message: {e}", "raw": str(message)}

    @override
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)

        # ping/pong 처리 (Huobi 특화)
        if isinstance(parsed_message, dict) and "ping" in parsed_message:
            ping_value = parsed_message["ping"]
            pong_response = orjson.dumps({"pong": ping_value}).decode("utf-8")
            await self._current_websocket.send(pong_response)
            return None  # ping 메시지는 수집하지 않음

        # tick 필드 병합 + symbol 추출 (Huobi는 tick 중첩 구조)
        if isinstance(parsed_message, dict):
            tick_data: dict | None = parsed_message.get("tick", None)
            if isinstance(tick_data, dict):
                # tick 필드를 최상위로 병합
                merged_message = update_dict(parsed_message, "tick")
            else:
                merged_message = parsed_message

            # ch 필드에서 symbol 추출 (예: "market.btcusdt.ticker" → "BTCUSDT")
            ch_field = merged_message.get("ch", "")
            if (
                isinstance(ch_field, str)
                and ch_field.startswith("market.")
                and ch_field.endswith(".ticker")
            ):
                # "market.btcusdt.ticker"에서 "btcusdt" 추출 후 대문자로 변환
                symbol_part = ch_field.split(".")[1]  # "btcusdt"
                merged_message["symbol"] = symbol_part.upper()  # "BTCUSDT"

            # 전처리된 메시지 생성 (projection 적용)
            preprocessed_message = preprocess_ticker_message(
                merged_message, self.projection
            )
            return await super().ticker_message(preprocessed_message)

        return await super().ticker_message(message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()


class OKXWebsocketHandler(BaseGlobalWebsocketHandler):
    """오케이엑스 거래소 웹소켓 핸들러 (배치 수집 지원)
    
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
    async def ticker_message(self, message: Any) -> TickerResponseData | None:
        parsed_message = self._parse_message(message)

        # data 배열 병합 (OKX는 data 배열 구조)
        if isinstance(parsed_message, dict):
            data_list = parsed_message.get("data", [])
            if isinstance(data_list, list) and len(data_list) > 0:
                # 첫 번째 데이터 항목을 최상위로 병합
                first_data = data_list[0]
                if isinstance(first_data, dict):
                    merged_message = {**parsed_message, **first_data}
                    merged_message.pop("data", None)  # 원본 data 필드 제거
                else:
                    merged_message = parsed_message
            else:
                merged_message = parsed_message

            # 전처리된 메시지 생성 (projection 적용)
            preprocessed_message = preprocess_ticker_message(
                merged_message, self.projection
            )
            return await super().ticker_message(preprocessed_message)

        return await super().ticker_message(message)

    @override
    async def disconnect(self) -> None:
        """연결 종료 시 배치 시스템 정리"""
        await self._cleanup_batch_system()
        await super().disconnect()


