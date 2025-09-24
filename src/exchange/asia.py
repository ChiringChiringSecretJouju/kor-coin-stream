from src.core.connection.handlers.global_handler import BaseGlobalWebsocketHandler
from src.core.types import TickerResponseData
from typing import Any, override
import gzip
import orjson


class BinanceWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이낸스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Binance는 일반적으로 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")


class BybitWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이비트 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Bybit은 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message='{"op":"ping"}')


class GateioWebsocketHandler(BaseGlobalWebsocketHandler):
    """게이트아이오 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Gate.io는 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message='{"method": "ping"}')


class HuobiWebsocketHandler(BaseGlobalWebsocketHandler):
    """후오비(HTX) 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Huobi는 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message='{"ping": 1}')

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

        if "ping" in parsed_message:
            ping_value = parsed_message["ping"]
            pong_response = orjson.dumps({"pong": ping_value}).decode("utf-8")
            await self._current_websocket.send(pong_response)
            return None  # 출력하지 않음

        return await super().ticker_message(message)


class OKXWebsocketHandler(BaseGlobalWebsocketHandler):
    """오케이엑스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # OKX는 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message="ping")
