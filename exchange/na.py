from core.connection._base_handler import BaseGlobalWebsocketHandler
from typing import Any, override


class CoinbaseWebsocketHandler(BaseGlobalWebsocketHandler):
    """코인베이스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Coinbase는 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")


class KrakenWebsocketHandler(BaseGlobalWebsocketHandler):
    """크라켄 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Kraken은 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")

    @override
    async def ticker_message(self, message: Any) -> Any | None:
        """티커 메시지 처리 함수 - Kraken 특화 (heartbeat 필터링)"""
        parsed_message = self._parse_message(message)

        # Kraken heartbeat 메시지 필터링
        if parsed_message.get("channel") == "heartbeat":
            return None  # heartbeat 메시지는 무시

        # 구독 성공 응답 처리
        if (
            parsed_message.get("method") == "subscribe"
            and parsed_message.get("success") is True
        ):
            self._log_status("subscribed")
            return None

        # 일반적인 글로벌 핸들러 로직 수행
        return await super().ticker_message(message)
