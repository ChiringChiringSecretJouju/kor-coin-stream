from core.connection._base_handler import BaseKoreaWebsocketHandler
from typing import Any


class UpbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """업비트 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.set_heartbeat(kind="frame")


class BithumbWebsocketHandler(BaseKoreaWebsocketHandler):
    """빗썸 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.set_heartbeat(kind="frame")


class CoinoneWebsocketHandler(BaseKoreaWebsocketHandler):
    """코인원 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.set_heartbeat(kind="text", message='{"request_type":"PING"}')


class KorbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """코빗 거래소 웹소켓 핸들러"""

    pass
