from .korea import (
    BithumbWebsocketHandler,
    CoinoneWebsocketHandler,
    KorbitWebsocketHandler,
    UpbitWebsocketHandler,
)

WorldWebSocket = (
    UpbitWebsocketHandler
    | KorbitWebsocketHandler
    | BithumbWebsocketHandler
    | CoinoneWebsocketHandler
)

__all__ = [
    "WorldWebSocket",
    "UpbitWebsocketHandler",
    "KorbitWebsocketHandler",
    "BithumbWebsocketHandler",
    "CoinoneWebsocketHandler",
]
