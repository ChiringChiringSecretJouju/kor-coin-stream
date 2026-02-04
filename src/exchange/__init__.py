from .asia import (
    BinanceWebsocketHandler,
    BybitWebsocketHandler,
    HuobiWebsocketHandler,
    OKXWebsocketHandler,
)
from .korea import (
    BithumbWebsocketHandler,
    CoinoneWebsocketHandler,
    KorbitWebsocketHandler,
    UpbitWebsocketHandler,
)

WKoreanExchangeHandler = (
    UpbitWebsocketHandler
    | KorbitWebsocketHandler
    | BithumbWebsocketHandler
    | CoinoneWebsocketHandler
)


WAsianExchangeHandler = (
    BinanceWebsocketHandler
    | BybitWebsocketHandler
    | HuobiWebsocketHandler
    | OKXWebsocketHandler
)


__all__ = [
    "WKoreanExchangeHandler",
    "UpbitWebsocketHandler",
    "KorbitWebsocketHandler",
    "BithumbWebsocketHandler",
    "CoinoneWebsocketHandler",
    "WAsianExchangeHandler",
    "BinanceWebsocketHandler",
    "BybitWebsocketHandler",
    "HuobiWebsocketHandler",
    "OKXWebsocketHandler",
]
