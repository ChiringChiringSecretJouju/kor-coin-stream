from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.parsers.base import parse_symbol
from src.infra.fx.fx_rate_service import FxRateService

USD_LIKE_QUOTES = {"USD", "USDT", "USDC", "BUSD"}


class KrwEnrichmentService:
    def __init__(self, fx_rate_service: FxRateService, enabled: bool = True) -> None:
        self._fx_rate_service = fx_rate_service
        self._enabled = enabled

    async def enrich_trade(self, payload: dict[str, Any], region: str) -> dict[str, Any]:
        if not self._enabled or region != "asia":
            return payload

        code = str(payload.get("code", ""))
        quote = _extract_quote_currency(code)
        if quote not in USD_LIKE_QUOTES:
            return payload

        fx_rate, fx_source, fx_ts = await self._fx_rate_service.get_usd_krw()
        enriched = dict(payload)
        enriched["trade_price_krw"] = _multiply(payload.get("trade_price"), fx_rate)
        enriched["trade_amount_krw"] = _multiply(payload.get("trade_amount"), fx_rate)
        enriched["fx_rate_usd_krw"] = fx_rate
        enriched["fx_rate_source"] = fx_source
        enriched["fx_rate_ts"] = fx_ts
        return enriched

    async def enrich_ticker(self, payload: dict[str, Any], region: str) -> dict[str, Any]:
        if not self._enabled or region != "asia":
            return payload

        code = str(payload.get("code", ""))
        quote = _extract_quote_currency(code)
        if quote not in USD_LIKE_QUOTES:
            return payload

        fx_rate, fx_source, fx_ts = await self._fx_rate_service.get_usd_krw()
        enriched = dict(payload)
        enriched["open_krw"] = _multiply(payload.get("open"), fx_rate)
        enriched["high_krw"] = _multiply(payload.get("high"), fx_rate)
        enriched["low_krw"] = _multiply(payload.get("low"), fx_rate)
        enriched["close_krw"] = _multiply(payload.get("close"), fx_rate)
        enriched["quote_volume_krw"] = _multiply(payload.get("quote_volume"), fx_rate)
        enriched["fx_rate_usd_krw"] = fx_rate
        enriched["fx_rate_source"] = fx_source
        enriched["fx_rate_ts"] = fx_ts
        return enriched


def _extract_quote_currency(code: str) -> str | None:
    if not code:
        return None
    _, quote = parse_symbol(code)
    return quote.upper() if isinstance(quote, str) and quote else None


def _multiply(value: Any, rate: float) -> float | None:
    if value is None:
        return None
    try:
        return float(value) * rate
    except (TypeError, ValueError):
        return None
