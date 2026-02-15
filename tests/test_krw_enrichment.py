from __future__ import annotations

import pytest

from src.core.connection.services.krw_enrichment import KrwEnrichmentService
from src.infra.fx.fx_rate_service import FxRateService


class _StubFxService:
    async def get_usd_krw(self) -> tuple[float, str, float]:
        return 1300.0, "stub", 1000.0


@pytest.mark.asyncio
async def test_fx_rate_service_returns_fallback_when_disabled() -> None:
    service = FxRateService(enabled=False, fallback_usd_krw=1388.0)

    rate, source, _ = await service.get_usd_krw()

    assert rate == 1388.0
    assert source == "disabled"


@pytest.mark.asyncio
async def test_fx_rate_service_uses_cache_between_calls() -> None:
    service = FxRateService(enabled=True, ttl_sec=120, fallback_usd_krw=1350.0)
    call_count = 0

    def _fetch() -> float:
        nonlocal call_count
        call_count += 1
        return 1333.3

    service._fetch_usd_krw_sync = _fetch  # type: ignore[method-assign]

    first = await service.get_usd_krw()
    second = await service.get_usd_krw()

    assert first[0] == 1333.3
    assert second[0] == 1333.3
    assert call_count == 1


@pytest.mark.asyncio
async def test_fx_rate_service_uses_stale_cache_on_refresh_failure() -> None:
    service = FxRateService(enabled=True, ttl_sec=1, fallback_usd_krw=1350.0)
    service._cached_rate = 1311.0
    service._cached_at = 1.0

    def _raise_error() -> float:
        raise RuntimeError("fx upstream down")

    service._fetch_usd_krw_sync = _raise_error  # type: ignore[method-assign]

    rate, source, ts = await service.get_usd_krw()

    assert rate == 1311.0
    assert source == "stale_cache"
    assert ts == 1.0


@pytest.mark.asyncio
async def test_krw_enrichment_adds_trade_fields_for_asia_usd_like_quotes() -> None:
    enrichment = KrwEnrichmentService(fx_rate_service=_StubFxService(), enabled=True)
    payload = {
        "code": "BTC-USDT",
        "trade_price": 100.0,
        "trade_amount": 2.0,
    }

    enriched = await enrichment.enrich_trade(payload, region="asia")

    assert enriched["trade_price_krw"] == 130000.0
    assert enriched["trade_amount_krw"] == 2600.0
    assert enriched["fx_rate_usd_krw"] == 1300.0
    assert enriched["fx_rate_source"] == "stub"
    assert enriched["fx_rate_ts"] == 1000.0


@pytest.mark.asyncio
async def test_krw_enrichment_skips_non_asia_or_non_usd_quotes() -> None:
    enrichment = KrwEnrichmentService(fx_rate_service=_StubFxService(), enabled=True)

    korea_payload = {"code": "KRW-BTC", "trade_price": 100.0, "trade_amount": 2.0}
    asia_non_usd_payload = {"code": "SOL-BTC", "trade_price": 10.0, "trade_amount": 1.0}

    korea_result = await enrichment.enrich_trade(korea_payload, region="korea")
    asia_non_usd_result = await enrichment.enrich_trade(asia_non_usd_payload, region="asia")

    assert korea_result == korea_payload
    assert asia_non_usd_result == asia_non_usd_payload
