from __future__ import annotations

import asyncio
import time

from forex_python.converter import CurrencyRates

from src.common.logger import PipelineLogger

logger = PipelineLogger.get_logger("fx_rate_service", "infra")


class FxRateService:
    """USD/KRW 환율 조회 + TTL 캐시 서비스."""

    def __init__(
        self,
        enabled: bool = True,
        ttl_sec: int = 60,
        fallback_usd_krw: float = 1350.0,
        timeout_sec: float = 2.5,
    ) -> None:
        self._enabled = enabled
        self._ttl_sec = max(1, ttl_sec)
        self._fallback_rate = fallback_usd_krw
        self._timeout_sec = max(0.1, timeout_sec)

        self._currency_rates = CurrencyRates(force_decimal=False)
        self._cached_rate: float | None = None
        self._cached_at: float = 0.0
        self._last_warn_at: float = 0.0
        self._lock = asyncio.Lock()

    async def get_usd_krw(self) -> tuple[float, str, float]:
        now = time.time()
        if not self._enabled:
            return self._fallback_rate, "disabled", now

        if self._cached_rate is not None and now - self._cached_at <= self._ttl_sec:
            return self._cached_rate, "cache", self._cached_at

        async with self._lock:
            now = time.time()
            if self._cached_rate is not None and now - self._cached_at <= self._ttl_sec:
                return self._cached_rate, "cache", self._cached_at

            try:
                rate = await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_usd_krw_sync),
                    timeout=self._timeout_sec,
                )
            except Exception as exc:
                return self._fallback_on_failure(exc)

            if rate <= 0:
                return self._fallback_on_failure(ValueError(f"invalid fx rate: {rate}"))

            self._cached_rate = rate
            self._cached_at = now
            return rate, "forex-python", now

    def _fetch_usd_krw_sync(self) -> float:
        return float(self._currency_rates.get_rate("USD", "KRW"))

    def _fallback_on_failure(self, exc: Exception) -> tuple[float, str, float]:
        now = time.time()

        if now - self._last_warn_at >= 30:
            logger.warning(
                "USD/KRW fetch failed",
                error=str(exc),
                fallback=self._fallback_rate,
            )
            self._last_warn_at = now

        if self._cached_rate is not None:
            return self._cached_rate, "stale_cache", self._cached_at

        return self._fallback_rate, "fallback", now
