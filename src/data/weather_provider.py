"""Data provider abstractions for weather strategy.

Separates external data dependencies (Open-Meteo, Gamma API) from the
strategy core, enabling:

- Live trading: real API calls
- Backtesting: replay from recorded historical data
- Paper simulation: dummy providers for testing

Providers are injected into WeatherStrategy via __init__.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any, Optional

import requests

from src.data.weather import EnsembleForecast, fetch_ensemble_forecast

logger = logging.getLogger("nachomarket.data.provider")


# ---------------------------------------------------------------------------
# ABCs
# ---------------------------------------------------------------------------


class ForecastProvider(ABC):
    """Abstract forecast data source."""

    @abstractmethod
    def get_forecast(
        self, city_name: str, target_date: date
    ) -> Optional[EnsembleForecast]:
        ...


class MarketProvider(ABC):
    """Abstract market data source (Polymarket Gamma API)."""

    @abstractmethod
    def get_events_by_slug(self, slug: str) -> Optional[list[dict]]:
        ...

    @abstractmethod
    def get_market(self, market_id: str) -> Optional[dict]:
        ...

    @abstractmethod
    def get_market_price(self, market_id: str) -> float:
        ...


# ---------------------------------------------------------------------------
# Live providers (real API calls)
# ---------------------------------------------------------------------------


class LiveForecastProvider(ForecastProvider):
    """Real-time Open-Meteo ensemble forecasts."""

    def get_forecast(
        self, city_name: str, target_date: date
    ) -> Optional[EnsembleForecast]:
        return fetch_ensemble_forecast(city_name, target_date)


class LiveMarketProvider(MarketProvider):
    """Real-time Polymarket Gamma API."""

    GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

    def get_events_by_slug(self, slug: str) -> Optional[list[dict]]:
        try:
            r = requests.get(
                self.GAMMA_EVENTS_URL,
                params={"slug": slug},
                timeout=10.0,
            )
            if r.status_code != 200 or not r.text.strip():
                return None
            data = r.json()
            if not data:
                return None
            return data if isinstance(data, list) else [data]
        except Exception:
            return None

    def get_market(self, market_id: str) -> Optional[dict]:
        try:
            r = requests.get(
                f"https://gamma-api.polymarket.com/markets/{market_id}",
                timeout=10.0,
            )
            if r.status_code != 200:
                return None
            return r.json()
        except Exception:
            return None

    def get_market_price(self, market_id: str) -> Optional[float]:
        market = self.get_market(market_id)
        if market is None:
            return None
        prices = market.get("outcomePrices", [])
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []
        if prices and len(prices) >= 1:
            p = float(prices[0])
            if 0.01 <= p <= 0.99:
                return p
        return None


# ---------------------------------------------------------------------------
# Historical providers (for backtesting)
# ---------------------------------------------------------------------------


class HistoricalForecastProvider(ForecastProvider):
    """Replays pre-recorded forecasts from disk.

    Expects directory structure:
        data/historical/{date}/forecasts/{city_key}_{target_date}.json
    """

    def __init__(self, data_dir: str = "data/historical") -> None:
        self._data_dir = Path(data_dir)
        self._cache: dict[str, Optional[EnsembleForecast]] = {}

    def get_forecast(
        self, city_name: str, target_date: date
    ) -> Optional[EnsembleForecast]:
        today = date.today()
        cache_key = f"{city_name}_{target_date.isoformat()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Try multiple date directories (recordings may be from different days)
        for days_back in range(3):
            recording_date = today
            try:
                from datetime import timedelta
                recording_date = today - timedelta(days=days_back)
            except Exception:
                pass
            path = (
                self._data_dir
                / recording_date.isoformat()
                / "forecasts"
                / f"{city_name.lower()}_{target_date.isoformat()}.json"
            )
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    fc = EnsembleForecast(
                        city_key=data.get("city_key", city_name),
                        city_name=data.get("city_name", city_name),
                        target_date=target_date,
                        member_highs=data.get("member_highs", []),
                        member_lows=data.get("member_lows", []),
                    )
                    self._cache[cache_key] = fc
                    return fc
                except Exception:
                    pass

        self._cache[cache_key] = None
        return None


class HistoricalMarketProvider(MarketProvider):
    """Replays pre-recorded market snapshots from disk.

    Expects directory structure:
        data/historical/{date}/markets/{slug}.json
    """

    def __init__(self, data_dir: str = "data/historical") -> None:
        self._data_dir = Path(data_dir)
        self._market_cache: dict[str, Optional[list[dict]]] = {}
        self._price_cache: dict[str, float] = {}

    def get_events_by_slug(self, slug: str) -> Optional[list[dict]]:
        if slug in self._market_cache:
            return self._market_cache[slug]

        today = date.today()
        from datetime import timedelta

        for days_back in range(3):
            recording_date = today - timedelta(days=days_back)
            path = (
                self._data_dir
                / recording_date.isoformat()
                / "markets"
                / f"{slug}.json"
            )
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    result = data if isinstance(data, list) else [data]
                    self._market_cache[slug] = result
                    return result
                except Exception:
                    pass

        self._market_cache[slug] = None
        return None

    def get_market(self, market_id: str) -> Optional[dict]:
        # Walk all cached market snapshots to find this market_id
        for events in self._market_cache.values():
            if events is None:
                continue
            for event in events:
                for mkt in event.get("markets", []):
                    if mkt.get("id") == market_id:
                        return mkt
        return None

    def get_market_price(self, market_id: str) -> Optional[float]:
        if market_id in self._price_cache:
            return self._price_cache[market_id]
        market = self.get_market(market_id)
        if market is None:
            return None
        prices = market.get("outcomePrices", [])
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []
        p = float(prices[0]) if prices else None
        if p is not None:
            p = max(0.01, min(0.99, p))
            self._price_cache[market_id] = p
        return p
