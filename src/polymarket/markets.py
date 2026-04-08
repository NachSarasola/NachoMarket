import logging
from typing import Any

from src.polymarket.client import PolymarketClient

logger = logging.getLogger("nachomarket.markets")


class MarketAnalyzer:
    """Seleccion y analisis de mercados para operar."""

    def __init__(self, client: PolymarketClient, config: dict[str, Any]) -> None:
        self._client = client
        self._min_volume = config.get("min_daily_volume_usd", 10000)
        self._max_markets = config.get("max_markets_simultaneous", 5)
        self._min_liquidity = config.get("filters", {}).get("min_liquidity_usd", 5000)
        self._max_spread = config.get("filters", {}).get("max_spread_pct", 5.0)
        self._min_time_to_resolution = config.get("filters", {}).get("min_time_to_resolution_hours", 24)

    def scan_markets(self) -> list[dict[str, Any]]:
        """Escanea y filtra mercados segun criterios de config."""
        all_markets = self._client.get_markets()
        eligible = []

        for market in all_markets:
            if self._passes_filters(market):
                score = self._score_market(market)
                market["_score"] = score
                eligible.append(market)

        eligible.sort(key=lambda m: m["_score"], reverse=True)
        selected = eligible[: self._max_markets]

        logger.info(
            f"Market scan: {len(all_markets)} total, {len(eligible)} eligible, "
            f"{len(selected)} selected"
        )
        return selected

    def _passes_filters(self, market: dict[str, Any]) -> bool:
        """Verifica si un mercado pasa todos los filtros."""
        volume = market.get("volume", 0)
        if volume < self._min_volume:
            return False

        liquidity = market.get("liquidity", 0)
        if liquidity < self._min_liquidity:
            return False

        spread = market.get("spread", 100)
        if spread > self._max_spread:
            return False

        return True

    def _score_market(self, market: dict[str, Any]) -> float:
        """Calcula un score de atractivo para un mercado."""
        volume_score = min(market.get("volume", 0) / 100000, 1.0)
        liquidity_score = min(market.get("liquidity", 0) / 50000, 1.0)
        spread_score = max(0, 1 - market.get("spread", 5) / 10)

        return volume_score * 0.4 + liquidity_score * 0.3 + spread_score * 0.3

    def get_multi_outcome_markets(self) -> list[dict[str, Any]]:
        """Encuentra mercados multi-outcome para arbitraje."""
        markets = self.scan_markets()
        multi = [m for m in markets if len(m.get("tokens", [])) > 2]
        logger.info(f"Found {len(multi)} multi-outcome markets")
        return multi

    def calculate_spread(self, token_id: str) -> float | None:
        """Calcula el spread actual de un mercado en porcentaje."""
        book = self._client.get_orderbook(token_id)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        if best_bid == 0:
            return None
        return ((best_ask - best_bid) / best_bid) * 100
