"""Kalshi Weather Temperature Trading Strategy.

Usa Open-Meteo Ensemble API (GFS + ECMWF) para estimar probabilidades
de temperatura en contratos binarios de Kalshi (series KXHIGH*, KXLOW*).

Estrategia: comprar YES cuando el ensemble dice que la probabilidad
es mayor que el precio de mercado. Hold hasta resolucion (24-48h).

Contratos: binarios a thresholds especificos (ej: "high >84F on May 5").
El ensemble da P(temp > threshold) y comparamos con el precio de Kalshi.
"""

from __future__ import annotations

import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from src.data.weather import (
    STATIONS,
    MONTH_MAP,
    fetch_ensemble_forecast,
    resolve_station,
)
from src.kalshi.client import KalshiClient
from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.weather")

# Series de Kalshi para temperatura diaria (high y low)
KALSHI_SERIES = [
    # High temperature (USA)
    "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLAX", "KXHIGHDEN",
    "KXHIGHHOU", "KXHIGHOU", "KXHOUHIGH", "KXHIGHTBOS", "KXHIGHAUS",
    "KXHIGHTPHX", "KXHIGHTLV", "KXHIGHTDAL", "KXHIGHTMIN", "KXHIGHPHIL",
    "KXHIGHTSEA", "KXHIGHTSFO", "KXHIGHTDC", "KXHIGHTSATX", "KXHIGHTATL",
    "KXHIGHTNOLA", "KXHIGHTOKC",
    # Low temperature (USA)
    "KXLOWNY", "KXLOWTNYC", "KXLOWTCHI", "KXLOWTMIA", "KXLOWTLAX",
    "KXLOWTDEN", "KXLOWTHOU", "KXLOWTBOS", "KXLOWTAUS", "KXLOWTPHX",
    "KXLOWTLV", "KXLOWTDAL", "KXLOWTMIN", "KXLOWTPHIL", "KXLOWTSEA",
    "KXLOWTSFO", "KXLOWTDC", "KXLOWTSATX", "KXLOWTATL", "KXLOWTNOLA",
    "KXLOWTOKC",
]

# Mapping de series Kalshi -> ciudad
SERIES_CITY_MAP = {
    "KXHIGHNY": "New York", "KXLOWNY": "New York", "KXLOWTNYC": "New York",
    "KXHIGHCHI": "Chicago", "KXLOWTCHI": "Chicago",
    "KXHIGHMIA": "Miami", "KXLOWTMIA": "Miami",
    "KXHIGHLAX": "Los Angeles", "KXLOWTLAX": "Los Angeles",
    "KXHIGHDEN": "Denver", "KXLOWTDEN": "Denver",
    "KXHIGHHOU": "Houston", "KXHIGHOU": "Houston", "KXHOUHIGH": "Houston", "KXLOWTHOU": "Houston",
    "KXHIGHTBOS": "Boston", "KXLOWTBOS": "Boston",
    "KXHIGHAUS": "Austin", "KXLOWTAUS": "Austin",
    "KXHIGHTPHX": "Phoenix", "KXLOWTPHX": "Phoenix",
    "KXHIGHTLV": "Las Vegas", "KXLOWTLV": "Las Vegas",
    "KXHIGHTDAL": "Dallas", "KXLOWTDAL": "Dallas",
    "KXHIGHTMIN": "Minneapolis", "KXLOWTMIN": "Minneapolis",
    "KXHIGHPHIL": "Philadelphia", "KXLOWTPHIL": "Philadelphia",
    "KXHIGHTSEA": "Seattle", "KXLOWTSEA": "Seattle",
    "KXHIGHTSFO": "San Francisco", "KXLOWTSFO": "San Francisco",
    "KXHIGHTDC": "Washington DC", "KXLOWTDC": "Washington DC",
    "KXHIGHTSATX": "San Antonio", "KXLOWTSATX": "San Antonio",
    "KXHIGHTATL": "Atlanta", "KXLOWTATL": "Atlanta",
    "KXHIGHTNOLA": "New Orleans", "KXLOWTNOLA": "New Orleans",
    "KXHIGHTOKC": "Oklahoma City", "KXLOWTOKC": "Oklahoma City",
}


@dataclass
class KalshiWeatherContract:
    """Contrato binario de temperatura en Kalshi."""
    ticker: str
    series_ticker: str
    city_name: str
    target_date: date
    threshold_f: float
    metric: str          # "high" or "low"
    direction: str       # "above" or "below"
    yes_bid: float       # en dolares (0-1)
    yes_ask: float
    last_price: float
    volume: float


class WeatherStrategy(BaseStrategy):
    """Estrategia de trading en temperatura via Kalshi con ensemble forecasting."""

    def __init__(
        self,
        client: Any,
        config: dict[str, Any],
        position_sizer: Any = None,
        circuit_breaker: Any = None,
        inventory: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__("weather", client, config, **kwargs)
        self._position_sizer = position_sizer
        self._circuit_breaker = circuit_breaker
        self._inventory = inventory

        wcfg = config.get("weather", {})
        self._enabled = bool(wcfg.get("enabled", False))
        self._scan_interval_min = int(wcfg.get("scan_interval_minutes", 5))
        self._min_edge_threshold = float(wcfg.get("min_edge_threshold", 0.08))
        self._max_entry_price = float(wcfg.get("max_entry_price", 0.85))
        self._max_trade_size = float(wcfg.get("max_trade_size_usdc", 25.0))
        self._max_total_allocation = float(wcfg.get("max_total_allocation", 50.0))
        self._max_trades_per_scan = int(wcfg.get("max_trades_per_scan", 3))
        self._kelly_fraction = float(wcfg.get("kelly_fraction", 0.15))
        self._min_edge_taker = float(wcfg.get("min_edge_taker", 0.30))

        self._kalshi = KalshiClient(paper=False)

        self._pending_tickers: set[str] = set()
        self._pending_orders: dict[str, dict[str, Any]] = {}
        self._markets_cache: list[KalshiWeatherContract] = []
        self._markets_cache_ts: float = 0.0
        self._markets_cache_ttl: float = 300.0

        self._signals_generated: int = 0
        self._trades_executed: int = 0

        self._logger.info(
            "WeatherStrategy v2 (Kalshi): edge_min=%.0f%% max_trade=%.0f "
            "max_entry=%.0f taker_edge=%.0f%% scan=%dmin",
            self._min_edge_threshold * 100, self._max_trade_size,
            self._max_entry_price, self._min_edge_taker * 100, self._scan_interval_min,
        )

    # --- BaseStrategy ---

    def should_act(self, market_data: dict[str, Any]) -> bool:
        return False

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        return []

    def execute(self, signals: list[Signal]) -> list[Trade]:
        trades: list[Trade] = []
        for sig in signals:
            try:
                ticker = sig.metadata.get("ticker", "")
                side = sig.side
                price = sig.price
                size = sig.size
                edge = float(sig.metadata.get("edge", 0.0))

                count = max(1, round(size / price)) if price > 0 else 1
                post_only = abs(edge) < self._min_edge_taker

                if post_only:
                    result = self._kalshi.place_order(ticker, side, count, "limit", price)
                else:
                    result = self._kalshi.place_order(ticker, side, count, "market")

                order_id = result.get("order_id", "") or result.get("order", {}).get("order_id", "")
                status = result.get("status", "submitted")

                trade = self._make_trade(sig, str(order_id), status)
                self.log_trade(trade)
                trades.append(trade)

                if order_id and status == "submitted":
                    self._pending_orders[str(order_id)] = {
                        "market_id": sig.market_id,
                        "token_id": ticker,
                        "side": side, "price": price, "size": size,
                    }
                self._pending_tickers.add(ticker)
                self._trades_executed += 1

            except Exception as e:
                logger.exception("Weather Kalshi: error colocando orden %s", sig.token_id[:12])
        return trades

    # --- Main scan ---

    def run_scan(self, balance: float, cached_positions: dict[str, Any]) -> list[Trade]:
        if not self._active:
            return []
        if self._circuit_breaker is not None and self._circuit_breaker.is_triggered():
            return []

        all_trades: list[Trade] = []

        # 1. Settlement check
        settlement = self._check_settlements()
        all_trades.extend(settlement)

        # 2. Discover contracts
        contracts = self._discover_contracts()
        if not contracts:
            return all_trades

        # 3. Generate signals
        signals: list[tuple[Signal, float]] = []
        for c in contracts:
            sig = self._generate_signal(c, balance)
            if sig is not None:
                edge = float(sig.metadata.get("edge", 0.0))
                if abs(edge) >= self._min_edge_threshold:
                    signals.append((sig, edge))

        self._signals_generated += len(signals)
        logger.info("Weather scan: %d contratos -> %d signals", len(contracts), len(signals))

        if contracts and not signals:
            logger.warning("Weather: %d contratos, 0 signals (Open-Meteo down?)", len(contracts))

        # 4. Sort by edge
        signals.sort(key=lambda x: abs(x[1]), reverse=True)

        # 5. Filter + execute
        to_execute: list[Signal] = []
        seen_tickers: set[str] = set()
        running_exposure = len(self._pending_tickers)

        for sig, _ in signals:
            if len(to_execute) >= self._max_trades_per_scan:
                break
            ticker = sig.metadata.get("ticker", "")
            if ticker in self._pending_tickers or ticker in seen_tickers:
                continue
            if running_exposure >= 10:
                break
            if balance - sig.size < float(self._config.get("loss_reserve_usdc", 20.0)):
                continue
            to_execute.append(sig)
            seen_tickers.add(ticker)
            running_exposure += 1

        if to_execute:
            logger.info("Weather: executing %d trades", len(to_execute))
            executed = self.execute(to_execute)
            all_trades.extend(executed)

        return all_trades

    # --- Contract discovery ---

    def _discover_contracts(self) -> list[KalshiWeatherContract]:
        now = time.time()
        if self._markets_cache and (now - self._markets_cache_ts) < self._markets_cache_ttl:
            return self._markets_cache

        contracts: list[KalshiWeatherContract] = []
        seen_tickers: set[str] = set()

        for series in KALSHI_SERIES:
            try:
                markets = self._kalshi.get_markets(series_ticker=series, status="open", limit=30)
                for m in markets:
                    c = self._parse_kalshi_market(m, series)
                    if c and c.ticker not in seen_tickers:
                        contracts.append(c)
                        seen_tickers.add(c.ticker)
            except Exception as e:
                logger.debug("Kalshi series %s: %s", series, e)

        self._markets_cache = contracts
        self._markets_cache_ts = now
        logger.info("Weather Kalshi: %d contratos descubiertos", len(contracts))
        return contracts

    def _parse_kalshi_market(self, m: dict[str, Any], series: str) -> KalshiWeatherContract | None:
        """Parsea un mercado de Kalshi a KalshiWeatherContract."""
        ticker = m.get("ticker", "")
        if not ticker:
            return None

        city_name = SERIES_CITY_MAP.get(series, "")
        if not city_name:
            return None

        # Parsear ticker: KXHIGHNY-26MAY05-T84
        match = re.match(r"^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})-([BT])([\d.]+)$", ticker)
        if not match:
            return None

        month_map = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                     "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        year_s = int(match.group(1))
        month_s = match.group(2)
        day_s = int(match.group(3))
        boundary = match.group(4)  # B = bottom (>threshold), T = top (<threshold)
        threshold = float(match.group(5))

        year = 2000 + year_s
        month = month_map.get(month_s, 0)
        if month == 0:
            return None

        try:
            target_date = date(year, month, day_s)
        except ValueError:
            return None

        if target_date < date.today():
            return None

        # Metric
        metric = "high" if "LOW" not in series.upper() else "low"

        # Direction
        if boundary == "B":
            direction = "above"
        else:
            direction = "below"

        # Prices (Kalshi API returns in cents, divide by 100)
        yes_bid = float(m.get("yes_bid", 0)) / 100.0
        yes_ask = float(m.get("yes_ask", 0)) / 100.0
        last_price = float(m.get("last_price", 0)) / 100.0
        volume = float(m.get("volume", 0))

        # Filter expensive contracts
        if yes_ask > self._max_entry_price and yes_bid > self._max_entry_price:
            return None

        return KalshiWeatherContract(
            ticker=ticker,
            series_ticker=series,
            city_name=city_name,
            target_date=target_date,
            threshold_f=threshold,
            metric=metric,
            direction=direction,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            last_price=last_price,
            volume=volume,
        )

    # --- Signal generation ---

    def _generate_signal(self, contract: KalshiWeatherContract, balance: float) -> Signal | None:
        """Genera senal de trading para un contrato de temperatura."""
        forecast = fetch_ensemble_forecast(contract.city_name, contract.target_date, platform="kalshi")
        if forecast is None or not forecast.member_highs or not forecast.member_lows:
            return None

        # Calcular probabilidad del ensemble
        if contract.metric == "high":
            if contract.direction == "above":
                model_prob = forecast.probability_high_above(contract.threshold_f)
            else:
                model_prob = forecast.probability_high_below(contract.threshold_f)
        else:
            if contract.direction == "above":
                model_prob = forecast.probability_low_above(contract.threshold_f)
            else:
                model_prob = forecast.probability_low_below(contract.threshold_f)

        model_prob = max(0.05, min(0.95, model_prob))

        market_prob = contract.last_price if contract.last_price > 0 else contract.yes_bid
        if market_prob <= 0:
            return None

        edge = model_prob - market_prob
        if edge <= 0:
            return None

        entry_price = contract.yes_ask if contract.yes_ask > 0 else market_prob
        if entry_price > self._max_entry_price:
            return None

        # Confidence
        if contract.metric == "high":
            members = forecast.member_highs
        else:
            members = forecast.member_lows
        if not members:
            return None

        above_count = sum(1 for m in members if m > contract.threshold_f)
        agreement = max(above_count, len(members) - above_count) / len(members)
        confidence = min(0.9, agreement)

        # Kelly sizing
        win_prob = model_prob
        price_kelly = entry_price
        if price_kelly <= 0 or price_kelly >= 1:
            return None
        odds = (1.0 - price_kelly) / price_kelly
        lose_prob = 1.0 - win_prob
        kelly = (win_prob * odds - lose_prob) / odds if odds > 0 else 0.0
        kelly *= self._kelly_fraction
        kelly = min(kelly, 0.05)
        kelly = max(kelly, 0.0)

        size = kelly * balance
        size = min(size, self._max_trade_size)
        if size <= 0:
            return None

        mean_val = forecast.mean_high if contract.metric == "high" else forecast.mean_low
        std_val = forecast.std_high if contract.metric == "high" else forecast.std_low

        reasoning = (
            f"{contract.city_name} {contract.metric} {contract.direction} "
            f"{contract.threshold_f:.0f}F on {contract.target_date} | "
            f"Ensemble: {mean_val:.1f}F +/- {std_val:.1f}F "
            f"({forecast.num_members}m) | "
            f"Model: {model_prob:.0%} vs Market: {market_prob:.0%} | "
            f"Edge: {edge:+.1%} -> BUY @ {entry_price:.3f} | "
            f"Agreement: {agreement:.0%}"
        )

        return self._make_signal(
            market_id=contract.ticker,
            token_id=contract.ticker,
            side="buy",
            price=entry_price,
            size=size,
            confidence=confidence,
            metadata={
                "ticker": contract.ticker,
                "model_prob": model_prob,
                "market_prob": market_prob,
                "edge": edge,
                "entry_price": entry_price,
                "ensemble_mean": mean_val,
                "ensemble_std": std_val,
                "agreement": agreement,
                "city_name": contract.city_name,
                "threshold_f": contract.threshold_f,
                "metric": contract.metric,
                "direction": contract.direction,
                "target_date": contract.target_date.isoformat(),
                "reasoning": reasoning,
                "category": "weather",
            },
        )

    # --- Settlement ---

    def _check_settlements(self) -> list[Trade]:
        """Verifica contratos resueltos y limpia tracking."""
        trades: list[Trade] = []
        resolved: list[str] = []

        for ticker in list(self._pending_tickers):
            try:
                markets = self._kalshi.get_markets(series_ticker=ticker.split("-")[0] if "-" in ticker else ticker, status="settled", limit=5)
                for m in markets:
                    t = m.get("ticker", "")
                    if t == ticker and m.get("status") == "settled":
                        resolved.append(ticker)
                        logger.info("Weather settlement: %s resolved", ticker)
                        break
            except Exception:
                pass

        for ticker in resolved:
            self._pending_tickers.discard(ticker)
            settle_sig = Signal(
                market_id=ticker, token_id=ticker, side="SETTLE",
                price=0.0, size=0.0, confidence=1.0, strategy_name=self.name,
                metadata={"resolved": True},
            )
            settle_trade = self._make_trade(settle_sig, f"settle_{ticker[:12]}", "settled")
            self.log_trade(settle_trade)
            trades.append(settle_trade)

        return trades

    # --- Helpers ---

    def get_weather_status(self) -> dict[str, Any]:
        return {
            "enabled": self._enabled,
            "active": self._active,
            "platform": "Kalshi",
            "open_positions": len(self._pending_tickers),
            "signals_generated": self._signals_generated,
            "trades_executed": self._trades_executed,
            "min_edge": self._min_edge_threshold,
            "max_trade_size": self._max_trade_size,
            "max_allocation": self._max_total_allocation,
        }

    @property
    def scan_interval_min(self) -> int:
        return self._scan_interval_min

    @property
    def is_enabled(self) -> bool:
        return self._enabled
