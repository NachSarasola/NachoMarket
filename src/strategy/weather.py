"""Polymarket Weather Temperature Trading Strategy.

Usa Open-Meteo Ensemble API (GFS, 31 miembros) para estimar probabilidades
de buckets de temperatura en Polymarket (multi-outcome negRisk events).

Estrategia: ensemble counting → probabilidad por bucket → comprar YES
del bucket mas probable si edge > 8%. Hold hasta resolucion (1-2 dias).

Market discovery: construye slugs por ciudad+fecha (el tag API no funciona).
60+ ciudades globales, mercados diarios, $10K-$300K volumen cada uno.
"""

from __future__ import annotations

import json
import logging
import math
import re
import statistics
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from src.data.weather import (
    STATIONS,
    MONTH_MAP,
    fetch_ensemble_forecast,
    fetch_nws_point_forecast,
    fetch_observed_temperature,
    get_wunderground_url,
    resolve_station,
)
from src.data.weather_provider import (
    ForecastProvider,
    MarketProvider,
    LiveForecastProvider,
    LiveMarketProvider,
)
from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.weather")

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# 64 ciudades que aparecen en polymarket.com/weather
CITY_SLUGS: dict[str, str] = {
    # USA
    "New York": "nyc", "Chicago": "chicago", "Miami": "miami",
    "Los Angeles": "los-angeles", "Denver": "denver", "Dallas": "dallas",
    "Atlanta": "atlanta", "Seattle": "seattle", "Houston": "houston",
    "Austin": "austin", "Boston": "boston", "Phoenix": "phoenix",
    "Las Vegas": "las-vegas", "Minneapolis": "minneapolis",
    "Philadelphia": "philadelphia", "San Francisco": "san-francisco",
    "Washington DC": "washington-dc", "San Antonio": "san-antonio",
    "New Orleans": "new-orleans", "Oklahoma City": "oklahoma-city",
    # Europa
    "London": "london", "Paris": "paris", "Madrid": "madrid",
    "Berlin": "berlin", "Amsterdam": "amsterdam", "Milan": "milan",
    "Munich": "munich", "Warsaw": "warsaw", "Helsinki": "helsinki",
    "Moscow": "moscow", "Ankara": "ankara", "Istanbul": "istanbul",
    "Tel Aviv": "tel-aviv",
    # Asia
    "Tokyo": "tokyo", "Seoul": "seoul", "Hong Kong": "hong-kong",
    "Singapore": "singapore", "Beijing": "beijing", "Shanghai": "shanghai",
    "Taipei": "taipei", "Shenzhen": "shenzhen", "Chengdu": "chengdu",
    "Chongqing": "chongqing", "Wuhan": "wuhan", "Qingdao": "qingdao",
    "Guangzhou": "guangzhou", "Manila": "manila", "Jakarta": "jakarta",
    "Kuala Lumpur": "kuala-lumpur", "Jeddah": "jeddah", "Karachi": "karachi",
    "Delhi": "delhi", "Lucknow": "lucknow", "Busan": "busan",
    # LatAm
    "Buenos Aires": "buenos-aires", "Mexico City": "mexico-city",
    "Sao Paulo": "sao-paulo", "Panama City": "panama-city",
    # Otros
    "Toronto": "toronto", "Lagos": "lagos", "Cape Town": "cape-town",
    "Wellington": "wellington", "Sydney": "sydney",
}

CITY_REGIONS: dict[str, list[str]] = {
    "US_NE": ["New York", "Boston", "Philadelphia", "Washington DC"],
    "US_SE": ["Miami", "Atlanta"],
    "US_S": ["Houston", "Dallas", "Austin", "San Antonio", "New Orleans"],
    "US_MW": ["Chicago", "Minneapolis"],
    "US_W": ["Los Angeles", "San Francisco", "Seattle"],
    "US_SW": ["Denver", "Phoenix", "Las Vegas"],
    "EU_W": ["London", "Paris", "Amsterdam"],
    "EU_C": ["Berlin", "Munich", "Warsaw"],
    "EU_S": ["Madrid", "Milan"],
    "EU_N": ["Helsinki"],
    "EU_E": ["Moscow", "Ankara", "Istanbul", "Tel Aviv"],
    "ASIA_E": ["Tokyo", "Seoul", "Beijing", "Shanghai", "Taipei", "Busan", "Hong Kong", "Shenzhen", "Guangzhou", "Qingdao"],
    "ASIA_SE": ["Singapore", "Manila", "Jakarta", "Kuala Lumpur"],
    "ASIA_S": ["Delhi", "Lucknow", "Karachi", "Jeddah"],
    "LATAM": ["Buenos Aires", "Sao Paulo", "Mexico City", "Panama City"],
    "OTHER": ["Toronto", "Lagos", "Cape Town", "Wellington", "Sydney"],
}


@dataclass
class WeatherMarket:
    """Un bucket de temperatura en un evento negRisk de Polymarket."""

    market_id: str
    condition_id: str
    question: str
    city_name: str
    target_date: date
    threshold: float       # temperatura del bucket en Fahrenheit (midpoint para ranges)
    bucket_type: str       # "below", "range", "above"
    metric: str            # "high" o "low"
    yes_price: float
    no_price: float
    tokens: list[dict[str, Any]] = field(default_factory=list)
    bucket_lower: float | None = None   # limite inferior exacto del bucket
    bucket_upper: float | None = None   # limite superior exacto del bucket
    volume: float = 0.0


class WeatherStrategy(BaseStrategy):
    """Estrategia de trading en temperatura via Polymarket con ensemble forecasting."""

    def __init__(
        self,
        client: Any,
        config: dict[str, Any],
        position_sizer: Any = None,
        circuit_breaker: Any = None,
        inventory: Any = None,
        forecast_provider: ForecastProvider | None = None,
        market_provider: MarketProvider | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__("weather", client, config, **kwargs)
        self._position_sizer = position_sizer
        self._circuit_breaker = circuit_breaker
        self._inventory = inventory

        # Phase 7: injectable data providers
        self._forecast_provider = forecast_provider or LiveForecastProvider()
        self._market_provider = market_provider or LiveMarketProvider()

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

        # Phase 3: verification + guardrails
        self._nws_cross_check = bool(wcfg.get("nws_cross_check", True))
        self._nws_max_divergence = float(wcfg.get("nws_max_divergence", 10.0))
        self._min_ensemble_members = int(wcfg.get("min_ensemble_members", 20))
        self._max_ensemble_std = float(wcfg.get("max_ensemble_std", 20.0))
        self._calibration_min_samples = int(wcfg.get("calibration_min_samples", 20))

        # Phase 4: adaptive dynamic thresholds
        dt = wcfg.get("dynamic_thresholds", {})
        self._dt = {
            "edge_base_narrow": float(dt.get("edge_base_narrow", 0.12)),
            "edge_base_mid": float(dt.get("edge_base_mid", 0.07)),
            "edge_base_wide": float(dt.get("edge_base_wide", 0.04)),
            "edge_std_penalty_rate": float(dt.get("edge_std_penalty_rate", 0.02)),
            "edge_quality_discount": float(dt.get("edge_quality_discount", 0.05)),
            "edge_min_floor": float(dt.get("edge_min_floor", 0.02)),
            "edge_max_ceiling": float(dt.get("edge_max_ceiling", 0.25)),
            "confidence_min": float(dt.get("confidence_min", 0.55)),
            "confidence_cap": float(dt.get("confidence_cap", 0.90)),
            "nws_confidence_penalty": float(dt.get("nws_confidence_penalty", 0.70)),
            "uncertainty_penalty_num": float(dt.get("uncertainty_penalty_num", 2.0)),
            "uncertainty_penalty_off": float(dt.get("uncertainty_penalty_off", 0.5)),
            "kelly_cap": float(dt.get("kelly_cap", 0.05)),
            "kelly_floor": float(dt.get("kelly_floor", 0.0)),
            "outlier_sigma_mult": float(dt.get("outlier_sigma_mult", 3.0)),
            "outlier_abs_max": float(dt.get("outlier_abs_max", 25.0)),
            "sanity_temp_min": float(dt.get("sanity_temp_min", -50.0)),
            "sanity_temp_max": float(dt.get("sanity_temp_max", 130.0)),
            # Phase 5
            "reposition_enabled": bool(dt.get("reposition_enabled", True)),
            "reposition_prob_delta": float(dt.get("reposition_prob_delta", 0.15)),
            "boundary_risk_rate": float(dt.get("boundary_risk_rate", 0.04)),
            "forecast_max_age_hours": float(dt.get("forecast_max_age_hours", 2.0)),
            "forecast_age_penalty_rate": float(dt.get("forecast_age_penalty_rate", 0.005)),
            "lead_time_penalty_rate": float(dt.get("lead_time_penalty_rate", 0.003)),
            "skew_penalty": float(dt.get("skew_penalty", 0.02)),
            # Phase 6: exit strategy
            "exit_take_profit_threshold": float(dt.get("exit_take_profit_threshold", 0.90)),
            "exit_stop_loss_ratio": float(dt.get("exit_stop_loss_ratio", 0.35)),
            "exit_temp_stop_loss": float(dt.get("exit_temp_stop_loss", 8.0)),
            "exit_max_hold_hours": float(dt.get("exit_max_hold_hours", 72.0)),
            "exit_cooldown_hours": float(dt.get("exit_cooldown_hours", 4.0)),
            "exit_partial_fraction": float(dt.get("exit_partial_fraction", 0.5)),
            "exit_trailing_enabled": bool(dt.get("exit_trailing_enabled", True)),
        }

        self._pending_trades: dict[str, dict[str, Any]] = {}

        self._markets_cache: list[WeatherMarket] = []
        self._markets_cache_ts: float = 0.0
        self._markets_cache_ttl: float = 300.0

        self._signals_generated: int = 0
        self._trades_executed: int = 0

        # Calibration: track predictions vs outcomes per city+lead_days
        self._calibration_file = Path("data/weather_calibration.json")
        self._calibration: dict[str, dict[str, Any]] = self._load_calibration()

        # Pending state persistence
        self._pending_file = Path("data/weather_pending.json")
        self._pending_trades = self._load_pending_state()
        self._reconcile_open_orders()

        # Phase 6: exit strategy state
        self._exit_cooldown: dict[str, float] = {}
        self._exits_file = Path("data/weather_exits.jsonl")
        self._trades_file = Path("data/weather_trades.jsonl")

        self._logger.info(
            "WeatherStrategy v6 (Polymarket): %d cities edge_min=%.0f%% "
            "max_trade=%.0f max_entry=%.2f kelly=%.0f%% scan=%dmin "
            "pending=%d calibrated=%d guard_ens=%d guard_std=%.0fF",
            len(CITY_SLUGS), self._min_edge_threshold * 100,
            self._max_trade_size, self._max_entry_price,
            self._kelly_fraction * 100, self._scan_interval_min,
            len(self._pending_trades), len(self._calibration),
            self._min_ensemble_members, self._max_ensemble_std,
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
                token_id = sig.token_id
                side = sig.side
                price = sig.price
                size = sig.size
                edge = abs(float(sig.metadata.get("effective_edge", 0.0)))

                shares = math.ceil(size / price) if price > 0 else 0
                shares = max(shares, 5)
                if shares <= 0:
                    continue

                post_only = edge < self._min_edge_taker
                result = self._client.place_limit_order(
                    token_id=token_id, side=side, price=price,
                    size=shares, post_only=post_only,
                )
                order_id = result.get("order_id", "") or result.get("id", "")
                status = result.get("status", "submitted")

                trade = self._make_trade(sig, order_id, status)
                self.log_trade(trade)
                trades.append(trade)

                if order_id and status in ("live", "submitted"):
                    mid = sig.market_id
                    self._pending_trades[mid] = {
                        "market_id": mid,
                        "token_id": token_id,
                        "side": side,
                        "price": price,
                        "size": size,
                        "order_id": order_id,
                        "city_name": sig.metadata.get("city_name", ""),
                        "target_date": sig.metadata.get("target_date", ""),
                        "threshold": float(sig.metadata.get("threshold", 0)),
                        "metric": sig.metadata.get("metric", ""),
                        "bucket_type": sig.metadata.get("bucket_type", ""),
                        "bucket_lower": sig.metadata.get("bucket_lower"),
                        "bucket_upper": sig.metadata.get("bucket_upper"),
                        "forecast_mean": float(sig.metadata.get("ens_mean", 0)),
                        "forecast_std": float(sig.metadata.get("ens_std", 0)),
                        "forecast_prob": float(sig.metadata.get("model_prob", 0)),
                        "confidence": float(sig.metadata.get("agreement", 0)),
                        "entry_price": price,
                        "executed_at": datetime.now(timezone.utc).isoformat(),
                    }
                    self._log_weather_trade(mid, self._pending_trades[mid], status="open")
                    self._save_pending_state()

                self._trades_executed += 1

            except Exception as e:
                self._logger.warning("Weather: error colocando orden: %s", e)
        return trades

    # --- Main scan ---

    def run_scan(self, balance: float) -> list[Trade]:
        if not self._active:
            return []
        if self._circuit_breaker is not None and self._circuit_breaker.is_triggered():
            return []

        all_trades: list[Trade] = []

        # 0. Exit management (Phase 6): take-profit, stop-loss, temp, prob delta
        all_trades.extend(self._manage_exits(balance))

        # 1. Settlement check
        all_trades.extend(self._check_settlements())

        # 2. Discover markets
        markets = self._discover_weather_markets()
        if not markets:
            return all_trades

        # 3. Generate signals
        signals: list[tuple[Signal, float]] = []
        for mkt in markets:
            sig = self._generate_signal(mkt, balance)
            if sig is not None:
                edge = float(sig.metadata.get("effective_edge", 0.0))
                if abs(edge) >= self._min_edge_threshold:
                    signals.append((sig, edge))

        self._signals_generated += len(signals)
        self._logger.info(
            "Weather scan: %d buckets -> %d signals (edge>=%.0f%%)",
            len(markets), len(signals), self._min_edge_threshold * 100,
        )
        if markets and len(signals) < 3:
            self._logger.info(
                "Weather: low signal rate %d/%d - mercado eficiente o filtros muy estrictos",
                len(signals), len(markets),
            )

        # 4. Sort by edge
        signals.sort(key=lambda x: abs(x[1]), reverse=True)

        # 5. Filter + execute
        to_execute: list[Signal] = []
        seen: set[str] = set()
        running = len(self._pending_trades)

        for sig, _ in signals:
            if len(to_execute) >= self._max_trades_per_scan:
                break
            if sig.market_id in self._pending_trades or sig.market_id in seen:
                continue

            # Phase 6: cooling period — don't re-enter recently exited markets
            city = sig.metadata.get("city_name", "")
            tgt = sig.metadata.get("target_date", "")
            met = sig.metadata.get("metric", "")
            cooldown_key = f"{city}_{tgt}_{met}"
            if cooldown_key in self._exit_cooldown:
                hours_since = (time.time() - self._exit_cooldown[cooldown_key]) / 3600.0
                if hours_since < self._dt["exit_cooldown_hours"]:
                    continue

            # Phase 5: Spatial Kelly / Correlated Risk
            region = "UNKNOWN"
            for r, cities in CITY_REGIONS.items():
                if city in cities:
                    region = r
                    break
            
            # --- Correlated Kelly: dividir por N posiciones en misma región/fecha ---
            # En lugar de un cap plano, el sizing cae naturalmente con cada ciudad correlacionada.
            n_region_same_date = sum(
                1 for pt in self._pending_trades.values()
                if pt.get("target_date", "")[:10] == tgt[:10]
                and pt.get("metric") == met
                and pt.get("city_name", "") in CITY_REGIONS.get(region, [])
            )
            if n_region_same_date > 0:
                sig.size /= (1 + n_region_same_date)
                if sig.size < 3.0:
                    self._logger.info(
                        "Weather Correlated Kelly: size < $3 after /%d divisor for %s in %s",
                        n_region_same_date + 1, city, region,
                    )
                    continue

            if running >= 10:
                break
            if balance - sig.size < float(self._config.get("loss_reserve_usdc", 20.0)):
                continue
            to_execute.append(sig)
            seen.add(sig.market_id)
            running += 1

        if to_execute:
            self._logger.info("Weather: executing %d trades", len(to_execute))
            all_trades.extend(self.execute(to_execute))

        return all_trades

    # --- Market discovery ---

    def _discover_weather_markets(self) -> list[WeatherMarket]:
        """Descubre mercados de temperatura construyendo slugs por ciudad+fecha."""
        now = time.time()
        if self._markets_cache and (now - self._markets_cache_ts) < self._markets_cache_ttl:
            return self._markets_cache

        markets: list[WeatherMarket] = []
        seen_ids: set[str] = set()

        today = date.today()
        dates = [today, today + timedelta(days=1)]

        for d in dates:
            date_slug = self._date_to_slug(d)
            for city_name, city_slug in CITY_SLUGS.items():
                if city_slug not in STATIONS:
                    self._logger.warning(
                        "Weather: %s (%s) not in STATIONS — skipping",
                        city_name, city_slug,
                    )
                    continue
                for prefix in ("highest-temperature", "lowest-temperature"):
                    slug = f"{prefix}-in-{city_slug}-on-{date_slug}"
                    try:
                        events = self._market_provider.get_events_by_slug(slug)
                        if not events:
                            continue
                        event = events[0]
                        if not isinstance(event, dict):
                            continue

                        metric = "high" if "highest" in prefix else "low"
                        for mkt_data in event.get("markets", []):
                            wm = self._parse_bucket(mkt_data, city_name, d, metric)
                            if wm and wm.market_id not in seen_ids:
                                markets.append(wm)
                                seen_ids.add(wm.market_id)
                    except Exception:
                        continue

        self._markets_cache = markets
        self._markets_cache_ts = now
        self._logger.info(
            "Weather discovery: %d buckets found across %d cities x %d dates",
            len(markets), len(CITY_SLUGS), len(dates),
        )
        return markets

    @staticmethod
    def _date_to_slug(d: date) -> str:
        """Convierte date a slug: May 5, 2026 -> 'may-5-2026'"""
        month = d.strftime("%B").lower()
        day = str(d.day)  # sin leading zero
        return f"{month}-{day}-{d.year}"

    @staticmethod
    def _parse_bucket(
        mkt_data: dict[str, Any], city_name: str, target_date: date, metric: str
    ) -> WeatherMarket | None:
        """Parsea un bucket de temperatura de un evento negRisk."""
        question = mkt_data.get("question", "")
        if not question:
            return None

        threshold, bucket_lower, bucket_upper = WeatherStrategy._parse_threshold(question)
        if threshold is None:
            return None

        ql = question.lower()
        if "or below" in ql or "or less" in ql:
            bucket_type = "below"
        elif "or higher" in ql or "or above" in ql:
            bucket_type = "above"
        else:
            bucket_type = "range"

        outcome_prices = mkt_data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            import json
            try:
                outcome_prices = json.loads(outcome_prices)
            except Exception:
                outcome_prices = []

        if len(outcome_prices) < 2:
            return None

        try:
            yes_price = float(outcome_prices[0])
            no_price = float(outcome_prices[1])
        except (ValueError, IndexError, TypeError):
            return None

        if yes_price > 0.98 or yes_price < 0.02:
            return None

        clob_ids = mkt_data.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            import json
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []

        if len(clob_ids) < 2:
            return None

        tokens = [
            {"token_id": str(clob_ids[0]), "outcome": "Yes"},
            {"token_id": str(clob_ids[1]), "outcome": "No"},
        ]

        volume = float(mkt_data.get("volume", 0) or 0)

        return WeatherMarket(
            market_id=str(mkt_data.get("id", "")),
            condition_id=mkt_data.get("conditionId", ""),
            question=question,
            city_name=city_name,
            target_date=target_date,
            threshold=threshold,
            bucket_type=bucket_type,
            bucket_lower=bucket_lower,
            bucket_upper=bucket_upper,
            metric=metric,
            tokens=tokens,
            yes_price=yes_price,
            no_price=no_price,
            volume=volume,
        )

    @staticmethod
    def _parse_threshold(question: str) -> tuple[float | None, float | None, float | None]:
        """Extrae temperatura de la pregunta. Retorna (midpoint, lower, upper)."""
        q = question.lower()
        # "72-73°F" -> range
        m = re.search(r"(\d+)\s*-\s*(\d+)\s*°?\s*f", q)
        if m:
            lo = float(m.group(1))
            hi = float(m.group(2))
            return ((lo + hi) / 2.0, lo, hi)
        # "24-25°C" -> range (Celsius)
        m = re.search(r"(\d+)\s*-\s*(\d+)\s*°?\s*c", q)
        if m:
            lo = float(m.group(1)) * 9.0 / 5.0 + 32.0
            hi = float(m.group(2)) * 9.0 / 5.0 + 32.0
            return ((lo + hi) / 2.0, lo, hi)
        # "71°F or below" / "82°F or higher"
        m = re.search(r"(\d+)\s*°?\s*f", q)
        if m:
            t = float(m.group(1))
            if "or below" in q or "or less" in q:
                return (t, None, t)
            return (t, t, None)
        # "24°C or higher" / "17°C or below"
        m = re.search(r"(\d+)\s*°?\s*c", q)
        if m:
            t = float(m.group(1)) * 9.0 / 5.0 + 32.0
            if "or below" in q or "or less" in q:
                return (t, None, t)
            return (t, t, None)
        return (None, None, None)

    # --- Signal generation ---

    def _generate_signal(self, market: WeatherMarket, balance: float) -> Signal | None:
        """Genera senal calibrada para un bucket de temperatura.

        Pipeline:
        1. Fetch GFS ensemble + data quality guardrails
        2. NWS cross-check (US stations only)
        3. Ensemble probability + calibrated bias/sigma
        4. Dynamic edge threshold + Kelly sizing
        """
        forecast = fetch_ensemble_forecast(market.city_name, market.target_date)
        if forecast is None or not forecast.member_highs or not forecast.member_lows:
            return None

        members = forecast.member_highs if market.metric == "high" else forecast.member_lows
        mean_val = sum(members) / len(members)
        ensemble_std = statistics.stdev(members) if len(members) > 1 else 1.0

        # --- Data quality guardrails ---
        if forecast.num_members < self._min_ensemble_members:
            self._logger.warning(
                "Weather: ensemble degraded — %s only %d members (min %d required)",
                market.city_name, forecast.num_members, self._min_ensemble_members,
            )
            return None
        if ensemble_std > self._max_ensemble_std:
            self._logger.warning(
                "Weather: ensemble too uncertain — %s std=%.1fF (max %.0fF)",
                market.city_name, ensemble_std, self._max_ensemble_std,
            )
            return None
        if mean_val < self._dt["sanity_temp_min"] or mean_val > self._dt["sanity_temp_max"]:
            self._logger.warning(
                "Weather: implausible temperature — %s mean=%.1fF",
                market.city_name, mean_val,
            )
            return None

        # --- NWS cross-check (US stations only) ---
        station = resolve_station(market.city_name)
        city_is_us = station and station.get("icao", "").startswith("K")
        nws_divergence = 0.0
        if city_is_us and self._nws_cross_check:
            try:
                station_key = market.city_name.lower().replace(" ", "-")
                nws_data = fetch_nws_point_forecast(station_key)
                if nws_data:
                    today = nws_data.get("today", {})
                    is_today = (market.target_date - date.today()).days == 0
                    day_data = today if is_today else nws_data.get("tomorrow", today)
                    nws_temp = day_data.get(
                        "high" if market.metric == "high" else "low"
                    )
                    if nws_temp is not None:
                        nws_divergence = abs(float(nws_temp) - mean_val)
                        if nws_divergence > self._nws_max_divergence:
                            self._logger.warning(
                                "Weather: NWS-GFS extreme divergence — %s delta=%.1fF "
                                "(NWS=%.0fF GFS=%.1fF) — skipping",
                                market.city_name, nws_divergence, nws_temp, mean_val,
                            )
                            return None
            except Exception:
                pass

        # --- Market quality filters ---
        if market.volume < 100:
            return None
        # Range buckets requieren más liquidez (spread más amplio, menos participantes)
        if market.bucket_type == "range" and market.volume < 500:
            return None
        if market.yes_price < 0.01:
            return None
        if market.yes_price > 0 and market.no_price > 0:
            if abs(market.yes_price - (1.0 - market.no_price)) > 0.15:
                return None
        # --- Mid-Price: evaluar edge contra el mid-market, no el Ask ---
        # Esto evita pagar el spread completo como Taker en cada entrada.
        # bid = 1 - no_price (precio de compra implícito del YES)
        ask_price = market.yes_price
        bid_price = 1.0 - market.no_price
        mid_price = (ask_price + bid_price) / 2.0
        mid_price = max(0.01, min(0.99, mid_price))
        # Usar mid para evaluar el edge; si lo hay, entrar como Maker al mid
        entry_price = mid_price
        if entry_price > self._max_entry_price:
            return None
        # Spread actual (indicador de liquidez)
        spread = ask_price - bid_price

        # --- Calibration: bias + sigma + quality from historical errors ---
        lead_days = (market.target_date - date.today()).days
        lead_days = max(0, lead_days)
        month = date.today().month
        
        # bias is now the historic temperature error (Actual - Forecast)
        bias, calibrated_sigma = self._get_calibration_stats(
            market.city_name, market.metric, lead_days, month,
        )
        
        if bias:
            members = [m + bias for m in members]
            mean_val += bias
            
        # --- Ensemble probability (Calibrated) ---
        if market.bucket_type == "below" and market.bucket_upper is not None:
            calibrated_prob = len([m for m in members if m <= market.bucket_upper]) / len(members)
        elif market.bucket_type == "above" and market.bucket_lower is not None:
            calibrated_prob = len([m for m in members if m >= market.bucket_lower]) / len(members)
        elif market.bucket_type == "range" and market.bucket_lower is not None and market.bucket_upper is not None:
            calibrated_prob = len([m for m in members if market.bucket_lower <= m <= market.bucket_upper]) / len(members)
        else:
            return None

        calibrated_prob = max(0.01, min(0.99, calibrated_prob))
        raw_edge = calibrated_prob - mid_price
        if raw_edge <= 0:
            return None

        # --- Filtro de probabilidad mínima por tipo de bucket ---
        # Un range al 40% comprado a 2c tiene edge positivo pero exp. negativa a largo plazo.
        # below/above (cola abierta): necesitan conviccion alta para ser rentables.
        # range (bucket cerrado): el modelo debe darle >= 50% para que valga la pena.
        if market.bucket_type == "range" and calibrated_prob < 0.50:
            self._logger.debug(
                "Weather: range prob too low — %s %.0f%% < 50%% floor",
                market.city_name, calibrated_prob * 100,
            )
            return None
        if market.bucket_type in ("below", "above") and calibrated_prob < 0.60:
            self._logger.debug(
                "Weather: tail prob too low — %s %.0f%% < 60%% floor",
                market.city_name, calibrated_prob * 100,
            )
            return None

        # Use calibrated sigma when available, fallback to live ensemble_std
        effective_std = calibrated_sigma if calibrated_sigma is not None else ensemble_std

        # --- Adaptive edge threshold (Phase 4) ---
        dt = self._dt
        bucket_width = (market.bucket_upper or market.threshold) - (market.bucket_lower or market.threshold)
        bucket_width = abs(bucket_width) if bucket_width else 5.0
        if bucket_width < 2.0:
            min_edge = dt["edge_base_narrow"]
        elif bucket_width >= 5.0:
            min_edge = dt["edge_base_wide"]
        else:
            min_edge = dt["edge_base_mid"]

        # Penalizar por incertidumbre
        if effective_std > 2.0:
            min_edge += (effective_std - 2.0) * dt["edge_std_penalty_rate"]

        # Descontar por baja calidad de calibracion
        quality = self._get_calibration_quality(
            market.city_name, market.metric, lead_days, month,
        )
        min_edge += (1.0 - quality) * dt["edge_quality_discount"]

        # Floor and ceiling
        min_edge = max(dt["edge_min_floor"], min(min_edge, dt["edge_max_ceiling"]))

        # --- Phase 5: bucket boundary risk ---
        if market.bucket_lower is not None and market.bucket_upper is not None:
            safe_std = effective_std if effective_std > 0 else 1.0
            dist_to_lower = (mean_val - market.bucket_lower) / safe_std
            dist_to_upper = (market.bucket_upper - mean_val) / safe_std
            dist_to_edge = min(dist_to_lower, dist_to_upper)
            if dist_to_edge < 1.5:
                boundary_penalty = (1.5 - dist_to_edge) * dt.get("boundary_risk_rate", 0.04)
                min_edge += boundary_penalty

        # --- Phase 5: forecast recency ---
        forecast_age_hours = (time.time() - forecast.fetched_at) / 3600.0
        max_age = dt.get("forecast_max_age_hours", 2.0)
        if forecast_age_hours > max_age:
            freshness_penalty = (forecast_age_hours - max_age) * dt.get("forecast_age_penalty_rate", 0.005)
            min_edge += min(freshness_penalty, 0.03)

        # --- Phase 5: lead-time decay ---
        if lead_days > 2:
            lead_penalty = (lead_days - 2) * dt.get("lead_time_penalty_rate", 0.003)
            min_edge += lead_penalty

        # --- Phase 5: ensemble shape analysis ---
        shape = self._analyze_ensemble_shape(members)
        if shape["bimodal"]:
            self._logger.warning("Weather: bimodal ensemble in %s — skip", market.city_name)
            return None
        if abs(shape["skew"]) > 1.0:
            min_edge += dt.get("skew_penalty", 0.02)

        # --- Confidence weighting ---
        above_count = len([m for m in members if m > market.threshold])
        agreement = max(above_count, len(members) - above_count) / len(members)
        confidence = min(dt["confidence_cap"], agreement)
        if confidence < dt["confidence_min"]:
            return None

        # NWS moderate divergence: penalize confidence
        if nws_divergence > 5.0:
            confidence *= dt["nws_confidence_penalty"]
            self._logger.info(
                "Weather: NWS-GFS moderate divergence %s delta=%.1fF — confidence=%.2f",
                market.city_name, nws_divergence, confidence,
            )

        # --- Phase 5: confidence calibration curve ---
        agreement_bin = self._agreement_bin(confidence)
        calib_lookup = f"{market.city_name}_{market.metric}_{lead_days}d_m{month}_{agreement_bin}"
        if calib_lookup in self._calibration:
            calib_entry = self._calibration[calib_lookup]
            calib_preds = calib_entry.get("predictions", [])
            if calib_preds:
                historical_wr = sum(calib_preds) / len(calib_preds)
                if historical_wr < confidence - 0.1:
                    confidence *= 0.8

        uncertainty_penalty = min(1.0, dt["uncertainty_penalty_num"] / (effective_std + dt["uncertainty_penalty_off"]))
        effective_edge = (calibrated_prob - mid_price) * confidence * uncertainty_penalty
        if effective_edge < min_edge:
            return None

        # --- Kelly sizing: escalado por convicción ---
        # Kelly × conviction evita apostar igual con 90% agreement que con 55%.
        if entry_price <= 0 or entry_price >= 1:
            return None
        odds = (1.0 - entry_price) / entry_price
        lose_prob = 1.0 - calibrated_prob
        kelly = (calibrated_prob * odds - lose_prob) / odds if odds > 0 else 0.0
        kelly *= self._kelly_fraction * confidence * uncertainty_penalty
        kelly = min(kelly, dt["kelly_cap"])
        kelly = max(kelly, dt["kelly_floor"])

        size = kelly * balance
        size = min(size, self._max_trade_size)
        if size <= 0:
            return None

        token_id = ""
        if market.tokens and len(market.tokens) > 0:
            token_id = str(market.tokens[0].get("token_id", ""))

        sigma_label = f"σ_cal={calibrated_sigma:.1f}" if calibrated_sigma is not None else f"σ_live={effective_std:.1f}"
        reasoning = (
            f"{market.city_name} {market.metric} {market.bucket_type} "
            f"{market.threshold:.0f}F on {market.target_date} | "
            f"Ensemble: {mean_val:.1f}F +/- {ensemble_std:.1f}F ({len(members)}m) | "
            f"CalProb: {calibrated_prob:.0%} (bias={bias:+.1f}F {sigma_label} Q={quality:.0%}) | "
            f"Mid: {mid_price:.0%} Spread: {spread:.2f} | EffEdge: {effective_edge:+.1%} (min={min_edge:.1%}) -> Maker @ {entry_price:.3f}"
        )

        return self._make_signal(
            market_id=market.market_id,
            token_id=token_id,
            side="BUY",
            price=entry_price,
            size=size,
            confidence=confidence,
            metadata={
                "model_prob": calibrated_prob,
                "market_prob": entry_price, "effective_edge": effective_edge,
                "entry_price": entry_price, "agreement": confidence,
                "city_name": market.city_name, "threshold": market.threshold,
                "metric": market.metric, "bucket_type": market.bucket_type,
                "target_date": market.target_date.isoformat(),
                "ens_std": ensemble_std, "calibrated_sigma": calibrated_sigma,
                "ens_mean": mean_val, "bias": bias, "min_edge": min_edge,
                "bucket_lower": market.bucket_lower, "bucket_upper": market.bucket_upper,
                "nws_divergence": nws_divergence,
                "reasoning": reasoning, "category": "weather",
            },
        )

    # --- Settlement ---

    def _check_settlements(self) -> list[Trade]:
        """Verifica mercados resueltos, graba calibracion y limpia tracking.

        Para cada mercado pendiente consulta Gamma API. Si cerro:
        1. Registra calibracion con key compuesto {city}_{lead_days}d_m{month}
        2. Opcional: fetchea temp observada y graba forecast error
        3. Limpia de _pending_trades y persiste
        """
        trades: list[Trade] = []
        if not self._pending_trades:
            return trades

        resolved: list[str] = []
        resolved_info: dict[str, bool] = {}

        for mid, pt in list(self._pending_trades.items()):
            try:
                data = self._market_provider.get_market(mid)
                if data is None:
                    continue
                if not isinstance(data, dict) or not data.get("closed", False):
                    continue
                resolved.append(mid)

                # 1. Determinar si YES gano
                outcome_prices = data.get("outcomePrices", [])
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)
                yes_won = False
                if isinstance(outcome_prices, list) and len(outcome_prices) >= 1:
                    try:
                        yes_won = float(outcome_prices[0]) > 0.99
                    except (ValueError, TypeError):
                        pass
                resolved_info[mid] = yes_won

                # 2. Extraer metadata del trade pendiente
                city_name = pt.get("city_name", "unknown")
                target_date_str = pt.get("target_date", "")
                forecast_mean = float(pt.get("forecast_mean", 0))
                try:
                    target_date = date.fromisoformat(target_date_str)
                    # Usar la fecha de ejecución original para calcular lead_days correcto,
                    # ya que al resolver el target_date ya está en el pasado (lead=0 siempre).
                    executed_at_str = pt.get("executed_at", "")
                    if executed_at_str:
                        entry_date = date.fromisoformat(executed_at_str[:10])
                        lead_days = max(0, (target_date - entry_date).days)
                    else:
                        lead_days = max(0, (target_date - date.today()).days)
                except (ValueError, TypeError):
                    lead_days = 0

                # 3. Fetch observed temperature (non-blocking)
                actual_temp = None
                try:
                    station_key = city_name.lower().replace(" ", "-")
                    station = STATIONS.get(station_key)
                    if station:
                        result = fetch_observed_temperature(station_key, target_date)
                        if result:
                            metric = pt.get("metric", "high")
                            actual_temp = result.get(metric)
                except Exception:
                    pass

                # 4. Grabar calibracion
                self._record_calibration(
                    city=city_name,
                    metric=pt.get("metric", "high"),
                    lead_days=lead_days,
                    yes_won=yes_won,
                    forecast_mean=forecast_mean,
                    actual_temp=actual_temp,
                    confidence=float(pt.get("confidence", 0.7)),
                )

                self._logger.info(
                    "Weather settlement: %s %s %s lead=%dd yes_won=%s "
                    "fc_mean=%.1fF actual=%s",
                    city_name, pt.get("metric", "?"), pt.get("target_date", "?"),
                    lead_days, yes_won, forecast_mean, actual_temp,
                )
            except Exception:
                pass

        for mid in resolved:
            pt = self._pending_trades.pop(mid, None) or {}
            yes_won = resolved_info.get(mid, False)
            self._log_weather_trade(
                mid, pt, status="settled",
                resolution="yes_won" if yes_won else "yes_lost",
                exit_price=1.0 if yes_won else 0.0,
                exit_reason="settlement",
            )
            if self._inventory:
                try:
                    self._inventory.clear_market(mid)
                except Exception:
                    pass
            settle_sig = Signal(
                market_id=mid, token_id="", side="SETTLE",
                price=0.0, size=0.0, confidence=1.0, strategy_name=self.name,
                metadata={"resolved": True},
            )
            settle_trade = self._make_trade(settle_sig, f"settle_{mid[:12]}", "settled")
            self.log_trade(settle_trade)
            trades.append(settle_trade)

        if resolved:
            self._save_pending_state()

        return trades

    # --- Reconciliation ---

    def _load_calibration(self) -> dict[str, dict[str, Any]]:
        """Carga calibracion desde disco con migracion automatica de formatos viejos.

        Migra:
        - Keys sin metrica ({city}_{lead_days}d_m{month}) → duplica a _high_ y _low_
        - Keys sin last_updated → agrega fecha actual
        - Keys sin sigma con forecast_errors → calcula sigma
        """
        if not self._calibration_file.exists():
            return {}

        import json
        try:
            data = json.loads(self._calibration_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

        if not isinstance(data, dict):
            return {}

        migrated = False
        new_data: dict[str, dict[str, Any]] = {}

        for key, entry in data.items():
            if not isinstance(entry, dict):
                continue

            # Detect old format without metric
            key_parts = key.split("_")
            has_metric = len(key_parts) >= 2 and key_parts[1] in ("high", "low")

            if not has_metric:
                entry.setdefault("last_updated", date.today().isoformat())
                if entry.get("forecast_errors") and "sigma" not in entry:
                    errs = entry["forecast_errors"]
                    entry["sigma"] = (sum(e * e for e in errs) / len(errs)) ** 0.5
                # Duplicate to high and low variants
                new_data[f"{key_parts[0]}_high_" + "_".join(key_parts[1:])] = {**entry, "metric": "high"}
                new_data[f"{key_parts[0]}_low_" + "_".join(key_parts[1:])] = {**entry, "metric": "low"}
                migrated = True
            else:
                entry.setdefault("last_updated", date.today().isoformat())
                if entry.get("forecast_errors") and "sigma" not in entry:
                    errs = entry["forecast_errors"]
                    entry["sigma"] = (sum(e * e for e in errs) / len(errs)) ** 0.5
                new_data[key] = entry

        if migrated:
            self._calibration_file.parent.mkdir(parents=True, exist_ok=True)
            self._calibration_file.write_text(
                json.dumps(new_data, indent=2), encoding="utf-8"
            )
            self._logger.info(
                "Calibration migration: upgraded %d old entries to per-metric format",
                len([k for k in new_data if new_data[k].get("metric")]),
            )

        return new_data

    def _save_calibration(self) -> None:
        import json
        try:
            self._calibration_file.parent.mkdir(parents=True, exist_ok=True)
            self._calibration_file.write_text(
                json.dumps(self._calibration, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    def _record_calibration(
        self,
        city: str,
        metric: str,
        lead_days: int,
        yes_won: bool,
        forecast_mean: float = 0.0,
        actual_temp: float | None = None,
        confidence: float = 0.7,
    ) -> None:
        """Graba resultado con key compuesto {city}_{metric}_{lead_days}d_m{month}.

        Separar high/low evita que errores en una metrica contaminen la otra.
        Outlier rejection: errores > 3σ o > 25F absolutos se rechazan.
        Confidence calibration: trackea win rate por nivel de agreement.
        """
        month = date.today().month
        key = f"{city}_{metric}_{lead_days}d_m{month}"
        if key not in self._calibration:
            self._calibration[key] = {
                "city": city,
                "metric": metric,
                "lead_days": lead_days,
                "month": month,
                "predictions": [],
                "forecast_errors": [],
            }
        entry = self._calibration[key]
        entry["predictions"].append(1.0 if yes_won else 0.0)

        if actual_temp is not None:
            error = actual_temp - forecast_mean
            current_sigma = entry.get("sigma")
            outlier_threshold = max(
                self._dt["outlier_sigma_mult"] * (current_sigma or 5.0),
                self._dt["outlier_abs_max"],
            )
            if current_sigma is not None and abs(error) > outlier_threshold:
                self._logger.warning(
                    "Calibration outlier rejected: %s error=%.1fF > %.1fF (σ=%.1fF)",
                    key, error, outlier_threshold, current_sigma,
                )
                rejected = entry.setdefault("rejected_outliers", [])
                rejected.append({
                    "error": error,
                    "forecast": forecast_mean,
                    "actual": actual_temp,
                    "date": date.today().isoformat(),
                })
                self._save_calibration()
                return
            entry["forecast_errors"].append(error)

        preds = entry["predictions"]
        entry["bias"] = (sum(preds) / len(preds) - 0.5) if preds else 0.0
        entry["count"] = len(preds)
        errors = entry["forecast_errors"]
        entry["mae"] = sum(abs(e) for e in errors) / len(errors) if errors else None
        if errors:
            squared = [e * e for e in errors]
            entry["sigma"] = (sum(squared) / len(squared)) ** 0.5
        entry["last_updated"] = date.today().isoformat()

        # Phase 5: confidence calibration curve — track win rate by agreement level
        agreement_bin = self._agreement_bin(confidence)
        calib_key = f"{city}_{metric}_{lead_days}d_m{month}_{agreement_bin}"
        if calib_key not in self._calibration:
            self._calibration[calib_key] = {"predictions": [], "count": 0}
        self._calibration[calib_key]["predictions"].append(1.0 if yes_won else 0.0)
        self._calibration[calib_key]["count"] = len(self._calibration[calib_key]["predictions"])

        self._save_calibration()

    def _get_calibration_stats(
        self, city: str, metric: str, lead_days: int, month: int
    ) -> tuple[float, float | None]:
        """Fase 4: ML Predictive Calibration (K-Nearest Neighbors approach).
        
        Utiliza regresión local por vecindad (KNN) basada en features para estimar
        el bias y la sigma, aprendiendo de ciudades, plazos y métricas similares.
        """
        W_CITY = 2.0
        W_METRIC = 2.0
        W_LEAD = 1.0
        W_MONTH = 0.5
        
        similar_errors = []
        similar_bias = []
        
        for key, entry in self._calibration.items():
            if not isinstance(entry, dict) or "count" not in entry:
                continue
                
            e_city = entry.get("city", "")
            e_metric = entry.get("metric", "")
            e_lead = float(entry.get("lead_days", lead_days))
            e_month = float(entry.get("month", month))
            
            # Distancia euclidiana ponderada
            dist = 0.0
            if e_city != city: dist += W_CITY
            if e_metric != metric: dist += W_METRIC
            dist += W_LEAD * abs(e_lead - lead_days)
            dist += W_MONTH * min(abs(e_month - month), 12 - abs(e_month - month))
            
            # Inverse distance weighting
            weight = 1.0 / (1.0 + dist)
            
            # Error de temperatura
            errors = entry.get("forecast_errors", [])
            if errors:
                avg_err = sum(errors) / len(errors)
                similar_bias.append((weight, count, avg_err))
                
            sigma = entry.get("sigma")
            if sigma is not None:
                similar_errors.append((weight, count, float(sigma)))
                
        total_w_bias = sum(w * min(c, 10) for w, c, _ in similar_bias)
        weighted_bias = sum(w * min(c, 10) * b for w, c, b in similar_bias) / total_w_bias if total_w_bias > 0 else 0.0
            
        total_w_err = sum(w * min(c, 10) for w, c, _ in similar_errors)
        weighted_sigma = sum(w * min(c, 10) * s for w, c, s in similar_errors) / total_w_err if total_w_err > 0 else None
            
        return weighted_bias, weighted_sigma

    def _get_calibration_quality(
        self, city: str, metric: str, lead_days: int, month: int
    ) -> float:
        """Estima la calidad de la calibración usando densidad local de KNN."""
        W_CITY = 2.0
        W_METRIC = 2.0
        W_LEAD = 1.0
        W_MONTH = 0.5
        
        density = 0.0
        for key, entry in self._calibration.items():
            if not isinstance(entry, dict) or "count" not in entry:
                continue
            e_city = entry.get("city", "")
            e_metric = entry.get("metric", "")
            e_lead = float(entry.get("lead_days", lead_days))
            e_month = float(entry.get("month", month))
            
            dist = 0.0
            if e_city != city: dist += W_CITY
            if e_metric != metric: dist += W_METRIC
            dist += W_LEAD * abs(e_lead - lead_days)
            dist += W_MONTH * min(abs(e_month - month), 12 - abs(e_month - month))
            
            weight = 1.0 / (1.0 + dist)
            count = min(int(entry.get("count", 0)), 15)
            density += weight * count
            
        # Normalizar a 0-1
        return min(1.0, density / 30.0)

    # --- Phase 5: ensemble shape analysis ---

    @staticmethod
    def _analyze_ensemble_shape(members: list[float]) -> dict[str, Any]:
        """Analiza forma de la distribucion del ensemble (31 miembros).

        Detecta bimodalidad y skewness para ajustar confianza.
        """
        n = len(members)
        if n < 5:
            return {"bimodal": False, "skew": 0.0}

        sorted_m = sorted(members)
        mean_val = sum(sorted_m) / n
        std_val = statistics.stdev(sorted_m) if n > 1 else 1.0

        m3 = sum((x - mean_val) ** 3 for x in sorted_m) / n
        skew = m3 / (std_val ** 3) if std_val > 0 else 0.0

        max_gap = 0.0
        max_gap_pos = 0
        for i in range(n - 1):
            gap = sorted_m[i + 1] - sorted_m[i]
            if gap > max_gap:
                max_gap = gap
                max_gap_pos = i

        avg_gap = (sorted_m[-1] - sorted_m[0]) / (n - 1) if n > 1 else 0
        bimodal = max_gap > 2.5 * avg_gap and max_gap > 1.2 * std_val

        return {"bimodal": bimodal, "skew": skew}

    # --- Phase 5: confidence calibration ---

    @staticmethod
    def _agreement_bin(confidence: float) -> str:
        """Segmenta confidence en bins para calibration curve."""
        if confidence >= 0.90:
            return "c90"
        elif confidence >= 0.80:
            return "c80"
        elif confidence >= 0.70:
            return "c70"
        elif confidence >= 0.60:
            return "c60"
        return "c50"

    # --- Phase 6: exit strategy ---

    def _manage_exits(self, balance: float) -> list[Trade]:
        """Gestiona salidas con 5 razones: time, take-profit, stop-loss, temp, prob.

        Orden de prioridad:
        1. Max hold time exceeded → exit
        2. Take-profit (price > 90%) → exit
        3. Stop-loss (price < entry * sl_ratio) → exit
        4. Temperature stop-loss (|forecast - bucket_center| > XF) → exit
        5. Probability delta (trailing) → partial/full exit
        """
        dt = self._dt
        if not dt.get("reposition_enabled", True):
            return []
        trades: list[Trade] = []
        now_ts = time.time()

        for mid, pt in list(self._pending_trades.items()):
            city_name = pt.get("city_name", "")
            target_date_str = pt.get("target_date", "")
            metric = pt.get("metric", "high")
            old_prob = float(pt.get("forecast_prob", 0.5))
            entry_price = float(pt.get("entry_price", 0.5))
            threshold = float(pt.get("threshold", 0))
            size_usdc = float(pt.get("size", 0))
            order_id = pt.get("order_id", "")
            bucket_lower_str = pt.get("bucket_lower")
            bucket_upper_str = pt.get("bucket_upper")

            if not city_name or not target_date_str or not order_id:
                continue

            try:
                target_date = date.fromisoformat(target_date_str)
            except (ValueError, TypeError):
                continue

            # 1. Time-based exit
            executed_at = pt.get("executed_at", "")
            if executed_at:
                try:
                    age_hours = (now_ts - datetime.fromisoformat(executed_at).timestamp()) / 3600.0
                    if age_hours > dt["exit_max_hold_hours"]:
                        self._logger.warning("Weather exit: %s max hold %.0fh exceeded", city_name, age_hours)
                        t = self._exit_full(mid, pt, "max_hold")
                        if t: trades.append(t)
                        continue
                except (ValueError, OSError):
                    pass

            # 2. Fetch current market price (once, reused for TP and SL)
            current_price = self._fetch_market_price(mid)

            # 3. Take-profit
            if current_price >= dt["exit_take_profit_threshold"]:
                self._logger.info("Weather exit: %s take-profit @ %.2f (entry=%.2f)", city_name, current_price, entry_price)
                t = self._exit_full(mid, pt, "take_profit")
                if t: trades.append(t)
                continue

            # 4. Stop-loss (price)
            if current_price < entry_price * dt["exit_stop_loss_ratio"]:
                self._logger.warning("Weather exit: %s stop-loss @ %.2f (entry=%.2f)", city_name, current_price, entry_price)
                t = self._exit_full(mid, pt, "stop_loss")
                if t: trades.append(t)
                continue

            # 5. Temperature stop-loss + probability delta need fresh forecast
            forecast = self._forecast_provider.get_forecast(city_name, target_date)
            if forecast is None:
                continue

            members = forecast.member_highs if metric == "high" else forecast.member_lows
            mean_val = sum(members) / len(members) if members else 0
            new_prob = self._compute_bucket_prob_from_pending(
                members, metric, bucket_lower_str, bucket_upper_str, pt
            )

            # 6. Temperature stop-loss
            temp_delta = abs(mean_val - threshold)
            if temp_delta > dt["exit_temp_stop_loss"]:
                self._logger.warning(
                    "Weather exit: %s temp-stop %.1fF from bucket (mean=%.1fF threshold=%.0fF)",
                    city_name, temp_delta, mean_val, threshold,
                )
                t = self._exit_full(mid, pt, "temp_stop")
                if t: trades.append(t)
                continue

            # 7. Probability delta with trailing threshold
            base_threshold = dt["reposition_prob_delta"]
            if dt["exit_trailing_enabled"]:
                base_threshold = self._get_trailing_threshold(target_date_str, base_threshold)

            prob_delta = old_prob - new_prob
            if prob_delta <= 0:
                continue

            if prob_delta < base_threshold:
                continue

            if prob_delta < base_threshold * 1.5:
                # Moderate shift: partial exit
                fraction = dt["exit_partial_fraction"]
                self._logger.info(
                    "Weather exit: %s partial (%.0f%%) prob %.0f%%->%.0f%% delta=%.0f%%",
                    city_name, fraction * 100, old_prob * 100, new_prob * 100, prob_delta * 100,
                )
                t = self._exit_fraction(mid, pt, "prob_delta", fraction)
            else:
                # Severe shift: full exit
                self._logger.info(
                    "Weather exit: %s full prob %.0f%%->%.0f%% delta=%.0f%%",
                    city_name, old_prob * 100, new_prob * 100, prob_delta * 100,
                )
                t = self._exit_full(mid, pt, "prob_delta")

            if t: trades.append(t)

        return trades

    def _fetch_market_price(self, mid: str) -> float:
        """Obtiene el precio YES actual via market provider."""
        return self._market_provider.get_market_price(mid)

    @staticmethod
    def _get_trailing_threshold(target_date_str: str, base_threshold: float) -> float:
        """Ajusta el threshold de salida segun tiempo hasta resolucion."""
        try:
            target = date.fromisoformat(target_date_str)
            hours_left = max(0, (target - date.today()).days * 24)
        except (ValueError, TypeError):
            return base_threshold

        if hours_left <= 6:
            return base_threshold * 0.33
        elif hours_left <= 12:
            return base_threshold * 0.5
        elif hours_left <= 24:
            return base_threshold * 0.75
        return base_threshold

    def _exit_full(self, mid: str, pt: dict, reason: str) -> Trade | None:
        return self._do_exit(mid, pt, reason, fraction=1.0)

    def _exit_fraction(self, mid: str, pt: dict, reason: str, fraction: float) -> Trade | None:
        return self._do_exit(mid, pt, reason, fraction=fraction)

    def _do_exit(self, mid: str, pt: dict, reason: str, fraction: float) -> Trade | None:
        """Ejecuta salida (total o parcial) con precio real de mercado."""
        token_id = pt.get("token_id", "")
        if not token_id:
            return None
        try:
            current_price = self._fetch_market_price(mid)
            position_size_usdc = float(pt.get("size", 0))
            exit_size_usdc = position_size_usdc * fraction
            shares = int(exit_size_usdc / current_price) if current_price > 0 else 0
            if shares <= 0:
                return None

            entry_price = float(pt.get("entry_price", 0.5))
            result = self._client.place_limit_order(
                token_id=token_id, side="SELL",
                price=current_price, size=shares, post_only=False,
            )
            order_id = result.get("order_id", "") or result.get("id", "")
            status = result.get("status", "")

            if status in ("live", "submitted", "filled"):
                if fraction >= 1.0:
                    self._pending_trades.pop(mid, None)
                else:
                    pt["size"] = position_size_usdc * (1.0 - fraction)
                self._save_pending_state()

                # Cooling period
                city = pt.get("city_name", "")
                tgt = pt.get("target_date", "")
                met = pt.get("metric", "")
                cooldown_key = f"{city}_{tgt}_{met}"
                self._exit_cooldown[cooldown_key] = time.time()

                realized_pnl = (
                    (current_price - entry_price) * exit_size_usdc / entry_price
                    if entry_price > 0 else 0.0
                )
                self._log_exit(mid, pt, reason, current_price, realized_pnl, fraction)

                if fraction >= 1.0:
                    self._log_weather_trade(
                        mid, pt, status="exited",
                        exit_price=current_price, exit_reason=reason,
                    )

                exit_sig = Signal(
                    market_id=mid, token_id=token_id, side="SELL",
                    price=current_price, size=exit_size_usdc,
                    confidence=1.0, strategy_name=self.name,
                    metadata={
                        "reason": reason, "city": pt.get("city_name"),
                        "realized_pnl": realized_pnl, "fraction": fraction,
                    },
                )
                return self._make_trade(exit_sig, order_id, "submitted")

        except Exception as e:
            self._logger.warning("Weather: exit failed for %s reason=%s: %s", mid[:14], reason, e)
        return None

    def _log_exit(self, mid: str, pt: dict, reason: str, exit_price: float, pnl: float, fraction: float) -> None:
        """Persiste exit a weather_exits.jsonl."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market_id": mid,
            "city": pt.get("city_name", ""),
            "metric": pt.get("metric", ""),
            "entry_price": pt.get("entry_price", 0),
            "exit_price": exit_price,
            "size_usdc": float(pt.get("size", 0)),
            "fraction": fraction,
            "reason": reason,
            "realized_pnl": pnl,
        }
        try:
            self._exits_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._exits_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass

    def _log_weather_trade(
        self, mid: str, pt: dict, status: str = "open",
        resolution: str | None = None,
        exit_price: float | None = None, exit_reason: str | None = None,
    ) -> None:
        """Persiste trade completo a data/weather_trades.jsonl con metadata."""
        entry_price = float(pt.get("entry_price", 0))
        size = float(pt.get("size", 0))
        record: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market_id": mid,
            "city": pt.get("city_name", ""),
            "metric": pt.get("metric", ""),
            "target_date": pt.get("target_date", ""),
            "threshold": pt.get("threshold"),
            "bucket_type": pt.get("bucket_type", ""),
            "entry_price": entry_price,
            "size_usdc": size,
            "model_prob": pt.get("forecast_prob"),
            "forecast_mean": pt.get("forecast_mean"),
            "forecast_std": pt.get("forecast_std"),
            "confidence": pt.get("confidence"),
            "status": status,
            "resolution": resolution,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
        }
        if exit_price is not None and entry_price > 0:
            record["realized_pnl"] = (exit_price - entry_price) * size / entry_price
        try:
            self._trades_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._trades_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass

    def get_performance(self) -> dict[str, Any]:
        """Calcula metricas de performance desde weather_trades.jsonl."""
        if not self._trades_file.exists():
            return {"total_trades": 0, "settled": 0, "win_rate": None}

        trades: list[dict] = []
        try:
            for line in self._trades_file.read_text(encoding="utf-8").strip().split("\n"):
                if line.strip():
                    trades.append(json.loads(line))
        except Exception:
            pass

        settled = [t for t in trades if t.get("status") == "settled"]
        exited = [t for t in trades if t.get("status") == "exited"]
        open_trades = [t for t in trades if t.get("status") == "open"]

        settled_wins = [t for t in settled if t.get("resolution") == "yes_won"]
        win_rate = len(settled_wins) / len(settled) if settled else None

        total_pnl = sum(
            t.get("realized_pnl", 0) or 0
            for t in settled + exited
        )

        exit_by_reason: dict[str, int] = {}
        for t in exited:
            reason = t.get("exit_reason", "unknown")
            exit_by_reason[reason] = exit_by_reason.get(reason, 0) + 1

        return {
            "total_trades": len(trades),
            "open": len(open_trades),
            "settled": len(settled),
            "settled_wins": len(settled_wins),
            "exited": len(exited),
            "win_rate": win_rate,
            "total_pnl_usdc": round(total_pnl, 2),
            "exit_by_reason": exit_by_reason,
        }

    @staticmethod
    def _compute_bucket_prob_from_pending(
        members: list[float],
        metric: str,
        bucket_lower: Any,
        bucket_upper: Any,
        pt: dict,
    ) -> float:
        """Recomputa probabilidad del bucket usando datos guardados en _pending_trades."""
        bucket_type = pt.get("bucket_type", "range")
        threshold = float(pt.get("threshold", 0))
        try:
            lower = float(bucket_lower) if bucket_lower is not None else None
            upper = float(bucket_upper) if bucket_upper is not None else None
        except (ValueError, TypeError):
            lower = None
            upper = None

        # Inferir límites faltantes desde threshold y bucket_type (legacy trades)
        if bucket_type == "below" and upper is None:
            upper = threshold
        if bucket_type == "above" and lower is None:
            lower = threshold

        if bucket_type == "below" and upper is not None:
            prob = len([m for m in members if m <= upper]) / len(members)
        elif bucket_type == "above" and lower is not None:
            prob = len([m for m in members if m >= lower]) / len(members)
        elif bucket_type == "range" and lower is not None and upper is not None:
            prob = len([m for m in members if lower <= m <= upper]) / len(members)
        else:
            prob = 0.5

        return max(0.01, min(0.99, prob))

    # --- Pending state persistence ---

    def _save_pending_state(self) -> None:
        """Persiste _pending_trades a weather_pending.json."""
        data = {
            "pending_trades": self._pending_trades,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._pending_file.parent.mkdir(parents=True, exist_ok=True)
            self._pending_file.write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )
        except Exception as e:
            self._logger.warning("Failed to save pending state: %s", e)

    def _load_pending_state(self) -> dict[str, dict[str, Any]]:
        """Carga _pending_trades desde weather_pending.json."""
        if not self._pending_file.exists():
            return {}
        try:
            data = json.loads(self._pending_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data.get("pending_trades", {})
        except Exception:
            self._logger.warning("Failed to load pending state, starting fresh")
        return {}

    def _reconcile_open_orders(self) -> None:
        """Cruza pending_trades de disco con ordenes abiertas del exchange.

        Si una orden en disco ya no esta en el exchange (filled/cancelled),
        se mueve a _pending_trades sin order_id para que _check_settlements
        la detecte como potencialmente resuelta.
        Si hay ordenes en el exchange que no estan en disco, se agregan
        con metadata minima.
        """
        # Cargar ordenes abiertas del exchange
        exchange_orders: dict[str, dict] = {}
        try:
            orders = self._client.get_orders() or []
            for o in orders:
                oid = o.get("id") or o.get("order_id", "")
                if oid:
                    exchange_orders[oid] = o
        except Exception:
            self._logger.debug("Could not fetch open orders for reconciliation")
            return

        # Marcar ordenes de disco que ya no estan en el exchange
        stale_disk: list[str] = []
        for mid, pt in self._pending_trades.items():
            oid = pt.get("order_id", "")
            if oid and oid not in exchange_orders:
                stale_disk.append(mid)
                pt["order_id"] = ""  # marca como filled, lista para settlement

        if stale_disk:
            self._logger.info(
                "Reconciliation: %d pending trades no longer open on exchange",
                len(stale_disk),
            )
            self._save_pending_state()

        # Agregar ordenes del exchange no trackeadas en disco
        for oid, o in exchange_orders.items():
            condition_id = o.get("condition_id", "")
            token_id = o.get("token_id", "")
            if not condition_id:
                continue
            if condition_id in self._pending_trades:
                # Ya trackeada, actualizar order_id si cambio
                if self._pending_trades[condition_id].get("order_id") != oid:
                    self._pending_trades[condition_id]["order_id"] = oid
                continue
            # Orden nueva no trackeada — agregar con metadata minima
            self._pending_trades[condition_id] = {
                "market_id": condition_id,
                "token_id": token_id,
                "side": o.get("side", ""),
                "price": float(o.get("price", 0)),
                "size": float(o.get("original_size", o.get("size", 0))),
                "order_id": oid,
                "city_name": "unknown",
                "target_date": "",
                "threshold": 0.0,
                "metric": "",
                "bucket_type": "",
                "forecast_mean": 0.0,
                "forecast_std": 0.0,
                "forecast_prob": 0.0,
                "entry_price": float(o.get("price", 0)),
                "executed_at": datetime.now(timezone.utc).isoformat(),
            }
            self._logger.warning(
                "Reconciliation: found untracked order %s on %s — added with minimal metadata",
                oid, condition_id[:14],
            )

    # --- Status ---

    def get_weather_status(self) -> dict[str, Any]:
        return {
            "enabled": self._enabled, "active": self._active,
            "platform": "Polymarket",
            "open_positions": len(self._pending_trades),
            "signals_generated": self._signals_generated,
            "trades_executed": self._trades_executed,
            "cities": len(CITY_SLUGS),
            "min_edge": self._min_edge_threshold,
            "max_trade_size": self._max_trade_size,
            "max_allocation": self._max_total_allocation,
            "calibration_entries": len(self._calibration),
        }

    @property
    def scan_interval_min(self) -> int:
        return self._scan_interval_min

    @property
    def is_enabled(self) -> bool:
        return self._enabled
