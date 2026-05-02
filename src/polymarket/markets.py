"""
Seleccion inteligente de mercados para NachoMarket.

Fuentes de datos:
- Gamma API (https://gamma-api.polymarket.com) — mercados activos, metadata
- CLOB API (https://clob.polymarket.com) — rewards, orderbook, spreads

Pipeline de select_top_markets:
  1. discover_markets()      — Gamma API, filtros iniciales
  2. filter.apply_all()      — ban, dedup, news-risk
  3. enrich_with_rewards()   — marca rewards_active, rate, min_size (SIN depth aun)
  4. _prefetch_orderbooks()  — batch fetch de todos los orderbooks
  5. enrich_density()        — reward_density real con depth cacheado, aplica filtros
  6. score_market()          — puntua con datos completos
  7. category cap + top n

Todos los resultados se cachean por CACHE_TTL_SEC (15 min).
"""

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

from src.polymarket.client import PolymarketClient
from src.polymarket.market_filter import MarketFilter
from src.utils.resilience import retry_with_backoff

logger = logging.getLogger("nachomarket.markets")

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
CACHE_TTL_SEC = 900  # 15 minutos
MIN_DAYS_TO_RESOLUTION = 7
# Shadow orders no necesitan capital real → viabilidad se chequea contra el tope nocional
_SHADOW_MAX_NOTIONAL = 150.0


class _Cache:
    def __init__(self, ttl_sec: float = CACHE_TTL_SEC) -> None:
        self._ttl = ttl_sec
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.monotonic() - ts > self._ttl:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.monotonic(), value)

    def invalidate(self, key: str | None = None) -> None:
        if key is None:
            self._store.clear()
        else:
            self._store.pop(key, None)


class MarketAnalyzer:
    """Seleccion y scoring de mercados para rewards farming."""

    def __init__(self, client: PolymarketClient, config: dict[str, Any], reward_tracker: Any = None) -> None:
        self._client = client
        self._cache = _Cache(ttl_sec=CACHE_TTL_SEC)
        self._reward_tracker = reward_tracker

        self._min_volume = config.get("min_daily_volume_usd", 0)
        self._small_market_volume = config.get("small_market_volume_usd", 0)
        self._max_market_volume: float = config.get("max_market_volume_usd", 50000)
        self._max_markets = config.get("max_markets_simultaneous", 8)
        filters = config.get("filters", {})
        self._min_liquidity = filters.get("min_liquidity_usd", 200)
        self._max_spread_pct = filters.get("max_spread_pct", 8.0)
        self._min_days_to_resolution = filters.get(
            "min_time_to_resolution_hours", MIN_DAYS_TO_RESOLUTION * 24
        ) / 24
        self._excluded_categories = filters.get("excluded_categories", [])
        self._excluded_keywords: list[str] = [
            kw.lower() for kw in filters.get("excluded_keywords", [])
        ]
        self._mid_price_min: float = filters.get("mid_price_min", 0.05)
        self._mid_price_max: float = filters.get("mid_price_max", 0.95)
        self._min_reward_density: float = filters.get("min_reward_density", 0.005)
        self._preferred_categories = config.get("preferred_categories", [])

        diversification = config.get("diversification", {})
        self._max_per_category: int = diversification.get("max_per_category", 3)

        competition = config.get("competition", {})
        self._max_book_depth_per_side = competition.get("max_book_depth_per_side", 5000.0)
        self._min_participation_share = competition.get("min_participation_share", 0.005)
        self._bot_order_size = config.get("bot_order_size", 7.5)

        self._filter = MarketFilter(config)

        self._rf_min_pool = float(config.get("rewards_farmer", {}).get("min_rewards_pool_usd", 5.0))

        # Pesos recalibrados para small-cap: priorizar baja competencia sobre pool alto.
        # competition=0.35: si el book está vacío, nuestro $50 captura 100% del share.
        # accessibility=0.20: min_size bajo es crítico con poco capital.
        # rewards=0.20: pool grande no ayuda si no capturás share significativo.
        self._weights = {
            "spread": 0.10,
            "competition": 0.35,
            "rewards": 0.20,
            "accessibility": 0.20,
            "volatility": 0.05,
            "time_to_resolution": 0.10,
        }

    # ------------------------------------------------------------------
    # 1. Discover
    # ------------------------------------------------------------------

    @retry_with_backoff(max_attempts=3, exceptions=(requests.RequestException,))
    def discover_markets(self) -> list[dict[str, Any]]:
        """Obtiene mercados activos de Gamma API con early-stop."""
        cached = self._cache.get("discover_markets")
        if cached is not None:
            logger.debug("discover_markets: cache hit (%d mercados)", len(cached))
            return cached

        if not getattr(self._client, "paper_mode", False):
            cache_file = Path("data/market_cache.json")
            if cache_file.exists():
                try:
                    disk_cache = json.loads(cache_file.read_text(encoding="utf-8"))
                    cache_ts = disk_cache.get("timestamp", 0)
                    if time.time() - cache_ts < CACHE_TTL_SEC:
                        markets = disk_cache.get("markets", [])
                        logger.info("discover_markets: disk cache hit (%d mercados)", len(markets))
                        self._cache.set("discover_markets", markets)
                        return markets
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

        logger.info("Consultando Gamma API para mercados activos...")
        all_markets: list[dict[str, Any]] = []
        seen_cids: set[str] = set()
        limit = 100
        # Dos pases para captar tanto mercados de alto volumen como recien salidos.
        # Los Epstein-style (low vol pero rewards activos) estan profundos en el sort
        # por volumen pero arriba en el sort por createdAt DESC.
        scan_passes = [
            ("volume24hr", "false", 5000, 500),    # alto vol, mas profundo, more streak
            ("createdAt",  "false", 1000, 1000),   # recien creados, sin early-stop agresivo
            ("volume24hr", "true",  2000, 200),    # menor volumen primero: captura vol=0 con rewards
        ]

        for order_by, ascending, max_fetch_pass, early_stop_threshold in scan_passes:
            offset = 0
            no_rewards_streak = 0
            pass_count = 0
            while True:
                params = {
                    "limit": limit,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "order": order_by,
                    "ascending": ascending,
                }
                resp = requests.get(
                    f"{GAMMA_API_URL}/markets",
                    params=params,
                    timeout=(15, 120),
                )
                resp.raise_for_status()
                batch = resp.json()

                if not batch:
                    break

                batch_has_rewards = any(bool(m.get("rewards")) for m in batch)
                if not batch_has_rewards:
                    no_rewards_streak += len(batch)
                else:
                    no_rewards_streak = 0

                # Dedup por conditionId entre pases
                for m in batch:
                    cid = m.get("conditionId", "")
                    if cid and cid not in seen_cids:
                        seen_cids.add(cid)
                        all_markets.append(m)
                        pass_count += 1

                offset += limit

                if len(batch) < limit:
                    break
                if pass_count >= max_fetch_pass:
                    break
                if no_rewards_streak >= early_stop_threshold:
                    logger.info(
                        "discover_markets: early-stop pase=%s (%d sin rewards)",
                        order_by, no_rewards_streak,
                    )
                    break

        cutoff = datetime.now(timezone.utc) + timedelta(days=self._min_days_to_resolution)
        eligible = [
            self._normalize_gamma_market(m)
            for m in all_markets
            if self._passes_gamma_filters(m, cutoff)
        ]

        self._cache.set("discover_markets", eligible)
        try:
            cache_file = Path("data/market_cache.json")
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(
                json.dumps({"timestamp": time.time(), "markets": eligible}, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            pass

        logger.info("discover_markets: %d total → %d elegibles", len(all_markets), len(eligible))
        return eligible

    def _passes_gamma_filters(self, market: dict[str, Any], cutoff: datetime) -> bool:
        if not market.get("acceptingOrders", market.get("accepting_orders", False)):
            return False

        if self._excluded_keywords:
            question_lower = market.get("question", "").lower()
            if any(kw in question_lower for kw in self._excluded_keywords):
                return False

        category = market.get("category", "").lower()
        if category in [c.lower() for c in self._excluded_categories]:
            return False

        # Volumen maximo: rechazar mercados con volumen > max_market_volume (whale territory)
        volume = _safe_float(market.get("volume24hr", market.get("volume", 0)))
        if self._max_market_volume > 0 and volume > self._max_market_volume:
            return False

        end_date_str = market.get("endDate", market.get("end_date_iso", ""))
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if end_date < cutoff:
                    return False
            except (ValueError, TypeError):
                pass

        return True

    def _normalize_gamma_market(self, raw: dict[str, Any]) -> dict[str, Any]:
        tokens: list[dict[str, Any]] = []
        raw_tokens = raw.get("tokens")

        if raw_tokens is None:
            clob_ids = raw.get("clobTokenIds", "")
            if clob_ids:
                try:
                    parsed = json.loads(clob_ids)
                    raw_tokens = parsed if isinstance(parsed, list) else [str(parsed)]
                except (json.JSONDecodeError, ValueError):
                    raw_tokens = [t.strip() for t in clob_ids.split(",") if t.strip()]
            else:
                raw_tokens = []
        elif not isinstance(raw_tokens, list):
            raw_tokens = []

        for token in raw_tokens:
            if isinstance(token, dict):
                tokens.append({
                    "token_id": token.get("token_id", ""),
                    "outcome": token.get("outcome", ""),
                    "price": _safe_float(token.get("price", 0)),
                })
            elif isinstance(token, str) and token.strip():
                tokens.append({"token_id": token.strip(), "outcome": "", "price": 0.0})

        condition_id = raw.get("conditionId", raw.get("condition_id", ""))
        best_bid = _safe_float(raw.get("bestBid", 0))
        best_ask = _safe_float(raw.get("bestAsk", 0))
        last_trade = _safe_float(raw.get("lastTradePrice", 0))
        if best_bid > 0 and best_ask > 0:
            gamma_mid = round((best_bid + best_ask) / 2, 4)
        elif last_trade > 0:
            gamma_mid = last_trade
        else:
            gamma_mid = 0.5

        return {
            "condition_id": condition_id,
            "question": raw.get("question", ""),
            "category": raw.get("category", ""),
            "tokens": tokens,
            "volume_24h": _safe_float(raw.get("volume24hr", raw.get("volume", 0))),
            "liquidity": _safe_float(raw.get("liquidity", 0)),
            "end_date": raw.get("endDate", raw.get("end_date_iso", "")),
            "accepting_orders": True,
            "rewards_active": False,
            "rewards_rate": 0.0,
            "mid_price": gamma_mid,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "tick_size": _safe_float(raw.get("orderPriceMinTickSize", 0.01)),
            "_raw": raw,
        }

    # ------------------------------------------------------------------
    # 2. Rewards enrichment (marks active, NO density yet)
    # ------------------------------------------------------------------

    def get_reward_markets(self) -> dict[str, dict[str, Any]]:
        cached = self._cache.get("reward_markets")
        if cached is not None and len(cached) > 0:
            return cached

        try:
            rewards_map = self._client.get_rewards()
        except Exception:
            logger.warning("get_reward_markets: API fallo, usando cache disco")
            rewards_map = getattr(self._client, "_rewards_cache", {}) or {}

        if rewards_map:
            self._cache.set("reward_markets", rewards_map)
        return rewards_map

    def enrich_with_rewards(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Marca rewards_active, rate, min_size, max_spread.

        NO calcula reward_density aqui — se hace en enrich_density() despues
        del prefetch de orderbooks para tener depth real.
        Aplica filtro de min_rewards_pool_usd.
        """
        rewards = self.get_reward_markets()
        if not rewards:
            logger.warning(
                "enrich_with_rewards: 0 mercados con rewards (API down). RF no opera."
            )
            return markets

        rewarded_count = 0
        skipped_pool = 0
        skipped_viability = 0

        for market in markets:
            cid = market.get("condition_id", "")
            if cid not in rewards:
                continue

            r = rewards[cid]
            rate = float(r.get("rewards_daily_rate", 0.0))
            min_size = float(r.get("min_size", 0.0))
            max_spread = float(r.get("max_spread", 0.0))

            if rate <= 0:
                continue

            # Filtro: pool minimo de rewards (descarta mercados con $0.10/dia)
            if rate < self._rf_min_pool:
                skipped_pool += 1
                continue

            # Viability check: podemos cubrir el min_size con nuestro capital?
            # Usar mid_price de Gamma (bestBid/bestAsk) como fuente primaria.
            # _get_representative_price() devuelve 0 cuando Gamma no incluye
            # precios por token en el list endpoint, forzando fallback a 0.5 y
            # rechazando mercados low-mid que son perfectamente viables.
            mid = market.get("mid_price") or self._get_representative_price(market)
            if mid <= 0:
                mid = 0.5
            # Shadow orders: viabilidad contra tope nocional, no contra capital del bot
            required_usd = min_size * mid
            if required_usd > _SHADOW_MAX_NOTIONAL:
                skipped_viability += 1
                continue

            market["rewards_active"] = True
            market["rewards_rate"] = rate
            market["rewards_min_size"] = min_size
            market["rewards_max_spread"] = max_spread
            market["reward_density"] = 0.0  # se llena en enrich_density
            rewarded_count += 1

        logger.info(
            "enrich_with_rewards: %d/%d con rewards activos (skip_pool=%d skip_viability=%d)",
            rewarded_count, len(markets), skipped_pool, skipped_viability,
        )
        return markets

    # ------------------------------------------------------------------
    # 3. Prefetch orderbooks
    # ------------------------------------------------------------------

    def _prefetch_orderbooks(self, markets: list[dict[str, Any]]) -> None:
        """Batch fetch de todos los orderbooks. Pobla cache de depth y spread."""
        all_token_ids: list[str] = []
        for market in markets:
            for token in market.get("tokens", []):
                tid = token.get("token_id", "")
                if tid and self._cache.get(f"depth_{tid}") is None:
                    all_token_ids.append(tid)

        if not all_token_ids:
            logger.info("_prefetch_orderbooks: todo en cache")
            return

        logger.info("_prefetch_orderbooks: precargando %d tokens...", len(all_token_ids))
        cached_count = 0
        batch_size = 500
        for i in range(0, len(all_token_ids), batch_size):
            batch = all_token_ids[i:i + batch_size]
            try:
                books = self._client.get_orderbooks_batch(batch)
                for tid, book in books.items():
                    bids = book.get("bids", [])
                    asks = book.get("asks", [])
                    bid_depth = sum(float(b.get("price", 0)) * float(b.get("size", 0)) for b in bids)
                    ask_depth = sum(float(a.get("price", 0)) * float(a.get("size", 0)) for a in asks)
                    total = bid_depth + ask_depth
                    self._cache.set(f"depth_{tid}", total)
                    if bids and asks:
                        best_bid = float(bids[0].get("price", 0))
                        best_ask = float(asks[0].get("price", 0))
                        if best_bid > 0:
                            spread_pct = ((best_ask - best_bid) / best_bid) * 100
                            self._cache.set(f"spread_{tid}", spread_pct)
                    cached_count += 1
            except Exception:
                logger.debug("_prefetch_orderbooks: batch %d fallo", i)

        logger.info("_prefetch_orderbooks: %d/%d tokens cacheados", cached_count, len(all_token_ids))

    # ------------------------------------------------------------------
    # 4. Density enrichment (requiere orderbooks ya cacheados)
    # ------------------------------------------------------------------

    def enrich_density(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Calcula reward_density real con depth del orderbook y aplica filtro.

        reward_density = rewards_daily_rate / max(book_depth_usd, 100)
        Unidad: $/dia por $1 de liquidez competidora.

        Solo se ejecuta DESPUES de _prefetch_orderbooks() para tener depth real.
        Descarta mercados con density < min_reward_density.
        """
        if self._min_reward_density <= 0:
            return markets

        kept = []
        skipped = 0
        for market in markets:
            if not market.get("rewards_active"):
                kept.append(market)
                continue

            rate = float(market.get("rewards_rate", 0.0))
            book_depth = self._get_book_depth(market)
            density = rate / max(book_depth, 100.0)
            market["reward_density"] = round(density, 6)

            if density < self._min_reward_density:
                market["rewards_active"] = False  # deja de ser candidato RF
                skipped += 1
                logger.debug(
                    "enrich_density: skip %s density=%.4f < min=%.4f (depth=$%.0f)",
                    market.get("condition_id", "")[:12], density,
                    self._min_reward_density, book_depth,
                )
                continue
            kept.append(market)

        logger.info(
            "enrich_density: %d/%d conservados (skip_density=%d min=%.4f)",
            len(kept), len(markets), skipped, self._min_reward_density,
        )
        return markets  # retorna todos (score_market filtra por rewards_active)

    # ------------------------------------------------------------------
    # 5. Scoring
    # ------------------------------------------------------------------

    def score_market(self, market: dict[str, Any]) -> float:
        """Puntua un mercado (0.0 a 1.0).

        Gate de mid-price: descarta si la ventana de rewards sale de [0,1].
        Para binarios: si mid < max_spread + tick o mid > 1 - max_spread - tick,
        no hay room para la ventana → score 0.
        """
        tokens = market.get("tokens", [])
        is_binary = len(tokens) == 2

        mid_price = self._get_representative_price(market)
        tick_size = float(market.get("tick_size", 0.01))

        if is_binary and mid_price > 0:
            max_spread_usd = float(market.get("rewards_max_spread", 0.0)) / 100.0
            if max_spread_usd <= 0:
                max_spread_usd = 0.04
            # Ventana de rewards: [mid - max_spread, mid) — si sale de [0,1], sin valor
            if mid_price < max_spread_usd + tick_size or mid_price > 1.0 - max_spread_usd - tick_size:
                return 0.0

        scores: dict[str, float] = {}

        participation = self._estimate_participation_share(market)
        market["_participation_share"] = participation
        cid = market.get("condition_id", "")
        if self._reward_tracker and cid:
            share_pct = self._reward_tracker.last_share_pct(cid)
            if share_pct is not None and share_pct > 0:
                # Share real observado: mapeo exponencial
                if share_pct >= 1.0:
                    scores["competition"] = 1.0
                elif share_pct >= 0.10:
                    scores["competition"] = 0.7 + (share_pct - 0.10) / 0.90 * 0.3
                elif share_pct >= 0.01:
                    scores["competition"] = 0.3 + (share_pct - 0.01) / 0.09 * 0.4
                else:
                    scores["competition"] = share_pct / 0.01 * 0.3
                market["_observed_share_pct"] = share_pct
            else:
                # Sin share real: estimar por participación en el book
                scores["competition"] = _competition_score_from_participation(participation)
        else:
            scores["competition"] = _competition_score_from_participation(participation)

        spread = self._get_market_spread(market)
        scores["spread"] = min(spread / 5.0, 1.0) if spread and spread > 0 else 0.0

        rewards_rate = market.get("rewards_rate", 0.0)
        rewards_active = market.get("rewards_active", False)
        rewards_min_size = market.get("rewards_min_size", 0.0)
        reward_density = market.get("reward_density", 0.0)

        if rewards_active and rewards_rate > 0:
            mid_for_rewards = mid_price if mid_price > 0 else 0.5
            capital_required = rewards_min_size * mid_for_rewards
            efficiency = rewards_rate / capital_required if capital_required > 0 else rewards_rate
            density_boost = min(reward_density / 0.01, 1.0) if reward_density > 0 else 0.5
            scores["rewards"] = min(efficiency / 1.0 * (0.7 + 0.3 * density_boost), 1.0)
        else:
            scores["rewards"] = 0.0

        # Accessibility: mercados con min_size bajo son accesibles a capital chico.
        # min_size=20 -> 1.0, min_size=50 -> 0.6, min_size=100 -> 0.2, min_size>=200 -> 0
        # Razon: en Epstein min_size=20 ($2-4 capital) podemos colocar muchos multiplos del minimo.
        if rewards_active and rewards_min_size > 0:
            if rewards_min_size <= 20:
                scores["accessibility"] = 1.0
            elif rewards_min_size <= 50:
                scores["accessibility"] = 1.0 - (rewards_min_size - 20) / 75.0  # 50 -> 0.6
            elif rewards_min_size <= 200:
                scores["accessibility"] = max(0.0, 0.6 - (rewards_min_size - 50) / 250.0)
            else:
                scores["accessibility"] = 0.0
        else:
            scores["accessibility"] = 0.5  # neutral si no hay info

        if mid_price > 0:
            scores["volatility"] = max(1.0 - abs(mid_price - 0.5) * 2, 0.0)
        else:
            scores["volatility"] = 0.0

        days_left = self._days_to_resolution(market)
        scores["time_to_resolution"] = min(days_left / 30.0, 1.0) if days_left and days_left > 0 else 0.5
        total = sum(scores.get(k, 0.0) * w for k, w in self._weights.items())

        # Boost suave por rewards activos. Antes era x2 -> saturaba todos en 1.0
        # perdiendo diferenciacion entre min_size=200 y min_size=20.
        if market.get("rewards_active") and total > 0:
            total = min(total * 1.3, 1.0)

        volume_24h = float(market.get("volume_24h", 0))

        logger.debug(
            "score '%s': comp=%.2f spread=%.2f rewards=%.2f acc=%.2f vol=%.2f time=%.2f vol24h=$%.0f -> %.3f",
            market.get("question", "?")[:40],
            scores["competition"], scores["spread"], scores["rewards"],
            scores["accessibility"], scores["volatility"], scores["time_to_resolution"],
            volume_24h, total,
        )
        return total

    # ------------------------------------------------------------------
    # 6. Pipeline principal
    # ------------------------------------------------------------------

    def select_top_markets(self, n: int = 3) -> list[dict[str, Any]]:
        """Retorna los N mejores mercados para RF."""
        markets = self.discover_markets()
        if not markets:
            logger.warning("select_top_markets: no se encontraron mercados")
            return []
        logger.debug("select_top_markets: post-discover=%d", len(markets))

        markets = self._filter.apply_all(markets)
        logger.debug("select_top_markets: post-filter=%d", len(markets))

        markets = self.enrich_with_rewards(markets)
        logger.debug("select_top_markets: post-rewards=%d", len(markets))

        self._prefetch_orderbooks(markets)

        markets = self.enrich_density(markets)
        logger.debug("select_top_markets: post-density=%d", len(markets))

        # Solo scorear mercados con rewards activos — los demás no se pueden farmear
        markets = [m for m in markets if m.get("rewards_active") and m.get("rewards_rate", 0) > 0]
        logger.info("select_top_markets: scoring %d mercados...", len(markets))
        scored = []
        for market in markets:
            s = self.score_market(market)
            market["_score"] = s
            if s > 0:
                scored.append(market)

        logger.info("select_top_markets: %d/%d con score>0", len(scored), len(markets))

        # Sort: vol=0 primero absoluto, luego MAYOR pool, luego MENOR volumen, score DESC
        scored.sort(key=lambda m: (
            0 if float(m.get("volume_24h", 0)) == 0 else 1,
            -float(m.get("rewards_rate", 0)),
            float(m.get("volume_24h", 0)),
            -float(m.get("_score", 0)),
        ))

        if scored:
            top5 = scored[:5]
            logger.info(
                "select_top_markets sort top-5: %s",
                " | ".join(
                    f"#{i+1} vol=${m.get('volume_24h', 0):.0f} pool=${m.get('rewards_rate', 0):.0f} score={m.get('_score', 0):.3f}"
                    for i, m in enumerate(top5)
                ),
            )
        selected = self._apply_category_cap(scored, n)

        if selected:
            for i, m in enumerate(selected, 1):
                logger.info(
                    "  #%d '%s' score=%.3f vol=$%.0f rewards=$%.0f/d min_size=%d share=%.1f%%",
                    i, m.get("question", "?")[:50], m["_score"],
                    m.get("volume_24h", 0),
                    m.get("rewards_rate", 0),
                    int(m.get("rewards_min_size", 0)),
                    m.get("_participation_share", 0) * 100,
                )

        return selected

    def scan_markets(self) -> list[dict[str, Any]]:
        return self.select_top_markets(n=self._max_markets)

    def _apply_category_cap(self, markets_sorted: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        if self._max_per_category <= 0:
            return markets_sorted[:n]

        selected: list[dict[str, Any]] = []
        category_counts: dict[str, int] = {}
        for market in markets_sorted:
            if len(selected) >= n:
                break
            cat = market.get("category", "").lower() or "unknown"
            if category_counts.get(cat, 0) < self._max_per_category:
                selected.append(market)
                category_counts[cat] = category_counts.get(cat, 0) + 1
        return selected

    # ------------------------------------------------------------------
    # Cache / helpers publicos
    # ------------------------------------------------------------------

    @property
    def market_filter(self) -> MarketFilter:
        return self._filter

    def invalidate_cache(self) -> None:
        self._cache.invalidate()
        logger.info("Cache de mercados invalidado")

    # ------------------------------------------------------------------
    # Helpers privados
    # ------------------------------------------------------------------

    def _estimate_participation_share(self, market: dict[str, Any]) -> float:
        book_depth = self._get_book_depth(market)
        if book_depth <= 0:
            return 0.10
        return self._bot_order_size / (book_depth + self._bot_order_size)

    def _get_book_depth(self, market: dict[str, Any]) -> float:
        total = 0.0
        for token in market.get("tokens", []):
            tid = token.get("token_id", "")
            if not tid:
                continue
            cached = self._cache.get(f"depth_{tid}")
            if cached is not None:
                total += cached
                continue
            try:
                book = self._client.get_orderbook(tid)
                bid_depth = sum(float(b.get("price", 0)) * float(b.get("size", 0)) for b in book.get("bids", []))
                ask_depth = sum(float(a.get("price", 0)) * float(a.get("size", 0)) for a in book.get("asks", []))
                depth = bid_depth + ask_depth
                self._cache.set(f"depth_{tid}", depth)
                total += depth
            except Exception:
                logger.debug("No depth para %s...", tid[:8])
        return total

    def _get_market_spread(self, market: dict[str, Any]) -> float | None:
        tokens = market.get("tokens", [])
        if not tokens:
            return None
        tid = tokens[0].get("token_id", "")
        if not tid:
            return None
        cached = self._cache.get(f"spread_{tid}")
        if cached is not None:
            return cached
        try:
            book = self._client.get_orderbook(tid)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if not bids or not asks:
                return None
            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
            if best_bid <= 0:
                return None
            spread_pct = ((best_ask - best_bid) / best_bid) * 100
            self._cache.set(f"spread_{tid}", spread_pct)
            return spread_pct
        except Exception:
            return None

    def _get_representative_price(self, market: dict[str, Any]) -> float:
        tokens = market.get("tokens", [])
        if not tokens:
            return 0.0
        price = tokens[0].get("price", 0.0)
        return price if price > 0 else 0.0

    def _days_to_resolution(self, market: dict[str, Any]) -> float | None:
        end_date_str = market.get("end_date", "")
        if not end_date_str:
            return None
        try:
            end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            return max((end_date - datetime.now(timezone.utc)).total_seconds() / 86400, 0)
        except (ValueError, TypeError):
            return None


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def _competition_score_from_participation(participation: float) -> float:
    """Score de competencia exponencial: prioriza mercados donde capturamos mucho share."""
    if participation >= 0.10:
        return 1.0
    if participation >= 0.05:
        return 0.7 + (participation - 0.05) / 0.05 * 0.3
    if participation >= 0.01:
        return 0.3 + (participation - 0.01) / 0.04 * 0.4
    return participation / 0.01 * 0.3
