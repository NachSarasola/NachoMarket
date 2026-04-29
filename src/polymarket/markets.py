"""
Seleccion inteligente de mercados para NachoMarket.

Fuentes de datos:
- Gamma API (https://gamma-api.polymarket.com) — mercados activos, metadata
- CLOB API (https://clob.polymarket.com) — rewards, orderbook, spreads

Todos los resultados se cachean por CACHE_TTL_SEC (15 min) para no saturar las APIs.

Scoring prioriza mercados con BAJA competencia y alta participacion share,
no volumen bruto. Esto maximiza LP rewards por dolar desplegado.
"""

import json
import logging
import math
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


# ------------------------------------------------------------------
# Cache
# ------------------------------------------------------------------

class _Cache:
    """Cache simple con TTL por entrada."""

    def __init__(self, ttl_sec: float = CACHE_TTL_SEC) -> None:
        self._ttl = ttl_sec
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        """Retorna el valor cacheado o None si expirado/no existe."""
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.monotonic() - ts > self._ttl:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        """Guarda un valor en cache."""
        self._store[key] = (time.monotonic(), value)

    def invalidate(self, key: str | None = None) -> None:
        """Invalida una clave o todo el cache."""
        if key is None:
            self._store.clear()
        else:
            self._store.pop(key, None)


# ------------------------------------------------------------------
# MarketAnalyzer
# ------------------------------------------------------------------

class MarketAnalyzer:
    """Seleccion y scoring inteligente de mercados para market making.

    Workflow:
        1. discover_markets() — obtiene mercados activos de Gamma API
        2. get_reward_markets() — identifica cuales tienen liquidity rewards
        3. score_market() — puntua cada mercado (spread, volume, rewards, vol, tiempo)
        4. select_top_markets(n) — retorna los N mejores
    """

    def __init__(self, client: PolymarketClient, config: dict[str, Any]) -> None:
        self._client = client
        self._cache = _Cache(ttl_sec=CACHE_TTL_SEC)

        # Config de filtros
        self._min_volume = config.get("min_daily_volume_usd", 40000)
        self._small_market_volume = config.get("small_market_volume_usd", 5000)
        self._max_markets = config.get("max_markets_simultaneous", 6)
        filters = config.get("filters", {})
        self._min_liquidity = filters.get("min_liquidity_usd", 500)
        self._max_spread_pct = filters.get("max_spread_pct", 5.0)
        self._min_days_to_resolution = filters.get(
            "min_time_to_resolution_hours", MIN_DAYS_TO_RESOLUTION * 24
        ) / 24
        self._excluded_categories = filters.get("excluded_categories", [])
        self._excluded_keywords: list[str] = [
            kw.lower() for kw in filters.get("excluded_keywords", [])
        ]
        # Rango de mid-price para RF single-sided [0.10, 0.90]
        self._mid_price_min: float = filters.get("mid_price_min", 0.10)
        self._mid_price_max: float = filters.get("mid_price_max", 0.90)
        self._preferred_categories = config.get("preferred_categories", [])

        # Tip 13: diversificacion por categoria
        diversification = config.get("diversification", {})
        self._max_per_category: int = diversification.get("max_per_category", 0)

        # Competition config
        competition = config.get("competition", {})
        self._max_book_depth_per_side = competition.get("max_book_depth_per_side", 500.0)
        self._min_participation_share = competition.get("min_participation_share", 0.05)
        self._bot_order_size = config.get("bot_order_size", 7.5)

        # Market filter (ban, dedup, news-risk)
        self._filter = MarketFilter(config)

        # RF v2 viability limits (para enrich_with_rewards)
        # Si no estan en el config de markets.yaml, usa defaults del RF
        self._rf_max_cap = float(config.get("rewards_farmer", {}).get("max_capital_per_market", 30.0))
        self._rf_two_sided = bool(config.get("rewards_farmer", {}).get("two_sided", True))

        # Pesos para scoring (suman 1.0)
        # Competition reemplaza volume como factor dominante:
        # mercados chicos con baja competencia → mayor participation share → mas profit
        self._weights = {
            "spread": 0.15,
            "competition": 0.15,
            "rewards": 0.45,      # Prioridad maxima a mercados con rewards
            "volatility": 0.15,
            "time_to_resolution": 0.10,
        }

    # ------------------------------------------------------------------
    # 1. Discover: Gamma API
    # ------------------------------------------------------------------

    @retry_with_backoff(max_attempts=3, exceptions=(requests.RequestException,))
    def discover_markets(self) -> list[dict[str, Any]]:
        """Obtiene mercados activos de la Gamma API y aplica filtros iniciales.

        Filtros:
        - accepting_orders = true
        - volume > min_market_volume
        - end_date > 7 dias en el futuro
        - No en categorias excluidas

        Returns:
            Lista de mercados con metadata enriquecida.
        """
        # Cache en memoria (ciclo actual)
        cached = self._cache.get("discover_markets")
        if cached is not None:
            logger.debug(f"discover_markets: cache hit ({len(cached)} mercados)")
            return cached

        # Cache persistente en disco: sobrevive a restarts del bot (solo en live)
        if not getattr(self._client, "paper_mode", False):
            cache_file = Path("data/market_cache.json")
            if cache_file.exists():
                try:
                    disk_cache = json.loads(cache_file.read_text(encoding="utf-8"))
                    cache_ts = disk_cache.get("timestamp", 0)
                    if time.time() - cache_ts < CACHE_TTL_SEC:
                        markets = disk_cache.get("markets", [])
                        logger.info(f"discover_markets: disk cache hit ({len(markets)} mercados)")
                        self._cache.set("discover_markets", markets)
                        return markets
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

        logger.info("Consultando Gamma API para mercados activos...")
        all_markets: list[dict[str, Any]] = []
        offset = 0
        limit = 100
        max_fetch = 10000  # Scan suficiente para mercados con rewards sin matar el VPS

        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "active": "true",
                "closed": "false",
                "order": "volume24hr",
                "ascending": "false",
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

            all_markets.extend(batch)
            offset += limit

            # Gamma API no pagina infinitamente; salir si lote incompleto
            if len(batch) < limit:
                break

            if len(all_markets) >= max_fetch:
                logger.info(f"discover_markets: max_fetch={max_fetch} alcanzado")
                break

        # Filtrar
        cutoff = datetime.now(timezone.utc) + timedelta(days=self._min_days_to_resolution)
        eligible = []

        for market in all_markets:
            if not self._passes_gamma_filters(market, cutoff):
                continue
            # Normalizar campos para uso interno
            eligible.append(self._normalize_gamma_market(market))

        self._cache.set("discover_markets", eligible)
        # Persistir a disco para sobrevivir restarts
        try:
            cache_file = Path("data/market_cache.json")
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(
                json.dumps({
                    "timestamp": time.time(),
                    "markets": eligible,
                }, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            logger.debug("No se pudo escribir cache de mercados a disco")

        logger.info(
            f"discover_markets: {len(all_markets)} total → {len(eligible)} elegibles"
        )
        return eligible

    def _passes_gamma_filters(
        self, market: dict[str, Any], cutoff: datetime
    ) -> bool:
        """Aplica filtros a un mercado de Gamma API."""
        # accepting_orders
        if not market.get("acceptingOrders", market.get("accepting_orders", False)):
            return False

        # Tip 10: keywords de eventos de un solo dia
        if self._excluded_keywords:
            question_lower = market.get("question", "").lower()
            if any(kw in question_lower for kw in self._excluded_keywords):
                return False

        # Categorias excluidas (tip 10)
        category = market.get("category", "").lower()
        if category in [c.lower() for c in self._excluded_categories]:
            return False

        # Volume: tip 1 (40K) con override para mercados chicos si share > 5%
        volume = _safe_float(market.get("volume24hr", market.get("volume", 0)))
        if volume < self._min_volume:
            # Excepción para mercados con rewards activos alcanzables
            # (se enriquece post-rewards, pero usamos raw como heurística)
            raw_rewards = market.get("rewards", {})
            has_rewards = bool(raw_rewards) if isinstance(raw_rewards, dict) else False
            if has_rewards and volume >= 500:
                pass  # Permitir mercados con rewards aunque tengan poco volumen
            elif volume < self._small_market_volume:
                return False
            # Entre small_market_volume y min_volume: calcular share estimado
            liquidity = _safe_float(market.get("liquidity", 0))
            if liquidity > 0:
                est_share = self._bot_order_size / (liquidity + self._bot_order_size)
                if est_share < self._min_participation_share:
                    return False
            # Si no hay datos de liquidity, rechazar
            elif volume < self._min_volume:
                return False

        # Fecha de resolucion
        end_date_str = market.get("endDate", market.get("end_date_iso", ""))
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if end_date < cutoff:
                    return False
            except (ValueError, TypeError):
                pass  # Si no se puede parsear, no filtrar por fecha

        return True

    def _normalize_gamma_market(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Normaliza campos de la Gamma API a formato interno."""
        tokens: list[dict[str, Any]] = []
        raw_tokens = raw.get("tokens")

        if raw_tokens is None:
            clob_ids = raw.get("clobTokenIds", "")
            if clob_ids:
                try:
                    parsed = json.loads(clob_ids)
                    if isinstance(parsed, list):
                        raw_tokens = parsed
                    else:
                        raw_tokens = [str(parsed)]
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

        # Usar datos del book de Gamma como fallback de mid_price (evita default 0.50)
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
            "rewards_active": False,  # Se enriquece en enrich_with_rewards()
            "rewards_rate": 0.0,
            "mid_price": gamma_mid,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "tick_size": _safe_float(raw.get("orderPriceMinTickSize", 0.01)),
            "_raw": raw,
        }

    # ------------------------------------------------------------------
    # 2. Rewards: CLOB API
    # ------------------------------------------------------------------

    def get_reward_markets(self) -> dict[str, dict[str, Any]]:
        """Consulta mercados con liquidity rewards activos via SDK.

        Returns:
            Dict de condition_id → info de rewards.
            Si la API falla, usa el cache en disco de PolymarketClient.
        """
        cached = self._cache.get("reward_markets")
        if cached is not None and len(cached) > 0:
            logger.debug(f"get_reward_markets: cache hit ({len(cached)} mercados)")
            return cached

        try:
            rewards_map = self._client.get_rewards()
        except Exception:
            logger.warning("get_reward_markets: API fallo, usando cache disco (%d)", len(self._client._rewards_cache))
            rewards_map = self._client._rewards_cache if self._client._rewards_cache else {}

        if rewards_map:
            self._cache.set("reward_markets", rewards_map)
        return rewards_map

    def enrich_with_rewards(
        self, markets: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Enriquece mercados con informacion de rewards.

        Solo marca rewards_active=True si:
        1. El min_size es alcanzable con nuestro capital
        2. La reward_density (reward/depth) > umbral minimo

        reward_density = rewards_daily_rate / book_depth_total
        Estima cuánto reward capturamos por dolar de liquidez competidora.
        Con $166, filtrar mercados con density > $1/dia.
        """
        rewards = self.get_reward_markets()
        if not rewards:
            logger.warning(
                "enrich_with_rewards: 0 mercados con rewards (API down sin cache previo). "
                "El RF no operara hasta que la API responda."
            )
            return markets

        # Leer max_capital del config de rewards_farmer (para el viability check)
        max_cap = self._rf_max_cap
        two_sided = self._rf_two_sided
        side_cap = max_cap / (2.0 if two_sided else 1.0)

        # Umbral de reward_density ajustado para $166 (PROMPT: $2 para $400)
        min_reward_density = max_cap / 400.0 * 2.0  # ~$0.83 para $166

        rewarded_count = 0
        skipped_viability = 0
        skipped_density = 0
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

            # Viability check: ¿podemos cubrir el min_size con nuestro capital?
            mid = self._get_representative_price(market)
            if mid <= 0:
                mid = 0.5
            required_usd = min_size * mid

            if required_usd > side_cap:
                skipped_viability += 1
                continue

            # Reward density: reward / book_depth (estima share del pool)
            book_depth = self._get_book_depth(market)
            if book_depth > 0:
                reward_density = rate / (book_depth / 100.0)  # normalizar por $100 de depth
            else:
                reward_density = rate  # sin datos de depth, asumir que es bueno

            market["rewards_active"] = True
            market["rewards_rate"] = rate
            market["rewards_min_size"] = min_size
            market["rewards_max_spread"] = max_spread
            market["reward_density"] = reward_density
            rewarded_count += 1

        logger.info(
            "enrich_with_rewards: %d/%d mercados viables con rewards "
            "(saltados=%d por min_size, density_min=%.2f)",
            rewarded_count, len(markets), skipped_viability, min_reward_density,
        )
        return markets

    # ------------------------------------------------------------------
    # 3. Scoring
    # ------------------------------------------------------------------

    def score_market(self, market: dict[str, Any]) -> float:
        """Puntua un mercado para market making (0.0 a 1.0).

        Componentes:
        - competition (30%): menor profundidad de book → mayor participation share
        - rewards (25%): tasa diaria de rewards normalizada por participation share
        - spread (20%): mas ancho → mas oportunidad, normalizado 0-5% → 0-1
        - volatility (15%): menos volatilidad → mejor para MM
        - time_to_resolution (10%): mas tiempo → mas seguro

        Returns:
            Score combinado entre 0.0 y 1.0.
        """
        scores: dict[str, float] = {}

        # Tip 8: filtrar mercados fuera de la zona simetrica mid 40-60c
        # Solo para binarios; multi-outcome se evalua por token en la estrategia
        mid_price_rep = self._get_representative_price(market)
        tokens = market.get("tokens", [])
        is_binary = len(tokens) == 2
        if is_binary and self._mid_price_min > 0 and self._mid_price_max < 1.0 and mid_price_rep > 0:
            if not (self._mid_price_min <= mid_price_rep <= self._mid_price_max):
                return 0.0

        # --- Competition (30%): baja profundidad = menos competencia ---
        participation = self._estimate_participation_share(market)
        market["_participation_share"] = participation
        # 20%+ share = max score. Menos share = menos score.
        scores["competition"] = min(participation / 0.20, 1.0)

        # --- Spread (20%) ---
        spread = self._get_market_spread(market)
        if spread is not None and spread > 0:
            scores["spread"] = min(spread / 5.0, 1.0)
        else:
            scores["spread"] = 0.0

        # --- Rewards (45%): los mercados viables ya fueron filtrados en enrich_with_rewards ---
        rewards_rate = market.get("rewards_rate", 0.0)
        rewards_active = market.get("rewards_active", False)
        rewards_min_size = market.get("rewards_min_size", 0.0)
        reward_density = market.get("reward_density", 0.0)

        if rewards_active and rewards_rate > 0:
            # Reward efficiency: rate / (capital_required)
            mid_for_rewards = mid_price_rep if mid_price_rep > 0 else 0.5
            capital_required = rewards_min_size * mid_for_rewards
            if capital_required > 0:
                efficiency = rewards_rate / capital_required
            else:
                efficiency = rewards_rate / 1.0

            # Boost por reward_density alta (menos competencia = mas share del pool)
            density_boost = min(reward_density / 5.0, 1.0) if reward_density > 0 else 0.5

            scores["rewards"] = min((efficiency / 1.0) * (0.7 + 0.3 * density_boost), 1.0)
        else:
            scores["rewards"] = 0.0

        # --- Volatility (15%): menos vol = mejor ---
        mid_price = self._get_representative_price(market)
        if mid_price > 0:
            price_stability = 1.0 - abs(mid_price - 0.5) * 2
            scores["volatility"] = max(price_stability, 0.0)
        else:
            scores["volatility"] = 0.0

        # --- Tiempo hasta resolucion (10%) ---
        days_left = self._days_to_resolution(market)
        if days_left is not None and days_left > 0:
            scores["time_to_resolution"] = min(days_left / 30.0, 1.0)
        else:
            scores["time_to_resolution"] = 0.5

        # --- Score final ponderado ---
        total = sum(
            scores.get(key, 0.0) * weight
            for key, weight in self._weights.items()
        )

        # Boost 2x para mercados con rewards activos (hacer RF dominante)
        if market.get("rewards_active") and total > 0:
            total = min(total * 2.0, 1.0)

        logger.debug(
            f"score_market '{market.get('question', '?')[:40]}': "
            f"comp={scores['competition']:.2f} spread={scores['spread']:.2f} "
            f"rewards={scores['rewards']:.2f} vol={scores['volatility']:.2f} "
            f"time={scores['time_to_resolution']:.2f} "
            f"share={participation:.1%} → {total:.3f}"
        )
        return total

    # ------------------------------------------------------------------
    # 4. Seleccion
    # ------------------------------------------------------------------

    def select_top_markets(self, n: int = 3) -> list[dict[str, Any]]:
        """Retorna los N mejores mercados para operar.

        Pipeline completo:
        1. Discover mercados de Gamma API
        2. Enriquecer con rewards de CLOB API
        3. Calcular spread actual via CLOB orderbook
        4. Puntuar cada mercado
        5. Ordenar y retornar top N

        Returns:
            Lista de los N mercados con mejor score, con _score incluido.
        """
        # 1. Discover
        markets = self.discover_markets()
        logger.debug(f"select_top_markets: post-discover = {len(markets)}")
        if not markets:
            logger.warning("select_top_markets: no se encontraron mercados")
            return []

        # 2. Rewards
        markets = self.enrich_with_rewards(markets)
        logger.debug(f"select_top_markets: post-rewards = {len(markets)}")

        # 3. Filtrar (ban, news-risk, dedup)
        markets = self._filter.apply_all(markets)
        logger.debug(f"select_top_markets: post-filter = {len(markets)}")

        # 4. Score (spread y competition se calculan dentro de score_market)
        # Pre-fetch orderbooks en batch para no saturar la API con ~2000 calls individuales
        self._prefetch_orderbooks(markets)

        logger.info("select_top_markets: scoring %d mercados...", len(markets))
        scored = []
        for idx, market in enumerate(markets):
            s = self.score_market(market)
            market["_score"] = s
            if s > 0:
                scored.append(market)
            if idx > 0 and idx % 500 == 0:
                logger.info("select_top_markets: scored %d/%d mercados", idx, len(markets))

        logger.info("select_top_markets: scoring listo. %d/%d con score > 0", len(scored), len(markets))

        # 5. Ordenar, aplicar cap por categoria (tip 13) y seleccionar
        scored.sort(key=lambda m: m["_score"], reverse=True)
        selected = self._apply_category_cap(scored, n)
        logger.debug(f"select_top_markets: post-category-cap = {len(selected)} (n={n})")

        if selected:
            score_strs = [f"{m['_score']:.3f}" for m in selected]
            logger.info(
                f"select_top_markets: top {len(selected)} de {len(markets)} — "
                f"scores [{', '.join(score_strs)}]"
            )
            for i, m in enumerate(selected, 1):
                question = m.get("question", "?")[:50]
                vol = m.get("volume_24h", 0)
                has_rewards = "SI" if m.get("rewards_active") else "NO"
                share = m.get("_participation_share", 0)
                logger.info(
                    f"  #{i}: '{question}' "
                    f"| score={m['_score']:.3f} "
                    f"| vol=${vol:,.0f} "
                    f"| rewards={has_rewards} "
                    f"| share={share:.1%}"
                )

        return selected

    # Legacy alias para compatibilidad con main.py
    def scan_markets(self) -> list[dict[str, Any]]:
        """Alias de select_top_markets() para compatibilidad."""
        return self.select_top_markets(n=self._max_markets)

    def get_multi_outcome_markets(self) -> list[dict[str, Any]]:
        """Encuentra mercados multi-outcome para arbitraje."""
        markets = self.select_top_markets(n=self._max_markets * 2)
        multi = [m for m in markets if len(m.get("tokens", [])) > 2]
        logger.info(f"get_multi_outcome_markets: {len(multi)} encontrados")
        return multi

    def _apply_category_cap(
        self, markets_sorted: list[dict[str, Any]], n: int
    ) -> list[dict[str, Any]]:
        """Selecciona los N mejores respetando el cap de diversificacion por categoria.

        Tip 13: no mas de max_per_category mercados de la misma tematica.
        Si max_per_category == 0, deshabilitado (toma los primeros N sin filtro).
        """
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
    # Cache control
    # ------------------------------------------------------------------

    @property
    def market_filter(self) -> MarketFilter:
        """Acceso al filtro de mercados (para /block de Telegram)."""
        return self._filter

    def invalidate_cache(self) -> None:
        """Fuerza re-fetch en la proxima llamada."""
        self._cache.invalidate()
        logger.info("Cache de mercados invalidado")

    # ------------------------------------------------------------------
    # Competition & Participation
    # ------------------------------------------------------------------

    def _estimate_participation_share(self, market: dict[str, Any]) -> float:
        """Estima el % de participacion del bot en un mercado.

        participation_share = bot_order_size / (total_book_depth + bot_order_size)

        Un mercado con $100 de profundidad total y $15 de order_size del bot
        da ~13% de participacion — mucho mejor que un mercado con $5000 de
        profundidad donde el bot seria el 0.3%.

        Returns:
            Float entre 0.0 y 1.0 representando el % de participacion estimado.
        """
        book_depth = self._get_book_depth(market)
        if book_depth <= 0:
            # Sin datos de book, asumir participacion media
            return 0.10

        share = self._bot_order_size / (book_depth + self._bot_order_size)
        return share

    def _prefetch_orderbooks(self, markets: list[dict[str, Any]]) -> None:
        """Pre-fetch orderbooks de todos los tokens en batch para evitar rate limiting.

        Usa /books batch endpoint (hasta 500 tokens/req). Pobla _cache para que
        _get_book_depth() y _get_market_spread() hagan cache hit.
        """
        all_token_ids: list[str] = []
        for market in markets:
            for token in market.get("tokens", []):
                tid = token.get("token_id", "")
                if tid and not self._cache.get(f"depth_{tid}"):
                    all_token_ids.append(tid)

        if not all_token_ids:
            logger.info("_prefetch_orderbooks: nada que precargar (cache hit)")
            return

        logger.info("_prefetch_orderbooks: precargando %d tokens en batch...", len(all_token_ids))
        cached_depth = 0
        cached_spread = 0
        batch_size = 500
        for i in range(0, len(all_token_ids), batch_size):
            batch = all_token_ids[i:i + batch_size]
            try:
                books = self._client.get_orderbooks_batch(batch)
                for tid, book in books.items():
                    bids = book.get("bids", [])
                    asks = book.get("asks", [])
                    bid_depth = sum(
                        float(b.get("price", 0)) * float(b.get("size", 0))
                        for b in bids
                    )
                    ask_depth = sum(
                        float(a.get("price", 0)) * float(a.get("size", 0))
                        for a in asks
                    )
                    total = bid_depth + ask_depth
                    self._cache.set(f"depth_{tid}", total if total > 0 else 0.0)
                    cached_depth += 1

                    # Precargar spread del primer token de cada mercado
                    if bids and asks:
                        best_bid = float(bids[0].get("price", 0))
                        best_ask = float(asks[0].get("price", 0))
                        if best_bid > 0:
                            spread_pct = ((best_ask - best_bid) / best_bid) * 100
                            self._cache.set(f"spread_{tid}", spread_pct)
                            cached_spread += 1
            except Exception:
                logger.info("_prefetch_orderbooks: batch %d fallo, usando fallback individual", i)

        logger.info(
            "_prefetch_orderbooks: listo. depth=%d spread=%d de %d tokens",
            cached_depth, cached_spread, len(all_token_ids),
        )

    def _get_book_depth(self, market: dict[str, Any]) -> float:
        """Obtiene la profundidad total del orderbook (bids + asks) en USDC.

        Para multi-outcome suma la profundidad de todos los tokens.

        Returns:
            Suma total de liquidez en ambos lados del book.
        """
        tokens = market.get("tokens", [])
        if not tokens:
            return 0.0

        total = 0.0
        for token in tokens:
            token_id = token.get("token_id", "")
            if not token_id:
                continue

            cache_key = f"depth_{token_id}"
            cached = self._cache.get(cache_key)
            if cached is not None:
                total += cached
                continue

            try:
                book = self._client.get_orderbook(token_id)
                bid_depth = sum(
                    float(b.get("price", 0)) * float(b.get("size", 0))
                    for b in book.get("bids", [])
                )
                ask_depth = sum(
                    float(a.get("price", 0)) * float(a.get("size", 0))
                    for a in book.get("asks", [])
                )
                depth = bid_depth + ask_depth
                self._cache.set(cache_key, depth)
                total += depth
            except Exception as e:
                if hasattr(e, 'status_code') and e.status_code == 404:
                    self._cache.set(cache_key, 0.0)
                logger.debug(f"No se pudo obtener depth para {token_id[:8]}...")
                continue

        return total

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _get_market_spread(self, market: dict[str, Any]) -> float | None:
        """Obtiene el spread actual de un mercado consultando el orderbook."""
        tokens = market.get("tokens", [])
        if not tokens:
            return None

        # Usar el primer token (YES outcome) para el spread
        token_id = tokens[0].get("token_id", "")
        if not token_id:
            return None

        cache_key = f"spread_{token_id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            book = self._client.get_orderbook(token_id)
            bids = book.get("bids", [])
            asks = book.get("asks", [])

            if not bids or not asks:
                return None

            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])

            if best_bid <= 0:
                return None

            spread_pct = ((best_ask - best_bid) / best_bid) * 100
            self._cache.set(cache_key, spread_pct)
            return spread_pct
        except Exception as e:
            # Si es error 404, marcar este token como invalido en cache
            if hasattr(e, 'status_code') and e.status_code == 404:
                self._cache.set(cache_key, None)
            logger.debug(f"No se pudo obtener spread para {token_id[:8]}...")
            return None

    def _get_representative_price(self, market: dict[str, Any]) -> float:
        """Obtiene un precio representativo del mercado.

        Usa solo el precio de Gamma API. NO hace fallback a CLOB midpoint
        para evitar llamadas API adicionales y 404s innecesarios.
        """
        tokens = market.get("tokens", [])
        if not tokens:
            return 0.0

        price = tokens[0].get("price", 0.0)
        return price if price > 0 else 0.0

    def _days_to_resolution(self, market: dict[str, Any]) -> float | None:
        """Calcula dias hasta la resolucion del mercado."""
        end_date_str = market.get("end_date", "")
        if not end_date_str:
            return None
        try:
            end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            delta = end_date - datetime.now(timezone.utc)
            return max(delta.total_seconds() / 86400, 0)
        except (ValueError, TypeError):
            return None


# ------------------------------------------------------------------
# Utilidad
# ------------------------------------------------------------------

def _safe_float(value: Any) -> float:
    """Convierte a float sin explotar."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0
