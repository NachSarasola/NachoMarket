"""
Seleccion inteligente de mercados para NachoMarket.

Fuentes de datos:
- Gamma API (https://gamma-api.polymarket.com) — mercados activos, metadata
- CLOB API (https://clob.polymarket.com) — rewards, orderbook, spreads

Todos los resultados se cachean por CACHE_TTL_SEC (15 min) para no saturar las APIs.
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import requests

from src.polymarket.client import PolymarketClient
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
        self._min_volume = config.get("min_daily_volume_usd", 10000)
        self._max_markets = config.get("max_markets_simultaneous", 5)
        filters = config.get("filters", {})
        self._min_liquidity = filters.get("min_liquidity_usd", 5000)
        self._max_spread_pct = filters.get("max_spread_pct", 5.0)
        self._min_days_to_resolution = filters.get(
            "min_time_to_resolution_hours", MIN_DAYS_TO_RESOLUTION * 24
        ) / 24
        self._excluded_categories = filters.get("excluded_categories", [])
        self._preferred_categories = config.get("preferred_categories", [])

        # Pesos para scoring (suman 1.0)
        self._weights = {
            "spread": 0.30,
            "volume": 0.25,
            "rewards": 0.20,
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
        cached = self._cache.get("discover_markets")
        if cached is not None:
            logger.debug(f"discover_markets: cache hit ({len(cached)} mercados)")
            return cached

        logger.info("Consultando Gamma API para mercados activos...")
        all_markets: list[dict[str, Any]] = []
        offset = 0
        limit = 100

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
                timeout=15,
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

        # Filtrar
        cutoff = datetime.now(timezone.utc) + timedelta(days=self._min_days_to_resolution)
        eligible = []

        for market in all_markets:
            if not self._passes_gamma_filters(market, cutoff):
                continue
            # Normalizar campos para uso interno
            eligible.append(self._normalize_gamma_market(market))

        self._cache.set("discover_markets", eligible)
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

        # Volume minimo
        volume = _safe_float(market.get("volume24hr", market.get("volume", 0)))
        if volume < self._min_volume:
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

        # Categorias excluidas
        category = market.get("category", "").lower()
        if category in [c.lower() for c in self._excluded_categories]:
            return False

        return True

    def _normalize_gamma_market(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Normaliza campos de la Gamma API a formato interno."""
        tokens = []
        for token in raw.get("tokens", raw.get("clobTokenIds", "").split(",") if raw.get("clobTokenIds") else []):
            if isinstance(token, dict):
                tokens.append({
                    "token_id": token.get("token_id", ""),
                    "outcome": token.get("outcome", ""),
                    "price": _safe_float(token.get("price", 0)),
                })
            elif isinstance(token, str) and token.strip():
                tokens.append({"token_id": token.strip(), "outcome": "", "price": 0.0})

        condition_id = raw.get("conditionId", raw.get("condition_id", ""))

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
            "_raw": raw,
        }

    # ------------------------------------------------------------------
    # 2. Rewards: CLOB API
    # ------------------------------------------------------------------

    @retry_with_backoff(max_attempts=3, exceptions=(requests.RequestException,))
    def get_reward_markets(self) -> dict[str, dict[str, Any]]:
        """Consulta mercados con liquidity rewards activos en la CLOB API.

        Returns:
            Dict de condition_id → info de rewards.
        """
        cached = self._cache.get("reward_markets")
        if cached is not None:
            logger.debug(f"get_reward_markets: cache hit ({len(cached)} mercados)")
            return cached

        logger.info("Consultando CLOB API para mercados con rewards activos...")
        try:
            resp = requests.get(
                f"{CLOB_API_URL}/rewards/markets",
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            logger.warning("No se pudo obtener rewards de CLOB API")
            raise

        # Indexar por condition_id
        rewards_map: dict[str, dict[str, Any]] = {}

        reward_list = data if isinstance(data, list) else data.get("data", [])
        for entry in reward_list:
            cid = entry.get("conditionId", entry.get("condition_id", ""))
            if cid:
                rewards_map[cid] = {
                    "rewards_daily_rate": _safe_float(entry.get("rewardsDailyRate", entry.get("rewards_daily_rate", 0))),
                    "min_size": _safe_float(entry.get("minSize", entry.get("min_size", 0))),
                    "max_spread": _safe_float(entry.get("maxSpread", entry.get("max_spread", 0))),
                }

        self._cache.set("reward_markets", rewards_map)
        logger.info(f"get_reward_markets: {len(rewards_map)} mercados con rewards")
        return rewards_map

    def enrich_with_rewards(
        self, markets: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Enriquece mercados con informacion de rewards."""
        try:
            rewards = self.get_reward_markets()
        except Exception:
            logger.warning("No se pudieron obtener rewards, continuando sin ellos")
            return markets

        for market in markets:
            cid = market.get("condition_id", "")
            if cid in rewards:
                market["rewards_active"] = True
                market["rewards_rate"] = rewards[cid].get("rewards_daily_rate", 0.0)
                market["rewards_min_size"] = rewards[cid].get("min_size", 0.0)
                market["rewards_max_spread"] = rewards[cid].get("max_spread", 0.0)

        return markets

    # ------------------------------------------------------------------
    # 3. Scoring
    # ------------------------------------------------------------------

    def score_market(self, market: dict[str, Any]) -> float:
        """Puntua un mercado para market making (0.0 a 1.0).

        Componentes:
        - spread (30%): mas ancho → mas oportunidad, normalizado 0-10% → 0-1
        - volume (25%): mas alto → mas actividad, log-normalizado
        - rewards (20%): tasa diaria de rewards normalizada
        - volatility (15%): menos volatilidad → mejor para MM
        - time_to_resolution (10%): mas tiempo → mas seguro

        Returns:
            Score combinado entre 0.0 y 1.0.
        """
        scores: dict[str, float] = {}

        # --- Spread ---
        spread = self._get_market_spread(market)
        # Spread optimo para MM: 1-5%. Muy ancho (>10%) puede ser riesgoso.
        if spread is not None and spread > 0:
            scores["spread"] = min(spread / 5.0, 1.0)
        else:
            scores["spread"] = 0.0

        # --- Volume ---
        volume = market.get("volume_24h", 0.0)
        if volume > 0:
            # Log-normalizar: $10k → 0.4, $100k → 0.7, $1M → 1.0
            import math
            scores["volume"] = min(math.log10(max(volume, 1)) / 6.0, 1.0)
        else:
            scores["volume"] = 0.0

        # --- Rewards ---
        rewards_rate = market.get("rewards_rate", 0.0)
        # Normalizar: $0 → 0.0, $100/dia → 0.5, $500+/dia → 1.0
        scores["rewards"] = min(rewards_rate / 500.0, 1.0)

        # --- Volatility (inversa: menos vol = mejor) ---
        volatility = market.get("volatility", 0.5)
        # Precio entre 0.2-0.8 = baja vol (bueno para MM)
        # Precio cerca de 0 o 1 = mercado casi resuelto (riesgoso)
        mid_price = self._get_representative_price(market)
        if mid_price > 0:
            # Distancia al 0.5 como proxy de estabilidad
            price_stability = 1.0 - abs(mid_price - 0.5) * 2  # 0.5→1.0, 0/1→0.0
            scores["volatility"] = max(price_stability, 0.0)
        else:
            scores["volatility"] = 0.0

        # --- Tiempo hasta resolucion ---
        days_left = self._days_to_resolution(market)
        if days_left is not None and days_left > 0:
            # 7 dias → 0.23, 30 dias → 1.0, >30 → 1.0
            scores["time_to_resolution"] = min(days_left / 30.0, 1.0)
        else:
            scores["time_to_resolution"] = 0.5  # Desconocido → neutro

        # --- Score final ponderado ---
        total = sum(
            scores.get(key, 0.0) * weight
            for key, weight in self._weights.items()
        )

        logger.debug(
            f"score_market '{market.get('question', '?')[:40]}': "
            f"spread={scores['spread']:.2f} vol={scores['volume']:.2f} "
            f"rewards={scores['rewards']:.2f} volatility={scores['volatility']:.2f} "
            f"time={scores['time_to_resolution']:.2f} → {total:.3f}"
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
        if not markets:
            logger.warning("select_top_markets: no se encontraron mercados")
            return []

        # 2. Rewards
        markets = self.enrich_with_rewards(markets)

        # 3+4. Score (spread se calcula dentro de score_market)
        for market in markets:
            market["_score"] = self.score_market(market)

        # 5. Ordenar y seleccionar
        markets.sort(key=lambda m: m["_score"], reverse=True)
        selected = markets[:n]

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
                logger.info(
                    f"  #{i}: '{question}' "
                    f"| score={m['_score']:.3f} "
                    f"| vol=${vol:,.0f} "
                    f"| rewards={has_rewards}"
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

    # ------------------------------------------------------------------
    # Cache control
    # ------------------------------------------------------------------

    def invalidate_cache(self) -> None:
        """Fuerza re-fetch en la proxima llamada."""
        self._cache.invalidate()
        logger.info("Cache de mercados invalidado")

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
        except Exception:
            logger.debug(f"No se pudo obtener spread para {token_id[:8]}...")
            return None

    def _get_representative_price(self, market: dict[str, Any]) -> float:
        """Obtiene un precio representativo del mercado."""
        tokens = market.get("tokens", [])
        if not tokens:
            return 0.0

        # Intentar price de Gamma
        price = tokens[0].get("price", 0.0)
        if price > 0:
            return price

        # Fallback: midpoint del primer token
        token_id = tokens[0].get("token_id", "")
        if token_id:
            try:
                return self._client.get_midpoint(token_id)
            except Exception:
                return 0.0
        return 0.0

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
