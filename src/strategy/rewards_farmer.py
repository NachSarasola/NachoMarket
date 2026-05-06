"""LP Rewards Farming Optimizer v3 — Shadow Quoting.

Estrategia "fantasma en el orderbook": presente para cobrar rewards,
invisible para los takers.

Politica de quoting por token (BUY y SELL):
  Ventana de rewards BID: [mid - max_spread, mid)
  Ventana de rewards ASK: (mid, mid + max_spread]

  BUY (bid):
    Si best_bid >= qual_low + SAFETY_TICKS * tick:
      bid = best_bid - SAFETY_TICKS * tick  (detras de la competencia)
    Si no:
      bid = qual_low  (borde lejano, minimo riesgo de fill)

  SELL (ask):
    Si best_ask <= qual_high - SAFETY_TICKS * tick:
      ask = best_ask + SAFETY_TICKS * tick  (detras de la competencia)
    Si no:
      ask = qual_high  (borde lejano, minimo riesgo de fill)

  SAFETY_TICKS = 2 (2 ticks detras del BBO = buffer de 2¢ en mercados con tick=0.01)

Danger zone: si una orden quedo al frente del book (≤1 tick del BBO),
cancelar y recolocar al extremo lejano inmediatamente.

Two-sided por TOKEN (no solo por mercado):
  BUY + SELL en el mismo token → Q_min = min(Qbid, Qask) = 3x vs solo BID.
  Sin SELL: Q_min = Qbid/3.

Gate de spread minimo: no operar si max_spread < 2¢ (sin room para esconderse).

Politica de cancelacion conservadora:
  Cancelar SOLO si: fuera de ventana | is_order_scoring=False |
  danger zone | mid movio >= max_spread/2 | size difiere > 30%.

Referencia: https://docs.polymarket.com/market-makers/liquidity-rewards
"""

import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.rewards_farmer")

SINGLE_SIDED_DIVISOR = 3.0   # c en Q_min formula
MIN_SHARES_FALLBACK = 20     # minimo practico si la API no reporta rewards_min_size
SAFETY_TICKS = 3             # ticks detras del BBO para evitar fills
DANGER_ZONE_TICKS = 2        # si estamos a ≤2 tick del BBO, huir
REPRICE_THRESHOLD_RATIO = 0.5  # recolocar si mid se movio >= max_spread * 0.5
SIZE_DRIFT_THRESHOLD = 0.30  # recolocar si size difiere > 30%
MIN_MAX_SPREAD_USD = 0.001   # no operar si max_spread < 0.1¢ (captura mercados de alto pool con spread ajustado)

# Shadow sizing — orders no se fillean, el tamaño no está limitado por capital real.
SHADOW_SIZE_MULT = 12         # ordenes en 12x min_size para capturar mas % del libro (capital $162)
MAX_ORDER_NOTIONAL = 500.0    # tope nocional por orden ($500); shadow orders no bloquean capital real

# Yield farming dinámico
GRACE_PERIOD_SEC = 360       # esperar 6min antes de aplicar non_earning (lag del endpoint ~1-2min + tiempo para que share crezca)
MIN_CENTS_PER_MIN = 0.05     # ~$0.72/day — mínimo aceptable para capital $162
NON_EARNING_BLOCK_HOURS = 1.0  # bloquear mercado 1h tras cancelar por non_earning (mínimo req. usuario)


def _qualifying_bid(
    mid: float,
    max_spread_usd: float,
    tick_size: float,
    best_bid: float,
    safety_ticks: int | None = None,
) -> float | None:
    """Calcula bid dentro de la ventana de rewards.

    Ventana valida: [mid - max_spread, mid)
    Coloca safety_ticks ticks detras del best_bid para minimizar fills.
    Si hay colchón (órdenes entre nosotros y el BBO), safety_ticks=1.
    Returns None si no se puede colocar dentro de la ventana.
    """
    if safety_ticks is None:
        safety_ticks = SAFETY_TICKS

    if max_spread_usd <= 0 or tick_size <=0 or mid <= 0:
        return None

    qual_low = round(round((mid - max_spread_usd + tick_size) / tick_size) * tick_size, 4)
    qual_high = round(round((mid - tick_size) / tick_size) * tick_size, 4)

    if qual_low > qual_high:
        return None

    if best_bid >= qual_low + safety_ticks * tick_size:
        bid = round(round((best_bid - safety_ticks * tick_size) / tick_size) * tick_size, 4)
    else:
        bid = qual_low

    bid = max(qual_low, min(qual_high, bid))
    bid = round(bid, 4)

    if bid < qual_low or bid > qual_high:
        return None
    return bid


def _qualifying_ask(
    mid: float,
    max_spread_usd: float,
    tick_size: float,
    best_ask: float,
    safety_ticks: int | None = None,
) -> float | None:
    """Calcula ask dentro de la ventana de rewards.

    Ventana valida: (mid, mid + max_spread]
    Coloca safety_ticks ticks por encima del best_ask para minimizar fills.
    Si hay colchon (ordenes entre nosotros y el BBO), safety_ticks=1.
    Returns None si no se puede colocar dentro de la ventana.
    """
    if safety_ticks is None:
        safety_ticks = SAFETY_TICKS

    if max_spread_usd <= 0 or tick_size <=0 or mid <= 0:
        return None

    qual_low = round(round((mid + tick_size) / tick_size) * tick_size, 4)
    qual_high = round(round((mid + max_spread_usd - tick_size) / tick_size) * tick_size, 4)

    if qual_low > qual_high:
        return None

    if best_ask > 0 and best_ask <= qual_high - safety_ticks * tick_size:
        ask = round(round((best_ask + safety_ticks * tick_size) / tick_size) * tick_size, 4)
    else:
        ask = qual_high

    ask = max(qual_low, min(qual_high, ask))
    ask = round(ask, 4)

    if ask < qual_low or ask > qual_high:
        return None
    return ask


def _in_danger_zone(order_price: float, side: str, best_bid: float, best_ask: float, tick_size: float) -> bool:
    """True si la orden esta a ≤ DANGER_ZONE_TICKS del frente del book."""
    if side == "BUY":
        return best_bid > 0 and order_price >= best_bid - DANGER_ZONE_TICKS * tick_size
    else:
        return best_ask > 0 and order_price <= best_ask + DANGER_ZONE_TICKS * tick_size


def _get_cushion_usd(
    book: list[dict],
    our_price: float,
    bbo_price: float,
) -> float:
    """Devuelve el volumen total en USD de las órdenes entre nuestro precio y el BBO."""
    if not book or bbo_price <= 0:
        return 0.0

    if our_price < bbo_price:  # BUY side
        price_low = our_price
        price_high = bbo_price
    else:  # SELL side
        price_low = bbo_price
        price_high = our_price

    cushion_usd = 0.0
    for level in book:
        price = float(level.get("price", level.get("p", 0.0)))
        size = float(level.get("size", level.get("s", 0.0)))
        if price_low < price < price_high:
            cushion_usd += price * size

    return cushion_usd


class RewardsFarmerStrategy(BaseStrategy):
    """Shadow quoting two-sided (BUY+SELL por token) para maximizar Q_min."""

    def __init__(
        self,
        client: Any,
        config: dict[str, Any],
        circuit_breaker: Any = None,
        reward_tracker: Any = None,
        market_filter: Any = None,
        ws_feed: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__("rewards_farmer", client, config, **kwargs)
        self._circuit_breaker = circuit_breaker
        self._reward_tracker = reward_tracker
        self._market_filter = market_filter
        self._ws_feed = ws_feed
        self._market_volume: dict[str, float] = {}
        rf_cfg = config.get("rewards_farmer", {})
        rf_safety = rf_cfg.get("rf_safety", {})
        self._min_cushion_usd = float(rf_safety.get("min_cushion_usd", 250.0))
        self._fast_cancel_enabled = bool(rf_safety.get("fast_cancel_enabled", True))
        self._imbalance_max_ratio = float(rf_safety.get("imbalance_max_ratio", 5.0))
        self._subscribed_tokens = set()
        markets_cfg = config.get("markets", config)

        self._max_capital_per_market = float(rf_cfg.get("max_capital_per_market", 33.0))
        self._min_rewards_pool_usd = float(rf_cfg.get("min_rewards_pool_usd", 5.0))
        self._max_markets = int(rf_cfg.get("max_markets_simultaneous", 4))
        self._two_sided = bool(rf_cfg.get("two_sided", True))
        self._merge_threshold = float(rf_cfg.get("inventory_merge_threshold", 5.0))
        self._min_share_pct = float(rf_cfg.get("competition_share_min", 0.005))
        self._max_mid_deviation = float(rf_cfg.get("max_mid_deviation", 0.45))

        tw = markets_cfg.get("time_windows", {})
        self._low_activity_hours: set[int] = set(tw.get("low_activity_hours", []))
        self._low_activity_factor: float = tw.get("low_activity_size_factor", 0.7)
        self._prime_hours: set[int] = set(tw.get("prime_placement_window_utc", [0, 1, 2, 3, 22, 23]))
        self._prime_boost: float = tw.get("prime_size_boost", 1.2)

        self._active_farms: dict[str, dict[str, Any]] = {}
        self._fill_inventory: dict[str, dict[str, float]] = {}
        self._reward_pct: dict[str, float] = {}
        self._pending_orders: dict[str, Signal] = {}
        self._order_placed_at: dict[str, float] = {}  # order_id → timestamp de colocación
        self._market_entry_ts: dict[str, float] = {}  # condition_id → timestamp primera orden

        # Exploration: rotar entre top candidates para medir c/min real
        self._exploration_results: dict[str, float] = {}  # cid → best_estimated_cpm
        self._explore_minutes: float = 5.0   # tiempo minimo antes de permitir rotacion
        self._last_reeval: float = time.time()  # inicializar con tiempo actual (no 0.0 que dispara re-eval inmediato)

        # Tracking de fill rate para auto-widen (si fill_rate > 5%, incrementar safety)
        self._orders_placed_today: int = 0
        self._orders_filled_today: int = 0

        self._reconcile_open_orders()

        self._logger.info(
            "RF v3 (shadow) inicializado: two_sided=%s safety_ticks=%d max_capital=%.0f max_mkts=%d",
            self._two_sided, SAFETY_TICKS, self._max_capital_per_market, self._max_markets,
        )

    def _reconcile_open_orders(self) -> None:
        """Pobla _pending_orders desde el exchange al arrancar con datos reales."""
        try:
            open_orders = self._client.get_positions() or []
            for o in open_orders:
                oid = o.get("id") or o.get("order_id") or ""
                if oid:
                    # Guardar datos necesarios para que execute() pueda procesar la orden
                    self._pending_orders[oid] = {
                        "market_id": o.get("condition_id", o.get("market_id", "")),
                        "price": float(o.get("price", 0.0)),
                        "size": float(o.get("original_size", o.get("size", 0.0))),
                        "side": o.get("side", ""),
                        "token_id": o.get("token_id", ""),
                    }
            if open_orders:
                self._logger.info("RF reconcilió %d ordenes abiertas del exchange", len(open_orders))
        except Exception:
            self._logger.warning("RF: no se pudo reconciliar ordenes abiertas al init")

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        rewards_rate = float(market_data.get("rewards_rate", 0.0))
        condition_id = market_data.get("condition_id", "")
        mid_price = float(market_data.get("mid_price", 0.0))

        # Gate: mercado bloqueado por non_earning o blacklist
        if self._market_filter and self._market_filter.is_banned({"condition_id": condition_id}):
            return False

        if self._min_rewards_pool_usd > 0 and rewards_rate < self._min_rewards_pool_usd:
            self._logger.info(
                "RF skip %s...: rewards_pool=%.2f (min %.2f)",
                condition_id[:12], rewards_rate, self._min_rewards_pool_usd,
            )
            return False

        # Gate: max_spread mínimo para tener room de esconderse
        max_spread_usd = float(market_data.get("rewards_max_spread", 0.0)) / 100.0
        if max_spread_usd > 0 and max_spread_usd < MIN_MAX_SPREAD_USD:
            self._logger.info(
                "RF skip %s...: max_spread=%.3f < min=%.2f (no hay room para shadow quoting)",
                condition_id[:12], max_spread_usd, MIN_MAX_SPREAD_USD,
            )
            return False

        tokens = market_data.get("tokens", [])
        if not tokens or len(tokens) < 2:
            self._logger.info("RF skip %s: tokens=%d", condition_id[:12], len(tokens))
            return False

        if mid_price <= 0.0:
            self._logger.info("RF skip %s: mid=%.4f", condition_id[:12], mid_price)
            return False

        if len(tokens) == 2 and abs(mid_price - 0.50) > self._max_mid_deviation:
            self._logger.info(
                "RF skip %s...: mid=%.4f fuera de rango",
                condition_id[:12], mid_price,
            )
            return False

        if self._detect_market_phase(market_data) == "live":
            return False

        if len(self._active_farms) >= self._max_markets and condition_id not in self._active_farms and condition_id not in self._market_entry_ts:
            return False

        # Gate de viabilidad: shadow orders no necesitan capital real — chequeamos contra
        # el tope nocional por orden, no contra el balance disponible.
        rewards_min_size = float(market_data.get("rewards_min_size", 0.0))
        min_shares_check = max(MIN_SHARES_FALLBACK, rewards_min_size)
        token_data = market_data.get("token_data", {})
        any_viable = False
        for tok in tokens:
            tid_c = tok.get("token_id", "")
            if not tid_c:
                continue
            tmid = float(token_data.get(tid_c, {}).get("mid_price", 0.0))
            if tmid <= 0:
                continue
            if min_shares_check * tmid <= MAX_ORDER_NOTIONAL:
                any_viable = True
                break
        if not any_viable:
            self._logger.info(
                "RF skip %s...: min_size=%d × mid supera tope nocional $%.0f",
                condition_id[:12], int(min_shares_check), MAX_ORDER_NOTIONAL,
            )
            return False

        return True

    def allocate_capital(self, candidate_markets: list[dict[str, Any]], available_cash: float) -> dict[str, float]:
        """Asigna capital a los mejores mercados usando earnings reales de la API.

        Usa share_pct * daily_rate como proxy de c/min (tiempo real, sin lag).
        Si no hay datos de share reales (RT vacio), usa el proxy de participacion
        basado en la densidad del libro (order_size / total_depth).
        """
        per_market_cap = available_cash * 0.95
        alloc: dict[str, float] = {}

        # Mapeo de metadata para candidatos y filtro de rango de precios (0.05 - 0.95)
        mkt_map = {}
        for m in candidate_markets:
            cid = m.get("condition_id", "")
            if not cid: continue
            mid = float(m.get("mid_price", 0.5))
            # Polymarket no paga rewards fuera de [0.05, 0.95]
            if mid > 0.95 or mid < 0.05:
                continue
            mkt_map[cid] = m

        active_cids = [
            cid for cid in mkt_map.keys()
            if not self._market_filter or not self._market_filter.is_banned({"condition_id": cid})
        ]
        if not active_cids:
            return alloc

        now = time.time()
        rt = self._reward_tracker

        # Compute estimated cpm for each candidate using RewardTracker data or Density Proxy
        estimated: dict[str, float] = {}
        all_share = rt.get_share_pct_map() if rt else {}
        all_rate = rt.get_daily_rate_map() if rt else {}
        for cid in active_cids:
            share = all_share.get(cid, 0)
            rate = all_rate.get(cid, 0)
            if share and rate:
                estimated[cid] = share * rate / 1440 * 100  # cents/min estimado REAL
            elif rate > 0:
                # Proxy basado en participacion estimada del MarketAnalyzer
                # (order_size / (depth + order_size))
                m_data = mkt_map.get(cid, {})
                share_proxy = float(m_data.get("_share_estimate", 0.02))
                estimated[cid] = (share_proxy * rate / 1440 * 100)
            else:
                estimated[cid] = 0.0

        # Track best ever observed
        for cid, cpm in estimated.items():
            prev = self._exploration_results.get(cid, 0)
            self._exploration_results[cid] = max(prev, cpm)

        best_cpm = max(self._exploration_results.values()) if self._exploration_results else 0

        # Get currently active markets
        current = [c for c in active_cids if c in self._market_entry_ts]
        min_explore = self._explore_minutes * 60  # seconds

        # Fill empty slots with best untested candidates
        if len(current) < self._max_markets:
            candidates = [c for c in active_cids if c not in self._market_entry_ts]
            candidates.sort(key=lambda c: estimated.get(c, 0), reverse=True)
            for c in candidates[:self._max_markets - len(current)]:
                self._market_entry_ts[c] = now
                current.append(c)
                self._logger.info("RF explore: +%s (est=%.1f¢/m)", c[:8], estimated.get(c, 0))

        # Rotate underperforming markets that had enough time
        for cid in list(current):
            entry = self._market_entry_ts.get(cid, now)
            if (now - entry) < min_explore:
                continue  # still warming up

            est = estimated.get(cid, 0)
            if best_cpm > 0.001 and est < best_cpm * 0.5:
                replacement = next((c for c in active_cids
                                    if c not in self._market_entry_ts
                                    and estimated.get(c, 0) > est), None)
                if replacement:
                    del self._market_entry_ts[cid]
                    self._market_entry_ts[replacement] = now
                    current.remove(cid)
                    current.append(replacement)
                    self._logger.info(
                        "RF rotate: %s=%.2f¢ -> %s=%.2f¢",
                        cid[:8], est, replacement[:8], estimated.get(replacement, 0),
                    )

        # Allocate to active markets (up to max_markets)
        # Cuando best_cpm == 0 (sin datos de earnings), usar el orden de
        # candidate_cids (que preserva el sort por score de select_top_markets)
        if best_cpm > 0.001:
            chosen = sorted(current, key=lambda c: estimated.get(c, 0), reverse=True)[:self._max_markets]
        else:
            # Sin earnings: preservar orden original de candidate_cids (scoring)
            cid_order = {c: i for i, c in enumerate(active_cids)}
            chosen = sorted(current, key=lambda c: cid_order.get(c, 999))[:self._max_markets]
        for c in chosen:
            alloc[c] = per_market_cap

        # Sync _active_farms: droppear mercados no elegidos, cancelar sus ordenes
        chosen_set = set(chosen)
        for cid in list(self._active_farms.keys()):
            if cid not in chosen_set:
                n = self._cancel_market_orders(cid)
                self._active_farms.pop(cid, None)
                self._logger.info(
                    "RF drop %s: %d orders cancelled, removed from active_farms", cid[:12], n,
                )

        if chosen:
            self._logger.info(
                "RF alloc: %d mkts x $%.0f | best=%.1f¢ | %s",
                len(chosen), per_market_cap, best_cpm,
                " ".join(f"{c[:8]}={estimated.get(c,0):.1f}¢" for c in chosen),
            )
        return alloc


    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera senales BUY y SELL por token para maximizar Q_min.

        Q_min con solo BID: Qbid/3.
        Q_min con BID+ASK en mismo token: min(Qbid, Qask) ≈ 3x.

        Shadow quoting: SAFETY_TICKS detras del BBO para minimizar fills.
        Danger zone implica que la orden se colocara al borde lejano (qual_low/high).
        """
        condition_id = market_data.get("condition_id", "")
        if not condition_id:
            return []
        self._market_volume[condition_id] = float(market_data.get("volume_24h", 0.0))

        tokens = market_data.get("tokens", [])
        token_data = market_data.get("token_data", {})
        if not tokens or len(tokens) < 2:
            return []

        rewards_rate = float(market_data.get("rewards_rate", 0.0))
        rewards_min_size = float(market_data.get("rewards_min_size", 0.0))
        rewards_max_spread = float(market_data.get("rewards_max_spread", 0.0))
        max_spread_usd = rewards_max_spread / 100.0 if rewards_max_spread > 0 else 0.04
        tick_size = float(market_data.get("tick_size", 0.01))
        min_shares = max(MIN_SHARES_FALLBACK, rewards_min_size)

        if max_spread_usd < MIN_MAX_SPREAD_USD:
            self._logger.info(
                "RF eval %s...: skip max_spread=%.4f < MIN=%.4f",
                condition_id[:12], max_spread_usd, MIN_MAX_SPREAD_USD,
            )
            return []

        available_cash = float(market_data.get("available_cash", self._max_capital_per_market))
        dynamic_cap = market_data.get("max_total_capital")
        if dynamic_cap is not None:
            max_total_usd = min(float(dynamic_cap), available_cash * 0.98)
        else:
            max_total_usd = min(self._max_capital_per_market, available_cash * 0.98)

        now_utc = datetime.now(timezone.utc).hour
        size_boost = 1.0
        if now_utc in self._prime_hours:
            size_boost = self._prime_boost
        elif now_utc in self._low_activity_hours:
            size_boost = self._low_activity_factor

        is_binary = len(tokens) == 2
        # Con two-sided (BUY+SELL por token), tenemos hasta 2 ordenes por token
        n_order_slots = 2 if self._two_sided else 1  # BUY + SELL por token
        n_tokens = len(tokens)

        # Pre-pass: detectar tokens viables (donde min_shares * t_mid <= max_total_usd).
        # Sin esto, el capital se reparte entre todos los tokens del binario aunque
        # solo quoteamos el barato. Resultado historico: orden de 181 shares cuando
        # min_size=200 -> no califica para rewards. Fix: dividir solo entre viables.
        viable_token_ids: set[str] = set()
        for token in tokens:
            tid_v = token.get("token_id", "")
            if not tid_v:
                continue
            td_v = token_data.get(tid_v, {})
            t_mid_v = float(td_v.get("mid_price", 0.0))
            if t_mid_v <= 0:
                continue
            # Shadow orders: viabilidad contra tope nocional, no contra capital
            if min_shares * t_mid_v <= MAX_ORDER_NOTIONAL:
                viable_token_ids.add(tid_v)

        if not viable_token_ids:
            self._logger.info(
                "RF eval %s...: skip (0 viable tokens, min_shares=%d)",
                condition_id[:12], int(min_shares),
            )
            return []

        n_viable = max(1, len(viable_token_ids))

        signals: list[Signal] = []
        token_signal_counts: dict[str, int] = {}  # cuantas senales tiene cada token

        # BUY only: priorizar el token más caro (ganador). Todo el capital a un solo lado.
        if not self._two_sided:
            # Ordenar tokens por precio descendente (el que tiene más probabilidad de ganar)
            sorted_tokens = sorted(
                [t for t in tokens if t.get("token_id", "") in viable_token_ids],
                key=lambda t: float(token_data.get(t.get("token_id", ""), {}).get("mid_price", 0.5)),
                reverse=True,
            )
            tokens_to_eval = sorted_tokens[:1]  # solo el "ganador"
        else:
            tokens_to_eval = [t for t in tokens if t.get("token_id", "") in viable_token_ids]

        for token in [t for t in tokens if t.get("token_id", "") in viable_token_ids]:
            tid = token.get("token_id", "")
            if tid and self._ws_feed and tid not in self._subscribed_tokens:
                self._ws_feed.subscribe(tid, self._on_ws_update, condition_id=condition_id)
                self._subscribed_tokens.add(tid)
            if not tid:
                continue
            if token not in tokens_to_eval:
                continue

            td = token_data.get(tid, {})
            t_mid = float(td.get("mid_price", 0.0))
            if t_mid <= 0:
                continue

            # Extraer BBO del orderbook WS
            bids_book = td.get("orderbook", {}).get("bids", [])
            asks_book = td.get("orderbook", {}).get("asks", [])
            if bids_book:
                raw = bids_book[0]
                best_bid = float(raw.get("price", raw.get("p", 0.0)))
                # Ignorar size=0 (REST fallback sin depth real)
                bid_size = float(raw.get("size", raw.get("s", 0.0)))
                if bid_size == 0:
                    best_bid = 0.0
            else:
                best_bid = float(td.get("best_bid", 0.0))

            if asks_book:
                raw = asks_book[0]
                best_ask = float(raw.get("price", raw.get("p", 0.0)))
                ask_size = float(raw.get("size", raw.get("s", 0.0)))
                if ask_size == 0:
                    best_ask = 0.0
            else:
                best_ask = float(td.get("best_ask", 0.0))

            # Capital por lado: two_sided=false -> todo al token mas barato
            if not self._two_sided:
                side_capital = max_total_usd  # $80 completo al YES
            else:
                side_capital = max_total_usd / (n_viable * n_order_slots)

            token_signal_counts[tid] = 0


            # Imbalance Protection
            bid_vol = sum(float(l.get("price", 0))*float(l.get("size", 0)) for l in bids_book[:5])
            ask_vol = sum(float(l.get("price", 0))*float(l.get("size", 0)) for l in asks_book[:5])
            buy_allowed = True
            sell_allowed = True
            if ask_vol > 0 and bid_vol / ask_vol > self._imbalance_max_ratio:
                sell_allowed = False # Too many bids, asks are weak and vulnerable to sweeps
                self._logger.info(f"Imbalance {tid[:8]}: bid_vol={bid_vol:.0f} ask_vol={ask_vol:.0f} -> SELL prohibido")
            if bid_vol > 0 and ask_vol / bid_vol > self._imbalance_max_ratio:
                buy_allowed = False # Too many asks, bids are weak
                self._logger.info(f"Imbalance {tid[:8]}: bid_vol={bid_vol:.0f} ask_vol={ask_vol:.0f} -> BUY prohibido")

            # --- BUY side ---
            # Calcular safety_ticks dinámico: 1 tick si hay colchón entre nosotros y el BBO
            our_bid_2t = best_bid - SAFETY_TICKS * tick_size
            cushion_bid = _get_cushion_usd(bids_book, our_bid_2t, best_bid)
            safety_ticks_bid = 2 if cushion_bid >= self._min_cushion_usd else SAFETY_TICKS
            bid_price = _qualifying_bid(t_mid, max_spread_usd, tick_size, best_bid, safety_ticks_bid) if buy_allowed else None
            if bid_price is not None:
                target_shares = _calc_shares(min_shares, size_boost, side_capital, bid_price, cushion_bid)
                # Threshold ESTRICTO a min_shares (antes era 0.9*min_shares -> ordenes
                # subnominales que Polymarket NO premia con rewards).
                if target_shares >= int(min_shares):
                    s_bid = t_mid - bid_price
                    score_bid = ((max_spread_usd - s_bid) / max_spread_usd) ** 2 if max_spread_usd > 0 else 0.0
                    signals.append(Signal(
                        strategy_name=self.name,
                        market_id=condition_id,
                        token_id=tid,
                        side="BUY",
                        price=bid_price,
                        size=float(target_shares),
                        confidence=0.7,
                        metadata={
                            "reason": f"rf_shadow_bid: rate={rewards_rate:.2f}",
                            "score": round(score_bid, 4),
                            "max_spread_usd": max_spread_usd,
                            "t_mid": t_mid,
                            "best_bid": best_bid,
                            "best_ask": best_ask,
                            "tick_size": tick_size,
                        },
                    ))
                    token_signal_counts[tid] += 1
                else:
                    self._logger.info(
                        "RF eval %s tid=%s...: BUY skip target=%d < min_size=%d (cap=$%.1f price=%.3f)",
                        condition_id[:12], tid[:8], target_shares,
                        int(min_shares), side_capital, bid_price,
                    )

            # --- SELL side (solo si two_sided) ---
            if self._two_sided:
                # Calcular safety_ticks dinámico: 1 tick si hay colchón entre nosotros y el BBO
                our_ask_2t = best_ask + SAFETY_TICKS * tick_size
                cushion_ask = _get_cushion_usd(asks_book, best_ask, our_ask_2t)
                safety_ticks_ask = 2 if cushion_ask >= self._min_cushion_usd else SAFETY_TICKS
                ask_price = _qualifying_ask(t_mid, max_spread_usd, tick_size, best_ask, safety_ticks_ask) if sell_allowed else None
                if ask_price is not None:
                    target_shares_ask = _calc_shares(min_shares, size_boost, side_capital, ask_price, cushion_ask)
                    if target_shares_ask >= int(min_shares):
                        s_ask = ask_price - t_mid
                        score_ask = ((max_spread_usd - s_ask) / max_spread_usd) ** 2 if max_spread_usd > 0 else 0.0
                        signals.append(Signal(
                            strategy_name=self.name,
                            market_id=condition_id,
                            token_id=tid,
                            side="SELL",
                            price=ask_price,
                            size=float(target_shares_ask),
                            confidence=0.7,
                            metadata={
                                "reason": f"rf_shadow_ask: rate={rewards_rate:.2f}",
                                "score": round(score_ask, 4),
                                "max_spread_usd": max_spread_usd,
                                "t_mid": t_mid,
                                "best_bid": best_bid,
                                "best_ask": best_ask,
                                "tick_size": tick_size,
                            },
                        ))
                        token_signal_counts[tid] += 1

        # Para binarios: descartar si ningun token tiene BUY+SELL (Q_min caeria 3x)
        if is_binary and self._two_sided:
            tokens_with_both_sides = sum(
                1 for tid, count in token_signal_counts.items() if count >= 2
            )
            if tokens_with_both_sides == 0:
                # Si no logramos BUY+SELL en ningún token, verificar que al menos
                # tenemos cobertura de ambos lados del mercado (BUY YES + BUY NO)
                buy_tokens = {s.token_id for s in signals if s.side == "BUY"}
                if len(buy_tokens) < 2:
                    self._logger.warning(
                        "RF eval %s...: sin cobertura two-sided → descartado",
                        condition_id[:12],
                    )
                    return []

        # Loguear Q_min estimado por token
        for tid, count in token_signal_counts.items():
            tok_signals = [s for s in signals if s.token_id == tid]
            if len(tok_signals) == 2:
                score_bid = next((s.metadata.get("score", 0) for s in tok_signals if s.side == "BUY"), 0)
                score_ask = next((s.metadata.get("score", 0) for s in tok_signals if s.side == "SELL"), 0)
                q_min = max(min(score_bid, score_ask), max(score_bid / SINGLE_SIDED_DIVISOR, score_ask / SINGLE_SIDED_DIVISOR))
                self._logger.info(
                    "RF eval %s tid=%s...: BUY@%.4f SELL@%.4f Q_min=%.4f (3x vs solo bid=%.4f)",
                    condition_id[:12], tid[:8],
                    next((s.price for s in tok_signals if s.side == "BUY"), 0),
                    next((s.price for s in tok_signals if s.side == "SELL"), 0),
                    q_min, score_bid / SINGLE_SIDED_DIVISOR,
                )
            elif len(tok_signals) == 1:
                sc = tok_signals[0].metadata.get("score", 0)
                self._logger.info(
                    "RF eval %s tid=%s...: %s only score=%.4f Q_min=%.4f (1/3x)",
                    condition_id[:12], tid[:8], tok_signals[0].side, sc, sc / SINGLE_SIDED_DIVISOR,
                )

        self._logger.info(
            "RF eval %s...: %d senales (tokens=%d)",
            condition_id[:12], len(signals), len(tokens),
        )
        return signals

    def _cancel_market_orders(self, condition_id: str) -> int:
        """Cancela todas las ordenes abiertas de un mercado especifico."""
        cancelled = 0
        try:
            open_orders = self._client.get_positions() or []
        except Exception:
            return 0
        for oo in open_orders:
            oo_market = oo.get("condition_id") or oo.get("market_id") or ""
            if oo_market != condition_id:
                continue
            oid = str(oo.get("id") or oo.get("order_id") or "")
            if not oid:
                continue
            try:
                self._client.cancel_order(oid)
                self._pending_orders.pop(oid, None)
                self._order_placed_at.pop(oid, None)
                if self._circuit_breaker:
                    self._circuit_breaker.order_closed()
                cancelled += 1
            except Exception:
                self._logger.debug("RF: error cancelando %s...", oid[:12])
        return cancelled


    async def _on_ws_update(self, token_id: str, ob_state: Any, change_type: str) -> None:
        if not self._fast_cancel_enabled:
            return
            
        bids = [{"price": p, "size": s} for p, s in ob_state.bids]
        asks = [{"price": p, "size": s} for p, s in ob_state.asks]
        best_bid = bids[0]["price"] if bids else 0.0
        best_ask = asks[0]["price"] if asks else 0.0
        
        # Buscar ordenes nuestras en este token
        for oid, order in list(self._pending_orders.items()):
            # order puede ser Signal o dict (legacy)
            if isinstance(order, dict):
                o_tid = order.get("token_id", "")
                o_side = order.get("side", "")
                o_price = order.get("price", 0)
            else:
                o_tid = getattr(order, "token_id", "")
                o_side = getattr(order, "side", "")
                o_price = getattr(order, "price", 0)
            if o_tid != token_id:
                continue
            
            side = o_side
            price = o_price
            
            cancel = False
            if side == "BUY" and best_bid > 0:
                cushion = _get_cushion_usd(bids, price, best_bid)
                if cushion < self._min_cushion_usd and best_bid - price <= 0.04:
                    cancel = True
            elif side == "SELL" and best_ask > 0:
                cushion = _get_cushion_usd(asks, best_ask, price)
                if cushion < self._min_cushion_usd and price - best_ask <= 0.04:
                    cancel = True
                    
            if cancel:
                try:
                    self._client.cancel_order(oid)
                    self._logger.warning(f"FAST CANCEL {oid[:8]}: cushion={cushion:.1f} < min {self._min_cushion_usd}")
                    self._pending_orders.pop(oid, None)
                    if self._circuit_breaker:
                        self._circuit_breaker.order_closed()
                except Exception as e:
                    self._logger.debug(f"FAST CANCEL fallo: {e}")
    def execute(self, signals: list[Signal]) -> list[Trade]:
        trades: list[Trade] = []
        if not signals:
            return trades

        # 1. Obtener ordenes abiertas del exchange
        open_orders: list[dict[str, Any]] = []
        try:
            open_orders = self._client.get_positions() or []
        except Exception:
            self._logger.exception("RF: error obteniendo ordenes abiertas")

        # Indexar por (token_id, side) — solo órdenes del mercado actual.
        # Sin este filtro, el cleanup cancela órdenes de otros mercados que
        # están en _pending_orders pero no en las señales del ciclo actual.
        current_market_id = signals[0].market_id if signals else ""
        open_by_token_side: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for oo in open_orders:
            oid = oo.get("id") or oo.get("order_id") or ""
            curr_pending = self._pending_orders.get(oid)
            if curr_pending is None:
                # No está en _pending_orders: podría ser orden huérfana o de otro mercado
                # Cachear market_id para cleanup posterior
                oo["_market_id_cache"] = oo.get("condition_id") or oo.get("market_id", "")
                tid = oo.get("asset_id") or oo.get("token_id") or ""
                side = str(oo.get("side", "")).upper()
                if tid and side in ("BUY", "SELL"):
                    open_by_token_side.setdefault((tid, side), []).append(oo)
                continue
            # Usar market_id del pending (reconciliado) o del open order
            pending_market = curr_pending.get("market_id", "") if isinstance(curr_pending, dict) else ""
            if pending_market and pending_market != current_market_id:
                continue
            tid = oo.get("asset_id") or oo.get("token_id") or ""
            side = str(oo.get("side", "")).upper()
            if tid and side in ("BUY", "SELL"):
                open_by_token_side.setdefault((tid, side), []).append(oo)

        to_cancel: list[str] = []
        to_place: list[Signal] = []
        signal_keys = {(s.token_id, s.side): s for s in signals}

        for sig in signals:
            key = (sig.token_id, sig.side)
            existing = open_by_token_side.get(key, [])
            if not existing:
                # No colocar si el mercado está bloqueado (p.ej. non_earning reciente)
                if self._market_filter and self._market_filter.is_banned({"condition_id": sig.market_id}):
                    continue
                if sig.market_id not in self._market_entry_ts:
                    self._market_entry_ts[sig.market_id] = time.time()
                to_place.append(sig)
                continue

            oo = existing[0]
            oo_price = float(oo.get("price", 0.0))
            oo_size = float(oo.get("original_size", oo.get("size", 0.0)))
            oid = str(oo.get("id", oo.get("order_id", "")))

            max_spread_usd = float(sig.metadata.get("max_spread_usd", 0.04))
            t_mid = float(sig.metadata.get("t_mid", sig.price))
            best_bid = float(sig.metadata.get("best_bid", 0.0))
            best_ask = float(sig.metadata.get("best_ask", 0.0))
            tick_size = float(sig.metadata.get("tick_size", 0.01))

            # Criterio 1: fuera de ventana de rewards
            # Redondear a 4dp para evitar falsos positivos por floating point (0.52-0.04=0.4800...004)
            win_low = max(0.0, round(t_mid - max_spread_usd, 4))
            win_high = min(1.0, round(t_mid + max_spread_usd, 4))
            if sig.side == "BUY":
                out_of_window = oo_price < win_low or oo_price >= t_mid
            else:
                out_of_window = oo_price <= t_mid or oo_price > win_high

            # Criterio 2: danger zone — orden al frente del book
            danger = _in_danger_zone(oo_price, sig.side, best_bid, best_ask, tick_size)

            # Criterio 3: non_earning — la orden lleva >= 3min y no genera ¢/min suficientes
            non_earning = False
            order_age_sec = 0.0
            vol_zero = self._market_volume.get(sig.market_id, 0.0) == 0.0
            if vol_zero:
                non_earning = False  # sin volumen no hay rewards que medir
                # danger se mantiene activo: si el mercado despierta, no queremos
                # ordenes en el frente del book que puedan ser filleadas
            elif oid and self._reward_tracker:
                placed_at = self._order_placed_at.get(oid, 0.0)
                order_age_sec = time.time() - placed_at if placed_at > 0 else 0.0
                if order_age_sec >= GRACE_PERIOD_SEC:
                    rate = self._reward_tracker.cents_per_min(sig.market_id)
                    if rate is not None:
                        # Track peak for this market (decay detection)
                        peak_key = f"_peak_{sig.market_id}"
                        current_peak = getattr(self, peak_key, 0.0)
                        if rate > current_peak:
                            setattr(self, peak_key, rate)
                            current_peak = rate
                        # non_earning: below absolute minimum OR decayed to <10% of peak
                        rel_floor = current_peak * 0.10 if current_peak > 0 else MIN_CENTS_PER_MIN
                        if rate < MIN_CENTS_PER_MIN or rate < rel_floor:
                            non_earning = True

            # Criterio 4: is_order_scoring
            not_scoring = False
            if oid:
                try:
                    not_scoring = not self._client.is_order_scoring(oid)
                except Exception:
                    pass

            # Criterio 5: mid se movio >= max_spread / 2
            repriced = abs(sig.price - oo_price) >= max_spread_usd * REPRICE_THRESHOLD_RATIO

            # Criterio 6: size difiere > 30% (drift significativo)
            size_drift = abs(sig.size - oo_size) / max(oo_size, 1e-9) > SIZE_DRIFT_THRESHOLD
            
            # Criterio 7: partial_fill — cualquier fill detectado (reposition immediately)
            # Usamos un pequeño buffer para evitar ruido de redondeo
            partial_fill = oo_size < (sig.size - 1.0)

            should_reprice = out_of_window or danger or non_earning or not_scoring or repriced or size_drift or partial_fill

            if should_reprice:
                # Evitar churn: si el nuevo precio es igual al existente, no cancelar
                # (ocurre en ventanas de 1 tick donde danger zone siempre es True)
                if sig.price == oo_price and not (out_of_window or not_scoring or non_earning):
                    self._logger.debug(
                        "RF: danger/size en ventana ajustada pero precio no cambia (%s %s @ %.4f) — manteniendo",
                        sig.side, sig.token_id[:8], oo_price,
                    )
                    continue

                reason = (
                    "out_of_window" if out_of_window
                    else "danger_zone" if danger
                    else "non_earning" if non_earning
                    else "not_scoring" if not_scoring
                    else "mid_drift" if repriced
                    else "partial_fill" if partial_fill
                    else "size_drift"
                )
                self._logger.info(
                    "RF: repricing %s %s tid=%s... @ %.4f → %.4f (%s)",
                    sig.market_id[:8], sig.side, sig.token_id[:8], oo_price, sig.price, reason,
                )
                if oid:
                    to_cancel.append(oid)

                if non_earning:
                    # Bloquear mercado — no re-colocar, liberar capital para reasignación
                    if self._market_filter:
                        self._market_filter.block_market_until(sig.market_id, NON_EARNING_BLOCK_HOURS)
                    self._market_entry_ts.pop(sig.market_id, None)
                    self._active_farms.pop(sig.market_id, None)
                    share_pct = self._reward_tracker.last_share_pct(sig.market_id)
                    share_str = f"share={share_pct:.4f}%" if share_pct else "share=0"
                    self._logger.info(
                        "RF non_earning cancel oid=%s... cond=%s... c/min=%.4f %s age=%.0fs block=%.0fmin",
                        oid[:8] if oid else "?",
                        sig.market_id[:12],
                        self._reward_tracker.cents_per_min(sig.market_id) or 0.0,
                        share_str,
                        order_age_sec,
                        NON_EARNING_BLOCK_HOURS * 60,
                    )
                else:
                    to_place.append(sig)
            else:
                self._logger.debug(
                    "RF: manteniendo %s %s tid=%s... @ %.4f",
                    sig.side, sig.market_id[:8], sig.token_id[:8], oo_price,
                )

        # Verificación universal: SOLO para el mercado actual
        # Si una orden del mercado actual no scorea, cancelarla
        current_open = [oo for oo in open_orders
                        if (oo.get("condition_id") or oo.get("market_id") or oo.get("_market_id_cache", "")) == current_market_id]
        for oo in current_open:
            oid_univ = str(oo.get("id", oo.get("order_id", "")))
            if not oid_univ:
                continue
            if oid_univ in to_cancel:
                continue
            try:
                if not self._client.is_order_scoring(oid_univ):
                    to_cancel.append(oid_univ)
                    self._logger.info(
                        "RF: orden no scoreando oid=%s... cancelando", oid_univ[:8]
                    )
            except Exception:
                pass

        # Limpiar ordenes del mercado actual que ya no estan en signals
        # NO cancelar órdenes de otros mercados — cada execute() maneja solo su mercado
        for (tid, side), oos in open_by_token_side.items():
            if (tid, side) in signal_keys:
                continue
            for oo in oos:
                oid = str(oo.get("id", oo.get("order_id", "")))
                if not oid:
                    continue
                oo_market = oo.get("_market_id_cache", "")
                if not oo_market:
                    oo_market = oo.get("condition_id", oo.get("market_id", ""))
                if not oo_market:
                    lookup_oid = str(oo.get("id", oo.get("order_id", "")))
                    if lookup_oid:
                        curr_pend = self._pending_orders.get(lookup_oid)
                        if curr_pend and isinstance(curr_pend, dict):
                            oo_market = curr_pend.get("market_id", "")
                # Solo cancelar si pertenece al mercado actual — no tocar otros
                if oo_market != current_market_id:
                    continue
                to_cancel.append(oid)
                self._logger.info(
                    "RF: cleanup %s tid=%s... market=%s...",
                    side, tid[:8], oo_market[:12],
                )

        for oid in to_cancel:
            try:
                self._client.cancel_order(oid)
                self._pending_orders.pop(oid, None)
                self._order_placed_at.pop(oid, None)
                if self._circuit_breaker:
                    self._circuit_breaker.order_closed()
            except Exception:
                self._logger.debug("RF: error cancelando %s...", oid[:12])

        if to_place:
            self._orders_placed_today += len(to_place)
            try:
                if hasattr(self._client, "post_batch_orders"):
                    batch_results = self._client.post_batch_orders(to_place)
                    for sig, res in zip(to_place, batch_results):
                        trade = self._build_trade(sig, res)
                        trades.append(trade)
                        if trade.order_id and trade.status not in ("error", "rejected"):
                            self._pending_orders[trade.order_id] = sig
                            self._order_placed_at[trade.order_id] = time.time()
                else:
                    for sig in to_place:
                        res = self._client.place_limit_order(
                            token_id=sig.token_id,
                            side=sig.side,
                            price=sig.price,
                            size=sig.size,
                            post_only=True,
                        )
                        trade = self._build_trade(sig, res)
                        trades.append(trade)
                        if trade.order_id and trade.status not in ("error", "rejected"):
                            self._pending_orders[trade.order_id] = sig
                            self._order_placed_at[trade.order_id] = time.time()
            except Exception:
                self._logger.exception("RF: error colocando ordenes")

        # Log fill rate diario (warning si > 5%)
        if self._orders_placed_today > 0:
            fill_rate = self._orders_filled_today / self._orders_placed_today
            if fill_rate > 0.05:
                self._logger.warning(
                    "RF: fill_rate=%.1f%% (%d fills / %d placed) — considerar aumentar SAFETY_TICKS",
                    fill_rate * 100, self._orders_filled_today, self._orders_placed_today,
                )

        for sig in signals:
            if sig.market_id not in self._active_farms:
                self._active_farms[sig.market_id] = {}
            self._active_farms[sig.market_id][(sig.token_id, sig.side)] = {
                "side": sig.side,
                "size": sig.size,
                "price": sig.price,
            }

        return trades

    # ------------------------------------------------------------------
    # Fills e inventario
    # ------------------------------------------------------------------

    def record_fill(
        self,
        token_id: str,
        side: str,
        size: float,
        market_id: str,
        tokens: list[dict[str, Any]],
    ) -> None:
        self._orders_filled_today += 1
        if market_id not in self._fill_inventory:
            self._fill_inventory[market_id] = {}
        inv = self._fill_inventory[market_id]
        if side == "BUY":
            inv[token_id] = inv.get(token_id, 0.0) + size
        elif side == "SELL":
            inv[token_id] = max(0.0, inv.get(token_id, 0.0) - size)

    def get_fill_inventory(self, market_id: str) -> dict[str, float]:
        return dict(self._fill_inventory.get(market_id, {}))

    def should_merge(self, market_id: str) -> bool:
        inv = self._fill_inventory.get(market_id, {})
        if len(inv) != 2:
            return False
        vals = list(inv.values())
        return min(abs(vals[0]), abs(vals[1])) >= self._merge_threshold

    def mark_merged(self, market_id: str, merged_amount: float) -> None:
        if market_id not in self._fill_inventory:
            return
        inv = self._fill_inventory[market_id]
        if len(inv) != 2:
            return
        keys = list(inv.keys())
        inv[keys[0]] = max(0.0, inv.get(keys[0], 0.0) - merged_amount)
        inv[keys[1]] = max(0.0, inv.get(keys[1], 0.0) - merged_amount)

    def reset_daily_counters(self) -> None:
        """Llamar al reset diario del circuit breaker."""
        self._orders_placed_today = 0
        self._orders_filled_today = 0

    def update_reward_pct(self, percentages: dict[str, float]) -> None:
        self._reward_pct.update(percentages)

    def get_low_share_markets(self) -> list[str]:
        return [
            cid for cid, pct in self._reward_pct.items()
            if pct < self._min_share_pct and cid in self._active_farms
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_trade(self, sig: Signal, result: dict[str, Any]) -> Trade:
        status = result.get("status", "unknown")
        order_id = result.get("order_id", result.get("orderID", ""))
        return Trade(
            timestamp=datetime.now(timezone.utc).isoformat(),
            strategy_name=self.name,
            market_id=sig.market_id,
            token_id=sig.token_id,
            side=sig.side,
            price=sig.price,
            size=sig.size,
            status=status,
            order_id=order_id,
        )

    @staticmethod
    def _detect_market_phase(market_data: dict[str, Any]) -> str:
        category = str(market_data.get("category", "")).lower()
        if category in ("sports", "esports"):
            end_str = str(market_data.get("end_date", ""))
            if end_str:
                try:
                    end_date = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    hours_remaining = (end_date - datetime.now(timezone.utc)).total_seconds() / 3600.0
                except (ValueError, TypeError):
                    hours_remaining = 9999
                if hours_remaining > 3:
                    return "pre_game"
                elif hours_remaining > 0:
                    return "live"
                else:
                    return "post_game"
        return "standard"


def _calc_shares(min_shares: float, size_boost: float, side_capital: float, price: float, cushion_usd: float = 0.0) -> int:
    """Calcula shares objetivo usando Dynamic Yield Scaling.
    Si el colchón es alto, somos agresivos (yield máximo). Si es bajo, conservadores.
    """
    if cushion_usd >= 1000.0:
        multiplier = SHADOW_SIZE_MULT  # 12x
    elif cushion_usd >= 250.0:
        multiplier = SHADOW_SIZE_MULT / 2  # 6x
    else:
        multiplier = 1.1  # Apenas el mínimo para farmear

    target = min_shares * multiplier * size_boost
    # El capital real se respeta (max_capital_per_market = 999.0 asegura que haya lugar,
    # pero el balance real nos limita).
    max_by_notional = MAX_ORDER_NOTIONAL / price if price > 0 else 0
    max_by_capital = side_capital / price if price > 0 else 0
    cap = min(max_by_notional, max_by_capital) if max_by_capital > 0 else max_by_notional
    return math.floor(min(target, cap))
