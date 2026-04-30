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
from datetime import datetime, timezone
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.rewards_farmer")

SINGLE_SIDED_DIVISOR = 3.0   # c en Q_min formula
MIN_SHARES_FALLBACK = 20     # minimo practico si la API no reporta rewards_min_size
SAFETY_TICKS = 2             # ticks detras del BBO para evitar fills
DANGER_ZONE_TICKS = 1        # si estamos a ≤1 tick del BBO, huir
REPRICE_THRESHOLD_RATIO = 0.5  # recolocar si mid se movio >= max_spread * 0.5
SIZE_DRIFT_THRESHOLD = 0.30  # recolocar si size difiere > 30%
MIN_MAX_SPREAD_USD = 0.02    # no operar si max_spread < 2¢ (sin room para esconderse)


def _qualifying_bid(
    mid: float,
    max_spread_usd: float,
    tick_size: float,
    best_bid: float,
) -> float | None:
    """Calcula bid dentro de la ventana de rewards con SAFETY_TICKS de buffer.

    Ventana valida: [mid - max_spread, mid)
    Coloca SAFETY_TICKS ticks detras del best_bid para minimizar fills.
    Returns None si no se puede colocar dentro de la ventana.
    """
    if max_spread_usd <= 0 or tick_size <= 0 or mid <= 0:
        return None

    qual_low = round(round((mid - max_spread_usd + tick_size) / tick_size) * tick_size, 4)
    qual_high = round(round((mid - tick_size) / tick_size) * tick_size, 4)

    if qual_low > qual_high:
        return None

    if best_bid >= qual_low + SAFETY_TICKS * tick_size:
        bid = round(round((best_bid - SAFETY_TICKS * tick_size) / tick_size) * tick_size, 4)
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
) -> float | None:
    """Calcula ask dentro de la ventana de rewards con SAFETY_TICKS de buffer.

    Ventana valida: (mid, mid + max_spread]
    Coloca SAFETY_TICKS ticks por encima del best_ask para minimizar fills.
    Returns None si no se puede colocar dentro de la ventana.
    """
    if max_spread_usd <= 0 or tick_size <= 0 or mid <= 0:
        return None

    qual_low = round(round((mid + tick_size) / tick_size) * tick_size, 4)
    qual_high = round(round((mid + max_spread_usd - tick_size) / tick_size) * tick_size, 4)

    if qual_low > qual_high:
        return None

    if best_ask > 0 and best_ask <= qual_high - SAFETY_TICKS * tick_size:
        ask = round(round((best_ask + SAFETY_TICKS * tick_size) / tick_size) * tick_size, 4)
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


class RewardsFarmerStrategy(BaseStrategy):
    """Shadow quoting two-sided (BUY+SELL por token) para maximizar Q_min."""

    def __init__(
        self,
        client: Any,
        config: dict[str, Any],
        circuit_breaker: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__("rewards_farmer", client, config, **kwargs)
        self._circuit_breaker = circuit_breaker
        rf_cfg = config.get("rewards_farmer", {})
        markets_cfg = config.get("markets", config)

        self._max_capital_per_market = float(rf_cfg.get("max_capital_per_market", 33.0))
        self._min_rewards_pool_usd = float(rf_cfg.get("min_rewards_pool_usd", 5.0))
        self._max_markets = int(rf_cfg.get("max_markets_simultaneous", 4))
        self._two_sided = bool(rf_cfg.get("two_sided", True))
        self._merge_threshold = float(rf_cfg.get("inventory_merge_threshold", 5.0))
        self._min_share_pct = float(rf_cfg.get("competition_share_min", 0.005))
        self._max_mid_deviation = float(rf_cfg.get("max_mid_deviation", 0.40))

        tw = markets_cfg.get("time_windows", {})
        self._low_activity_hours: set[int] = set(tw.get("low_activity_hours", []))
        self._low_activity_factor: float = tw.get("low_activity_size_factor", 0.7)
        self._prime_hours: set[int] = set(tw.get("prime_placement_window_utc", [0, 1, 2, 3, 22, 23]))
        self._prime_boost: float = tw.get("prime_size_boost", 1.2)

        self._active_farms: dict[str, dict[str, Any]] = {}
        self._fill_inventory: dict[str, dict[str, float]] = {}
        self._reward_pct: dict[str, float] = {}
        self._pending_orders: dict[str, Signal] = {}

        # Tracking de fill rate para auto-widen (si fill_rate > 5%, incrementar safety)
        self._orders_placed_today: int = 0
        self._orders_filled_today: int = 0

        self._reconcile_open_orders()

        self._logger.info(
            "RF v3 (shadow) inicializado: two_sided=%s safety_ticks=%d max_capital=%.0f max_mkts=%d",
            self._two_sided, SAFETY_TICKS, self._max_capital_per_market, self._max_markets,
        )

    def _reconcile_open_orders(self) -> None:
        """Pobla _pending_orders desde el exchange al arrancar."""
        try:
            open_orders = self._client.get_positions() or []
            for o in open_orders:
                oid = o.get("id") or o.get("order_id") or ""
                if oid:
                    self._pending_orders[oid] = None  # type: ignore[assignment]
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

        if self._min_rewards_pool_usd > 0 and rewards_rate < self._min_rewards_pool_usd:
            self._logger.info(
                "RF skip %s...: rewards_pool=%.2f (min %.2f)",
                condition_id[:12], rewards_rate, self._min_rewards_pool_usd,
            )
            return False

        # Gate: max_spread minimo para tener room de esconderse
        max_spread_usd = float(market_data.get("rewards_max_spread", 0.0)) / 100.0
        if max_spread_usd > 0 and max_spread_usd < MIN_MAX_SPREAD_USD:
            self._logger.info(
                "RF skip %s...: max_spread=%.3f < min=%.2f (no hay room para shadow quoting)",
                condition_id[:12], max_spread_usd, MIN_MAX_SPREAD_USD,
            )
            return False

        tokens = market_data.get("tokens", [])
        if not tokens or len(tokens) < 2:
            return False

        if mid_price <= 0.0:
            return False

        if len(tokens) == 2 and abs(mid_price - 0.50) > self._max_mid_deviation:
            self._logger.info(
                "RF skip %s...: mid=%.4f fuera de rango",
                condition_id[:12], mid_price,
            )
            return False

        if self._detect_market_phase(market_data) == "live":
            return False

        if len(self._active_farms) >= self._max_markets and condition_id not in self._active_farms:
            return False

        return True

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
            return []

        available_cash = float(market_data.get("available_cash", self._max_capital_per_market))
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

        signals: list[Signal] = []
        token_signal_counts: dict[str, int] = {}  # cuantas senales tiene cada token

        for token in tokens:
            tid = token.get("token_id", "")
            if not tid:
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

            # Capital por token por lado
            side_capital = max_total_usd / (n_tokens * n_order_slots)

            token_signal_counts[tid] = 0

            # --- BUY side ---
            bid_price = _qualifying_bid(t_mid, max_spread_usd, tick_size, best_bid)
            if bid_price is not None:
                target_shares = _calc_shares(min_shares, size_boost, side_capital, bid_price)
                if target_shares >= max(1, int(min_shares * 0.9)):
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
                        },
                    ))
                    token_signal_counts[tid] += 1
                else:
                    self._logger.info(
                        "RF eval %s tid=%s...: BUY target_shares=%d < min (capital insuf.)",
                        condition_id[:12], tid[:8], target_shares,
                    )

            # --- SELL side (solo si two_sided) ---
            if self._two_sided:
                ask_price = _qualifying_ask(t_mid, max_spread_usd, tick_size, best_ask)
                if ask_price is not None:
                    target_shares_ask = _calc_shares(min_shares, size_boost, side_capital, ask_price)
                    if target_shares_ask >= max(1, int(min_shares * 0.9)):
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

        # Indexar por (token_id, side)
        open_by_token_side: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for oo in open_orders:
            oid = oo.get("id") or oo.get("order_id") or ""
            if oid not in self._pending_orders:
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
            tick_size = 0.01

            # Criterio 1: fuera de ventana de rewards
            # Redondear a 4dp para evitar falsos positivos por floating point (0.52-0.04=0.4800...004)
            win_low = round(t_mid - max_spread_usd, 4)
            win_high = round(t_mid + max_spread_usd, 4)
            if sig.side == "BUY":
                out_of_window = oo_price < win_low or oo_price >= t_mid
            else:
                out_of_window = oo_price <= t_mid or oo_price > win_high

            # Criterio 2: danger zone — orden al frente del book
            danger = _in_danger_zone(oo_price, sig.side, best_bid, best_ask, tick_size)

            # Criterio 3: is_order_scoring
            not_scoring = False
            if oid:
                try:
                    not_scoring = not self._client.is_order_scoring(oid)
                except Exception:
                    pass

            # Criterio 4: mid se movio >= max_spread / 2
            repriced = abs(sig.price - oo_price) >= max_spread_usd * REPRICE_THRESHOLD_RATIO

            # Criterio 5: size difiere > 30%
            size_drift = abs(sig.size - oo_size) / max(oo_size, 1e-9) > SIZE_DRIFT_THRESHOLD

            should_reprice = out_of_window or danger or not_scoring or repriced or size_drift

            if should_reprice:
                # Evitar churn: si el nuevo precio es igual al existente, no cancelar
                # (ocurre en ventanas de 1 tick donde danger zone siempre es True)
                if sig.price == oo_price and not (out_of_window or not_scoring):
                    self._logger.debug(
                        "RF: danger/size en ventana ajustada pero precio no cambia (%s %s @ %.4f) — manteniendo",
                        sig.side, sig.token_id[:8], oo_price,
                    )
                    continue

                reason = (
                    "out_of_window" if out_of_window
                    else "danger_zone" if danger
                    else "not_scoring" if not_scoring
                    else "mid_drift" if repriced
                    else "size_drift"
                )
                self._logger.info(
                    "RF: repricing %s %s tid=%s... @ %.4f → %.4f (%s)",
                    sig.market_id[:8], sig.side, sig.token_id[:8], oo_price, sig.price, reason,
                )
                if oid:
                    to_cancel.append(oid)
                to_place.append(sig)
            else:
                self._logger.debug(
                    "RF: manteniendo %s %s tid=%s... @ %.4f",
                    sig.side, sig.market_id[:8], sig.token_id[:8], oo_price,
                )

        # Limpiar ordenes de tokens/sides que ya no estan en signals
        for (tid, side), oos in open_by_token_side.items():
            if (tid, side) not in signal_keys:
                for oo in oos:
                    oid = str(oo.get("id", oo.get("order_id", "")))
                    if oid:
                        to_cancel.append(oid)
                        self._logger.info("RF: cleanup %s tid=%s...", side, tid[:8])

        for oid in to_cancel:
            try:
                self._client.cancel_order(oid)
                self._pending_orders.pop(oid, None)
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


def _calc_shares(min_shares: float, size_boost: float, side_capital: float, price: float) -> int:
    """Calcula shares objetivo respetando capital y minimo de rewards."""
    target = min_shares * size_boost
    max_by_cap = side_capital / price if price > 0 else 0
    target = min(target, max_by_cap)
    return math.floor(target)
