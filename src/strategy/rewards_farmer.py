"""LP Rewards Farming Optimizer v3-multi.

Estrategia two-sided optimizada para maximizar Q_min en la formula de
liquidity rewards de Polymarket (abril 2026).

Soporta mercados binarios (YES/NO) y multi-outcome (N tokens).

Para mercados multi-outcome, cada token se trata como un lado independiente
con su propio orderbook y scoring S(v,s). El sizing distribuye shares
balanceadamente entre todos los outcomes para maximizar Q_min agregado.

Politica de ordenes: nunca cancelar ciegamente. Se obtienen las ordenes
abiertas, se comparan precios/size, y solo se recoloca si cambio > 2 ticks
o la orden dejo de scorear (S < 0.10). Esto preserva la cola FIFO y
maximiza el tiempo acumulado scoreando.

Batch orders: hasta 15 ordenes por request via post_orders() del SDK.

Referencia: https://docs.polymarket.com/market-makers/liquidity-rewards
"""

import logging
import math
from datetime import datetime, timezone
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.rewards_farmer")

# --- Constantes oficiales de Polymarket ---
SINGLE_SIDED_DIVISOR = 3.0  # c en Q_min
MIN_SHARES_FALLBACK = 20    # minimo practico si la API no reporta rewards_min_size
TICK_MOVE_THRESHOLD = 2     # recolocar solo si el precio cambio >= N ticks
SIZE_CHANGE_PCT = 0.10      # recolocar si el size cambio >= 10%
MIN_SCORE_TO_KEEP = 0.10    # S minimo para mantener una orden abierta


class RewardsFarmerStrategy(BaseStrategy):
    """Farming de rewards multi-outcome con Q_min real y gestion inteligente de ordenes."""

    def __init__(self, client: Any, config: dict[str, Any], **kwargs: Any) -> None:
        super().__init__("rewards_farmer", client, config, **kwargs)
        rf_cfg = config.get("rewards_farmer", {})
        markets_cfg = config.get("markets", config)

        self._max_capital_per_market = float(rf_cfg.get("max_capital_per_market", 50.0))
        self._min_rewards_pool_usd = float(rf_cfg.get("min_rewards_pool_usd", 0.0))
        self._max_markets = int(rf_cfg.get("max_markets_simultaneous", 5))
        self._spread_pct = float(rf_cfg.get("spread_pct_of_max", 0.50))
        self._two_sided = bool(rf_cfg.get("two_sided", True))
        self._merge_threshold = float(rf_cfg.get("inventory_merge_threshold", 5.0))
        self._min_share_pct = float(rf_cfg.get("competition_share_min", 0.05))
        self._max_mid_deviation = float(rf_cfg.get("max_mid_deviation", 0.35))

        # Time windows
        tw = markets_cfg.get("time_windows", {})
        self._low_activity_hours: set[int] = set(tw.get("low_activity_hours", []))
        self._low_activity_factor: float = tw.get("low_activity_size_factor", 0.5)
        self._prime_hours: set[int] = set(tw.get("prime_placement_window_utc", [0, 1, 2, 3]))
        self._prime_boost: float = tw.get("prime_size_boost", 1.3)

        # Estado
        self._active_farms: dict[str, dict[str, Any]] = {}
        self._fill_inventory: dict[str, dict[str, float]] = {}  # market_id -> {token_id: shares}
        self._reward_pct: dict[str, float] = {}
        self._pending_orders: dict[str, Signal] = {}

        self._logger.info(
            "RF v3-multi inicializado: two_sided=%s spread_pct=%.2f max_capital=%.0f max_mkts=%d",
            self._two_sided, self._spread_pct, self._max_capital_per_market, self._max_markets,
        )

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        rewards_rate = float(market_data.get("rewards_rate", 0.0))
        condition_id = market_data.get("condition_id", "")
        mid_price = float(market_data.get("mid_price", 0.0))

        if self._min_rewards_pool_usd > 0 and rewards_rate < self._min_rewards_pool_usd:
            self._logger.info(
                "RF skip %s...: rewards_pool=%.2f USD/dia (min %.2f)",
                condition_id[:12], rewards_rate, self._min_rewards_pool_usd,
            )
            return False

        tokens = market_data.get("tokens", [])
        if not tokens or len(tokens) < 2:
            return False

        if mid_price <= 0.0:
            self._logger.info("RF skip %s...: mid_price=0 (WS data pending)", condition_id[:12])
            return False
        # Filtro de mid solo para binarios; multi-outcome se evalua por token individual
        if len(tokens) == 2 and abs(mid_price - 0.50) > self._max_mid_deviation:
            self._logger.info(
                "RF skip %s...: mid=%.4f fuera de [%.2f, %.2f]",
                condition_id[:12], mid_price,
                0.50 - self._max_mid_deviation, 0.50 + self._max_mid_deviation,
            )
            return False

        phase = self._detect_market_phase(market_data)
        if phase == "live":
            self._logger.info("RF skip %s...: phase=live (riesgo directional)", condition_id[:12])
            return False

        token_data = market_data.get("token_data", {})
        rewards_min_size = float(market_data.get("rewards_min_size", 0.0))
        min_shares = max(MIN_SHARES_FALLBACK, rewards_min_size)

        can_any = False
        for token in tokens:
            tid = token.get("token_id", "")
            t_mid = float(token_data.get(tid, {}).get("mid_price", mid_price))
            if t_mid <= 0:
                continue
            side_capital = self._max_capital_per_market / (float(len(tokens)) if self._two_sided else 1.0)
            required_usd = min_shares * t_mid
            if required_usd <= side_capital:
                can_any = True
                break
            if not self._two_sided and required_usd <= self._max_capital_per_market:
                can_any = True
                break

        if not can_any:
            self._logger.info(
                "RF skip %s...: ni un lado alcanza min_shares=%.0f con capital disponible",
                condition_id[:12], min_shares,
            )
            return False

        if len(self._active_farms) >= self._max_markets and condition_id not in self._active_farms:
            self._logger.info("RF skip %s...: max_markets=%d reached", condition_id[:12], self._max_markets)
            return False

        self._logger.info(
            "RF ACT: %s... mid=%.4f pool=$%.2f/dia phase=%s tokens=%d",
            condition_id[:12], mid_price, rewards_rate, phase, len(tokens),
        )
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera senales de BID optimizadas para Q_min de Polymarket.

        Formula oficial (paso 1-7):
          S(v,s) = ((v - s) / v)^2 * b
          Qmin = max(min(Qone, Qtwo), max(Qone/c, Qtwo/c))  donde c=3.0

        Estrategia para $166:
          - BID principal a 1c del mid -> score=0.44 (sweet spot riesgo/reward)
          - Siempre ambos lados -> ~3x Q_min vs un solo lado
          - En mercados extremos (<0.10 o >0.90): obligatorio ambos lados
        """
        condition_id = market_data.get("condition_id", "")
        rewards_rate = float(market_data.get("rewards_rate", 0.0))
        rewards_min_size = float(market_data.get("rewards_min_size", 0.0))
        rewards_max_spread = float(market_data.get("rewards_max_spread", 0.0))
        if not condition_id:
            return []

        tokens = market_data.get("tokens", [])
        token_data = market_data.get("token_data", {})
        if not tokens or len(tokens) < 2:
            return []

        max_spread_usd = rewards_max_spread / 100.0 if rewards_max_spread > 0 else 0.04
        min_shares = max(MIN_SHARES_FALLBACK, rewards_min_size)
        tick_size = float(market_data.get("tick_size", 0.01))

        # Calcular mids por token
        mids: list[tuple[dict[str, Any], float]] = []
        sum_mids = 0.0
        for token in tokens:
            tid = token.get("token_id", "")
            td = token_data.get(tid, {})
            t_mid = float(td.get("mid_price", 0.0))
            if t_mid <= 0:
                continue
            mids.append((token, t_mid))
            sum_mids += t_mid

        if not mids or sum_mids <= 0:
            self._logger.info("RF eval %s...: sin mids WS (tokens=%d)", condition_id[:12], len(tokens))
            return []

        # Cash disponible (inyectado por main.py)
        available_cash = float(market_data.get("available_cash", self._max_capital_per_market))
        max_total_usd = min(self._max_capital_per_market, available_cash * 0.98)

        # Shares por token: balanceado para maximizar Q_min agregado
        base_shares = max(min_shares, max_total_usd / sum_mids)

        # Boost de ventana horaria
        now_utc = datetime.now(timezone.utc).hour
        boost = 1.0
        if now_utc in self._prime_hours:
            boost = self._prime_boost
        elif now_utc in self._low_activity_hours:
            boost = self._low_activity_factor

        if boost != 1.0:
            base_shares = base_shares * boost
            max_shares = max_total_usd / sum_mids
            if base_shares > max_shares:
                base_shares = max_shares
            if base_shares < min_shares:
                base_shares = min_shares

        # Distancia optima: 1c del mid -> S=0.44 (sweet spot para $166)
        # Con max_spread=3c: distance=1c -> ((3-1)/3)^2 = 0.44
        # Con max_spread=4c: distance=1.3c -> ((4-1.3)/4)^2 = 0.46
        # Target: estar a ~1/3 del max_spread del mid
        optimal_distance = max_spread_usd / 3.0
        optimal_distance = max(optimal_distance, tick_size)

        signals: list[Signal] = []
        per_token_scores: list[float] = []

        for token, t_mid in mids:
            tid = token.get("token_id", "")
            if not tid:
                continue

            td = token_data.get(tid, {})
            asks_book = td.get("orderbook", {}).get("asks", [])
            best_ask = float(asks_book[0].get("price", asks_book[0].get("p", 1.0))) if asks_book else td.get("best_ask", 1.0)

            bid_price = self._calc_bid_price(t_mid, optimal_distance, best_ask, max_spread_usd, tick_size)
            if bid_price <= 0 or bid_price >= t_mid:
                continue

            # Calcular score de esta orden: S(v,s) = ((v-s)/v)^2
            s_usd = t_mid - bid_price
            v_usd = max_spread_usd
            score = ((v_usd - s_usd) / v_usd) ** 2 if v_usd > 0 else 0.0

            # Si score < minimo, intentar con distancia minima (1 tick)
            if score < MIN_SCORE_TO_KEEP:
                bid_price = self._calc_bid_price(t_mid, tick_size, best_ask, max_spread_usd, tick_size)
                if bid_price <= 0 or bid_price >= t_mid:
                    continue
                s_usd = t_mid - bid_price
                score = ((v_usd - s_usd) / v_usd) ** 2 if v_usd > 0 else 0.0
                if score < MIN_SCORE_TO_KEEP:
                    continue

            # Size en USD: shares balanceadas * mid del token
            size_usd = base_shares * t_mid
            side_capital = max_total_usd * t_mid / sum_mids
            size_usd = round(min(size_usd, side_capital), 2)
            if size_usd < min_shares * t_mid * 0.9:
                continue

            per_token_scores.append(score)
            signals.append(Signal(
                strategy_name=self.name,
                market_id=condition_id,
                token_id=tid,
                side="BUY",
                price=bid_price,
                size=size_usd,
                confidence=0.7,
                metadata={
                    "reason": f"rf_v3q: rate={rewards_rate:.2f}/day dist={optimal_distance:.4f}",
                    "phase": self._detect_market_phase(market_data),
                    "score": round(score, 4),
                    "shares": round(base_shares * boost, 2),
                },
            ))

        # Q_min check para binarios (2 tokens)
        # Si ambos lados tienen senales, verificar Q_min
        if len(per_token_scores) == 2 and self._two_sided:
            q_one = per_token_scores[0]
            q_two = per_token_scores[1]
            q_min = max(min(q_one, q_two), max(q_one / SINGLE_SIDED_DIVISOR, q_two / SINGLE_SIDED_DIVISOR))
            self._logger.info(
                "RF eval %s...: Q_min=%.4f (Qone=%.4f Qtwo=%.4f) ratio=%.1fx",
                condition_id[:12], q_min, q_one, q_two,
                q_min / min(q_one, q_two) if min(q_one, q_two) > 0 else 0,
            )
        elif len(per_token_scores) == 1 and len(mids) == 2:
            # Mercado binario con un solo lado: Q_min = score/3
            single = per_token_scores[0]
            q_min_est = single / SINGLE_SIDED_DIVISOR
            self._logger.warning(
                "RF eval %s...: SOLO UN LADO! score=%.4f Q_min_est=%.4f (3x menos rewards)",
                condition_id[:12], single, q_min_est,
            )

        if not self._two_sided and signals:
            signals = [signals[0]]

        self._logger.info(
            "RF eval %s...: %d senales generadas (tokens=%d mids_ok=%d)",
            condition_id[:12], len(signals), len(tokens), len(mids),
        )
        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        trades: list[Trade] = []
        if not signals:
            return trades

        self._logger.info("RF execute: %d señales a colocar", len(signals))

        # 1. Obtener ordenes abiertas actuales del exchange
        open_orders: list[dict[str, Any]] = []
        try:
            open_orders = self._client.get_positions() or []
        except Exception:
            self._logger.exception("RF: error obteniendo ordenes abiertas")

        # Indexar SOLO las ordenes que colocamos nosotros (evita cancelar manuales)
        open_by_token: dict[str, list[dict[str, Any]]] = {}
        for oo in open_orders:
            oid = oo.get("id") or oo.get("order_id") or ""
            if oid not in self._pending_orders:
                continue  # ignorar ordenes manuales / ajenas
            tid = oo.get("asset_id") or oo.get("token_id") or ""
            side = str(oo.get("side", "")).upper()
            if tid and side == "BUY":
                open_by_token.setdefault(tid, []).append(oo)

        # 2. Decidir que cancelar y que colocar
        to_cancel: list[str] = []
        to_place: list[Signal] = []
        signal_tokens = {s.token_id: s for s in signals}

        for sig in signals:
            existing = open_by_token.get(sig.token_id, [])
            if not existing:
                to_place.append(sig)
                continue

            oo = existing[0]
            oo_price = float(oo.get("price", 0.0))
            oo_size = float(oo.get("original_size", oo.get("size", 0.0)))
            tick_size = 0.01  # se podria obtener del market_data

            price_diff_ticks = abs(round((sig.price - oo_price) / tick_size))
            size_diff_pct = abs(sig.size - oo_size) / max(oo_size, 1e-9)

            if price_diff_ticks >= TICK_MOVE_THRESHOLD or size_diff_pct >= SIZE_CHANGE_PCT:
                oid = str(oo.get("id", oo.get("order_id", "")))
                if oid:
                    to_cancel.append(oid)
                to_place.append(sig)
            else:
                self._logger.debug(
                    "RF: manteniendo orden %s en %s @ %.4f (diff %d ticks, size %.1f%%)",
                    str(oo.get("id", ""))[:12], sig.token_id[:8], oo_price,
                    price_diff_ticks, size_diff_pct * 100,
                )

        # Cancelar ordenes propias de tokens que ya no estan en signals
        for tid, oos in open_by_token.items():
            if tid not in signal_tokens:
                for oo in oos:
                    oid = str(oo.get("id", oo.get("order_id", "")))
                    if oid:
                        to_cancel.append(oid)

        # 3. Ejecutar cancelaciones
        for oid in to_cancel:
            try:
                self._client.cancel_order(oid)
                self._pending_orders.pop(oid, None)
            except Exception:
                self._logger.debug("RF: error cancelando orden %s...", oid[:12])

        # 4. Ejecutar colocaciones (batch si es posible)
        if to_place:
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
                for sig in to_place:
                    trades.append(Trade(
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        strategy_name=self.name,
                        market_id=sig.market_id,
                        token_id=sig.token_id,
                        side=sig.side,
                        price=sig.price,
                        size=sig.size,
                        status="error",
                        order_id="",
                    ))

        # 5. Actualizar _active_farms
        for sig in signals:
            if sig.market_id not in self._active_farms:
                self._active_farms[sig.market_id] = {}
            self._active_farms[sig.market_id][sig.token_id] = {
                "side": sig.side,
                "size": sig.size,
                "price": sig.price,
            }

        return trades

    # ------------------------------------------------------------------
    # Fills e inventario
    # ------------------------------------------------------------------

    def record_fill(self, token_id: str, side: str, size: float, market_id: str, tokens: list[dict[str, Any]]) -> None:
        """Registra un fill en el inventario local del RF.

        Usa token_id directamente como key. tokens se usa solo para
        detectar si es binario (len==2) para posible merge posterior.
        """
        if market_id not in self._fill_inventory:
            self._fill_inventory[market_id] = {}
        inv = self._fill_inventory[market_id]

        if side == "BUY":
            inv[token_id] = inv.get(token_id, 0.0) + size
        elif side == "SELL":
            inv[token_id] = max(0.0, inv.get(token_id, 0.0) - size)

    def get_fill_inventory(self, market_id: str) -> dict[str, float]:
        """Retorna inventario por token_id."""
        return dict(self._fill_inventory.get(market_id, {}))

    def should_merge(self, market_id: str) -> bool:
        """True si es binario y ambos lados >= merge_threshold.

        Para multi-outcome retorna False (no hay merge nativo; hay que
        vender cada token individualmente al mercado).
        """
        inv = self._fill_inventory.get(market_id, {})
        if len(inv) != 2:
            return False
        vals = list(inv.values())
        return min(abs(vals[0]), abs(vals[1])) >= self._merge_threshold

    def mark_merged(self, market_id: str, merged_amount: float) -> None:
        """Reduce inventario binario tras un merge."""
        if market_id not in self._fill_inventory:
            return
        inv = self._fill_inventory[market_id]
        if len(inv) != 2:
            return
        keys = list(inv.keys())
        inv[keys[0]] = max(0.0, inv.get(keys[0], 0.0) - merged_amount)
        inv[keys[1]] = max(0.0, inv.get(keys[1], 0.0) - merged_amount)

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
        """Construye un Trade desde un Signal y el dict resultado del SDK."""
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
    def _calc_bid_price(
        mid: float,
        distance: float,
        best_ask: float,
        max_spread_usd: float,
        tick_size: float,
    ) -> float:
        """Calcula precio de BID respetando max_spread y post-only."""
        bid = round(mid - distance, 4)
        if best_ask > 0 and bid >= best_ask:
            bid = round(best_ask - tick_size, 4)
        bid = max(tick_size, bid)
        if bid < mid - max_spread_usd:
            bid = round(mid - max_spread_usd, 4)
        return bid if 0 < bid < mid else 0.0

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

    def should_exit_live(self, market_data: dict[str, Any]) -> bool:
        phase = self._detect_market_phase(market_data)
        condition_id = market_data.get("condition_id", "")
        inv = self.get_fill_inventory(condition_id)
        has_inventory = any(v > 0 for v in inv.values())
        return phase == "live" or (phase == "post_game" and not has_inventory)
