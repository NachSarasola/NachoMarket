import re
import sys

def main():
    path = "src/strategy/rewards_farmer.py"
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update _get_dynamic_safety_ticks to _get_cushion_usd
    old_cushion_func = """def _get_dynamic_safety_ticks(
    book: list[dict],
    our_price: float,
    bbo_price: float,
    tick_size: float,
) -> int:
    \"\"\"Devuelve 1 si hay colchón (órdenes entre nosotros y el BBO), sino SAFETY_TICKS.

    Si hay órdenes entre our_price y bbo_price, esas se ejecutarán primero
    y podemos usar 1 tick de seguridad. Si no hay colchón, usar 2 ticks.
    \"\"\"
    if not book or bbo_price <= 0:
        return SAFETY_TICKS

    # Determinar rango a chequear según lado
    if our_price < bbo_price:  # BUY side: nuestro bid está debajo del best_bid
        price_low = our_price
        price_high = bbo_price
    else:  # SELL side: nuestro ask está arriba del best_ask
        price_low = bbo_price
        price_high = our_price

    if price_high - price_low <= tick_size:
        return SAFETY_TICKS  # No hay espacio para colchón

    # Contar si hay órdenes en el rango (colchón)
    for level in book:
        price = float(level.get("price", level.get("p", 0.0)))
        if price_low < price < price_high:
            return 1  # Colchón detectado → 1 tick alcanza

    return SAFETY_TICKS"""

    new_cushion_func = """def _get_cushion_usd(
    book: list[dict],
    our_price: float,
    bbo_price: float,
) -> float:
    \"\"\"Devuelve el volumen total en USD de las órdenes entre nuestro precio y el BBO.\"\"\"
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

    return cushion_usd"""

    content = content.replace(old_cushion_func, new_cushion_func)

    # 2. Update __init__
    init_old = """    def __init__(
        self,
        client: Any,
        config: dict[str, Any],
        circuit_breaker: Any = None,
        reward_tracker: Any = None,
        market_filter: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__("rewards_farmer", client, config, **kwargs)
        self._circuit_breaker = circuit_breaker
        self._reward_tracker = reward_tracker
        self._market_filter = market_filter
        self._market_volume: dict[str, float] = {}
        rf_cfg = config.get("rewards_farmer", {})"""

    init_new = """    def __init__(
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
        self._subscribed_tokens = set()"""

    content = content.replace(init_old, init_new)

    # 3. Update _calc_shares to accept cushion_usd for dynamic scaling
    calc_old = """def _calc_shares(min_shares: float, size_boost: float, side_capital: float, price: float) -> int:
    \"\"\"Calcula shares objetivo para shadow orders.

    Limitado por: SHADOW_SIZE_MULT × min_shares, MAX_ORDER_NOTIONAL,
    y el capital disponible por lado (side_capital).
    \"\"\"
    target = min_shares * SHADOW_SIZE_MULT * size_boost
    max_by_notional = MAX_ORDER_NOTIONAL / price if price > 0 else 0
    max_by_capital = side_capital / price if price > 0 else 0
    cap = min(max_by_notional, max_by_capital) if max_by_capital > 0 else max_by_notional
    return math.floor(min(target, cap))"""

    calc_new = """def _calc_shares(min_shares: float, size_boost: float, side_capital: float, price: float, cushion_usd: float = 0.0) -> int:
    \"\"\"Calcula shares objetivo usando Dynamic Yield Scaling.
    Si el colchón es alto, somos agresivos (yield máximo). Si es bajo, conservadores.
    \"\"\"
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
    return math.floor(min(target, cap))"""

    content = content.replace(calc_old, calc_new)

    # 4. Update evaluate
    content = content.replace(
        "            safety_ticks_bid = _get_dynamic_safety_ticks(bids_book, our_bid_2t, best_bid, tick_size)",
        "            cushion_bid = _get_cushion_usd(bids_book, our_bid_2t, best_bid)\n            safety_ticks_bid = 1 if cushion_bid >= self._min_cushion_usd else SAFETY_TICKS"
    )

    content = content.replace(
        "                target_shares = _calc_shares(min_shares, size_boost, side_capital, bid_price)",
        "                target_shares = _calc_shares(min_shares, size_boost, side_capital, bid_price, cushion_bid)"
    )

    content = content.replace(
        "                safety_ticks_ask = _get_dynamic_safety_ticks(asks_book, best_ask, our_ask_2t, tick_size)",
        "                cushion_ask = _get_cushion_usd(asks_book, best_ask, our_ask_2t)\n                safety_ticks_ask = 1 if cushion_ask >= self._min_cushion_usd else SAFETY_TICKS"
    )

    content = content.replace(
        "                    target_shares_ask = _calc_shares(min_shares, size_boost, side_capital, ask_price)",
        "                    target_shares_ask = _calc_shares(min_shares, size_boost, side_capital, ask_price, cushion_ask)"
    )

    # Subscribe to tokens during evaluate
    eval_hook = """        for token in [t for t in tokens if t.get("token_id", "") in viable_token_ids]:
            tid = token.get("token_id", "")"""
    eval_hook_new = """        for token in [t for t in tokens if t.get("token_id", "") in viable_token_ids]:
            tid = token.get("token_id", "")
            if tid and self._ws_feed and tid not in self._subscribed_tokens:
                self._ws_feed.subscribe(tid, self._on_ws_update, condition_id=condition_id)
                self._subscribed_tokens.add(tid)"""
    content = content.replace(eval_hook, eval_hook_new)

    # WS Callback
    ws_cb = """
    async def _on_ws_update(self, token_id: str, ob_state: Any, change_type: str) -> None:
        if not self._fast_cancel_enabled:
            return
            
        bids = [{"price": p, "size": s} for p, s in ob_state.bids]
        asks = [{"price": p, "size": s} for p, s in ob_state.asks]
        best_bid = bids[0]["price"] if bids else 0.0
        best_ask = asks[0]["price"] if asks else 0.0
        
        # Buscar ordenes nuestras en este token
        for oid, order in list(self._pending_orders.items()):
            if order.get("token_id") != token_id:
                continue
            
            side = order.get("side")
            price = order.get("price")
            
            cancel = False
            if side == "BUY" and best_bid > 0:
                cushion = _get_cushion_usd(bids, price, best_bid)
                if cushion < self._min_cushion_usd and best_bid - price <= 0.02:
                    cancel = True
            elif side == "SELL" and best_ask > 0:
                cushion = _get_cushion_usd(asks, best_ask, price)
                if cushion < self._min_cushion_usd and price - best_ask <= 0.02:
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
"""
    
    # Inject before execute method
    content = content.replace("    def execute(", ws_cb + "    def execute(")

    # Imbalance protection in evaluate
    imbalance_check = """            # --- BUY side ---"""
    imbalance_check_new = """
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

            # --- BUY side ---"""
    
    content = content.replace(imbalance_check, imbalance_check_new)

    # Also wrap the side logic with buy_allowed / sell_allowed
    content = content.replace(
        "bid_price = _qualifying_bid(t_mid, max_spread_usd, tick_size, best_bid, safety_ticks_bid)",
        "bid_price = _qualifying_bid(t_mid, max_spread_usd, tick_size, best_bid, safety_ticks_bid) if buy_allowed else None"
    )

    content = content.replace(
        "ask_price = _qualifying_ask(t_mid, max_spread_usd, tick_size, best_ask, safety_ticks_ask)",
        "ask_price = _qualifying_ask(t_mid, max_spread_usd, tick_size, best_ask, safety_ticks_ask) if sell_allowed else None"
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    main()
