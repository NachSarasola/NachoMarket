import logging
import random
import time
from datetime import datetime, timezone
from typing import Any

from src.analysis.wall_detector import is_large_wall
from src.strategy.base import BaseStrategy, Signal, Trade
from src.strategy.repositioner import FillRepositioner

logger = logging.getLogger("nachomarket.strategy.market_maker")


class MarketMakerStrategy(BaseStrategy):
    """Market making con rewards. Siempre usa Post Only para evitar taker fees.

    Features:
    - evaluate(): verifica spread vs tick_size, calcula bid/ask con inventory skew
    - execute(): cancela ordenes previas, coloca GTC Post Only
    - manage_inventory(): merge YES+NO → USDC, pausa lados sobreexpuestos
    - Loop cada refresh_seconds (45s default)
    """

    def __init__(self, client, config: dict[str, Any], **kwargs) -> None:
        super().__init__("market_maker", client, config, **kwargs)
        mm = config.get("market_maker", config.get("market_making", {}))
        self._spread_offset = mm.get("spread_offset", 0.02)
        self._min_spread = mm.get("min_spread", 0.01)
        self._order_size = mm.get("order_size", mm.get("order_size_usdc", 5.0))
        self._refresh_seconds = mm.get("refresh_seconds", 45)
        self._max_inventory = mm.get("max_inventory_per_market", 50.0)
        self._num_levels = mm.get("num_levels", 3)
        self._level_spacing = mm.get("level_spacing", self._spread_offset / self._num_levels)

        # Inventario interno por token_id: positivo = long, negativo = short
        self._inventory: dict[str, float] = {}
        # Lados pausados por inventario: {token_id: {"BUY", "SELL"}}
        self._paused_sides: dict[str, set[str]] = {}
        # Ultimo refresh por condition_id
        self._last_refresh: dict[str, float] = {}
        # Reposicionador post-fill
        self._repositioner = FillRepositioner(config)

        # Time-based sizing: reducir en horas bajas, boost en ventana prime
        tw = config.get("time_windows", {})
        self._low_activity_hours: set[int] = set(tw.get("low_activity_hours", range(0, 8)))
        self._low_activity_factor: float = tw.get("low_activity_size_factor", 0.5)
        # Tip 19: prime placement window (00-04 UTC) — boost de +30%
        self._prime_hours: set[int] = set(tw.get("prime_placement_window_utc", [0, 1, 2, 3]))
        self._prime_boost: float = tw.get("prime_size_boost", 1.3)

        # Anti-frontrunning jitter (TODO 1.5)
        jitter_cfg = config.get("anti_frontrun", {})
        self._jitter_enabled: bool = jitter_cfg.get("enabled", True)
        self._jitter_size_pct: float = jitter_cfg.get("size_jitter_pct", 0.15)   # ±15%
        self._jitter_price_prob: float = jitter_cfg.get("price_jitter_prob", 0.30)  # 30%
        self._jitter_timing_sec: float = jitter_cfg.get("timing_jitter_sec", 5.0)  # ±5s

        # Near-resolution gate: cancelar quotes cuando faltan < N horas
        self._near_resolution_hours: float = mm.get(
            "near_resolution_hours",
            config.get("near_resolution_hours", 336.0),
        )

        # Tip 21: no reposicionar si el mid no se movio suficiente
        self._min_mid_change: float = config.get("min_mid_change_to_reposition", 0.02)
        # Cache de ultimo mid conocido por condition_id
        self._last_mid: dict[str, float] = {}

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Entra si el spread del mercado es suficiente para ser rentable."""
        spread = market_data.get("spread", market_data.get("spread_bps", 0) / 100)
        if spread < self._min_spread:
            self._logger.debug(f"Spread {spread:.3f} too tight (min {self._min_spread}), skipping")
            return False
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera senales bid/ask verificando tick_size, fee_rate e inventario.

        Para cada mercado activo:
        1. Obtener midpoint y spread actual
        2. Obtener tick_size y fee_rate del mercado
        3. Si spread > 2 * tick_size: hay oportunidad
        4. Calcular precios de bid y ask: mid ± spread_offset
        5. Verificar inventario no exceda max_inventory_per_market
        6. Si inventario sesgado: wider en el lado sobreexpuesto
        7. Generar Signal para bid (BUY) y ask (SELL)
        """
        market_id = market_data.get("condition_id", market_data.get("market_id", ""))
        token_id = market_data.get("token_id", "")
        mid_price = market_data.get("mid_price", 0.5)

        if mid_price <= 0 or mid_price >= 1:
            return []

        # Si hay tokens, usar el primero (YES = clobTokenIds[0])
        tokens = market_data.get("tokens", [])
        if tokens and not token_id:
            token_id = tokens[0].get("token_id", "")

        # --- Obtener tick_size y fee_rate del mercado ---
        tick_size = self._get_tick_size(token_id)
        fee_rate_bps = self._get_fee_rate(token_id)
        spread = market_data.get("spread", 2 * self._spread_offset)

        # Verificar que spread > 2 * tick_size (oportunidad minima)
        if spread <= 2 * tick_size:
            self._logger.debug(
                f"Spread {spread:.4f} <= 2*tick_size {2*tick_size:.4f}, no opportunity"
            )
            return []

        # Tip 9: detectar walls grandes en el book (>= 10x min_share del mercado)
        # Si hay wall, el MM se posiciona en el mismo precio (junto a la wall)
        rewards_min = market_data.get("rewards_min_size", 0.0) or 5.0
        bids_book = self._extract_book_side(market_data, "bids")
        asks_book = self._extract_book_side(market_data, "asks")
        bid_wall_found, bid_wall_price = is_large_wall(bids_book, rewards_min)
        ask_wall_found, ask_wall_price = is_large_wall(asks_book, rewards_min)

        # Tip 17: metadata para enriquecer Trade (categoria, share, mid)
        signal_metadata: dict[str, Any] = {
            "mid_at_entry": float(mid_price),
            "participation_share_at_entry": float(market_data.get("_participation_share", 0.0)),
            "category": str(market_data.get("category", "")),
        }

        # --- Inventario y skew ---
        current_inv = self._inventory.get(token_id, 0.0)
        inv_ratio = abs(current_inv) / self._max_inventory if self._max_inventory > 0 else 0.0

        # Skew: si estamos long, wider en BUY (menos agresivo comprando)
        # si estamos short, wider en SELL (menos agresivo vendiendo)
        skew = 0.0
        if self._max_inventory > 0:
            skew = (current_inv / self._max_inventory) * self._spread_offset

        # Lados pausados por manage_inventory
        paused = self._paused_sides.get(token_id, set())

        # Tamaño efectivo: reducido en horas de baja actividad
        effective_size = self._get_effective_order_size()
        signals: list[Signal] = []

        for level in range(self._num_levels):
            level_offset = self._spread_offset + (level * self._level_spacing)
            # Confidence decrece con el nivel
            confidence = max(0.3, 1.0 - level * 0.2)

            # --- BID (BUY) ---
            if "BUY" not in paused:
                # Si estamos long (skew > 0), alejar el bid (wider)
                bid_offset = level_offset + max(skew, 0)
                bid_price = self._round_to_tick(mid_price - bid_offset, tick_size)
                # Tip 9: nivel 0 + wall detectada → posicionar junto a la wall
                if level == 0 and bid_wall_found and 0 < bid_wall_price < mid_price:
                    bid_price = self._round_to_tick(bid_wall_price, tick_size)

                if 0 < bid_price < mid_price:
                    # Verificar que no excedemos inventario
                    projected = current_inv + effective_size
                    if abs(projected) <= self._max_inventory:
                        signals.append(self._make_signal(
                            market_id=market_id,
                            token_id=token_id,
                            side="BUY",
                            price=bid_price,
                            size=effective_size,
                            confidence=confidence,
                            metadata=signal_metadata,
                        ))

            # --- ASK (SELL) ---
            if "SELL" not in paused:
                # Si estamos short (skew < 0), alejar el ask (wider)
                ask_offset = level_offset + max(-skew, 0)
                ask_price = self._round_to_tick(mid_price + ask_offset, tick_size)
                # Tip 9: nivel 0 + wall detectada → posicionar junto a la wall
                if level == 0 and ask_wall_found and mid_price < ask_wall_price < 1.0:
                    ask_price = self._round_to_tick(ask_wall_price, tick_size)

                if mid_price < ask_price < 1.0:
                    # Verificar que no excedemos inventario
                    projected = current_inv - effective_size
                    if abs(projected) <= self._max_inventory:
                        signals.append(self._make_signal(
                            market_id=market_id,
                            token_id=token_id,
                            side="SELL",
                            price=ask_price,
                            size=effective_size,
                            confidence=confidence,
                            metadata=signal_metadata,
                        ))

        self._logger.info(
            f"MM evaluate: {len(signals)} signals for {token_id[:8]}... "
            f"(mid={mid_price:.4f}, inv={current_inv:.1f}, skew={skew:.4f})"
        )
        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Cancela ordenes previas y coloca nuevas GTC Post Only.

        1. Cancelar ordenes previas en el mercado (cancel_market_orders)
        2. Colocar nuevas ordenes con GTC y Post Only
        3. Registrar las ordenes colocadas
        """
        trades: list[Trade] = []
        if not signals:
            return trades

        # Agrupar por market_id para cancelar una vez por mercado
        markets_seen: set[str] = set()

        for signal in signals:
            # Cancelar ordenes previas del mercado (una vez por mercado)
            if signal.market_id and signal.market_id not in markets_seen:
                markets_seen.add(signal.market_id)
                try:
                    self._client.cancel_market_orders(
                        condition_id=signal.market_id,
                        token_id=signal.token_id,
                    )
                    self._logger.info(
                        f"Cancelled previous orders for {signal.market_id[:8]}..."
                    )
                except Exception:
                    self._logger.exception(
                        f"Error cancelling orders for {signal.market_id[:8]}..."
                    )

            # Colocar nueva orden GTC Post Only
            try:
                result = self._client.place_limit_order(
                    token_id=signal.token_id,
                    side=signal.side,
                    price=signal.price,
                    size=signal.size,
                    post_only=True,  # SIEMPRE Post Only para market making
                )

                trade = self._make_trade(
                    signal=signal,
                    order_id=result.get("order_id", "unknown"),
                    status=result.get("status", "submitted"),
                    fee_paid=0.0,  # Post Only = maker = sin taker fee
                )
                self.log_trade(trade)
                trades.append(trade)

                # Actualizar inventario interno
                self._update_inventory(signal.token_id, signal.side, signal.size)

            except Exception:
                self._logger.exception(
                    f"Error placing MM order: {signal.side} {signal.size} @ {signal.price}"
                )
                trade = self._make_trade(
                    signal=signal,
                    order_id="",
                    status="error",
                )
                self.log_trade(trade)
                trades.append(trade)

        return trades

    # ------------------------------------------------------------------
    # Inventory management
    # ------------------------------------------------------------------

    def manage_inventory(self, market_data: dict[str, Any]) -> None:
        """Gestiona inventario: merge YES+NO → USDC y pausa lados sobreexpuestos.

        1. Si tenemos YES shares Y NO shares del mismo mercado: merge → USDC
        2. Si inventario de un lado > 80% del max: pausar ese lado temporalmente
        """
        tokens = market_data.get("tokens", [])
        if len(tokens) < 2:
            return

        yes_token = tokens[0].get("token_id", "")
        no_token = tokens[1].get("token_id", "")

        yes_inv = self._inventory.get(yes_token, 0.0)
        no_inv = self._inventory.get(no_token, 0.0)

        # --- Merge YES + NO → USDC ---
        # Si tenemos posicion long en ambos lados, podemos mergear
        if yes_inv > 0 and no_inv > 0:
            merge_size = min(yes_inv, no_inv)
            try:
                self._client.merge_positions(
                    token_id=yes_token,
                    size=merge_size,
                )
                self._inventory[yes_token] = yes_inv - merge_size
                self._inventory[no_token] = no_inv - merge_size
                self._logger.info(
                    f"Merged {merge_size:.2f} YES+NO shares → USDC"
                )
            except Exception:
                self._logger.exception("Error merging positions")

        # --- Pausar lados sobreexpuestos ---
        threshold = self._max_inventory * 0.8

        for token_id in [yes_token, no_token]:
            inv = self._inventory.get(token_id, 0.0)
            paused = self._paused_sides.setdefault(token_id, set())

            if inv > threshold:
                # Demasiado long → pausar BUY (no comprar mas)
                if "BUY" not in paused:
                    paused.add("BUY")
                    self._logger.warning(
                        f"Pausing BUY for {token_id[:8]}... "
                        f"(inv={inv:.1f} > 80% of {self._max_inventory})"
                    )
            elif inv < -threshold:
                # Demasiado short → pausar SELL (no vender mas)
                if "SELL" not in paused:
                    paused.add("SELL")
                    self._logger.warning(
                        f"Pausing SELL for {token_id[:8]}... "
                        f"(inv={inv:.1f} < -80% of {self._max_inventory})"
                    )
            else:
                # Dentro de limites → desbloquear
                if paused:
                    self._logger.info(f"Unpausing sides for {token_id[:8]}...")
                    paused.clear()

    def needs_refresh(self, condition_id: str, current_mid: float = 0.0) -> bool:
        """Verifica si hay que refrescar quotes.

        Tip 21: solo refresca si paso el tiempo Y el mid cambio >= min_mid_change.
        Preservar la orden quieta en el book acumula mas rewards por minuto.
        """
        last = self._last_refresh.get(condition_id, 0.0)
        interval = self._jittered_refresh_seconds()
        if (time.time() - last) < interval:
            return False
        # Timer paso: verificar si el mid se movio lo suficiente
        if current_mid > 0 and self._min_mid_change > 0:
            last_mid = self._last_mid.get(condition_id, 0.0)
            if last_mid > 0 and abs(current_mid - last_mid) < self._min_mid_change:
                self._logger.debug(
                    "mid cambio %.4f (< %.4f) — no reposicionar %s",
                    abs(current_mid - last_mid), self._min_mid_change, condition_id[:12],
                )
                return False
        return True

    def mark_refreshed(self, condition_id: str, current_mid: float = 0.0) -> None:
        """Marca el mercado como recien refresheado y guarda el mid actual."""
        self._last_refresh[condition_id] = time.time()
        if current_mid > 0:
            self._last_mid[condition_id] = current_mid

    # ------------------------------------------------------------------
    # Override run() para incluir manage_inventory y refresh timing
    # ------------------------------------------------------------------

    def run(self, market_data: dict[str, Any]) -> list[Trade]:
        """Pipeline completo: check refresh → manage_inventory → evaluate → execute."""
        if not self.is_active:
            return []

        all_trades: list[Trade] = []
        condition_id = market_data.get("condition_id", market_data.get("market_id", ""))

        # Cancelar reposiciones expiradas antes de operar
        expired_ids = self._repositioner.check_expirations()
        for order_id in expired_ids:
            try:
                self._client.cancel_order(order_id)
            except Exception:
                self._logger.debug(f"Could not cancel expired reposition {order_id[:12]}...")

        # Near-resolution gate: cancelar quotes y salir si el mercado resuelve pronto
        if self._is_near_resolution(market_data):
            self._logger.info(
                "Near-resolution (< %.0fh): cancelando quotes para %s",
                self._near_resolution_hours, condition_id[:14],
            )
            try:
                self._client.cancel_market_orders(condition_id=condition_id)
            except Exception:
                self._logger.debug("No se pudieron cancelar órdenes near-resolution")
            return []

        # Verificar si necesita refresh (tip 21: solo si mid cambio >= 2c)
        current_mid = market_data.get("mid_price", 0.0)
        if not self.needs_refresh(condition_id, current_mid):
            return []

        # Gestionar inventario antes de evaluar
        self.manage_inventory(market_data)

        if not self.should_act(market_data):
            return []

        signals = self.evaluate(market_data)
        if not signals:
            return []

        trades = self.execute(signals)
        all_trades.extend(trades)

        # Marcar como refresheado (guarda mid para proxima comparacion)
        self.mark_refreshed(condition_id, current_mid)

        return all_trades

    def process_fill(self, trade: Trade) -> list[Trade]:
        """Procesa un fill confirmado y genera reposicionamiento si aplica.

        Llamado desde main.py cuando se detecta que una orden fue matcheada.

        Returns:
            Lista de trades de reposicionamiento ejecutados (puede ser vacia).
        """
        signal = self._repositioner.on_fill(trade)
        if signal is None:
            return []

        # Ejecutar la señal de reposicionamiento directamente
        result = self._client.place_limit_order(
            token_id=signal.token_id,
            side=signal.side,
            price=signal.price,
            size=signal.size,
            post_only=True,
        )
        order_id = result.get("order_id", "unknown")

        # Registrar el order_id para tracking
        self._repositioner.register_reposition_order(
            trade.order_id, order_id
        )

        reposition_trade = self._make_trade(
            signal=signal,
            order_id=order_id,
            status=result.get("status", "submitted"),
            fee_paid=0.0,
        )
        self.log_trade(reposition_trade)

        self._logger.info(
            f"Reposition placed: {signal.side} {signal.size} @ {signal.price:.4f} "
            f"(after fill @ {trade.price:.4f})"
        )
        return [reposition_trade]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_effective_order_size(self) -> float:
        """Retorna el tamaño de orden ajustado por hora del dia y jitter.

        Tip 19: boost +30% en ventana prime (00-04 UTC) para acumular rewards.
        Anti-frontrunning: aplica ±15% de variacion aleatoria al tamaño.
        """
        current_hour = datetime.now(timezone.utc).hour
        base_size = self._order_size
        if current_hour in self._low_activity_hours:
            base_size = self._order_size * self._low_activity_factor
            self._logger.debug(
                "Baja actividad (hora %d UTC): order_size reducido a %.1f",
                current_hour, base_size,
            )
        elif current_hour in self._prime_hours:
            base_size = self._order_size * self._prime_boost
            self._logger.debug(
                "Ventana prime (hora %d UTC): order_size boost a %.1f",
                current_hour, base_size,
            )

        if self._jitter_enabled and self._jitter_size_pct > 0:
            jitter = random.uniform(
                1.0 - self._jitter_size_pct,
                1.0 + self._jitter_size_pct,
            )
            jittered = base_size * jitter
            self._logger.debug("Jitter tamaño: %.2f → %.2f", base_size, jittered)
            return max(1.0, jittered)

        return base_size

    def _apply_price_jitter(self, price: float, tick_size: float) -> float:
        """Aplica jitter de ±1 tick al precio con probabilidad 30%.

        Reduce predictibilidad de los precios de las ordenes.
        """
        if not self._jitter_enabled:
            return price
        if random.random() < self._jitter_price_prob:
            direction = random.choice([-1, 0, 1])
            return round(price + direction * tick_size, 4)
        return price

    def _jittered_refresh_seconds(self) -> float:
        """Retorna el tiempo de refresh con jitter ±timing_jitter_sec."""
        if not self._jitter_enabled or self._jitter_timing_sec <= 0:
            return float(self._refresh_seconds)
        jitter = random.uniform(-self._jitter_timing_sec, self._jitter_timing_sec)
        return max(10.0, self._refresh_seconds + jitter)

    def _get_tick_size(self, token_id: str) -> float:
        """Obtiene tick_size del mercado via cliente."""
        try:
            tick_str = self._client.get_tick_size(token_id)
            return float(tick_str)
        except Exception:
            return 0.01  # Default conservador

    def _get_fee_rate(self, token_id: str) -> int:
        """Obtiene fee_rate en bps del mercado via cliente."""
        try:
            return self._client.get_fee_rate(token_id)
        except Exception:
            return 0  # Post Only = 0 fees normalmente

    def _update_inventory(self, token_id: str, side: str, size: float) -> None:
        """Actualiza inventario interno tras un trade."""
        current = self._inventory.get(token_id, 0.0)
        if side == "BUY":
            self._inventory[token_id] = current + size
        else:
            self._inventory[token_id] = current - size

    @staticmethod
    def _round_to_tick(price: float, tick_size: float) -> float:
        """Redondea precio al tick_size mas cercano."""
        if tick_size <= 0:
            return round(price, 4)
        return round(round(price / tick_size) * tick_size, 4)

    @staticmethod
    def _extract_book_side(
        market_data: dict[str, Any], side: str
    ) -> list[tuple[float, float]]:
        """Extrae un lado del orderbook como lista de (price, size) ordenada.

        side: "bids" (mejor primero, precio desc) | "asks" (mejor primero, precio asc).
        Retorna [] si no hay book disponible.
        """
        book = market_data.get("orderbook", {})
        levels = book.get(side, [])
        result: list[tuple[float, float]] = []
        for lvl in levels:
            try:
                price = float(lvl.get("price", lvl.get("p", 0)))
                size = float(lvl.get("size", lvl.get("s", 0)))
                if price > 0 and size > 0:
                    result.append((price, size))
            except (TypeError, ValueError):
                continue
        return result

    def get_inventory(self, token_id: str) -> float:
        """Retorna inventario neto para un token."""
        return self._inventory.get(token_id, 0.0)

    def get_paused_sides(self, token_id: str) -> set[str]:
        """Retorna lados pausados para un token."""
        return self._paused_sides.get(token_id, set())

    def _is_near_resolution(self, market_data: dict[str, Any]) -> bool:
        """True si el mercado resuelve en menos de near_resolution_hours."""
        end_date_str = market_data.get("end_date_iso", market_data.get("end_date", ""))
        if not end_date_str:
            return False
        try:
            end_dt = datetime.fromisoformat(str(end_date_str).replace("Z", "+00:00"))
            hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
            return hours_left < self._near_resolution_hours
        except (ValueError, TypeError, AttributeError):
            return False
