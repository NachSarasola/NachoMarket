import logging
import time
from datetime import datetime, timezone
from typing import Any

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

        # Time-based sizing: reducir tamaño en horas de baja actividad
        tw = config.get("time_windows", {})
        self._low_activity_hours: set[int] = set(tw.get("low_activity_hours", range(0, 8)))
        self._low_activity_factor: float = tw.get("low_activity_size_factor", 0.5)

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
                        ))

            # --- ASK (SELL) ---
            if "SELL" not in paused:
                # Si estamos short (skew < 0), alejar el ask (wider)
                ask_offset = level_offset + max(-skew, 0)
                ask_price = self._round_to_tick(mid_price + ask_offset, tick_size)

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

    def needs_refresh(self, condition_id: str) -> bool:
        """Verifica si paso suficiente tiempo desde el ultimo refresh."""
        last = self._last_refresh.get(condition_id, 0.0)
        return (time.time() - last) >= self._refresh_seconds

    def mark_refreshed(self, condition_id: str) -> None:
        """Marca el mercado como recien refresheado."""
        self._last_refresh[condition_id] = time.time()

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

        # Verificar si necesita refresh
        if not self.needs_refresh(condition_id):
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

        # Marcar como refresheado
        self.mark_refreshed(condition_id)

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
        """Retorna el tamaño de orden ajustado por hora del dia (UTC).

        En horas de baja actividad, reduce el tamaño para evitar fill
        involuntario con liquidez escasa.
        """
        current_hour = datetime.now(timezone.utc).hour
        if current_hour in self._low_activity_hours:
            reduced = self._order_size * self._low_activity_factor
            self._logger.debug(
                f"Baja actividad (hora {current_hour} UTC): "
                f"order_size reducido a {reduced:.1f}"
            )
            return reduced
        return self._order_size

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

    def get_inventory(self, token_id: str) -> float:
        """Retorna inventario neto para un token."""
        return self._inventory.get(token_id, 0.0)

    def get_paused_sides(self, token_id: str) -> set[str]:
        """Retorna lados pausados para un token."""
        return self._paused_sides.get(token_id, set())
