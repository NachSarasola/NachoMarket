"""Copy-trading strategy: sigue a whale traders de PolyScan (TODO 4.5).

Lógica:
  1. WhaleTracker provee trades recientes de wallets con alto PnL 30d.
  2. Si un whale entra >$5000 en un mercado, esta estrategia copia
     una fracción proporcional (5-10% del size, escalado a nuestro capital).
  3. Stop-loss automático: si la posición pierde >10% → exit.
  4. Las posiciones se gestionan en _open_positions.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.copy_trade")


@dataclass
class CopyPosition:
    """Posición abierta de copy-trade."""

    market_id: str
    token_id: str
    side: str
    entry_price: float
    size: float
    whale_size: float
    stop_loss_pct: float = 0.10   # 10% stop-loss por defecto
    opened_at: float = field(default_factory=time.time)

    @property
    def stop_price_buy(self) -> float:
        """Precio de stop-loss para posición BUY."""
        return self.entry_price * (1.0 - self.stop_loss_pct)

    @property
    def stop_price_sell(self) -> float:
        """Precio de stop-loss para posición SELL (short side)."""
        return self.entry_price * (1.0 + self.stop_loss_pct)


class CopyTradeStrategy(BaseStrategy):
    """Estrategia que replica trades de whale wallets con alta rentabilidad.

    Parámetros (config['copy_trade']):
        min_whale_size_usdc: Tamaño mínimo del trade whale para copiar (default $5000)
        copy_fraction: Fracción del whale size a copiar (default 0.07 = 7%)
        max_copy_size_usdc: Máximo USDC a poner en una sola copia (default $20)
        stop_loss_pct: Stop-loss automático como porcentaje (default 0.10 = 10%)
        max_positions: Máximo de posiciones abiertas simultáneas (default 3)
        lookback_hours: Ventana de lookback para whale trades (default 1.0h)
    """

    name = "copy_trade"

    def __init__(
        self,
        client: Any,
        config: dict[str, Any],
        whale_tracker: Any | None = None,
    ) -> None:
        super().__init__("copy_trade", client, config)
        ct_cfg = config.get("copy_trade", {})

        self._min_whale_size: float = ct_cfg.get("min_whale_size_usdc", 5000.0)
        self._copy_fraction: float = ct_cfg.get("copy_fraction", 0.07)
        self._max_copy_size: float = ct_cfg.get("max_copy_size_usdc", 20.0)
        self._stop_loss_pct: float = ct_cfg.get("stop_loss_pct", 0.10)
        self._max_positions: int = ct_cfg.get("max_positions", 3)
        self._lookback_hours: float = ct_cfg.get("lookback_hours", 1.0)

        # Whale tracker (inyectable; puede ser None en paper mode)
        self._whale_tracker = whale_tracker

        # Posiciones abiertas: market_id → CopyPosition
        self._open_positions: dict[str, CopyPosition] = {}
        # IDs de trades whale ya procesados para no duplicar
        self._processed_whale_ids: set[str] = set()

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Actuar si hay un whale tracker disponible y posiciones libres."""
        if self._whale_tracker is None:
            return False
        if len(self._open_positions) >= self._max_positions:
            return False
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Busca whale trades recientes y genera señales de copia."""
        if self._whale_tracker is None:
            return []

        market_id = market_data.get("condition_id", "")
        if not market_id:
            return []

        # Obtener whale trades recientes para este mercado
        try:
            whale_trades = self._whale_tracker.get_recent_whales(
                market_id=market_id,
                lookback_hours=self._lookback_hours,
            )
        except Exception:
            logger.exception("Error consultando WhaleTracker para %s...", market_id[:12])
            return []

        signals: list[Signal] = []
        tokens = market_data.get("tokens", [])
        if not tokens:
            return []

        token_id = tokens[0].get("token_id", "")
        mid_price = market_data.get("mid_price", 0.5)

        for wt in whale_trades:
            trade_id = wt.get("trade_id", "")
            if trade_id in self._processed_whale_ids:
                continue

            whale_size = float(wt.get("size", 0))
            if whale_size < self._min_whale_size:
                continue

            if market_id in self._open_positions:
                continue  # Ya tenemos posición en este mercado

            if len(self._open_positions) >= self._max_positions:
                break

            # Calcular size a copiar: fraction del whale, capped por max
            copy_size = min(whale_size * self._copy_fraction, self._max_copy_size)
            if copy_size < 1.0:
                continue

            side = wt.get("side", "BUY").upper()
            price = float(wt.get("price", mid_price))

            signal = Signal(
                market_id=market_id,
                token_id=token_id,
                side=side,
                price=price,
                size=copy_size,
                confidence=0.6,  # Confianza media para copy-trades
                strategy_name=self.name,
                metadata={
                    "whale_trade_id": trade_id,
                    "whale_size": whale_size,
                    "copy_fraction": self._copy_fraction,
                },
            )
            signals.append(signal)

            self._processed_whale_ids.add(trade_id)
            # Mantener el set acotado
            if len(self._processed_whale_ids) > 500:
                oldest = next(iter(self._processed_whale_ids))
                self._processed_whale_ids.discard(oldest)

        return signals

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Ejecuta las señales de copia."""
        trades: list[Trade] = []
        for sig in signals:
            try:
                trade = self._client.place_order(
                    token_id=sig.token_id,
                    side=sig.side,
                    size=sig.size,
                    price=sig.price,
                    order_type="FOK",  # Fill-or-Kill para atomicidad
                )
                if trade and trade.status not in ("error", "rejected"):
                    self._open_positions[sig.market_id] = CopyPosition(
                        market_id=sig.market_id,
                        token_id=sig.token_id,
                        side=sig.side,
                        entry_price=trade.price,
                        size=trade.size,
                        whale_size=sig.metadata.get("whale_size", 0),
                        stop_loss_pct=self._stop_loss_pct,
                    )
                    logger.info(
                        "CopyTrade abierto: %s %s@%.4f size=%.2f (whale=%.0f)",
                        sig.side, sig.market_id[:12], sig.price, sig.size,
                        sig.metadata.get("whale_size", 0),
                    )
                    trades.append(trade)
            except Exception:
                logger.exception("Error ejecutando copia en %s...", sig.market_id[:12])

        return trades

    # ------------------------------------------------------------------
    # Stop-loss management
    # ------------------------------------------------------------------

    def check_stop_losses(self, market_data_map: dict[str, dict[str, Any]]) -> list[Trade]:
        """Verifica stop-losses en todas las posiciones abiertas.

        Llamar desde el trading cycle con un mapa de market_id → market_data.

        Returns:
            Lista de Trade de exit ejecutados.
        """
        exits: list[Trade] = []
        to_close: list[str] = []

        for market_id, pos in self._open_positions.items():
            mkt = market_data_map.get(market_id, {})
            mid = mkt.get("mid_price", 0.0)
            if mid <= 0:
                continue

            triggered = False
            if pos.side == "BUY" and mid <= pos.stop_price_buy:
                triggered = True
                logger.warning(
                    "Stop-loss BUY: %s mid=%.4f <= stop=%.4f",
                    market_id[:12], mid, pos.stop_price_buy,
                )
            elif pos.side == "SELL" and mid >= pos.stop_price_sell:
                triggered = True
                logger.warning(
                    "Stop-loss SELL: %s mid=%.4f >= stop=%.4f",
                    market_id[:12], mid, pos.stop_price_sell,
                )

            if not triggered:
                continue

            # Ejecutar exit como orden contraria
            exit_side = "SELL" if pos.side == "BUY" else "BUY"
            try:
                trade = self._client.place_order(
                    token_id=pos.token_id,
                    side=exit_side,
                    size=pos.size,
                    price=mid,
                    order_type="MKT",
                )
                if trade:
                    exits.append(trade)
                    to_close.append(market_id)
                    logger.info(
                        "Stop-loss exit ejecutado: %s @ %.4f", market_id[:12], mid
                    )
            except Exception:
                logger.exception("Error ejecutando stop-loss exit en %s...", market_id[:12])

        for mid in to_close:
            self._open_positions.pop(mid, None)

        return exits

    def get_open_positions(self) -> dict[str, CopyPosition]:
        """Retorna copia del dict de posiciones abiertas."""
        return dict(self._open_positions)
