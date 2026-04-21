"""Estrategia direccional con detección de régimen y Kelly fraccional (Fase 3).

Reemplaza la lógica predictiva anterior por un pipeline de 3 pasos:

  1. Régimen: sólo opera en MEAN_REVERTING (Hurst < 0.45) donde existe
     reversión a la media demostrable.
  2. Edge: desviación del midprice vs fair_value estimado, filtrada por
     CostModel (edge debe cubrir fees + slippage + oportunidad × 2).
  3. Sizing: Kelly fraccional (Quarter-Kelly) con precio de mercado como
     probabilidad implícita del contrato.

Patrones GoF:
- Template Method: hereda run() de BaseStrategy; sobreescribe should_act/evaluate/execute.
- Strategy: el CostModel y el RegimeDetector son dependencias intercambiables.

Programación funcional:
- _compute_fair_value(): función pura basada en VWAP 24h + desviación reciente.
- _build_signal(): función pura que construye el Signal a partir de los inputs.
"""

from __future__ import annotations

import logging
from typing import Any

from src.analysis.cost_model import CostModel
from src.analysis.regime_detector import MarketRegimeDetector, Regime
from src.risk.position_sizer import kelly_fraction, calculate_size
from src.strategy.base import BaseStrategy, Signal, Trade

logger = logging.getLogger("nachomarket.strategy.directional")

_DEFAULT_CAPITAL = 400.0
_DEFAULT_MIN_SIZE = 5.0
_DEFAULT_MAX_SIZE = 20.0      # 5% de $400 — regla INQUEBRANTABLE
_DEFAULT_HOLD_SEC = 7200.0    # 2h de tenencia estimada para cost model
_DEFAULT_KELLY_MULT = 0.25    # Quarter-Kelly


class DirectionalStrategy(BaseStrategy):
    """Trading direccional basado en régimen de mercado y edge vs fair value.

    Sólo actúa en régimen MEAN_REVERTING con edge positivo validado por CostModel.
    La dirección (BUY/SELL) viene de la desviación del mid respecto al fair value.
    El tamaño usa Quarter-Kelly con el precio de mercado como probabilidad implícita.
    """

    def __init__(self, client: Any, config: dict[str, Any], **kwargs: Any) -> None:
        super().__init__("directional", client, config, **kwargs)

        dir_cfg = config.get("directional", {})
        self._capital: float = config.get("capital_total", _DEFAULT_CAPITAL)
        self._min_size: float = dir_cfg.get("min_size_usdc", _DEFAULT_MIN_SIZE)
        self._max_size: float = dir_cfg.get("max_size_usdc", _DEFAULT_MAX_SIZE)
        self._kelly_mult: float = dir_cfg.get("kelly_fraction", _DEFAULT_KELLY_MULT)
        self._hold_sec: float = dir_cfg.get("expected_hold_seconds", _DEFAULT_HOLD_SEC)
        self._min_edge_bps: float = dir_cfg.get("min_edge_bps", 100.0)  # 1% mínimo

        self._regime_detector = MarketRegimeDetector()
        self._cost_model = CostModel(config)

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def should_act(self, market_data: dict[str, Any]) -> bool:
        """Sólo actúa si el régimen es MEAN_REVERTING o hay datos insuficientes."""
        token_id = self._resolve_token_id(market_data)
        mid = float(market_data.get("mid_price", 0.5))
        if mid > 0:
            self._regime_detector.update(token_id, mid)

        state = self._regime_detector.get_state(token_id)
        if state.regime == Regime.VOLATILE:
            self._logger.debug("Régimen VOLATILE — saltando directional para %s", token_id[:12])
            return False
        if state.regime == Regime.TRENDING:
            self._logger.debug("Régimen TRENDING — saltando directional para %s", token_id[:12])
            return False
        return True

    def evaluate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Genera señal si hay edge positivo sobre fair_value en régimen mean-reverting."""
        token_id = self._resolve_token_id(market_data)
        market_id = market_data.get("condition_id", market_data.get("market_id", ""))
        mid = float(market_data.get("mid_price", 0.5))

        if mid <= 0.02 or mid >= 0.98:
            return []

        fair_value = _compute_fair_value(market_data)
        if fair_value <= 0:
            return []

        deviation = mid - fair_value          # positivo → mid sobreestimado (SELL)
        edge_bps = abs(deviation) * 10_000.0  # convertir a basis points

        # Filtrar por CostModel: edge debe cubrir fees + slippage + opp × 2
        cost_estimate = self._cost_model.estimate(
            size_usdc=self._min_size,
            edge_bps=edge_bps,
            is_post_only=True,
            expected_hold_seconds=self._hold_sec,
        )
        if not cost_estimate.is_profitable:
            self._logger.debug(
                "Edge insuficiente para directional: %.0fbps < min=%.0fbps",
                edge_bps, cost_estimate.total_bps * 2,
            )
            return []

        if edge_bps < self._min_edge_bps:
            return []

        # Kelly fraccional: usa fair_value como probabilidad estimada
        # y mid como probabilidad implícita del mercado
        if deviation > 0:
            # Mid > fair_value → mercado sobreestima la prob → SELL
            estimated_prob = 1.0 - fair_value
            market_price = 1.0 - mid
            side = "SELL"
            limit_price = round(mid * 1.005, 4)  # ligeramente por encima del mid
        else:
            # Mid < fair_value → mercado subestima → BUY
            estimated_prob = fair_value
            market_price = mid
            side = "BUY"
            limit_price = round(mid * 0.995, 4)  # ligeramente por debajo del mid

        kf = kelly_fraction(estimated_prob, market_price, self._kelly_mult)
        size = calculate_size(self._capital, kf, self._min_size, self._max_size)

        if size <= 0:
            return []

        signal = _build_signal(
            strategy_name=self.name,
            market_id=market_id,
            token_id=token_id,
            side=side,
            price=limit_price,
            size=size,
            confidence=min(edge_bps / 500.0, 1.0),
            edge_bps=edge_bps,
            fair_value=fair_value,
        )

        self._logger.info(
            "Directional signal: %s %.2f USDC @ %.4f | edge=%.0fbps | "
            "fair=%.4f mid=%.4f | kf=%.4f",
            side, size, limit_price, edge_bps, fair_value, mid, kf,
        )
        return [signal]

    def execute(self, signals: list[Signal]) -> list[Trade]:
        """Ejecuta señales direccionales como órdenes Post Only."""
        trades: list[Trade] = []

        for signal in signals:
            try:
                result = self._client.place_limit_order(
                    token_id=signal.token_id,
                    side=signal.side,
                    price=signal.price,
                    size=signal.size,
                    post_only=True,
                )
                trade = self._make_trade(
                    signal=signal,
                    order_id=result.get("order_id", "unknown"),
                    status=result.get("status", "submitted"),
                )
                self.log_trade(trade)
                trades.append(trade)

            except Exception:
                self._logger.exception(
                    "Error colocando orden directional: %s @ %.4f",
                    signal.side, signal.price,
                )
                trade = self._make_trade(signal=signal, order_id="", status="error")
                self.log_trade(trade)
                trades.append(trade)

        return trades

    # ------------------------------------------------------------------
    # Helpers de instancia
    # ------------------------------------------------------------------

    def _resolve_token_id(self, market_data: dict[str, Any]) -> str:
        token_id = market_data.get("token_id", "")
        if not token_id:
            tokens = market_data.get("tokens", [])
            if tokens:
                token_id = tokens[0].get("token_id", "")
        return token_id or market_data.get("condition_id", "unknown")


# ------------------------------------------------------------------
# Funciones puras — sin efectos secundarios
# ------------------------------------------------------------------

def _compute_fair_value(market_data: dict[str, Any]) -> float:
    """Estima el fair value a partir de VWAP 24h y desviación reciente.

    Función pura: sólo lee de market_data, no modifica estado externo.

    Lógica:
    - Si hay avg_price_24h: usar como ancla de fair value.
    - Ajustar por volume_ratio: si hay más volumen del normal → precio es
      más informativo, reducir suavemente la regresión al promedio.
    - Fallback: mid_price si no hay datos históricos.
    """
    mid = float(market_data.get("mid_price", 0.0))
    avg_24h = float(market_data.get("avg_price_24h", 0.0))
    volume_ratio = float(market_data.get("volume_ratio", 1.0))

    if avg_24h <= 0 or avg_24h >= 1:
        return mid

    # Peso del promedio histórico: mayor volumen → mercado más informado
    # → dar más peso al precio actual (price discovery)
    hist_weight = max(0.3, min(0.8, 1.0 / max(volume_ratio, 0.5)))
    fair = hist_weight * avg_24h + (1.0 - hist_weight) * mid

    return round(max(0.02, min(0.98, fair)), 4)


def _build_signal(
    strategy_name: str,
    market_id: str,
    token_id: str,
    side: str,
    price: float,
    size: float,
    confidence: float,
    edge_bps: float,
    fair_value: float,
) -> Signal:
    """Factory function pura que construye un Signal con metadata enriquecida."""
    return Signal(
        market_id=market_id,
        token_id=token_id,
        side=side,
        price=price,
        size=size,
        confidence=confidence,
        strategy_name=strategy_name,
        metadata={
            "edge_bps": round(edge_bps, 1),
            "fair_value": fair_value,
        },
    )
