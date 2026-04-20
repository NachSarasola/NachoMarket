"""Modelo de costos ocultos: fees + slippage + opportunity cost (TODO 2.3).

Estima el costo total real de un trade antes de ejecutarlo.
El Sizer usa este modelo para filtrar signals con edge < 2x costos.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("nachomarket.cost_model")

# Fee de Polymarket para market makers (Post Only)
_DEFAULT_MAKER_FEE_BPS = 0         # 0 bps (Post Only no paga fees)
_DEFAULT_TAKER_FEE_BPS = 100       # 1% (100 bps) si se ejecuta como taker
_RISK_FREE_ANNUAL = 0.04           # 4% anual T-bills
_RISK_FREE_DAILY = _RISK_FREE_ANNUAL / 365.0
_SECONDS_PER_DAY = 86400.0


@dataclass
class CostEstimate:
    """Desglose de costos estimados para un trade."""
    taker_fee_usdc: float = 0.0
    maker_fee_usdc: float = 0.0
    slippage_usdc: float = 0.0
    opportunity_cost_usdc: float = 0.0
    total_usdc: float = 0.0
    total_bps: float = 0.0           # Total en basis points sobre el size
    is_profitable: bool = True        # True si edge > min_edge_multiplier * total


class CostModel:
    """Estima costos totales de un trade para validar profitabilidad.

    Uso:
        model = CostModel(config)
        estimate = model.estimate(signal)
        if estimate.is_profitable:
            execute(signal)
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("cost_model", {})
        self._maker_fee_bps = cfg.get("maker_fee_bps", _DEFAULT_MAKER_FEE_BPS)
        self._taker_fee_bps = cfg.get("taker_fee_bps", _DEFAULT_TAKER_FEE_BPS)
        self._slippage_depth_factor = cfg.get("slippage_depth_factor", 0.005)
        self._rf_daily = cfg.get("risk_free_daily", _RISK_FREE_DAILY)
        # Cuantas veces el costo total debe ser < edge para ejecutar
        self._min_edge_multiplier = cfg.get("min_edge_multiplier", 2.0)

    def estimate(
        self,
        size_usdc: float,
        edge_bps: float,
        is_post_only: bool = True,
        book_depth_usdc: float = 1000.0,
        expected_hold_seconds: float = 3600.0,
    ) -> CostEstimate:
        """Estima el costo total de un trade.

        Args:
            size_usdc: Tamaño de la orden en USDC.
            edge_bps: Edge estimado en basis points (ej. 200 = 2%).
            is_post_only: True si la orden es Post Only (maker, 0 fees en Poly).
            book_depth_usdc: Liquidez total disponible en el libro.
            expected_hold_seconds: Tiempo esperado de tenencia en segundos.

        Returns:
            CostEstimate con desglose y flag is_profitable.
        """
        if size_usdc <= 0:
            return CostEstimate()

        # --- 1. Fee ---
        fee_bps = self._maker_fee_bps if is_post_only else self._taker_fee_bps
        fee_usdc = size_usdc * fee_bps / 10_000

        # --- 2. Slippage estimado (orden grande vs profundidad del libro) ---
        # Modelo simple: slippage ∝ (size / book_depth)^0.5
        depth_ratio = size_usdc / max(book_depth_usdc, 1.0)
        slippage_bps = self._slippage_depth_factor * (depth_ratio ** 0.5) * 10_000
        slippage_usdc = size_usdc * slippage_bps / 10_000

        # --- 3. Opportunity cost (capital inmovilizado * RF rate) ---
        hold_days = expected_hold_seconds / _SECONDS_PER_DAY
        opp_cost_usdc = size_usdc * self._rf_daily * hold_days

        # --- 4. Total ---
        total_usdc = fee_usdc + slippage_usdc + opp_cost_usdc
        total_bps = (total_usdc / size_usdc) * 10_000 if size_usdc > 0 else 0.0

        # --- 5. Es rentable si edge > min_multiplier * total_cost ---
        is_profitable = edge_bps >= self._min_edge_multiplier * total_bps

        return CostEstimate(
            taker_fee_usdc=0.0 if is_post_only else fee_usdc,
            maker_fee_usdc=fee_usdc if is_post_only else 0.0,
            slippage_usdc=round(slippage_usdc, 6),
            opportunity_cost_usdc=round(opp_cost_usdc, 6),
            total_usdc=round(total_usdc, 6),
            total_bps=round(total_bps, 2),
            is_profitable=is_profitable,
        )

    def min_edge_bps(
        self,
        size_usdc: float,
        is_post_only: bool = True,
        book_depth_usdc: float = 1000.0,
        expected_hold_seconds: float = 3600.0,
    ) -> float:
        """Calcula el edge minimo en bps necesario para que el trade sea rentable."""
        est = self.estimate(
            size_usdc=size_usdc,
            edge_bps=0.0,
            is_post_only=is_post_only,
            book_depth_usdc=book_depth_usdc,
            expected_hold_seconds=expected_hold_seconds,
        )
        return est.total_bps * self._min_edge_multiplier

    def filter_signal(
        self,
        signal: dict[str, Any],
        book_depth_usdc: float = 1000.0,
    ) -> bool:
        """Retorna True si el signal supera el costo minimo para ejecutarse.

        Espera signal con keys: size_usdc, edge_bps, is_post_only (opcional).
        """
        size = signal.get("size_usdc", signal.get("size", 0.0))
        edge = signal.get("edge_bps", 0.0)
        post_only = signal.get("post_only", True)
        hold_sec = signal.get("expected_hold_seconds", 3600.0)

        est = self.estimate(
            size_usdc=float(size),
            edge_bps=float(edge),
            is_post_only=bool(post_only),
            book_depth_usdc=book_depth_usdc,
            expected_hold_seconds=float(hold_sec),
        )
        if not est.is_profitable:
            logger.debug(
                "Signal filtrado por cost model: edge=%.0fbps < min=%.0fbps "
                "(fee=%.4f slip=%.4f opp=%.4f)",
                edge, est.total_bps * 2, est.taker_fee_usdc + est.maker_fee_usdc,
                est.slippage_usdc, est.opportunity_cost_usdc,
            )
        return est.is_profitable
