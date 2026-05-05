"""PositionLimits: limites de posiciones concurrentes con auto-cierre.

Reglas:
  1. Max N posiciones abiertas simultaneas (default 15).
  2. Max P% del capital por posicion (default 5%).
  3. Si se excede el limite de posiciones, recomienda cerrar la peor.

Diseniado para ser usado por el orquestador (main.py) y las estrategias.
No importa otros modulos de src/risk/ ÔÇö recibe datos como argumentos.
"""

from __future__ import annotations

from typing import Any

_MAX_POSITIONS = 15
_MAX_PCT = 0.05


class PositionLimitsManager:
    """Control de limites de posiciones."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("position_limits", {})
        self._max_positions = cfg.get("max_positions", _MAX_POSITIONS)
        self._max_pct = cfg.get("max_pct_per_position", _MAX_PCT)
        self._auto_close = cfg.get("auto_close_worst", True)

    def can_open_position(
        self, current_positions: int, capital: float, size_usd: float
    ) -> tuple[bool, str]:
        """Verifica si se puede abrir una nueva posicion.

        Args:
            current_positions: Numero de posiciones abiertas actualmente.
            capital: Capital total disponible.
            size_usd: Tamano propuesto en USD.

        Returns:
            (ok, reason).
        """
        if current_positions >= self._max_positions:
            return False, (
                f"Max positions ({self._max_positions}) reached "
                f"({current_positions} open)"
            )
        if capital <= 0:
            return True, "OK (sin capital)"

        pct = size_usd / capital
        if pct > self._max_pct:
            return False, (
                f"Position size {pct:.1%} exceeds max {self._max_pct:.1%}"
            )
        return True, "OK"

    def get_excess_positions(
        self, positions: dict[str, float], capital: float
    ) -> list[str]:
        """Identifica posiciones que exceden el limite por market.

        Args:
            positions: Dict {market_id: size_usd}.
            capital: Capital total.

        Returns:
            Lista de market_ids que exceden el limite.
        """
        if capital <= 0:
            return []
        limit = capital * self._max_pct
        return [mid for mid, size in positions.items() if size > limit]

    def select_worst_to_close(
        self, positions: dict[str, float], pnl_by_market: dict[str, float]
    ) -> list[str]:
        """Ordena posiciones de peor a mejor para auto-cierre.

        Args:
            positions: {market_id: size_usd}.
            pnl_by_market: {market_id: unrealized_pnl} (negativo = perdida).

        Returns:
            Lista de market_ids ordenados: peor primero.
        """
        scored = [
            (mid, pnl_by_market.get(mid, 0.0))
            for mid in positions
        ]
        scored.sort(key=lambda x: x[1])
        return [mid for mid, _ in scored]

    @property
    def max_positions(self) -> int:
        return self._max_positions

    @property
    def max_pct_per_position(self) -> float:
        return self._max_pct

    @property
    def auto_close_enabled(self) -> bool:
        return self._auto_close
