"""CashReserves: gestion de reserva minima de efectivo.

Garantiza que el bot siempre tenga capital liquido para
pagar fees de gas en Polygon y cubrir emergencias.

Niveles:
  min_reserve_pct: reserva normal (default 0.5% del capital).
    El cash disponible para trading se descuenta de este valor.

  emergency_pct: halt total (default 0.2% del capital).
    Si el cash disponible cae por debajo, se detiene el trading.
"""

from __future__ import annotations

from typing import Any

_MIN_RESERVE = 0.005
_EMERGENCY = 0.002


class CashReserves:
    """Control de reserva minima de efectivo."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("cash_reserves", {})
        self._min_reserve = cfg.get("min_reserve_pct", _MIN_RESERVE)
        self._emergency = cfg.get("emergency_pct", _EMERGENCY)

    def check(self, total_capital: float, cash_available: float) -> tuple[bool, str]:
        """Verifica que el cash disponible sea suficiente.

        Args:
            total_capital: Capital total (balance + exposure).
            cash_available: Cash liquido (balance sin exposure bloqueada).

        Returns:
            (ok, reason) donde ok=False significa emergency halt.
        """
        if total_capital <= 0:
            return True, "OK (sin capital)"

        reserve_pct = cash_available / total_capital
        if reserve_pct < self._emergency:
            return False, (
                f"EMERGENCY: cash reserve {reserve_pct:.2%} < "
                f"{self._emergency:.2%}"
            )
        if reserve_pct < self._min_reserve:
            return True, (
                f"WARNING: cash reserve {reserve_pct:.2%} < "
                f"{self._min_reserve:.2%}"
            )
        return True, f"OK: {reserve_pct:.2%}"

    def available_for_trading(
        self, total_capital: float, cash_available: float
    ) -> float:
        """Cash disponible para trading descontando la reserva minima.

        Returns:
            Cantidad en USD que puede usarse para nuevas operaciones.
        """
        reserve = total_capital * self._min_reserve
        available = cash_available - reserve
        return max(0.0, available)

    def is_emergency(self, total_capital: float, cash_available: float) -> bool:
        """True si el cash esta en nivel de emergencia."""
        ok, _ = self.check(total_capital, cash_available)
        return not ok

    @property
    def min_reserve_pct(self) -> float:
        return self._min_reserve

    @property
    def emergency_pct(self) -> float:
        return self._emergency
