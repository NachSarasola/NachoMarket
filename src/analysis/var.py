"""Value at Risk (VaR) y Stress Testing (TODO 2.5).

Calcula:
- Historical VaR 95% sobre ultimos 60 dias de retornos
- Parametric VaR con ajuste de fat-tail (t-distribution approx)
- Stress scenarios: election crash, whale dump, volatility spike

Si daily VaR > $40, envia alerta.
"""

import logging
import math
from dataclasses import dataclass
from typing import Sequence

logger = logging.getLogger("nachomarket.var")

_CONFIDENCE_95 = 0.95
_CONFIDENCE_99 = 0.99
_VAR_ALERT_THRESHOLD = 15.0   # $15 — alerta si VaR supera esto (5% de $300)
_NORMAL_Z_95 = 1.6449          # z-score para 95% (una cola)
_NORMAL_Z_99 = 2.3263

# Escenarios de stress (multiplicadores sobre el capital)
STRESS_SCENARIOS = {
    "election_day_crash": -0.30,     # -30% en mercados politicos
    "whale_dump": -0.15,             # -15% sudden dump
    "fed_surprise_volatility": 0.10, # +10% vol → spreads más amplios
    "crypto_bear": -0.20,            # -20% en mercados crypto
    "black_swan": -0.50,             # -50% tail event
}


@dataclass
class VaRResult:
    """Resultado del calculo de VaR."""
    historical_var_95: float    # Perdida maxima al 95% de confianza
    historical_var_99: float    # Perdida maxima al 99% de confianza
    parametric_var_95: float    # VaR parametrico con fat-tail correction
    daily_mean: float           # Retorno medio diario
    daily_std: float            # Volatilidad diaria
    n_days: int
    exceeds_alert_threshold: bool  # True si VaR > $40
    stress_results: dict[str, float]  # {scenario: estimated_loss}


class VaRCalculator:
    """Calcula VaR historico, parametrico y escenarios de stress.

    Uso:
        calc = VaRCalculator(daily_returns)
        result = calc.compute()
        if result.exceeds_alert_threshold:
            send_alert(...)
    """

    def __init__(
        self,
        daily_returns: Sequence[float] | None = None,
        capital: float = 300.0,
        var_alert_threshold: float = _VAR_ALERT_THRESHOLD,
    ) -> None:
        """
        Args:
            daily_returns: Retornos diarios en USDC. None o [] = sin datos aún.
            capital: Capital total del bot en USDC.
            var_alert_threshold: Umbral de alerta en USDC.
        """
        self._returns = sorted(daily_returns) if daily_returns else []
        self._capital = capital
        self._alert_threshold = var_alert_threshold

    # ------------------------------------------------------------------
    # Calculo principal
    # ------------------------------------------------------------------

    def compute(self) -> VaRResult:
        """Calcula todas las metricas de VaR y stress tests."""
        n = len(self._returns)

        if n < 5:
            return VaRResult(
                historical_var_95=0.0,
                historical_var_99=0.0,
                parametric_var_95=0.0,
                daily_mean=0.0,
                daily_std=0.0,
                n_days=n,
                exceeds_alert_threshold=False,
                stress_results={},
            )

        # --- Historical VaR (percentil 5% = perdida en el peor 5% de dias) ---
        hist_var_95 = abs(self._percentile(0.05))
        hist_var_99 = abs(self._percentile(0.01))

        # --- Parametric VaR con fat-tail correction ---
        mean = sum(self._returns) / n
        std = _std(self._returns)
        # Fat-tail: usar t-distribution approx (multiply normal z by 1.15 para df~5)
        fat_tail_correction = 1.15
        param_var_95 = abs(mean - std * _NORMAL_Z_95 * fat_tail_correction)

        # --- Stress scenarios ---
        stress = {}
        for scenario, shock in STRESS_SCENARIOS.items():
            # Estimacion: perdida = shock% del capital expuesto
            stress[scenario] = round(self._capital * abs(min(shock, 0)), 2)

        exceeds = hist_var_95 > self._alert_threshold

        return VaRResult(
            historical_var_95=round(hist_var_95, 4),
            historical_var_99=round(hist_var_99, 4),
            parametric_var_95=round(param_var_95, 4),
            daily_mean=round(mean, 4),
            daily_std=round(std, 4),
            n_days=n,
            exceeds_alert_threshold=exceeds,
            stress_results=stress,
        )

    def format_telegram(self) -> str:
        """Formatea resultado para /var Telegram command."""
        r = self.compute()
        icon = "🚨" if r.exceeds_alert_threshold else "🟢"
        lines = [
            f"{icon} *Value at Risk Report* ({r.n_days} dias)\n",
            f"📊 VaR 95% (historical): `${r.historical_var_95:.2f}`",
            f"📊 VaR 99% (historical): `${r.historical_var_99:.2f}`",
            f"📊 VaR 95% (parametric): `${r.parametric_var_95:.2f}`",
            f"📈 Mean daily: `${r.daily_mean:+.4f}`",
            f"📉 Daily std: `${r.daily_std:.4f}`",
            "\n*Stress Scenarios:*",
        ]
        for scenario, loss in r.stress_results.items():
            lines.append(f"  ⚡ {scenario}: `-${loss:.0f}`")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _percentile(self, p: float) -> float:
        """Percentil p de los retornos (ya ordenados)."""
        n = len(self._returns)
        if n == 0:
            return 0.0
        idx = max(0, min(n - 1, int(math.floor(p * n))))
        return self._returns[idx]


def _std(values: list[float]) -> float:
    """Desviacion estandar muestral."""
    n = len(values)
    if n < 2:
        return 0.0
    mu = sum(values) / n
    return math.sqrt(sum((x - mu) ** 2 for x in values) / (n - 1))
