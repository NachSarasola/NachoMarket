"""Metricas de rendimiento ajustadas por riesgo (TODO 2.1).

Calcula Sharpe, Sortino y Calmar ratio sobre retornos diarios.
Usado por SelfReviewer y /stats Telegram command.
"""

import logging
import math
from typing import Sequence

logger = logging.getLogger("nachomarket.performance")

# Risk-free rate anual asumida (T-bills 4%)
_RF_ANNUAL = 0.04
_RF_DAILY = _RF_ANNUAL / 365.0
_TRADING_DAYS_YEAR = 365  # Polymarket opera 24/7


class PerformanceMetrics:
    """Calculador de metricas de rendimiento profesionales.

    Uso tipico:
        pm = PerformanceMetrics(daily_returns)
        sharpe = pm.sharpe_ratio()
        sortino = pm.sortino_ratio()
        calmar = pm.calmar_ratio()
    """

    def __init__(
        self,
        daily_returns: Sequence[float],
        risk_free_daily: float = _RF_DAILY,
    ) -> None:
        """
        Args:
            daily_returns: Lista de retornos diarios en USDC (ej. [0.5, -0.2, 1.1]).
            risk_free_daily: Tasa libre de riesgo diaria.
        """
        self._returns = list(daily_returns)
        self._rf = risk_free_daily

    # ------------------------------------------------------------------
    # Metricas principales
    # ------------------------------------------------------------------

    def sharpe_ratio(self) -> float:
        """Sharpe Ratio anualizado: (mean_excess_return / std) * sqrt(365).

        Returns:
            Sharpe ratio. 0.0 si no hay datos suficientes o std=0.
        """
        if len(self._returns) < 2:
            return 0.0

        excess = [r - self._rf for r in self._returns]
        mean_e = _mean(excess)
        std_e = _std(excess)

        if std_e == 0.0:
            return 0.0 if mean_e == 0.0 else float("inf")

        return (mean_e / std_e) * math.sqrt(_TRADING_DAYS_YEAR)

    def sortino_ratio(self) -> float:
        """Sortino Ratio anualizado: (mean_excess_return / downside_std) * sqrt(365).

        Solo penaliza volatilidad negativa (downside deviation).

        Returns:
            Sortino ratio. 0.0 si no hay datos suficientes o downside_std=0.
        """
        if len(self._returns) < 2:
            return 0.0

        excess = [r - self._rf for r in self._returns]
        mean_e = _mean(excess)

        downside_sq = [r ** 2 for r in excess if r < 0]
        if not downside_sq:
            return float("inf") if mean_e > 0 else 0.0

        downside_std = math.sqrt(_mean(downside_sq))
        if downside_std == 0.0:
            return 0.0

        return (mean_e / downside_std) * math.sqrt(_TRADING_DAYS_YEAR)

    def calmar_ratio(self) -> float:
        """Calmar Ratio: annualized_return / max_drawdown.

        Mide retorno por unidad de caida maxima (risk of ruin).

        Returns:
            Calmar ratio. 0.0 si drawdown=0 o no hay datos.
        """
        if not self._returns:
            return 0.0

        annual_return = _mean(self._returns) * _TRADING_DAYS_YEAR
        mdd = self.max_drawdown()

        if mdd == 0.0:
            return float("inf") if annual_return > 0 else 0.0

        return annual_return / abs(mdd)

    def max_drawdown(self) -> float:
        """Maxima caida desde pico a valle en los retornos acumulados.

        Returns:
            Max drawdown como numero negativo (ej. -15.3 = -$15.30).
        """
        if not self._returns:
            return 0.0

        # Construir curva de equity acumulada
        equity = 0.0
        peak = 0.0
        mdd = 0.0

        for r in self._returns:
            equity += r
            if equity > peak:
                peak = equity
            drawdown = equity - peak
            if drawdown < mdd:
                mdd = drawdown

        return mdd

    def mean_return(self) -> float:
        """Retorno medio diario."""
        return _mean(self._returns) if self._returns else 0.0

    def total_return(self) -> float:
        """Suma total de retornos."""
        return sum(self._returns)

    def win_rate(self) -> float:
        """Porcentaje de dias con retorno positivo."""
        if not self._returns:
            return 0.0
        wins = sum(1 for r in self._returns if r > 0)
        return wins / len(self._returns)

    def annualized_return(self) -> float:
        """Retorno anualizado (media diaria * 365)."""
        return _mean(self._returns) * _TRADING_DAYS_YEAR if self._returns else 0.0

    def volatility(self) -> float:
        """Volatilidad anualizada (std * sqrt(365))."""
        if len(self._returns) < 2:
            return 0.0
        return _std(self._returns) * math.sqrt(_TRADING_DAYS_YEAR)

    def summary(self) -> dict[str, float]:
        """Retorna todas las metricas en un dict."""
        return {
            "sharpe": self.sharpe_ratio(),
            "sortino": self.sortino_ratio(),
            "calmar": self.calmar_ratio(),
            "max_drawdown": self.max_drawdown(),
            "total_return": self.total_return(),
            "annualized_return": self.annualized_return(),
            "volatility": self.volatility(),
            "win_rate": self.win_rate(),
            "n_days": len(self._returns),
        }


# ------------------------------------------------------------------
# Helpers de modulo
# ------------------------------------------------------------------

def _mean(values: list[float]) -> float:
    """Media aritmetica."""
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std(values: list[float], ddof: int = 1) -> float:
    """Desviacion estandar muestral (ddof=1) o poblacional (ddof=0)."""
    n = len(values)
    if n <= ddof:
        return 0.0
    mu = _mean(values)
    variance = sum((x - mu) ** 2 for x in values) / (n - ddof)
    return math.sqrt(variance)


def compute_metrics_from_trades_file(
    trades_path: str = "data/trades.jsonl",
    days: int = 30,
) -> dict[str, float]:
    """Lee trades.jsonl y calcula metricas sobre los ultimos N dias.

    Agrupa trades por dia UTC y calcula PnL diario como retorno.
    Retorna el dict de summary() o vacio si no hay datos.
    """
    import json
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    path = Path(trades_path)
    if not path.exists():
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    daily_pnl: dict[str, float] = {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    trade = json.loads(line)
                    ts_str = trade.get("timestamp", "")
                    pnl = trade.get("pnl", 0.0) or 0.0
                    if not ts_str:
                        continue
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    if ts < cutoff:
                        continue
                    day_key = ts.strftime("%Y-%m-%d")
                    daily_pnl[day_key] = daily_pnl.get(day_key, 0.0) + pnl
                except Exception:
                    continue
    except OSError:
        logger.exception("Error leyendo trades.jsonl para metricas")
        return {}

    if not daily_pnl:
        return {}

    returns = list(daily_pnl.values())
    pm = PerformanceMetrics(returns)
    return pm.summary()
