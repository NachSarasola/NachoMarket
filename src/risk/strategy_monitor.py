"""Monitor de rendimiento por estrategia con kill switch automatico (TODO 1.6).

Si una estrategia tiene Calmar ratio < 0.5 sostenido por 14 dias:
- Auto-pausa la estrategia
- Envia alerta Telegram
- Requiere /revive_strategy manual para reactivar

Estrategias muertas se persisten en data/strategy_graveyard.jsonl.
"""

import json
import logging
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.strategy_monitor")

_GRAVEYARD_FILE = Path("data/strategy_graveyard.jsonl")
_KILL_CALMAR_THRESHOLD = 0.5    # Calmar < 0.5 por 14d → matar
_KILL_EVALUATION_DAYS = 14      # Ventana de evaluacion en dias
_MIN_TRADES_FOR_KILL = 10       # Minimo de trades para evaluar
_SECONDS_PER_DAY = 86400.0
_ONE_DAY = _SECONDS_PER_DAY


class StrategyMonitor:
    """Monitorea rendimiento por estrategia y ejecuta kill switch si es necesario.

    Uso:
        monitor = StrategyMonitor(pause_callback=bot.pause_strategy)
        monitor.record_trade("market_maker", pnl=0.5)
        monitor.evaluate()  # Llamar periodicamente (cada 1h)
    """

    def __init__(
        self,
        pause_callback: Any | None = None,
        alert_callback: Any | None = None,
        kill_calmar_threshold: float = _KILL_CALMAR_THRESHOLD,
        kill_evaluation_days: int = _KILL_EVALUATION_DAYS,
        min_trades: int = _MIN_TRADES_FOR_KILL,
    ) -> None:
        self._pause_callback = pause_callback
        self._alert_callback = alert_callback
        self._kill_threshold = kill_calmar_threshold
        self._eval_days = kill_evaluation_days
        self._min_trades = min_trades

        # {strategy_name: deque of (timestamp, pnl)}
        self._trade_history: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque()
        )
        # Estrategias actualmente pausadas por el monitor
        self._killed: set[str] = set()

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def record_trade(self, strategy_name: str, pnl: float) -> None:
        """Registra un trade de una estrategia."""
        self._trade_history[strategy_name].append((time.time(), pnl))

    def evaluate(self) -> list[str]:
        """Evalua todas las estrategias y mata las que tienen bajo rendimiento.

        Returns:
            Lista de estrategias que fueron matadas en este ciclo.
        """
        newly_killed = []
        cutoff = time.time() - self._eval_days * _ONE_DAY

        for strategy, history in self._trade_history.items():
            if strategy in self._killed:
                continue  # Ya muerta

            # Filtrar trades en la ventana de evaluacion
            recent = [(ts, pnl) for ts, pnl in history if ts >= cutoff]
            if len(recent) < self._min_trades:
                continue  # Insuficientes datos

            pnls = [pnl for _, pnl in recent]
            calmar = _calmar_ratio(pnls, self._eval_days)

            logger.debug(
                "Strategy %s: calmar=%.2f (threshold=%.2f, trades=%d)",
                strategy, calmar, self._kill_threshold, len(recent),
            )

            if calmar < self._kill_threshold:
                self._kill_strategy(strategy, calmar, pnls)
                newly_killed.append(strategy)

        return newly_killed

    def revive_strategy(self, strategy_name: str) -> bool:
        """Reactiva una estrategia muerta (requiere /revive_strategy manual).

        Returns:
            True si fue reactivada, False si no estaba muerta.
        """
        if strategy_name not in self._killed:
            return False

        self._killed.discard(strategy_name)
        # Limpiar historial para dar una segunda oportunidad
        self._trade_history[strategy_name].clear()
        logger.info("Estrategia %s REACTIVADA manualmente", strategy_name)
        return True

    def is_killed(self, strategy_name: str) -> bool:
        """Indica si una estrategia fue matada por el monitor."""
        return strategy_name in self._killed

    def get_status(self) -> dict[str, Any]:
        """Retorna estado completo del monitor."""
        result = {}
        cutoff = time.time() - self._eval_days * _ONE_DAY

        for strategy in self._trade_history:
            recent = [
                (ts, pnl) for ts, pnl in self._trade_history[strategy]
                if ts >= cutoff
            ]
            pnls = [p for _, p in recent]
            calmar = _calmar_ratio(pnls, self._eval_days) if len(pnls) >= self._min_trades else None
            result[strategy] = {
                "trade_count_14d": len(recent),
                "calmar_14d": round(calmar, 3) if calmar is not None else None,
                "total_pnl_14d": round(sum(pnls), 4),
                "is_killed": strategy in self._killed,
            }

        return result

    def get_graveyard(self) -> list[dict[str, Any]]:
        """Lee el graveyard desde disco."""
        if not _GRAVEYARD_FILE.exists():
            return []
        entries = []
        try:
            with open(_GRAVEYARD_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        entries.append(json.loads(line))
        except Exception:
            logger.exception("Error leyendo graveyard")
        return entries

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _kill_strategy(
        self,
        strategy_name: str,
        calmar: float,
        pnls: list[float],
    ) -> None:
        """Ejecuta el kill de una estrategia."""
        self._killed.add(strategy_name)

        msg = (
            f"Strategy {strategy_name} KILLED: "
            f"Calmar={calmar:.2f} < threshold={self._kill_threshold:.2f} "
            f"({self._eval_days}d window, {len(pnls)} trades)"
        )
        logger.warning(msg)

        # Persistir en graveyard
        self._write_graveyard(strategy_name, calmar, pnls)

        # Pausar via callback
        if self._pause_callback:
            try:
                self._pause_callback(strategy_name)
            except Exception:
                logger.exception("Error en pause_callback para %s", strategy_name)

        # Alerta Telegram
        if self._alert_callback:
            try:
                self._alert_callback(
                    "strategy_killed",
                    f"☠️ Estrategia *{strategy_name}* PAUSADA automáticamente\n"
                    f"Calmar 14d: `{calmar:.2f}` < `{self._kill_threshold:.1f}`\n"
                    f"Usar `/revive_strategy {strategy_name}` para reactivar.",
                )
            except Exception:
                logger.exception("Error enviando alerta kill para %s", strategy_name)

    def _write_graveyard(
        self,
        strategy_name: str,
        calmar: float,
        pnls: list[float],
    ) -> None:
        """Persiste estrategia muerta en data/strategy_graveyard.jsonl."""
        try:
            _GRAVEYARD_FILE.parent.mkdir(parents=True, exist_ok=True)
            entry = {
                "strategy": strategy_name,
                "killed_at": time.time(),
                "calmar_14d": round(calmar, 3),
                "total_pnl_14d": round(sum(pnls), 4),
                "trade_count": len(pnls),
            }
            with open(_GRAVEYARD_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            logger.exception("Error escribiendo graveyard")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _calmar_ratio(pnls: list[float], days: float) -> float:
    """Calcula Calmar ratio simplificado: annual_return / max_drawdown."""
    if not pnls:
        return 0.0

    annual_return = (sum(pnls) / days) * 365.0

    # Max drawdown desde curva de equity
    equity = 0.0
    peak = 0.0
    mdd = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = equity - peak
        if dd < mdd:
            mdd = dd

    if abs(mdd) < 0.001:
        return float("inf") if annual_return > 0 else 0.0

    return annual_return / abs(mdd)
