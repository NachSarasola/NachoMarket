"""Walk-Forward Backtesting Engine (TODO 3.2).

Valida estrategias sobre datos historicos sin overfitting usando
ventanas de train/test solapadas.

Estructura:
    |--- train_days ---|--- test_days ---|
                        |--- train_days ---|--- test_days ---|
                                           ...

Cada fold: entrena sobre train_days, evalua en test_days, reporta metricas.
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from src.analysis.performance_metrics import PerformanceMetrics

logger = logging.getLogger("nachomarket.backtest")

_DEFAULT_TRAIN_DAYS = 14
_DEFAULT_TEST_DAYS = 7
_DEFAULT_N_FOLDS = 5
_HISTORICAL_DATA_DIR = Path("data/historical")


@dataclass
class FoldResult:
    """Resultado de un fold del walk-forward test."""
    fold_id: int
    train_start: float      # Unix timestamp
    train_end: float
    test_start: float
    test_end: float
    train_trades: int
    test_trades: int
    test_pnl: float
    test_sharpe: float
    test_win_rate: float
    test_max_drawdown: float


@dataclass
class BacktestResult:
    """Resultado completo del walk-forward backtest."""
    strategy_name: str
    n_folds: int
    fold_results: list[FoldResult] = field(default_factory=list)
    avg_test_pnl: float = 0.0
    avg_test_sharpe: float = 0.0
    avg_win_rate: float = 0.0
    max_drawdown: float = 0.0
    is_valid: bool = False    # True si Sharpe > 0.5 en mayoria de folds

    def summary(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy_name,
            "n_folds": self.n_folds,
            "avg_test_pnl": round(self.avg_test_pnl, 4),
            "avg_test_sharpe": round(self.avg_test_sharpe, 3),
            "avg_win_rate": round(self.avg_win_rate, 3),
            "max_drawdown": round(self.max_drawdown, 4),
            "is_valid": self.is_valid,
        }


class BacktestEngine:
    """Motor de backtesting walk-forward sobre datos historicos de orderbooks.

    Los datos historicos se almacenan en data/historical/*.jsonl
    (snapshots del orderbook guardados por el bot en produccion).

    Uso:
        engine = BacktestEngine()
        result = engine.run_walk_forward("market_maker")
        if result.is_valid:
            print("Estrategia validada, ok para produccion")
    """

    def __init__(
        self,
        data_dir: str = str(_HISTORICAL_DATA_DIR),
        commission_bps: float = 0.0,   # 0 para Post Only MM
    ) -> None:
        self._data_dir = Path(data_dir)
        self._commission_bps = commission_bps

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def run_walk_forward(
        self,
        strategy_name: str,
        train_days: int = _DEFAULT_TRAIN_DAYS,
        test_days: int = _DEFAULT_TEST_DAYS,
        n_folds: int = _DEFAULT_N_FOLDS,
        signal_generator: Callable | None = None,
    ) -> BacktestResult:
        """Ejecuta walk-forward validation sobre datos historicos.

        Args:
            strategy_name: Nombre de la estrategia a validar.
            train_days: Dias de entrenamiento por fold.
            test_days: Dias de test por fold.
            n_folds: Numero de folds a ejecutar.
            signal_generator: Funcion opcional que genera signals desde
                               market snapshots. Si None, usa datos de trades.jsonl.

        Returns:
            BacktestResult con metricas por fold y metricas agregadas.
        """
        logger.info(
            "Walk-forward %s: %d folds, train=%dd, test=%dd",
            strategy_name, n_folds, train_days, test_days,
        )

        # Cargar datos historicos de trades.jsonl
        trades = self._load_historical_trades(strategy_name)
        if not trades:
            logger.warning("Sin datos historicos para %s", strategy_name)
            return BacktestResult(
                strategy_name=strategy_name,
                n_folds=0,
            )

        fold_results = []
        stride_days = test_days  # Avanzar el fold de a test_days

        for fold_id in range(n_folds):
            # Definir ventanas de tiempo para este fold
            # Fold 0: train=[now-train-n*stride, now-n*stride], test=[now-n*stride, now-(n-1)*stride]
            test_end_offset = (n_folds - fold_id) * stride_days * 86400
            test_start_offset = test_end_offset - test_days * 86400
            train_start_offset = test_start_offset - train_days * 86400
            train_end_offset = test_start_offset

            import time
            now = time.time()
            train_start = now - train_start_offset
            train_end = now - train_end_offset
            test_start = now - test_start_offset
            test_end = now - test_end_offset + test_days * 86400

            # Filtrar trades
            train_trades = [
                t for t in trades
                if train_start <= t.get("_ts", 0) <= train_end
            ]
            test_trades_data = [
                t for t in trades
                if test_start <= t.get("_ts", 0) <= test_end
            ]

            if not test_trades_data:
                continue

            # Calcular metricas del fold
            fold_result = self._evaluate_fold(
                fold_id=fold_id,
                train_trades=train_trades,
                test_trades=test_trades_data,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
            fold_results.append(fold_result)

        if not fold_results:
            return BacktestResult(strategy_name=strategy_name, n_folds=0)

        # Agregar resultados
        avg_pnl = sum(f.test_pnl for f in fold_results) / len(fold_results)
        avg_sharpe = sum(f.test_sharpe for f in fold_results) / len(fold_results)
        avg_wr = sum(f.test_win_rate for f in fold_results) / len(fold_results)
        max_dd = min(f.test_max_drawdown for f in fold_results)
        # Estrategia valida si Sharpe > 0.5 en >50% de folds
        positive_folds = sum(1 for f in fold_results if f.test_sharpe > 0.5)
        is_valid = positive_folds > len(fold_results) / 2

        return BacktestResult(
            strategy_name=strategy_name,
            n_folds=len(fold_results),
            fold_results=fold_results,
            avg_test_pnl=avg_pnl,
            avg_test_sharpe=avg_sharpe,
            avg_win_rate=avg_wr,
            max_drawdown=max_dd,
            is_valid=is_valid,
        )

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _load_historical_trades(self, strategy_name: str) -> list[dict[str, Any]]:
        """Carga trades historicos desde trades.jsonl filtrados por estrategia."""
        path = Path("data/trades.jsonl")
        if not path.exists():
            return []

        import time
        from datetime import datetime, timezone

        trades = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        t = json.loads(line)
                        if strategy_name != "all" and t.get("strategy_name") != strategy_name:
                            continue
                        # Agregar timestamp unix para comparaciones
                        ts_str = t.get("timestamp", "")
                        if ts_str:
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            t["_ts"] = ts.timestamp()
                        else:
                            t["_ts"] = time.time()
                        trades.append(t)
                    except Exception:
                        continue
        except OSError:
            logger.exception("Error leyendo trades.jsonl")

        return sorted(trades, key=lambda t: t.get("_ts", 0))

    def _evaluate_fold(
        self,
        fold_id: int,
        train_trades: list[dict],
        test_trades: list[dict],
        train_start: float,
        train_end: float,
        test_start: float,
        test_end: float,
    ) -> FoldResult:
        """Evalua un fold calculando metricas sobre los test trades."""
        test_pnls = [t.get("pnl", 0.0) or 0.0 for t in test_trades]
        train_pnls = [t.get("pnl", 0.0) or 0.0 for t in train_trades]

        if len(test_pnls) > 1:
            pm = PerformanceMetrics(test_pnls)
            sharpe = pm.sharpe_ratio()
            win_rate = pm.win_rate()
            mdd = pm.max_drawdown()
        else:
            sharpe = 0.0
            win_rate = 1.0 if (test_pnls and test_pnls[0] > 0) else 0.0
            mdd = min(test_pnls) if test_pnls else 0.0

        return FoldResult(
            fold_id=fold_id,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            train_trades=len(train_trades),
            test_trades=len(test_trades),
            test_pnl=round(sum(test_pnls), 4),
            test_sharpe=round(sharpe, 3),
            test_win_rate=round(win_rate, 3),
            test_max_drawdown=round(mdd, 4),
        )
