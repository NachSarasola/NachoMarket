"""Multi-Armed Bandit Strategy Allocator con Thompson Sampling (TODO 3.1).

Asigna capital dinamicamente a estrategias segun su historial de PnL.
Cada dia actualiza la posterior Beta(wins, losses) por estrategia.
Capital asignado ∝ probabilidad de ser la mejor estrategia.

Exploration rate decae de 30% a 5% en 90 dias (epsilon-greedy sobre el sampling).
"""

import json
import logging
import math
import random
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.allocator")

_ALPHA_PRIOR = 1.0      # Prior Beta(1,1) = uniforme
_BETA_PRIOR = 1.0
_EXPLORE_START = 0.30   # 30% exploration al inicio
_EXPLORE_END = 0.05     # 5% al cabo de 90 dias
_EXPLORE_DECAY_DAYS = 90.0
_SECONDS_PER_DAY = 86400.0
_STATE_FILE = "data/allocator_state.json"


class StrategyAllocator:
    """Thompson Sampling Multi-Armed Bandit para asignacion de capital.

    Uso:
        allocator = StrategyAllocator(["market_maker", "multi_arb", "stat_arb"])
        allocator.record_outcome("market_maker", pnl=1.5)  # Win
        allocator.record_outcome("multi_arb", pnl=-0.5)    # Loss
        allocs = allocator.get_allocations(total_capital=400.0)
        # → {"market_maker": 250.0, "multi_arb": 100.0, "stat_arb": 50.0}
    """

    def __init__(
        self,
        strategy_names: list[str],
        state_path: str = _STATE_FILE,
        explore_start: float = _EXPLORE_START,
        explore_end: float = _EXPLORE_END,
        explore_decay_days: float = _EXPLORE_DECAY_DAYS,
        seed: int | None = None,
    ) -> None:
        self._strategies = list(strategy_names)
        self._state_path = Path(state_path)
        self._explore_start = explore_start
        self._explore_end = explore_end
        self._explore_decay_days = explore_decay_days
        self._rng = random.Random(seed)

        # {strategy_name: {"alpha": float, "beta": float, "total_pnl": float}}
        self._state: dict[str, dict[str, float]] = {}
        self._creation_time: float = time.time()

        self._load_state()
        self._ensure_all_strategies()

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def record_outcome(self, strategy_name: str, pnl: float) -> None:
        """Registra el resultado de un trade de una estrategia.

        Un pnl > 0 incrementa alpha (win), pnl <= 0 incrementa beta (loss).

        Args:
            strategy_name: Nombre de la estrategia.
            pnl: PnL del trade en USDC.
        """
        if strategy_name not in self._state:
            self._state[strategy_name] = {
                "alpha": _ALPHA_PRIOR,
                "beta": _BETA_PRIOR,
                "total_pnl": 0.0,
            }

        entry = self._state[strategy_name]
        if pnl > 0:
            entry["alpha"] += 1.0
        else:
            entry["beta"] += 1.0
        entry["total_pnl"] = entry.get("total_pnl", 0.0) + pnl
        self._save_state()

    def get_allocations(self, total_capital: float) -> dict[str, float]:
        """Calcula asignacion de capital via Thompson Sampling.

        Con probabilidad epsilon: exploracion uniforme.
        Con probabilidad (1-epsilon): sampling Thompson.

        Returns:
            Dict {strategy_name: capital_usdc} que suma = total_capital.
        """
        if not self._strategies:
            return {}

        epsilon = self._current_epsilon()

        if self._rng.random() < epsilon:
            # Exploración: distribucion uniforme
            alloc_per = total_capital / len(self._strategies)
            return {s: round(alloc_per, 2) for s in self._strategies}

        # Thompson Sampling: samplear Beta(alpha, beta) para cada estrategia
        samples = {}
        for s in self._strategies:
            entry = self._state.get(s, {"alpha": _ALPHA_PRIOR, "beta": _BETA_PRIOR})
            sample = self._beta_sample(entry["alpha"], entry["beta"])
            samples[s] = max(sample, 1e-9)  # Evitar 0

        total_sample = sum(samples.values())
        return {
            s: round(total_capital * samples[s] / total_sample, 2)
            for s in self._strategies
        }

    def get_win_probs(self) -> dict[str, float]:
        """Retorna la probabilidad de win estimada (media de Beta) por estrategia."""
        result = {}
        for s in self._strategies:
            entry = self._state.get(s, {"alpha": _ALPHA_PRIOR, "beta": _BETA_PRIOR})
            alpha = entry["alpha"]
            beta = entry["beta"]
            result[s] = alpha / (alpha + beta)
        return result

    def get_stats(self) -> dict[str, Any]:
        """Retorna estadisticas completas del allocator."""
        allocs = self.get_allocations(400.0)
        win_probs = self.get_win_probs()
        epsilon = self._current_epsilon()

        strategies_info = {}
        for s in self._strategies:
            entry = self._state.get(s, {"alpha": _ALPHA_PRIOR, "beta": _BETA_PRIOR})
            strategies_info[s] = {
                "wins": int(entry["alpha"] - _ALPHA_PRIOR),
                "losses": int(entry["beta"] - _BETA_PRIOR),
                "total_pnl": round(entry.get("total_pnl", 0.0), 4),
                "win_prob": round(win_probs[s], 3),
                "allocation_pct": round(allocs.get(s, 0) / 4.0, 1),  # % de $400
            }

        return {
            "epsilon": round(epsilon, 3),
            "strategies": strategies_info,
            "days_since_creation": round(
                (time.time() - self._creation_time) / _SECONDS_PER_DAY, 1
            ),
        }

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _current_epsilon(self) -> float:
        """Calcula epsilon actual con decay exponencial."""
        days_elapsed = (time.time() - self._creation_time) / _SECONDS_PER_DAY
        t = min(1.0, days_elapsed / self._explore_decay_days)
        # Interpolacion lineal de explore_start a explore_end
        return self._explore_start + t * (self._explore_end - self._explore_start)

    def _beta_sample(self, alpha: float, beta: float) -> float:
        """Samplea de una distribucion Beta(alpha, beta).

        Usa la relacion Beta = X/(X+Y) donde X~Gamma(alpha) e Y~Gamma(beta).
        """
        try:
            x = self._rng.gammavariate(max(alpha, 0.1), 1.0)
            y = self._rng.gammavariate(max(beta, 0.1), 1.0)
            return x / (x + y) if (x + y) > 0 else 0.5
        except (ValueError, ZeroDivisionError):
            return 0.5

    def _ensure_all_strategies(self) -> None:
        """Asegura que todas las estrategias tengan entradas en el estado."""
        for s in self._strategies:
            if s not in self._state:
                self._state[s] = {
                    "alpha": _ALPHA_PRIOR,
                    "beta": _BETA_PRIOR,
                    "total_pnl": 0.0,
                }

    def _load_state(self) -> None:
        """Carga estado previo del allocator desde disco."""
        if not self._state_path.exists():
            return
        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._state = data.get("state", {})
            self._creation_time = data.get("creation_time", self._creation_time)
        except Exception:
            logger.exception("Error cargando estado del allocator")

    def _save_state(self) -> None:
        """Persiste estado en disco."""
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._state_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"state": self._state, "creation_time": self._creation_time},
                    f, indent=2,
                )
        except Exception:
            logger.exception("Error guardando estado del allocator")
