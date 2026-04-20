"""A/B Testing de parámetros de estrategia en paralelo (TODO 3.3).

Permite correr 2-3 variantes de la misma estrategia con distintos hyperparams.
Cada variante recibe 1/N del capital. Después de N días, consolida en el ganador.

Ejemplo de uso en settings.yaml:
  ab_tests:
    - strategy: market_maker
      variants:
        - name: "spread_tight"
          params: {spread_offset: 0.01}
        - name: "spread_normal"
          params: {spread_offset: 0.02}
        - name: "spread_wide"
          params: {spread_offset: 0.03}
      evaluation_days: 7
      capital_fraction_each: 0.33
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.strategy.ab_tester")

_AB_STATE_FILE = Path("data/ab_test_state.json")


@dataclass
class VariantStats:
    """Estadísticas de una variante A/B."""

    name: str
    params: dict[str, Any]
    pnl_total: float = 0.0
    trade_count: int = 0
    wins: int = 0
    losses: int = 0
    started_at: float = field(default_factory=time.time)
    is_winner: bool = False
    is_eliminated: bool = False

    @property
    def winrate(self) -> float:
        total = self.wins + self.losses
        return self.wins / total if total > 0 else 0.0

    @property
    def avg_pnl_per_trade(self) -> float:
        return self.pnl_total / self.trade_count if self.trade_count > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "params": self.params,
            "pnl_total": self.pnl_total,
            "trade_count": self.trade_count,
            "wins": self.wins,
            "losses": self.losses,
            "winrate": self.winrate,
            "avg_pnl_per_trade": self.avg_pnl_per_trade,
            "started_at": self.started_at,
            "is_winner": self.is_winner,
            "is_eliminated": self.is_eliminated,
        }


class ABTester:
    """Gestiona tests A/B de parámetros de estrategia.

    Args:
        strategy_name: Nombre de la estrategia que se está probando.
        variants: Lista de dicts con 'name' y 'params'.
        evaluation_days: Días antes de consolidar en el ganador.
        capital_fraction_each: Fracción del capital para cada variante.
        consolidation_callback: Llamado con (winner_params) cuando se elige ganador.
    """

    def __init__(
        self,
        strategy_name: str,
        variants: list[dict[str, Any]],
        evaluation_days: int = 7,
        capital_fraction_each: float = 0.33,
        consolidation_callback: Any | None = None,
    ) -> None:
        self._strategy_name = strategy_name
        self._evaluation_days = evaluation_days
        self._capital_fraction_each = capital_fraction_each
        self._consolidation_callback = consolidation_callback

        # Crear VariantStats para cada variante
        self._variants: dict[str, VariantStats] = {}
        for v in variants:
            name = v.get("name", f"variant_{len(self._variants)}")
            params = v.get("params", {})
            self._variants[name] = VariantStats(name=name, params=params)

        self._winner: str | None = None
        self._started_at: float = time.time()
        self._test_active: bool = bool(self._variants)

        # Intentar cargar estado previo
        self._load_state()

        if self._variants:
            logger.info(
                "A/B test iniciado: strategy=%s variantes=%s evaluation=%dd",
                strategy_name,
                list(self._variants.keys()),
                evaluation_days,
            )

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_outcome(
        self,
        variant_name: str,
        pnl: float,
    ) -> None:
        """Registra el resultado de un trade para una variante.

        Args:
            variant_name: Nombre de la variante que ejecutó el trade.
            pnl: PnL del trade (positivo = win, negativo = loss).
        """
        if variant_name not in self._variants:
            logger.debug("Variante desconocida: %s", variant_name)
            return

        v = self._variants[variant_name]
        if v.is_eliminated:
            return

        v.pnl_total += pnl
        v.trade_count += 1
        if pnl > 0:
            v.wins += 1
        else:
            v.losses += 1

        self._save_state()

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(self) -> str | None:
        """Evalúa si el test ha terminado y consolida al ganador.

        Returns:
            Nombre de la variante ganadora si el test terminó, None si sigue.
        """
        if not self._test_active:
            return self._winner

        elapsed_days = (time.time() - self._started_at) / 86400.0
        if elapsed_days < self._evaluation_days:
            return None

        # Comprobar trades mínimos
        total_trades = sum(v.trade_count for v in self._variants.values())
        if total_trades < 10:
            logger.info(
                "A/B test: sólo %d trades — esperando más data", total_trades
            )
            return None

        # Elegir ganador por PnL total
        active_variants = [
            v for v in self._variants.values()
            if not v.is_eliminated
        ]
        if not active_variants:
            return None

        winner = max(active_variants, key=lambda v: v.pnl_total)
        winner.is_winner = True
        self._winner = winner.name
        self._test_active = False

        # Marcar perdedores
        for v in self._variants.values():
            if v.name != winner.name:
                v.is_eliminated = True

        logger.info(
            "A/B test completado: ganador=%s pnl=%.4f winrate=%.1f%%",
            winner.name, winner.pnl_total, winner.winrate * 100,
        )

        # Callback de consolidación
        if self._consolidation_callback is not None:
            try:
                self._consolidation_callback(winner.params)
            except Exception:
                logger.exception("Error en consolidation_callback")

        self._save_state()
        return self._winner

    # ------------------------------------------------------------------
    # Capital routing
    # ------------------------------------------------------------------

    def get_capital_allocation(self, total_capital: float) -> dict[str, float]:
        """Retorna la asignación de capital por variante.

        Si el test terminó, el ganador recibe todo el capital.

        Args:
            total_capital: Capital total disponible en USDC.

        Returns:
            Dict variant_name → capital_usdc.
        """
        if not self._test_active and self._winner:
            return {self._winner: total_capital}

        active = [v for v in self._variants.values() if not v.is_eliminated]
        if not active:
            return {}

        per_variant = total_capital * self._capital_fraction_each
        return {v.name: per_variant for v in active}

    def get_params_for_variant(self, variant_name: str) -> dict[str, Any]:
        """Retorna los parámetros de una variante específica."""
        v = self._variants.get(variant_name)
        return v.params if v else {}

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def get_report(self) -> dict[str, Any]:
        """Retorna un reporte completo del estado del test."""
        return {
            "strategy_name": self._strategy_name,
            "test_active": self._test_active,
            "winner": self._winner,
            "evaluation_days": self._evaluation_days,
            "elapsed_days": round((time.time() - self._started_at) / 86400.0, 2),
            "variants": {
                name: v.to_dict() for name, v in self._variants.items()
            },
        }

    def format_telegram(self) -> str:
        """Formato Markdown para reporte por Telegram."""
        report = self.get_report()
        lines = [
            f"*A/B Test: {self._strategy_name}*",
            f"Estado: `{'activo' if self._test_active else 'completado'}`",
            f"Días: `{report['elapsed_days']:.1f}` / `{self._evaluation_days}`",
        ]
        if self._winner:
            lines.append(f"🏆 Ganador: `{self._winner}`")

        for name, v in report["variants"].items():
            icon = "🏆" if v["is_winner"] else ("❌" if v["is_eliminated"] else "🔄")
            lines.append(
                f"{icon} `{name}`: PnL=`${v['pnl_total']:.4f}` "
                f"WR=`{v['winrate']:.1%}` trades=`{v['trade_count']}`"
            )
        return "\n".join(lines)

    def _save_state(self) -> None:
        """Persiste el estado del test en disco."""
        try:
            _AB_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            state = self.get_report()
            _AB_STATE_FILE.write_text(
                json.dumps(state, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            logger.exception("Error guardando estado A/B test")

    def _load_state(self) -> None:
        """Carga estado previo del test si existe."""
        if not _AB_STATE_FILE.exists():
            return
        try:
            data = json.loads(_AB_STATE_FILE.read_text(encoding="utf-8"))
            if data.get("strategy_name") != self._strategy_name:
                return  # Test diferente — ignorar

            self._winner = data.get("winner")
            self._test_active = data.get("test_active", True)
            self._started_at = min(
                v.get("started_at", time.time())
                for v in data.get("variants", {}).values()
            ) if data.get("variants") else self._started_at

            for name, vdata in data.get("variants", {}).items():
                if name in self._variants:
                    v = self._variants[name]
                    v.pnl_total = vdata.get("pnl_total", 0.0)
                    v.trade_count = vdata.get("trade_count", 0)
                    v.wins = vdata.get("wins", 0)
                    v.losses = vdata.get("losses", 0)
                    v.started_at = vdata.get("started_at", v.started_at)
                    v.is_winner = vdata.get("is_winner", False)
                    v.is_eliminated = vdata.get("is_eliminated", False)

            logger.info(
                "A/B test: estado previo cargado (ganador=%s, activo=%s)",
                self._winner, self._test_active,
            )
        except Exception:
            logger.exception("Error cargando estado A/B test previo")


class ABTestManager:
    """Gestiona múltiples tests A/B simultáneos (uno por estrategia).

    Se configura desde settings.yaml#ab_tests.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._tests: dict[str, ABTester] = {}
        ab_config = config.get("ab_tests", [])

        for test_cfg in ab_config:
            strategy = test_cfg.get("strategy", "")
            if not strategy:
                continue
            tester = ABTester(
                strategy_name=strategy,
                variants=test_cfg.get("variants", []),
                evaluation_days=test_cfg.get("evaluation_days", 7),
                capital_fraction_each=test_cfg.get("capital_fraction_each", 0.33),
            )
            self._tests[strategy] = tester

        logger.info("ABTestManager iniciado con %d tests", len(self._tests))

    def get_tester(self, strategy_name: str) -> ABTester | None:
        """Retorna el ABTester para una estrategia, o None si no hay test activo."""
        return self._tests.get(strategy_name)

    def record_outcome(self, strategy_name: str, variant_name: str, pnl: float) -> None:
        """Registra resultado en el test de una estrategia."""
        tester = self._tests.get(strategy_name)
        if tester:
            tester.record_outcome(variant_name, pnl)

    def evaluate_all(self) -> list[str]:
        """Evalúa todos los tests y retorna nombres de ganadores que consolidaron."""
        winners = []
        for name, tester in self._tests.items():
            winner = tester.evaluate()
            if winner:
                winners.append(f"{name}:{winner}")
        return winners

    def format_all_telegram(self) -> str:
        """Reporte combinado de todos los tests activos."""
        if not self._tests:
            return "No hay tests A/B activos."
        parts = []
        for tester in self._tests.values():
            parts.append(tester.format_telegram())
        return "\n\n".join(parts)
