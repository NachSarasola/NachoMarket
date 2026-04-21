"""Stage Machine para promoción gradual de estrategias (Fase 2).

Implementa un ciclo de vida por etapas: SHADOW → PAPER → LIVE_SMALL → LIVE_FULL.

Patrones GoF usados:
- State: cada Stage encapsula el comportamiento de esa etapa (multiplicador, transiciones)
- Observer: StageMachine notifica cambios via alert_callback (desacoplado del bot)

Uso:
    sm = StageMachine(["market_maker", "multi_arb", "stat_arb"])
    sm.record_review("market_maker", passed=True)   # auto-promoverá tras 7/10
    sm.promote("market_maker")                      # override manual (Telegram)
    multiplier = sm.get_size_multiplier("market_maker")  # 0.0, 0.25 ó 1.0
"""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger("nachomarket.stages")

_STAGES_FILE = Path("data/stages.json")
_PROMOTE_THRESHOLD = 7       # reviews positivos necesarios en la ventana
_REVIEW_WINDOW = 10          # últimos N reviews que se consideran


class Stage(str, Enum):
    """Estados de despliegue de una estrategia.

    El valor str permite serialización directa a JSON y comparación con strings.
    Orden ascendente: SHADOW < PAPER < LIVE_SMALL < LIVE_FULL.
    """

    SHADOW = "SHADOW"          # Evalúa señales, NO opera ni simula
    PAPER = "PAPER"            # Simula trades (paper mode), sin capital real
    LIVE_SMALL = "LIVE_SMALL"  # 25% del capital asignado por el allocator
    LIVE_FULL = "LIVE_FULL"    # 100% del capital asignado por el allocator


# Multiplicador de capital que cada stage aplica al allocation del Bandit
STAGE_SIZE_MULTIPLIER: dict[Stage, float] = {
    Stage.SHADOW: 0.0,
    Stage.PAPER: 0.0,
    Stage.LIVE_SMALL: 0.25,
    Stage.LIVE_FULL: 1.0,
}

# Transiciones válidas (State Pattern): qué stages puede alcanzar cada estado
_TRANSITIONS: dict[Stage, frozenset[Stage]] = {
    Stage.SHADOW: frozenset({Stage.PAPER}),
    Stage.PAPER: frozenset({Stage.LIVE_SMALL, Stage.SHADOW}),
    Stage.LIVE_SMALL: frozenset({Stage.LIVE_FULL, Stage.PAPER}),
    Stage.LIVE_FULL: frozenset({Stage.LIVE_SMALL}),
}

# Orden total del Enum para determinar "avance" vs "retroceso"
_STAGE_ORDER = [Stage.SHADOW, Stage.PAPER, Stage.LIVE_SMALL, Stage.LIVE_FULL]


class StrategyStageState:
    """Value object que representa el estado de una estrategia en la máquina.

    Encapsula stage actual + historial de reviews para cálculo de promoción.
    Mutable internamente pero con interfaz controlada por StageMachine.
    """

    def __init__(self, name: str, stage: Stage = Stage.PAPER) -> None:
        self.name = name
        self.stage = stage
        self.review_history: deque[bool] = deque(maxlen=_REVIEW_WINDOW)
        self.promoted_at: float = time.time()
        self.demoted_at: float | None = None
        self.total_reviews: int = 0

    # ------------------------------------------------------------------
    # Lógica de promoción automática
    # ------------------------------------------------------------------

    def record_review(self, passed: bool) -> None:
        self.review_history.append(passed)
        self.total_reviews += 1

    def should_auto_promote(self) -> bool:
        """True si hay suficientes reviews positivos para auto-promover."""
        if len(self.review_history) < _REVIEW_WINDOW:
            return False
        positive = sum(1 for r in self.review_history if r)
        return positive >= _PROMOTE_THRESHOLD

    def next_stage(self) -> Stage | None:
        """Próximo stage válido en dirección ascendente (promote)."""
        allowed = _TRANSITIONS.get(self.stage, frozenset())
        current_idx = _STAGE_ORDER.index(self.stage)
        for candidate in _STAGE_ORDER[current_idx + 1:]:
            if candidate in allowed:
                return candidate
        return None

    def prev_stage(self) -> Stage | None:
        """Stage previo válido en dirección descendente (demote)."""
        allowed = _TRANSITIONS.get(self.stage, frozenset())
        current_idx = _STAGE_ORDER.index(self.stage)
        for candidate in reversed(_STAGE_ORDER[:current_idx]):
            if candidate in allowed:
                return candidate
        return None

    # ------------------------------------------------------------------
    # Serialización
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "stage": self.stage.value,
            "review_history": list(self.review_history),
            "promoted_at": self.promoted_at,
            "demoted_at": self.demoted_at,
            "total_reviews": self.total_reviews,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyStageState":
        obj = cls(data["name"], Stage(data.get("stage", Stage.PAPER.value)))
        history = data.get("review_history", [])
        obj.review_history = deque(history, maxlen=_REVIEW_WINDOW)
        obj.promoted_at = data.get("promoted_at", time.time())
        obj.demoted_at = data.get("demoted_at")
        obj.total_reviews = data.get("total_reviews", len(history))
        return obj


class StageMachine:
    """Gestiona el ciclo de vida de múltiples estrategias.

    Combina:
    - State Pattern: cada estrategia tiene su StrategyStageState con transiciones válidas
    - Observer Pattern: notifica cambios via alert_callback (desacoplado de Telegram)

    Uso:
        sm = StageMachine(["market_maker", "multi_arb"])
        sm.record_review("market_maker", passed=True)
        ok = sm.promote("market_maker")   # True si la transición fue válida
        mult = sm.get_size_multiplier("market_maker")  # 0.0 | 0.25 | 1.0
    """

    def __init__(
        self,
        strategy_names: list[str],
        state_file: Path = _STAGES_FILE,
        alert_callback: Callable[[str], Any] | None = None,
    ) -> None:
        self._state_file = state_file
        self._alert_callback = alert_callback
        self._strategies: dict[str, StrategyStageState] = {}
        self._load()
        for name in strategy_names:
            if name not in self._strategies:
                self._strategies[name] = StrategyStageState(name, Stage.PAPER)
        self._save()

    # ------------------------------------------------------------------
    # API pública — reviews y transiciones
    # ------------------------------------------------------------------

    def record_review(self, strategy_name: str, passed: bool) -> bool:
        """Registra un review y auto-promueve si se alcanza el umbral.

        Returns:
            True si se produjo una auto-promoción.
        """
        state = self._get_or_create(strategy_name)
        state.record_review(passed)

        auto_promoted = False
        if state.should_auto_promote():
            next_stage = state.next_stage()
            if next_stage is not None:
                old = state.stage
                self._transition(state, next_stage)
                logger.info(
                    "Auto-promoted '%s': %s → %s (reviews: %d/%d positive)",
                    strategy_name, old.value, next_stage.value,
                    _PROMOTE_THRESHOLD, _REVIEW_WINDOW,
                )
                self._notify(
                    f"📈 *{strategy_name}* auto-promovida: "
                    f"`{old.value}` → `{next_stage.value}`\n"
                    f"_{_PROMOTE_THRESHOLD}/{_REVIEW_WINDOW} reviews positivos_"
                )
                auto_promoted = True

        self._save()
        return auto_promoted

    def promote(self, strategy_name: str) -> bool:
        """Promoción manual (override via Telegram /promote).

        Returns:
            True si la transición fue válida y se ejecutó.
        """
        state = self._get_or_create(strategy_name)
        next_stage = state.next_stage()
        if next_stage is None:
            logger.info(
                "No se puede promover '%s': ya está en %s o no hay transición válida",
                strategy_name, state.stage.value,
            )
            return False

        old = state.stage
        self._transition(state, next_stage)
        self._save()
        logger.info("Promote manual '%s': %s → %s", strategy_name, old.value, next_stage.value)
        self._notify(
            f"⬆️ *{strategy_name}* promovida manualmente: "
            f"`{old.value}` → `{next_stage.value}`"
        )
        return True

    def demote(self, strategy_name: str) -> bool:
        """Demote manual (override via Telegram /demote).

        Returns:
            True si la transición fue válida y se ejecutó.
        """
        state = self._get_or_create(strategy_name)
        prev_stage = state.prev_stage()
        if prev_stage is None:
            logger.info(
                "No se puede demotear '%s': ya está en %s o no hay transición válida",
                strategy_name, state.stage.value,
            )
            return False

        old = state.stage
        state.stage = prev_stage
        state.demoted_at = time.time()
        state.review_history.clear()
        self._save()
        logger.info("Demote manual '%s': %s → %s", strategy_name, old.value, prev_stage.value)
        self._notify(
            f"⬇️ *{strategy_name}* demoteada: "
            f"`{old.value}` → `{prev_stage.value}`"
        )
        return True

    # ------------------------------------------------------------------
    # API pública — consultas de estado
    # ------------------------------------------------------------------

    def get_stage(self, strategy_name: str) -> Stage:
        return self._get_or_create(strategy_name).stage

    def get_size_multiplier(self, strategy_name: str) -> float:
        """Retorna el multiplicador de capital [0.0, 0.25, 1.0] para la estrategia."""
        return STAGE_SIZE_MULTIPLIER[self.get_stage(strategy_name)]

    def is_live(self, strategy_name: str) -> bool:
        """True si la estrategia opera con capital real (LIVE_SMALL o LIVE_FULL)."""
        return self.get_stage(strategy_name) in (Stage.LIVE_SMALL, Stage.LIVE_FULL)

    def get_all_stages(self) -> dict[str, str]:
        return {name: s.stage.value for name, s in self._strategies.items()}

    def get_stats(self) -> dict[str, Any]:
        """Retorna estadísticas completas para /status de Telegram."""
        return {
            name: {
                "stage": s.stage.value,
                "multiplier": STAGE_SIZE_MULTIPLIER[s.stage],
                "total_reviews": s.total_reviews,
                "recent_positive": sum(1 for r in s.review_history if r),
                "review_window": len(s.review_history),
                "next_stage": (s.next_stage() or Stage.LIVE_FULL).value,
                "reviews_to_promote": max(0, _PROMOTE_THRESHOLD - sum(1 for r in s.review_history if r)),
            }
            for name, s in self._strategies.items()
        }

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _get_or_create(self, strategy_name: str) -> StrategyStageState:
        if strategy_name not in self._strategies:
            self._strategies[strategy_name] = StrategyStageState(strategy_name, Stage.PAPER)
            self._save()
        return self._strategies[strategy_name]

    def _transition(self, state: StrategyStageState, target: Stage) -> None:
        """Ejecuta la transición de stage y limpia el historial de reviews."""
        state.stage = target
        state.promoted_at = time.time()
        state.review_history.clear()

    def _notify(self, message: str) -> None:
        """Observer notification — desacoplado del módulo de Telegram."""
        if self._alert_callback:
            try:
                self._alert_callback(message)
            except Exception:
                logger.exception("Error en alert_callback del StageMachine")

    def _load(self) -> None:
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
            self._strategies = {
                name: StrategyStageState.from_dict(s_data)
                for name, s_data in data.items()
            }
        except Exception:
            logger.exception("Error cargando stages.json")

    def _save(self) -> None:
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            self._state_file.write_text(
                json.dumps(
                    {name: s.to_dict() for name, s in self._strategies.items()},
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            logger.exception("Error guardando stages.json")
