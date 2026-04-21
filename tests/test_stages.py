"""Tests para src/strategy/stages.py — Stage Machine."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.strategy.stages import (
    Stage,
    STAGE_SIZE_MULTIPLIER,
    StageMachine,
    StrategyStageState,
    _PROMOTE_THRESHOLD,
    _REVIEW_WINDOW,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "stages.json"


@pytest.fixture
def sm(state_file: Path) -> StageMachine:
    return StageMachine(
        ["market_maker", "multi_arb", "stat_arb"],
        state_file=state_file,
    )


# ------------------------------------------------------------------
# Tests: Stage enum y multiplicadores
# ------------------------------------------------------------------

class TestStageEnum:
    def test_multipliers_completos(self) -> None:
        assert STAGE_SIZE_MULTIPLIER[Stage.SHADOW] == 0.0
        assert STAGE_SIZE_MULTIPLIER[Stage.PAPER] == 0.0
        assert STAGE_SIZE_MULTIPLIER[Stage.LIVE_SMALL] == pytest.approx(0.25)
        assert STAGE_SIZE_MULTIPLIER[Stage.LIVE_FULL] == pytest.approx(1.0)

    def test_valor_string(self) -> None:
        assert Stage.PAPER.value == "PAPER"
        assert Stage("LIVE_SMALL") == Stage.LIVE_SMALL


# ------------------------------------------------------------------
# Tests: StrategyStageState
# ------------------------------------------------------------------

class TestStrategyStageState:
    def test_next_stage_desde_paper(self) -> None:
        s = StrategyStageState("mm", Stage.PAPER)
        assert s.next_stage() == Stage.LIVE_SMALL

    def test_next_stage_desde_live_full_es_none(self) -> None:
        s = StrategyStageState("mm", Stage.LIVE_FULL)
        assert s.next_stage() is None

    def test_prev_stage_desde_live_small(self) -> None:
        s = StrategyStageState("mm", Stage.LIVE_SMALL)
        assert s.prev_stage() == Stage.PAPER

    def test_prev_stage_desde_shadow_es_none(self) -> None:
        s = StrategyStageState("mm", Stage.SHADOW)
        assert s.prev_stage() is None

    def test_should_auto_promote_requiere_ventana_llena(self) -> None:
        s = StrategyStageState("mm", Stage.PAPER)
        # Menos de _REVIEW_WINDOW → no promover aunque todos sean positivos
        for _ in range(_REVIEW_WINDOW - 1):
            s.record_review(True)
        assert s.should_auto_promote() is False

    def test_should_auto_promote_con_suficientes_positivos(self) -> None:
        s = StrategyStageState("mm", Stage.PAPER)
        for i in range(_REVIEW_WINDOW):
            s.record_review(i < _PROMOTE_THRESHOLD)
        assert s.should_auto_promote() is True

    def test_should_not_auto_promote_con_pocos_positivos(self) -> None:
        s = StrategyStageState("mm", Stage.PAPER)
        for i in range(_REVIEW_WINDOW):
            s.record_review(i < (_PROMOTE_THRESHOLD - 1))  # 1 menos del umbral
        assert s.should_auto_promote() is False

    def test_serializacion_roundtrip(self) -> None:
        s = StrategyStageState("mm", Stage.LIVE_SMALL)
        for b in [True, False, True]:
            s.record_review(b)
        s.promoted_at = 1_700_000_000.0

        d = s.to_dict()
        s2 = StrategyStageState.from_dict(d)

        assert s2.name == "mm"
        assert s2.stage == Stage.LIVE_SMALL
        assert list(s2.review_history) == [True, False, True]
        assert s2.promoted_at == pytest.approx(1_700_000_000.0)


# ------------------------------------------------------------------
# Tests: StageMachine
# ------------------------------------------------------------------

class TestStageMachine:
    def test_inicializa_estrategias_en_paper(self, sm: StageMachine) -> None:
        for name in ["market_maker", "multi_arb", "stat_arb"]:
            assert sm.get_stage(name) == Stage.PAPER

    def test_multiplier_paper_es_cero(self, sm: StageMachine) -> None:
        assert sm.get_size_multiplier("market_maker") == pytest.approx(0.0)

    def test_promote_manual_sube_stage(self, sm: StageMachine) -> None:
        ok = sm.promote("market_maker")
        assert ok is True
        assert sm.get_stage("market_maker") == Stage.LIVE_SMALL

    def test_demote_manual_baja_stage(self, sm: StageMachine) -> None:
        sm.promote("market_maker")
        ok = sm.demote("market_maker")
        assert ok is True
        assert sm.get_stage("market_maker") == Stage.PAPER

    def test_promote_desde_live_full_retorna_false(
        self, sm: StageMachine
    ) -> None:
        sm.promote("market_maker")  # PAPER → LIVE_SMALL
        sm.promote("market_maker")  # LIVE_SMALL → LIVE_FULL
        ok = sm.promote("market_maker")  # LIVE_FULL → no hay siguiente
        assert ok is False

    def test_demote_desde_shadow_retorna_false(
        self, sm: StageMachine
    ) -> None:
        # Demotear desde PAPER → SHADOW, luego intentar de nuevo
        sm.demote("market_maker")  # PAPER → SHADOW
        ok = sm.demote("market_maker")
        assert ok is False

    def test_auto_promote_tras_reviews_suficientes(
        self, state_file: Path
    ) -> None:
        sm = StageMachine(["mm"], state_file=state_file)
        # Enviar _REVIEW_WINDOW reviews, _PROMOTE_THRESHOLD positivos
        promoted = False
        for i in range(_REVIEW_WINDOW):
            result = sm.record_review("mm", passed=(i < _PROMOTE_THRESHOLD))
            promoted = promoted or result

        assert promoted is True
        assert sm.get_stage("mm") == Stage.LIVE_SMALL

    def test_no_auto_promote_con_pocos_positivos(
        self, state_file: Path
    ) -> None:
        sm = StageMachine(["mm"], state_file=state_file)
        for i in range(_REVIEW_WINDOW):
            sm.record_review("mm", passed=(i < _PROMOTE_THRESHOLD - 1))

        assert sm.get_stage("mm") == Stage.PAPER

    def test_alert_callback_llamado_en_promote(
        self, state_file: Path
    ) -> None:
        callback = MagicMock()
        sm = StageMachine(["mm"], state_file=state_file, alert_callback=callback)
        sm.promote("mm")
        callback.assert_called_once()
        assert "mm" in callback.call_args[0][0]

    def test_persistencia_en_disco(self, state_file: Path) -> None:
        sm = StageMachine(["mm"], state_file=state_file)
        sm.promote("mm")

        # Recargar
        sm2 = StageMachine(["mm"], state_file=state_file)
        assert sm2.get_stage("mm") == Stage.LIVE_SMALL

    def test_is_live_paper_retorna_false(self, sm: StageMachine) -> None:
        assert sm.is_live("market_maker") is False

    def test_is_live_live_small_retorna_true(self, sm: StageMachine) -> None:
        sm.promote("market_maker")
        assert sm.is_live("market_maker") is True

    def test_get_all_stages(self, sm: StageMachine) -> None:
        stages = sm.get_all_stages()
        assert "market_maker" in stages
        assert stages["market_maker"] == "PAPER"

    def test_get_stats_contiene_info_completa(self, sm: StageMachine) -> None:
        stats = sm.get_stats()
        assert "market_maker" in stats
        info = stats["market_maker"]
        assert "stage" in info
        assert "multiplier" in info
        assert "reviews_to_promote" in info

    def test_review_limpia_historial_tras_promote_manual(
        self, state_file: Path
    ) -> None:
        sm = StageMachine(["mm"], state_file=state_file)
        sm.record_review("mm", passed=True)
        sm.promote("mm")
        # Tras promote manual, el historial debe estar limpio
        state = sm._strategies["mm"]
        assert len(state.review_history) == 0

    def test_estrategia_nueva_se_crea_on_demand(
        self, sm: StageMachine
    ) -> None:
        stage = sm.get_stage("nueva_estrategia")
        assert stage == Stage.PAPER
