"""Tests del veredicto mecanico (decide.py) — determinista y sin correr validate."""

from __future__ import annotations

import importlib.util
import pathlib

# decide.py es un script (no paquete); lo cargamos por ruta.
_DECIDE = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "decide.py"
_spec = importlib.util.spec_from_file_location("decide", _DECIDE)
decide = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(decide)


def _report(strategy="sweep", source="BTC", **over) -> dict:
    """Reporte 'dorado' que PASA todos los gates; overrides para romper uno puntual."""
    rep = {
        "strategy": strategy,
        "data": {"source": source},
        "in_sample": {"trades": 200, "sharpe": 2.0, "total_return": 0.5,
                      "profit_factor": 1.8, "max_drawdown": -0.1},
        "walk_forward": [
            {"fold": 0, "sharpe": 2.1}, {"fold": 1, "sharpe": 1.8},
            {"fold": 2, "sharpe": 2.0}, {"fold": 3, "sharpe": 1.5},
        ],
        "gate5_verdict": "consistente",
        "gate5_oos_is_ratio": 1.2,
        "oos": {"sharpe": 3.0, "total_return": 0.4},
        "oos_buy_hold": {"total_return": 0.05},
        "deflated_sharpe": {"dsr": 0.99},
        "benchmarks_in_sample": {"buy_hold": {"sharpe": 0.2}, "ma_cross": {"sharpe": 0.1}},
        "monte_carlo_ruin": {"prob_ruin": 0.0},
    }
    for k, v in over.items():
        rep[k] = v
    return rep


def test_golden_report_passes() -> None:
    c = decide.classify_report(_report())
    assert c["classification"] == "PASS", c["hard_fail"] + c["soft_fail"]


def test_overfit_oos_is_hard_fail() -> None:
    c = decide.classify_report(_report(gate5_oos_is_ratio=0.2))
    assert c["classification"] == "FAIL"
    assert any("OOS/IS" in r for r in c["hard_fail"])


def test_low_dsr_is_hard_fail() -> None:
    c = decide.classify_report(_report(deflated_sharpe={"dsr": 0.4}))
    assert c["classification"] == "FAIL"


def test_not_beating_benchmarks_is_hard_fail() -> None:
    c = decide.classify_report(_report(benchmarks_in_sample={
        "buy_hold": {"sharpe": 3.0}, "ma_cross": {"sharpe": 3.0}}))
    assert c["classification"] == "FAIL"


def test_few_trades_is_soft_adjust() -> None:
    c = decide.classify_report(_report(in_sample={"trades": 40, "sharpe": 2.0,
                                                  "total_return": 0.5}))
    assert c["classification"] == "ADJUST"
    assert any("trades" in r for r in c["soft_fail"])


def test_in_sample_not_profitable_is_hard_fail() -> None:
    c = decide.classify_report(_report(gate5_verdict="in_sample_not_profitable"))
    assert c["classification"] == "FAIL"


# --- combine() ---

def test_combine_go_dry_run() -> None:
    res = decide.combine([decide.classify_report(_report(strategy="sweep"))])
    assert res["overall"] == "GO_DRY_RUN"


def test_combine_donchian_beats_sweep() -> None:
    # Sweep pasa pero el donchian pasa con mejor OOS -> quedarse con donchian.
    sweep = decide.classify_report(_report(strategy="sweep", oos={"sharpe": 1.0, "total_return": 0.3}))
    donchian = decide.classify_report(_report(strategy="donchian", oos={"sharpe": 2.5, "total_return": 0.5}))
    res = decide.combine([sweep, donchian])
    assert res["overall"] == "DESCARTAR_SWEEP_QUEDA_DONCHIAN"


def test_combine_no_operar() -> None:
    sweep = decide.classify_report(_report(strategy="sweep", gate5_oos_is_ratio=0.1))
    res = decide.combine([sweep])
    assert res["overall"] == "NO_OPERAR"


def test_combine_adjust_when_only_soft() -> None:
    sweep = decide.classify_report(_report(strategy="sweep",
                                           in_sample={"trades": 30, "sharpe": 2.0, "total_return": 0.5}))
    res = decide.combine([sweep])
    assert res["overall"] == "AJUSTE_UNICO"
