"""Tests de la sensibilidad a costos."""

from __future__ import annotations

import importlib.util
import pathlib

from crypto.scripts.validate import DEFAULT_PARAMS
from crypto.smc.synthetic import random_walk_ohlcv, sweep_market_ohlcv

_C = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "cost_sensitivity.py"
_spec = importlib.util.spec_from_file_location("cost_sensitivity", _C)
cs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cs)


def test_more_cost_lower_return_same_trades() -> None:
    df = sweep_market_ohlcv(seed=3)
    res = cs.run_cost_grid(df, "sweep", DEFAULT_PARAMS["sweep"], cs.SCENARIOS, 500.0)
    # Los costos no cambian las senales -> mismo nro de trades en todos los escenarios.
    assert len({r["trades"] for r in res}) == 1
    # Mas costo -> retorno no-creciente (monotonia debil).
    returns = [r["total_return"] for r in res]
    assert all(returns[i] >= returns[i + 1] - 1e-9 for i in range(len(returns) - 1)), returns


def test_cliff_detected_on_no_edge() -> None:
    # Sin edge (random-walk) el retorno ya es negativo en el escenario optimista.
    df = random_walk_ohlcv(n=4000, seed=1)
    res = cs.run_cost_grid(df, "sweep", DEFAULT_PARAMS["sweep"], cs.SCENARIOS, 500.0)
    cliff = cs.cost_cliff(res)
    assert cliff is not None
    assert cliff["escenario"] == "optimista"


def test_no_cliff_when_edge_is_strong() -> None:
    # Con edge fuerte y costos hasta pesimistas, el retorno se mantiene positivo.
    df = sweep_market_ohlcv(n_events=340, seed=7)
    res = cs.run_cost_grid(df, "sweep", DEFAULT_PARAMS["sweep"], cs.SCENARIOS, 500.0)
    # Al menos el escenario 'base' debe seguir siendo rentable.
    base = next(r for r in res if r["escenario"] == "base")
    assert base["total_return"] > 0
