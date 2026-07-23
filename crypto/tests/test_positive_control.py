"""Control POSITIVO + NEGATIVO: la estrategia gana con edge presente, pierde sin el.

Junto con los tests de fills, esto cierra el circulo: el backtester no solo "no miente"
en casos borde, sino que sobre datos con la microestructura que la estrategia persigue
(barrido de liquidez + reversion) produce PnL positivo neto de costos, y sobre ruido
puro produce PnL negativo. Un detector roto (siempre gana / siempre pierde) fallaria aca.
"""

from __future__ import annotations

from crypto.smc.backtest import run_backtest
from crypto.smc.signals import smc_sweep_signals
from crypto.smc.synthetic import random_walk_ohlcv, sweep_market_ohlcv

BT = dict(fee_bps=10, slippage_bps=5, risk_pct=0.01, initial_equity=500, direction="long")


def _run(df):
    sig = smc_sweep_signals(df)
    return run_backtest(df, sig, **BT).metrics(), int(sig["enter_long"].sum())


def test_positive_control_is_profitable_net_of_costs() -> None:
    m, longs = _run(sweep_market_ohlcv(seed=3))
    assert longs >= 20, "el generador deberia producir muchos sweeps long"
    assert m["trades"] >= 20
    assert m["total_return"] > 0, f"con edge presente debe ganar neto de costos: {m}"
    assert m["profit_factor"] is not None and m["profit_factor"] > 1.0


def test_negative_control_loses_on_random_walk() -> None:
    m, _ = _run(random_walk_ohlcv(n=5000, seed=1))
    assert m["trades"] >= 20
    assert m["total_return"] < 0, f"en random-walk debe perder por costos: {m}"


def test_positive_beats_negative_across_seeds() -> None:
    # Robustez a la semilla: el control positivo bate al negativo consistentemente.
    for seed in (3, 4, 5):
        pos, _ = _run(sweep_market_ohlcv(seed=seed))
        neg, _ = _run(random_walk_ohlcv(n=4000, seed=seed))
        assert pos["total_return"] > neg["total_return"], f"seed {seed}: pos={pos['total_return']} neg={neg['total_return']}"
        assert pos["total_return"] > 0
