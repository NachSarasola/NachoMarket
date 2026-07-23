"""Tests de la logica de FILLS del backtester.

El bot anterior se fundio en parte porque su paper sim reportaba 100% winrate: nunca
modelaba fills adversos. Estos tests verifican explicitamente que el backtester:
  - llena la entrada en la barra SIGUIENTE (sin look-ahead),
  - cobra fees/slippage (un round-trip breakeven bruto da PnL neto negativo),
  - respeta el stop y lo ejecuta ANTES que el target cuando ambos caen en la barra,
  - mueve el stop a breakeven tras TP1 parcial.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from crypto.smc.backtest import run_backtest


def _frame(rows: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    idx = pd.date_range("2021-01-01", periods=len(rows), freq="4h", tz="UTC")
    a = np.array(rows, dtype=float)
    return pd.DataFrame(
        {"open": a[:, 0], "high": a[:, 1], "low": a[:, 2], "close": a[:, 3],
         "volume": np.ones(len(rows))},
        index=idx,
    )


def _empty_signals(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    return pd.DataFrame(
        {
            "enter_long": np.zeros(n, dtype=bool),
            "enter_short": np.zeros(n, dtype=bool),
            "sl_price": np.full(n, np.nan),
            "tp1_price": np.full(n, np.nan),
            "tp2_price": np.full(n, np.nan),
            "entry_price": df["close"].to_numpy(dtype=float),
            "enter_tag": np.array([""] * n, dtype=object),
            "time_stop_bars": np.full(n, 1e9),
        },
        index=df.index,
    )


def test_entry_fills_next_bar_open_no_lookahead() -> None:
    # Senal en barra 0 (close=100), pero el open de la barra 1 es 102 -> fill ~102.
    df = _frame([
        (100, 100, 100, 100),   # 0: senal
        (102, 110, 102, 108),   # 1: fill en open=102
        (108, 112, 100, 100),   # 2
        (100, 101, 90, 95),     # 3
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_long"] = True
    sig.loc[df.index[0], "sl_price"] = 95.0
    sig.loc[df.index[0], "tp1_price"] = 111.0
    sig.loc[df.index[0], "tp2_price"] = 111.0
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0, risk_pct=0.01, initial_equity=1000)
    assert len(res.trades) == 1
    assert np.isclose(res.trades[0].entry_price, 102.0), "entry debe ser el open de la barra 1"


def test_long_stop_loss_is_realized() -> None:
    df = _frame([
        (100, 100, 100, 100),   # 0 senal
        (100, 101, 100, 100),   # 1 fill open=100
        (100, 100, 90, 92),     # 2 low=90 <= stop 95 -> stop
        (92, 95, 90, 93),
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_long"] = True
    sig.loc[df.index[0], "sl_price"] = 95.0
    sig.loc[df.index[0], "tp1_price"] = 130.0
    sig.loc[df.index[0], "tp2_price"] = 130.0
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0, risk_pct=0.02, initial_equity=1000)
    assert len(res.trades) == 1
    t = res.trades[0]
    assert t.exit_reason == "stop"
    assert t.pnl < 0
    # Riesgo = 2% de 1000 = 20 (distancia entry100->stop95 = 5). PnL ~ -20.
    assert np.isclose(t.pnl, -20.0, atol=1e-6)


def test_long_take_profit_is_realized() -> None:
    df = _frame([
        (100, 100, 100, 100),
        (100, 101, 100, 100),   # fill 100
        (100, 130, 100, 128),   # high 130 >= tp2 120 -> tp2
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_long"] = True
    sig.loc[df.index[0], "sl_price"] = 95.0
    sig.loc[df.index[0], "tp1_price"] = 120.0
    sig.loc[df.index[0], "tp2_price"] = 120.0
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0, risk_pct=0.02, initial_equity=1000)
    t = res.trades[0]
    assert t.exit_reason == "tp2"
    assert t.pnl > 0


def test_stop_taken_before_tp_intrabar() -> None:
    # Barra cuyo rango cubre stop (95) y tp (120): conservador -> stop.
    df = _frame([
        (100, 100, 100, 100),
        (100, 101, 100, 100),   # fill 100
        (100, 125, 90, 100),    # abarca stop 95 y tp 120
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_long"] = True
    sig.loc[df.index[0], "sl_price"] = 95.0
    sig.loc[df.index[0], "tp1_price"] = 120.0
    sig.loc[df.index[0], "tp2_price"] = 120.0
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0, risk_pct=0.02, initial_equity=1000)
    assert res.trades[0].exit_reason == "stop"


def test_costs_turn_breakeven_into_loss() -> None:
    # Compra y vende al mismo precio efectivo: con fees/slippage debe perder.
    df = _frame([
        (100, 100, 100, 100),
        (100, 100, 100, 100),   # fill 100
        (100, 120, 100, 100),   # tp a 100 (>= entry) cierra en 100 nominal
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_long"] = True
    sig.loc[df.index[0], "sl_price"] = 95.0
    sig.loc[df.index[0], "tp1_price"] = 100.0
    sig.loc[df.index[0], "tp2_price"] = 100.0
    res = run_backtest(df, sig, fee_bps=10, slippage_bps=5, risk_pct=0.02, initial_equity=1000)
    assert res.trades[0].pnl < 0, "fees + slippage deben hacer negativo un round-trip plano"


def test_partial_tp_moves_stop_to_breakeven() -> None:
    # Tras TP1, el resto no debe cerrar en el stop original si el precio vuelve al entry.
    df = _frame([
        (100, 100, 100, 100),
        (100, 101, 100, 100),   # fill 100
        (100, 112, 100, 111),   # TP1 (110) tocado -> 50% cerrado, stop -> 100 (BE)
        (111, 111, 99, 100),    # vuelve a 100: resto sale ~BE (low 99 toca stop=100)
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_long"] = True
    sig.loc[df.index[0], "sl_price"] = 90.0   # stop original lejano
    sig.loc[df.index[0], "tp1_price"] = 110.0
    sig.loc[df.index[0], "tp2_price"] = 130.0
    res = run_backtest(df, sig, fee_bps=0, slippage_bps=0, risk_pct=0.02, initial_equity=1000)
    t = res.trades[0]
    # Debe haber ganancia (mitad a 110 = +10/unit) pese a que el precio volvio al entry,
    # porque el stop del resto se movio a breakeven (100), no al original (90).
    assert t.pnl > 0
    assert t.exit_reason == "stop"  # el resto salio en el BE-stop


def test_short_direction_disabled_in_long_mode() -> None:
    df = _frame([
        (100, 100, 100, 100),
        (100, 101, 95, 96),
        (96, 97, 80, 82),
    ])
    sig = _empty_signals(df)
    sig.loc[df.index[0], "enter_short"] = True
    sig.loc[df.index[0], "sl_price"] = 105.0
    sig.loc[df.index[0], "tp1_price"] = 85.0
    sig.loc[df.index[0], "tp2_price"] = 85.0
    res = run_backtest(df, sig, direction="long", fee_bps=0, slippage_bps=0)
    assert len(res.trades) == 0, "en modo long-only no debe abrir shorts"


def test_metrics_report_includes_drawdown_not_just_winrate() -> None:
    df = _frame([(100, 100, 100, 100)] + [(100, 101, 99, 100)] * 30)
    sig = _empty_signals(df)
    res = run_backtest(df, sig, initial_equity=500)
    m = res.metrics()
    # Siempre presentes las claves de riesgo (aunque no haya trades).
    for key in ("max_drawdown", "profit_factor", "expectancy_r", "trades"):
        assert key in m
