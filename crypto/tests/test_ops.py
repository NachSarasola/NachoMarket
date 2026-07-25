"""Tests de la capa operativa: cascadas (H9), carry monitor, presupuesto de riesgo vivo."""

from __future__ import annotations

import importlib.util
import pathlib
from datetime import datetime, timezone

import numpy as np
import pandas as pd

_SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --------------------------- H9: eventos de cascada --------------------------- #

def _bars(n: int = 300, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2023-01-01", periods=n, freq="4h", tz="UTC")
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.004, n)))
    open_ = np.empty(n)
    open_[0] = 100.0
    open_[1:] = close[:-1]
    hi = np.maximum(open_, close) * 1.002
    lo = np.minimum(open_, close) * 0.998
    vol = rng.uniform(0.8, 1.2, n)  # con ruido: un volumen constante da std=0 -> z NaN
    return pd.DataFrame({"open": open_, "high": hi, "low": lo, "close": close,
                         "volume": vol}, index=idx)


def _oi_5m(bars: pd.DataFrame, level: float = 1e6) -> pd.Series:
    start, end = bars.index[0], bars.index[-1] + pd.Timedelta(hours=4)
    idx = pd.date_range(start, end, freq="5min", tz="UTC")
    return pd.Series(level, index=idx)


def test_cascade_event_detected_at_bar_close_causal() -> None:
    mc = _load_script("make_cascade_events")
    bars = _bars()
    k = 150
    bars.iloc[k, bars.columns.get_loc("close")] = bars["open"].iloc[k] * 0.97  # barra roja
    bars.iloc[k, bars.columns.get_loc("low")] = bars["close"].iloc[k] * 0.999
    bars.iloc[k, bars.columns.get_loc("volume")] = 8.0  # climax (z >> 2 con historia=1)
    oi = _oi_5m(bars)
    close_t = bars.index[k] + pd.Timedelta(hours=4)
    oi.loc[oi.index >= close_t - pd.Timedelta(minutes=5)] = 1e6 * 0.95  # purga -5%
    ev = mc.cascade_events(bars, oi, oi_drop_pct=3.0, vol_z_min=2.0)
    assert len(ev) == 1
    assert ev["timestamp"].iloc[0] == close_t.value // 10**6  # evento = CIERRE de la barra
    assert ev["oi_drop_pct"].iloc[0] < -3.0


def test_cascade_no_event_without_volume_climax() -> None:
    mc = _load_script("make_cascade_events")
    bars = _bars()
    k = 150
    bars.iloc[k, bars.columns.get_loc("close")] = bars["open"].iloc[k] * 0.97
    oi = _oi_5m(bars)
    close_t = bars.index[k] + pd.Timedelta(hours=4)
    oi.loc[oi.index >= close_t - pd.Timedelta(minutes=5)] = 1e6 * 0.95
    ev = mc.cascade_events(bars, oi, oi_drop_pct=3.0, vol_z_min=2.0)
    assert len(ev) == 0  # sin climax de volumen no hay evento


def test_oi_alignment_never_uses_future_observation() -> None:
    mc = _load_script("make_cascade_events")
    bars = _bars(50)
    oi = _oi_5m(bars)
    k = 30
    close_t = bars.index[k] + pd.Timedelta(hours=4)
    # La caida ocurre 1 SEGUNDO despues del cierre de la barra k -> pertenece a k+1.
    oi.loc[oi.index > close_t] = 5e5
    aligned = mc.align_oi_to_bar_close(bars, oi)
    assert aligned.iloc[k] == 1e6      # al cierre de k el OI aun no cayo
    assert aligned.iloc[k + 1] == 5e5  # recien se ve en k+1


def test_fetch_metrics_parser_and_month_range() -> None:
    fm = _load_script("fetch_metrics")
    csv_text = ("create_time,symbol,sum_open_interest,sum_open_interest_value\n"
                "2024-01-01 00:05:00,BTCUSDT,81000.5,3400000000\n"
                "mal_formato,BTCUSDT,x,y\n"
                "2024-01-01 00:10:00,BTCUSDT,81100.0,3410000000\n")
    rows = fm.parse_metrics_csv(csv_text)
    assert len(rows) == 2 and rows[0][1] == 81000.5
    assert rows[0][0] == int(datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc).timestamp() * 1000)
    months = fm.month_range("2021-12")
    assert months[0] == "2021-12" and "2022-01" in months and len(months) > 30


# --------------------------- carry monitor --------------------------- #

def test_carry_math_annualize_and_breakeven() -> None:
    cm = _load_script("carry_monitor")
    # 0.01% cada 8h -> 3x/dia -> ~10.95% anual
    assert np.isclose(cm.annualize_funding(0.0001, 8.0), 0.0001 * 3 * 365)
    # breakeven decrece si sube el APR; infinito con APR<=0
    b1 = cm.carry_breakeven_days(1000, 0.10)
    b2 = cm.carry_breakeven_days(1000, 0.30)
    assert b2 < b1 < float("inf")
    assert cm.carry_breakeven_days(1000, 0.0) == float("inf")
    # fees redondos: 500/2 * (2*10bps + 2*5bps) = 250 * 0.003 = $0.75
    assert np.isclose(cm.carry_roundtrip_fees(500), 0.75)


def test_carry_net_apr_costs_dominate_small_capital_short_holding() -> None:
    cm = _load_script("carry_monitor")
    # Mismo APR bruto: neto mejora con mas dias de holding (los fees se amortizan).
    n7 = cm.carry_net_apr(500, 0.20, holding_days=7)
    n90 = cm.carry_net_apr(500, 0.20, holding_days=90)
    assert n90 > n7
    # APR bruto bajo con capital chico y 30d -> neto negativo (los fees comen todo).
    assert cm.carry_net_apr(300, 0.02, holding_days=30) < 0


def test_capital_plan_months() -> None:
    cm = _load_script("carry_monitor")
    # Sin rendimiento ni incentivos: (5000-1000)/500 = 8 meses.
    assert cm.capital_plan(1000, 500, idle_apr=0.0, incentive_monthly=0.0) == 8
    # Incentivos aceleran; rendimiento ocioso tambien.
    assert cm.capital_plan(1000, 500, 0.0, 100.0) < 8
    assert cm.capital_plan(1000, 500, 0.10, 0.0) <= 8


# --------------------------- presupuesto de riesgo vivo --------------------------- #

def _journal(rows: list[dict]) -> pd.DataFrame:
    base = {"date": "2026-07-10", "lane": "incentivos", "venue": "x", "fees_usd": 0.0,
            "pnl_usd": 0.0, "est_value_usd": 0.0, "realized_usd": 0.0, "notes": ""}
    return pd.DataFrame([{**base, **r} for r in rows])


def test_budget_within_limits() -> None:
    br = _load_script("budget_review")
    df = br.load_journal.__wrapped__ if hasattr(br.load_journal, "__wrapped__") else None
    j = _journal([{"fees_usd": 5.0}, {"lane": "hlp", "est_value_usd": 100.0}])
    j["date"] = pd.to_datetime(j["date"], utc=True)
    rep = br.review(j, budget_monthly=30.0, hlp_max=150.0,
                    now=datetime(2026, 7, 15, tzinfo=timezone.utc))
    assert rep["verdict"] == "DENTRO_PRESUPUESTO" and rep["burn_mes"] == 5.0


def test_budget_stop_burn_on_fees_plus_losses() -> None:
    br = _load_script("budget_review")
    j = _journal([{"fees_usd": 20.0}, {"lane": "experimento", "pnl_usd": -15.0}])
    j["date"] = pd.to_datetime(j["date"], utc=True)
    rep = br.review(j, budget_monthly=30.0, hlp_max=150.0,
                    now=datetime(2026, 7, 15, tzinfo=timezone.utc))
    assert rep["verdict"] == "STOP_BURN" and rep["burn_mes"] == 35.0


def test_budget_stop_hlp_on_oversize() -> None:
    br = _load_script("budget_review")
    j = _journal([{"lane": "hlp", "est_value_usd": 200.0}])
    j["date"] = pd.to_datetime(j["date"], utc=True)
    rep = br.review(j, budget_monthly=30.0, hlp_max=150.0,
                    now=datetime(2026, 7, 15, tzinfo=timezone.utc))
    assert rep["verdict"] == "STOP_HLP"


def test_budget_burn_ignores_profits_as_offset() -> None:
    br = _load_script("budget_review")
    # Ganancias NO compensan el techo: burn = fees + perdidas, aunque el pnl neto sea +.
    j = _journal([{"fees_usd": 25.0}, {"lane": "hlp", "pnl_usd": +50.0},
                  {"lane": "experimento", "pnl_usd": -10.0}])
    j["date"] = pd.to_datetime(j["date"], utc=True)
    rep = br.review(j, budget_monthly=30.0, hlp_max=150.0,
                    now=datetime(2026, 7, 15, tzinfo=timezone.utc))
    assert rep["burn_mes"] == 35.0 and rep["verdict"] == "STOP_BURN"
