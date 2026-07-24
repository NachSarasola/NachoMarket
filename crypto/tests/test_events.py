"""Tests del event-study H7/H8: fills causales, stop conservador, funding, controles.

La tesis invertida: ningún gate usa winrate; se valida expectancy neta + OOS + DSR.
"""

from __future__ import annotations

import importlib.util
import pathlib

import numpy as np
import pandas as pd

from crypto.smc.events import (
    EventSpec,
    classify_event_study,
    event_study,
    simulate_event,
    summarize_event_study,
)
from crypto.smc.stats import bootstrap_mean_pvalue

_SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_prices(n: int = 400, start: str = "2024-01-01", price0: float = 10.0,
                bar_ret: np.ndarray | None = None, seed: int = 0) -> pd.DataFrame:
    """Serie 4h determinista: OHLC coherente alrededor de un close por retornos dados."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="4h", tz="UTC")
    rets = bar_ret if bar_ret is not None else rng.normal(0.0, 0.01, n)
    close = price0 * np.exp(np.cumsum(rets))
    open_ = np.empty(n)
    open_[0] = price0
    open_[1:] = close[:-1]
    hi = np.maximum(open_, close) * 1.004
    lo = np.minimum(open_, close) * 0.996
    return pd.DataFrame({"open": open_, "high": hi, "low": lo, "close": close,
                         "volume": np.ones(n)}, index=idx)


SPEC = EventSpec(direction="short", entry_offset_h=-48, exit_offset_h=24,
                 stop_pct=0.04, fee_bps=6.0, slippage_bps=10.0)


# --------------------------- fills / causalidad --------------------------- #

def test_entry_at_first_bar_open_after_entry_time() -> None:
    df = make_prices(100)
    event = df.index[30] + pd.Timedelta(hours=1)  # evento a mitad de barra
    tr = simulate_event(df, event, SPEC)
    entry_time = event + pd.Timedelta(hours=SPEC.entry_offset_h)
    expected_pos = df.index.searchsorted(entry_time, side="left")
    assert tr["entry_time"] == df.index[expected_pos]
    assert tr["entry_time"] >= entry_time  # la barra abre DESPUÉS de decidir: causal
    assert tr["entry_price"] == df["open"].iloc[expected_pos]


def test_time_exit_at_open_of_first_bar_after_exit_time() -> None:
    df = make_prices(100, bar_ret=np.zeros(100))  # plano: sin stop posible
    event = df.index[50]
    tr = simulate_event(df, event, SPEC)
    assert tr["reason"] == "time"
    exit_time = event + pd.Timedelta(hours=SPEC.exit_offset_h)
    pos = df.index.searchsorted(exit_time, side="left")
    assert tr["exit_time"] == df.index[pos]
    assert tr["exit_price"] == df["open"].iloc[pos]


def test_stop_conservative_short_fills_at_stop_price() -> None:
    rets = np.zeros(100)
    rets[41] = 0.06  # +6% en una barra dentro de la ventana -> cruza el stop de 4%
    df = make_prices(100, bar_ret=rets)
    event = df.index[50]
    tr = simulate_event(df, event, SPEC)
    assert tr["reason"] == "stop"
    assert np.isclose(tr["exit_price"], tr["entry_price"] * (1 + SPEC.stop_pct))
    assert tr["net_ret"] < -SPEC.stop_pct + 1e-9  # pérdida = stop + costos


def test_skips_event_without_data_coverage() -> None:
    df = make_prices(100, start="2024-06-01")
    event = pd.Timestamp("2024-01-01", tz="UTC")  # muy antes del primer dato
    assert simulate_event(df, event, SPEC) is None


def test_data_end_flagged() -> None:
    df = make_prices(20, bar_ret=np.zeros(20))
    event = df.index[-1]  # ventana de salida más allá del fin de los datos
    tr = simulate_event(df, event, SPEC)
    assert tr is not None and tr["reason"] == "data_end"


# --------------------------- funding --------------------------- #

def test_funding_sign_short_pays_negative_receives_positive() -> None:
    df = make_prices(100, bar_ret=np.zeros(100))
    event = df.index[50]
    fund_idx = pd.date_range(df.index[30], periods=40, freq="8h", tz="UTC")
    neg = pd.Series(-0.001, index=fund_idx)
    pos = pd.Series(+0.001, index=fund_idx)
    tr_neg = simulate_event(df, event, SPEC, funding=neg)
    tr_pos = simulate_event(df, event, SPEC, funding=pos)
    assert tr_neg["funding_ret"] < 0 < tr_pos["funding_ret"]  # corto PAGA funding negativo
    assert tr_neg["net_ret"] < tr_pos["net_ret"]


def test_funding_long_mirror() -> None:
    df = make_prices(100, bar_ret=np.zeros(100))
    spec_long = EventSpec(direction="long", entry_offset_h=-48, exit_offset_h=24)
    fund = pd.Series(+0.001, index=pd.date_range(df.index[30], periods=40, freq="8h", tz="UTC"))
    tr = simulate_event(df, df.index[50], spec_long, funding=fund)
    assert tr["funding_ret"] < 0  # el largo PAGA funding positivo


# --------------------------- event_study --------------------------- #

def test_overlapping_events_same_symbol_keep_first() -> None:
    df = make_prices(200, bar_ret=np.zeros(200))
    events = pd.DataFrame({
        "symbol": ["XUSDT", "XUSDT"],
        "timestamp": [df.index[100].value // 10**6, df.index[103].value // 10**6],
    })
    journal, skipped = event_study(events, {"XUSDT": df}, SPEC)
    assert len(journal) == 1 and skipped["solapado"] == 1


def test_event_study_counts_missing_symbols() -> None:
    events = pd.DataFrame({"symbol": ["NOPEUSDT"], "timestamp": [1700000000000]})
    journal, skipped = event_study(events, {}, SPEC)
    assert len(journal) == 0 and skipped["sin_datos"] == 1


# --------------------------- controles positivo/negativo --------------------------- #

def _synthetic_events(n_events: int, drift: float, seed: int) -> tuple[pd.DataFrame, dict]:
    """n eventos en símbolos independientes; drift POR BARRA dentro de la ventana del evento."""
    rng = np.random.default_rng(seed)
    events, prices = [], {}
    t0 = pd.Timestamp("2023-02-01", tz="UTC")
    for k in range(n_events):
        sym = f"S{k}USDT"
        start = t0 + pd.Timedelta(days=10 * k)
        rets = rng.normal(0.0, 0.01, 160)
        event_pos = 80
        # ventana del trade: [T-48h, T+24h] = 18 barras de 4h alrededor del evento
        rets[event_pos - 12: event_pos + 6] += drift
        df = make_prices(160, start=str(start.date()), bar_ret=rets, seed=k)
        prices[sym] = df
        events.append({"symbol": sym, "timestamp": df.index[event_pos].value // 10**6,
                       "pct_supply": 3.0})
    return pd.DataFrame(events), prices


def test_positive_control_unlock_drift_validates() -> None:
    # 90 eventos con drift -0.5%/barra en la ventana (≈ -9% por evento): el short lo captura.
    # 60 caen en IS (<=2024-12-31) y 30 en OOS -> pasa gates con n_trials chico.
    events, prices = _synthetic_events(90, drift=-0.005, seed=1)
    journal, skipped = event_study(events, prices, SPEC)
    assert len(journal) >= 85
    summary = summarize_event_study(journal, skipped, SPEC, "2024-12-31", n_trials=5)
    cls = classify_event_study(summary)
    assert summary["in_sample"]["expectancy_net"] > 0.02
    assert cls["verdict"] == "EDGE_EVENTO_VALIDADO", (cls, summary["in_sample"])


def test_negative_control_random_walk_fails() -> None:
    events, prices = _synthetic_events(90, drift=0.0, seed=2)
    journal, skipped = event_study(events, prices, SPEC)
    summary = summarize_event_study(journal, skipped, SPEC, "2024-12-31", n_trials=5)
    cls = classify_event_study(summary)
    assert cls["verdict"] != "EDGE_EVENTO_VALIDADO", summary["in_sample"]


def test_costs_and_funding_can_kill_marginal_edge() -> None:
    # Drift chico (-0.05%/barra ≈ -0.9%/evento bruto) + funding muy negativo: el neto muere.
    events, prices = _synthetic_events(40, drift=-0.0005, seed=3)
    funding = {}
    for sym, df in prices.items():
        fidx = pd.date_range(df.index[0], periods=50, freq="8h", tz="UTC")
        funding[sym] = pd.Series(-0.004, index=fidx)  # -0.4% por 8h: capitulación extrema
    journal, _ = event_study(events, prices, SPEC, funding_by_symbol=funding)
    assert journal["funding_ret"].mean() < 0
    assert journal["net_ret"].mean() < journal["gross_ret"].mean() - 0.003


# --------------------------- clasificación (gates puros) --------------------------- #

def _summary(n_is=80, n_oos=25, exp_is=0.02, exp_oos=0.015, p=0.001, dsr=0.99,
             ruin=0.0, data_end=0.0) -> dict:
    return {
        "n_total": n_is + n_oos, "n_trials": 130,
        "in_sample": {"n": n_is, "expectancy_net": exp_is, "p_mean_leq_0": p,
                      "dsr": {"dsr": dsr}},
        "oos": {"n": n_oos, "expectancy_net": exp_oos},
        "monte_carlo_ruin": {"prob_ruin": ruin},
        "data_end_frac": data_end,
    }


def test_classify_golden_passes() -> None:
    assert classify_event_study(_summary())["verdict"] == "EDGE_EVENTO_VALIDADO"


def test_classify_oos_collapse_is_hard() -> None:
    cls = classify_event_study(_summary(exp_oos=0.001))
    assert cls["verdict"] == "NO_OPERAR" and any("OOS/IS" in h for h in cls["hard_fail"])


def test_classify_small_sample_insufficient_not_fail() -> None:
    cls = classify_event_study(_summary(n_is=30, n_oos=5))
    assert cls["verdict"] == "MUESTRA_INSUFICIENTE" and not cls["hard_fail"]


def test_classify_low_dsr_is_hard() -> None:
    assert classify_event_study(_summary(dsr=0.5))["verdict"] == "NO_OPERAR"


def test_winrate_never_gates() -> None:
    # Winrate 10% con expectancy positiva -> PASA (la tesis invertida en un test).
    s = _summary()
    s["in_sample"]["win_rate"] = 0.10
    assert classify_event_study(s)["verdict"] == "EDGE_EVENTO_VALIDADO"


# --------------------------- stats.bootstrap --------------------------- #

def test_bootstrap_pvalue_directions() -> None:
    rng = np.random.default_rng(0)
    strong = rng.normal(0.02, 0.01, 80)
    noise = rng.normal(0.0, 0.02, 80)
    assert bootstrap_mean_pvalue(strong)["p_value"] < 0.01
    assert bootstrap_mean_pvalue(noise)["p_value"] > 0.05


# --------------------------- parsers de los fetchers --------------------------- #

def test_parse_emission_plain_and_body_encoded() -> None:
    fu = _load_script("fetch_unlocks")
    payload = {
        "metadata": {"events": [
            {"timestamp": 1720000000, "noOfTokens": [1_000_000], "unlockType": "cliff"},
            {"timestamp": 1720100000, "noOfTokens": 500_000, "unlockType": "linear"},
        ], "maxSupply": 10_000_000, "symbol": "abc"},
    }
    evs = fu.parse_emission(payload, "abc-protocol")
    assert len(evs) == 1  # solo el cliff
    assert evs[0]["symbol"] == "ABC" and np.isclose(evs[0]["pct_supply"], 10.0)
    assert evs[0]["timestamp"] == 1720000000000  # s -> ms

    import json as _json
    wrapped = {"body": _json.dumps(payload)}
    assert len(fu.parse_emission(wrapped, "abc-protocol")) == 1


def test_parse_emission_without_supply_gives_nan_pct() -> None:
    fu = _load_script("fetch_unlocks")
    payload = {"events": [{"timestamp": 1720000000, "noOfTokens": 5, "category": "cliff"}],
               "symbol": "x"}
    evs = fu.parse_emission(payload, "x")
    assert evs[0]["pct_supply"] is None and evs[0]["pct_basis"] == "desconocido"


def test_parse_emission_dedups_repeated_event_lists() -> None:
    # El pageProps del sitio repite la misma lista de eventos en varios props.
    fu = _load_script("fetch_unlocks")
    ev = {"timestamp": 1720000000, "noOfTokens": 100.0, "unlockType": "cliff"}
    payload = {"pageProps": {"a": {"events": [ev], "maxSupply": 1000, "symbol": "zz"},
                             "b": {"chartData": {"events": [dict(ev)]}}}}
    evs = fu.parse_emission(payload, "zz")
    assert len(evs) == 1 and np.isclose(evs[0]["pct_supply"], 10.0)


def test_extract_next_data_and_build_id() -> None:
    fu = _load_script("fetch_unlocks")
    html = ('<html><script id="__NEXT_DATA__" type="application/json">'
            '{"buildId":"abc123","props":{"pageProps":{"x":1}}}</script></html>')
    nd = fu.extract_next_data(html)
    assert nd.get("buildId") == "abc123"
    assert fu.extract_next_data("<html>nope</html>") == {}


def test_slugify_defillama_convention() -> None:
    fu = _load_script("fetch_unlocks")
    assert fu.slugify("Jupiter") == "jupiter"
    assert fu.slugify("Curve DAO") == "curve-dao"
    assert fu.slugify("Ether.fi") == "ether-fi"


def test_extract_index_protocols_from_next_data() -> None:
    fu = _load_script("fetch_unlocks")
    nd = {"buildId": "b", "props": {"pageProps": {"protocols": [
        {"name": "Aptos", "tSymbol": "APT", "maxSupply": 1},
        {"name": "Arbitrum", "nextEvent": {"timestamp": 1}},
        {"name": "Aptos", "tSymbol": "APT"},          # duplicado -> una sola vez
        {"name": "loose-string-sin-campos-token"},    # no parece protocolo
    ]}}}
    assert fu.extract_index_protocols(nd) == ["Aptos", "Arbitrum"]


def test_perp_symbols_from_info() -> None:
    fu = _load_script("fetch_unlocks")
    info = {"symbols": [
        {"symbol": "ABCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
        {"symbol": "ABCUSDT_240628", "contractType": "CURRENT_QUARTER", "quoteAsset": "USDT"},
        {"symbol": "ABCUSDC", "contractType": "PERPETUAL", "quoteAsset": "USDC"},
    ]}
    assert fu.perp_symbols_from_info(info) == {"ABCUSDT"}


def test_listings_filters_leveraged_and_stables_keep_jup() -> None:
    fl = _load_script("fetch_listings")
    info = {"symbols": [
        {"symbol": "JUPUSDT", "baseAsset": "JUP", "quoteAsset": "USDT", "status": "TRADING",
         "isSpotTradingAllowed": True},
        {"symbol": "BTCUPUSDT", "baseAsset": "BTCUP", "quoteAsset": "USDT", "status": "TRADING",
         "isSpotTradingAllowed": True},
        {"symbol": "USDCUSDT", "baseAsset": "USDC", "quoteAsset": "USDT", "status": "TRADING",
         "isSpotTradingAllowed": True},
        {"symbol": "XYZUSDT", "baseAsset": "XYZ", "quoteAsset": "USDT", "status": "BREAK",
         "isSpotTradingAllowed": True},
        {"symbol": "ABCBTC", "baseAsset": "ABC", "quoteAsset": "BTC", "status": "TRADING",
         "isSpotTradingAllowed": True},
    ]}
    got = {s["base"] for s in fl.filter_spot_symbols(info)}
    assert got == {"JUP"}


def test_perp_onboard_map() -> None:
    fl = _load_script("fetch_listings")
    info = {"symbols": [
        {"symbol": "AUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT",
         "onboardDate": 1700000000000},
        {"symbol": "BUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
    ]}
    m = fl.perp_onboard_map(info)
    assert m["AUSDT"] == 1700000000000 and m["BUSDT"] == 0
