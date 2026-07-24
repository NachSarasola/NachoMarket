"""Event studies (H7 unlocks / H8 listings) — simulador honesto POR EVENTO.

La tesis invertida del programa: no importa el winrate, importa el PnL neto. Un evento es
(symbol, timestamp, meta) con contraparte FORZADA (vesting programado, distribución
post-listing). El trade v1 es corto: entrada al OPEN de la primera barra >= entry_time,
salida por stop (conservador intrabarra) o por tiempo al OPEN de la primera barra >=
exit_time.

Causalidad:
- Unlocks: el calendario de vesting es público con semanas/meses de anticipación → entrar
  ANTES del evento (offset negativo) es causal. El % de supply usa max_supply (tokenomics
  estática) → sin lookahead.
- Listings: la entrada es POSTERIOR al evento (offset positivo obligatorio — no se puede
  conocer un listing antes de que exista la primera vela).

Costos: fee + slippage por lado + funding ACUMULADO del período. Convención de funding de
perps: si el funding es positivo los longs pagan a los shorts (el corto COBRA); si es
negativo el corto PAGA — el asesino documentado del short de unlocks, por eso se modela
SIEMPRE que haya serie de funding.

Sesgos conocidos (pre-registrados, ambos CONSERVADORES para la tesis corta): los universos
de DeFiLlama/exchangeInfo solo contienen sobrevivientes — los tokens delistados (los mejores
shorts) no aparecen → el efecto real sería >= al medido.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from crypto.smc.stats import bootstrap_mean_pvalue, deflated_sharpe_ratio, monte_carlo_ruin

logger = logging.getLogger(__name__)

__all__ = [
    "EventSpec",
    "simulate_event",
    "event_study",
    "summarize_event_study",
    "classify_event_study",
    "load_ohlcv_csv",
    "load_funding_csv",
]

# --- Gates del event-study (congelados 2026-07-24; ver REGLAS_CONGELADAS.md) ---
EV_MIN_EVENTS = 60      # muestra mínima total para validar (menos -> INSUFICIENTE)
EV_MIN_OOS = 15         # eventos mínimos en OOS para juzgar el OOS
EV_P_MAX = 0.05         # p(bootstrap, media<=0) máxima en IS
EV_OOS_IS_MIN = 0.5     # expectancy OOS >= 50% de la IS
EV_DSR_MIN = 0.95       # DSR sobre retornos por evento (multiple-testing)
EV_RUIN_MAX = 0.10      # P(ruina) Monte Carlo tolerable (soft)
EV_DATA_END_MAX = 0.10  # fracción tolerable de eventos truncados por fin de datos (soft)


@dataclass(frozen=True)
class EventSpec:
    """Spec congelada de un event-trade. Direction 'short' = tesis invertida v1."""

    direction: str = "short"
    entry_offset_h: float = -48.0   # entrada relativa al evento (negativa = antes, causal solo si el evento es programado/público)
    exit_offset_h: float = 24.0     # salida por tiempo relativa al evento
    stop_pct: float = 0.04          # cap INQUEBRANTABLE del programa
    fee_bps: float = 6.0            # taker perps pesimista (VIP0 Binance = 5 bps)
    slippage_bps: float = 10.0      # libros finos de alts
    risk_pct: float = 0.01


def load_ohlcv_csv(path: str) -> pd.DataFrame:
    """CSV de fetch_data (timestamp ms o ISO) -> DataFrame OHLCV indexado UTC."""
    df = pd.read_csv(path)
    tcol = "timestamp" if "timestamp" in df.columns else "date"
    if pd.api.types.is_numeric_dtype(df[tcol]):
        df.index = pd.to_datetime(df[tcol], unit="ms", utc=True)
    else:
        df.index = pd.to_datetime(df[tcol], utc=True)
    cols = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    return df[cols].astype(float).sort_index()


def load_funding_csv(path: str) -> pd.Series:
    """CSV de fetch_funding (timestamp ms, funding_rate) -> Series indexada UTC."""
    df = pd.read_csv(path)
    idx = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return pd.Series(df["funding_rate"].astype(float).to_numpy(), index=idx, name="funding_rate").sort_index()


def _bar_width(prices: pd.DataFrame) -> pd.Timedelta:
    if len(prices) < 3:
        return pd.Timedelta(hours=4)
    return prices.index.to_series().diff().median()


def simulate_event(
    prices: pd.DataFrame,
    event_time: pd.Timestamp,
    spec: EventSpec,
    funding: pd.Series | None = None,
) -> dict | None:
    """Simula UN evento. Devuelve dict del trade o None si no es ejecutable.

    Fills: entrada al open de la primera barra con open_time >= entry_time (la decisión se
    toma en entry_time, la barra abre después → causal). Stop conservador: dentro de la
    barra se asume que el stop se toca ANTES que cualquier movimiento favorable. Salida por
    tiempo: al open de la primera barra con open_time >= exit_time (chequeada ANTES que el
    stop de esa misma barra). Si los datos terminan antes: salida al último close, razón
    ``data_end`` (se reporta la fracción — gate soft).
    """
    if spec.direction not in ("short", "long"):
        raise ValueError(f"direction inválida: {spec.direction}")
    entry_time = event_time + pd.Timedelta(hours=spec.entry_offset_h)
    exit_time = event_time + pd.Timedelta(hours=spec.exit_offset_h)
    if exit_time <= entry_time:
        return None

    n = len(prices)
    if n == 0:
        return None
    entry_pos = int(prices.index.searchsorted(entry_time, side="left"))
    if entry_pos >= n:
        return None
    bw = _bar_width(prices)
    # Sin cobertura razonable al momento de entrada (símbolo aún no listado / hueco de datos).
    if prices.index[entry_pos] - entry_time > 2 * bw:
        return None
    if prices.index[entry_pos] >= exit_time:
        return None  # ventana degenerada para este timeframe

    o = prices["open"].to_numpy(dtype=float)
    h = prices["high"].to_numpy(dtype=float)
    lo = prices["low"].to_numpy(dtype=float)
    c = prices["close"].to_numpy(dtype=float)
    idx = prices.index

    entry_price = o[entry_pos]
    short = spec.direction == "short"
    stop_price = entry_price * (1 + spec.stop_pct) if short else entry_price * (1 - spec.stop_pct)

    exit_price = float("nan")
    exit_ts = idx[-1]
    reason = "data_end"
    i = entry_pos
    while i < n:
        if i > entry_pos and idx[i] >= exit_time:
            exit_price, exit_ts, reason = o[i], idx[i], "time"
            break
        hit = (h[i] >= stop_price) if short else (lo[i] <= stop_price)
        if hit:
            exit_price, exit_ts, reason = stop_price, idx[i], "stop"
            break
        i += 1
    if reason == "data_end":
        exit_price, exit_ts = c[-1], idx[-1]

    gross = (entry_price - exit_price) / entry_price if short else (exit_price - entry_price) / entry_price
    costs = 2.0 * (spec.fee_bps + spec.slippage_bps) / 1e4

    funding_ret = 0.0
    if funding is not None and len(funding):
        acc = funding.loc[(funding.index >= idx[entry_pos]) & (funding.index < exit_ts)].sum()
        # Funding positivo: longs pagan a shorts (el corto lo cobra; el largo lo paga).
        funding_ret = float(acc) if short else -float(acc)

    net = gross - costs + funding_ret
    return {
        "event_time": event_time,
        "entry_time": idx[entry_pos],
        "entry_price": float(entry_price),
        "exit_time": exit_ts,
        "exit_price": float(exit_price),
        "reason": reason,
        "bars_held": int(i - entry_pos + 1) if reason != "data_end" else int(n - entry_pos),
        "gross_ret": float(gross),
        "costs": float(costs),
        "funding_ret": float(funding_ret),
        "net_ret": float(net),
        "r_multiple": float(net / spec.stop_pct),
    }


def event_study(
    events: pd.DataFrame,
    prices_by_symbol: dict[str, pd.DataFrame],
    spec: EventSpec,
    funding_by_symbol: dict[str, pd.Series] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Corre la spec sobre todos los eventos. Devuelve (journal, contadores de descartes).

    ``events`` requiere columnas ``symbol`` y ``timestamp`` (ms epoch o datetime). Eventos
    solapados del MISMO símbolo (la entrada cae antes de la salida del anterior): se toma el
    PRIMERO y se descarta el resto (pre-registrado — en vivo habría una sola posición).
    """
    ev = events.copy()
    if pd.api.types.is_numeric_dtype(ev["timestamp"]):
        ev["event_time"] = pd.to_datetime(ev["timestamp"], unit="ms", utc=True)
    else:
        ev["event_time"] = pd.to_datetime(ev["timestamp"], utc=True)
    ev = ev.sort_values("event_time")

    skipped = {"sin_datos": 0, "solapado": 0, "no_ejecutable": 0}
    last_exit: dict[str, pd.Timestamp] = {}
    rows: list[dict] = []
    for _, r in ev.iterrows():
        sym = str(r["symbol"])
        prices = prices_by_symbol.get(sym)
        if prices is None or not len(prices):
            skipped["sin_datos"] += 1
            continue
        entry_time = r["event_time"] + pd.Timedelta(hours=spec.entry_offset_h)
        if sym in last_exit and entry_time < last_exit[sym]:
            skipped["solapado"] += 1
            continue
        fund = (funding_by_symbol or {}).get(sym)
        trade = simulate_event(prices, r["event_time"], spec, funding=fund)
        if trade is None:
            skipped["no_ejecutable"] += 1
            continue
        trade["symbol"] = sym
        for extra in ("pct_supply", "category", "protocol"):
            if extra in r.index and pd.notna(r[extra]):
                trade[extra] = r[extra]
        last_exit[sym] = trade["exit_time"]
        rows.append(trade)

    journal = pd.DataFrame(rows)
    if len(journal):
        journal = journal.sort_values("event_time").reset_index(drop=True)
    return journal, skipped


def _segment_stats(net: np.ndarray, n_trials: int) -> dict:
    """Métricas de un segmento (IS u OOS) sobre retornos netos POR EVENTO."""
    if net.size == 0:
        return {"n": 0}
    out = {
        "n": int(net.size),
        "expectancy_net": round(float(net.mean()), 5),
        "median_net": round(float(np.median(net)), 5),
        "win_rate": round(float((net > 0).mean()), 3),  # INFORMATIVO — jamás un gate
        "total_net": round(float(net.sum()), 4),
        "std_net": round(float(net.std(ddof=1)), 5) if net.size > 2 else None,
    }
    boot = bootstrap_mean_pvalue(net)
    out["p_mean_leq_0"] = boot.get("p_value")
    out["mean_ci95"] = [boot.get("ci_lo"), boot.get("ci_hi")]
    out["dsr"] = deflated_sharpe_ratio(net, n_trials=n_trials)
    return out


def summarize_event_study(
    journal: pd.DataFrame,
    skipped: dict,
    spec: EventSpec,
    is_end: str,
    n_trials: int,
) -> dict:
    """Resumen IS/OOS del journal de eventos + Monte Carlo de ruina sobre los R del IS."""
    summary: dict = {
        "spec": asdict(spec),
        "is_end": is_end,
        "n_trials": n_trials,
        "skipped": skipped,
        "n_total": int(len(journal)),
    }
    if not len(journal):
        summary["in_sample"] = {"n": 0}
        summary["oos"] = {"n": 0}
        return summary

    cutoff = pd.Timestamp(is_end, tz="UTC")
    is_j = journal[journal["event_time"] <= cutoff]
    oos_j = journal[journal["event_time"] > cutoff]

    summary["in_sample"] = _segment_stats(is_j["net_ret"].to_numpy(dtype=float), n_trials)
    summary["oos"] = _segment_stats(oos_j["net_ret"].to_numpy(dtype=float), n_trials)
    summary["data_end_frac"] = round(float((journal["reason"] == "data_end").mean()), 3)
    summary["funding_mean"] = round(float(journal["funding_ret"].mean()), 5)
    summary["gross_mean"] = round(float(journal["gross_ret"].mean()), 5)
    summary["stop_frac"] = round(float((journal["reason"] == "stop").mean()), 3)

    r_is = is_j["r_multiple"].to_numpy(dtype=float)
    if r_is.size:
        summary["monte_carlo_ruin"] = monte_carlo_ruin(
            r_is, n_sims=5000, risk_pct=spec.risk_pct, initial=500.0, ruin_frac=0.5, seed=0
        )
    return summary


def classify_event_study(summary: dict) -> dict:
    """Aplica los gates congelados al resumen. HARD = sin efecto; SOFT = muestra/borde.

    Veredictos: EDGE_EVENTO_VALIDADO / MUESTRA_INSUFICIENTE / ALERTA_SOFT / NO_OPERAR.
    El winrate NO participa de ningún gate (tesis invertida: PnL, no winrate).
    """
    hard: list[str] = []
    soft: list[str] = []
    ins = summary.get("in_sample", {}) or {}
    oos = summary.get("oos", {}) or {}
    n_total = summary.get("n_total", 0)

    if n_total < EV_MIN_EVENTS:
        soft.append(f"eventos={n_total}<{EV_MIN_EVENTS}")

    n_is = ins.get("n", 0)
    if n_is < 3:
        hard.append("IS sin eventos suficientes para estimar nada")
    else:
        exp_is = ins.get("expectancy_net")
        p = ins.get("p_mean_leq_0")
        if exp_is is None or exp_is <= 0:
            hard.append(f"IS expectancy={exp_is} <= 0")
        elif p is None or p >= EV_P_MAX:
            hard.append(f"IS p(media<=0)={p} >= {EV_P_MAX}")
        dsr = (ins.get("dsr") or {}).get("dsr")
        if dsr is None or not (dsr == dsr) or dsr < EV_DSR_MIN:
            hard.append(f"DSR={dsr}<{EV_DSR_MIN} (no distinguible de {summary.get('n_trials')} intentos)")

    n_oos = oos.get("n", 0)
    if n_oos < EV_MIN_OOS:
        soft.append(f"OOS n={n_oos}<{EV_MIN_OOS}: OOS no juzgable aún")
    elif not hard:
        exp_is = ins.get("expectancy_net") or 0.0
        exp_oos = oos.get("expectancy_net")
        if exp_oos is None or exp_oos <= 0:
            hard.append(f"OOS expectancy={exp_oos} <= 0")
        elif exp_is > 0 and exp_oos / exp_is < EV_OOS_IS_MIN:
            hard.append(f"OOS/IS={exp_oos / exp_is:.2f}<{EV_OOS_IS_MIN}")

    ruin = (summary.get("monte_carlo_ruin") or {}).get("prob_ruin")
    if ruin is not None and ruin > EV_RUIN_MAX:
        soft.append(f"P(ruina)={ruin:.1%}>{EV_RUIN_MAX:.0%}")
    de = summary.get("data_end_frac")
    if de is not None and de > EV_DATA_END_MAX:
        soft.append(f"eventos truncados por fin de datos={de:.0%}>{EV_DATA_END_MAX:.0%}")

    if hard:
        verdict = "NO_OPERAR"
    elif any(s.startswith("eventos=") or s.startswith("OOS n=") for s in soft):
        verdict = "MUESTRA_INSUFICIENTE"
    elif soft:
        verdict = "ALERTA_SOFT"
    else:
        verdict = "EDGE_EVENTO_VALIDADO"
    return {"hard_fail": hard, "soft_fail": soft, "verdict": verdict}
