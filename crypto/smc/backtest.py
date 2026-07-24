"""Backtester event-driven bar-by-bar, honesto y compacto.

Diseñado para NO repetir el error del bot anterior (paper sim que reportaba 100%
winrate porque nunca modelaba fills adversos). Garantias:

1. SIN LOOK-AHEAD EN FILLS: la senal se calcula al cierre de la barra ``t`` y la
   entrada se llena al OPEN de la barra ``t+1`` (mas slippage). Ningun fill usa datos
   de su propia barra de decision.
2. COSTOS EXPLICITOS: fee por lado + slippage por lado, aplicados en cada entrada y
   salida (incluidos parciales).
3. ORDEN INTRABAR CONSERVADOR: si en una misma barra el precio toca stop y target, se
   asume que toco el STOP primero (peor caso).
4. SPOT LONG-ONLY por defecto (``direction='long'``): en spot no se puede shortear. El
   modo ``'both'`` existe solo para investigacion sobre perps.

La logica de fills esta cubierta por ``tests/test_smc_backtest.py`` con casos borde.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class Trade:
    """Una operacion cerrada (puede haber tenido salidas parciales)."""

    side: str  # 'long' | 'short'
    entry_time: pd.Timestamp
    entry_price: float
    exit_time: pd.Timestamp
    exit_price: float  # precio promedio ponderado de salida
    qty: float  # cantidad de base asset (tamaño total abierto)
    pnl: float  # PnL neto en quote (USDT), ya descontados fees/slippage
    r_multiple: float  # PnL / riesgo inicial
    exit_reason: str  # 'stop' | 'tp2' | 'time_stop' | 'eod'
    tag: str = ""
    regime: str = ""  # etiqueta de regimen al momento de la senal (journal/slices)


@dataclass
class BacktestResult:
    trades: list[Trade]
    equity: pd.Series  # equity mark-to-market por barra
    initial_equity: float
    bars_per_year: float
    params: dict = field(default_factory=dict)

    def metrics(self) -> dict:
        return compute_metrics(
            self.equity, self.trades, self.bars_per_year, self.initial_equity
        )


def run_backtest(
    df: pd.DataFrame,
    signals: pd.DataFrame,
    *,
    fee_bps: float = 10.0,
    slippage_bps: float = 5.0,
    risk_pct: float = 0.01,
    initial_equity: float = 500.0,
    direction: str = "long",
    tp1_fraction: float = 0.5,
    bars_per_year: float = 6 * 365,  # 4h -> 6 barras/dia
    max_notional_pct: float = 1.0,
) -> BacktestResult:
    """Corre el backtest sobre ``df`` (OHLCV) usando las columnas de ``signals``.

    Args:
        df: OHLCV con indice temporal ascendente.
        signals: salida de ``smc_sweep_signals`` / ``donchian_bms_signals``.
        fee_bps: fee por lado en basis points (10 bps = 0.10%).
        slippage_bps: slippage por lado en basis points.
        risk_pct: fraccion del equity arriesgada por trade (distancia al stop).
        direction: 'long' (spot), 'short', o 'both' (perps, investigacion).
        tp1_fraction: fraccion cerrada en TP1 (resto va a TP2/time-stop). Si el
            target de la senal no tiene TP1 != TP2, actua como salida unica.
        bars_per_year: para anualizar metricas (4h => 2190).
        max_notional_pct: tope de notional por trade como fraccion del equity (spot: 1.0).

    Returns:
        BacktestResult con trades, curva de equity y ``.metrics()``.
    """
    if direction not in ("long", "short", "both"):
        raise ValueError(f"direction invalido: {direction}")

    idx = df.index
    open_ = df["open"].astype(float).to_numpy()
    high = df["high"].astype(float).to_numpy()
    low = df["low"].astype(float).to_numpy()
    close = df["close"].astype(float).to_numpy()

    enter_long = signals["enter_long"].to_numpy(dtype=bool)
    enter_short = signals["enter_short"].to_numpy(dtype=bool)
    sl_arr = signals["sl_price"].to_numpy(dtype=float)
    tp1_arr = signals["tp1_price"].to_numpy(dtype=float)
    tp2_arr = signals["tp2_price"].to_numpy(dtype=float)
    tag_arr = signals["enter_tag"].to_numpy(dtype=object)
    tstop_arr = (
        signals["time_stop_bars"].to_numpy(dtype=float)
        if "time_stop_bars" in signals
        else np.full(len(idx), np.inf)
    )
    regime_arr = (
        signals["regime"].to_numpy(dtype=object)
        if "regime" in signals
        else np.array([""] * len(idx), dtype=object)
    )

    fee = fee_bps / 1e4
    slip = slippage_bps / 1e4

    n = len(idx)
    equity = initial_equity
    equity_curve = np.full(n, initial_equity, dtype=float)

    trades: list[Trade] = []

    # Estado de posicion abierta.
    in_pos = False
    pos = {}

    def _open_from_signal(t_sig: int) -> None:
        """Programa apertura a partir de una senal en la barra t_sig (fill en t_sig+1)."""
        nonlocal in_pos, pos
        fill_i = t_sig + 1
        if fill_i >= n:
            return
        want_long = enter_long[t_sig] and direction in ("long", "both")
        want_short = enter_short[t_sig] and direction in ("short", "both")
        if not (want_long or want_short):
            return
        side = "long" if want_long else "short"
        raw_fill = open_[fill_i]
        # Slippage en contra en la entrada.
        entry = raw_fill * (1 + slip) if side == "long" else raw_fill * (1 - slip)
        stop = sl_arr[t_sig]
        if not np.isfinite(stop):
            return
        risk_per_unit = (entry - stop) if side == "long" else (stop - entry)
        if risk_per_unit <= 0:
            return
        risk_budget = equity * risk_pct
        qty = risk_budget / risk_per_unit
        # Tope de notional (spot: sin apalancamiento).
        max_qty = (equity * max_notional_pct) / entry
        qty = min(qty, max_qty)
        if qty <= 0:
            return
        # Fee de entrada (se contabiliza aparte para no doblarla en el MTM).
        entry_fee = entry * qty * fee
        pos = {
            "side": side,
            "entry_i": fill_i,
            "entry_time": idx[fill_i],
            "entry": entry,
            "stop": stop,
            "tp1": tp1_arr[t_sig],
            "tp2": tp2_arr[t_sig],
            "qty": qty,
            "qty_open": qty,
            "risk_per_unit": risk_per_unit,
            "tag": tag_arr[t_sig] if tag_arr[t_sig] else "",
            "regime": regime_arr[t_sig] if regime_arr[t_sig] else "",
            "tstop": tstop_arr[t_sig],
            "entry_fee": entry_fee,
            "fees_paid": entry_fee,  # total fees (entrada + salidas), solo para reporte
            "realized": 0.0,  # sum(gross_partial - exit_fee_partial) de parciales cerrados
            "exit_value": 0.0,  # sum(exit_price*qty) para precio prom de salida
            "exit_qty": 0.0,
            "tp1_done": False,
            "last_exit_reason": "",
        }
        in_pos = True

    def _close_qty(price: float, qty_close: float, reason: str) -> None:
        """Cierra ``qty_close`` de la posicion al ``price`` (ya con slippage aplicado)."""
        nonlocal equity
        side = pos["side"]
        exit_fee = price * qty_close * fee
        pos["fees_paid"] += exit_fee
        if side == "long":
            gross = (price - pos["entry"]) * qty_close
        else:
            gross = (pos["entry"] - price) * qty_close
        pos["realized"] += gross - exit_fee
        pos["exit_value"] += price * qty_close
        pos["exit_qty"] += qty_close
        pos["qty_open"] -= qty_close
        pos["last_exit_reason"] = reason

    def _finalize() -> None:
        nonlocal in_pos, equity, pos
        # ``realized`` ya neteo las fees de cada salida parcial; solo resta la fee de
        # entrada para obtener el PnL neto total. Sin doble conteo.
        net = pos["realized"] - pos["entry_fee"]
        exit_avg = pos["exit_value"] / pos["exit_qty"] if pos["exit_qty"] > 0 else pos["entry"]
        equity += net
        risk0 = pos["risk_per_unit"] * pos["qty"]
        r_mult = net / risk0 if risk0 > 0 else 0.0
        trades.append(
            Trade(
                side=pos["side"],
                entry_time=pos["entry_time"],
                entry_price=pos["entry"],
                exit_time=idx[pos["exit_i"]],
                exit_price=exit_avg,
                qty=pos["qty"],
                pnl=net,
                r_multiple=r_mult,
                exit_reason=pos["last_exit_reason"],
                tag=pos["tag"],
                regime=pos.get("regime", ""),
            )
        )
        in_pos = False
        pos = {}

    i = 0
    while i < n:
        # 1) Gestion de posicion abierta en la barra i.
        if in_pos and i > pos["entry_i"]:
            side = pos["side"]
            stop = pos["stop"]
            tp1 = pos["tp1"]
            tp2 = pos["tp2"]
            bars_held = i - pos["entry_i"]
            exited = False

            # --- Stop primero (conservador) ---
            if side == "long" and low[i] <= stop:
                fill = stop * (1 - slip)
                _close_qty(fill, pos["qty_open"], "stop")
                pos["exit_i"] = i
                _finalize()
                exited = True
            elif side == "short" and high[i] >= stop:
                fill = stop * (1 + slip)
                _close_qty(fill, pos["qty_open"], "stop")
                pos["exit_i"] = i
                _finalize()
                exited = True

            # --- TP1 parcial + mover stop a breakeven ---
            if not exited and not pos["tp1_done"] and np.isfinite(tp1):
                hit_tp1 = (side == "long" and high[i] >= tp1) or (
                    side == "short" and low[i] <= tp1
                )
                # Solo si tp1 != tp2 (si son iguales, se maneja como salida unica en tp2)
                if hit_tp1 and not math.isclose(tp1, tp2, rel_tol=1e-9):
                    fill = tp1 * (1 - slip) if side == "long" else tp1 * (1 + slip)
                    _close_qty(fill, pos["qty_open"] * tp1_fraction, "tp1")
                    pos["tp1_done"] = True
                    pos["stop"] = pos["entry"]  # breakeven, nunca en contra

            # --- TP2 / target final ---
            if not exited and np.isfinite(tp2):
                hit_tp2 = (side == "long" and high[i] >= tp2) or (
                    side == "short" and low[i] <= tp2
                )
                if hit_tp2:
                    fill = tp2 * (1 - slip) if side == "long" else tp2 * (1 + slip)
                    _close_qty(fill, pos["qty_open"], "tp2")
                    pos["exit_i"] = i
                    _finalize()
                    exited = True

            # --- Time-stop ---
            if not exited and bars_held >= pos["tstop"]:
                fill = close[i] * (1 - slip) if side == "long" else close[i] * (1 + slip)
                _close_qty(fill, pos["qty_open"], "time_stop")
                pos["exit_i"] = i
                _finalize()
                exited = True

        # 2) Nueva senal en la barra i (fill en i+1) si no hay posicion.
        if not in_pos and (enter_long[i] or enter_short[i]):
            _open_from_signal(i)

        # 3) Marcar equity mark-to-market de la barra (sin doble conteo de fees:
        #    unrealized bruto sobre la qty abierta + parciales netos - fee de entrada).
        if in_pos and i >= pos["entry_i"]:
            if pos["side"] == "long":
                unreal = (close[i] - pos["entry"]) * pos["qty_open"]
            else:
                unreal = (pos["entry"] - close[i]) * pos["qty_open"]
            equity_curve[i] = equity + unreal + pos["realized"] - pos["entry_fee"]
        else:
            equity_curve[i] = equity
        i += 1

    # Cierre forzado al final del historico.
    if in_pos:
        side = pos["side"]
        fill = close[n - 1] * (1 - slip) if side == "long" else close[n - 1] * (1 + slip)
        _close_qty(fill, pos["qty_open"], "eod")
        pos["exit_i"] = n - 1
        _finalize()
        equity_curve[n - 1] = equity

    return BacktestResult(
        trades=trades,
        equity=pd.Series(equity_curve, index=idx, name="equity"),
        initial_equity=initial_equity,
        bars_per_year=bars_per_year,
        params={
            "fee_bps": fee_bps,
            "slippage_bps": slippage_bps,
            "risk_pct": risk_pct,
            "direction": direction,
        },
    )


def compute_metrics(
    equity: pd.Series,
    trades: list[Trade],
    bars_per_year: float,
    initial_equity: float,
) -> dict:
    """Metricas completas — incluye maxDD y rachas, NUNCA solo winrate.

    El winrate aislado fue la senal que enmascaro el riesgo de cola del bot anterior;
    aca siempre se reporta junto a profit factor, expectancy en R, maxDD y Sharpe.
    """
    eq = equity.to_numpy(dtype=float)
    n = len(eq)
    if n == 0 or initial_equity <= 0:
        return {"trades": 0}

    rets = np.diff(eq) / eq[:-1]
    rets = rets[np.isfinite(rets)]

    total_return = eq[-1] / initial_equity - 1.0
    years = n / bars_per_year if bars_per_year > 0 else np.nan
    cagr = (eq[-1] / initial_equity) ** (1 / years) - 1 if years and years > 0 and eq[-1] > 0 else np.nan

    if rets.size > 1 and rets.std(ddof=1) > 0:
        sharpe = rets.mean() / rets.std(ddof=1) * math.sqrt(bars_per_year)
        downside = rets[rets < 0]
        sortino = (
            rets.mean() / downside.std(ddof=1) * math.sqrt(bars_per_year)
            if downside.size > 1 and downside.std(ddof=1) > 0
            else np.nan
        )
    else:
        sharpe = np.nan
        sortino = np.nan

    running_max = np.maximum.accumulate(eq)
    dd = (eq - running_max) / running_max
    max_dd = float(dd.min()) if n > 0 else np.nan
    calmar = (cagr / abs(max_dd)) if (max_dd and max_dd < 0 and not np.isnan(cagr)) else np.nan

    n_trades = len(trades)
    if n_trades > 0:
        pnls = np.array([t.pnl for t in trades])
        rs = np.array([t.r_multiple for t in trades])
        wins = pnls[pnls > 0]
        losses = pnls[pnls < 0]
        win_rate = wins.size / n_trades
        gross_win = wins.sum()
        gross_loss = -losses.sum()
        profit_factor = (gross_win / gross_loss) if gross_loss > 0 else np.inf
        expectancy_r = float(rs.mean())
        avg_win = float(wins.mean()) if wins.size else 0.0
        avg_loss = float(losses.mean()) if losses.size else 0.0
        # Peor racha de perdidas.
        max_loss_streak = 0
        cur = 0
        for p in pnls:
            if p < 0:
                cur += 1
                max_loss_streak = max(max_loss_streak, cur)
            else:
                cur = 0
    else:
        win_rate = profit_factor = expectancy_r = avg_win = avg_loss = np.nan
        max_loss_streak = 0

    return {
        "trades": n_trades,
        "total_return": round(float(total_return), 4),
        "cagr": round(float(cagr), 4) if not np.isnan(cagr) else None,
        "sharpe": round(float(sharpe), 3) if not np.isnan(sharpe) else None,
        "sortino": round(float(sortino), 3) if not np.isnan(sortino) else None,
        "max_drawdown": round(float(max_dd), 4) if not np.isnan(max_dd) else None,
        "calmar": round(float(calmar), 3) if calmar is not None and not np.isnan(calmar) else None,
        "win_rate": round(float(win_rate), 4) if not np.isnan(win_rate) else None,
        "profit_factor": round(float(profit_factor), 3) if np.isfinite(profit_factor) else None,
        "expectancy_r": round(float(expectancy_r), 4) if not np.isnan(expectancy_r) else None,
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "max_loss_streak": int(max_loss_streak),
        "final_equity": round(float(eq[-1]), 2),
    }
