"""Reporte de trades: slices por regimen/salida, export e import del journal.

Compartido por ``scripts/validate.py`` (genera el journal en backtest) y
``scripts/weekly_review.py`` (lo lee en vivo). Una sola definicion -> validate y review no
pueden divergir en como agrupan/leen los trades.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import Any

_CSV_HEADER = [
    "entry_time", "exit_time", "side", "tag", "regime",
    "entry_price", "exit_price", "qty", "pnl", "r_multiple", "exit_reason",
]


@dataclass
class TradeRow:
    """Fila de journal leida desde CSV (subset de ``backtest.Trade`` con lo necesario)."""

    entry_time: str
    exit_time: str
    side: str
    tag: str
    regime: str
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    r_multiple: float
    exit_reason: str


def slice_report(trades: list[Any]) -> dict:
    """Agrupa trades por regimen y por exit_reason: n, winrate, pnl total, R promedio.

    Acepta cualquier objeto con atributos ``pnl``, ``r_multiple``, ``regime``,
    ``exit_reason`` (tanto ``backtest.Trade`` como ``TradeRow``). Es el corazon del
    review de TESIS.md: ver DONDE gana y donde pierde, sin tocar parametros.
    """
    def _agg(keyfn) -> dict:
        groups: dict[str, list] = {}
        for t in trades:
            groups.setdefault(keyfn(t) or "(sin)", []).append(t)
        out = {}
        for k, ts in sorted(groups.items()):
            pnls = [t.pnl for t in ts]
            wins = sum(1 for p in pnls if p > 0)
            out[k] = {
                "n": len(ts),
                "winrate": round(wins / len(ts), 3),
                "pnl": round(sum(pnls), 2),
                "avg_r": round(sum(t.r_multiple for t in ts) / len(ts), 3),
            }
        return out

    return {
        "por_regimen": _agg(lambda t: t.regime),
        "por_salida": _agg(lambda t: t.exit_reason),
    }


def export_trades_csv(trades: list[Any], path: str) -> None:
    """Journal de trades a CSV (una fila por trade, con regimen y salida)."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(_CSV_HEADER)
        for t in trades:
            w.writerow([
                t.entry_time, t.exit_time, t.side, t.tag, getattr(t, "regime", ""),
                round(t.entry_price, 6), round(t.exit_price, 6), round(t.qty, 8),
                round(t.pnl, 4), round(t.r_multiple, 4), t.exit_reason,
            ])


def load_trades_csv(path: str) -> list[TradeRow]:
    """Lee un journal CSV (formato de ``export_trades_csv``) a una lista de TradeRow."""
    rows: list[TradeRow] = []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for d in r:
            rows.append(
                TradeRow(
                    entry_time=d.get("entry_time", ""),
                    exit_time=d.get("exit_time", ""),
                    side=d.get("side", ""),
                    tag=d.get("tag", ""),
                    regime=d.get("regime", ""),
                    entry_price=float(d.get("entry_price", 0) or 0),
                    exit_price=float(d.get("exit_price", 0) or 0),
                    qty=float(d.get("qty", 0) or 0),
                    pnl=float(d.get("pnl", 0) or 0),
                    r_multiple=float(d.get("r_multiple", 0) or 0),
                    exit_reason=d.get("exit_reason", ""),
                )
            )
    return rows


def loss_streaks(trades: list[Any]) -> list[int]:
    """Longitudes de las rachas de perdidas consecutivas (pnl < 0)."""
    streaks: list[int] = []
    cur = 0
    for t in trades:
        if t.pnl < 0:
            cur += 1
        elif cur > 0:
            streaks.append(cur)
            cur = 0
    if cur > 0:
        streaks.append(cur)
    return streaks
