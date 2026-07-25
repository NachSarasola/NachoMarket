#!/usr/bin/env python3
"""Presupuesto de RIESGO VIVO — despliegue consciente en vías +EV con varianza real.

Mandato del usuario (2026-07-25): "no quiero algo perfecto, acepto algo de riesgo".
Traducción honesta: el riesgo aceptable NO es bajar los gates (eso es operar ruido), es
DESPLEGAR capital acotado en las vías con EV documentado y varianza real:
  - incentivos : points/airdrops (fees de volumen = costo de lotería +EV)
  - hlp        : depósito en vault de liquidación (~APR real, cola JELLY −27% posible)
  - experimento: pruebas live capadas (p.ej. MM long-tail midiendo markouts)

Journal CSV (una fila por movimiento):
    date,lane,venue,fees_usd,pnl_usd,est_value_usd,realized_usd,notes
    2026-08-01,incentivos,lighter,3.20,0,15,0,"volumen delta-neutral 2k"
    2026-08-03,hlp,hyperliquid,0,1.10,0,1.10,"apr acreditado"

Reglas mecánicas (KILL = parar la vía hasta el review semanal):
  - burn del mes (fees + pnl negativo) <= presupuesto  → si no: STOP_BURN
  - exposición HLP <= hlp-max (lo que tolerás perder un −30% intradía) → si no: STOP_HLP
El presupuesto NO es una meta: es un TECHO. Gastarlo entero no es obligatorio.

Uso:
    python crypto/scripts/budget_review.py --journal crypto/data/riesgo_vivo.csv \
        --budget-monthly 30 --hlp-max 150
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

import pandas as pd

LANES = ("incentivos", "hlp", "experimento")


def load_journal(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    need = {"date", "lane", "venue", "fees_usd", "pnl_usd", "est_value_usd", "realized_usd"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"journal sin columnas: {sorted(missing)}")
    df["date"] = pd.to_datetime(df["date"], utc=True)
    for c in ("fees_usd", "pnl_usd", "est_value_usd", "realized_usd"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["lane"] = df["lane"].astype(str).str.lower().str.strip()
    return df


def month_burn(df: pd.DataFrame, year: int, month: int) -> float:
    """Burn del mes = fees + pérdidas realizadas (el PnL positivo NO compensa el techo)."""
    m = df[(df["date"].dt.year == year) & (df["date"].dt.month == month)]
    fees = float(m["fees_usd"].sum())
    losses = float((-m.loc[m["pnl_usd"] < 0, "pnl_usd"]).sum())
    return fees + losses


def review(df: pd.DataFrame, budget_monthly: float, hlp_max: float,
           now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    burn = month_burn(df, now.year, now.month)
    # Exposición HLP viva = suma de est_value_usd de la vía hlp (depósitos +, retiros −).
    hlp_expo = float(df.loc[df["lane"] == "hlp", "est_value_usd"].sum())

    per_lane = {}
    for lane in LANES:
        d = df[df["lane"] == lane]
        per_lane[lane] = {
            "movs": int(len(d)),
            "fees": round(float(d["fees_usd"].sum()), 2),
            "pnl": round(float(d["pnl_usd"].sum()), 2),
            "ev_estimado": round(float(d["est_value_usd"].sum()), 2),
            "realizado": round(float(d["realized_usd"].sum()), 2),
        }

    alerts: list[str] = []
    if burn > budget_monthly:
        alerts.append(f"STOP_BURN: burn del mes ${burn:.2f} > techo ${budget_monthly:.2f}")
    if hlp_expo > hlp_max:
        alerts.append(f"STOP_HLP: exposición ${hlp_expo:.2f} > máximo ${hlp_max:.2f}")
    verdict = alerts[0].split(":")[0] if alerts else "DENTRO_PRESUPUESTO"
    return {"burn_mes": round(burn, 2), "budget": budget_monthly,
            "hlp_exposicion": round(hlp_expo, 2), "hlp_max": hlp_max,
            "por_via": per_lane, "alerts": alerts, "verdict": verdict}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--journal", required=True)
    p.add_argument("--budget-monthly", type=float, default=30.0,
                   help="techo de burn mensual (fees + pérdidas) de TODAS las vías vivas")
    p.add_argument("--hlp-max", type=float, default=150.0,
                   help="exposición máxima en HLP (= lo que tolerás perder −30% en un día)")
    args = p.parse_args()

    try:
        df = load_journal(args.journal)
    except FileNotFoundError:
        print(f"Sin journal aún ({args.journal}). Crearlo con el header del docstring.",
              file=sys.stderr)
        return 1
    rep = review(df, args.budget_monthly, args.hlp_max)

    print("\n== RIESGO VIVO — review ==")
    print(f"  burn del mes: ${rep['burn_mes']} / techo ${rep['budget']}")
    print(f"  exposición HLP: ${rep['hlp_exposicion']} / máx ${rep['hlp_max']}")
    print("  por vía:")
    for lane, s in rep["por_via"].items():
        line = (f"    {lane:<11} movs={s['movs']:<3} fees=${s['fees']:<8} "
                f"pnl=${s['pnl']:<8} EV est=${s['ev_estimado']:<8} "
                f"realizado=${s['realizado']:<8}")
        if s["fees"]:
            line += f" (realizado/fees={s['realizado'] / s['fees']:.2f})"
        print(line)
    for a in rep["alerts"]:
        print(f"  ⚠️  {a}")
    print(f"\n  >>> {rep['verdict']} <<<")
    print("  Regla: STOP_* = parar esa vía hasta el review semanal. El techo no es meta.")
    return 0 if rep["verdict"] == "DENTRO_PRESUPUESTO" else 2


if __name__ == "__main__":
    raise SystemExit(main())
