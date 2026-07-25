#!/usr/bin/env python3
"""Extrae EVENTOS de cascada de liquidación (H9) desde klines 4h + open interest 5m.

Evento (spec CONGELADA en REGLAS_CONGELADAS.md → H9): en la barra t,
    ΔOI = OI_close(t)/OI_close(t−1) − 1 ≤ −oi_drop%   (purga de posiciones)
    volume_z(90, causal) ≥ vol_z                       (clímax de volumen)
    retorno de la barra < 0                            (purga de LONGS)
→ timestamp del evento = CIERRE de la barra t (la entrada long contrarian la hace el
event-study al open siguiente; causalidad pura: todo se observa al cierre).

El OI 5m se alinea a cada barra tomando la ÚLTIMA observación ≤ cierre de barra (causal).

Uso (VPS):
    python crypto/scripts/make_cascade_events.py --data-dir crypto/data/cascade \
        --symbols BTCUSDT,ETHUSDT --oi-drop 3.0 --vol-z 2.0 \
        --out crypto/data/cascade/events.csv
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])  # raíz del repo en path

from crypto.smc.events import load_ohlcv_csv  # noqa: E402
from crypto.smc.signals import volume_zscore  # noqa: E402

VOL_Z_WINDOW = 90  # congelado (mismo default causal de volume_zscore)


def align_oi_to_bar_close(bars: pd.DataFrame, oi: pd.Series) -> pd.Series:
    """OI vigente al CIERRE de cada barra: última observación 5m con ts <= close.

    ``bars`` indexado por OPEN de barra; close = open + ancho de barra. merge_asof
    backward garantiza causalidad (jamás toma una observación posterior al cierre).
    """
    if len(bars) < 3:
        return pd.Series(dtype=float, index=bars.index)
    bar_width = bars.index.to_series().diff().median()
    closes = bars.index + bar_width
    oi_df = oi.sort_index()
    merged = pd.merge_asof(
        pd.DataFrame({"close_ts": closes}),
        pd.DataFrame({"ts": oi_df.index, "oi": oi_df.to_numpy()}),
        left_on="close_ts", right_on="ts", direction="backward",
    )
    return pd.Series(merged["oi"].to_numpy(), index=bars.index, name="oi_close")


def cascade_events(bars: pd.DataFrame, oi: pd.Series, oi_drop_pct: float,
                   vol_z_min: float) -> pd.DataFrame:
    """Eventos de purga de longs. Devuelve DataFrame con timestamp(ms)=cierre de barra."""
    oi_close = align_oi_to_bar_close(bars, oi)
    doi = oi_close.pct_change()
    vz = volume_zscore(bars, VOL_Z_WINDOW)
    ret = bars["close"].astype(float) / bars["open"].astype(float) - 1.0

    mask = (doi <= -oi_drop_pct / 100.0) & (vz >= vol_z_min) & (ret < 0)
    mask = mask.fillna(False)
    if not mask.any():
        return pd.DataFrame(columns=["timestamp", "oi_drop_pct", "vol_z", "bar_ret"])
    bar_width = bars.index.to_series().diff().median()
    sel = bars.index[mask]
    out = pd.DataFrame({
        "timestamp": [(t + bar_width).value // 10**6 for t in sel],
        "oi_drop_pct": (doi[mask] * 100).round(3).to_numpy(),
        "vol_z": vz[mask].round(2).to_numpy(),
        "bar_ret": ret[mask].round(4).to_numpy(),
    })
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", required=True,
                   help="directorio con <SYM>-4h.csv y <SYM>-oi.csv")
    p.add_argument("--symbols", required=True, help="lista separada por comas")
    p.add_argument("--oi-drop", type=float, default=3.0, help="umbral congelado 3.0 (variante: 2.0)")
    p.add_argument("--vol-z", type=float, default=2.0)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    frames = []
    for sym in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        kpath = os.path.join(args.data_dir, f"{sym}-4h.csv")
        opath = os.path.join(args.data_dir, f"{sym}-oi.csv")
        if not (os.path.exists(kpath) and os.path.exists(opath)):
            print(f"  ⚠️ {sym}: falta {kpath if not os.path.exists(kpath) else opath}",
                  file=sys.stderr)
            continue
        bars = load_ohlcv_csv(kpath)
        oi_df = pd.read_csv(opath)
        oi = pd.Series(oi_df["open_interest"].astype(float).to_numpy(),
                       index=pd.to_datetime(oi_df["timestamp"], unit="ms", utc=True))
        ev = cascade_events(bars, oi, args.oi_drop, args.vol_z)
        ev.insert(0, "symbol", sym)
        ev["category"] = "cascade"
        frames.append(ev)
        print(f"  {sym}: {len(ev)} eventos", file=sys.stderr)

    if not frames:
        print("Sin eventos (¿faltan datos?).", file=sys.stderr)
        return 1
    all_ev = pd.concat(frames).sort_values("timestamp")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    all_ev.to_csv(args.out, index=False)
    print(f"OK: {len(all_ev)} eventos de cascada -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
