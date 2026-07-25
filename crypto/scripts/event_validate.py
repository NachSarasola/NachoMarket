#!/usr/bin/env python3
"""Validación de hipótesis EVENT-DRIVEN (H7 unlocks / H8 listings) con gates congelados.

La tesis invertida: el winrate NO importa (se reporta, jamás decide) — importa la
expectancy NETA de costos y funding, su OOS y su DSR. Specs CONGELADAS en
REGLAS_CONGELADAS.md el 2026-07-24; este script solo las ejecuta.

Uso (VPS, tras fetch_unlocks/fetch_listings + klines/funding en --data-dir):
    python crypto/scripts/event_validate.py --strategy h7_unlock \
        --events crypto/data/events/unlocks.csv --data-dir crypto/data/events \
        --funding-dir crypto/data/events --deflated-sharpe 130 \
        --out crypto/data/rep_h7.json --trades-out crypto/data/journal_h7.csv

Convención de archivos: <SYMBOL>-4h.csv (klines perp) y <SYMBOL>-funding.csv.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd

sys.path.insert(0, __file__.rsplit("/crypto/", 1)[0])  # raíz del repo en path

from crypto.smc.events import (  # noqa: E402
    EventSpec,
    classify_event_study,
    event_study,
    load_funding_csv,
    load_ohlcv_csv,
    summarize_event_study,
)

# --- SPECS CONGELADAS 2026-07-24 (ver REGLAS_CONGELADAS.md; NO tocar fuera de la
#     variante única pre-registrada, que se corre con --variant y cuenta como trial) ---
EVENT_SPECS: dict[str, dict] = {
    "h7_unlock": {
        "spec": EventSpec(direction="short", entry_offset_h=-48.0, exit_offset_h=24.0,
                          stop_pct=0.04, fee_bps=6.0, slippage_bps=10.0, risk_pct=0.01),
        "min_pct": 2.0,   # cliff >= 2% del supply (basis max_supply: conservador)
        "variant": {"min_pct": 1.0},  # ÚNICA variante permitida (palanca de muestra)
    },
    "h8_listing": {
        "spec": EventSpec(direction="short", entry_offset_h=24.0, exit_offset_h=168.0,
                          stop_pct=0.04, fee_bps=6.0, slippage_bps=10.0, risk_pct=0.01),
        "min_pct": None,
        "variant": {"exit_offset_h": 72.0},  # ÚNICA variante permitida (salida día 3)
    },
    # H9 (spec congelada 2026-07-25): LONG contrarian tras purga de longs (ΔOI<=-3% +
    # vol_z>=2 + barra roja) en 10 majors. El evento se OBSERVA al cierre -> entrada al
    # open siguiente (entry_offset 0). La variante (oi_drop 2%) se aplica al GENERAR los
    # eventos (make_cascade_events --oi-drop 2.0), no a este spec.
    "h9_cascade": {
        "spec": EventSpec(direction="long", entry_offset_h=0.0, exit_offset_h=48.0,
                          stop_pct=0.04, fee_bps=6.0, slippage_bps=10.0, risk_pct=0.01),
        "min_pct": None,
        "variant": {"note": "regenerar eventos con make_cascade_events --oi-drop 2.0"},
    },
}
DEFAULT_IS_END = "2024-12-31"  # IS hasta 2024, OOS 2025+ (una sola pasada)


def load_events(path: str) -> pd.DataFrame:
    ev = pd.read_csv(path)
    if "symbol" not in ev.columns or "timestamp" not in ev.columns:
        raise ValueError("events CSV requiere columnas symbol,timestamp")
    return ev


def load_market_data(symbols: list[str], data_dir: str, funding_dir: str | None
                     ) -> tuple[dict[str, pd.DataFrame], dict[str, pd.Series]]:
    prices: dict[str, pd.DataFrame] = {}
    funding: dict[str, pd.Series] = {}
    for sym in symbols:
        kpath = os.path.join(data_dir, f"{sym}-4h.csv")
        if os.path.exists(kpath):
            try:
                prices[sym] = load_ohlcv_csv(kpath)
            except Exception as e:  # noqa: BLE001
                print(f"⚠️  klines ilegibles {kpath}: {e}", file=sys.stderr)
        if funding_dir:
            fpath = os.path.join(funding_dir, f"{sym}-funding.csv")
            if os.path.exists(fpath):
                try:
                    funding[sym] = load_funding_csv(fpath)
                except Exception as e:  # noqa: BLE001
                    print(f"⚠️  funding ilegible {fpath}: {e}", file=sys.stderr)
    return prices, funding


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--strategy", choices=sorted(EVENT_SPECS), required=True)
    p.add_argument("--events", required=True, help="CSV de eventos (symbol,timestamp,...)")
    p.add_argument("--data-dir", required=True, help="directorio con <SYMBOL>-4h.csv")
    p.add_argument("--funding-dir", default="", help="directorio con <SYMBOL>-funding.csv")
    p.add_argument("--is-end", default=DEFAULT_IS_END)
    p.add_argument("--deflated-sharpe", type=int, default=1, metavar="N_TRIALS")
    p.add_argument("--variant", action="store_true",
                   help="corre la ÚNICA variante pre-registrada (cuenta como trial extra)")
    p.add_argument("--out", default="", help="reporte JSON")
    p.add_argument("--trades-out", default="", help="journal CSV por evento")
    args = p.parse_args()

    cfg = EVENT_SPECS[args.strategy]
    spec: EventSpec = cfg["spec"]
    min_pct = cfg["min_pct"]
    if args.variant:
        var = dict(cfg["variant"])
        min_pct = var.pop("min_pct", min_pct)
        note = var.pop("note", "")
        if var:
            from dataclasses import replace
            spec = replace(spec, **var)
        print(f"### VARIANTE pre-registrada activa: {cfg['variant']} — cuenta como trial ###")
        if note:
            print(f"### NOTA: {note} ###")

    events = load_events(args.events)
    n_raw = len(events)
    dropped_pct = 0
    if min_pct is not None:
        if "pct_supply" not in events.columns:
            print("⚠️  events sin columna pct_supply; no se puede aplicar el umbral", file=sys.stderr)
            return 2
        with_pct = events[pd.to_numeric(events["pct_supply"], errors="coerce").notna()].copy()
        dropped_pct = n_raw - len(with_pct)
        with_pct["pct_supply"] = with_pct["pct_supply"].astype(float)
        events = with_pct[with_pct["pct_supply"] >= min_pct]
    print(f"Eventos: {n_raw} crudos -> {len(events)} tras filtro"
          f" (min_pct={min_pct}, sin_pct_descartados={dropped_pct})")

    symbols = sorted(set(events["symbol"].astype(str)))
    prices, funding = load_market_data(symbols, args.data_dir, args.funding_dir or None)
    print(f"Símbolos con klines: {len(prices)}/{len(symbols)}"
          f" | con funding: {len(funding)}/{len(symbols)}")
    if args.strategy == "h7_unlock" and not funding:
        print("⚠️  H7 sin NINGUNA serie de funding: el costo del short queda subestimado. "
              "Se corre igual pero el reporte lo marca.", file=sys.stderr)

    journal, skipped = event_study(events, prices, spec, funding_by_symbol=funding)
    summary = summarize_event_study(journal, skipped, spec, args.is_end, args.deflated_sharpe)
    summary["strategy"] = args.strategy
    summary["variant"] = bool(args.variant)
    summary["min_pct"] = min_pct
    summary["events_raw"] = n_raw
    summary["funding_series"] = len(funding)
    cls = classify_event_study(summary)
    summary["gates"] = cls

    print(f"\n== {args.strategy} ({spec.direction}, entry {spec.entry_offset_h:+.0f}h, "
          f"exit {spec.exit_offset_h:+.0f}h, stop {spec.stop_pct:.0%}) ==")
    print(f"  descartes: {skipped} | trades simulados: {summary['n_total']}")
    for seg in ("in_sample", "oos"):
        s = summary.get(seg, {})
        if s.get("n"):
            print(f"  {seg:>10}: n={s['n']:>3} expectancy={s['expectancy_net']:+.4f} "
                  f"mediana={s['median_net']:+.4f} wr={s['win_rate']} (informativo) "
                  f"p(≤0)={s['p_mean_leq_0']} DSR={s['dsr'].get('dsr')}")
        else:
            print(f"  {seg:>10}: sin eventos")
    if "monte_carlo_ruin" in summary:
        mc = summary["monte_carlo_ruin"]
        print(f"  MC ruina: P={mc['prob_ruin']:.1%} | retorno p5/p50/p95 = "
              f"{mc['final_return_p5']}/{mc['final_return_p50']}/{mc['final_return_p95']}")
    print(f"  gross medio={summary.get('gross_mean')} funding medio={summary.get('funding_mean')} "
          f"(share del edge que se lleva el funding) | stops={summary.get('stop_frac')} "
          f"| truncados={summary.get('data_end_frac')}")

    print("\n" + "=" * 55)
    if cls["hard_fail"]:
        print("  HARD:", cls["hard_fail"])
    if cls["soft_fail"]:
        print("  SOFT:", cls["soft_fail"])
    print(f"  >>> VEREDICTO: {cls['verdict']} <<<")
    print("=" * 55)
    print("  Registrar como fila en crypto/REGLAS_CONGELADAS.md (cuenta para el DSR).")

    if args.trades_out and len(journal):
        os.makedirs(os.path.dirname(args.trades_out) or ".", exist_ok=True)
        journal.to_csv(args.trades_out, index=False)
        print(f"\nJournal por evento -> {args.trades_out}")
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"Reporte JSON -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
