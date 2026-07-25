#!/usr/bin/env python3
"""Monitor de CARRY delta-neutral (funding) multi-venue + plan de capital hacia $5k.

La Etapa 3 (el único edge "grande" con paper detrás: cobrar el funding sin predecir) es
inviable por debajo de ~$5k. Este monitor convierte "esperar" en "saber":
  1. --snapshot: funding VIVO en Binance/Bybit/Hyperliquid, anualizado, con breakeven en
     días y APR NETO al capital actual → dice mecánicamente si el carry ya paga o no.
  2. --plan: meses hasta $5k con ahorro mensual + rendimiento del capital ocioso +
     escenarios de EV de incentivos (B6). El "cuándo" deja de ser una sensación.

Matemática del carry modelada (v1, sin apalancamiento): capital partido 50/50 en spot
long + perp short 1x; el funding se cobra sobre el notional del perp (= capital/2);
costos = ida y vuelta de las 2 patas (spot 10 bps + perp 5 bps por lado, pesimista).

Uso (VPS para --snapshot; --plan corre en cualquier lado):
    python crypto/scripts/carry_monitor.py --snapshot --capital 500
    python crypto/scripts/carry_monitor.py --plan --start 300 --monthly 200
"""

from __future__ import annotations

import argparse
import sys

SPOT_FEE = 0.0010   # 10 bps por lado (Binance spot sin BNB)
PERP_FEE = 0.0005   # 5 bps por lado (taker USDⓈ-M)
VIABLE_NET_APR = 0.10   # umbral registrado en Etapa 3: carry neto > 10% anual
HOLDING_DAYS_REF = 30   # horizonte de referencia para el APR neto
UA = {"User-Agent": "nacho-crypto/1.0"}


# --------------------------------------------------------------------------- #
# Matemática pura (testeada offline)
# --------------------------------------------------------------------------- #

def annualize_funding(rate: float, interval_h: float) -> float:
    """Funding por intervalo -> tasa anual simple (Binance/Bybit 8h, Hyperliquid 1h)."""
    return rate * (24.0 / interval_h) * 365.0


def carry_roundtrip_fees(capital: float) -> float:
    """Fees de armar Y desarmar las dos patas (spot + perp), cada una capital/2."""
    notional = capital / 2.0
    return notional * (2 * SPOT_FEE + 2 * PERP_FEE)


def carry_breakeven_days(capital: float, apr_gross: float) -> float:
    """Días de funding necesarios para pagar los fees de entrada+salida."""
    if apr_gross <= 0:
        return float("inf")
    daily = (capital / 2.0) * apr_gross / 365.0
    return carry_roundtrip_fees(capital) / daily


def carry_net_apr(capital: float, apr_gross: float, holding_days: float = HOLDING_DAYS_REF) -> float:
    """APR neto sobre el CAPITAL TOTAL, manteniendo la posición ``holding_days`` días."""
    if capital <= 0 or holding_days <= 0:
        return 0.0
    earn = (capital / 2.0) * apr_gross * holding_days / 365.0 - carry_roundtrip_fees(capital)
    return (earn / capital) * (365.0 / holding_days)


def capital_plan(start: float, monthly: float, idle_apr: float = 0.05,
                 incentive_monthly: float = 0.0, target: float = 5000.0,
                 max_months: int = 120) -> int:
    """Meses hasta ``target`` con ahorro + rendimiento del ocioso + EV de incentivos."""
    eq = start
    for m in range(1, max_months + 1):
        eq = eq * (1 + idle_apr / 12.0) + monthly + incentive_monthly
        if eq >= target:
            return m
    return max_months


# --------------------------------------------------------------------------- #
# Fetchers (red: correr en el VPS)
# --------------------------------------------------------------------------- #

def fetch_binance() -> list[dict]:
    import requests  # diferido

    r = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=30, headers=UA)
    r.raise_for_status()
    out = []
    for it in r.json():
        sym = it.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        try:
            rate = float(it.get("lastFundingRate") or 0)
        except (TypeError, ValueError):
            continue
        out.append({"venue": "binance", "symbol": sym, "rate": rate, "interval_h": 8.0})
    return out


def fetch_bybit() -> list[dict]:
    import requests  # diferido

    r = requests.get("https://api.bybit.com/v5/market/tickers?category=linear",
                     timeout=30, headers=UA)
    r.raise_for_status()
    out = []
    for it in (r.json().get("result") or {}).get("list") or []:
        sym = it.get("symbol", "")
        fr = it.get("fundingRate")
        if not sym.endswith("USDT") or fr in (None, ""):
            continue
        try:
            out.append({"venue": "bybit", "symbol": sym, "rate": float(fr), "interval_h": 8.0})
        except (TypeError, ValueError):
            continue
    return out


def fetch_hyperliquid() -> list[dict]:
    import requests  # diferido

    r = requests.post("https://api.hyperliquid.xyz/info", json={"type": "metaAndAssetCtxs"},
                      timeout=30, headers=UA)
    r.raise_for_status()
    meta, ctxs = r.json()
    names = [u.get("name", "") for u in meta.get("universe", [])]
    out = []
    for name, ctx in zip(names, ctxs):
        fr = ctx.get("funding")
        if fr in (None, ""):
            continue
        try:
            out.append({"venue": "hyperliquid", "symbol": name, "rate": float(fr),
                        "interval_h": 1.0})
        except (TypeError, ValueError):
            continue
    return out


def snapshot(capital: float, top: int = 15) -> int:
    rows: list[dict] = []
    for fn in (fetch_binance, fetch_bybit, fetch_hyperliquid):
        try:
            rows += fn()
        except Exception as e:  # noqa: BLE001
            print(f"⚠️ {fn.__name__} falló: {e}", file=sys.stderr)
    if not rows:
        print("Sin datos de funding (¿red?).", file=sys.stderr)
        return 1
    for r in rows:
        r["apr"] = annualize_funding(r["rate"], r["interval_h"])
        r["net_apr"] = carry_net_apr(capital, abs(r["apr"]))
        r["breakeven_d"] = carry_breakeven_days(capital, abs(r["apr"]))
    rows.sort(key=lambda r: -abs(r["apr"]))

    print(f"\n== CARRY SNAPSHOT (capital ${capital:.0f}; fees spot {SPOT_FEE*1e4:.0f}bps + "
          f"perp {PERP_FEE*1e4:.0f}bps por lado; holding ref {HOLDING_DAYS_REF}d) ==")
    print(f"  {'venue':<12} {'symbol':<14} {'funding':>10} {'APR bruto':>10} "
          f"{'APR neto':>9} {'breakeven':>10}")
    for r in rows[:top]:
        print(f"  {r['venue']:<12} {r['symbol']:<14} {r['rate']*100:>9.4f}% "
              f"{r['apr']*100:>9.1f}% {r['net_apr']*100:>8.1f}% {r['breakeven_d']:>8.1f}d")
    majors = [r for r in rows if r["symbol"] in
              ("BTCUSDT", "ETHUSDT", "BTC", "ETH") and r["venue"] in ("binance", "hyperliquid")]
    if majors:
        print("\n  Majors (referencia del ciclo):")
        for r in majors:
            print(f"    {r['venue']:<12} {r['symbol']:<8} APR bruto {r['apr']*100:>6.1f}%")
    best = max(rows, key=lambda r: r["net_apr"])
    verdict = "VIABLE" if best["net_apr"] >= VIABLE_NET_APR else "NO VIABLE aún"
    print(f"\n  >>> a ${capital:.0f}: mejor carry neto = {best['net_apr']*100:.1f}% anual "
          f"({best['venue']} {best['symbol']}) -> {verdict} "
          f"(umbral registrado {VIABLE_NET_APR:.0%}) <<<")
    print("  Recordatorio: funding es CÍCLICO; paga en mercado caliente. Riesgos: flip de "
          "funding, basis, liquidación de la pata short, venue.")
    return 0


def plan(start: float, monthly: float, idle_apr: float, target: float) -> int:
    print(f"\n== PLAN DE CAPITAL hacia ${target:.0f} (arranque ${start:.0f}, "
          f"ocioso {idle_apr:.0%} anual) ==")
    print(f"  {'ahorro/mes':>10} | {'sin incentivos':>14} | {'B6 +$50/mes':>12} | "
          f"{'B6 +$150/mes':>13}")
    for m in sorted({monthly, 100.0, 200.0, 300.0, 500.0}):
        cols = [capital_plan(start, m, idle_apr, inc, target) for inc in (0.0, 50.0, 150.0)]
        mark = " <— tu plan" if m == monthly else ""
        print(f"  ${m:>8.0f} | {cols[0]:>11} m | {cols[1]:>9} m | {cols[2]:>10} m{mark}")
    print("  (B6 = cosecha de incentivos con caps duros; EV NO garantizado — escenarios)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--snapshot", action="store_true", help="funding vivo multi-venue")
    p.add_argument("--capital", type=float, default=500.0)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--plan", action="store_true", help="meses hasta el target")
    p.add_argument("--start", type=float, default=300.0)
    p.add_argument("--monthly", type=float, default=200.0)
    p.add_argument("--idle-apr", type=float, default=0.05,
                   help="rendimiento anual del capital ocioso (stables ~5%%)")
    p.add_argument("--target", type=float, default=5000.0)
    args = p.parse_args()

    if not args.snapshot and not args.plan:
        p.error("elegir --snapshot y/o --plan")
    rc = 0
    if args.snapshot:
        rc = snapshot(args.capital, args.top)
    if args.plan:
        rc = plan(args.start, args.monthly, args.idle_apr, args.target) or rc
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
