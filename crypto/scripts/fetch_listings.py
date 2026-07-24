#!/usr/bin/env python3
"""Deriva los LISTINGS spot de Binance mecánicamente: fecha = primera vela 1d del par.

Sin scrapear anuncios: el dato es la propia serie (primera kline spot = primer día de
trading). Se limita a pares USDT en TRADING, excluyendo stables/fiat y tokens apalancados.
También descarga el exchangeInfo de fapi (perps) para saber si el token tiene perp y desde
cuándo (onboardDate) — el short de H8 se ejecuta en el perp.

SESGO PRE-REGISTRADO: exchangeInfo solo lista símbolos VIVOS → los listings ya delistados
(los peores, mejores shorts) no aparecen. El efecto medido es un PISO del efecto real.

Uso (VPS):
    python crypto/scripts/fetch_listings.py --since 2023-01-01 \
        --out crypto/data/events/listings.csv --perps-out crypto/data/events/perps.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

SPOT_INFO = "https://api.binance.com/api/v3/exchangeInfo"
SPOT_KLINES = "https://api.binance.com/api/v3/klines"
FAPI_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"

STABLE_OR_FIAT = {
    "USDC", "TUSD", "FDUSD", "BUSD", "DAI", "USDP", "UST", "USTC", "PAX", "AEUR", "EURI",
    "EUR", "GBP", "TRY", "BRL", "ARS", "RUB", "UAH", "NGN", "ZAR", "JPY", "PLN", "RON",
    "CZK", "MXN", "COP", "USD1", "USDE", "XUSD", "WBTC", "WBETH", "WETH",
}
LEVERAGED_SUFFIX = ("UP", "DOWN", "BULL", "BEAR")
LEVERAGED_EXCEPTIONS = {"JUP", "SUP", "PUMP", "DOWN"}  # bases reales que parecen sufijo


def _to_ms(date_str: str) -> int:
    dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _get_json(url: str, params: dict | None = None, timeout: int = 30):
    import requests  # diferido

    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def is_leveraged_base(base: str) -> bool:
    if base in LEVERAGED_EXCEPTIONS:
        return False
    return any(base.endswith(s) and len(base) > len(s) for s in LEVERAGED_SUFFIX)


def filter_spot_symbols(info: dict, quote: str = "USDT") -> list[dict]:
    """exchangeInfo spot -> [{'symbol','base'}] de pares operables no-stable no-apalancados."""
    out = []
    for s in info.get("symbols", []):
        if s.get("quoteAsset") != quote or s.get("status") != "TRADING":
            continue
        if not s.get("isSpotTradingAllowed", True):
            continue
        base = s.get("baseAsset", "")
        if base in STABLE_OR_FIAT or is_leveraged_base(base):
            continue
        out.append({"symbol": s["symbol"], "base": base})
    return out


def perp_onboard_map(info: dict) -> dict[str, int]:
    """exchangeInfo fapi -> {'BTCUSDT': onboard_ms} de perps USDT."""
    out: dict[str, int] = {}
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
            try:
                out[s["symbol"]] = int(s.get("onboardDate", 0))
            except (TypeError, ValueError):
                out[s["symbol"]] = 0
    return out


def first_kline_ms(symbol: str) -> int | None:
    batch = _get_json(SPOT_KLINES, {"symbol": symbol, "interval": "1d",
                                    "startTime": 0, "limit": 1})
    if not batch:
        return None
    return int(batch[0][0])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since", default="2023-01-01", help="solo listings desde esta fecha")
    p.add_argument("--out", required=True)
    p.add_argument("--perps-out", default="", help="guardar exchangeInfo fapi crudo (reuso)")
    args = p.parse_args()
    since_ms = _to_ms(args.since)

    print("Descargando exchangeInfo spot y fapi...", file=sys.stderr)
    spot = filter_spot_symbols(_get_json(SPOT_INFO))
    fapi_raw = _get_json(FAPI_INFO)
    perps = perp_onboard_map(fapi_raw)
    if args.perps_out:
        os.makedirs(os.path.dirname(args.perps_out) or ".", exist_ok=True)
        with open(args.perps_out, "w") as f:
            json.dump(fapi_raw, f)
        print(f"exchangeInfo fapi -> {args.perps_out}", file=sys.stderr)
    print(f"Pares spot candidatos: {len(spot)} | perps USDT: {len(perps)}", file=sys.stderr)

    rows: list[dict] = []
    for i, s in enumerate(spot):
        try:
            ts = first_kline_ms(s["symbol"])
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️ {s['symbol']}: {e}", file=sys.stderr)
            continue
        if ts is None or ts < since_ms:
            continue
        perp = s["base"] + "USDT"
        rows.append({
            "symbol": perp,                      # el short se ejecuta en el perp
            "spot_symbol": s["symbol"],
            "timestamp": ts,                     # primera vela spot = evento
            "has_perp": int(perp in perps),
            "perp_onboard": perps.get(perp, ""),
        })
        if i % 50 == 0:
            print(f"  {i}/{len(spot)} pares ({len(rows)} listings)...", file=sys.stderr)
        time.sleep(0.15)

    rows.sort(key=lambda r: r["timestamp"])
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    cols = ["symbol", "spot_symbol", "timestamp", "has_perp", "perp_onboard"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    n_perp = sum(r["has_perp"] for r in rows)
    print(f"OK: {len(rows)} listings desde {args.since} ({n_perp} con perp) -> {args.out}",
          file=sys.stderr)
    print("   (solo símbolos VIVOS hoy: los delistados faltan → el fade medido es un PISO)",
          file=sys.stderr)
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
