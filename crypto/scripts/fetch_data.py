#!/usr/bin/env python3
"""Descarga OHLCV historico a CSV para el backtest/validacion.

Uso (en una maquina con acceso a exchanges, p.ej. el VPS):

    python crypto/scripts/fetch_data.py --exchange binance --symbol BTC/USDT \
        --timeframe 4h --since 2019-01-01 --out crypto/data/BTC_USDT-4h.csv

Requiere ``ccxt`` (``pip install ccxt``). Pagina automaticamente. Alternativa
oficial de freqtrade (equivalente):

    freqtrade download-data -c crypto/config-backtest.json \
        --timerange 20190101- --timeframes 4h 1d

NOTA: en el entorno de desarrollo de Claude los exchanges estan bloqueados por el
proxy; este script esta pensado para correrse donde haya acceso de red a mercados.
El CSV resultante lo consume ``crypto/scripts/validate.py --data <csv>``.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone


def _to_ms(date_str: str) -> int:
    dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch(exchange_id: str, symbol: str, timeframe: str, since_ms: int) -> list[list[float]]:
    import ccxt  # import diferido: solo necesario al correr el fetch

    ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
    tf_ms = ex.parse_timeframe(timeframe) * 1000
    all_rows: list[list[float]] = []
    cursor = since_ms
    now = ex.milliseconds()
    while cursor < now:
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=1000)
        if not batch:
            break
        all_rows.extend(batch)
        cursor = batch[-1][0] + tf_ms
        print(f"  {ex.iso8601(batch[-1][0])}  ({len(all_rows)} velas)", file=sys.stderr)
        time.sleep(ex.rateLimit / 1000)
        if len(batch) < 2:
            break
    # Dedup por timestamp preservando orden.
    seen = set()
    out = []
    for r in all_rows:
        if r[0] not in seen:
            seen.add(r[0])
            out.append(r)
    return out


_BINANCE_TF_MS = {"1h": 3600_000, "4h": 4 * 3600_000, "1d": 24 * 3600_000}


def fetch_binance_raw(symbol: str, timeframe: str, since_ms: int) -> list[list[float]]:
    """Klines crudas de Binance spot (sin ccxt): incluye taker_buy_volume (campo 9).

    Es el proxy de order flow GRATIS con historia completa: cuanto del volumen de cada
    vela fue agresion compradora. ccxt no lo expone en fetch_ohlcv; el endpoint publico si.
    Devuelve filas [ts, open, high, low, close, volume, taker_buy_volume].
    """
    import requests  # import diferido

    tf_ms = _BINANCE_TF_MS[timeframe]
    sym = symbol.replace("/", "")
    url = "https://api.binance.com/api/v3/klines"
    out: list[list[float]] = []
    cursor = since_ms
    while True:
        resp = requests.get(url, params={
            "symbol": sym, "interval": timeframe, "startTime": cursor, "limit": 1000,
        }, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for k in batch:
            # k: [openTime, open, high, low, close, volume, closeTime, quoteVol,
            #     nTrades, takerBuyBaseVol, takerBuyQuoteVol, ignore]
            out.append([int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                        float(k[5]), float(k[9])])
        cursor = batch[-1][0] + tf_ms
        print(f"  {len(out)} velas...", file=sys.stderr)
        time.sleep(0.35)  # rate limit amable
        if len(batch) < 1000:
            break
    # dedup preservando orden
    seen: set[int] = set()
    dedup = []
    for r in out:
        if r[0] not in seen:
            seen.add(r[0])
            dedup.append(r)
    return dedup


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--exchange", default="binance")
    p.add_argument("--symbol", default="BTC/USDT")
    p.add_argument("--timeframe", default="4h")
    p.add_argument("--since", default="2019-01-01")
    p.add_argument("--out", required=True)
    p.add_argument("--with-flow", action="store_true",
                   help="usar el endpoint raw de Binance e incluir taker_buy_volume (order flow)")
    args = p.parse_args()

    print(f"Descargando {args.symbol} {args.timeframe} desde {args.since} "
          f"({'binance-raw+flow' if args.with_flow else args.exchange})...", file=sys.stderr)
    if args.with_flow:
        if args.timeframe not in _BINANCE_TF_MS:
            print(f"--with-flow soporta timeframes {list(_BINANCE_TF_MS)}", file=sys.stderr)
            return 1
        rows = fetch_binance_raw(args.symbol, args.timeframe, _to_ms(args.since))
        header = ["timestamp", "open", "high", "low", "close", "volume", "taker_buy_volume"]
    else:
        rows = fetch(args.exchange, args.symbol, args.timeframe, _to_ms(args.since))
        header = ["timestamp", "open", "high", "low", "close", "volume"]
    if not rows:
        print("Sin datos.", file=sys.stderr)
        return 1

    import csv
    import os
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
    print(f"OK: {len(rows)} velas -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
