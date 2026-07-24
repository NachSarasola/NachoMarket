#!/usr/bin/env python3
"""Descarga la historia COMPLETA de funding rate de Binance perps (gratis) a CSV.

El funding es uno de los pocos "footprints institucionales" con historia completa gratuita:
funding extremo = posicionamiento apalancado extremo (input de la hipótesis H3: squeezes /
mean-reversion). Endpoint público /fapi/v1/fundingRate, sin API key, 1000 filas por página
(cada 8h → ~3 páginas por año).

Uso (en el VPS):
    python crypto/scripts/fetch_funding.py --symbol BTCUSDT --since 2019-09-01 \
        --out crypto/data/BTCUSDT-funding.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone


def _to_ms(date_str: str) -> int:
    dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_funding(symbol: str, since_ms: int) -> list[tuple[int, float]]:
    """Pagina /fapi/v1/fundingRate desde ``since_ms`` hasta hoy. [(ts, rate), ...]."""
    import requests  # diferido

    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    out: list[tuple[int, float]] = []
    cursor = since_ms
    while True:
        resp = requests.get(url, params={
            "symbol": symbol, "startTime": cursor, "limit": 1000,
        }, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for row in batch:
            out.append((int(row["fundingTime"]), float(row["fundingRate"])))
        cursor = out[-1][0] + 1
        print(f"  {len(out)} registros...", file=sys.stderr)
        time.sleep(0.35)
        if len(batch) < 1000:
            break
    # dedup por timestamp
    seen: set[int] = set()
    dedup = []
    for ts, r in out:
        if ts not in seen:
            seen.add(ts)
            dedup.append((ts, r))
    return dedup


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", default="BTCUSDT", help="símbolo de perps (sin barra)")
    p.add_argument("--since", default="2019-09-01", help="inicio (los perps de Binance nacen 2019-09)")
    p.add_argument("--out", required=True)
    args = p.parse_args()

    print(f"Descargando funding de {args.symbol} desde {args.since}...", file=sys.stderr)
    rows = fetch_funding(args.symbol, _to_ms(args.since))
    if not rows:
        print("Sin datos.", file=sys.stderr)
        return 1
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "funding_rate"])
        for ts, r in rows:
            w.writerow([ts, r])
    ann = sum(r for _, r in rows[-90:]) / max(1, len(rows[-90:])) * 3 * 365 * 100
    print(f"OK: {len(rows)} registros -> {args.out} (funding medio 30d anualizado ~{ann:.1f}%)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
