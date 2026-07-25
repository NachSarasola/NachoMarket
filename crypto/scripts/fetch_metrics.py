#!/usr/bin/env python3
"""Descarga OPEN INTEREST histórico 5m de Binance perps (dumps públicos) a CSV.

El API vivo de OI (/futures/data/openInterestHist) solo da 30 días; la historia completa
vive en los dumps mensuales públicos: data.binance.vision → futures/um/monthly/metrics.
Cada ZIP trae un CSV 5-minutal con sum_open_interest (desde ~dic-2021). Es EL dato de H9
(cascadas): la purga de OI es la huella de las liquidaciones forzadas.

Uso (VPS):
    python crypto/scripts/fetch_metrics.py --symbol BTCUSDT --since 2021-12 \
        --out crypto/data/cascade/BTCUSDT-oi.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
import zipfile
from datetime import datetime, timezone

MONTHLY = "https://data.binance.vision/data/futures/um/monthly/metrics/{sym}/{sym}-metrics-{ym}.zip"
DAILY = "https://data.binance.vision/data/futures/um/daily/metrics/{sym}/{sym}-metrics-{ymd}.zip"


def month_range(since_ym: str) -> list[str]:
    """['2021-12', '2022-01', ..., mes pasado] (el mes corriente no tiene dump aún)."""
    y, m = (int(x) for x in since_ym.split("-"))
    now = datetime.now(timezone.utc)
    out: list[str] = []
    while (y, m) < (now.year, now.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def days_in_month(ym: str) -> list[str]:
    """['2024-02-01', ..., '2024-02-29'] — días de calendario del mes."""
    import calendar as _cal

    y, m = (int(x) for x in ym.split("-"))
    return [f"{y:04d}-{m:02d}-{d:02d}" for d in range(1, _cal.monthrange(y, m)[1] + 1)]


def parse_metrics_csv(text: str) -> list[tuple[int, float]]:
    """CSV del dump -> [(ts_ms, open_interest)]. Tolerante a columnas extra/orden."""
    rows: list[tuple[int, float]] = []
    reader = csv.DictReader(io.StringIO(text))
    for r in reader:
        t = (r.get("create_time") or "").strip()
        oi = r.get("sum_open_interest")
        if not t or oi in (None, ""):
            continue
        try:
            dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            rows.append((int(dt.timestamp() * 1000), float(oi)))
        except ValueError:
            continue
    return rows


def _fetch_zip_rows(session, url: str) -> list[tuple[int, float]] | None:
    """Baja un ZIP y parsea su CSV; None si no existe (404)."""
    r = session.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        name = z.namelist()[0]
        return parse_metrics_csv(z.read(name).decode())


def fetch_month(session, sym: str, ym: str) -> tuple[list[tuple[int, float]], int]:
    """Un mes de metrics: intenta el ZIP mensual y cae a los ZIPs DIARIOS.

    (Binance Vision publica 'metrics' principalmente como dumps diarios; el intento
    mensual queda por si algún día existe.) Devuelve (filas, días_faltantes).
    """
    rows = _fetch_zip_rows(session, MONTHLY.format(sym=sym, ym=ym))
    if rows is not None:
        return rows, 0
    out: list[tuple[int, float]] = []
    missing_days = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for ymd in days_in_month(ym):
        if ymd >= today:
            break
        try:
            day = _fetch_zip_rows(session, DAILY.format(sym=sym, ymd=ymd))
        except Exception:  # noqa: BLE001 — día puntual ilegible: contar y seguir
            day = None
        if day is None:
            missing_days += 1
        else:
            out.extend(day)
        time.sleep(0.08)
    return out, missing_days


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", required=True, help="perp, p.ej. BTCUSDT")
    p.add_argument("--since", default="2021-12", help="YYYY-MM (los dumps arrancan ~2021-12)")
    p.add_argument("--out", required=True)
    args = p.parse_args()

    import requests  # diferido

    session = requests.Session()
    session.headers["User-Agent"] = "nacho-crypto/1.0"
    months = month_range(args.since)
    all_rows: list[tuple[int, float]] = []
    missing = 0
    for ym in months:
        try:
            rows, missing_days = fetch_month(session, args.symbol, ym)
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️ {ym}: {e}", file=sys.stderr)
            missing += 1
            continue
        missing += missing_days
        all_rows.extend(rows)
        print(f"  {ym}: {len(rows)} obs ({len(all_rows)} total; "
              f"dias sin dump: {missing_days})", file=sys.stderr)

    seen: set[int] = set()
    dedup = []
    for ts, oi in sorted(all_rows):
        if ts not in seen:
            seen.add(ts)
            dedup.append((ts, oi))
    if not dedup:
        print(f"Sin datos de metrics para {args.symbol}.", file=sys.stderr)
        return 1
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open_interest"])
        for ts, oi in dedup:
            w.writerow([ts, oi])
    print(f"OK: {len(dedup)} obs 5m -> {args.out} (dumps faltantes: {missing})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
