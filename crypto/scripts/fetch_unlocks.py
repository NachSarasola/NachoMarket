#!/usr/bin/env python3
"""Descarga el calendario HISTÓRICO de unlocks (cliffs) desde DeFiLlama a CSV de eventos.

Fuente gratis: https://api.llama.fi/emissions (índice) + /emission/{protocol} (cronograma
completo, pasado incluido). Se filtran eventos tipo CLIFF y se calcula el % del supply
usando max_supply (tokenomics estática → sin lookahead; SUBESTIMA el % vs circulante →
umbral conservador, pre-registrado).

Mapeo a perps: symbol.upper()+"USDT" contra el exchangeInfo de Binance USDⓈ-M (se pasa con
--perps-json, generado por fetch_listings.py --perps-out, o se descarga acá).

La forma exacta del JSON de DeFiLlama puede variar; el parser es tolerante y ante un
protocolo ilegible lo salta y lo cuenta. Para inspeccionar la forma real:
    python crypto/scripts/fetch_unlocks.py --dump-raw aptos --out /tmp/x.csv

Uso (VPS):
    python crypto/scripts/fetch_unlocks.py --perps-json crypto/data/events/perps.json \
        --min-pct 1.0 --out crypto/data/events/unlocks.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time

LLAMA_INDEX = "https://api.llama.fi/emissions"
LLAMA_ONE = "https://api.llama.fi/emission/{slug}"
FAPI_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"


def _get_json(url: str, timeout: int = 30):
    import requests  # diferido

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _maybe_unwrap_body(payload):
    """Algunos endpoints de Llama devuelven {'body': '<json-string>'} — desanidar."""
    if isinstance(payload, dict) and isinstance(payload.get("body"), str):
        try:
            return json.loads(payload["body"])
        except json.JSONDecodeError:
            return payload
    return payload


def _first(d: dict, keys: tuple[str, ...]):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return None


def parse_emission(payload, protocol: str) -> list[dict]:
    """Extrae eventos cliff de un payload de /emission/{slug}. Tolerante a variantes.

    Devuelve [{protocol, symbol, timestamp(ms), tokens, pct_supply|None, pct_basis,
    category}]. pct_supply = tokens / max_supply * 100 (None si no hay max_supply).
    """
    payload = _maybe_unwrap_body(payload)
    if not isinstance(payload, dict):
        return []
    meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    symbol = _first(payload, ("symbol", "tokenSymbol")) or _first(meta, ("symbol", "tokenSymbol")) or ""
    max_supply = _first(payload, ("maxSupply",)) or _first(meta, ("maxSupply", "totalSupply"))
    try:
        max_supply = float(max_supply) if max_supply is not None else None
    except (TypeError, ValueError):
        max_supply = None

    events = payload.get("events") or meta.get("events") or []
    out: list[dict] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        ts = _first(ev, ("timestamp", "date"))
        toks = ev.get("noOfTokens")
        if isinstance(toks, list):
            toks = sum(float(t) for t in toks if t is not None)
        try:
            ts = int(float(ts))
            toks = float(toks)
        except (TypeError, ValueError):
            continue
        kind = str(_first(ev, ("unlockType", "category", "description")) or "").lower()
        if "cliff" not in kind:
            continue
        if toks <= 0:
            continue
        pct = round(toks / max_supply * 100.0, 4) if max_supply else None
        out.append({
            "protocol": protocol,
            "symbol": str(symbol).upper(),
            "timestamp": ts * 1000 if ts < 10**12 else ts,  # segundos -> ms
            "tokens": toks,
            "pct_supply": pct,
            "pct_basis": "max_supply" if max_supply else "desconocido",
            "category": kind[:40],
        })
    return out


def perp_symbols_from_info(info: dict) -> set[str]:
    """exchangeInfo de fapi -> set de símbolos PERPETUAL activos ('BTCUSDT', ...)."""
    out = set()
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
            out.add(s["symbol"])
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", required=True)
    p.add_argument("--perps-json", default="", help="exchangeInfo fapi cacheado (JSON)")
    p.add_argument("--min-pct", type=float, default=1.0,
                   help="pre-filtro de % supply (1.0 cubre la variante; el gate congelado usa 2.0)")
    p.add_argument("--limit", type=int, default=0, help="máx protocolos (0 = todos)")
    p.add_argument("--dump-raw", default="", help="slug: guardar el JSON crudo y salir")
    args = p.parse_args()

    if args.dump_raw:
        payload = _get_json(LLAMA_ONE.format(slug=args.dump_raw))
        path = args.out if args.out.endswith(".json") else args.out + ".raw.json"
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"RAW {args.dump_raw} -> {path}", file=sys.stderr)
        return 0

    if args.perps_json and os.path.exists(args.perps_json):
        with open(args.perps_json) as f:
            perps = perp_symbols_from_info(json.load(f))
    else:
        print("Descargando exchangeInfo de fapi...", file=sys.stderr)
        perps = perp_symbols_from_info(_get_json(FAPI_INFO))
    print(f"Perps USDT activos: {len(perps)}", file=sys.stderr)

    index = _get_json(LLAMA_INDEX)
    if isinstance(index, dict):
        index = index.get("protocols") or index.get("body") or []
    slugs: list[str] = []
    for item in index:
        if isinstance(item, str):
            slugs.append(item)
        elif isinstance(item, dict):
            slug = _first(item, ("gecko_id", "name", "protocol", "slug"))
            if slug:
                slugs.append(str(slug).lower().replace(" ", "-"))
    if args.limit:
        slugs = slugs[: args.limit]
    print(f"Protocolos en el índice: {len(slugs)}", file=sys.stderr)

    rows: list[dict] = []
    unparsed = 0
    unmapped: set[str] = set()
    for i, slug in enumerate(slugs):
        try:
            payload = _get_json(LLAMA_ONE.format(slug=slug))
            evs = parse_emission(payload, slug)
        except Exception:  # noqa: BLE001 — protocolo ilegible: contar y seguir
            unparsed += 1
            continue
        for ev in evs:
            if ev["pct_supply"] is None or ev["pct_supply"] < args.min_pct:
                continue
            perp = ev["symbol"] + "USDT" if ev["symbol"] else ""
            if perp not in perps:
                unmapped.add(ev["symbol"] or slug)
                continue
            ev["symbol"] = perp
            rows.append(ev)
        if i % 25 == 0:
            print(f"  {i}/{len(slugs)} protocolos ({len(rows)} eventos)...", file=sys.stderr)
        time.sleep(0.25)

    rows.sort(key=lambda r: r["timestamp"])
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    cols = ["symbol", "timestamp", "pct_supply", "pct_basis", "tokens", "category", "protocol"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    print(f"OK: {len(rows)} eventos cliff (>= {args.min_pct}% supply, con perp) -> {args.out}",
          file=sys.stderr)
    print(f"   protocolos ilegibles: {unparsed} | símbolos sin perp (descartados): "
          f"{len(unmapped)} — sesgo superviviente/mapeo: CONSERVADOR para el short",
          file=sys.stderr)
    if not rows:
        print("⚠️  0 eventos: inspeccionar la forma del API con --dump-raw <slug>", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
