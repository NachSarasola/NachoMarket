#!/usr/bin/env python3
"""Descarga el calendario HISTÓRICO de unlocks (cliffs) a CSV de eventos — multi-fuente.

2026-07-24: el API clásico (api.llama.fi/emissions) pasó a ser PAGO (HTTP 402). El SITIO
https://defillama.com/unlocks sigue siendo gratuito y embebe los mismos datos (Next.js,
backend abierto: github.com/DefiLlama/emissions-adapters). Fuentes, en orden:

  1. llama-site : scrape del JSON embebido del sitio (gratis, sin key).
  2. llama-api  : API clásico; si existe DEFILLAMA_API_KEY usa pro-api.llama.fi.
  (fallback pre-diseñado si ambas mueren: reconstrucción por saltos de supply circulante —
   requiere registrar el supuesto de conocibilidad ex-ante; NO implementada hasta necesitarla.)

El % del supply usa max_supply (tokenomics estática → sin lookahead; SUBESTIMA vs
circulante → umbral conservador, pre-registrado).

Uso (VPS):
    python crypto/scripts/fetch_unlocks.py --perps-json crypto/data/events/perps.json \
        --min-pct 1.0 --out crypto/data/events/unlocks.csv
    python crypto/scripts/fetch_unlocks.py --probe          # diagnóstico de fuentes
    python crypto/scripts/fetch_unlocks.py --dump-raw aptos --out /tmp/aptos.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time

SITE_INDEX = "https://defillama.com/unlocks"
SITE_PAGE = "https://defillama.com/unlocks/{slug}"
SITE_DATA = "https://defillama.com/_next/data/{build}/unlocks/{slug}.json"
SITE_DATA_Q = "https://defillama.com/_next/data/{build}/unlocks/[protocol].json?protocol={slug}"
API_INDEX = "https://api.llama.fi/emissions"
API_ONE = "https://api.llama.fi/emission/{slug}"
PRO_ONE = "https://pro-api.llama.fi/{key}/api/emission/{slug}"
FAPI_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) nacho-crypto/1.0 research"}


def _get(url: str, timeout: int = 30):
    import requests  # diferido

    r = requests.get(url, timeout=timeout, headers=UA)
    r.raise_for_status()
    return r


def _get_json(url: str, timeout: int = 30):
    return _get(url, timeout).json()


# --------------------------------------------------------------------------- #
# Parsing puro (unit-testeado offline)
# --------------------------------------------------------------------------- #

def _maybe_unwrap_body(payload):
    """Algunos endpoints devuelven {'body': '<json-string>'} — desanidar."""
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


def extract_next_data(html: str) -> dict:
    """Extrae el JSON de <script id="__NEXT_DATA__"> de una página Next.js."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def slugify(name: str) -> str:
    """Convención de slugs de DefiLlama: minúsculas, espacios→guiones, sin raros."""
    s = name.strip().lower().replace(" ", "-").replace("'", "").replace(".", "-")
    s = re.sub(r"[^a-z0-9$.-]", "", s)
    return re.sub(r"-+", "-", s).strip("-")


def _iter_dicts(obj):
    """Recorre recursivamente dicts/listas rindiendo cada dict encontrado."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_dicts(v)


def _is_event_dict(d: dict) -> bool:
    return ("noOfTokens" in d) and (("timestamp" in d) or ("date" in d))


def find_supply_and_symbol(payload) -> tuple[float | None, str]:
    """Busca max_supply y símbolo en CUALQUIER nivel del payload (forma variable)."""
    max_supply = None
    symbol = ""
    for d in _iter_dicts(payload):
        if max_supply is None:
            v = _first(d, ("maxSupply", "totalSupply"))
            try:
                if v is not None:
                    max_supply = float(v)
            except (TypeError, ValueError):
                pass
        if not symbol:
            v = _first(d, ("tokenSymbol", "tSymbol", "symbol"))
            if isinstance(v, str) and 1 <= len(v) <= 12:
                symbol = v
        if max_supply is not None and symbol:
            break
    return max_supply, symbol


def parse_emission(payload, protocol: str) -> list[dict]:
    """Extrae eventos CLIFF de un payload (API clásico o pageProps del sitio).

    Devuelve [{protocol, symbol, timestamp(ms), tokens, pct_supply|None, pct_basis,
    category}]. Tolerante a la forma: escanea recursivamente cualquier lista de eventos.
    """
    payload = _maybe_unwrap_body(payload)
    if not isinstance(payload, (dict, list)):
        return []
    max_supply, symbol = find_supply_and_symbol(payload)

    seen: set[tuple[int, float]] = set()
    out: list[dict] = []
    for d in _iter_dicts(payload):
        if not _is_event_dict(d):
            continue
        ts = _first(d, ("timestamp", "date"))
        toks = d.get("noOfTokens")
        if isinstance(toks, list):
            toks = sum(float(t) for t in toks if t is not None)
        try:
            ts = int(float(ts))
            toks = float(toks)
        except (TypeError, ValueError):
            continue
        kind = str(_first(d, ("unlockType", "category", "description")) or "").lower()
        if "cliff" not in kind:
            continue
        if toks <= 0:
            continue
        ts_ms = ts * 1000 if ts < 10**12 else ts
        key = (ts_ms, toks)
        if key in seen:  # el sitio puede repetir la lista en varios props
            continue
        seen.add(key)
        pct = round(toks / max_supply * 100.0, 4) if max_supply else None
        out.append({
            "protocol": protocol,
            "symbol": str(symbol).upper(),
            "timestamp": ts_ms,
            "tokens": toks,
            "pct_supply": pct,
            "pct_basis": "max_supply" if max_supply else "desconocido",
            "category": kind[:40],
        })
    return out


def extract_index_protocols(next_data: dict) -> list[str]:
    """Del __NEXT_DATA__ del índice /unlocks: nombres de protocolos con datos de emisión."""
    props = (next_data.get("props") or {}).get("pageProps") or {}
    names: list[str] = []
    seen: set[str] = set()
    for d in _iter_dicts(props):
        name = d.get("name")
        if not isinstance(name, str) or not name:
            continue
        looks_protocol = any(k in d for k in ("tSymbol", "tokenSymbol", "symbol",
                                              "maxSupply", "nextEvent", "totalLocked"))
        if looks_protocol and name.lower() not in seen:
            seen.add(name.lower())
            names.append(name)
    return names


def perp_symbols_from_info(info: dict) -> set[str]:
    """exchangeInfo de fapi -> set de símbolos PERPETUAL activos ('BTCUSDT', ...)."""
    out = set()
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
            out.add(s["symbol"])
    return out


# --------------------------------------------------------------------------- #
# Fuentes
# --------------------------------------------------------------------------- #

def site_get_index() -> tuple[str, list[str]]:
    """Índice del sitio: devuelve (buildId, [nombres de protocolo])."""
    html = _get(SITE_INDEX).text
    nd = extract_next_data(html)
    build = nd.get("buildId", "")
    names = extract_index_protocols(nd)
    return build, names


def site_get_protocol(build: str, name: str, cache_dir: str = "") -> dict:
    """pageProps de un protocolo vía ruta de datos de Next (con fallback a la página)."""
    slug = slugify(name)
    if cache_dir:
        cpath = os.path.join(cache_dir, f"{slug}.json")
        if os.path.exists(cpath):
            with open(cpath) as f:
                return json.load(f)
    payload: dict = {}
    if build:
        for url in (SITE_DATA.format(build=build, slug=slug),
                    SITE_DATA_Q.format(build=build, slug=slug)):
            try:
                payload = _get_json(url)
                break
            except Exception:  # noqa: BLE001 — probar la siguiente forma
                continue
    if not payload:
        html = _get(SITE_PAGE.format(slug=slug)).text
        payload = extract_next_data(html).get("props", {})
    if cache_dir and payload:
        os.makedirs(cache_dir, exist_ok=True)
        with open(os.path.join(cache_dir, f"{slug}.json"), "w") as f:
            json.dump(payload, f)
    return payload


def api_get_index() -> list[str]:
    index = _maybe_unwrap_body(_get_json(API_INDEX))
    if isinstance(index, dict):
        index = index.get("protocols") or []
    slugs = []
    for item in index:
        if isinstance(item, str):
            slugs.append(item)
        elif isinstance(item, dict):
            s = _first(item, ("gecko_id", "name", "protocol", "slug"))
            if s:
                slugs.append(slugify(str(s)))
    return slugs


def api_get_protocol(slug: str) -> dict:
    key = os.environ.get("DEFILLAMA_API_KEY", "")
    url = PRO_ONE.format(key=key, slug=slug) if key else API_ONE.format(slug=slug)
    return _get_json(url)


def probe() -> int:
    """Diagnóstico de una pasada: estado de cada fuente + muestras. Pegar la salida."""
    import requests  # diferido

    checks = [
        ("site index", SITE_INDEX),
        ("api index", API_INDEX),
        ("api one", API_ONE.format(slug="aptos")),
    ]
    build = ""
    for label, url in checks:
        try:
            r = requests.get(url, timeout=30, headers=UA)
            body = r.text[:200].replace("\n", " ")
            print(f"[{label}] {r.status_code} {url}\n    {body}")
            if label == "site index" and r.ok:
                nd = extract_next_data(r.text)
                build = nd.get("buildId", "")
                names = extract_index_protocols(nd)
                print(f"    buildId={build!r} | protocolos detectados={len(names)} "
                      f"| muestra={names[:5]}")
        except Exception as e:  # noqa: BLE001
            print(f"[{label}] EXCEPCION {url}: {e}")
    if build:
        for form, url in (("next-data", SITE_DATA.format(build=build, slug="aptos")),
                          ("next-data-q", SITE_DATA_Q.format(build=build, slug="aptos"))):
            try:
                r = requests.get(url, timeout=30, headers=UA)
                print(f"[{form}] {r.status_code} {url}\n    {r.text[:200]}")
            except Exception as e:  # noqa: BLE001
                print(f"[{form}] EXCEPCION: {e}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="crypto/data/events/unlocks.csv")
    p.add_argument("--perps-json", default="", help="exchangeInfo fapi cacheado (JSON)")
    p.add_argument("--min-pct", type=float, default=1.0,
                   help="pre-filtro de % supply (1.0 cubre la variante; el gate congelado usa 2.0)")
    p.add_argument("--limit", type=int, default=0, help="máx protocolos (0 = todos)")
    p.add_argument("--source", choices=["auto", "llama-site", "llama-api"], default="auto")
    p.add_argument("--cache-dir", default="crypto/data/events/llama_cache")
    p.add_argument("--dump-raw", default="", help="slug/nombre: guardar el JSON crudo y salir")
    p.add_argument("--probe", action="store_true", help="diagnóstico de fuentes y salir")
    args = p.parse_args()

    if args.probe:
        return probe()

    if args.dump_raw:
        try:
            build, _ = site_get_index()
            payload = site_get_protocol(build, args.dump_raw)
            src = "llama-site"
        except Exception as e:  # noqa: BLE001
            print(f"(sitio fallo: {e}; pruebo API clásico)", file=sys.stderr)
            payload = api_get_protocol(slugify(args.dump_raw))
            src = "llama-api"
        path = args.out if args.out.endswith(".json") else args.out + ".raw.json"
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"RAW {args.dump_raw} ({src}) -> {path}", file=sys.stderr)
        return 0

    if args.perps_json and os.path.exists(args.perps_json):
        with open(args.perps_json) as f:
            perps = perp_symbols_from_info(json.load(f))
    else:
        print("Descargando exchangeInfo de fapi...", file=sys.stderr)
        perps = perp_symbols_from_info(_get_json(FAPI_INFO))
    print(f"Perps USDT activos: {len(perps)}", file=sys.stderr)

    # --- elegir fuente ---
    build = ""
    names: list[str] = []
    source = args.source
    if source in ("auto", "llama-site"):
        try:
            build, names = site_get_index()
            if names:
                source = "llama-site"
                print(f"Fuente: sitio gratuito (buildId={build!r}, "
                      f"{len(names)} protocolos)", file=sys.stderr)
            elif args.source == "llama-site":
                print("❌ sitio sin protocolos detectables; correr --probe y pegar salida",
                      file=sys.stderr)
                return 1
        except Exception as e:  # noqa: BLE001
            if args.source == "llama-site":
                print(f"❌ sitio inaccesible: {e}", file=sys.stderr)
                return 1
            print(f"(sitio fallo: {e})", file=sys.stderr)
    if source in ("auto", "llama-api") and not names:
        try:
            names = api_get_index()
            source = "llama-api"
            print(f"Fuente: API clásico ({len(names)} protocolos)", file=sys.stderr)
        except Exception as e:  # noqa: BLE001
            print(f"❌ Ninguna fuente disponible (API: {e}).", file=sys.stderr)
            print("   Diagnóstico: python crypto/scripts/fetch_unlocks.py --probe",
                  file=sys.stderr)
            return 1

    if args.limit:
        names = names[: args.limit]

    rows: list[dict] = []
    unparsed = 0
    unmapped: set[str] = set()
    for i, name in enumerate(names):
        try:
            if source == "llama-site":
                payload = site_get_protocol(build, name, cache_dir=args.cache_dir)
            else:
                payload = api_get_protocol(slugify(name))
            evs = parse_emission(payload, slugify(name))
        except Exception:  # noqa: BLE001 — protocolo ilegible: contar y seguir
            unparsed += 1
            continue
        for ev in evs:
            if ev["pct_supply"] is None or ev["pct_supply"] < args.min_pct:
                continue
            perp = ev["symbol"] + "USDT" if ev["symbol"] else ""
            if perp not in perps:
                unmapped.add(ev["symbol"] or slugify(name))
                continue
            ev["symbol"] = perp
            rows.append(ev)
        if i % 25 == 0:
            print(f"  {i}/{len(names)} protocolos ({len(rows)} eventos)...", file=sys.stderr)
        time.sleep(0.25)

    rows.sort(key=lambda r: r["timestamp"])
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    cols = ["symbol", "timestamp", "pct_supply", "pct_basis", "tokens", "category", "protocol"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    print(f"OK ({source}): {len(rows)} eventos cliff (>= {args.min_pct}% supply, con perp) "
          f"-> {args.out}", file=sys.stderr)
    print(f"   protocolos ilegibles: {unparsed} | símbolos sin perp (descartados): "
          f"{len(unmapped)} — sesgo superviviente/mapeo: CONSERVADOR para el short",
          file=sys.stderr)
    if not rows:
        print("⚠️  0 eventos: correr --probe y pegar la salida.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
