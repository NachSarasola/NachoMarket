#!/usr/bin/env python3
"""Calendario HISTÓRICO de unlocks (cliffs) a CSV de eventos — multi-fuente.

Estado de las puertas (verificado en el VPS 2026-07-24): el API clásico
(api.llama.fi/emissions) es PAGO (402) y el sitio defillama.com está tras challenge de
Cloudflare (403) — no se elude: vamos a la FUENTE. Los cronogramas de vesting que alimentan
ese dashboard son open-source: github.com/DefiLlama/emissions-adapters (archivos TS
declarativos por protocolo). Fuentes, en orden:

  1. adapters   : parsear un clone local del repo emissions-adapters (fuente de verdad,
                  sin scraping). `git clone --depth 1` + `--adapters-dir`.
  2. llama-site : JSON embebido del sitio (por si el challenge se levanta).
  3. llama-api  : API clásico; con DEFILLAMA_API_KEY usa pro-api.llama.fi.

En la fuente `adapters` el % del supply usa la SUMA de todas las secciones declaradas del
protocolo (= max supply documentado; estático → sin lookahead). Supuestos registrados:
`manualStep` genera un evento por escalón en start+k·duración (k=1..steps, categoría
"step"); protocolos con helpers no parseables quedan marcados `pct_basis=sum_parcial`.

Uso (VPS):
    git clone --depth 1 https://github.com/DefiLlama/emissions-adapters <dir>
    python crypto/scripts/fetch_unlocks.py --source adapters --adapters-dir <dir> \
        --perps-json crypto/data/events/perps.json --min-pct 1.0 \
        --out crypto/data/events/unlocks.csv
    python crypto/scripts/fetch_unlocks.py --probe          # diagnóstico de fuentes web
"""

from __future__ import annotations

import argparse
import ast
import calendar
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

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


def _write_rows(out_path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    cols = ["symbol", "timestamp", "pct_supply", "pct_basis", "tokens", "category", "protocol"]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})


# --------------------------------------------------------------------------- #
# Fuente 1: repo open-source emissions-adapters (parser TS declarativo, testeado offline)
# --------------------------------------------------------------------------- #

PERIOD_DEFAULTS = {"hour": 3600, "day": 86400, "week": 604800,
                   "month": 2628000, "year": 31536000}


def strip_ts_comments(text: str) -> str:
    """Quita comentarios // y /* */ SIN romper strings (las URLs contienen '//')."""
    out: list[str] = []
    i, n = 0, len(text)
    in_str: str | None = None
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if c == in_str:
                in_str = None
            i += 1
            continue
        if c in ("'", '"', "`"):
            in_str = c
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            i += 2
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def parse_period_seconds(repo_dir: str) -> dict[str, float]:
    """Lee periodToSeconds del propio repo clonado (no confiar en memoria)."""
    for path in Path(repo_dir).rglob("*.ts"):
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        if "periodToSeconds" not in text or "=" not in text:
            continue
        m = re.search(r"periodToSeconds[^=]*=\s*\{([^}]*)\}", text, re.DOTALL)
        if not m:
            continue
        vals: dict[str, float] = {}
        for k, v in re.findall(r"(\w+)\s*:\s*([\d_.e+*\s]+)", m.group(1)):
            try:
                vals[k] = float(eval_expr(v, {}))
            except Exception:  # noqa: BLE001
                continue
        if {"day", "month", "year"} <= set(vals):
            return vals
    return dict(PERIOD_DEFAULTS)


_ALLOWED_NODES = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant, ast.Name,
                  ast.Load, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.USub, ast.UAdd,
                  ast.Pow)


def eval_expr(expr: str, env: dict[str, float]) -> float:
    """Evalúa aritmética TS simple de forma SEGURA (sin llamadas ni atributos).

    Soporta: números con '_' (1_000_000), notación 1e9, + - * / **, paréntesis,
    identificadores del env y periodToSeconds.X (sustituido antes del parseo).
    """
    s = expr.strip().rstrip(",;")
    s = re.sub(r"periodToSeconds\.(\w+)",
               lambda m: str(env.get(f"__period_{m.group(1)}", float("nan"))), s)
    s = re.sub(r"(?<=\d)_(?=\d)", "", s)
    tree = ast.parse(s, mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(f"nodo no permitido: {type(node).__name__} en {expr!r}")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float)):
            raise ValueError(f"constante no numérica en {expr!r}")

    def _ev(n) -> float:
        if isinstance(n, ast.Expression):
            return _ev(n.body)
        if isinstance(n, ast.Constant):
            return float(n.value)
        if isinstance(n, ast.Name):
            if n.id in env:
                return float(env[n.id])
            raise ValueError(f"identificador desconocido: {n.id}")
        if isinstance(n, ast.UnaryOp):
            v = _ev(n.operand)
            return -v if isinstance(n.op, ast.USub) else v
        if isinstance(n, ast.BinOp):
            a, b = _ev(n.left), _ev(n.right)
            if isinstance(n.op, ast.Add):
                return a + b
            if isinstance(n.op, ast.Sub):
                return a - b
            if isinstance(n.op, ast.Mult):
                return a * b
            if isinstance(n.op, ast.Div):
                return a / b
            if isinstance(n.op, ast.Pow):
                return a ** b
        raise ValueError("expresión no soportada")

    return _ev(tree)


def _parse_ts_date(token: str) -> float | None:
    """'2023-11-12' / '2023-11-12T08:00:00Z' (con o sin comillas) -> unix segundos UTC."""
    t = token.strip().strip("'\"`")
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?Z?)?$", t)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hh = int(m.group(4) or 0)
    mm = int(m.group(5) or 0)
    ss = int(m.group(6) or 0)
    return float(calendar.timegm((y, mo, d, hh, mm, ss)))


def _eval_time_arg(token: str, env: dict[str, float]) -> float:
    ts = _parse_ts_date(token)
    if ts is not None:
        return ts
    return eval_expr(token, env)


def find_calls(text: str, func: str):
    """Encuentra llamadas ``func(...)`` con paréntesis balanceados; rinde la lista de args."""
    for m in re.finditer(rf"\b{func}\s*\(", text):
        i = m.end()
        depth = 1
        start = i
        while i < len(text) and depth:
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
            i += 1
        if depth:
            continue
        inner = text[start: i - 1]
        args: list[str] = []
        buf: list[str] = []
        d = 0
        for ch in inner:
            if ch in "([{":
                d += 1
            elif ch in ")]}":
                d -= 1
            if ch == "," and d == 0:
                args.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            args.append("".join(buf).strip())
        yield args


def parse_adapter_file(text: str, protocol: str, periods: dict[str, float]) -> dict:
    """Parsea UN adapter TS declarativo. Devuelve eventos cliff/step + total declarado.

    {'protocol', 'gecko_id', 'events': [{'ts', 'tokens', 'category'}], 'total': float,
     'incomplete': bool}. Los helpers no-manuales (lecturas on-chain) no se computan:
    si existen, incomplete=True (el % queda sobre suma parcial → se marca en el CSV).
    """
    src = strip_ts_comments(text)
    env: dict[str, float] = {f"__period_{k}": v for k, v in periods.items()}

    for name, expr in re.findall(r"\bconst\s+([A-Za-z_$][\w$]*)\s*=\s*([^;\n]+)", src):
        try:
            env[name] = eval_expr(expr, env)
        except Exception:  # noqa: BLE001 — const no numérica (objetos, strings): ignorar
            continue

    events: list[dict] = []
    total = 0.0
    ok = True

    for args in find_calls(src, "manualCliff"):
        if len(args) < 2:
            ok = False
            continue
        try:
            ts = _eval_time_arg(args[0], env)
            amt = eval_expr(args[1], env)
        except Exception:  # noqa: BLE001
            ok = False
            continue
        if amt > 0:
            total += amt
            events.append({"ts": ts, "tokens": amt, "category": "cliff"})

    for args in find_calls(src, "manualLinear"):
        if len(args) < 3:
            ok = False
            continue
        try:
            amt = eval_expr(args[2], env)
        except Exception:  # noqa: BLE001
            ok = False
            continue
        if amt > 0:
            total += amt  # vesting continuo: cuenta para el supply, no genera evento

    for args in find_calls(src, "manualStep"):
        if len(args) < 4:
            ok = False
            continue
        try:
            start = _eval_time_arg(args[0], env)
            dur = eval_expr(args[1], env)
            steps = int(eval_expr(args[2], env))
            amt = eval_expr(args[3], env)
        except Exception:  # noqa: BLE001
            ok = False
            continue
        if amt > 0 and steps > 0:
            total += steps * amt
            for k in range(1, steps + 1):
                events.append({"ts": start + k * dur, "tokens": amt, "category": "step"})

    known = {"Cliff", "Linear", "Step"}
    others = set(re.findall(r"\bmanual([A-Za-z]+)\s*\(", src)) - known
    incomplete = bool(others) or not ok

    gecko = ""
    m = re.search(r"token:\s*[\"']coingecko:([^\"']+)[\"']", src)
    if m:
        gecko = m.group(1)
    return {"protocol": protocol, "gecko_id": gecko, "events": events,
            "total": total, "incomplete": incomplete}


def adapter_events_to_rows(parsed: dict, symbol: str) -> list[dict]:
    """Agrega eventos del MISMO timestamp (un unlock por día) y calcula % del total."""
    total = parsed["total"]
    if total <= 0:
        return []
    basis = "sum_parcial" if parsed["incomplete"] else "sum_secciones"
    by_ts: dict[int, dict] = {}
    for ev in parsed["events"]:
        ts_ms = int(ev["ts"]) * 1000 if ev["ts"] < 10**12 else int(ev["ts"])
        cur = by_ts.setdefault(ts_ms, {"tokens": 0.0, "cats": set()})
        cur["tokens"] += ev["tokens"]
        cur["cats"].add(ev["category"])
    rows = []
    for ts_ms, cur in sorted(by_ts.items()):
        rows.append({
            "protocol": parsed["protocol"],
            "symbol": symbol,
            "timestamp": ts_ms,
            "tokens": cur["tokens"],
            "pct_supply": round(cur["tokens"] / total * 100.0, 4),
            "pct_basis": basis,
            "category": "+".join(sorted(cur["cats"])),
        })
    return rows


GECKO_BASE = "https://api.coingecko.com/api/v3"
GECKO_LIST = GECKO_BASE + "/coins/list"


def _cg_get(path_or_url: str, cache_path: str = "", max_retries: int = 4):
    """GET a CoinGecko con demo key opcional, backoff en 429 y cache en disco."""
    if cache_path and os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)
    import requests  # diferido

    url = path_or_url if path_or_url.startswith("http") else GECKO_BASE + path_or_url
    headers = dict(UA)
    key = os.environ.get("COINGECKO_API_KEY", "")
    if key:
        headers["x-cg-demo-api-key"] = key
    for attempt in range(max_retries):
        r = requests.get(url, timeout=60, headers=headers)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "65") or 65)
            print(f"  (rate limit CoinGecko: espero {wait}s)", file=sys.stderr)
            time.sleep(min(wait, 120))
            continue
        r.raise_for_status()
        data = r.json()
        if cache_path:
            os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(data, f)
        return data
    raise RuntimeError(f"CoinGecko agoto reintentos: {url}")


def load_gecko_list(cache_dir: str) -> list[dict]:
    """coins/list crudo (id, symbol, name), cacheado."""
    return _cg_get("/coins/list", os.path.join(cache_dir, "gecko_list.json"))


def load_gecko_symbol_map(cache_path: str) -> dict[str, str]:
    """gecko_id -> SYMBOL (ticker). Cachea; usa COINGECKO_API_KEY (demo) si existe."""
    if cache_path and os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)
    data = _cg_get("/coins/list")
    out = {c["id"]: str(c.get("symbol", "")).upper() for c in data if c.get("id")}
    if cache_path:
        os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(out, f)
    return out


# --------------------------------------------------------------------------- #
# Fuente PLAN C: reconstrucción de cliffs por SALTOS de supply circulante
# --------------------------------------------------------------------------- #
# circulante_t = market_cap_t / price_t (diario, CoinGecko). Un cliff contractual aparece
# como salto discreto y PERSISTENTE del circulante. Supuestos PRE-REGISTRADOS (REGLAS):
# (1) conocibilidad ex-ante: los cliffs de vesting son contractuales y públicos desde el
#     TGE → un salto realizado era conocible antes (válido para cliffs; los mints no
#     programados generan falsos eventos que DILUYEN el efecto → conservador);
# (2) timestamp = punto medio entre las dos muestras diarias (blur ±12h);
# (3) filtros de MEDICIÓN congelados: persistencia 0.7, ruido 0.2, separación 5 días.

STEP_PERSIST_FRAC = 0.7
STEP_NOISE_FRAC = 0.2
STEP_MIN_SEP_DAYS = 5


def circ_series(prices: list[list[float]], mcaps: list[list[float]]) -> list[tuple[int, float]]:
    """Alinea prices y market_caps de market_chart por timestamp -> [(ms, circulante)]."""
    pm = {int(t): float(p) for t, p in prices if p and float(p) > 0}
    out: list[tuple[int, float]] = []
    for t, m in mcaps:
        t = int(t)
        p = pm.get(t)
        if p and m and float(m) > 0:
            out.append((t, float(m) / p))
    out.sort(key=lambda x: x[0])
    return out


def detect_supply_steps(series: list[tuple[int, float]], min_pct: float) -> list[dict]:
    """Saltos discretos y persistentes del circulante (proxy de cliff). Puro y testeado.

    Devuelve [{'ts_ms' (punto medio), 'step_pct'}]. Filtros congelados arriba.
    """
    n = len(series)
    events: list[dict] = []
    for i in range(1, n):
        t1, c1 = series[i]
        t0, c0 = series[i - 1]
        if c0 <= 0:
            continue
        step = c1 / c0 - 1.0
        if step < min_pct / 100.0:
            continue
        prev = [abs(series[j][1] / series[j - 1][1] - 1.0)
                for j in range(max(1, i - 15), i) if series[j - 1][1] > 0]
        if prev and sorted(prev)[len(prev) // 2] > step * STEP_NOISE_FRAC:
            continue  # serie ruidosa: el "salto" no se distingue del ruido de mcap/price
        nxt = [series[j][1] for j in range(i, min(n, i + 3))]
        if not nxt or sorted(nxt)[len(nxt) // 2] < c0 * (1 + STEP_PERSIST_FRAC * step):
            continue  # no persiste: glitch de datos, no un unlock
        events.append({"ts_ms": (t0 + t1) // 2, "step_pct": round(step * 100.0, 4)})
    events.sort(key=lambda e: e["ts_ms"])
    out: list[dict] = []
    for e in events:
        if out and e["ts_ms"] - out[-1]["ts_ms"] < STEP_MIN_SEP_DAYS * 86_400_000:
            if e["step_pct"] > out[-1]["step_pct"]:
                out[-1] = e
        else:
            out.append(e)
    return out


def gecko_candidates(bases: set[str], gecko_list: list[dict]) -> dict[str, list[str]]:
    """SYMBOL base ('APT') -> ids candidatos de CoinGecko (colisiones incluidas)."""
    by_sym: dict[str, list[str]] = {}
    for c in gecko_list:
        sym = str(c.get("symbol", "")).upper()
        if sym in bases and c.get("id"):
            by_sym.setdefault(sym, []).append(c["id"])
    return by_sym


def pick_ids_by_mcap(cands: dict[str, list[str]], markets: list[dict]) -> dict[str, str]:
    """Desambigua colisiones de ticker: gana el id con mayor market cap."""
    mcap = {m.get("id"): (m.get("market_cap") or 0) for m in markets if m.get("id")}
    out: dict[str, str] = {}
    for sym, ids in cands.items():
        best = max(ids, key=lambda i: mcap.get(i, 0))
        if mcap.get(best, 0) > 0:
            out[sym] = best
    return out


def source_supply_step(perps: set[str], min_pct: float, cache_dir: str,
                       since_ms: int = 1672531200000) -> tuple[list[dict], dict]:
    """Plan C completo: perps -> ids CoinGecko -> series de circulante -> saltos."""
    bases = {p[: -len("USDT")] for p in perps if p.endswith("USDT")}
    glist = load_gecko_list(cache_dir)
    cands = gecko_candidates(bases, glist)
    all_ids = sorted({i for ids in cands.values() for i in ids})
    print(f"supply-step: {len(bases)} bases, {len(cands)} con id CoinGecko "
          f"({len(all_ids)} candidatos)", file=sys.stderr)

    markets: list[dict] = []
    for k in range(0, len(all_ids), 200):
        chunk = all_ids[k: k + 200]
        markets += _cg_get(
            "/coins/markets?vs_currency=usd&per_page=200&ids=" + ",".join(chunk),
            os.path.join(cache_dir, f"markets_{k}.json"))
        time.sleep(2.2 if os.environ.get("COINGECKO_API_KEY") else 12.0)
    chosen = pick_ids_by_mcap(cands, markets)
    print(f"supply-step: {len(chosen)} tokens desambiguados por mcap", file=sys.stderr)

    rows: list[dict] = []
    stats = {"tokens": len(chosen), "con_serie": 0, "eventos": 0, "errores": 0}
    charts_dir = os.path.join(cache_dir, "charts")
    for j, (base, cid) in enumerate(sorted(chosen.items())):
        try:
            data = _cg_get(f"/coins/{cid}/market_chart?vs_currency=usd&days=max&interval=daily",
                           os.path.join(charts_dir, f"{cid}.json"))
            series = circ_series(data.get("prices") or [], data.get("market_caps") or [])
        except Exception:  # noqa: BLE001
            stats["errores"] += 1
            continue
        if len(series) < 30:
            continue
        stats["con_serie"] += 1
        for ev in detect_supply_steps(series, min_pct):
            if ev["ts_ms"] < since_ms:
                continue
            rows.append({
                "protocol": cid,
                "symbol": base + "USDT",
                "timestamp": ev["ts_ms"],
                "tokens": "",
                "pct_supply": ev["step_pct"],
                "pct_basis": "circ_supply_step",
                "category": "supply_step",
            })
            stats["eventos"] += 1
        if j % 25 == 0:
            print(f"  {j}/{len(chosen)} tokens ({stats['eventos']} eventos)...",
                  file=sys.stderr)
        time.sleep(2.2 if os.environ.get("COINGECKO_API_KEY") else 12.0)
    rows.sort(key=lambda r: r["timestamp"])
    return rows, stats


def source_adapters(adapters_dir: str, perps: set[str], min_pct: float,
                    gecko_map: dict[str, str]) -> tuple[list[dict], dict]:
    """Parsea protocols/*.ts del clone. Devuelve (rows, contadores de diagnóstico)."""
    pdir = Path(adapters_dir) / "protocols"
    files = sorted(pdir.glob("*.ts")) if pdir.is_dir() else []
    periods = parse_period_seconds(adapters_dir)
    stats = {"files": len(files), "parsed": 0, "sin_gecko": 0, "sin_symbol": 0,
             "sin_perp": 0, "sin_eventos": 0, "error": 0,
             "periods": {k: periods[k] for k in ("day", "month", "year") if k in periods}}
    rows: list[dict] = []
    for path in files:
        if path.stem in ("index", "types"):
            continue
        try:
            parsed = parse_adapter_file(path.read_text(errors="ignore"), path.stem, periods)
        except Exception:  # noqa: BLE001
            stats["error"] += 1
            continue
        stats["parsed"] += 1
        if not parsed["events"]:
            stats["sin_eventos"] += 1
            continue
        if not parsed["gecko_id"]:
            stats["sin_gecko"] += 1
            continue
        symbol = gecko_map.get(parsed["gecko_id"], "")
        if not symbol:
            stats["sin_symbol"] += 1
            continue
        perp = symbol.upper() + "USDT"
        if perp not in perps:
            stats["sin_perp"] += 1
            continue
        for row in adapter_events_to_rows(parsed, perp):
            if row["pct_supply"] >= min_pct:
                rows.append(row)
    rows.sort(key=lambda r: r["timestamp"])
    return rows, stats


# --------------------------------------------------------------------------- #
# Fuentes web (sitio / API clásico)
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
    p.add_argument("--source",
                   choices=["auto", "adapters", "supply-step", "llama-site", "llama-api"],
                   default="auto")
    p.add_argument("--adapters-dir", default="crypto/data/events/emissions-adapters",
                   help="clone local de github.com/DefiLlama/emissions-adapters")
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

    # --- PLAN C: reconstrucción por saltos de supply circulante (CoinGecko) ---
    if args.source == "supply-step":
        rows, stats = source_supply_step(perps, args.min_pct, args.cache_dir)
        print(f"Fuente: supply-step ({stats})", file=sys.stderr)
        if not rows:
            print("❌ supply-step: 0 eventos (¿CoinGecko accesible? ¿key?)", file=sys.stderr)
            return 1
        _write_rows(args.out, rows)
        print(f"OK (supply-step): {len(rows)} eventos (salto >= {args.min_pct}% del "
              f"circulante, con perp) -> {args.out}", file=sys.stderr)
        print("   Supuestos pre-registrados: conocibilidad ex-ante (cliffs contractuales), "
              "timestamp = punto medio ±12h, filtros de medición congelados.", file=sys.stderr)
        return 0

    # --- Fuente 1: clone del repo emissions-adapters (fuente de verdad, sin scraping) ---
    if args.source in ("auto", "adapters"):
        pdir = Path(args.adapters_dir) / "protocols"
        if pdir.is_dir():
            try:
                gecko_map = load_gecko_symbol_map(
                    os.path.join(args.cache_dir, "gecko_symbols.json"))
            except Exception as e:  # noqa: BLE001
                print(f"⚠️ coins/list de CoinGecko falló ({e}); "
                      "export COINGECKO_API_KEY=<demo key gratis> y reintentar",
                      file=sys.stderr)
                gecko_map = {}
            rows, stats = source_adapters(args.adapters_dir, perps, args.min_pct, gecko_map)
            print(f"Fuente: adapters ({stats})", file=sys.stderr)
            if rows:
                _write_rows(args.out, rows)
                print(f"OK (adapters): {len(rows)} eventos (>= {args.min_pct}% supply, "
                      f"con perp) -> {args.out}", file=sys.stderr)
                print("   sesgo superviviente + steps en start+k·duración: registrados.",
                      file=sys.stderr)
                return 0
            if args.source == "adapters":
                print("❌ adapters: 0 eventos. Pegar 2 muestras para ajustar el parser:\n"
                      f"   ls {pdir} | head -20\n"
                      f"   sed -n '1,80p' {pdir}/aptos.ts", file=sys.stderr)
                return 1
        elif args.source == "adapters":
            print(f"❌ falta el clone: git clone --depth 1 "
                  f"https://github.com/DefiLlama/emissions-adapters {args.adapters_dir}",
                  file=sys.stderr)
            return 1

    # --- elegir fuente web ---
    build = ""
    names: list[str] = []
    source = args.source if args.source != "adapters" else "auto"
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
    _write_rows(args.out, rows)
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
