#!/usr/bin/env python3
"""Veredicto MECANICO de los gates a partir de los reportes JSON de validate.py.

El auto-engano vive en la INTERPRETACION del backtest ("bueno, el OOS casi pasa..."). Este
script lo elimina: aplica los gates de REGLAS_CONGELADAS.md en orden, de forma determinista,
y emite uno de cuatro veredictos accionables. No mira datos: solo los reportes ya generados.

Uso:
    python crypto/scripts/decide.py crypto/data/report_*.json

Veredictos:
  GO_DRY_RUN                      -> el sweep paso todo; arrancar dry-run (Fase C).
  AJUSTE_UNICO(<gate>)            -> fallo SOLO algo corregible (pocos trades / folds borde);
                                     una sola tanda de ajuste, contada como trial, y re-validar.
  DESCARTAR_SWEEP_QUEDA_DONCHIAN  -> el sweep no tiene edge pero el control (donchian) si.
  NO_OPERAR                       -> ninguno tiene edge; no hay live. Preservar capital.

Diseno anti-overfitting: los fallos de "no hay edge" (OOS overfit, DSR bajo, no batir
benchmarks) son HARD -> NO son "ajustables" (ajustar seria curve-fitting). Solo los fallos de
"muestra chica / borde" son SOFT -> ameritan UNA mirada mas.
"""

from __future__ import annotations

import argparse
import json
import sys

# --- Umbrales (coinciden con REGLAS_CONGELADAS.md) ---
MIN_TRADES = 100
WF_MIN_POS_FRAC = 0.5     # >=50% de los folds temporales con Sharpe>0
WF_WORST_MIN = -1.0       # ningun fold peor que esto
OOS_IS_MIN = 0.5          # Sharpe OOS >= 50% del in-sample
DSR_MIN = 0.95
RUIN_MAX = 0.10           # P(ruina) tolerable (warning si se supera)


def _num(x, default=float("nan")) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def classify_report(report: dict) -> dict:
    """Aplica los gates a UN reporte. Devuelve gates + clasificacion PASS/ADJUST/FAIL.

    HARD fail -> FAIL (no hay edge). SOFT fail -> ADJUST (borde/muestra). Todo OK -> PASS.
    """
    ins = report.get("in_sample", {}) or {}
    gates: dict[str, dict] = {}
    hard_fail: list[str] = []
    soft_fail: list[str] = []

    # G3 — trades minimos (SOFT: muestra chica).
    trades = _num(ins.get("trades"), 0)
    ok = trades >= MIN_TRADES
    gates["trades"] = {"ok": ok, "val": trades, "need": MIN_TRADES, "sev": "soft"}
    if not ok:
        soft_fail.append(f"trades={int(trades)}<{MIN_TRADES}")

    # G4 — consistencia walk-forward (SOFT: borde).
    wf = report.get("walk_forward", []) or []
    sharpes = [_num(f.get("sharpe")) for f in wf if f.get("sharpe") is not None]
    if sharpes:
        pos_frac = sum(1 for s in sharpes if s > 0) / len(sharpes)
        worst = min(sharpes)
        ok = pos_frac >= WF_MIN_POS_FRAC and worst >= WF_WORST_MIN
        gates["walk_forward"] = {"ok": ok, "pos_frac": round(pos_frac, 2),
                                 "worst_fold": round(worst, 2), "sev": "soft"}
        if not ok:
            soft_fail.append(f"wf pos_frac={pos_frac:.2f} worst={worst:.2f}")
    else:
        gates["walk_forward"] = {"ok": False, "reason": "sin folds", "sev": "soft"}
        soft_fail.append("wf sin folds")

    # G5 — OOS no overfit (HARD: sin edge fuera de muestra).
    verdict = report.get("gate5_verdict")
    ratio = _num(report.get("gate5_oos_is_ratio"))
    has_oos = "oos" in report
    if verdict == "in_sample_not_profitable":
        gates["oos"] = {"ok": False, "reason": "in-sample no rentable", "sev": "hard"}
        hard_fail.append("in-sample Sharpe<=0")
    elif not has_oos:
        gates["oos"] = {"ok": False, "reason": "sin OOS (pocos datos)", "sev": "soft"}
        soft_fail.append("sin OOS")
    else:
        ok = ratio >= OOS_IS_MIN
        gates["oos"] = {"ok": ok, "ratio": round(ratio, 2), "need": OOS_IS_MIN, "sev": "hard"}
        if not ok:
            hard_fail.append(f"OOS/IS={ratio:.2f}<{OOS_IS_MIN}")

    # G_DSR — Deflated Sharpe (HARD: no se distingue del azar).
    dsr = _num((report.get("deflated_sharpe") or {}).get("dsr"))
    ok = dsr >= DSR_MIN
    gates["dsr"] = {"ok": ok, "val": round(dsr, 3) if dsr == dsr else None, "need": DSR_MIN, "sev": "hard"}
    if not ok:
        hard_fail.append(f"DSR={dsr:.3f}<{DSR_MIN}" if dsr == dsr else "DSR nan")

    # G_BENCH — batir buy&hold y MA en IS, y buy&hold en OOS (HARD: no aporta).
    bh = (report.get("benchmarks_in_sample") or {}).get("buy_hold", {}) or {}
    ma = (report.get("benchmarks_in_sample") or {}).get("ma_cross", {}) or {}
    s_sharpe = _num(ins.get("sharpe"))
    beat_is = s_sharpe > _num(bh.get("sharpe"), -1e9) and s_sharpe > _num(ma.get("sharpe"), -1e9)
    beat_oos = True
    if has_oos:
        oos_ret = _num((report.get("oos") or {}).get("total_return"))
        bh_oos = _num((report.get("oos_buy_hold") or {}).get("total_return"), -1e9)
        beat_oos = oos_ret > bh_oos
    ok = beat_is and beat_oos
    gates["benchmarks"] = {"ok": ok, "beat_is": beat_is, "beat_oos": beat_oos, "sev": "hard"}
    if not ok:
        hard_fail.append("no bate benchmarks")

    # G_RUIN — informativo (SOFT/warning).
    ruin = _num((report.get("monte_carlo_ruin") or {}).get("prob_ruin"))
    if ruin == ruin:
        ok = ruin <= RUIN_MAX
        gates["ruin"] = {"ok": ok, "prob_ruin": round(ruin, 3), "max": RUIN_MAX, "sev": "soft"}
        if not ok:
            soft_fail.append(f"P(ruina)={ruin:.1%}>{RUIN_MAX:.0%}")

    if hard_fail:
        cls = "FAIL"
    elif soft_fail:
        cls = "ADJUST"
    else:
        cls = "PASS"

    return {
        "strategy": report.get("strategy", "?"),
        "source": (report.get("data") or {}).get("source", "?"),
        "gates": gates,
        "hard_fail": hard_fail,
        "soft_fail": soft_fail,
        "classification": cls,
        "oos_sharpe": _num((report.get("oos") or ins).get("sharpe")),
    }


def combine(classified: list[dict]) -> dict:
    """Combina las clasificaciones por fuente (par): sweep vs donchian -> veredicto por par."""
    by_source: dict[str, dict] = {}
    for c in classified:
        by_source.setdefault(c["source"], {})[c["strategy"]] = c

    per_pair: dict[str, dict] = {}
    for src, strat in by_source.items():
        s = strat.get("sweep")
        d = strat.get("donchian")
        if s is None:
            per_pair[src] = {"verdict": "SIN_SWEEP", "reasons": ["falta reporte del sweep"]}
            continue
        sweep_beats = True
        if d is not None:
            sweep_beats = s["oos_sharpe"] >= d["oos_sharpe"]
        if s["classification"] == "PASS" and sweep_beats:
            v = {"verdict": "GO_DRY_RUN", "strategy": "sweep", "reasons": []}
        elif s["classification"] == "PASS" and d and d["classification"] == "PASS":
            v = {"verdict": "DESCARTAR_SWEEP_QUEDA_DONCHIAN",
                 "reasons": [f"donchian OOS Sharpe {d['oos_sharpe']:.2f} >= sweep {s['oos_sharpe']:.2f}"]}
        elif s["classification"] == "FAIL" and d and d["classification"] == "PASS":
            v = {"verdict": "DESCARTAR_SWEEP_QUEDA_DONCHIAN", "reasons": s["hard_fail"]}
        elif s["classification"] == "ADJUST":
            v = {"verdict": "AJUSTE_UNICO", "reasons": s["soft_fail"]}
        else:
            v = {"verdict": "NO_OPERAR", "reasons": s["hard_fail"] or s["soft_fail"]}
        per_pair[src] = v

    # Veredicto global por prioridad.
    priority = ["GO_DRY_RUN", "DESCARTAR_SWEEP_QUEDA_DONCHIAN", "AJUSTE_UNICO", "NO_OPERAR", "SIN_SWEEP"]
    overall = "NO_OPERAR"
    for p in priority:
        if any(v["verdict"] == p for v in per_pair.values()):
            overall = p
            break
    return {"per_pair": per_pair, "overall": overall}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("reports", nargs="+", help="reportes JSON de validate.py")
    args = ap.parse_args()

    classified = []
    for path in args.reports:
        try:
            with open(path) as f:
                rep = json.load(f)
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  no pude leer {path}: {e}", file=sys.stderr)
            continue
        c = classify_report(rep)
        classified.append(c)
        print(f"\n== {c['source']} / {c['strategy']} -> {c['classification']} ==")
        for name, g in c["gates"].items():
            mark = "✅" if g.get("ok") else "❌"
            extra = {k: v for k, v in g.items() if k not in ("ok", "sev")}
            print(f"  {mark} {name:>12} {extra}")
        if c["hard_fail"]:
            print(f"  HARD: {c['hard_fail']}")
        if c["soft_fail"]:
            print(f"  SOFT: {c['soft_fail']}")

    if not classified:
        print("Sin reportes validos.", file=sys.stderr)
        return 2

    result = combine(classified)
    print("\n" + "=" * 55)
    print("  VEREDICTO POR PAR")
    for src, v in result["per_pair"].items():
        print(f"    {src}: {v['verdict']}" + (f"  ({'; '.join(v['reasons'])})" if v["reasons"] else ""))
    print(f"\n  >>> VEREDICTO GLOBAL: {result['overall']} <<<")
    print("=" * 55)
    print("  Registrar este veredicto como fila en crypto/REGLAS_CONGELADAS.md.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
