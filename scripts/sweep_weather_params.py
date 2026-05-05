#!/usr/bin/env python3
"""Parameter sweep for WeatherStrategy.

Runs backtests across parameter combinations to find optimal config.

Usage:
    python scripts/sweep_weather_params.py --start 2026-03-01 --end 2026-04-30
    python scripts/sweep_weather_params.py --start 2026-03-01 --end 2026-04-30 --top 5
"""

import argparse
import copy
import json
import itertools
from datetime import date
from pathlib import Path
from typing import Any

from scripts.backtest_weather import CONFIG_TEMPLATE, run_backtest

SWEEP_PARAMS = {
    "min_edge_threshold": [0.06, 0.08, 0.10],
    "kelly_fraction": [0.10, 0.15, 0.20],
    "dynamic_thresholds.reposition_prob_delta": [0.10, 0.15, 0.20],
    "dynamic_thresholds.exit_take_profit_threshold": [0.85, 0.90],
    "dynamic_thresholds.exit_stop_loss_ratio": [0.25, 0.35, 0.50],
    "dynamic_thresholds.edge_base_mid": [0.05, 0.07, 0.09],
    "dynamic_thresholds.boundary_risk_rate": [0.02, 0.04, 0.06],
    "dynamic_thresholds.exit_trailing_enabled": [True, False],
}


def set_nested(d: dict, key_path: str, value: Any) -> None:
    parts = key_path.split(".")
    target = d
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = value


def generate_combinations() -> list[dict[str, Any]]:
    keys = list(SWEEP_PARAMS.keys())
    values = list(SWEEP_PARAMS.values())
    configs = []

    for combo in itertools.product(*values):
        config = copy.deepcopy(CONFIG_TEMPLATE)
        for key, val in zip(keys, combo):
            if key.startswith("dynamic_thresholds."):
                dt_key = key.split(".", 1)[1]
                config["weather"].setdefault("dynamic_thresholds", {})[dt_key] = val
            else:
                config["weather"][key] = val
        configs.append(config)

    return configs


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep WeatherStrategy parameters")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--balance", type=float, default=166.0)
    parser.add_argument("--data-dir", default="data/historical")
    parser.add_argument("--top", type=int, default=10, help="Show top N results")
    parser.add_argument("--output", default=None, help="Output JSON file")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    configs = generate_combinations()

    print(f"Sweeping {len(configs)} parameter combinations...")
    results = []

    for i, config in enumerate(configs):
        # Modify CONFIG_TEMPLATE for this run
        run_config = copy.deepcopy(config)
        report = run_backtest(start, end, args.balance, args.data_dir, override_config=run_config)
        results.append({
            "config": {
                k: config["weather"].get(k, config["weather"].get("dynamic_thresholds", {}).get(k.split(".", 1)[1] if "." in k else k))
                for k in SWEEP_PARAMS
            },
            "sharpe": report["sharpe_ratio"],
            "total_pnl": report["total_pnl"],
            "max_dd": report["max_drawdown_pct"],
            "win_rate": report["win_rate"],
            "trades": report["total_trades"],
        })

        if (i + 1) % 20 == 0 or i == 0:
            print(f"  {i+1}/{len(configs)} done...")

    results.sort(key=lambda r: r["sharpe"], reverse=True)

    print(f"\nTop {args.top} by Sharpe ratio:")
    for i, r in enumerate(results[:args.top]):
        cfg = r["config"]
        print(f"  #{i+1}: Sharpe={r['sharpe']:.3f} PnL=${r['total_pnl']:.2f} DD={r['max_dd']:.1f}% WR={r['win_rate']}")
        print(f"       edge={cfg.get('min_edge_threshold')} kelly={cfg.get('kelly_fraction')} repos={cfg.get('reposition_prob_delta')}")

    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"\nFull results saved to {args.output}")


if __name__ == "__main__":
    main()
