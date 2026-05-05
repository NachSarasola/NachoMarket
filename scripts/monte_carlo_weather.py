#!/usr/bin/env python3
"""Monte Carlo simulation for WeatherStrategy.

Simulates N parallel universes using calibrated probabilities to estimate
expected PnL, risk of ruin, and drawdown distribution.

Usage:
    python scripts/monte_carlo_weather.py --trades 100 --simulations 1000
    python scripts/monte_carlo_weather.py --trades 200 --simulations 5000 --capital 166
"""

import argparse
import json
import random
import statistics
from datetime import date
from pathlib import Path
from typing import Any


def load_calibration() -> dict[str, dict[str, Any]]:
    """Load calibration data from weather_calibration.json."""
    path = Path("data/weather_calibration.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def simulate_trade(calibration: dict, trade_params: dict) -> float:
    """Simulate one trade outcome and return PnL in USDC.

    Uses calibrated probability to determine win/loss.
    Applies Kelly sizing to compute position size.
    """
    city = trade_params.get("city", "unknown")
    metric = trade_params.get("metric", "high")
    lead_days = trade_params.get("lead_days", 1)
    month = trade_params.get("month", 5)
    entry_price = trade_params.get("entry_price", 0.35)
    size = trade_params.get("size_usdc", 15.0)
    kelly_fraction = trade_params.get("kelly_fraction", 0.15)

    # Look up calibrated probability
    keys = [
        f"{city}_{metric}_{lead_days}d_m{month}",
        f"{city}_{metric}_{lead_days}d",
        f"{city}_{metric}",
        f"{city}_{lead_days}d_m{month}",
        city,
    ]
    calibrated_prob = 0.5
    for key in keys:
        if key in calibration:
            entry = calibration[key]
            bias = entry.get("bias", 0.0)
            raw_prob = trade_params.get("model_prob", 0.5)
            calibrated_prob = max(0.01, min(0.99, raw_prob - bias))
            break

    # Kelly sizing
    odds = (1.0 - entry_price) / entry_price if entry_price > 0 else 1.0
    lose_prob = 1.0 - calibrated_prob
    kelly = (calibrated_prob * odds - lose_prob) / odds if odds > 0 else 0.0
    kelly = min(kelly * kelly_fraction, 0.05)

    position_size = kelly * trade_params.get("capital", 166.0)
    position_size = min(position_size, size)

    # Simulate outcome
    win = random.random() < calibrated_prob
    if win:
        return position_size * (1.0 / entry_price - 1.0)
    return -position_size


def generate_trade_params(num_trades: int, capital: float) -> list[dict]:
    """Generate plausible trade parameters from historical ranges."""
    cities = ["New York", "Chicago", "Miami", "Dallas", "Atlanta", "Seattle",
              "London", "Tokyo", "Paris", "Buenos Aires"]
    metrics = ["high", "low"]
    params = []

    for _ in range(num_trades):
        params.append({
            "city": random.choice(cities),
            "metric": random.choice(metrics),
            "lead_days": random.choice([0, 1, 2]),
            "month": random.randint(1, 12),
            "entry_price": round(random.uniform(0.15, 0.65), 2),
            "model_prob": round(random.uniform(0.25, 0.75), 2),
            "size_usdc": random.uniform(5.0, 25.0),
            "kelly_fraction": 0.15,
            "capital": capital,
        })
    return params


def run_simulation(
    n_simulations: int,
    n_trades: int,
    capital: float,
) -> dict[str, Any]:
    """Run Monte Carlo simulation."""
    calibration = load_calibration()
    trade_params = generate_trade_params(n_trades, capital)

    final_balances: list[float] = []
    drawdowns: list[float] = []
    ruin_count = 0
    ruin_threshold = capital * 0.5

    for sim in range(n_simulations):
        balance = capital
        peak = capital
        max_dd = 0.0
        sim_params = [dict(t) for t in trade_params]

        for t in sim_params:
            t["capital"] = balance
            pnl = simulate_trade(calibration, t)
            balance += pnl
            peak = max(peak, balance)
            dd = (peak - balance) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)

            if balance <= ruin_threshold:
                ruin_count += 1
                break

        final_balances.append(balance)
        drawdowns.append(max_dd)

    final_balances.sort()
    n = len(final_balances)

    return {
        "simulations": n_simulations,
        "trades_per_sim": n_trades,
        "initial_capital": capital,
        "mean_final": round(statistics.mean(final_balances), 2),
        "median_final": round(final_balances[n // 2], 2),
        "p5_final": round(final_balances[int(n * 0.05)], 2),
        "p95_final": round(final_balances[int(n * 0.95)], 2),
        "min_final": round(min(final_balances), 2),
        "max_final": round(max(final_balances), 2),
        "ruin_probability": round(ruin_count / n_simulations, 4),
        "mean_drawdown": round(statistics.mean(drawdowns) * 100, 1),
        "max_drawdown_p95": round(sorted(drawdowns)[int(n * 0.95)] * 100, 1),
        "positive_outcomes": round(
            sum(1 for b in final_balances if b > capital) / n * 100, 1
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Monte Carlo WeatherStrategy simulation")
    parser.add_argument("--trades", type=int, default=100, help="Trades per simulation")
    parser.add_argument("--simulations", "-n", type=int, default=1000)
    parser.add_argument("--capital", type=float, default=166.0)
    parser.add_argument("--output", default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)

    print(f"Monte Carlo: {args.simulations} simulations × {args.trades} trades with ${args.capital}")
    report = run_simulation(args.simulations, args.trades, args.capital)

    print(f"\nResults:")
    print(f"  Mean final:     ${report['mean_final']:.2f}")
    print(f"  Median final:   ${report['median_final']:.2f}")
    print(f"  5th percentile: ${report['p5_final']:.2f}")
    print(f"  95th percentile: ${report['p95_final']:.2f}")
    print(f"  Min/Max:        ${report['min_final']:.2f} / ${report['max_final']:.2f}")
    print(f"  Ruin prob:      {report['ruin_probability']:.1%}")
    print(f"  Mean drawdown:  {report['mean_drawdown']}%")
    print(f"  P95 drawdown:   {report['max_drawdown_p95']}%")
    print(f"  Positive:       {report['positive_outcomes']}%")

    if args.output:
        import json as _json
        Path(args.output).write_text(_json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
