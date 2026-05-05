#!/usr/bin/env python3
"""Backtest engine for WeatherStrategy.

Replays the strategy against recorded historical market data and forecasts.

Requirements:
    1. Historical data recorded via scripts/record_weather_data.py
    2. Data in data/historical/{date}/markets/ and /forecasts/

Usage:
    python scripts/backtest_weather.py --start 2026-03-01 --end 2026-04-30
    python scripts/backtest_weather.py --start 2026-03-01 --end 2026-04-30 --balance 166 --output report.json
"""

import argparse
import json
import statistics
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from src.data.weather_provider import HistoricalForecastProvider, HistoricalMarketProvider
from src.strategy.weather import WeatherStrategy

CONFIG_TEMPLATE = {
    "weather": {
        "enabled": True,
        "scan_interval_minutes": 5,
        "min_edge_threshold": 0.08,
        "max_entry_price": 0.85,
        "max_trade_size_usdc": 25.0,
        "max_total_allocation": 50.0,
        "max_trades_per_scan": 3,
        "kelly_fraction": 0.15,
        "min_edge_taker": 0.30,
        "nws_cross_check": False,
        "nws_max_divergence": 10.0,
        "min_ensemble_members": 20,
        "max_ensemble_std": 20.0,
        "calibration_min_samples": 20,
        "dynamic_thresholds": {
            "reposition_enabled": True,
            "reposition_prob_delta": 0.15,
            "boundary_risk_rate": 0.04,
            "exit_take_profit_threshold": 0.90,
            "exit_stop_loss_ratio": 0.35,
            "exit_temp_stop_loss": 8.0,
            "exit_max_hold_hours": 72,
            "exit_cooldown_hours": 4,
        },
    },
    "loss_reserve_usdc": 20.0,
}


class MockClient:
    """Simulates order execution for backtesting."""

    def __init__(self) -> None:
        self._order_counter = 0

    def place_limit_order(self, **kwargs: Any) -> dict[str, Any]:
        self._order_counter += 1
        price = kwargs.get("price", 0.5)
        size = kwargs.get("size", 10)
        return {
            "order_id": f"backtest_{self._order_counter}",
            "id": f"backtest_{self._order_counter}",
            "status": "filled_paper",
            "price": price,
            "size": size,
        }

    def get_orders(self) -> list:
        return []

    def get_positions(self) -> list:
        return []


def run_backtest(
    start_date: date,
    end_date: date,
    balance: float,
    historical_dir: str = "data/historical",
) -> dict[str, Any]:
    """Run backtest over date range and return performance metrics."""
    client = MockClient()
    config: dict[str, Any] = dict(CONFIG_TEMPLATE)

    forecast_provider = HistoricalForecastProvider(historical_dir)
    market_provider = HistoricalMarketProvider(historical_dir)

    strategy = WeatherStrategy(
        client=client,
        config=config,
        forecast_provider=forecast_provider,
        market_provider=market_provider,
    )

    daily_pnl: list[dict] = []
    current_date = start_date

    while current_date <= end_date:
        trades = strategy.run_scan(balance)
        day_pnl = 0.0
        for t in trades:
            if t.side == "SELL":
                pnl_val = float(t.metadata.get("realized_pnl", 0))
                day_pnl += pnl_val
                balance += pnl_val

        daily_pnl.append({
            "date": current_date.isoformat(),
            "balance": round(balance, 2),
            "trades": len(trades),
            "pnl": round(day_pnl, 2),
        })
        current_date += timedelta(days=1)

    perf = strategy.get_performance()

    # Compute additional metrics
    pnl_values = [d["pnl"] for d in daily_pnl if d["pnl"] != 0]
    balances = [d["balance"] for d in daily_pnl]

    if len(pnl_values) > 1:
        sharpe = (
            (statistics.mean(pnl_values) / (statistics.stdev(pnl_values) + 0.0001))
            * (252 ** 0.5)
        )
    else:
        sharpe = 0.0

    peak = balances[0] if balances else balance
    max_dd = 0.0
    for b in balances:
        peak = max(peak, b)
        dd = (peak - b) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "initial_balance": balance,
        "final_balance": round(balances[-1], 2) if balances else balance,
        "total_pnl": round(balances[-1] - balance, 2) if balances else 0,
        "sharpe_ratio": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd * 100, 1),
        "total_trades": perf.get("total_trades", 0),
        "settled": perf.get("settled", 0),
        "settled_wins": perf.get("settled_wins", 0),
        "exited": perf.get("exited", 0),
        "win_rate": perf.get("win_rate"),
        "exit_by_reason": perf.get("exit_by_reason", {}),
        "daily_pnl": daily_pnl,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest WeatherStrategy")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--balance", type=float, default=166.0, help="Initial balance")
    parser.add_argument("--data-dir", default="data/historical")
    parser.add_argument("--output", default=None, help="Output JSON file")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    print(f"Backtesting {start} to {end} with ${args.balance:.0f} capital...")
    report = run_backtest(start, end, args.balance, args.data_dir)

    print(f"\nResults:")
    print(f"  Initial: ${report['initial_balance']:.2f}")
    print(f"  Final:   ${report['final_balance']:.2f}")
    print(f"  PnL:     ${report['total_pnl']:.2f}")
    print(f"  Sharpe:  {report['sharpe_ratio']:.3f}")
    print(f"  Max DD:  {report['max_drawdown_pct']:.1f}%")
    print(f"  Trades:  {report['total_trades']} ({report['settled_wins']}W/{report['settled']}S, {report['exited']}E)")
    if report['win_rate'] is not None:
        print(f"  Win Rate: {report['win_rate']:.1%}")

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nReport saved to {args.output}")


if __name__ == "__main__":
    main()
