#!/usr/bin/env python3
"""Record weather data snapshots for future backtesting.

Graba estados actuales de Gamma API y Open-Meteo para construir
un dataset historico que el backtest engine pueda usar.

Usage:
    python scripts/record_weather_data.py                    # today
    python scripts/record_weather_data.py --date 2026-05-05  # specific date
    python scripts/record_weather_data.py --days 3           # last 3 days

Output:
    data/historical/{date}/markets/{slug}.json
    data/historical/{date}/forecasts/{city_key}_{target_date}.json
"""

import argparse
import json
import time
from datetime import date, timedelta
from pathlib import Path

import requests

# 63 cities from CITY_SLUGS in weather.py
CITY_SLUGS_LITE: dict[str, str] = {
    "New York": "nyc", "Chicago": "chicago", "Miami": "miami",
    "Los Angeles": "los-angeles", "Denver": "denver", "Dallas": "dallas",
    "Atlanta": "atlanta", "Seattle": "seattle", "Houston": "houston",
    "Austin": "austin", "San Francisco": "san-francisco",
    "London": "london", "Paris": "paris", "Madrid": "madrid",
    "Amsterdam": "amsterdam", "Milan": "milan", "Munich": "munich",
    "Warsaw": "warsaw", "Helsinki": "helsinki", "Moscow": "moscow",
    "Ankara": "ankara", "Istanbul": "istanbul", "Tel Aviv": "tel-aviv",
    "Tokyo": "tokyo", "Seoul": "seoul", "Hong Kong": "hong-kong",
    "Singapore": "singapore", "Beijing": "beijing", "Shanghai": "shanghai",
    "Lucknow": "lucknow", "Buenos Aires": "buenos-aires",
    "Mexico City": "mexico-city", "Sao Paulo": "sao-paulo",
    "Toronto": "toronto", "Sydney": "sydney",
}


def date_to_slug(d: date) -> str:
    month = d.strftime("%B").lower()
    return f"{month}-{d.day}-{d.year}"


def record_markets(data_dir: Path, recording_date: date) -> int:
    """Record Gamma API snapshots for all city+date combinations."""
    markets_dir = data_dir / recording_date.isoformat() / "markets"
    markets_dir.mkdir(parents=True, exist_ok=True)

    dates = [recording_date, recording_date + timedelta(days=1)]
    recorded = 0

    for d in dates:
        date_slug = date_to_slug(d)
        for city_name, city_slug in CITY_SLUGS_LITE.items():
            for prefix in ("highest-temperature", "lowest-temperature"):
                slug = f"{prefix}-in-{city_slug}-on-{date_slug}"
                path = markets_dir / f"{slug}.json"
                if path.exists():
                    continue

                try:
                    r = requests.get(
                        "https://gamma-api.polymarket.com/events",
                        params={"slug": slug},
                        timeout=10.0,
                    )
                    if r.status_code == 200 and r.text.strip():
                        data = r.json()
                        if data:
                            path.write_text(json.dumps(data), encoding="utf-8")
                            recorded += 1
                except Exception:
                    pass
                time.sleep(0.2)  # rate limit

    return recorded


def record_forecasts(data_dir: Path, recording_date: date) -> int:
    """Record Open-Meteo ensemble forecasts for all cities."""
    forecasts_dir = data_dir / recording_date.isoformat() / "forecasts"
    forecasts_dir.mkdir(parents=True, exist_ok=True)

    # Use the STATIONS dict for coordinates
    from src.data.weather import STATIONS

    dates = [recording_date, recording_date + timedelta(days=1)]
    recorded = 0

    for d in dates:
        for city_key, station in STATIONS.items():
            path = forecasts_dir / f"{city_key}_{d.isoformat()}.json"
            if path.exists():
                continue

            try:
                params = {
                    "latitude": station["lat"],
                    "longitude": station["lon"],
                    "daily": "temperature_2m_max,temperature_2m_min",
                    "temperature_unit": "fahrenheit",
                    "start_date": d.isoformat(),
                    "end_date": d.isoformat(),
                    "models": "gfs_seamless",
                }
                r = requests.get(
                    "https://ensemble-api.open-meteo.com/v1/ensemble",
                    params=params, timeout=15.0,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                daily = data.get("daily", {})
                member_highs = []
                member_lows = []
                for key, values in daily.items():
                    if not isinstance(values, list) or not values:
                        continue
                    val = values[0]
                    if val is None:
                        continue
                    if key == "temperature_2m_max" or key.startswith("temperature_2m_max_member"):
                        member_highs.append(float(val))
                    elif key == "temperature_2m_min" or key.startswith("temperature_2m_min_member"):
                        member_lows.append(float(val))

                if member_highs:
                    record = {
                        "city_key": city_key,
                        "city_name": station["city_name"],
                        "target_date": d.isoformat(),
                        "member_highs": member_highs,
                        "member_lows": member_lows,
                    }
                    path.write_text(json.dumps(record), encoding="utf-8")
                    recorded += 1
            except Exception:
                pass
            time.sleep(0.2)

    return recorded


def main():
    parser = argparse.ArgumentParser(description="Record weather data for backtesting")
    parser.add_argument("--date", type=str, help="Recording date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=1, help="Record N past days")
    parser.add_argument("--data-dir", default="data/historical")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    today = date.today()

    if args.date:
        recording_date = date.fromisoformat(args.date)
        dates_to_record = [recording_date]
    else:
        dates_to_record = [today - timedelta(days=i) for i in range(args.days)]

    total_markets = 0
    total_forecasts = 0

    for d in dates_to_record:
        print(f"Recording {d.isoformat()}...")
        m = record_markets(data_dir, d)
        f = record_forecasts(data_dir, d)
        print(f"  Markets: {m}, Forecasts: {f}")
        total_markets += m
        total_forecasts += f

    print(f"\nDone. {total_markets} markets, {total_forecasts} forecasts in {data_dir}")


if __name__ == "__main__":
    main()
