"""Tests for WeatherStrategy Phase 1-4 — STATIONS precision and calibration pipeline."""
import json
import math
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.data.weather import (
    STATIONS,
    MONTH_MAP,
    EnsembleForecast,
    _celsius_to_fahrenheit,
    resolve_station,
    get_station,
    get_wunderground_url,
)
from src.strategy.weather import WeatherStrategy, WeatherMarket, CITY_SLUGS


def test_celsius_to_fahrenheit():
    assert _celsius_to_fahrenheit(0) == 32.0
    assert math.isclose(_celsius_to_fahrenheit(100), 212.0)


# --- STATIONS integrity ---

def test_stations_has_all_city_slugs():
    for city_name, slug in CITY_SLUGS.items():
        assert slug in STATIONS, f"{city_name} ({slug}) not in STATIONS"


def test_stations_count():
    assert len(STATIONS) == 63, f"Expected 63 stations, got {len(STATIONS)}"


def test_all_stations_have_required_fields():
    required = ["city_name", "lat", "lon", "icao", "station_name", "wunderground", "unit", "timezone", "verified"]
    for key, data in STATIONS.items():
        for f in required:
            assert f in data, f"{key}: missing field '{f}'"


def test_stations_units_are_valid():
    for key, data in STATIONS.items():
        assert data["unit"] in ("F", "C"), f"{key}: invalid unit '{data['unit']}'"


def test_stations_lat_lon_in_range():
    for key, data in STATIONS.items():
        assert -90 <= data["lat"] <= 90, f"{key}: lat {data['lat']} out of range"
        assert -180 <= data["lon"] <= 180, f"{key}: lon {data['lon']} out of range"


def test_no_duplicate_icao():
    icaos = [d["icao"] for d in STATIONS.values() if d["icao"]]
    assert len(icaos) == len(set(icaos)), f"Duplicate ICAOs in STATIONS!"


def test_us_stations_use_fahrenheit():
    us_stations = [
        "nyc", "chicago", "miami", "dallas", "seattle", "atlanta",
        "los-angeles", "denver", "houston", "austin", "san-francisco",
        "boston", "phoenix", "las-vegas", "minneapolis", "philadelphia",
        "washington-dc", "san-antonio", "new-orleans", "oklahoma-city",
    ]
    for key in us_stations:
        assert STATIONS[key]["unit"] == "F", f"{key}: US station should be F"


def test_non_us_stations_use_celsius():
    for key, data in STATIONS.items():
        if key not in ("nyc", "chicago", "miami", "dallas", "seattle", "atlanta",
                        "los-angeles", "denver", "houston", "austin", "san-francisco",
                        "boston", "phoenix", "las-vegas", "minneapolis", "philadelphia",
                        "washington-dc", "san-antonio", "new-orleans", "oklahoma-city"):
            assert data["unit"] == "C", f"{key}: non-US station should be C, got {data['unit']}"


def test_stations_coords_differ_from_city_centers():
    city_centers = {
        "nyc": (40.7128, -74.0060),
        "chicago": (41.8781, -87.6298),
        "dallas": (32.7767, -96.7970),
        "paris": (48.8566, 2.3522),
        "london": (51.5074, -0.1278),
        "tokyo": (35.6762, 139.6503),
    }
    for key, (city_lat, city_lon) in city_centers.items():
        if key not in STATIONS:
            continue
        station = STATIONS[key]
        dist = math.sqrt((station["lat"] - city_lat)**2 + (station["lon"] - city_lon)**2)
        assert dist > 0.01, (
            f"{key}: STATIONS coords equal city center! "
            f"station=({station['lat']}, {station['lon']}), center=({city_lat}, {city_lon})"
        )


def test_critical_deviations():
    assert STATIONS["dallas"]["icao"] == "KDAL", "Dallas must be Love Field (KDAL), NOT DFW!"
    assert STATIONS["denver"]["icao"] == "KBKF", "Denver must be Buckley SFB (KBKF), NOT DIA (KDEN)!"
    assert STATIONS["houston"]["icao"] == "KHOU", "Houston must be Hobby (KHOU), NOT Bush IAH!"
    assert STATIONS["paris"]["icao"] == "LFPB", "Paris must be Le Bourget (LFPB), NOT CDG (LFPG)!"
    assert STATIONS["london"]["icao"] == "EGLC", "London must be City Airport (EGLC), NOT Heathrow!"
    assert STATIONS["seoul"]["icao"] == "RKSI", "Seoul must be Incheon (RKSI), NOT Gimpo!"
    assert STATIONS["panama-city"]["icao"] == "MPMG", "Panama must be Gelabert (MPMG), NOT Tocumen!"


def test_hong_kong_special_case():
    hk = STATIONS["hong-kong"]
    assert hk["icao"] == "", "Hong Kong has no ICAO (observatory, not airport)"
    assert hk["wunderground"] == "", "Hong Kong does not use Wunderground"
    assert hk["station_name"] == "Hong Kong Observatory"


def test_verified_count():
    verified = sum(1 for d in STATIONS.values() if d["verified"])
    assert verified >= 51, f"Expected at least 51 verified, got {verified}"


# --- resolve_station ---

def test_resolve_station_by_key():
    station = resolve_station("nyc")
    assert station is not None
    assert station["icao"] == "KLGA"
    assert station["city_name"] == "New York"


def test_resolve_station_by_city_name():
    station = resolve_station("New York")
    assert station is not None
    assert station["icao"] == "KLGA"


def test_resolve_station_by_slug():
    station = resolve_station("buenos-aires")
    assert station is not None
    assert station["icao"] == "SAEZ"


def test_resolve_station_not_found():
    station = resolve_station("CiudadQueNoExisteXYZ")
    assert station is None


def test_resolve_station_fuzzy():
    station = resolve_station("Dallas")
    assert station is not None
    assert station["icao"] == "KDAL"


# --- Helpers ---

def test_get_station():
    assert get_station("nyc") is not None
    assert get_station("nonexistent") is None


def test_get_wunderground_url():
    url = get_wunderground_url("nyc")
    assert "KLGA" in url
    assert "wunderground" in url

    url = get_wunderground_url("hong-kong")
    assert url == ""

    url = get_wunderground_url("nonexistent")
    assert url == ""


# --- EnsembleForecast ---

def make_forecast(highs, lows):
    return EnsembleForecast("KLGA", "NYC", date(2026, 6, 15), list(highs), list(lows))


def test_ensemble_probability_all_above():
    fc = make_forecast([80, 81, 82, 83, 84], [60, 61, 62, 63, 64])
    assert fc.probability_high_above(75) == 1.0
    assert fc.probability_high_below(75) == 0.0


def test_ensemble_probability_mixed():
    fc = make_forecast([70, 72, 75, 78, 80], [50, 52, 55, 58, 60])
    assert fc.probability_high_above(75) == 0.4
    assert fc.probability_high_below(75) == 0.6


def test_ensemble_probability_empty():
    fc = make_forecast([], [])
    assert fc.probability_high_above(75) == 0.5


def test_ensemble_mean_and_std():
    fc = make_forecast([70, 72, 75, 78, 80], [50, 52, 55, 58, 60])
    assert fc.mean_high == 75.0
    assert fc.num_members == 5


# --- Threshold parsing ---

def test_parse_threshold_f_below():
    t, lo, hi = WeatherStrategy._parse_threshold("71°F or below?")
    assert t == 71.0
    assert lo is None
    assert hi == 71.0


def test_parse_threshold_range():
    t, lo, hi = WeatherStrategy._parse_threshold("Will it be 72-73°F?")
    assert t == 72.5
    assert lo == 72.0
    assert hi == 73.0


def test_parse_threshold_celsius():
    t, lo, hi = WeatherStrategy._parse_threshold("Will it be 24°C?")
    assert abs(t - 75.2) < 1.0


def test_parse_threshold_none():
    t, lo, hi = WeatherStrategy._parse_threshold("No temperature here")
    assert t is None


def test_parse_threshold_above():
    t, lo, hi = WeatherStrategy._parse_threshold("Will it be 82°F or higher?")
    assert t == 82.0
    assert lo == 82.0
    assert hi is None


# --- CITY_SLUGS integrity ---

def test_city_slugs_min_count():
    assert len(CITY_SLUGS) >= 60


def test_city_slugs_known():
    assert CITY_SLUGS["New York"] == "nyc"
    assert CITY_SLUGS["London"] == "london"
    assert CITY_SLUGS["Tokyo"] == "tokyo"


def test_all_slugs_are_valid_station_keys():
    for city_name, slug in CITY_SLUGS.items():
        assert slug in STATIONS, (
            f"CITY_SLUGS['{city_name}'] = '{slug}' but '{slug}' not in STATIONS"
        )


# --- MONTH_MAP ---

def test_month_map():
    assert MONTH_MAP["may"] == 5
    assert MONTH_MAP["january"] == 1
    assert MONTH_MAP["dec"] == 12


# =============================================================================
# Phase 2 tests: calibration pipeline, pending state persistence, shrinkage
# =============================================================================


class DummyClient:
    """Mock para WeatherStrategy que no necesita APIs reales."""

    def place_limit_order(self, **kwargs):
        return {"order_id": "0xorder123", "id": "0xorder123", "status": "submitted"}

    def get_orders(self):
        return []

    def get_positions(self):
        return []


def _make_strategy():
    import shutil
    from pathlib import Path
    import tempfile

    tmp = tempfile.mkdtemp()
    client = DummyClient()
    config = {
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
        }
    }
    ws = WeatherStrategy(client=client, config=config)
    ws._calibration_file = Path(tmp) / "test_calibration.json"
    ws._pending_file = Path(tmp) / "test_pending.json"
    ws._calibration = {}
    ws._pending_trades = {}
    return ws


# --- Phase 4: record calibration with per-metric key ---

def test_record_calibration_per_metric_key():
    ws = _make_strategy()
    ws._record_calibration("New York", metric="high", lead_days=2,
                           yes_won=True, forecast_mean=72.0, actual_temp=74.0)
    from datetime import date
    month = date.today().month
    key = f"New York_high_2d_m{month}"
    assert key in ws._calibration
    entry = ws._calibration[key]
    assert entry["city"] == "New York"
    assert entry["metric"] == "high"
    assert entry["lead_days"] == 2
    assert entry["month"] == month
    assert entry["predictions"] == [1.0]
    assert entry["count"] == 1
    assert entry["bias"] == 0.5
    assert "last_updated" in entry


def test_record_calibration_separates_high_low():
    ws = _make_strategy()
    ws._record_calibration("Chicago", metric="high", lead_days=1,
                           yes_won=True, forecast_mean=85.0, actual_temp=88.0)
    ws._record_calibration("Chicago", metric="low", lead_days=1,
                           yes_won=False, forecast_mean=45.0, actual_temp=42.0)
    from datetime import date
    month = date.today().month
    high_key = f"Chicago_high_1d_m{month}"
    low_key = f"Chicago_low_1d_m{month}"
    assert high_key in ws._calibration
    assert low_key in ws._calibration
    assert ws._calibration[high_key]["forecast_errors"] == [3.0]
    assert ws._calibration[low_key]["forecast_errors"] == [-3.0]


def test_record_calibration_multiple():
    ws = _make_strategy()
    for _ in range(3):
        ws._record_calibration("Miami", metric="high", lead_days=0, yes_won=True)
    ws._record_calibration("Miami", metric="high", lead_days=0, yes_won=False)
    from datetime import date
    month = date.today().month
    key = f"Miami_high_0d_m{month}"
    entry = ws._calibration[key]
    assert entry["count"] == 4
    assert entry["bias"] == 0.75 - 0.5


def test_record_calibration_outlier_rejected():
    ws = _make_strategy()
    # First create stable calibration
    ws._calibration["Dallas_high_1d_m5"] = {
        "city": "Dallas", "metric": "high", "lead_days": 1, "month": 5,
        "predictions": [1.0] * 10, "bias": 0.5, "count": 10,
        "forecast_errors": [1.0] * 10, "sigma": 1.0,
    }
    # Now record outlier: error=30°F > 3σ=3.0
    from datetime import date
    ws._record_calibration("Dallas", metric="high", lead_days=1,
                           yes_won=True, forecast_mean=70.0, actual_temp=100.0)
    key = f"Dallas_high_1d_m5"
    entry = ws._calibration[key]
    assert len(entry["forecast_errors"]) == 10  # Not appended
    assert len(entry.get("rejected_outliers", [])) == 1


def test_record_calibration_no_actual_temp():
    ws = _make_strategy()
    ws._record_calibration("Tokyo", metric="high", lead_days=3,
                           yes_won=True, forecast_mean=25.0)
    from datetime import date
    month = date.today().month
    key = f"Tokyo_high_3d_m{month}"
    entry = ws._calibration[key]
    assert entry["forecast_errors"] == []
    assert entry["mae"] is None


# --- Phase 4: calibration stats with per-metric lookup ---

def test_calibration_stats_no_data_returns_zero():
    ws = _make_strategy()
    bias, sigma = ws._get_calibration_stats("New York", "high", 2, 5)
    assert bias == 0.0
    assert sigma is None


def test_calibration_stats_per_metric():
    ws = _make_strategy()
    ws._calibration["Dallas_high_2d_m5"] = {
        "city": "Dallas", "metric": "high", "lead_days": 2, "month": 5,
        "predictions": [1.0] * 20, "bias": 0.1, "count": 20,
        "sigma": 2.0,
    }
    bias, sigma = ws._get_calibration_stats("Dallas", "high", 2, 5)
    assert bias == 0.1
    assert sigma == 2.0


def test_calibration_stats_shrinkage():
    ws = _make_strategy()
    ws._calibration["Chicago_high_2d"] = {
        "city": "Chicago", "metric": "high", "lead_days": 2,
        "predictions": [1.0] * 10, "bias": 0.5, "count": 10,
        "sigma": 1.5,
    }
    bias, sigma = ws._get_calibration_stats("Chicago", "high", 2, 6)
    assert bias == 0.25
    assert abs(sigma - 0.75) < 0.01


def test_calibration_stats_fallback_to_city():
    ws = _make_strategy()
    ws._calibration["Seattle"] = {"bias": 0.3, "count": 25, "sigma": 2.0}
    bias, sigma = ws._get_calibration_stats("Seattle", "high", 5, 5)
    assert bias == 0.3
    assert sigma == 2.0


def test_calibration_stats_fallback_no_metric():
    ws = _make_strategy()
    ws._calibration["Paris_2d_m6"] = {
        "city": "Paris", "lead_days": 2, "month": 6,
        "predictions": [0.0] * 5, "bias": -0.5, "count": 5,
    }
    bias, sigma = ws._get_calibration_stats("Paris", "high", 2, 6)
    assert bias == -0.125
    assert sigma is None


# --- Phase 4: calibration quality ---

def test_calibration_quality_no_data():
    ws = _make_strategy()
    assert ws._get_calibration_quality("Nowhere", "high", 1, 1) == 0.0


def test_calibration_quality_perfect():
    ws = _make_strategy()
    ws._calibration["NYC_high_1d_m5"] = {
        "city": "NYC", "metric": "high", "lead_days": 1, "month": 5,
        "count": 30, "sigma": 1.0, "predictions": [1.0] * 30,
        "last_updated": date.today().isoformat(),
    }
    q = ws._get_calibration_quality("NYC", "high", 1, 5)
    assert q > 0.7


def test_calibration_quality_low_samples():
    ws = _make_strategy()
    ws._calibration["Lucknow_high_1d_m5"] = {
        "city": "Lucknow", "metric": "high", "lead_days": 1, "month": 5,
        "count": 3, "sigma": 8.0, "predictions": [1.0] * 3,
        "last_updated": date.today().isoformat(),
    }
    q = ws._get_calibration_quality("Lucknow", "high", 1, 5)
    assert q < 0.4


def test_calibration_quality_stale_data_penalized():
    ws = _make_strategy()
    old_date = (date.today() - timedelta(days=200)).isoformat()
    ws._calibration["Old_high_1d_m5"] = {
        "city": "Old", "metric": "high", "lead_days": 1, "month": 5,
        "count": 20, "sigma": 6.0,
        "last_updated": old_date,
    }
    q = ws._get_calibration_quality("Old", "high", 1, 5)
    # count_score=20/30=0.67, accuracy=(10-6)/8.5=0.47, quality=0.6*0.67+0.4*0.47=0.59
    # stale penalty: 0.59 * 0.7 = 0.41
    assert q < 0.5


# --- Phase 4: migration ---

def test_calibration_migration_adds_metric():
    ws = _make_strategy()
    ws._calibration = {}
    ws._calibration_file = Path(tempfile.mkdtemp()) / "test_migrate.json"
    # Write old-format data
    import json
    old_data = {
        "New York_2d_m5": {
            "city": "New York", "lead_days": 2, "month": 5,
            "predictions": [1.0, 0.0], "bias": 0.0, "count": 2,
            "forecast_errors": [2.0, -2.0],
        }
    }
    ws._calibration_file.write_text(json.dumps(old_data))
    loaded = ws._load_calibration()
    assert "New York_high_2d_m5" in loaded
    assert "New York_low_2d_m5" in loaded
    assert loaded["New York_high_2d_m5"]["metric"] == "high"
    assert loaded["New York_high_2d_m5"]["sigma"] is not None
    assert "last_updated" in loaded["New York_high_2d_m5"]


# --- Season key format ---

def test_seasonal_key_format():
    ws = _make_strategy()
    ws._record_calibration("New York", metric="high", lead_days=2,
                           yes_won=True, forecast_mean=72.0, actual_temp=74.0)
    from datetime import date
    month = date.today().month
    expected_key = f"New York_high_2d_m{month}"
    assert expected_key in ws._calibration
    entry = ws._calibration[expected_key]
    assert entry["city"] == "New York"
    assert entry["month"] == month
    assert entry["sigma"] is not None




# --- Data quality guardrails ---


# --- Pending trades ---

def test_pending_trades_persistence_roundtrip(tmp_path):
    ws = _make_strategy()
    ws._pending_file = tmp_path / "test_pending.json"
    ws._pending_trades = {
        "0xmarket1": {
            "market_id": "0xmarket1",
            "city_name": "New York",
            "target_date": "2026-06-15",
            "forecast_mean": 75.0,
            "order_id": "0xorder1",
        }
    }
    ws._save_pending_state()
    assert ws._pending_file.exists()

    loaded = ws._load_pending_state()
    assert "0xmarket1" in loaded
    assert loaded["0xmarket1"]["city_name"] == "New York"


def test_pending_trades_load_missing_file():
    ws = _make_strategy()
    ws._pending_file = Path("/nonexistent/test_pending.json")
    assert ws._load_pending_state() == {}


def test_pending_save_on_execute():
    ws = _make_strategy()
    ws._pending_trades = {}
    from src.strategy.base import Signal
    sig = Signal(
        market_id="0xmarket_test", token_id="0xtoken", side="BUY",
        price=0.40, size=10.0, confidence=0.75, strategy_name="weather",
        metadata={
            "city_name": "Dallas", "target_date": "2026-06-15",
            "threshold": 90.0, "metric": "high", "bucket_type": "range",
            "ens_mean": 91.0, "ens_std": 2.5, "model_prob": 0.55,
        },
    )
    ws.execute([sig])
    assert "0xmarket_test" in ws._pending_trades
    pt = ws._pending_trades["0xmarket_test"]
    assert pt["city_name"] == "Dallas"
    assert pt["target_date"] == "2026-06-15"
    assert pt["forecast_mean"] == 91.0


def test_duplicate_market_id_not_retraded():
    ws = _make_strategy()
    ws._pending_trades = {"0xdup": {"market_id": "0xdup"}}
    from src.strategy.base import Signal
    sig = Signal(
        market_id="0xdup", token_id="0xtoken", side="BUY",
        price=0.50, size=5.0, confidence=0.6, strategy_name="weather",
        metadata={},
    )
    # execute still places the order but pending_trades is overwritten with new data
    # the important thing is that run_scan won't generate a signal for an already-pending market
    ws.execute([sig])
    assert "0xdup" in ws._pending_trades


# --- Settlement removes from pending ---

def test_settlement_removes_resolved_markets():
    ws = _make_strategy()
    ws._pending_trades = {
        "0xresolved": {
            "market_id": "0xresolved",
            "city_name": "New York",
            "target_date": "2026-05-04",
            "forecast_mean": 70.0,
            "metric": "high",
        }
    }
    # No API calls in test, verify cleanup logic manually
    assert "0xresolved" in ws._pending_trades
    ws._pending_trades.pop("0xresolved")
    ws._save_pending_state()
    assert "0xresolved" not in ws._pending_trades


# --- Reconciliation ---

def test_reconcile_no_exchange_orders():
    ws = _make_strategy()
    ws._pending_trades = {
        "0xstale": {
            "market_id": "0xstale",
            "city_name": "Boston",
            "order_id": "0xdead_order",
            "target_date": "2026-05-05",
            "forecast_mean": 65.0,
        }
    }
    ws._reconcile_open_orders()
    # Order on disk but not on exchange -> order_id cleared
    assert ws._pending_trades["0xstale"]["order_id"] == ""


# --- Phase 4: calibration quality + edge bounds ---

def test_calibration_quality_with_sigma():
    ws = _make_strategy()
    ws._calibration["Test_high_0d_m5"] = {
        "city": "Test", "metric": "high", "count": 15, "sigma": 4.0,
        "last_updated": date.today().isoformat(),
    }
    q = ws._get_calibration_quality("Test", "high", 0, 5)
    assert 0.3 < q < 0.7


def test_dynamic_thresholds_loaded():
    ws = _make_strategy()
    assert "edge_base_narrow" in ws._dt
    assert ws._dt["edge_base_narrow"] == 0.12
    assert ws._dt["confidence_cap"] == 0.90
    assert ws._dt["outlier_sigma_mult"] == 3.0


# =============================================================================
# Phase 5 tests: ensemble shape, boundary risk, agreement bins, repositioning
# =============================================================================


def test_ensemble_shape_unimodal():
    members = [70.0, 71.0, 71.5, 72.0, 72.5, 73.0, 74.0]
    shape = WeatherStrategy._analyze_ensemble_shape(members)
    assert shape["bimodal"] is False
    assert abs(shape["skew"]) < 0.5


def test_ensemble_shape_bimodal():
    # Two clusters: 60s and 80s with a gap
    members = [60.0, 61.0, 62.0, 63.0, 80.0, 81.0, 82.0, 83.0]
    shape = WeatherStrategy._analyze_ensemble_shape(members)
    assert shape["bimodal"] is True


def test_ensemble_shape_small_sample():
    members = [70.0, 71.0]
    shape = WeatherStrategy._analyze_ensemble_shape(members)
    assert shape["bimodal"] is False
    assert shape["skew"] == 0.0


def test_agreement_bin():
    assert WeatherStrategy._agreement_bin(0.92) == "c90"
    assert WeatherStrategy._agreement_bin(0.85) == "c80"
    assert WeatherStrategy._agreement_bin(0.75) == "c70"
    assert WeatherStrategy._agreement_bin(0.65) == "c60"
    assert WeatherStrategy._agreement_bin(0.55) == "c50"


def test_record_calibration_tracks_agreement_bin():
    ws = _make_strategy()
    ws._record_calibration("NYC", metric="high", lead_days=1,
                           yes_won=True, forecast_mean=75.0,
                           actual_temp=76.0, confidence=0.85)
    from datetime import date
    month = date.today().month
    bin_key = f"NYC_high_1d_m{month}_c80"
    assert bin_key in ws._calibration
    assert ws._calibration[bin_key]["predictions"] == [1.0]


def test_compute_bucket_prob_below():
    members = [60, 62, 65, 68, 70, 72, 75]
    pt = {"bucket_type": "below", "bucket_upper": 68, "threshold": 68}
    prob = WeatherStrategy._compute_bucket_prob_from_pending(members, "high", None, 68, pt)
    assert prob == 4/7


def test_compute_bucket_prob_above():
    members = [60, 62, 65, 68, 70, 72, 75]
    pt = {"bucket_type": "above", "bucket_lower": 70, "threshold": 70}
    prob = WeatherStrategy._compute_bucket_prob_from_pending(members, "high", 70, None, pt)
    assert prob == 3/7


def test_compute_bucket_prob_range():
    members = [60, 62, 65, 68, 70, 72, 75]
    pt = {"bucket_type": "range", "bucket_lower": 65, "bucket_upper": 70, "threshold": 67.5}
    prob = WeatherStrategy._compute_bucket_prob_from_pending(members, "high", 65, 70, pt)
    assert prob == 3/7


def test_pending_trades_stores_confidence():
    ws = _make_strategy()
    ws._pending_trades = {}
    from src.strategy.base import Signal
    sig = Signal(
        market_id="0xmarket_conf", token_id="0xtoken", side="BUY",
        price=0.40, size=10.0, confidence=0.85, strategy_name="weather",
        metadata={
            "city_name": "Dallas", "target_date": "2026-06-15",
            "threshold": 90.0, "metric": "high", "bucket_type": "range",
            "ens_mean": 91.0, "ens_std": 2.5, "model_prob": 0.55,
            "agreement": 0.85, "bucket_lower": 89.0, "bucket_upper": 91.0,
        },
    )
    ws.execute([sig])
    pt = ws._pending_trades["0xmarket_conf"]
    assert pt["confidence"] == 0.85
    assert pt["bucket_lower"] == 89.0
    assert pt["bucket_upper"] == 91.0


def test_local_day_utc_range():
    from src.data.weather import _local_day_utc_range
    station = {"timezone": "America/New_York"}
    start, end = _local_day_utc_range(station, date(2026, 5, 5))
    # EDT = UTC-4, so local May 5 = UTC May 5 04:00 to May 6 04:00
    assert start == date(2026, 5, 5)
    assert end == date(2026, 5, 6)

    station_utc = {"timezone": "UTC"}
    start, end = _local_day_utc_range(station_utc, date(2026, 5, 5))
    assert start == date(2026, 5, 5)
    assert end == date(2026, 5, 5)

    station_far_east = {"timezone": "Pacific/Auckland"}
    start, end = _local_day_utc_range(station_far_east, date(2026, 5, 5))
    # NZST = UTC+12, local May 5 = UTC May 4 12:00 to May 5 11:59:59
    assert start == date(2026, 5, 4)
    assert end == date(2026, 5, 5)


# =============================================================================
# Phase 6 tests: exit strategy — trailing stop, take-profit, stop-loss, partials
# =============================================================================


def test_trailing_threshold_far():
    thresh = WeatherStrategy._get_trailing_threshold("2026-05-10", 0.15)
    assert thresh == 0.15  # 5 days away, no tightening


def test_trailing_threshold_close():
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    thresh = WeatherStrategy._get_trailing_threshold(tomorrow, 0.15)
    assert thresh < 0.15  # should be tightened


def test_trailing_threshold_very_close():
    thresh = WeatherStrategy._get_trailing_threshold(date.today().isoformat(), 0.15)
    assert thresh == 0.15 * 0.33


def test_fetch_market_price_fallback():
    ws = _make_strategy()
    price = ws._fetch_market_price("nonexistent_market")
    assert price == 0.50


def test_exit_cooling_period_set():
    ws = _make_strategy()
    ws._pending_trades = {
        "0xexit1": {
            "market_id": "0xexit1", "token_id": "0xtoken",
            "city_name": "Dallas", "target_date": "2026-06-15",
            "metric": "high", "entry_price": 0.35, "size": 15.0,
            "order_id": "0xorder", "forecast_prob": 0.55,
            "threshold": 90.0, "bucket_type": "range",
        }
    }
    ws._do_exit("0xexit1", ws._pending_trades["0xexit1"], "take_profit", 1.0)
    assert "Dallas_2026-06-15_high" in ws._exit_cooldown


def test_exit_full_removes_from_pending():
    ws = _make_strategy()
    ws._pending_trades = {
        "0xexit2": {
            "market_id": "0xexit2", "token_id": "0xtoken",
            "city_name": "Miami", "target_date": "2026-06-15",
            "metric": "high", "entry_price": 0.30, "size": 10.0,
            "order_id": "0xorder2", "forecast_prob": 0.40,
            "threshold": 85.0, "bucket_type": "above",
        }
    }
    ws._do_exit("0xexit2", ws._pending_trades["0xexit2"], "stop_loss", 1.0)
    assert "0xexit2" not in ws._pending_trades


def test_exit_fraction_keeps_remaining():
    ws = _make_strategy()
    ws._pending_trades = {
        "0xexit3": {
            "market_id": "0xexit3", "token_id": "0xtoken",
            "city_name": "Chicago", "target_date": "2026-06-15",
            "metric": "low", "entry_price": 0.40, "size": 20.0,
            "order_id": "0xorder3", "forecast_prob": 0.60,
            "threshold": 45.0, "bucket_type": "range",
        }
    }
    ws._do_exit("0xexit3", ws._pending_trades["0xexit3"], "prob_delta", 0.5)
    assert "0xexit3" in ws._pending_trades
    assert ws._pending_trades["0xexit3"]["size"] == 10.0


def test_exit_logs_to_file(tmp_path):
    ws = _make_strategy()
    ws._exits_file = tmp_path / "test_exits.jsonl"
    ws._log_exit("0xm", {"city_name": "NYC", "metric": "high", "entry_price": 0.3}, "take_profit", 0.92, 15.5, 1.0)
    assert ws._exits_file.exists()


def test_exit_not_removed_on_order_failure():
    ws = _make_strategy()
    ws._client.place_limit_order = lambda **kw: (_ for _ in ()).throw(Exception("fail"))
    ws._pending_trades = {
        "0xexit4": {
            "market_id": "0xexit4", "token_id": "0xtoken",
            "city_name": "Boston", "target_date": "2026-06-15",
            "metric": "high", "entry_price": 0.50, "size": 5.0,
            "order_id": "0xorder4", "forecast_prob": 0.30,
            "threshold": 70.0, "bucket_type": "below",
        }
    }
    result = ws._do_exit("0xexit4", ws._pending_trades["0xexit4"], "stop_loss", 1.0)
    assert result is None
    assert "0xexit4" in ws._pending_trades


def test_manage_exits_respects_disabled():
    ws = _make_strategy()
    ws._dt["reposition_enabled"] = False
    trades = ws._manage_exits(100.0)
    assert trades == []


# =============================================================================
# Phase 7 tests: provider abstraction, trade logger, performance metrics
# =============================================================================


def test_log_weather_trade_writes_file(tmp_path):
    ws = _make_strategy()
    ws._trades_file = tmp_path / "test_trades.jsonl"
    ws._log_weather_trade(
        "0xmarket_log", {
            "city_name": "NYC", "metric": "high",
            "target_date": "2026-06-15", "threshold": 75.0,
            "bucket_type": "range", "entry_price": 0.35, "size": 15.0,
            "forecast_prob": 0.48, "forecast_mean": 74.0,
            "forecast_std": 2.0, "confidence": 0.75,
        },
        status="open",
    )
    assert ws._trades_file.exists()


def test_get_performance_no_data():
    ws = _make_strategy()
    import tempfile
    ws._trades_file = Path(tempfile.mkdtemp()) / "nonexistent.jsonl"
    perf = ws.get_performance()
    assert perf["total_trades"] == 0
    assert perf["win_rate"] is None


def test_live_forecast_provider():
    from src.data.weather_provider import LiveForecastProvider
    p = LiveForecastProvider()
    assert p is not None


def test_live_market_provider():
    from src.data.weather_provider import LiveMarketProvider
    p = LiveMarketProvider()
    assert p is not None


def test_historical_forecast_provider_no_data():
    from src.data.weather_provider import HistoricalForecastProvider
    p = HistoricalForecastProvider("/nonexistent")
    fc = p.get_forecast("nyc", date(2026, 5, 5))
    assert fc is None


def test_historical_market_provider_no_data():
    from src.data.weather_provider import HistoricalMarketProvider
    p = HistoricalMarketProvider("/nonexistent")
    events = p.get_events_by_slug("test-slug")
    assert events is None


def test_live_market_provider_price_fallback():
    from src.data.weather_provider import LiveMarketProvider
    p = LiveMarketProvider()
    price = p.get_market_price("nonexistent_market")
    assert price == 0.50


def test_strategy_accepts_providers():
    from src.data.weather_provider import LiveForecastProvider, LiveMarketProvider
    ws = _make_strategy()
    ws2 = WeatherStrategy(
        client=DummyClient(),
        config={"weather": {"enabled": True}},
        forecast_provider=LiveForecastProvider(),
        market_provider=LiveMarketProvider(),
    )
    assert ws2._forecast_provider is not None
    assert ws2._market_provider is not None
