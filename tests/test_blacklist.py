"""Tests para src/risk/blacklist.py — round-trip WR blacklist."""

import json
import time
from pathlib import Path

import pytest

from src.risk.blacklist import (
    MarketBlacklist,
    RoundTrip,
    _compute_round_trips,
    _pair_fifo,
    _win_rate,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def trades_file(tmp_path: Path) -> Path:
    return tmp_path / "trades.jsonl"


@pytest.fixture
def blacklist_file(tmp_path: Path) -> Path:
    return tmp_path / "blacklist.json"


def _write_trades(path: Path, trades: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t) + "\n")


def _make_trade(
    market_id: str,
    side: str,
    price: float,
    size: float = 10.0,
    status: str = "submitted",
    ts: str = "2026-01-01T00:00:00+00:00",
) -> dict:
    return {
        "market_id": market_id,
        "side": side,
        "price": price,
        "size": size,
        "status": status,
        "timestamp": ts,
    }


# ------------------------------------------------------------------
# Tests: RoundTrip
# ------------------------------------------------------------------

class TestRoundTrip:
    def test_won_true_when_sell_above_buy(self) -> None:
        rt = RoundTrip(market_id="m1", buy_price=0.40, sell_price=0.50, size=10.0)
        assert rt.won is True

    def test_won_false_when_sell_below_buy(self) -> None:
        rt = RoundTrip(market_id="m1", buy_price=0.50, sell_price=0.40, size=10.0)
        assert rt.won is False

    def test_won_false_when_equal(self) -> None:
        rt = RoundTrip(market_id="m1", buy_price=0.50, sell_price=0.50, size=10.0)
        assert rt.won is False

    def test_round_trip_mutable(self) -> None:
        """RoundTrip es mutable (clase normal, no frozen dataclass)."""
        rt = RoundTrip(market_id="m1", buy_price=0.40, sell_price=0.50, size=10.0)
        rt.buy_price = 0.99
        assert rt.buy_price == 0.99


# ------------------------------------------------------------------
# Tests: funciones puras
# ------------------------------------------------------------------

class TestPairFifo:
    def test_empareja_en_orden_cronologico(self) -> None:
        trades = [
            _make_trade("m1", "BUY", 0.40, ts="2026-01-01T00:00:00+00:00"),
            _make_trade("m1", "SELL", 0.50, ts="2026-01-01T01:00:00+00:00"),
            _make_trade("m1", "BUY", 0.42, ts="2026-01-01T02:00:00+00:00"),
            _make_trade("m1", "SELL", 0.38, ts="2026-01-01T03:00:00+00:00"),
        ]
        pairs = _pair_fifo("m1", trades)
        assert len(pairs) == 2
        assert pairs[0].buy_price == pytest.approx(0.40)
        assert pairs[0].sell_price == pytest.approx(0.50)
        assert pairs[1].buy_price == pytest.approx(0.42)
        assert pairs[1].sell_price == pytest.approx(0.38)

    def test_sobra_buy_sin_sell(self) -> None:
        trades = [
            _make_trade("m1", "BUY", 0.40),
            _make_trade("m1", "BUY", 0.42),
        ]
        pairs = _pair_fifo("m1", trades)
        assert len(pairs) == 0

    def test_empate_exacto(self) -> None:
        trades = [
            _make_trade("m1", "BUY", 0.45),
            _make_trade("m1", "SELL", 0.45),
        ]
        pairs = _pair_fifo("m1", trades)
        assert len(pairs) == 1
        assert pairs[0].won is False


class TestWinRate:
    def test_todos_ganadores(self) -> None:
        trips = [
            RoundTrip("m1", 0.40, 0.50, 10.0),
            RoundTrip("m1", 0.45, 0.55, 10.0),
        ]
        assert _win_rate(trips) == pytest.approx(1.0)

    def test_todos_perdedores(self) -> None:
        trips = [
            RoundTrip("m1", 0.50, 0.40, 10.0),
            RoundTrip("m1", 0.55, 0.45, 10.0),
        ]
        assert _win_rate(trips) == pytest.approx(0.0)

    def test_mitad_mitad(self) -> None:
        trips = [
            RoundTrip("m1", 0.40, 0.50, 10.0),
            RoundTrip("m1", 0.50, 0.40, 10.0),
        ]
        assert _win_rate(trips) == pytest.approx(0.5)

    def test_lista_vacia(self) -> None:
        assert _win_rate([]) == pytest.approx(0.0)


class TestComputeRoundTrips:
    def test_archivo_inexistente_retorna_vacio(self, tmp_path: Path) -> None:
        result = _compute_round_trips(tmp_path / "noexiste.jsonl")
        assert result == []

    def test_emparejar_multiples_mercados(self, trades_file: Path) -> None:
        _write_trades(trades_file, [
            _make_trade("mA", "BUY", 0.40),
            _make_trade("mA", "SELL", 0.50),
            _make_trade("mB", "BUY", 0.30),
            _make_trade("mB", "SELL", 0.20),
        ])
        result = _compute_round_trips(trades_file)
        assert len(result) == 2
        market_ids = {rt.market_id for rt in result}
        assert market_ids == {"mA", "mB"}


# ------------------------------------------------------------------
# Tests: MarketBlacklist
# ------------------------------------------------------------------

class TestMarketBlacklist:
    def test_mercado_no_en_blacklist_por_defecto(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        assert bl.is_blacklisted("0xabc") is False

    def test_manual_add_blacklists(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        bl.manual_add("0xabc", days=1)
        assert bl.is_blacklisted("0xabc") is True

    def test_expiracion(self, trades_file: Path, blacklist_file: Path) -> None:
        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        bl._blacklisted["0xabc"] = time.time() - 1
        assert bl.is_blacklisted("0xabc") is False

    def test_remove(self, trades_file: Path, blacklist_file: Path) -> None:
        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        bl.manual_add("0xabc", days=7)
        assert bl.is_blacklisted("0xabc") is True
        bl.remove("0xabc")
        assert bl.is_blacklisted("0xabc") is False

    def test_refresh_blacklistea_mercado_con_wr_bajo(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        """refresh() blacklistea mercados con WR < 30% y >= 10 trades (hardcoded)."""
        trades = []
        for i in range(10):
            sell_price = 0.50 if i < 2 else 0.40  # 2 wins, 8 losses → WR=20%
            trades.append(_make_trade("0xbad", "BUY", 0.45, ts=f"2026-01-{i+1:02d}T00:00:00+00:00"))
            trades.append(_make_trade("0xbad", "SELL", sell_price, ts=f"2026-01-{i+1:02d}T01:00:00+00:00"))
        _write_trades(trades_file, trades)

        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        newly = bl.refresh()
        assert "0xbad" in newly
        assert bl.is_blacklisted("0xbad") is True

    def test_refresh_no_blacklistea_mercado_con_wr_alto(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        trades = []
        for i in range(10):
            sell_price = 0.55 if i < 8 else 0.40  # 8 wins, 2 losses → WR=80%
            trades.append(_make_trade("0xgood", "BUY", 0.45, ts=f"2026-01-{i+1:02d}T00:00:00+00:00"))
            trades.append(_make_trade("0xgood", "SELL", sell_price, ts=f"2026-01-{i+1:02d}T01:00:00+00:00"))
        _write_trades(trades_file, trades)

        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        newly = bl.refresh()
        assert "0xgood" not in newly
        assert bl.is_blacklisted("0xgood") is False

    def test_refresh_respeta_min_trades_hardcoded(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        """refresh() hardcodea min_trades=10: <10 pares no blacklistea aunque WR=0%."""
        trades = []
        for i in range(5):
            trades.append(_make_trade("0xfew", "BUY", 0.45, ts=f"2026-01-{i+1:02d}T00:00:00+00:00"))
            trades.append(_make_trade("0xfew", "SELL", 0.40, ts=f"2026-01-{i+1:02d}T01:00:00+00:00"))
        _write_trades(trades_file, trades)

        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        newly = bl.refresh()
        assert "0xfew" not in newly

    def test_from_config(self) -> None:
        """from_config lee paths de config dict."""
        config = {"blacklist": {"trades_file": "data/custom_trades.jsonl", "blacklist_file": "data/custom_bl.json"}}
        bl = MarketBlacklist.from_config(config)
        assert bl._trades_file == Path("data/custom_trades.jsonl")
        assert bl._blacklist_file == Path("data/custom_bl.json")

    def test_from_config_defaults(self) -> None:
        """from_config usa defaults si no hay paths en config."""
        bl = MarketBlacklist.from_config({})
        assert bl._trades_file == Path("data/trades.jsonl")
        assert bl._blacklist_file == Path("data/blacklist.json")

    def test_persistencia_en_disco(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        bl.manual_add("0xpersist", days=1)
        assert blacklist_file.exists()

        bl2 = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        assert bl2.is_blacklisted("0xpersist") is True

    def test_get_active_solo_no_expirados(
        self, trades_file: Path, blacklist_file: Path
    ) -> None:
        bl = MarketBlacklist(trades_file=trades_file, blacklist_file=blacklist_file)
        bl.manual_add("0xactive", days=1)
        bl._blacklisted["0xexpired"] = time.time() - 1

        active = bl.get_active()
        assert "0xactive" in active
        assert "0xexpired" not in active
