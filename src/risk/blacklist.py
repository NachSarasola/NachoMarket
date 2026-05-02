"""Market blacklist: persiste mercados baneados por bajo performance."""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.blacklist")

BLACKLIST_FILE = Path("data/blacklist.json")
TRADES_FILE = Path("data/trades.jsonl")


def _pair_fifo(market_id: str, trades: list[dict]) -> list["RoundTrip"]:
    """Empareja buys/sells FIFO por market_id."""
    buys = [t for t in trades if t.get("market_id") == market_id and t.get("side", "").upper() == "BUY"]
    sells = [t for t in trades if t.get("market_id") == market_id and t.get("side", "").upper() == "SELL"]
    
    pairs = []
    buy_idx = 0
    sell_idx = 0
    
    while buy_idx < len(buys) and sell_idx < len(sells):
        buy = buys[buy_idx]
        sell = sells[sell_idx]
        
        buy_time = buy.get("timestamp", "")
        sell_time = sell.get("timestamp", "")
        
        if buy_time and sell_time:
            buy_dt = datetime.fromisoformat(buy_time.replace("Z", "+00:00"))
            sell_dt = datetime.fromisoformat(sell_time.replace("Z", "+00:00"))
            if buy_dt <= sell_dt:
                pair = RoundTrip(
                    market_id=market_id,
                    buy_price=float(buy.get("price", 0)),
                    sell_price=float(sell.get("price", 0)),
                    size=float(buy.get("size", 0)),
                    buy_time=buy_time,
                    sell_time=sell_time,
                )
                pairs.append(pair)
                buy_idx += 1
                sell_idx += 1
            else:
                buy_idx += 1
        else:
            break
    
    return pairs


def _compute_round_trips(trades_file: Path) -> list["RoundTrip"]:
    """Lee trades.jsonl y empareja en round-trips."""
    if not trades_file.exists():
        return []
    
    trades = []
    try:
        with open(trades_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        trades.append(json.loads(line))
                    except Exception:
                        continue
    except Exception:
        return []
    
    # Agrupar por market_id
    market_ids = {t.get("market_id") for t in trades if t.get("market_id")}
    
    all_trips = []
    for mid in market_ids:
        trips = _pair_fifo(mid, trades)
        all_trips.extend(trips)
    
    return all_trips


def _win_rate(trips: list["RoundTrip"]) -> float:
    """Calcula win rate de round-trips."""
    if not trips:
        return 0.0
    
    wins = sum(1 for t in trips if t.won)
    return wins / len(trips)


class RoundTrip:
    """Representa un par buy-sell (round-trip)."""
    
    def __init__(
        self,
        market_id: str = "",
        buy_price: float = 0.0,
        sell_price: float = 0.0,
        size: float = 0.0,
        buy_time: str = "",
        sell_time: str = "",
    ) -> None:
        self.market_id = market_id
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.size = size
        self.buy_time = buy_time
        self.sell_time = sell_time
    
    @property
    def won(self) -> bool:
        """True si el round-trip fue ganador."""
        return self.sell_price > self.buy_price


class MarketBlacklist:
    """Gestiona blacklist de mercados basado en win rate y otros criterios."""

    def __init__(
        self,
        trades_file: Path = TRADES_FILE,
        blacklist_file: Path = BLACKLIST_FILE,
    ) -> None:
        self._trades_file = trades_file
        self._blacklist_file = blacklist_file
        self._blacklisted: dict[str, float] = self._load()

    def _load(self) -> dict[str, float]:
        """Carga blacklist persistida."""
        if self._blacklist_file.exists():
            try:
                data = json.loads(self._blacklist_file.read_text("utf-8"))
                # Migrar formato antiguo (list) a nuevo (dict)
                if isinstance(data, list):
                    return {cid: time.time() + 86400 * 7 for cid in data}
                return data
            except Exception:
                logger.debug("No se pudo cargar blacklist.json", exc_info=True)
        return {}

    def _save(self) -> None:
        """Persiste blacklist a disco."""
        try:
            self._blacklist_file.parent.mkdir(parents=True, exist_ok=True)
            self._blacklist_file.write_text(
                json.dumps(self._blacklisted, ensure_ascii=False, indent=2),
                "utf-8",
            )
        except Exception:
            logger.debug("No se pudo guardar blacklist.json", exc_info=True)

    def is_blacklisted(self, market_id: str) -> bool:
        """True si el mercado está en blacklist y no ha expirado."""
        if market_id not in self._blacklisted:
            return False
        until = self._blacklisted[market_id]
        if time.time() < until:
            return True
        # Expirado: remover
        del self._blacklisted[market_id]
        self._save()
        return False

    def manual_add(self, market_id: str, days: int = 7) -> None:
        """Agrega un mercado a la blacklist por N días."""
        self._blacklisted[market_id] = time.time() + days * 86400
        self._save()
        logger.info("Blacklist: agregado %s... por %d días", market_id[:12], days)

    def remove(self, market_id: str) -> None:
        """Remueve un mercado de la blacklist."""
        if market_id in self._blacklisted:
            del self._blacklisted[market_id]
            self._save()
            logger.info("Blacklist: removido %s...", market_id[:12])

    def get_active(self) -> dict[str, float]:
        """Retorna mercados activos en blacklist (no expirados)."""
        now = time.time()
        active = {
            cid: until for cid, until in self._blacklisted.items()
            if until > now
        }
        return active

    def refresh(self) -> list[str]:
        """Actualiza blacklist basado en WR de round-trips. Retorna nuevos agregados."""
        newly = []
        trips = _compute_round_trips(self._trades_file)
        
        # Agrupar por market_id
        by_market: dict[str, list["RoundTrip"]] = {}
        for rt in trips:
            by_market.setdefault(rt.market_id, []).append(rt)
        
        for market_id, market_trips in by_market.items():
            wr = _win_rate(market_trips)
            # Criterio: WR < 30% con al menos 10 trades
            if len(market_trips) >= 10 and wr < 0.30:
                if not self.is_blacklisted(market_id):
                    self.manual_add(market_id, days=7)
                    newly.append(market_id)
        
        return newly

    @staticmethod
    def from_config(config: dict) -> "MarketBlacklist":
        """Crea instancia desde config dict."""
        bl_config = config.get("blacklist", {})
        trades_file = Path(bl_config.get("trades_file", "data/trades.jsonl"))
        blacklist_file = Path(bl_config.get("blacklist_file", "data/blacklist.json"))
        return MarketBlacklist(
            trades_file=trades_file,
            blacklist_file=blacklist_file,
        )
