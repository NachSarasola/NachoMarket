"""Blacklist de mercados por round-trip win rate (Fase 4).

Calcula WR emparejando fills BUY→SELL por market_id en FIFO desde trades.jsonl.
Si WR < threshold con >= min_trades round-trips → blacklist por N días.

Patrones GoF usados:
- Repository: MarketBlacklist abstrae la persistencia en blacklist.json
- Factory Method: from_config() para construcción desde YAML

Programación funcional:
- _compute_round_trips(): función pura sin efectos secundarios
- _pair_fifo(): transformación de datos sin mutación de estado externo
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.blacklist")

_BLACKLIST_FILE = Path("data/blacklist.json")
_TRADES_FILE = Path("data/trades.jsonl")
_DEFAULT_MIN_TRADES = 10
_DEFAULT_WR_THRESHOLD = 0.30
_DEFAULT_BLACKLIST_DAYS = 7.0


@dataclass(frozen=True)
class RoundTrip:
    """Value object inmutable que representa un par buy+sell emparejado (FIFO)."""

    market_id: str
    buy_price: float
    sell_price: float
    size: float
    won: bool = field(init=False)

    def __post_init__(self) -> None:
        # frozen=True requiere object.__setattr__ para campos derivados
        object.__setattr__(self, "won", self.sell_price > self.buy_price)


class MarketBlacklist:
    """Gestiona la blacklist de mercados por round-trip win rate.

    Ciclo de vida:
        bl = MarketBlacklist.from_config(config)
        bl.refresh()                 # recalcula desde trades.jsonl
        if bl.is_blacklisted(mid):   # gate antes de operar
            return []

    El estado se persiste en data/blacklist.json para sobrevivir reinicios.
    """

    def __init__(
        self,
        trades_file: Path = _TRADES_FILE,
        blacklist_file: Path = _BLACKLIST_FILE,
        min_trades: int = _DEFAULT_MIN_TRADES,
        wr_threshold: float = _DEFAULT_WR_THRESHOLD,
        blacklist_days: float = _DEFAULT_BLACKLIST_DAYS,
    ) -> None:
        self._trades_file = trades_file
        self._blacklist_file = blacklist_file
        self._min_trades = min_trades
        self._wr_threshold = wr_threshold
        self._blacklist_secs = blacklist_days * 86_400.0
        # {market_id: expire_unix_timestamp}
        self._blacklisted: dict[str, float] = {}
        self._load()

    # ------------------------------------------------------------------
    # Factory Method (GoF) — construcción desde config YAML
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MarketBlacklist":
        """Crea una instancia a partir del bloque 'blacklist' en config YAML."""
        bl = config.get("blacklist", {})
        return cls(
            min_trades=bl.get("min_trades", _DEFAULT_MIN_TRADES),
            wr_threshold=bl.get("wr_threshold", _DEFAULT_WR_THRESHOLD),
            blacklist_days=bl.get("blacklist_days", _DEFAULT_BLACKLIST_DAYS),
        )

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def is_blacklisted(self, market_id: str) -> bool:
        """True si el mercado está en blacklist activa (no expirada)."""
        expire = self._blacklisted.get(market_id)
        if expire is None:
            return False
        if time.time() > expire:
            del self._blacklisted[market_id]
            self._save()
            return False
        return True

    def refresh(self) -> list[str]:
        """Recalcula WR desde trades.jsonl y actualiza la blacklist.

        Returns:
            Lista de market_ids recién añadidos a blacklist en este refresh.
        """
        round_trips = _compute_round_trips(self._trades_file)
        newly_blacklisted: list[str] = []

        by_market = _group_by_market(round_trips)

        for market_id, trips in by_market.items():
            if len(trips) < self._min_trades:
                continue
            wr = _win_rate(trips)
            if wr < self._wr_threshold and not self.is_blacklisted(market_id):
                expire = time.time() + self._blacklist_secs
                self._blacklisted[market_id] = expire
                newly_blacklisted.append(market_id)
                logger.warning(
                    "Blacklisted %s: WR=%.1f%% (%d round-trips, threshold=%.0f%%)",
                    market_id[:14],
                    wr * 100,
                    len(trips),
                    self._wr_threshold * 100,
                )

        if newly_blacklisted:
            self._save()

        return newly_blacklisted

    def manual_add(self, market_id: str, days: float | None = None) -> None:
        """Añade manualmente un mercado a la blacklist (usado desde tests/bot)."""
        secs = (days or _DEFAULT_BLACKLIST_DAYS) * 86_400.0
        self._blacklisted[market_id] = time.time() + secs
        self._save()
        logger.info("Manual blacklist: %s por %.0f días", market_id[:14], days or _DEFAULT_BLACKLIST_DAYS)

    def get_active(self) -> dict[str, float]:
        """Retorna copia del estado actual (market_id → expire_timestamp)."""
        now = time.time()
        return {mid: exp for mid, exp in self._blacklisted.items() if exp > now}

    def get_stats(self) -> dict[str, Any]:
        """Estadísticas para /status de Telegram."""
        active = self.get_active()
        return {
            "blacklisted_count": len(active),
            "min_trades": self._min_trades,
            "wr_threshold": self._wr_threshold,
            "blacklist_days": self._blacklist_secs / 86_400.0,
            "markets": {
                mid: {
                    "expires_in_hours": round((exp - time.time()) / 3600, 1),
                }
                for mid, exp in active.items()
            },
        }

    # ------------------------------------------------------------------
    # Repository — persistencia
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not self._blacklist_file.exists():
            return
        try:
            data = json.loads(self._blacklist_file.read_text(encoding="utf-8"))
            self._blacklisted = {
                mid: exp
                for mid, exp in data.items()
                if isinstance(exp, (int, float)) and time.time() < exp
            }
        except Exception:
            logger.exception("Error cargando blacklist.json")

    def _save(self) -> None:
        try:
            self._blacklist_file.parent.mkdir(parents=True, exist_ok=True)
            self._blacklist_file.write_text(
                json.dumps(self._blacklisted, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.exception("Error guardando blacklist.json")


# ------------------------------------------------------------------
# Funciones puras — programación funcional, sin efectos secundarios
# ------------------------------------------------------------------

def _compute_round_trips(trades_file: Path) -> list[RoundTrip]:
    """Función pura: lee trades.jsonl y empareja buy+sell (FIFO) por market_id.

    No modifica ningún estado externo. Retorna lista inmutable de RoundTrip.
    """
    if not trades_file.exists():
        return []

    raw_trades = _load_valid_trades(trades_file)
    by_market = _group_raw_by_market(raw_trades)

    return [
        rt
        for market_id, trades in by_market.items()
        for rt in _pair_fifo(market_id, trades)
    ]


def _load_valid_trades(trades_file: Path) -> list[dict[str, Any]]:
    """Carga y filtra trades con status válido desde trades.jsonl."""
    trades: list[dict[str, Any]] = []
    try:
        with open(trades_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    if (
                        t.get("status") not in ("error",)
                        and t.get("market_id")
                        and t.get("side") in ("BUY", "SELL")
                        and float(t.get("price", 0)) > 0
                    ):
                        trades.append(t)
                except (json.JSONDecodeError, ValueError):
                    continue
    except OSError:
        logger.exception("Error leyendo trades.jsonl para blacklist")
    return trades


def _group_raw_by_market(trades: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Agrupa trades por market_id (función pura, sin mutación externa)."""
    result: dict[str, list[dict[str, Any]]] = {}
    for t in trades:
        result.setdefault(t["market_id"], []).append(t)
    return result


def _pair_fifo(market_id: str, trades: list[dict[str, Any]]) -> list[RoundTrip]:
    """Empareja compras y ventas FIFO para un mercado dado (función pura)."""
    sorted_trades = sorted(trades, key=lambda t: t.get("timestamp", ""))
    buys = [t for t in sorted_trades if t.get("side") == "BUY"]
    sells = [t for t in sorted_trades if t.get("side") == "SELL"]

    return [
        RoundTrip(
            market_id=market_id,
            buy_price=float(buy.get("price", 0)),
            sell_price=float(sell.get("price", 0)),
            size=float(buy.get("size", 0)),
        )
        for buy, sell in zip(buys, sells)
    ]


def _group_by_market(round_trips: list[RoundTrip]) -> dict[str, list[RoundTrip]]:
    """Agrupa RoundTrips por market_id (función pura)."""
    result: dict[str, list[RoundTrip]] = {}
    for rt in round_trips:
        result.setdefault(rt.market_id, []).append(rt)
    return result


def _win_rate(trips: list[RoundTrip]) -> float:
    """Calcula WR como fracción de round-trips ganados (función pura)."""
    if not trips:
        return 0.0
    return sum(1 for rt in trips if rt.won) / len(trips)
