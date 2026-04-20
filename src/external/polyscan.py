"""PolyScan Whale Tracker — scraper de trades grandes en Polymarket (TODO 5.1).

Consulta el API publico de Polymarket cada 60s para detectar trades >$5000.
Persiste en data/whale_trades.jsonl para uso de las estrategias.

COSTO: $0 (API publica de Polymarket).
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.polyscan")

_WHALE_TRADES_FILE = Path("data/whale_trades.jsonl")
_DEFAULT_MIN_SIZE = 5000.0      # $5000 minimo para considerar whale
_DEFAULT_POLL_INTERVAL = 60.0   # 60s entre consultas
_MAX_STORED_TRADES = 1000       # Limitar archivo a 1000 registros
_POLYMARKET_API = "https://clob.polymarket.com/trades"


@dataclass
class WhaleTrade:
    """Representacion de un trade grande detectado."""
    trade_id: str = ""
    market_id: str = ""
    token_id: str = ""
    side: str = ""            # BUY o SELL
    size: float = 0.0
    price: float = 0.0
    timestamp: float = 0.0
    trader_address: str = ""
    question: str = ""


class WhaleTracker:
    """Detecta y almacena trades grandes de ballenas en Polymarket.

    Uso:
        tracker = WhaleTracker()
        # Llamar periodicamente (cada 60s) via scheduler
        new_trades = tracker.poll()
        # Consultar trades recientes por mercado
        whales = tracker.get_recent_whales("condition_id_123", min_size=5000)
    """

    def __init__(
        self,
        min_size: float = _DEFAULT_MIN_SIZE,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        output_file: str = str(_WHALE_TRADES_FILE),
    ) -> None:
        self._min_size = min_size
        self._poll_interval = poll_interval
        self._output_file = Path(output_file)
        self._last_poll: float = 0.0
        self._seen_trade_ids: set[str] = set()
        self._output_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_seen_ids()

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def poll(self, force: bool = False) -> list[WhaleTrade]:
        """Consulta la API de Polymarket y retorna nuevos whale trades.

        Args:
            force: Si True, ignora el intervalo y consulta inmediatamente.

        Returns:
            Lista de WhaleTrade nuevos detectados.
        """
        now = time.time()
        if not force and (now - self._last_poll) < self._poll_interval:
            return []

        self._last_poll = now
        new_trades = []

        try:
            trades_raw = self._fetch_recent_trades()
            for raw in trades_raw:
                trade = self._parse_trade(raw)
                if trade is None:
                    continue
                if trade.size < self._min_size:
                    continue
                if trade.trade_id in self._seen_trade_ids:
                    continue

                self._seen_trade_ids.add(trade.trade_id)
                new_trades.append(trade)
                self._save_trade(trade)
                logger.info(
                    "🐳 Whale trade: %s %s @ %.4f size=$%.0f | %s...",
                    trade.side, trade.token_id[:8], trade.price,
                    trade.size, trade.market_id[:12],
                )

        except Exception:
            logger.exception("Error en WhaleTracker.poll()")

        return new_trades

    def get_recent_whales(
        self,
        market_id: str,
        min_size: float | None = None,
        lookback_hours: float = 1.0,
    ) -> list[WhaleTrade]:
        """Retorna whale trades recientes para un mercado especifico.

        Args:
            market_id: condition_id del mercado.
            min_size: Override del tamaño minimo.
            lookback_hours: Cuantas horas hacia atras buscar.

        Returns:
            Lista de WhaleTrade filtrada.
        """
        min_sz = min_size or self._min_size
        cutoff = time.time() - lookback_hours * 3600

        return [
            t for t in self._load_trades()
            if (
                t.market_id == market_id
                and t.size >= min_sz
                and t.timestamp >= cutoff
            )
        ]

    def get_all_recent(
        self,
        lookback_hours: float = 1.0,
        min_size: float | None = None,
    ) -> list[WhaleTrade]:
        """Retorna todos los whale trades en la ventana de tiempo."""
        min_sz = min_size or self._min_size
        cutoff = time.time() - lookback_hours * 3600
        return [
            t for t in self._load_trades()
            if t.size >= min_sz and t.timestamp >= cutoff
        ]

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _fetch_recent_trades(self) -> list[dict[str, Any]]:
        """Descarga trades recientes via API publica de Polymarket.

        Usa el endpoint REST del CLOB. En paper mode o si falla,
        retorna lista vacia.
        """
        try:
            import urllib.request
            import urllib.error

            # Pedir las ultimas 200 transacciones
            url = f"{_POLYMARKET_API}?limit=200"
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "NachoMarket/1.0 (bot@polymarket)"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            if isinstance(data, dict):
                return data.get("data", [])
            return data if isinstance(data, list) else []

        except Exception:
            logger.debug("WhaleTracker: no se pudo conectar a Polymarket API")
            return []

    def _parse_trade(self, raw: dict[str, Any]) -> WhaleTrade | None:
        """Parsea un trade raw de la API."""
        try:
            size = float(raw.get("size", 0) or raw.get("matchedAmount", 0) or 0)
            price = float(raw.get("price", 0) or 0)
            return WhaleTrade(
                trade_id=str(raw.get("id", raw.get("tradeId", ""))),
                market_id=str(raw.get("conditionId", raw.get("market", ""))),
                token_id=str(raw.get("assetId", raw.get("tokenId", ""))),
                side=str(raw.get("side", "BUY")).upper(),
                size=size * price,   # size en USDC = shares * price
                price=price,
                timestamp=float(raw.get("timestamp", time.time())),
                trader_address=str(raw.get("trader", raw.get("makerAddress", ""))),
                question=str(raw.get("question", ""))[:80],
            )
        except Exception:
            return None

    def _save_trade(self, trade: WhaleTrade) -> None:
        """Persiste un trade en el archivo JSONL."""
        try:
            with open(self._output_file, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "trade_id": trade.trade_id,
                    "market_id": trade.market_id,
                    "token_id": trade.token_id,
                    "side": trade.side,
                    "size": trade.size,
                    "price": trade.price,
                    "timestamp": trade.timestamp,
                    "trader": trade.trader_address,
                    "question": trade.question,
                }) + "\n")
        except Exception:
            logger.exception("Error guardando whale trade")

    def _load_trades(self) -> list[WhaleTrade]:
        """Carga trades del archivo JSONL."""
        if not self._output_file.exists():
            return []
        trades = []
        try:
            with open(self._output_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        trades.append(WhaleTrade(
                            trade_id=raw.get("trade_id", ""),
                            market_id=raw.get("market_id", ""),
                            token_id=raw.get("token_id", ""),
                            side=raw.get("side", ""),
                            size=float(raw.get("size", 0)),
                            price=float(raw.get("price", 0)),
                            timestamp=float(raw.get("timestamp", 0)),
                            trader_address=raw.get("trader", ""),
                            question=raw.get("question", ""),
                        ))
                    except Exception:
                        continue
        except OSError:
            pass
        return trades

    def _load_seen_ids(self) -> None:
        """Carga IDs de trades ya vistos para evitar duplicados."""
        for trade in self._load_trades():
            if trade.trade_id:
                self._seen_trade_ids.add(trade.trade_id)
