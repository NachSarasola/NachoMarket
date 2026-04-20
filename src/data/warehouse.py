"""Data Warehouse usando DuckDB (TODO 6.1).

Base de datos OLAP local para queries rapidas sobre trades, signals y snapshots.
Reemplaza los JSONL files para analytics.

Tablas:
    trades          — Todos los trades ejecutados
    signals         — Todas las senales generadas
    market_snapshots — Snapshots de orderbook cada 1min
    whale_trades    — Trades grandes de ballenas (de polyscan.py)
    economic_events — Eventos del calendario FRED

Vistas:
    v_daily_pnl     — PnL por dia
    v_strategy_attribution — PnL por estrategia x categoria x regimen
    v_market_roi    — ROI por mercado
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.warehouse")

_DB_PATH = "data/warehouse.duckdb"
_TRADES_FILE = Path("data/trades.jsonl")
_WHALE_TRADES_FILE = Path("data/whale_trades.jsonl")


class DataWarehouse:
    """Data warehouse OLAP usando DuckDB para analytics del bot.

    Uso:
        wh = DataWarehouse()
        wh.initialize()  # Crear tablas y vistas
        wh.ingest_trades()  # Importar trades.jsonl
        df = wh.query("SELECT * FROM v_daily_pnl ORDER BY date DESC LIMIT 7")
    """

    def __init__(self, db_path: str = _DB_PATH) -> None:
        self._db_path = db_path
        self._conn = None

    def __enter__(self) -> "DataWarehouse":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Conexion y setup
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Conecta a la base de datos DuckDB. Crea el archivo si no existe."""
        try:
            import duckdb
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(self._db_path)
            logger.info("DataWarehouse conectado: %s", self._db_path)
            return True
        except ImportError:
            logger.warning(
                "duckdb no instalado. Instalar con: pip install duckdb. "
                "DataWarehouse deshabilitado."
            )
            return False
        except Exception:
            logger.exception("Error conectando a DataWarehouse")
            return False

    def close(self) -> None:
        """Cierra la conexion."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def initialize(self) -> bool:
        """Crea todas las tablas y vistas si no existen."""
        if not self._conn:
            return False
        try:
            self._create_tables()
            self._create_views()
            logger.info("DataWarehouse inicializado")
            return True
        except Exception:
            logger.exception("Error inicializando DataWarehouse")
            return False

    # ------------------------------------------------------------------
    # Ingesta
    # ------------------------------------------------------------------

    def ingest_trades(self, trades_file: str = str(_TRADES_FILE)) -> int:
        """Importa trades desde trades.jsonl a la tabla trades.

        Returns:
            Numero de trades importados.
        """
        if not self._conn:
            return 0

        path = Path(trades_file)
        if not path.exists():
            return 0

        count = 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        trade = json.loads(line)
                        self._insert_trade(trade)
                        count += 1
                    except Exception:
                        continue
            logger.info("Ingesta completada: %d trades", count)
        except Exception:
            logger.exception("Error en ingest_trades")

        return count

    def insert_trade(self, trade: dict[str, Any]) -> None:
        """Inserta un trade individual."""
        if self._conn:
            self._insert_trade(trade)

    def insert_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Inserta un snapshot de orderbook."""
        if not self._conn:
            return
        try:
            self._conn.execute("""
                INSERT OR IGNORE INTO market_snapshots
                (token_id, timestamp, mid_price, best_bid, best_ask, spread, depth)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                snapshot.get("token_id", ""),
                snapshot.get("timestamp", datetime.now(timezone.utc).isoformat()),
                snapshot.get("mid_price", 0.0),
                snapshot.get("best_bid", 0.0),
                snapshot.get("best_ask", 0.0),
                snapshot.get("spread", 0.0),
                snapshot.get("depth", 0.0),
            ])
        except Exception:
            logger.debug("Error insertando snapshot: %s", snapshot.get("token_id", "?")[:8])

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def query(self, sql: str, params: list | None = None) -> list[dict[str, Any]]:
        """Ejecuta una query SQL y retorna lista de dicts.

        Returns:
            Lista de filas como dicts. Vacia si hay error o no hay datos.
        """
        if not self._conn:
            return []
        try:
            if params:
                result = self._conn.execute(sql, params)
            else:
                result = self._conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception:
            logger.exception("Error ejecutando query")
            return []

    def daily_pnl(self, days: int = 30) -> list[dict[str, Any]]:
        """PnL diario de los ultimos N dias."""
        return self.query(f"""
            SELECT * FROM v_daily_pnl
            WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY date DESC
        """)

    def strategy_attribution(self) -> list[dict[str, Any]]:
        """Attribution de PnL por estrategia x categoria."""
        return self.query("SELECT * FROM v_strategy_attribution ORDER BY total_pnl DESC")

    def market_roi(self, top_n: int = 10) -> list[dict[str, Any]]:
        """Top N mercados por ROI."""
        return self.query(f"SELECT * FROM v_market_roi ORDER BY roi DESC LIMIT {top_n}")

    # ------------------------------------------------------------------
    # Internos — DDL
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        """Crea las tablas del warehouse."""
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS trades (
                trade_id        VARCHAR PRIMARY KEY,
                timestamp       TIMESTAMP,
                strategy_name   VARCHAR,
                market_id       VARCHAR,
                token_id        VARCHAR,
                side            VARCHAR,
                price           DOUBLE,
                size            DOUBLE,
                pnl             DOUBLE DEFAULT 0.0,
                fee_paid        DOUBLE DEFAULT 0.0,
                rewards         DOUBLE DEFAULT 0.0,
                status          VARCHAR,
                order_id        VARCHAR,
                market_category VARCHAR DEFAULT 'unknown',
                regime_detected VARCHAR DEFAULT 'UNKNOWN',
                entry_edge_bps  DOUBLE DEFAULT 0.0,
                exit_reason     VARCHAR DEFAULT ''
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS signals (
                signal_id       VARCHAR PRIMARY KEY,
                timestamp       TIMESTAMP,
                strategy_name   VARCHAR,
                market_id       VARCHAR,
                token_id        VARCHAR,
                side            VARCHAR,
                price           DOUBLE,
                size            DOUBLE,
                reason          VARCHAR,
                executed        BOOLEAN DEFAULT FALSE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                token_id    VARCHAR,
                timestamp   TIMESTAMP,
                mid_price   DOUBLE,
                best_bid    DOUBLE,
                best_ask    DOUBLE,
                spread      DOUBLE,
                depth       DOUBLE,
                PRIMARY KEY (token_id, timestamp)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS whale_trades (
                trade_id    VARCHAR PRIMARY KEY,
                market_id   VARCHAR,
                token_id    VARCHAR,
                side        VARCHAR,
                size        DOUBLE,
                price       DOUBLE,
                timestamp   TIMESTAMP,
                trader      VARCHAR,
                question    VARCHAR
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS economic_events (
                event_id        VARCHAR PRIMARY KEY,
                name            VARCHAR,
                description     VARCHAR,
                category        VARCHAR,
                market_impact   VARCHAR,
                release_date    TIMESTAMP,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
        ]
        for ddl in ddl_statements:
            self._conn.execute(ddl)

    def _create_views(self) -> None:
        """Crea las vistas de analytics."""
        views = [
            """
            CREATE OR REPLACE VIEW v_daily_pnl AS
            SELECT
                CAST(timestamp AS DATE) AS date,
                SUM(pnl)                AS total_pnl,
                COUNT(*)                AS trade_count,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS winning_trades,
                SUM(fee_paid)           AS total_fees,
                SUM(rewards)            AS total_rewards
            FROM trades
            GROUP BY CAST(timestamp AS DATE)
            ORDER BY date DESC
            """,
            """
            CREATE OR REPLACE VIEW v_strategy_attribution AS
            SELECT
                strategy_name,
                market_category,
                regime_detected,
                SUM(pnl)        AS total_pnl,
                COUNT(*)        AS trade_count,
                AVG(pnl)        AS avg_pnl,
                SUM(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*) AS win_rate
            FROM trades
            GROUP BY strategy_name, market_category, regime_detected
            ORDER BY total_pnl DESC
            """,
            """
            CREATE OR REPLACE VIEW v_market_roi AS
            SELECT
                market_id,
                SUM(pnl)            AS total_pnl,
                SUM(size)           AS total_deployed,
                COUNT(*)            AS trade_count,
                CASE
                    WHEN SUM(size) > 0 THEN SUM(pnl) / SUM(size)
                    ELSE 0.0
                END                 AS roi
            FROM trades
            GROUP BY market_id
            ORDER BY roi DESC
            """,
        ]
        for view_sql in views:
            self._conn.execute(view_sql)

    def _insert_trade(self, trade: dict[str, Any]) -> None:
        """Inserta o ignora un trade en la tabla trades."""
        trade_id = trade.get("order_id") or trade.get("trade_id") or ""
        if not trade_id:
            import hashlib
            import json as _json
            trade_id = hashlib.md5(
                _json.dumps(trade, sort_keys=True).encode()
            ).hexdigest()[:16]

        ts = trade.get("timestamp", datetime.now(timezone.utc).isoformat())

        try:
            self._conn.execute("""
                INSERT OR IGNORE INTO trades
                (trade_id, timestamp, strategy_name, market_id, token_id,
                 side, price, size, pnl, fee_paid, rewards, status, order_id,
                 market_category, regime_detected, entry_edge_bps, exit_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                trade_id,
                ts,
                trade.get("strategy_name", "unknown"),
                trade.get("market_id", ""),
                trade.get("token_id", ""),
                trade.get("side", ""),
                float(trade.get("price", 0) or 0),
                float(trade.get("size", 0) or 0),
                float(trade.get("pnl", 0) or 0),
                float(trade.get("fee_paid", 0) or 0),
                float(trade.get("rewards", 0) or 0),
                trade.get("status", "unknown"),
                trade.get("order_id", ""),
                trade.get("market_category", "unknown"),
                trade.get("regime_detected", "UNKNOWN"),
                float(trade.get("entry_edge_bps", 0) or 0),
                trade.get("exit_reason", ""),
            ])
        except Exception:
            logger.debug("Error insertando trade %s", trade_id[:8])
