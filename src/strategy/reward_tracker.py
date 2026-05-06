"""RewardTracker v2 — mide centavos/minuto REALES farmeados por mercado.

Usa GET /rewards/user/markets de Polymarket (datos server-side, mismos que
la columna 'Ingresos' de la web). Sin estimaciones: earnings reales por mercado.
"""

import json
import logging
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger("nachomarket.reward_tracker")

SAMPLE_INTERVAL_SEC = 60
WINDOW_SEC = 300
EMA_ALPHA = 0.6
PERSIST_INTERVAL_SEC = 300
PERSIST_PATH = "data/reward_tracker.json"


class _Sample(NamedTuple):
    ts: float
    earnings: float       # USDC reales acumulados del dia


class RewardTracker:
    """Thread daemon que consulta earnings reales cada 60s."""

    def __init__(
        self,
        client: Any,
        sample_interval_sec: float = SAMPLE_INTERVAL_SEC,
        window_sec: float = WINDOW_SEC,
        persist_path: str = PERSIST_PATH,
    ) -> None:
        self._client = client
        self._sample_interval = sample_interval_sec
        self._window_sec = window_sec
        self._persist_path = Path(persist_path)
        self._lock = threading.Lock()
        self._buffers: dict[str, deque[_Sample]] = {}
        self._ema: dict[str, float] = {}
        self._ema_share: dict[str, float] = {}
        self._ema_n: dict[str, int] = {}
        self._share_pct: dict[str, float] = {}
        self._daily_rates: dict[str, float] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._load_state()

    # --- Control ---

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("RewardTracker v2 iniciado (interval=%ds window=%ds)", self._sample_interval, self._window_sec)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._save_state()

    # --- Loop ---

    def _loop(self) -> None:
        last_persist = time.time()
        while not self._stop.is_set():
            self._sample()
            if time.time() - last_persist > PERSIST_INTERVAL_SEC:
                self._save_state()
                last_persist = time.time()
            self._stop.wait(self._sample_interval)

    def _sample(self) -> None:
        """Consulta earnings reales por mercado via /rewards/user/markets."""
        if self._client.paper_mode:
            return
        try:
            markets = self._client.get_user_earnings_markets()
        except Exception as e:
            logger.warning("RT sample error: %s", e)
            return

        if not markets:
            logger.debug("RT sample: 0 markets returned")
            return

        now = time.time()
        tracked = 0
        total_earnings = 0.0
        for m in markets:
            cid = m.get("condition_id", "")
            if not cid:
                continue

            # Parse earnings (puede venir como string JSON)
            earnings_raw = m.get("earnings", [])
            if isinstance(earnings_raw, str):
                try:
                    earnings_raw = json.loads(earnings_raw)
                except json.JSONDecodeError:
                    continue
            earnings = 0.0
            if isinstance(earnings_raw, list):
                for e in earnings_raw:
                    if isinstance(e, dict):
                        earnings += float(e.get("earnings", 0))

            # Parse rewards_config para daily_rate
            cfg_raw = m.get("rewards_config", [])
            if isinstance(cfg_raw, str):
                try:
                    cfg_raw = json.loads(cfg_raw)
                except json.JSONDecodeError:
                    cfg_raw = []
            daily_rate = 0.0
            if isinstance(cfg_raw, list):
                for r in cfg_raw:
                    if isinstance(r, dict):
                        daily_rate += float(r.get("rate_per_day", 0))

            pct_raw = m.get("earning_percentage", 0)
            try:
                share_pct = float(pct_raw)
            except (ValueError, TypeError):
                share_pct = 0.0

            sample = _Sample(ts=now, earnings=earnings)

            with self._lock:
                if cid not in self._buffers:
                    self._buffers[cid] = deque(maxlen=100)
                buf = self._buffers[cid]
                buf.append(sample)
                self._share_pct[cid] = share_pct
                self._daily_rates[cid] = daily_rate
                tracked += 1

        # f2. Complementar con datos globales de rewards para mercados no activos aun
        # Esto permite que la estrategia RF vea el 'rate' de mercados nuevos para rotar
        try:
            global_rewards = self._client.get_rewards()
            if global_rewards:
                with self._lock:
                    for cid, rinfo in global_rewards.items():
                        if cid not in self._daily_rates or self._daily_rates[cid] <= 0:
                            self._daily_rates[cid] = float(rinfo.get("rewards_daily_rate", 0))
        except Exception as e:
            logger.debug("RT global rewards fetch error: %s", e)

        if tracked:
            with self._lock:
                top = self.best_cents_per_min()
            logger.info("RT sample: %d mercados tracked | top: %.3f cent/min", tracked, top)

    # --- Queries ---

    def cents_per_min(self, condition_id: str) -> float | None:
        """Centavos/minuto REALES basados en earnings del servidor."""
        buf = self._buffers.get(condition_id)
        if not buf or len(buf) < 2:
            return None

        buf_list = list(buf)
        now = time.time()
        recent = [s for s in buf_list if now - s.ts <= self._window_sec]
        if len(recent) < 2:
            return None

        first, last = recent[0], recent[-1]
        elapsed_min = (last.ts - first.ts) / 60.0
        if elapsed_min <= 0:
            return None

        earned_cents = (last.earnings - first.earnings) * 100.0
        if earned_cents < 0:
            earned_cents = 0

        cpm = earned_cents / elapsed_min
        prev = self._ema.get(condition_id, cpm)
        n = self._ema_n.get(condition_id, 0) + 1
        smoothed = EMA_ALPHA * cpm + (1 - EMA_ALPHA) * prev if n > 1 else cpm
        self._ema[condition_id] = smoothed
        self._ema_n[condition_id] = n
        return smoothed

    def best_cents_per_min(self) -> float:
        """Mayor c/min entre todos los mercados trackeados."""
        best = 0.0
        ema = self._ema
        for cid in self._buffers:
            cpm = ema.get(cid, 0)
            if cpm > best:
                best = cpm
        return best

    def realized_cents_since(self, condition_id: str, since_ts: float) -> float | None:
        """Centavos REALES acumulados desde since_ts."""
        buf = list(self._buffers.get(condition_id, deque()))
        relevant = [s for s in buf if s.ts >= since_ts]
        if len(relevant) < 2:
            return None
        return (relevant[-1].earnings - relevant[0].earnings) * 100.0

    def last_share_pct(self, condition_id: str) -> float | None:
        return self._share_pct.get(condition_id)

    def last_daily_rate(self, condition_id: str) -> float | None:
        return self._daily_rates.get(condition_id)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Snapshot sin lock (dict reads son seguros si writes son infrecuentes)."""
        result = {}
        buffers = self._buffers  # referencia local
        ema = self._ema
        share = self._share_pct
        rates = self._daily_rates
        for cid in buffers:
            buf = buffers[cid]
            result[cid] = {
                "cents_per_min": ema.get(cid, 0),
                "last_share_pct": share.get(cid, 0),
                "last_daily_rate": rates.get(cid, 0),
                "sample_count": len(buf),
            }
        return result

    def get_share_pct_map(self) -> dict[str, float]:
        return dict(self._share_pct)

    def get_daily_rate_map(self) -> dict[str, float]:
        return dict(self._daily_rates)

    # --- Persistence ---

    def _save_state(self) -> None:
        with self._lock:
            data = {"ema": self._ema, "share_pct": self._share_pct, "daily_rates": self._daily_rates}
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist_path.write_text(json.dumps(data), encoding="utf-8")
        except Exception:
            pass

    def _load_state(self) -> None:
        if not self._persist_path.exists():
            return
        try:
            data = json.loads(self._persist_path.read_text(encoding="utf-8"))
            with self._lock:
                self._ema = data.get("ema", {})
                self._share_pct = data.get("share_pct", {})
                self._daily_rates = data.get("daily_rates", {})
        except Exception:
            pass
