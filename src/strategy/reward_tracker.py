"""RewardTracker — mide ¢/min realmente farmeados por mercado.

Muestrea client.get_reward_percentages() cada 60s y calcula el delta de
share acumulado × daily_rate para obtener una tasa observada de ¢/min.

Sin este módulo el bot solo sabe si una orden está "scoring" (bool). Con él
puede cancelar mercados que no generan suficiente retorno y concentrar capital
en los que sí rinden.

Mecanismo:
  - get_reward_percentages() → % share acumulado del día por mercado (0-100)
  - delta_pct × daily_rate = USD farmeados en el intervalo
  - EMA(α=0.4) sobre los pares de delta para suavizar lag del endpoint (~1-2min)
  - Retorna None si no hay suficiente historia (< 2 muestras separadas ≥ 30s)
"""

import json
import logging
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger("nachomarket.reward_tracker")

SAMPLE_INTERVAL_SEC = 60
WINDOW_SEC = 300        # ventana de 5 min para capturar tendencia reciente
EMA_ALPHA = 0.6
STALE_FACTOR = 2.5      # si buf[-1].ts > STALE_FACTOR * interval → dato muerto
PERSIST_INTERVAL_SEC = 300
PERSIST_PATH = "data/reward_tracker.json"


class _Sample(NamedTuple):
    ts: float
    share_pct: float
    daily_rate: float


class RewardTracker:
    """Thread daemon que muestrea reward percentages y calcula ¢/min por mercado."""

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
        self._buffers: dict[str, deque[_Sample]] = defaultdict(lambda: deque(maxlen=16))
        self._ema: dict[str, float] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._last_persist = time.monotonic()
        self._daily_rates: dict[str, float] = {}

    def start(self) -> None:
        thread = threading.Thread(target=self._run, daemon=True, name="reward-tracker")
        thread.start()
        logger.info(
            "RewardTracker iniciado (interval=%ds window=%ds)",
            self._sample_interval, self._window_sec,
        )

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._sample()
            except Exception:
                logger.debug("RewardTracker: error en sample", exc_info=True)
            self._stop_event.wait(self._sample_interval)

    def _sample(self) -> None:
        percentages = self._client.get_reward_percentages()
        if not percentages:
            return

        # Actualizar daily_rates desde get_rewards (best-effort)
        try:
            rewards_map = self._client.get_rewards()
            with self._lock:
                for cid, r in rewards_map.items():
                    self._daily_rates[cid] = float(r.get("rewards_daily_rate", 0.0))
        except Exception:
            pass

        now = time.time()
        with self._lock:
            for cid, share_pct in percentages.items():
                daily_rate = self._daily_rates.get(cid, 0.0)
                self._buffers[cid].append(_Sample(ts=now, share_pct=float(share_pct), daily_rate=daily_rate))
                self._update_ema(cid)

        if time.monotonic() - self._last_persist >= PERSIST_INTERVAL_SEC:
            self._persist()
            self._last_persist = time.monotonic()

        # Log conciso cada sample
        active = {cid: self._ema[cid] for cid in self._ema if self._ema[cid] is not None}
        if active:
            top = sorted(active.items(), key=lambda x: x[1], reverse=True)[:3]
            logger.info(
                "RT sample: %d mercados tracked | top: %s",
                len(active),
                " ".join(f"{cid[:8]}={r:.3f}¢/m" for cid, r in top),
            )

    def _update_ema(self, cid: str) -> None:
        buf = list(self._buffers[cid])
        if len(buf) < 2:
            return

        # Usar time.time() como referencia — buf[-1].ts puede ser viejo si el
        # mercado acaba de volver a aparecer en la API tras una ausencia.
        now = time.time()
        rates: list[float] = []
        for i in range(1, len(buf)):
            prev, cur = buf[i - 1], buf[i]
            if (now - cur.ts) > self._window_sec + self._sample_interval:
                continue
            delta_pct = cur.share_pct - prev.share_pct
            if delta_pct < 0:
                # Reset diario UTC: contar como 0-rate (no ignorar) para que
                # el EMA decaiga en lugar de quedarse congelado.
                rates.append(0.0)
                continue
            delta_min = max((cur.ts - prev.ts) / 60.0, 0.1)
            delta_usd = (delta_pct / 100.0) * prev.daily_rate
            rates.append((delta_usd * 100.0) / delta_min)

        if not rates:
            return

        current = self._ema.get(cid)
        for rate in rates:
            current = rate if current is None else EMA_ALPHA * rate + (1 - EMA_ALPHA) * current
        self._ema[cid] = current

    def cents_per_min(self, condition_id: str) -> float | None:
        """¢/min observado para el mercado.

        Returns:
            None   — sin historia suficiente (mercado nuevo → exploring)
            0.0    — dato stale o mercado que dejó de aparecer en la API
            float  — EMA reciente
        """
        with self._lock:
            buf = list(self._buffers.get(condition_id, deque()))

        if len(buf) < 2:
            return None

        # Staleness guard: si el tracker no ha visto este mercado recientemente
        # (desapareció de get_reward_percentages), su EMA es historia muerta.
        # Retornamos 0.0 — diferente de None para que no vaya al bucket exploring.
        if time.time() - buf[-1].ts > STALE_FACTOR * self._sample_interval:
            return 0.0

        now = time.time()
        valid = [s for s in buf if now - s.ts <= self._window_sec + self._sample_interval]
        if len(valid) < 2:
            return None

        if (valid[-1].ts - valid[0].ts) < 30:
            return None

        with self._lock:
            return self._ema.get(condition_id)

    def best_cents_per_min(self) -> float:
        """Retorna el mayor ¢/min observado entre todos los mercados tracked."""
        with self._lock:
            rates = [v for v in self._ema.values() if v is not None]
        return max(rates) if rates else 0.0

    def last_share_pct(self, condition_id: str) -> float | None:
        with self._lock:
            buf = self._buffers.get(condition_id)
            if buf:
                return buf[-1].share_pct
        return None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                cid: {
                    "cents_per_min": self._ema.get(cid),
                    "last_share_pct": buf[-1].share_pct if buf else None,
                    "last_daily_rate": buf[-1].daily_rate if buf else None,
                    "sample_count": len(buf),
                }
                for cid, buf in self._buffers.items()
            }

    def _persist(self) -> None:
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist_path.write_text(
                json.dumps({"timestamp": time.time(), "markets": self.snapshot()}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass
