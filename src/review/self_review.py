import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("nachomarket.review")

REVIEWS_DIR = Path("data/reviews")
TRADES_FILE = Path("data/trades.jsonl")


class SelfReviewer:
    """Analisis periodico del bot con Claude Haiku (~$0.01/review)."""

    def __init__(self, model: str = "claude-haiku-4-5-20251001") -> None:
        self._client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        self._model = model
        REVIEWS_DIR.mkdir(parents=True, exist_ok=True)

    def run_review(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        """Ejecuta un self-review analizando trades recientes."""
        trades = self._load_recent_trades(limit=50)
        if not trades:
            logger.info("No trades to review")
            return {"status": "no_trades"}

        prompt = self._build_prompt(trades, state)

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            analysis = response.content[0].text

            review = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "model": self._model,
                "trades_analyzed": len(trades),
                "analysis": analysis,
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }

            self._save_review(review)
            logger.info(f"Self-review completed: {len(trades)} trades analyzed")
            return review

        except Exception:
            logger.exception("Self-review failed")
            return {"status": "error"}

    def _build_prompt(self, trades: list[dict], state: dict[str, Any] | None) -> str:
        """Construye el prompt para Claude Haiku."""
        trades_summary = json.dumps(trades[-20:], indent=2)  # Ultimos 20 trades

        prompt = f"""Eres el sistema de self-review de NachoMarket, un bot de trading en Polymarket.
Capital: $400 USDC. Regla: nunca arriesgar >5% en un solo mercado.

Analiza estos trades recientes y responde en espanol:

TRADES:
{trades_summary}

{"ESTADO ACTUAL: " + json.dumps(state, indent=2) if state else ""}

Responde con:
1. PnL estimado y win rate
2. Patrones problematicos detectados
3. Recomendaciones concretas (max 3)
4. Risk score (1-10, donde 10 = peligroso)
5. Accion sugerida: CONTINUAR / AJUSTAR / PAUSAR

Se breve y directo."""

        return prompt

    def _load_recent_trades(self, limit: int = 50) -> list[dict]:
        """Carga los trades mas recientes."""
        if not TRADES_FILE.exists():
            return []

        trades = []
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        trades.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        return trades[-limit:]

    def _save_review(self, review: dict[str, Any]) -> None:
        """Guarda el review en un archivo JSON."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        review_file = REVIEWS_DIR / f"review_{timestamp}.json"
        review_file.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"Review saved to {review_file}")


if __name__ == "__main__":
    from src.utils.logger import setup_logger
    setup_logger("nachomarket")
    reviewer = SelfReviewer()
    result = reviewer.run_review()
    print(json.dumps(result, indent=2, ensure_ascii=False))
