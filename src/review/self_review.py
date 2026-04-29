"""Self-review periodico con Claude Haiku (~$0.008/review).

Calcula metricas de las ultimas 8 horas leyendo data/trades.jsonl,
pide analisis a Claude Haiku en formato JSON, aplica ajustes seguros
a config/settings.yaml y notifica el resumen por Telegram.

Ejecutar manualmente:
    python src/review/self_review.py
"""

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anthropic
import yaml
from dotenv import load_dotenv

from src.utils.performance_metrics import compute_metrics_from_trades_file

# StageMachine eliminado en refactor v3

load_dotenv()
logger = logging.getLogger("nachomarket.review")

REVIEWS_DIR = Path("data/reviews")
TRADES_FILE = Path("data/trades.jsonl")
SETTINGS_FILE = Path("config/settings.yaml")

# Rangos seguros para ajustes automaticos de parametros de MM
_PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "spread_offset": (0.01, 0.05),
    "order_size": (10.0, 40.0),
    "refresh_seconds": (30.0, 120.0),
}

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# Precios de Haiku: $0.80/1M input, $4.00/1M output (en USD)
_COST_PER_INPUT_TOKEN = 0.80 / 1_000_000
_COST_PER_OUTPUT_TOKEN = 4.00 / 1_000_000


class SelfReviewer:
    """Meta-agente de analisis periodico con Claude Haiku.

    Se ejecuta cada 8 horas (configurado en main.py via schedule).
    Lee trades.jsonl, calcula metricas, consulta a Claude Haiku,
    aplica ajustes seguros a settings.yaml y notifica por Telegram.

    Args:
        model: Modelo de Claude a usar para el review.
        telegram_callback: Funcion sync para enviar mensajes por Telegram.
                           Si es None, se intenta importar send_alert de bot.py.
    """

    def __init__(
        self,
        model: str = _DEFAULT_MODEL,
        telegram_callback: Callable[[str], Any] | None = None,
        stage_machine: "StageMachine | None" = None,
        capital: float = 300.0,
    ) -> None:
        self._client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        self._model = model
        self._capital = capital
        self._telegram_callback = telegram_callback
        self._stage_machine = stage_machine  # Observer hook para Fase 2
        REVIEWS_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Punto de entrada principal
    # ------------------------------------------------------------------

    def run_review(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        """Ejecuta un ciclo completo de self-review.

        1. Carga trades de las ultimas 8 horas
        2. Calcula metricas de performance
        3. Consulta a Claude Haiku en formato JSON
        4. Aplica ajustes seguros a settings.yaml
        5. Guarda el review en data/reviews/
        6. Notifica el resumen por Telegram

        Returns:
            Dict con el review completo o {'status': 'no_trades'} si no hay datos.
        """
        trades = self._load_recent_trades(hours=8)
        if not trades:
            logger.info("No trades in the last 8 hours — skipping review")
            return {"status": "no_trades"}

        metrics = self._calculate_metrics(trades)
        # Enriquecer con Sharpe/Sortino/Calmar sobre los últimos 30 días
        quant_metrics = self._calculate_quant_metrics()
        logger.info(
            "Metrics: winrate=%.1f%% pnl=%.4f profit_factor=%.2f "
            "spread=%.4f trades=%d errors=%d",
            metrics["winrate"] * 100,
            metrics["total_pnl"],
            metrics["profit_factor"],
            metrics["avg_spread_captured"],
            metrics["trade_count"],
            metrics["error_trades"],
        )

        prompt = self._build_prompt(metrics, state, quant_metrics)

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text
            analysis = self._parse_analysis(raw)

            estimated_cost = (
                response.usage.input_tokens * _COST_PER_INPUT_TOKEN
                + response.usage.output_tokens * _COST_PER_OUTPUT_TOKEN
            )

            review: dict[str, Any] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "model": self._model,
                "window_hours": 8,
                "trade_count": len(trades),
                "metrics": metrics,
                "quant_metrics": quant_metrics,
                "analysis": analysis,
                "raw_response": raw,
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
                "estimated_cost_usd": round(estimated_cost, 6),
            }

            # Aplicar ajustes solo si Claude no recomienda pausar
            adjustments_applied: list[dict] = []
            if isinstance(analysis, dict):
                if analysis.get("should_pause"):
                    logger.warning(
                        "Claude recommends pausing — risk_level=%s",
                        analysis.get("risk_level", "?"),
                    )
                    review["pause_recommended"] = True
                else:
                    adjustments_applied = self._apply_adjustments(
                        analysis.get("adjustments", [])
                    )

            review["adjustments_applied"] = adjustments_applied
            self._save_review(review)
            self._notify_telegram(analysis, metrics, adjustments_applied, quant_metrics)

            # Observer: notificar a StageMachine el resultado de este review
            self._notify_stage_machine(analysis, metrics)

            logger.info(
                "Self-review completed: %d trades analyzed, cost≈$%.5f",
                len(trades),
                estimated_cost,
            )
            return review

        except Exception:
            logger.exception("Self-review failed")
            return {"status": "error"}

    # ------------------------------------------------------------------
    # Calculo de metricas
    # ------------------------------------------------------------------

    def _calculate_metrics(self, trades: list[dict]) -> dict[str, Any]:
        """Calcula las 7 metricas de performance a partir de trades.jsonl.

        Metodologia:
        - Las entradas en trades.jsonl son colocaciones de ordenes (no fills).
        - Para estimar PnL de MM: agrupar por mercado y calcular spread capturado
          entre el precio promedio de BUY y el precio promedio de SELL.
        - Winrate: mercados donde avg_sell > avg_buy (spread positivo capturado).
        - Profit factor: gross_profit / gross_loss calculados por mercado.
        - Rewards: suma del campo 'rewards' si existe en el trade.
        """
        if not trades:
            return self._empty_metrics()

        total = len(trades)
        error_trades = sum(1 for t in trades if t.get("status") == "error")

        # Agrupar por (market_id, strategy_name) para calcular spread capturado
        groups: dict[str, list[dict]] = {}
        for t in trades:
            key = t.get("market_id", "unknown")
            groups.setdefault(key, []).append(t)

        gross_profit = 0.0
        gross_loss = 0.0
        wins = 0
        losses = 0
        spreads_captured: list[float] = []
        total_fees = sum(t.get("fee_paid", 0.0) for t in trades)
        rewards_earned = sum(t.get("rewards", 0.0) for t in trades)
        capital_deployed = sum(t.get("size", 0.0) for t in trades if t.get("side") == "BUY")

        for market_id, market_trades in groups.items():
            buys = [t for t in market_trades if t.get("side") == "BUY"]
            sells = [t for t in market_trades if t.get("side") == "SELL"]

            if not buys or not sells:
                continue

            avg_buy = sum(t.get("price", 0.0) for t in buys) / len(buys)
            avg_sell = sum(t.get("price", 0.0) for t in sells) / len(sells)
            spread = avg_sell - avg_buy

            # Tamano efectivo del round-trip (limitado por el lado menor)
            buy_vol = sum(t.get("size", 0.0) for t in buys)
            sell_vol = sum(t.get("size", 0.0) for t in sells)
            matched_size = min(buy_vol, sell_vol)

            market_pnl = spread * matched_size

            if spread > 0:
                gross_profit += market_pnl
                spreads_captured.append(spread)
                wins += 1
            else:
                gross_loss += abs(market_pnl)
                losses += 1

        total_pnl = gross_profit - gross_loss - total_fees
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (
            float("inf") if gross_profit > 0 else 0.0
        )
        winrate = wins / (wins + losses) if (wins + losses) > 0 else 0.0
        avg_spread = sum(spreads_captured) / len(spreads_captured) if spreads_captured else 0.0

        # Inventory turnover: capital_deployed normalizado por capital total
        inventory_turnover = capital_deployed / self._capital if self._capital > 0 else 0.0

        max_dd = self._calculate_max_drawdown(trades)

        return {
            "trade_count": total,
            "win_count": wins,
            "loss_count": losses,
            "winrate": round(winrate, 4),
            "gross_profit": round(gross_profit, 4),
            "gross_loss": round(gross_loss, 4),
            "total_pnl": round(total_pnl, 4),
            "total_fees": round(total_fees, 4),
            "profit_factor": round(min(profit_factor, 999.0), 4),
            "max_drawdown": round(max_dd, 4),
            "avg_spread_captured": round(avg_spread, 4),
            "capital_deployed": round(capital_deployed, 2),
            "inventory_turnover": round(inventory_turnover, 4),
            "rewards_earned": round(rewards_earned, 4),
            "error_trades": error_trades,
            # Rewards farming metrics (PROMPT paso 7)
            "fill_rate": round(
                sum(1 for t in trades if t.get("status") in ("ORDER_STATUS_MATCHED", "matched", "paper"))
                / total if total > 0 else 0.0, 4
            ),
            "reward_yield_apy": round(
                (rewards_earned / self._capital * 365) if self._capital > 0 and rewards_earned > 0 else 0.0, 4
            ),
            "merge_count": sum(1 for t in trades if "merge" in str(t.get("metadata", "")).lower()),
        }

    def _calculate_max_drawdown(self, trades: list[dict]) -> float:
        """Calcula el max drawdown real a partir de la curva de equity.

        Agrupa trades por mercado, calcula PnL incremental cuando un mercado
        tiene tanto buys como sells, y trackea peak-to-trough.
        """
        if not trades:
            return 0.0

        sorted_trades = sorted(trades, key=lambda t: t.get("timestamp", ""))

        equity = 0.0
        peak = 0.0
        max_dd = 0.0

        # Track running buys/sells per market for PnL estimation
        market_buys: dict[str, list[float]] = {}
        market_sells: dict[str, list[float]] = {}
        market_buy_sizes: dict[str, list[float]] = {}
        market_sell_sizes: dict[str, list[float]] = {}

        for t in sorted_trades:
            mid = t.get("market_id", "unknown")
            side = t.get("side", "")
            price = t.get("price", 0.0)
            size = t.get("size", 0.0)

            # Acumular fees como costo directo
            equity -= t.get("fee_paid", 0.0)

            if side == "BUY":
                market_buys.setdefault(mid, []).append(price)
                market_buy_sizes.setdefault(mid, []).append(size)
            elif side == "SELL":
                market_sells.setdefault(mid, []).append(price)
                market_sell_sizes.setdefault(mid, []).append(size)
                # Calcular PnL incremental cuando tenemos buys previos
                if mid in market_buys and market_buys[mid]:
                    avg_buy = (
                        sum(p * s for p, s in zip(market_buys[mid], market_buy_sizes[mid]))
                        / sum(market_buy_sizes[mid])
                    )
                    spread_pnl = (price - avg_buy) * size
                    equity += spread_pnl

            peak = max(peak, equity)
            dd = peak - equity
            max_dd = max(max_dd, dd)

        return max_dd

    @staticmethod
    def _empty_metrics() -> dict[str, Any]:
        return {
            "trade_count": 0, "win_count": 0, "loss_count": 0,
            "winrate": 0.0, "gross_profit": 0.0, "gross_loss": 0.0,
            "total_pnl": 0.0, "total_fees": 0.0, "profit_factor": 0.0,
            "max_drawdown": 0.0, "avg_spread_captured": 0.0,
            "capital_deployed": 0.0, "inventory_turnover": 0.0,
            "rewards_earned": 0.0, "error_trades": 0,
            "fill_rate": 0.0, "reward_yield_apy": 0.0, "merge_count": 0,
        }

    # ------------------------------------------------------------------
    # Prompt y parsing
    # ------------------------------------------------------------------

    def _calculate_quant_metrics(self) -> dict[str, Any]:
        """Calcula Sharpe/Sortino/Calmar sobre los últimos 30 días."""
        try:
            result = compute_metrics_from_trades_file(
                str(TRADES_FILE),
                window_days=30,
                risk_free_rate=0.04,
            )
            return result
        except Exception:
            logger.debug("No se pudieron calcular quant metrics (sin datos suficientes)")
            return {
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "calmar_ratio": 0.0,
                "max_drawdown": 0.0,
                "trade_count_30d": 0,
            }

    def _build_prompt(
        self,
        metrics: dict[str, Any],
        state: dict[str, Any] | None,
        quant_metrics: dict[str, Any] | None = None,
    ) -> str:
        """Construye el prompt para Claude Haiku."""
        current_params = self._load_current_params()
        state_block = ("ESTADO DEL BOT:\n" + json.dumps(state, indent=2)) if state else ""
        quant_block = ""
        if quant_metrics:
            quant_block = (
                "MÉTRICAS CUANTITATIVAS (30 días):\n"
                + json.dumps(quant_metrics, indent=2)
            )

        return f"""Sos un analista de trading cuantitativo. Analizá estas métricas de las últimas 8 horas de un market making bot en Polymarket con ${self._capital:.0f} de capital.

MÉTRICAS 8H:
{json.dumps(metrics, indent=2)}

{quant_block}

PARÁMETROS ACTUALES:
{json.dumps(current_params, indent=2)}

{state_block}

Respondé en JSON con este formato exacto:
{{
  "summary": "resumen en 1-2 oraciones de la performance",
  "issues": ["problema detectado 1", "problema detectado 2"],
  "adjustments": [
    {{
      "param": "spread_offset",
      "current": 0.02,
      "suggested": 0.025,
      "reason": "explicacion breve"
    }}
  ],
  "should_pause": false,
  "risk_level": "LOW"
}}

Parámetros ajustables y sus rangos seguros:
- spread_offset: entre 0.01 y 0.05 (offset al mid para quotes de bid/ask)
- order_size: entre 10 y 40 (USDC por orden)
- refresh_seconds: entre 30 y 120 (frecuencia de refresh de órdenes)

risk_level debe ser: LOW | MEDIUM | HIGH | CRITICAL
Alerta CRITICAL si Sharpe < 0 por 14d o Calmar < 0.5.
Sugerí ajustes SOLO si hay evidencia clara en las métricas (winrate < 40%, profit_factor < 1.0, Sharpe < 0.5, error_rates altas, etc.).
Si el bot funciona bien, devolvé adjustments como lista vacía [].
Devolvé SOLO el JSON, sin texto adicional."""

    def _parse_analysis(self, raw: str) -> dict[str, Any] | str:
        """Extrae y parsea el JSON de la respuesta de Claude."""
        raw = raw.strip()
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(raw[start:end])
            except json.JSONDecodeError:
                pass
        logger.warning("Could not parse Claude response as JSON — storing raw string")
        return raw

    # ------------------------------------------------------------------
    # Ajustes de configuracion
    # ------------------------------------------------------------------

    def _load_current_params(self) -> dict[str, Any]:
        """Lee parametros actuales de market_maker desde settings.yaml."""
        if not SETTINGS_FILE.exists():
            return {}
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                settings = yaml.safe_load(f) or {}
            mm = settings.get("market_maker", {})
            return {
                "spread_offset": mm.get("spread_offset", 0.02),
                "order_size": mm.get("order_size", 20),
                "refresh_seconds": mm.get("refresh_seconds", 45),
            }
        except Exception:
            logger.exception("Could not read current params from settings.yaml")
            return {}

    def _apply_adjustments(self, adjustments: list[dict]) -> list[dict]:
        """Aplica ajustes dentro de los bounds seguros a settings.yaml.

        Solo modifica los parametros de market_maker dentro de _PARAM_BOUNDS.
        Si Claude sugiere un valor fuera del rango, se clampea sin rechazar el ajuste.

        Args:
            adjustments: Lista de {param, current, suggested, reason} de Claude.

        Returns:
            Lista de ajustes efectivamente aplicados con valores before/after.
        """
        if not adjustments or not SETTINGS_FILE.exists():
            return []

        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                settings = yaml.safe_load(f) or {}
        except Exception:
            logger.exception("Could not read settings.yaml for adjustment")
            return []

        applied: list[dict] = []
        mm = settings.setdefault("market_maker", {})

        for adj in adjustments:
            param = adj.get("param", "")
            suggested = adj.get("suggested")

            if param not in _PARAM_BOUNDS or suggested is None:
                logger.debug("Skipping unknown/invalid param: %s", param)
                continue

            lo, hi = _PARAM_BOUNDS[param]
            clamped = max(lo, min(hi, float(suggested)))

            if clamped != float(suggested):
                logger.warning(
                    "Clamped %s: suggested %.4f → applied %.4f (bounds [%.2f, %.2f])",
                    param, suggested, clamped, lo, hi,
                )

            before = mm.get(param)
            mm[param] = clamped

            applied.append({
                "param": param,
                "before": before,
                "after": clamped,
                "reason": adj.get("reason", ""),
            })
            logger.info("Adjusted %s: %s → %.4f | %s", param, before, clamped, adj.get("reason", ""))

        if applied:
            try:
                with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                    yaml.dump(settings, f, allow_unicode=True, default_flow_style=False)
                logger.info("settings.yaml updated with %d adjustments", len(applied))
            except Exception:
                logger.exception("Could not write adjusted settings.yaml")
                return []

        return applied

    # ------------------------------------------------------------------
    # Persistencia y notificaciones
    # ------------------------------------------------------------------

    def _notify_stage_machine(
        self,
        analysis: dict | str,
        metrics: dict[str, Any],
    ) -> None:
        """Observer notification — registra el review en la StageMachine por estrategia.

        Un review se considera positivo si:
        - should_pause=False y risk_level in (LOW, MEDIUM)
        - winrate >= 0.40 y profit_factor >= 1.0

        Se notifica a todas las estrategias conocidas en la StageMachine.
        """
        if self._stage_machine is None:
            return

        if not isinstance(analysis, dict):
            return

        should_pause = analysis.get("should_pause", False)
        risk_level = analysis.get("risk_level", "MEDIUM")
        winrate = metrics.get("winrate", 0.0)
        profit_factor = metrics.get("profit_factor", 0.0)

        passed = (
            not should_pause
            and risk_level in ("LOW", "MEDIUM")
            and winrate >= 0.40
            and profit_factor >= 1.0
        )

        stages_info = self._stage_machine.get_all_stages()
        for strategy_name in stages_info:
            promoted = self._stage_machine.record_review(strategy_name, passed)
            if promoted:
                logger.info(
                    "Stage machine auto-promoted '%s' tras review (passed=%s)",
                    strategy_name, passed,
                )

    def _load_recent_trades(self, hours: int = 8) -> list[dict]:
        """Carga trades de las ultimas N horas desde trades.jsonl."""
        if not TRADES_FILE.exists():
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        trades: list[dict] = []

        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    trade = json.loads(line)
                    ts_str = trade.get("timestamp", "")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts >= cutoff:
                            trades.append(trade)
                    else:
                        # Sin timestamp: incluir de todas formas
                        trades.append(trade)
                except (json.JSONDecodeError, ValueError):
                    continue

        return trades

    def _save_review(self, review: dict[str, Any]) -> None:
        """Guarda el review en data/reviews/<timestamp>.json."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        review_file = REVIEWS_DIR / f"review_{ts}.json"
        review_file.write_text(
            json.dumps(review, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Review saved: %s", review_file)

    def _notify_telegram(
        self,
        analysis: dict | str,
        metrics: dict[str, Any],
        adjustments: list[dict],
        quant_metrics: dict[str, Any] | None = None,
    ) -> None:
        """Envia resumen del review por Telegram (3 lineas + ajustes si aplica)."""
        # Resolver callback: primero el inyectado, luego el modulo bot.py
        callback = self._telegram_callback
        if callback is None:
            try:
                from src.telegram.bot import send_alert  # noqa: PLC0415
                callback = send_alert
            except ImportError:
                return

        if not isinstance(analysis, dict):
            summary = "Review completado (respuesta no estructurada)"
            risk = "?"
            should_pause = False
            issues: list[str] = []
        else:
            summary = analysis.get("summary", "Sin resumen")
            risk = analysis.get("risk_level", "?")
            should_pause = analysis.get("should_pause", False)
            issues = analysis.get("issues", [])

        risk_icon = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴", "CRITICAL": "🚨"}.get(risk, "⚪")

        lines = [
            f"*Self-Review 8h completado* {risk_icon} `{risk}`",
            (
                f"PnL: `${metrics['total_pnl']:.4f}` | "
                f"Winrate: `{metrics['winrate']:.1%}` | "
                f"Trades: `{metrics['trade_count']}`"
            ),
            (
                f"Spread: `{metrics['avg_spread_captured']:.4f}` | "
                f"Rewards: `${metrics['rewards_earned']:.4f}` | "
                f"Errores: `{metrics['error_trades']}`"
            ),
            (
                f"🎯 Fill rate: `{metrics.get('fill_rate', 0):.1%}` | "
                f"Reward yield: `{metrics.get('reward_yield_apy', 0):.1%}` APY | "
                f"Merges: `{metrics.get('merge_count', 0)}`"
            ),
            f"_{summary}_",
        ]

        # Añadir métricas cuantitativas (Sharpe / Sortino / Calmar)
        if quant_metrics and quant_metrics.get("trade_count_30d", 0) > 0:
            sharpe = quant_metrics.get("sharpe_ratio", 0.0)
            sortino = quant_metrics.get("sortino_ratio", 0.0)
            calmar = quant_metrics.get("calmar_ratio", 0.0)
            sharpe_icon = "🟢" if sharpe > 1.0 else ("🟡" if sharpe > 0 else "🔴")
            lines.append(
                f"📊 30d Sharpe: {sharpe_icon}`{sharpe:.2f}` | "
                f"Sortino: `{sortino:.2f}` | "
                f"Calmar: `{calmar:.2f}`"
            )

        if issues:
            lines.append("*Problemas:* " + " · ".join(f"`{i}`" for i in issues[:3]))

        if adjustments:
            adj_parts = [
                f"{a['param']}: `{a['before']}` → `{a['after']}`"
                for a in adjustments
            ]
            lines.append("*Ajustes aplicados:* " + " | ".join(adj_parts))

        if should_pause:
            lines.append("⚠️ *Claude recomienda PAUSAR el bot*")

        try:
            callback("\n".join(lines))
        except Exception:
            logger.exception("Could not send Telegram review notification")


if __name__ == "__main__":
    from src.utils.logger import setup_logger

    setup_logger("nachomarket")
    reviewer = SelfReviewer()
    result = reviewer.run_review()
    print(json.dumps(result, indent=2, ensure_ascii=False))
