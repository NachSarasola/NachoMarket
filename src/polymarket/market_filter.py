"""
Filtrado de mercados: banlist, deduplicacion, y filtro de riesgo por noticias.

Tres capas de filtrado que se aplican ANTES del scoring:
1. Banlist: condition_ids y regex patterns en el titulo
2. Deduplicacion: mercados con preguntas similares → queda solo el mejor
3. News-risk: mercados nuevos con altos rewards o cercanos a resolucion
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.market_filter")


class MarketFilter:
    """Filtra mercados no aptos antes de scoring y seleccion."""

    _BLOCKS_FILE = Path("data/market_blocks.json")

    def __init__(self, config: dict[str, Any]) -> None:
        banned = config.get("banned_markets", {})
        self._banned_ids: set[str] = set(banned.get("condition_ids", []))
        self._banned_patterns: list[re.Pattern] = [
            re.compile(p, re.IGNORECASE) for p in banned.get("question_patterns", [])
        ]

        filters = config.get("filters", {})
        self._min_market_age_hours = filters.get("min_market_age_hours", 48)
        self._dedup_similarity_threshold = 0.7

        # Temporal blocks: {condition_id: expiry_timestamp} — persisten entre reinicios
        self._temporal_blocks: dict[str, float] = self._load_blocks()

    # ------------------------------------------------------------------
    # 1. Banlist
    # ------------------------------------------------------------------

    def is_banned(self, market: dict[str, Any]) -> bool:
        """Verifica si un mercado esta en la banlist."""
        cid = market.get("condition_id", "")
        if cid in self._banned_ids:
            return True

        # Check temporal blocks (formato nuevo: (expiry, count) o antiguo: float)
        if cid in self._temporal_blocks:
            entry = self._temporal_blocks[cid]
            if isinstance(entry, (int, float)):
                # Formato antiguo
                if time.time() < entry:
                    return True
                del self._temporal_blocks[cid]
            elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                # Formato nuevo: (expiry, count)
                exp, _ = entry
                if time.time() < exp:
                    return True
                del self._temporal_blocks[cid]

        question = market.get("question", "")
        for pattern in self._banned_patterns:
            if pattern.search(question):
                return True

        return False

    def remove_banned(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filtra mercados que estan en la banlist."""
        result = [m for m in markets if not self.is_banned(m)]
        removed = len(markets) - len(result)
        if removed:
            logger.info(f"MarketFilter: {removed} mercados removidos por banlist")
        return result

    def block_market_until(self, condition_id: str, hours: float) -> None:
        """Bloquea temporalmente un mercado por N horas (persiste a disco).

        Incremental x2: si el mercado ya estaba bloqueado, duplica el tiempo.
        Mínimo 1h. Resetea contador si pasaron >30min desde el último block.
        """
        now = time.time()
        min_hours = max(1.0, hours)  # Mínimo 1h (req. usuario)

        if condition_id in self._temporal_blocks:
            entry = self._temporal_blocks[condition_id]
            # Migrar formato antiguo (float) a nuevo (tuple)
            if isinstance(entry, (int, float)):
                old_expiry = entry
                block_count = 1
            else:
                old_expiry, block_count = entry

            # Si el bloqueo anterior ya expiró hace >30min, resetea contador
            if now - old_expiry > 1800:  # 30 min
                block_count = 1
            else:
                block_count += 1

            new_hours = min_hours * (2 ** (block_count - 1))
            self._temporal_blocks[condition_id] = (now + new_hours * 3600, block_count)
            logger.info(
                f"Mercado {condition_id[:8]}... re-bloqueado: {new_hours:.0f}h (intento #{block_count})"
            )
        else:
            # Primer bloqueo
            self._temporal_blocks[condition_id] = (now + min_hours * 3600, 1)
            logger.info(f"Mercado {condition_id[:8]}... bloqueado por {min_hours:.0f}h")

        self._save_blocks()

    def _load_blocks(self) -> dict[str, tuple[float, int]]:
        """Carga bloques persistidos, descartando los ya expirados.

        Formato nuevo: {cid: (expiry_timestamp, block_count)}
        Formato antiguo (migración): {cid: expiry_timestamp}
        """
        try:
            if self._BLOCKS_FILE.exists():
                raw: dict = json.loads(self._BLOCKS_FILE.read_text("utf-8"))
                now = time.time()
                active: dict[str, tuple[float, int]] = {}
                for cid, entry in raw.items():
                    if isinstance(entry, (int, float)):
                        # Formato antiguo: migrar a nuevo
                        if entry > now:
                            active[cid] = (entry, 1)
                    elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                        exp, count = entry
                        if exp > now:
                            active[cid] = (exp, int(count))
                expired = len(raw) - len(active)
                if active:
                    logger.info(
                        "MarketFilter: %d bloques cargados desde disco (%d expirados ignorados)",
                        len(active), expired,
                    )
                    for cid, (exp, count) in active.items():
                        mins = (exp - now) / 60
                        logger.info("  · %s... bloqueado %.0f min más (intento #%d)", cid[:8], mins, count)
                return active
        except Exception:
            logger.debug("MarketFilter: no se pudo cargar market_blocks.json", exc_info=True)
        return {}

    def _save_blocks(self) -> None:
        """Persiste los bloques activos a disco."""
        try:
            self._BLOCKS_FILE.parent.mkdir(parents=True, exist_ok=True)
            self._BLOCKS_FILE.write_text(
                json.dumps(self._temporal_blocks, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            logger.debug("MarketFilter: no se pudo guardar market_blocks.json", exc_info=True)

    # ------------------------------------------------------------------
    # 2. Deduplicacion
    # ------------------------------------------------------------------

    def deduplicate(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Elimina mercados duplicados basandose en similitud del titulo.

        Usa Jaccard similarity sobre tokens de la pregunta.
        De un grupo de duplicados, conserva el que tiene mayor score o rewards.
        """
        if len(markets) <= 1:
            return markets

        # Indice de grupo: cada mercado apunta a su grupo representante
        groups: list[list[int]] = []
        assigned: set[int] = set()

        for i in range(len(markets)):
            if i in assigned:
                continue
            group = [i]
            assigned.add(i)
            tokens_i = _tokenize(markets[i].get("question", ""))

            for j in range(i + 1, len(markets)):
                if j in assigned:
                    continue
                # No colapsar dos mercados que ambos tienen rewards — son oportunidades independientes
                if markets[i].get("rewards_active") and markets[j].get("rewards_active"):
                    continue
                tokens_j = _tokenize(markets[j].get("question", ""))
                sim = _jaccard_similarity(tokens_i, tokens_j)
                if sim >= self._dedup_similarity_threshold:
                    group.append(j)
                    assigned.add(j)

            groups.append(group)

        # De cada grupo, conservar el mejor (mayor _score o rewards_rate)
        result: list[dict[str, Any]] = []
        deduped_count = 0

        for group in groups:
            if len(group) == 1:
                result.append(markets[group[0]])
            else:
                # Elegir el mercado con mayor score, fallback a rewards
                best_idx = max(
                    group,
                    key=lambda idx: (
                        markets[idx].get("_score", 0),
                        markets[idx].get("rewards_rate", 0),
                    ),
                )
                result.append(markets[best_idx])
                deduped_count += len(group) - 1

        if deduped_count:
            logger.info(
                f"MarketFilter: {deduped_count} mercados duplicados eliminados "
                f"({len(groups)} grupos unicos)"
            )

        return result

    # ------------------------------------------------------------------
    # 3. News-risk filter
    # ------------------------------------------------------------------

    def is_news_dependent(self, market: dict[str, Any]) -> bool:
        """Detecta mercados con alto riesgo de resolucion por noticias.

        Criterios:
        - Mercado nuevo (< min_market_age_hours) con rewards altos (> $500/dia)
        - Mercado con < 48h hasta resolucion y keywords de tiempo

        EXCEPCION: si min_size <= 50, NO aplicar el cooldown por edad nueva.
        Estos son los mercados Epstein-style que justamente queremos capturar al
        lanzamiento (poca competencia inicial). El shadow quoting + reposicionamiento
        cubre el riesgo de news.
        """
        rewards_min_size = float(market.get("rewards_min_size", 0))
        small_market_exception = 0 < rewards_min_size <= 50

        # Mercado nuevo con rewards altos = trampa potencial
        # (saltamos este check para mercados con min_size <= 50)
        if not small_market_exception:
            created_at = market.get("_raw", {}).get("createdAt", "")
            if created_at:
                try:
                    created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                    rewards = market.get("rewards_rate", 0)
                    if age_hours < self._min_market_age_hours and rewards > 500:
                        return True
                except (ValueError, TypeError):
                    pass

        # Mercado cercano a resolucion con keywords temporales
        end_date_str = market.get("end_date", "")
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                hours_left = (end_date - datetime.now(timezone.utc)).total_seconds() / 3600
                if hours_left < 48:
                    question = market.get("question", "").lower()
                    time_keywords = [
                        "today", "tonight", "this week", "tomorrow",
                        "hoy", "esta semana", "manana", "by friday",
                        "by monday", "by tuesday", "by wednesday",
                        "by thursday", "by saturday", "by sunday",
                    ]
                    if any(kw in question for kw in time_keywords):
                        return True
            except (ValueError, TypeError):
                pass

        return False

    def remove_news_dependent(
        self, markets: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Filtra mercados con alto riesgo de noticias."""
        result = [m for m in markets if not self.is_news_dependent(m)]
        removed = len(markets) - len(result)
        if removed:
            logger.info(f"MarketFilter: {removed} mercados removidos por news-risk")
        return result

    # ------------------------------------------------------------------
    # Pipeline completo
    # ------------------------------------------------------------------

    def apply_all(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Aplica todos los filtros en orden."""
        markets = self.remove_banned(markets)
        markets = self.remove_news_dependent(markets)
        markets = self.deduplicate(markets)
        return markets


# ------------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------------

_STOP_WORDS = frozenset({
    "will", "the", "a", "an", "be", "to", "in", "on", "at", "by",
    "of", "or", "and", "is", "it", "for", "this", "that", "with",
    "from", "as", "are", "was", "were", "been", "being", "have",
    "has", "had", "do", "does", "did", "can", "could", "would",
    "should", "may", "might", "shall", "not", "no", "yes",
})


def _tokenize(text: str) -> set[str]:
    """Tokeniza texto en palabras significativas (sin stop words)."""
    words = re.findall(r"[a-z0-9]+", text.lower())
    return {w for w in words if w not in _STOP_WORDS and len(w) > 1}


def _jaccard_similarity(a: set[str], b: set[str]) -> float:
    """Similitud de Jaccard entre dos conjuntos de tokens."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)
