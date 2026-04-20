"""Matriz de correlacion rolling entre mercados (TODO 2.4).

Detecta clusters de mercados correlacionados para evitar concentracion
de riesgo oculta. Si la exposicion combinada en un cluster supera $100,
bloquea nuevas entradas.
"""

import logging
import math
import time
from collections import deque
from typing import Any

logger = logging.getLogger("nachomarket.correlation")

_DEFAULT_WINDOW_DAYS = 30
_DEFAULT_MAX_CLUSTER_EXPOSURE = 100.0   # $100 max en mercados con corr > threshold
_DEFAULT_CORR_THRESHOLD = 0.70          # r > 0.7 = mismo cluster


class CorrelationTracker:
    """Mantiene correlaciones rolling entre midprices de mercados activos.

    Uso:
        tracker = CorrelationTracker()
        tracker.update("token_a", 0.55)
        tracker.update("token_b", 0.48)
        matrix = tracker.compute_matrix()
        clusters = tracker.get_clusters(threshold=0.7)
        blocked = tracker.check_cluster_limit(positions)
    """

    def __init__(
        self,
        window_days: int = _DEFAULT_WINDOW_DAYS,
        max_price_points: int = 1440,   # 1 punto por minuto x 24h
        corr_threshold: float = _DEFAULT_CORR_THRESHOLD,
        max_cluster_exposure: float = _DEFAULT_MAX_CLUSTER_EXPOSURE,
    ) -> None:
        self._window_days = window_days
        self._max_points = max_price_points
        self._corr_threshold = corr_threshold
        self._max_cluster_exposure = max_cluster_exposure
        # token_id → deque de (timestamp, price)
        self._price_series: dict[str, deque[tuple[float, float]]] = {}

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def update(self, token_id: str, midprice: float) -> None:
        """Registra un nuevo midprice con timestamp actual."""
        if token_id not in self._price_series:
            self._price_series[token_id] = deque(maxlen=self._max_points)
        if midprice > 0:
            self._price_series[token_id].append((time.time(), midprice))

    def compute_matrix(self) -> dict[str, dict[str, float]]:
        """Calcula la matriz de correlacion Pearson entre todos los pares de tokens.

        Solo incluye tokens con >= 10 puntos en comun en la ventana de tiempo.

        Returns:
            Dict anidado {token_a: {token_b: correlation_coeff}}.
        """
        tokens = list(self._price_series.keys())
        matrix: dict[str, dict[str, float]] = {t: {} for t in tokens}

        cutoff = time.time() - self._window_days * 86400

        for i, ta in enumerate(tokens):
            for j, tb in enumerate(tokens):
                if i >= j:
                    matrix[ta][tb] = 1.0 if i == j else matrix.get(tb, {}).get(ta, 0.0)
                    continue

                # Alinear series temporales por proximidad (join aproximado)
                corr = self._compute_pearson(ta, tb, cutoff)
                matrix[ta][tb] = corr
                matrix[tb][ta] = corr

        return matrix

    def get_clusters(
        self,
        threshold: float | None = None,
    ) -> list[list[str]]:
        """Identifica clusters de mercados con correlacion > threshold.

        Usa union-find para agrupar mercados correlacionados.

        Returns:
            Lista de clusters (cada cluster es una lista de token_ids).
        """
        th = threshold or self._corr_threshold
        matrix = self.compute_matrix()
        tokens = list(matrix.keys())

        # Union-Find simple
        parent = {t: t for t in tokens}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: str, y: str) -> None:
            parent[find(x)] = find(y)

        for ta in tokens:
            for tb in tokens:
                if ta != tb and abs(matrix.get(ta, {}).get(tb, 0.0)) >= th:
                    union(ta, tb)

        # Agrupar por raiz
        groups: dict[str, list[str]] = {}
        for t in tokens:
            root = find(t)
            groups.setdefault(root, []).append(t)

        return [g for g in groups.values() if len(g) > 1]

    def check_cluster_limit(
        self,
        positions: dict[str, float],
        max_exposure: float | None = None,
    ) -> dict[str, list[str]]:
        """Verifica si algún cluster excede el limite de exposicion.

        Args:
            positions: {token_id: exposure_usdc}
            max_exposure: Override del limite por cluster.

        Returns:
            Dict {cluster_root: [token_ids]} de clusters bloqueados.
        """
        limit = max_exposure or self._max_cluster_exposure
        clusters = self.get_clusters()
        blocked: dict[str, list[str]] = {}

        for cluster in clusters:
            exposure = sum(
                abs(positions.get(t, 0.0)) for t in cluster
            )
            if exposure > limit:
                root = cluster[0]
                blocked[root] = cluster
                logger.warning(
                    "Cluster bloqueado: %d mercados con correlacion alta, "
                    "exposicion $%.2f > limite $%.2f",
                    len(cluster), exposure, limit,
                )

        return blocked

    def format_telegram(self, top_n: int = 5) -> str:
        """Formatea los clusters mas correlacionados para Telegram /correlation."""
        clusters = self.get_clusters()
        if not clusters:
            return "Sin clusters de correlacion detectados (r > 0.7)."

        lines = [f"*Correlation Clusters* (r > {self._corr_threshold:.0%})\n"]
        for i, cluster in enumerate(clusters[:top_n], 1):
            short_ids = [t[:8] for t in cluster]
            lines.append(f"{i}. `{'`, `'.join(short_ids)}`... ({len(cluster)} tokens)")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _compute_pearson(
        self,
        ta: str,
        tb: str,
        cutoff: float,
    ) -> float:
        """Calcula correlacion Pearson entre dos series de precios alineadas."""
        series_a = [(ts, p) for ts, p in self._price_series.get(ta, []) if ts >= cutoff]
        series_b = [(ts, p) for ts, p in self._price_series.get(tb, []) if ts >= cutoff]

        if len(series_a) < 10 or len(series_b) < 10:
            return 0.0

        # Usar solo los precios (sin alineacion temporal exacta)
        # Tomar la misma cantidad de puntos equiespaciados
        n = min(len(series_a), len(series_b))
        if n < 5:
            return 0.0

        # Submuestrear si es necesario
        step_a = max(1, len(series_a) // n)
        step_b = max(1, len(series_b) // n)
        prices_a = [p for _, p in series_a[::step_a]][:n]
        prices_b = [p for _, p in series_b[::step_b]][:n]

        n = min(len(prices_a), len(prices_b))
        if n < 5:
            return 0.0

        prices_a = prices_a[:n]
        prices_b = prices_b[:n]

        return _pearson(prices_a, prices_b)


def _pearson(xs: list[float], ys: list[float]) -> float:
    """Coeficiente de correlacion de Pearson."""
    n = len(xs)
    if n < 2:
        return 0.0

    mx = sum(xs) / n
    my = sum(ys) / n

    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))

    if den_x == 0 or den_y == 0:
        return 0.0

    return num / (den_x * den_y)
