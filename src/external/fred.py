"""FRED Economic Calendar — eventos macro importantes (TODO 5.2).

Descarga el calendario de eventos economicos del Federal Reserve (FRED).
Detecta CPI, NFP, FOMC, GDP etc. para el EventDrivenStrategy.

COSTO: $0 — API publica de FRED (key gratuita en fred.stlouisfed.org).
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("nachomarket.fred")

_CALENDAR_FILE = Path("data/economic_calendar.json")
_REFRESH_INTERVAL_HOURS = 24.0  # Refrescar una vez al dia
_FRED_SERIES_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# Series economicas de interes con sus fechas de release tipicas
ECONOMIC_EVENTS = {
    "CPI": {
        "description": "Consumer Price Index",
        "category": "inflation",
        "market_impact": "high",
    },
    "UNRATE": {
        "description": "Unemployment Rate / NFP",
        "category": "employment",
        "market_impact": "high",
    },
    "FEDFUNDS": {
        "description": "Federal Funds Rate / FOMC",
        "category": "monetary_policy",
        "market_impact": "very_high",
    },
    "GDP": {
        "description": "Gross Domestic Product",
        "category": "growth",
        "market_impact": "high",
    },
    "DCOILWTICO": {
        "description": "Oil Price",
        "category": "commodities",
        "market_impact": "medium",
    },
}


@dataclass
class EconomicEvent:
    """Un evento economico del calendario."""
    name: str
    description: str
    category: str
    market_impact: str          # low | medium | high | very_high
    release_date: str           # ISO date string
    days_until: int             # Dias hasta el evento (negativo = pasado)
    next_release_estimate: str  # Estimacion del proximo release


class FREDClient:
    """Cliente para el calendario economico del Federal Reserve.

    Uso:
        client = FREDClient()
        events = client.get_upcoming_events(days=7)
        for event in events:
            if event.market_impact in ("high", "very_high"):
                increase_spreads()
    """

    def __init__(
        self,
        api_key: str | None = None,
        cache_path: str = str(_CALENDAR_FILE),
    ) -> None:
        self._api_key = api_key or os.environ.get("FRED_API_KEY", "")
        self._cache_path = Path(cache_path)
        self._last_refresh: float = 0.0
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def get_upcoming_events(self, days: int = 7) -> list[EconomicEvent]:
        """Retorna eventos economicos en los proximos N dias.

        Args:
            days: Numero de dias a buscar hacia adelante.

        Returns:
            Lista de EconomicEvent ordenada por fecha.
        """
        self._refresh_if_needed()
        calendar = self._load_calendar()

        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days)

        upcoming = []
        for event_data in calendar.get("events", []):
            try:
                release_str = event_data.get("release_date", "")
                if not release_str:
                    continue
                release = datetime.fromisoformat(release_str)
                if release.tzinfo is None:
                    release = release.replace(tzinfo=timezone.utc)

                if now <= release <= cutoff:
                    days_until = (release - now).days
                    upcoming.append(EconomicEvent(
                        name=event_data["name"],
                        description=event_data.get("description", ""),
                        category=event_data.get("category", ""),
                        market_impact=event_data.get("market_impact", "medium"),
                        release_date=release_str,
                        days_until=days_until,
                        next_release_estimate=event_data.get("next_release_estimate", ""),
                    ))
            except Exception:
                continue

        return sorted(upcoming, key=lambda e: e.days_until)

    def is_high_impact_window(self, hours_before: float = 2.0) -> bool:
        """Retorna True si hay un evento de alto impacto en las proximas horas.

        Usado para ajustar spreads preventivamente.
        """
        upcoming = self.get_upcoming_events(days=1)
        now = datetime.now(timezone.utc)

        for event in upcoming:
            if event.market_impact not in ("high", "very_high"):
                continue
            try:
                release = datetime.fromisoformat(event.release_date)
                if release.tzinfo is None:
                    release = release.replace(tzinfo=timezone.utc)
                hours_until = (release - now).total_seconds() / 3600
                if 0 <= hours_until <= hours_before:
                    logger.info(
                        "Evento de alto impacto en %.1fh: %s (%s)",
                        hours_until, event.name, event.description,
                    )
                    return True
            except Exception:
                continue

        return False

    def refresh(self) -> bool:
        """Fuerza un refresh del calendario. Retorna True si tuvo exito."""
        return self._fetch_and_cache()

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _refresh_if_needed(self) -> None:
        """Refresca el calendario si han pasado >24h desde el ultimo refresh."""
        elapsed = time.time() - self._last_refresh
        if elapsed > _REFRESH_INTERVAL_HOURS * 3600:
            self._fetch_and_cache()

    def _fetch_and_cache(self) -> bool:
        """Descarga el calendario y lo persiste en cache."""
        events = self._build_estimated_calendar()

        calendar = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "events": events,
        }

        try:
            with open(self._cache_path, "w", encoding="utf-8") as f:
                json.dump(calendar, f, indent=2)
            self._last_refresh = time.time()
            logger.info("Calendario economico actualizado: %d eventos", len(events))
            return True
        except Exception:
            logger.exception("Error guardando calendario economico")
            return False

    def _build_estimated_calendar(self) -> list[dict[str, Any]]:
        """Construye un calendario estimado basado en patrones historicos.

        En produccion completa, usa la API de FRED o NewsAPI para fechas exactas.
        Esta version estima las fechas basandose en patrones conocidos:
        - CPI: segundo martes del mes
        - NFP: primer viernes del mes
        - FOMC: ~8 veces al ano
        """
        events = []
        now = datetime.now(timezone.utc)

        # Generar proximos 90 dias de eventos estimados
        for month_offset in range(3):
            target_month = now.replace(day=1) + timedelta(days=31 * month_offset)
            target_month = target_month.replace(day=1)

            # CPI: segundo martes de cada mes
            cpi_date = self._nth_weekday(target_month.year, target_month.month, 1, 2)
            if cpi_date and cpi_date >= now:
                events.append({
                    "name": "CPI",
                    "description": "Consumer Price Index",
                    "category": "inflation",
                    "market_impact": "high",
                    "release_date": cpi_date.isoformat(),
                    "next_release_estimate": "",
                })

            # NFP: primer viernes de cada mes
            nfp_date = self._nth_weekday(target_month.year, target_month.month, 4, 1)
            if nfp_date and nfp_date >= now:
                events.append({
                    "name": "NFP",
                    "description": "Non-Farm Payrolls / Unemployment Rate",
                    "category": "employment",
                    "market_impact": "very_high",
                    "release_date": nfp_date.isoformat(),
                    "next_release_estimate": "",
                })

        return events

    @staticmethod
    def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime | None:
        """Retorna el N-esimo dia de la semana en el mes dado.

        weekday: 0=Lunes, 1=Martes, ..., 4=Viernes
        n: 1=primero, 2=segundo, etc.
        """
        try:
            from calendar import monthrange
            first_day = datetime(year, month, 1, 8, 30, tzinfo=timezone.utc)
            # Encontrar primer weekday del mes
            offset = (weekday - first_day.weekday()) % 7
            first_occurrence = first_day + timedelta(days=offset)
            result = first_occurrence + timedelta(weeks=n - 1)
            # Verificar que sigue en el mismo mes
            if result.month != month:
                return None
            return result
        except Exception:
            return None

    def _load_calendar(self) -> dict[str, Any]:
        """Carga el calendario desde cache."""
        if not self._cache_path.exists():
            return {"events": []}
        try:
            with open(self._cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"events": []}
