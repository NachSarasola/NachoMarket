"""Tests para el Dead Man's Switch (health monitor) del OrderbookFeed.

Cubre:
- mark_message_received / seconds_since_last_message
- is_healthy con distintos thresholds
- _health_monitor_loop dispara "stale" callback al superar threshold
- _health_monitor_loop dispara "recovered" al recibir nuevo mensaje
- Multiples callbacks se invocan todos
- Callback "stale" no se repite si el estado ya es stale
- Health task se cancela correctamente en stop()
- Integracion: _process_message actualiza el timestamp
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.polymarket.websocket import (
    DEFAULT_STALENESS_THRESHOLD_SEC,
    HEALTH_CHECK_INTERVAL_SEC,
    OrderbookFeed,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_feed(threshold: float = 60.0) -> OrderbookFeed:
    return OrderbookFeed(staleness_threshold_sec=threshold)


async def _run_health_loop_ticks(feed: OrderbookFeed, ticks: int) -> None:
    """Ejecuta el loop de health por N ciclos simulados."""
    feed._running = True
    with patch("src.polymarket.websocket.HEALTH_CHECK_INTERVAL_SEC", 0.0):
        task = asyncio.create_task(feed._health_monitor_loop())
        # Dar al loop asyncio tiempo para ejecutar los ticks
        for _ in range(ticks):
            await asyncio.sleep(0)
        # Un sleep corto para que el loop procese
        await asyncio.sleep(0.05)
        feed._running = False
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


# ---------------------------------------------------------------------------
# mark_message_received / seconds_since_last_message
# ---------------------------------------------------------------------------

class TestMarkMessage:
    def test_never_received_returns_inf(self):
        feed = make_feed()
        assert feed.seconds_since_last_message() == float("inf")

    def test_mark_updates_timestamp(self):
        feed = make_feed()
        before = time.time()
        feed.mark_message_received()
        after = time.time()
        elapsed = feed.seconds_since_last_message()
        # Debería ser < 1 segundo
        assert 0.0 <= elapsed < 1.0
        assert before <= feed._last_message_time <= after

    def test_mark_twice_updates_again(self):
        feed = make_feed()
        feed.mark_message_received()
        t1 = feed._last_message_time
        time.sleep(0.01)
        feed.mark_message_received()
        t2 = feed._last_message_time
        assert t2 >= t1


# ---------------------------------------------------------------------------
# is_healthy
# ---------------------------------------------------------------------------

class TestIsHealthy:
    def test_never_received_is_not_healthy(self):
        feed = make_feed(threshold=60.0)
        # Sin mensajes, seconds_since = inf → no saludable
        assert not feed.is_healthy()

    def test_just_received_is_healthy(self):
        feed = make_feed(threshold=60.0)
        feed.mark_message_received()
        assert feed.is_healthy()

    def test_stale_feed_not_healthy(self):
        feed = make_feed(threshold=60.0)
        # Simular mensaje recibido hace 90 segundos
        feed._last_message_time = time.time() - 90.0
        assert not feed.is_healthy()

    def test_custom_threshold_override(self):
        feed = make_feed(threshold=60.0)
        feed._last_message_time = time.time() - 5.0
        # Con threshold de 3s → stale
        assert not feed.is_healthy(staleness_threshold_sec=3.0)
        # Con threshold de 60s → saludable
        assert feed.is_healthy(staleness_threshold_sec=60.0)

    def test_threshold_boundary_exact(self):
        feed = make_feed(threshold=10.0)
        # Usar tiempo fijo para que el test no sea flaky: 9.9s < 10s → saludable
        feed._last_message_time = time.time() - 9.9
        assert feed.is_healthy()


# ---------------------------------------------------------------------------
# register_health_callback
# ---------------------------------------------------------------------------

class TestRegisterCallback:
    def test_register_single_callback(self):
        feed = make_feed()
        cb = MagicMock()
        feed.register_health_callback(cb)
        assert len(feed._health_callbacks) == 1
        assert feed._health_callbacks[0] is cb

    def test_register_multiple_callbacks(self):
        feed = make_feed()
        cb1 = MagicMock()
        cb2 = MagicMock()
        cb3 = MagicMock()
        feed.register_health_callback(cb1)
        feed.register_health_callback(cb2)
        feed.register_health_callback(cb3)
        assert len(feed._health_callbacks) == 3

    def test_fire_all_callbacks_on_stale(self):
        feed = make_feed()
        results = []
        feed.register_health_callback(lambda e, s: results.append((e, s)))
        feed.register_health_callback(lambda e, s: results.append((e, s)))

        feed._fire_health_callbacks("stale", 75.0)

        assert len(results) == 2
        assert all(r == ("stale", 75.0) for r in results)

    def test_callback_exception_doesnt_stop_others(self):
        feed = make_feed()
        bad_cb = MagicMock(side_effect=RuntimeError("boom"))
        good_results = []
        feed.register_health_callback(bad_cb)
        feed.register_health_callback(lambda e, s: good_results.append(e))

        # No debe lanzar excepción
        feed._fire_health_callbacks("stale", 80.0)

        assert good_results == ["stale"]


# ---------------------------------------------------------------------------
# _health_monitor_loop — dispara "stale"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestHealthMonitorStale:
    async def test_no_callback_before_first_message(self):
        """Si nunca se recibió mensaje, el loop NO dispara stale."""
        feed = make_feed(threshold=0.001)  # threshold muy pequeño
        cb = MagicMock()
        feed.register_health_callback(cb)
        # last_message_time == 0.0 → loop hace continue

        await _run_health_loop_ticks(feed, ticks=5)

        cb.assert_not_called()

    async def test_stale_fires_when_threshold_exceeded(self):
        """Loop dispara 'stale' cuando staleness > threshold."""
        feed = make_feed(threshold=0.01)  # 10ms threshold
        cb = MagicMock()
        feed.register_health_callback(cb)

        # Simular mensaje recibido hace 1 segundo (muy stale)
        feed._last_message_time = time.time() - 1.0

        await _run_health_loop_ticks(feed, ticks=3)

        # Debe haber sido llamado exactamente 1 vez con "stale"
        stale_calls = [c for c in cb.call_args_list if c[0][0] == "stale"]
        assert len(stale_calls) >= 1
        assert stale_calls[0][0][1] > 0  # staleness > 0

    async def test_stale_fires_only_once(self):
        """El callback 'stale' se dispara solo una vez aunque el loop corra varias veces."""
        feed = make_feed(threshold=0.01)
        cb = MagicMock()
        feed.register_health_callback(cb)
        feed._last_message_time = time.time() - 1.0

        # Múltiples ticks
        await _run_health_loop_ticks(feed, ticks=10)

        stale_calls = [c for c in cb.call_args_list if c[0][0] == "stale"]
        # Solo 1 llamada "stale" (idempotente mientras siga stale)
        assert len(stale_calls) == 1


# ---------------------------------------------------------------------------
# _health_monitor_loop — dispara "recovered"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestHealthMonitorRecovered:
    async def test_recovered_fires_after_stale(self):
        """Loop dispara 'recovered' cuando se recibe mensaje nuevo tras estar stale."""
        feed = make_feed(threshold=0.01)
        events = []
        feed.register_health_callback(lambda e, s: events.append(e))

        # Primero: stale
        feed._last_message_time = time.time() - 1.0
        await _run_health_loop_ticks(feed, ticks=3)

        # Ahora: recuperar
        feed.mark_message_received()
        await _run_health_loop_ticks(feed, ticks=3)

        assert "stale" in events
        assert "recovered" in events

    async def test_recovered_resets_stale_flag(self):
        """Tras 'recovered', _is_stale vuelve a False."""
        # Usar threshold de 10s para que mark_message_received() no vuelva a expirar
        feed = make_feed(threshold=10.0)
        feed.register_health_callback(MagicMock())
        # Simular stale directamente
        feed._last_message_time = time.time() - 20.0

        await _run_health_loop_ticks(feed, ticks=3)
        assert feed._is_stale is True

        # Recuperar: mensaje reciente dentro del threshold de 10s
        feed.mark_message_received()
        await _run_health_loop_ticks(feed, ticks=3)
        assert feed._is_stale is False


# ---------------------------------------------------------------------------
# Integración: _process_message actualiza timestamp
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestProcessMessageUpdatesTimestamp:
    async def test_process_message_marks_received(self):
        feed = make_feed()
        feed.subscribe("token_abc", callback=MagicMock(), condition_id="cond_1")

        # Simular mensaje book event
        import json
        event = json.dumps({
            "event_type": "book",
            "asset_id": "token_abc",
            "sequence": 1,
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}],
        })

        before = time.time()
        await feed._process_message(event)
        after = time.time()

        assert before <= feed._last_message_time <= after + 0.1

    async def test_process_invalid_json_doesnt_crash(self):
        """Mensaje no-JSON no rompe el feed ni actualiza timestamp erróneamente."""
        feed = make_feed()
        before_time = feed._last_message_time  # 0.0

        # JSON inválido: mark_message_received se llama ANTES del parse
        # pero el error se maneja silenciosamente
        await feed._process_message("not-json!!!")

        # El timestamp SÍ se actualiza (mark_message_received antes del parse)
        # Esto es correcto: cualquier byte del servidor indica que está vivo
        assert feed._last_message_time >= before_time


# ---------------------------------------------------------------------------
# stop() cancela el health task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestStopCancelsHealthTask:
    async def test_stop_cancels_health_task(self):
        feed = make_feed()
        feed._running = True

        # Arrancar el health task manualmente
        feed._health_task = asyncio.create_task(feed._health_monitor_loop())
        await asyncio.sleep(0.01)

        assert not feed._health_task.done()

        await feed.stop()

        # Dar tiempo a la cancelación
        await asyncio.sleep(0.05)
        assert feed._health_task.done()

    async def test_stop_idempotent_without_task(self):
        """stop() no falla si no hay health task activo."""
        feed = make_feed()
        feed._running = False
        feed._health_task = None
        # No debe lanzar excepción
        await feed.stop()


# ---------------------------------------------------------------------------
# Integración main.py: _on_feed_health_event
# ---------------------------------------------------------------------------

class TestMainFeedHealthIntegration:
    """Verifica que NachoMarketBot registra y reacciona al health callback."""

    def _make_bot_stub(self):
        """Crea un stub minimal de NachoMarketBot con los atributos clave."""
        import types
        from src.polymarket.websocket import OrderbookFeed

        bot = types.SimpleNamespace()
        bot._feed = OrderbookFeed()
        bot._state = "running"
        bot._feed_was_stale = False
        bot._paused_by_feed = False

        # Mock de los métodos que se invocan
        bot.pause = MagicMock(side_effect=lambda: setattr(bot, "_state", "paused"))
        bot._client = MagicMock()
        bot._client.cancel_all_orders = MagicMock()
        bot._logger = MagicMock()

        # Extraer el método real del módulo main
        import importlib
        import sys
        # Importar sin ejecutar main()
        from src.main import NachoMarketBot
        bot._on_feed_health_event = NachoMarketBot._on_feed_health_event.__get__(bot)

        return bot

    @patch("src.main.send_alert")
    def test_stale_event_pauses_bot(self, mock_alert):
        bot = self._make_bot_stub()
        bot._on_feed_health_event("stale", 75.0)

        bot.pause.assert_called_once()
        assert bot._state == "paused"

    @patch("src.main.send_alert")
    def test_stale_event_cancels_orders(self, mock_alert):
        bot = self._make_bot_stub()
        bot._on_feed_health_event("stale", 75.0)

        bot._client.cancel_all_orders.assert_called_once()

    @patch("src.main.send_alert")
    def test_stale_event_sends_critical_alert(self, mock_alert):
        bot = self._make_bot_stub()
        bot._on_feed_health_event("stale", 75.0)

        mock_alert.assert_called_once()
        alert_text = mock_alert.call_args[0][0]
        assert "DEAD MAN" in alert_text or "SWITCH" in alert_text

    @patch("src.main.send_alert")
    def test_stale_event_idempotent(self, mock_alert):
        """Doble stale no pausa dos veces ni envía dos alertas."""
        bot = self._make_bot_stub()
        bot._on_feed_health_event("stale", 75.0)
        bot._on_feed_health_event("stale", 80.0)

        # pause y cancel solo una vez
        assert bot.pause.call_count == 1
        assert mock_alert.call_count == 1

    @patch("src.main.send_alert")
    def test_recovered_event_resets_stale_flag(self, mock_alert):
        bot = self._make_bot_stub()
        bot._feed_was_stale = True  # Simular estado stale previo

        bot._on_feed_health_event("recovered", 2.5)

        assert bot._feed_was_stale is False
        mock_alert.assert_called_once()
        alert_text = mock_alert.call_args[0][0]
        assert "recuperado" in alert_text.lower() or "WS" in alert_text

    @patch("src.main.send_alert")
    def test_recovered_does_not_auto_resume(self, mock_alert):
        """El bot NO se reanuda automáticamente tras recovered (requiere /resume)."""
        bot = self._make_bot_stub()
        bot._state = "paused"
        bot._feed_was_stale = True
        # Agregar método resume como mock
        bot.resume = MagicMock()

        bot._on_feed_health_event("recovered", 2.5)

        bot.resume.assert_not_called()
        assert bot._state == "paused"
