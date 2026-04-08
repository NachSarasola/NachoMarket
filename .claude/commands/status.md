# Status

Muestra el estado actual del bot:

1. Lee `data/state.json` para obtener estado actual (running/paused/stopped)
2. Lee las ultimas 10 lineas de `data/trades.jsonl` para trades recientes
3. Verifica si hay reviews pendientes en `data/reviews/`
4. Muestra posiciones abiertas y PnL no realizado
5. Verifica salud de conexiones (API, WebSocket, Telegram)
6. Resume todo en un dashboard compacto
