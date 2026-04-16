# NachoMarket — Bot de Trading para Polymarket

## Vision general

Bot automatizado de market making y trading para Polymarket. 

Opera 24/7 en Oracle Cloud Free Tier. Capital: $400 USDC.

PRIORIDAD ABSOLUTA: preservacion de capital. Nunca arriesgar >5% en un solo mercado.

## Stack tecnico

- Python 3.11+, pip, venv
- py-clob-client (SDK oficial de Polymarket)
- websockets (para orderbook real-time)
- anthropic (Claude Haiku para self-review cada 8h)
- python-telegram-bot (alertas y control)
- pyyaml (configuracion)
- tenacity (retry con backoff)
- schedule (tareas periodicas)

## Directorios clave

- src/polymarket/ — Conexion a Polymarket CLOB API
- src/strategy/ — Estrategias de trading (market_maker, multi_arb, directional)
- src/risk/ — Position sizing (Kelly fraccional), circuit breakers, inventory management
- src/review/ — Self-review cada 8 horas con Claude Haiku (~$0.01/review)
- src/telegram/ — Notificaciones y comandos (/status, /pause, /resume, /kill)
- config/ — YAML con parametros del bot (NO hardcodear valores)
- data/ — Logs de trades en JSONL, estado persistente en JSON

## Comandos

- python -m pytest tests/ — Correr tests
- python src/main.py — Arrancar bot (lee modo de config/settings.yaml)
- python src/main.py --paper — Modo paper trading (simula sin dinero real)
- python src/review/self_review.py — Forzar self-review manual

## Reglas INQUEBRANTABLES

- JAMAS commitear .env ni private keys
- JAMAS arriesgar mas del 5% del capital en un solo mercado
- SIEMPRE usar try/except con retry en llamadas a API
- SIEMPRE loguear cada decision de trading a data/trades.jsonl
- SIEMPRE verificar feeRateBps dinamicamente antes de operar
- SIEMPRE usar Post Only para market making (evitar pagar taker fees)
- El bot DEBE poder pausarse instantaneamente via Telegram /pause
- Si el drawdown diario supera $20 (5% de $400), PARAR todo el trading
- Cada 8 horas, ejecutar self-review con Claude Haiku
- NUNCA operar en mercados sin liquidity rewards Y con volume < $1,000 diario

## Estilo de codigo

- Type hints en todas las funciones
- Docstrings en funciones publicas
- Usar logging (no print)
- snake_case para todo
- Config en YAML, nunca hardcodeada
