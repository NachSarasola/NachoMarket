# NachoMarket — Bot de Trading para Polymarket

## Vision general

Bot automatizado de market making y trading para Polymarket. 

Opera 24/7 en VPS AWS Lightsail (Dublin, IE - 3.252.244.181). Capital: $166 USDC.

El CLOB de Polymarket corre en AWS eu-west-2 (London). Esta VPS está en
eu-west-1 (Dublin), garantizando baja latencia (~2-5ms).
scripts/check_geo.py verifica acceso via /api/geoblock antes de arrancar.

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
- python scripts/check_geo.py — Verificar acceso geo antes de arrancar

## Deploy al VPS

- bash scripts/deploy.sh ubuntu@<IP-VPS> — Sync de archivos al VPS
- bash scripts/deploy.sh ubuntu@<IP-VPS> --setup — Sync + setup completo
- ssh ubuntu@<IP-VPS> — Conectar al VPS
- sudo systemctl start polymarket-bot — Arrancar bot en VPS
- sudo journalctl -u polymarket-bot -f — Ver logs en vivo

## Reglas INQUEBRANTABLES

- JAMAS commitear .env ni private keys
- JAMAS arriesgar mas del 5% del capital en un solo mercado
- SIEMPRE usar try/except con retry en llamadas a API
- SIEMPRE loguear cada decision de trading a data/trades.jsonl
- SIEMPRE verificar feeRateBps dinamicamente antes de operar
- SIEMPRE usar Post Only para market making (evitar pagar taker fees)
- El bot DEBE poder pausarse instantaneamente via Telegram /pause
- Si el drawdown diario supera $8.3 (5% de $166), PARAR todo el trading
- Cada 8 horas, ejecutar self-review con Claude Haiku
- NUNCA operar en mercados sin liquidity rewards Y con volume < $1,000 diario

## Estilo de codigo

- Type hints en todas las funciones
- Docstrings en funciones publicas
- Usar logging (no print)
- snake_case para todo
- Config en YAML, nunca hardcodeada

---

# Pivot: bot de crypto SMC (directorio `crypto/`)

Tras fundir el capital con el rewards farming single-sided de Polymarket (fill toxico
sin tope de riesgo en dolares), se pivota a un bot de crypto DIRECCIONAL basado en Smart
Money Concepts (barrido de liquidez / turtle soup). El codigo de Polymarket queda intacto;
el pivot vive aislado en `crypto/`. Ver `crypto/README.md` y `crypto/REGLAS_CONGELADAS.md`.

## Principios del pivot (del usuario)

- SIN overfitting, SIN sobre-ingenieria. Presupuesto duro: 3-5 parametros libres.
- Preservacion de capital primero. Con $150-500 el objetivo es UN setup validado, no ingresos.
- Solo se mecaniza el componente SMC con fundamento (clustering de stops, Osler/NY Fed).
  Si el out-of-sample no muestra edge, NO se opera.

## Stack del pivot

- numpy + pandas (deteccion y backtester) — ligero, ya usable.
- freqtrade (runtime de produccion: backtest + dry-run + live, Telegram nativo) — en el VPS.
- Un solo modulo de deteccion (`crypto/smc/signals.py`) alimenta backtester y freqtrade →
  las senales no pueden divergir entre validacion y live.

## Comandos del pivot

- python -m pytest crypto/tests -q — 32 tests: causalidad, fills, control positivo/negativo, stats, FVG
- python crypto/scripts/validate.py --synthetic --strategy sweep --compare --deflated-sharpe 20 — Smoke del pipeline (+ DSR + Monte Carlo)
- python crypto/scripts/param_sweep.py --synthetic — Barrido de parametros + deteccion de meseta vs pico
- python crypto/scripts/fetch_data.py --symbol BTC/USDT --timeframe 4h --since 2019-01-01 --out crypto/data/BTC_USDT-4h.csv
- python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv --strategy sweep --compare --deflated-sharpe <N_variantes> — Validacion real
- freqtrade backtesting -c crypto/config-backtest.json --strategy SmcSweep --strategy-path crypto/user_data/strategies
- freqtrade trade -c crypto/config-dryrun.json --strategy SmcSweep --strategy-path crypto/user_data/strategies — Paper

## Reglas INQUEBRANTABLES del pivot

- Stop OBLIGATORIO en toda posicion; JAMAS mover el stop en contra.
- Riesgo 1% del equity por trade (por distancia al stop); descartar senal si el stop supera 4%.
- SPOT, SIN apalancamiento. Solo BTC/USDT y ETH/USDT. Timeframe 4h (nada < 1h: los costos matan).
- El backtester DEBE modelar fees + slippage + fills adversos. Un backtest con 100% winrate MIENTE.
- NO pasar a live hasta: OOS 2024+ creible (Sharpe OOS >= 50% del IS), batir buy&hold y MA diaria,
  y confirmar fills/fees en dry-run 4-8 semanas.
- Toda variante de parametros probada se anota en `crypto/REGLAS_CONGELADAS.md` (multiple-testing).
