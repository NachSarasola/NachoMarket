# NachoMarket — Bot de crypto SMC (sweep de liquidez)

## Vision general

Bot de trading DIRECCIONAL de criptomonedas basado en el unico componente de Smart Money
Concepts con fundamento medible: el barrido de liquidez / turtle soup (clustering de stops,
Osler NY Fed). Todo el bot vive en `crypto/`.

Historia: el bot anterior de Polymarket (rewards farming single-sided) fundio el capital por
un fill toxico sin tope de riesgo. Fue DESCARTADO de este branch; sigue integro en `master` y
en el historial de git. En el VPS (Dublin) convive OTRO bot en `~/nachomarket` — **no tocarlo**;
el bot de crypto se despliega aislado en `~/nacho-crypto` (ver `crypto/DESPLIEGUE_VPS.md`).

PRIORIDAD ABSOLUTA: preservacion de capital. La tesis, el roadmap por etapas y el ciclo de
revision diaria/semanal viven en `crypto/TESIS.md`. Las reglas pre-registradas y el registro
de variantes (multiple-testing) en `crypto/REGLAS_CONGELADAS.md`.

## Principios (del usuario)

- SIN overfitting, SIN sobre-ingenieria. Presupuesto duro: 3-5 parametros libres por estrategia.
- Con $150-500 el objetivo es UN setup validado, no ingresos. Sobrevivir > ganar.
- Si el out-of-sample no muestra edge, NO se opera. Ese "no" tambien es un resultado.

## Stack

- numpy + pandas (deteccion `crypto/smc/signals.py` + backtester `crypto/smc/backtest.py`).
- freqtrade (runtime de produccion: backtest + dry-run + live + Telegram) — en el VPS.
- Un solo modulo de deteccion alimenta backtester y freqtrade → las senales no pueden divergir.

## Estructura

- crypto/smc/ — deteccion causal, backtester honesto, event-study (H7/H8), stats (DSR/MC), sinteticos
- crypto/user_data/strategies/ — SmcSweep + DonchianControl (freqtrade v3)
- crypto/scripts/ — fetch_data/funding/unlocks/listings, validate + event_validate (gates), param_sweep, vps_* (aislados)
- crypto/tests/ — 104 tests: causalidad, fills, controles positivo/negativo, stats, regimen, eventos
- Docs: README (uso), ESTRATEGIA (que hace y por que), TESIS (tesis/roadmap/revision),
  REGLAS_CONGELADAS (spec + trials), RECURSOS (aprendizaje), DESPLIEGUE_VPS (runbook)

## Comandos

- python -m pytest crypto/tests -q — correr TODOS los tests antes de tocar nada
- python crypto/scripts/validate.py --synthetic --strategy sweep --compare — control negativo
- python crypto/scripts/validate.py --synthetic-positive --strategy sweep — pipeline en verde
- python crypto/scripts/param_sweep.py --synthetic — meseta vs pico de parametros
- python crypto/scripts/fetch_data.py --symbol BTC/USDT --timeframe 4h --since 2019-01-01 --out crypto/data/BTC_USDT-4h.csv
- python crypto/scripts/validate.py --data <csv> --strategy sweep --compare --deflated-sharpe <N> --trades-out journal.csv
- python crypto/scripts/decide.py crypto/data/report_*.json — veredicto mecanico de los gates
- python crypto/scripts/portfolio.py --data <csv> <csv> — multi-par + correlacion (¿diversifica?)
- python crypto/scripts/weekly_review.py --freqtrade-csv <trades.csv> --baseline <report.json> — review semanal
- bash crypto/scripts/vps_validate_all.sh — one-shot en el VPS (baja datos + gates + veredicto)
- bash crypto/scripts/vps_run_hipotesis.sh — one-shot del programa de hipotesis (H1+H2 congeladas + veredicto)
- bash crypto/scripts/vps_run_eventos.sh — one-shot event-driven (H7 unlocks + H8 listings, tesis invertida)
- bash crypto/scripts/vps_run_cascadas.sh — one-shot H9 (cascadas: OI 5m + eventos de purga + gates)
- python crypto/scripts/event_validate.py --strategy h7_unlock --events <csv> --data-dir <dir> — event study con gates
- python crypto/scripts/carry_monitor.py --snapshot --capital 500 — funding multi-venue + viabilidad del carry
- python crypto/scripts/carry_monitor.py --plan --start 300 --monthly 200 — meses hasta $5k (Etapa 3)
- python crypto/scripts/budget_review.py --journal crypto/data/riesgo_vivo.csv — presupuesto de riesgo vivo (B6)
- bash crypto/scripts/vps_quarterly.sh — review trimestral: re-corre specs muertas + carry (vigilancia de regimen)
- freqtrade backtesting -c crypto/config-backtest.json --strategy SmcSweep --strategy-path crypto/user_data/strategies --enable-protections
- freqtrade trade -c crypto/config-dryrun.json --strategy SmcSweep --strategy-path crypto/user_data/strategies — paper

## Reglas INQUEBRANTABLES

- JAMAS commitear .env ni claves API.
- Stop OBLIGATORIO en toda posicion; JAMAS mover el stop en contra.
- Riesgo 1% del equity por trade (por distancia al stop); descartar senal si el stop supera 4%.
- v1: SPOT sin apalancamiento, solo BTC/USDT y ETH/USDT, timeframe 4h (nada < 1h).
- Un backtest con 100% winrate MIENTE (fue la senal que enmascaro la fundida anterior).
- NO pasar a live sin: OOS 2024+ creible (Sharpe OOS >= 50% del IS), batir buy&hold y MA
  diaria neto de costos, DSR decente, y 4-8 semanas de dry-run con fills = simulados.
- Toda variante probada se anota en REGLAS_CONGELADAS.md (cuenta para el Deflated Sharpe).
- Cambios de regimen/parametros: solo con >=30 trades de evidencia y en el review trimestral.
- En el VPS: NUNCA tocar ~/nachomarket ni el servicio del otro bot.

## Estilo de codigo

- Type hints en todas las funciones; docstrings en publicas; logging (no print); snake_case.
- Causalidad estricta: toda senal en la barra t usa solo datos <= t (hay tests que lo exigen).
