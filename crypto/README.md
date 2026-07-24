# NachoMarket — Pivot a bot de crypto SMC

Bot de trading direccional de crypto basado en Smart Money Concepts (turtle soup /
barrido de liquidez), construido tras la fundida del bot de rewards farming de Polymarket.
Diseño guiado por dos principios del usuario: **sin overfitting, sin sobre-ingeniería**, y
la prioridad heredada de **preservación de capital**.

## Por qué existe

El bot anterior se fundió por un fill tóxico: órdenes gigantes single-sided sin tope de
riesgo en dólares, y circuit breakers que solo actuaban *después* del fill. Acá el riesgo es
estructural: **stop obligatorio en toda posición, 1% de riesgo por trade, sin apalancamiento,
y un backtester cuyos fills están unit-testeados para que no puedan mentir** (el paper sim
anterior reportaba 100% winrate porque nunca modelaba fills adversos).

La honestidad primero: **SMC como sistema completo no tiene backtest público riguroso ni
edge demostrado.** Solo se mecaniza el único componente con fundamento (clustering de stops,
Osler / NY Fed). Si la validación out-of-sample dice que no hay edge, no se opera. Ese
resultado también vale: es la fundida que no ocurre.

## Estructura

```
crypto/
├── smc/
│   ├── signals.py        # deteccion SMC pura (pandas/numpy), causal, sin look-ahead
│   ├── backtest.py       # backtester bar-by-bar con fees/slippage, fills unit-testeados
│   ├── stats.py          # Deflated Sharpe Ratio + Monte Carlo de ruina (numpy puro)
│   └── synthetic.py      # generadores de control POSITIVO (sweeps) y NEGATIVO (random-walk)
├── user_data/strategies/
│   ├── smc_sweep.py      # estrategia freqtrade (produccion/dry-run/live) — importa smc/signals
│   └── donchian_control.py  # control/benchmark (breakout Donchian)
├── scripts/
│   ├── fetch_data.py     # descarga OHLCV a CSV (ccxt) — correr donde haya acceso a exchanges
│   ├── validate.py       # pipeline anti-overfitting: IS/walk-forward/OOS/benchmarks/DSR/MonteCarlo
│   ├── param_sweep.py    # grilla de parametros + deteccion de MESETA vs PICO
│   ├── decide.py         # veredicto MECANICO de los gates desde los reportes JSON
│   ├── portfolio.py      # validacion multi-par + correlacion (¿diversifica o solo suma fees?)
│   ├── weekly_review.py  # revision semanal: journal vivo vs cono del backtest -> NORMAL/ALERTA/KILL
│   ├── vps_setup.sh      # setup AISLADO en el VPS (venv propio, no toca el otro bot)
│   └── vps_validate_all.sh  # one-shot VPS: baja datos + gates + veredicto + empaqueta
├── tests/                # 32 tests: causalidad, fills, control positivo/negativo, stats, FVG
├── config-backtest.json  # config freqtrade para backtesting
├── config-dryrun.json    # config freqtrade para paper trading (dry-run)
├── REGLAS_CONGELADAS.md  # spec pre-registrada (Gate 1) + registro de variantes
├── ESTRATEGIA.md         # la estrategia en lenguaje llano + como leer la validacion
├── RECURSOS.md           # recursos curados para aprender (microestructura, quant, freqtrade)
└── DESPLIEGUE_VPS.md     # runbook de despliegue AISLADO en el VPS (sin tocar el otro bot)
```

Para entender el sistema: leer **ESTRATEGIA.md** (qué hace y por qué, mapeado al ebook).
Para seguir aprendiendo: **RECURSOS.md** (libros/papers/repos vetados, con nivel y credibilidad).

**Un solo módulo de detección** (`smc/signals.py`) alimenta tanto el backtester de validación
como la estrategia de freqtrade → las señales no pueden divergir entre backtest y live.

## Setup

```bash
# Dependencias del backtester/validador (ligeras, ya usables):
pip install numpy pandas pytest

# Runtime de produccion (VPS):
pip install freqtrade        # trae ccxt; requiere TA-Lib del sistema
```

## Flujo de trabajo (los gates de REGLAS_CONGELADAS.md, en orden)

```bash
# 0) Correr los tests (deben pasar antes de tocar nada):
python -m pytest crypto/tests -q
#   Incluye el control POSITIVO (con sweeps inyectados la estrategia DEBE ganar) y el
#   NEGATIVO (en random-walk DEBE perder por costos). Un detector roto falla aca.

# 1) Smoke del pipeline. Dos modos para APRENDER a leer la validacion:
python crypto/scripts/validate.py --synthetic --strategy sweep --compare --deflated-sharpe 20
#   SIN edge (random-walk): todo NEGATIVO, DSR ~0. Asi se ve "no hay edge".
python crypto/scripts/validate.py --synthetic-positive --strategy sweep --deflated-sharpe 108
#   CON edge (sweeps inyectados): IS/OOS en verde, walk-forward estable (meseta), DSR pasa,
#   P(ruina)=0. Asi se ve el exito atravesando TODOS los gates. (Sigue sin ser dato real.)

# 1b) Barrido de parametros con deteccion de meseta (anti curve-fitting):
python crypto/scripts/param_sweep.py --synthetic

# 2) Bajar datos reales (en una maquina con acceso a exchanges, p.ej. el VPS):
python crypto/scripts/fetch_data.py --symbol BTC/USDT --timeframe 4h \
    --since 2019-01-01 --out crypto/data/BTC_USDT-4h.csv

# 3) Validar sobre datos reales (IS / walk-forward / OOS / benchmarks):
python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv \
    --strategy sweep --out crypto/data/report_sweep.json
python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv \
    --strategy donchian   # el control: sweep DEBE batirlo

# 4) Backtest en freqtrade (paridad con validate.py) — en el VPS:
freqtrade download-data -c crypto/config-backtest.json --timerange 20190101- --timeframes 4h
freqtrade backtesting -c crypto/config-backtest.json --strategy SmcSweep \
    --strategy-path crypto/user_data/strategies --timerange 20190101-20231231

# 5) Paper trading (dry-run) 4-8 semanas:
freqtrade trade -c crypto/config-dryrun.json --strategy SmcSweep \
    --strategy-path crypto/user_data/strategies
```

## Reglas de riesgo (innegociables)

- Stop obligatorio en toda posición; nunca se mueve en contra.
- 1% de riesgo por trade (por distancia al stop); descartar señales cuyo stop supere el 4%.
- SPOT, sin apalancamiento. Máx 2 trades abiertos.
- Protections activas: StoplossGuard, MaxDrawdown 10%, CooldownPeriod.
- **No pasar a live hasta que el OOS sea creíble y el dry-run confirme fills/fees.**

## Verificación (por qué confiar en el backtester)

- **Control positivo**: sobre series con sweeps inyectados (el fenómeno de Osler), la
  estrategia es rentable neto de costos. → el detector no está muerto.
- **Control negativo**: sobre random-walk, pierde por costos. → el backtester no infla
  resultados (a diferencia del paper sim anterior que reportaba 100% winrate).
- **Deflated Sharpe Ratio**: penaliza el Sharpe por el nº de variantes probadas.
- **Monte Carlo de ruina**: distribución de drawdown y P(ruina) por remuestreo de trades.
- Toda señal es **causal** (tests que exigen que no cambie al agregar barras futuras).

## Estado

- Detección + backtester + estadística + tooling + tests: **completos y verificados**
  (32 tests en verde).
- **FVG**: primitiva testeada (`fair_value_gap`) pero DELIBERADAMENTE fuera del baseline v1
  (evita overfitting); base de un futuro setup `fvg_pullback` solo si el núcleo valida.
- Validación sobre datos reales: **pendiente** (los exchanges están bloqueados en el entorno
  de desarrollo; correr en el VPS).
- Estrategias freqtrade: escritas contra la API v3, **pendientes de correr en el VPS**.
