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
│   └── backtest.py       # backtester bar-by-bar con fees/slippage, fills unit-testeados
├── user_data/strategies/
│   ├── smc_sweep.py      # estrategia freqtrade (produccion/dry-run/live) — importa smc/signals
│   └── donchian_control.py  # control/benchmark (breakout Donchian)
├── scripts/
│   ├── fetch_data.py     # descarga OHLCV a CSV (ccxt) — correr donde haya acceso a exchanges
│   └── validate.py       # pipeline anti-overfitting: IS / walk-forward / OOS / benchmarks
├── tests/                # tests de deteccion (causalidad) y de fills del backtester
├── config-backtest.json  # config freqtrade para backtesting
├── config-dryrun.json    # config freqtrade para paper trading (dry-run)
└── REGLAS_CONGELADAS.md  # spec pre-registrada (Gate 1) + registro de variantes
```

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

# 1) Smoke del pipeline sobre datos sinteticos (NO es evidencia de edge):
python crypto/scripts/validate.py --synthetic --strategy sweep

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

## Estado

- Detección + backtester + tests: **completos y verificados** (19 tests en verde).
- Validación sobre datos reales: **pendiente** (los exchanges están bloqueados en el entorno
  de desarrollo; correr en el VPS).
- Estrategias freqtrade: escritas contra la API v3, **pendientes de correr en el VPS**.
