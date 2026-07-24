# Programa de investigación de edge — v2 (post GATE 1)

> Objetivo: **encontrar el edge y aprender de lo que no lo es** — con el sistema que acaba de
> falsar la primera hipótesis por $0. Método: familias de hipótesis, UNA a la vez, reglas
> congeladas antes de tocar datos, pipeline completo (IS → walk-forward → OOS una pasada →
> DSR → benchmarks → decide.py), y cada intento se anota. **Investigar es continuo; operar
> en LIVE sigue gateado.** Trials acumulados hasta hoy: **~113** (base + grilla 108 + runs).

## Lo que los datos reales ya nos enseñaron (post-mortem GATE 1, 2026-07-24)

1. **Sweep-reversal long-only (SMC turtle soup): FALSADO en toda la grilla.** 108 combos, el
   mejor dio Sharpe -0.32 (y era un pico, no meseta). No es tuning: la familia como se
   especificó no captura edge neto de costos en majors 4h. RIP, y a mucha honra: eso era SMC
   mecanizado sin adornos.
2. **Asimetría de régimen brutal y consistente** (la lección #1): las 4 combinaciones
   estrategia×par solo ganaron en regímenes `up_*` (precio > SMA 30d) y perdieron en `dn_*`.
   Operar señales long con precio bajo la media de 30d destruyó valor SIEMPRE.
3. **La celda `up_hi` del sweep en BTC** (n=20, winrate 0.80, avgR +0.47) es la única vida del
   sweep → semilla de la hipótesis H2, pero n<30: no es evidencia todavía.
4. **`ma_cross` (SMA200 long/flat) batió a todo** — Sharpe IS 1.45 (BTC) / 1.56 (ETH), 16x/42x.
   El edge real observado en NUESTROS datos es el filtro de tendencia, coincidente con la
   literatura (time-series momentum). Su OOS todavía NO fue mirado → es testeable limpio.
5. **Trend-following 4h (donchian) murió en OOS 2024+** (chop post-ETF): cualquier hipótesis
   de tendencia debe demostrar que sobrevive ese régimen o que su filtro la saca a tiempo.

## Reality-check de la lista de deseos (por qué el programa es este y no "todo junto")

- **Scalping (<1h): DESCARTADO por aritmética.** Movimiento medio por vela 1-5m en BTC: 5-15
  bps; round-trip real: 11-20+ bps. Los costos superan el edge antes de empezar. Piso de
  investigación: **1h con fills maker**; el terreno principal: **4h-1d (swing)**.
- **Volumen/order-flow: LA adición más fuerte.** Éramos price-only. Binance regala en las
  klines el *taker buy volume* (historia completa) → agresión compradora/vendedora medible.
- **"Whales": la versión útil son los proxies gratis** — taker-flow, funding rate (historia
  completa gratis), open interest (histórico corto). Wallet-tracking on-chain: infra pesada,
  evidencia retail floja → FUERA por ahora (se revisa con la investigación de vanguardia).
- **Armónicos**: sin un solo backtest público riguroso conocido; ratios fijos o nada; prior
  MUY BAJO → al final de la cola, y solo si sobra presupuesto de trials.
- **Estructuras/SMC**: vuelven SOLO como hipótesis condicionadas (régimen + flujo), no como
  fe. El sweep desnudo ya fue falsado.

## Backlog de hipótesis (prioridad = prior × dato disponible × costo de test)

| ID | Familia (reglas a congelar antes de correr) | Prior | Datos | Estado |
|----|---------------------------------------------|-------|-------|--------|
| **H1** | **Tendencia diaria long/flat** (SMA/Donchian 1d + vol-targeting; la formalización del benchmark que ganó) | ALTO (evidencia académica + NUESTROS datos IS) | ya los tenemos | LISTA para congelar reglas. OJO: su IS ya fue "visto" como benchmark → cuenta ese peek; el juicio real es OOS+DSR |
| **H2** | **Sweep 2.0 condicionado**: solo régimen `up_*` + confirmación de flujo (volume climax / taker-sell exhaustion en el barrido) | MEDIO (semilla up_hi n=20 + Osler + flow) | requiere fetch de taker-volume (gratis) | requiere infra F0 |
| **H3** | **Funding/OI extremos** → mean-reversion o squeeze (la versión perp del stop-hunt: cascadas de liquidación) | MEDIO (pendiente evidencia agente) | funding gratis historia completa; OI corto | requiere infra F0 |
| **H4** | **Short side en régimen `dn_*`** (donchian/sweep espejo; investigación con `direction='both'`; live solo etapa perps) | MEDIO-BAJO | ya los tenemos | tras H1/H2 |
| **H5** | Estructuras BOS/CHoCH multi-TF (1h piso, maker-only, con filtro de régimen) | BAJO | ya + 1h fetch | cola |
| **H6** | Armónicos (Gartley/Bat, ratios FIJOS de la tabla del ebook, tolerancia única) | MUY BAJO | ya los tenemos | cola, opcional |

**Reglas del programa** (innegociables):
1. UNA familia por vez. Reglas y parámetros congelados en este archivo ANTES de tocar datos.
2. Presupuesto por familia: ≤5 parámetros, ≤2 tandas de variantes (todas anotadas).
3. Veredicto por `decide.py`; HARD fail = familia muerta y documentada (sección RIP).
4. El contador de trials es ACUMULATIVO entre familias → el `--deflated-sharpe N` crece
   siempre. Encontrar edge tarde exige que sea más fuerte. Así funciona la honestidad.
5. Nada pasa a LIVE sin GATE completo + dry-run. Investigar ≠ operar.
6. Cada familia muerta se escribe en el RIP con SU lección (qué aprendimos que NO es el edge).

## Infra F0 (habilita H1-H3; se construye ya, testeable con sintéticos)

- `fetch_data.py`: modo raw-Binance que además del OHLCV guarde `taker_buy_volume`
  (klines campo 9, gratis) — y `fetch_funding.py` para la historia completa de funding.
- `signals.py`: features de flujo (`volume_zscore`, `taker_buy_ratio`) causales + tests.
- `validate.py`: inferir el timeframe del CSV (1h/4h/1d) para anualizar bien las métricas.
- H1 como estrategia formal (`trend_daily_signals`): SMA/Donchian diario long/flat +
  vol-targeting opcional; control positivo/negativo propio; MISMOS gates.

## RIP — lo que ya sabemos que NO es el edge (se alimenta con cada kill)

- **Sweep-reversal long-only 4h price-only** (108 combos, 2019-2026, BTC+ETH): sin edge neto
  de costos. Lección: el patrón de entrada sin contexto de régimen/flujo no vale nada.
- **Donchian 4h long-only sin filtro de régimen**: estructura IS pero muere en chop 2024+ y
  no bate hold. Lección: la tendencia existe pero el timeframe/gestión importan más que la
  señal de ruptura.
- **Scalping <1h con capital chico**: muerto por aritmética de costos, sin necesidad de test.
