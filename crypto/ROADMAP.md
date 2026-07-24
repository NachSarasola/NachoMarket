# Roadmap de etapas — diseño detallado (GATED)

> ⚠️ **Esto es DISEÑO, no código.** Nada acá se implementa hasta que la etapa anterior pase
> sus gates con DATOS REALES. Construir estrategias sin evidencia es lo que fundió el bot
> anterior. Cada estrategia/par nuevo pasa TODOS los gates por separado y cuenta como trial
> para el Deflated Sharpe. La tesis y el ciclo de revisión están en `TESIS.md`; las reglas y
> el registro de trials en `REGLAS_CONGELADAS.md`.

Estado: **Etapa 1 construida y verificada** (falta correr GATE 1 con datos reales en el VPS).
Etapas 2 y 3 = diseño abajo, sin implementar.

---

## Etapa 2 — completar el setup direccional
**Se desbloquea con: GATE 2 aprobado (v1 en live estable) + capital ≥ $500.**

### D1 — Shorts en perps (Hyperliquid)
Hoy el sweep es simétrico pero solo operamos la mitad long (spot no shortea). Hyperliquid
(maker 0.015%, mínimo $10) permite el lado short fraccionando capital chico.

Trabajo concreto (cuando se desbloquee):
1. **Funding como costo en el backtester** — única pieza de infra que falta. Extender
   `crypto/smc/backtest.py::run_backtest` con un costo de holding `funding_bps_per_day`
   (o una serie de funding real por barra), aplicado por barra que la posición está abierta.
   Es infra, NO una estrategia nueva. Tests: posición larga N barras con funding X → PnL baja
   N×X; funding=0 → idéntico a hoy (no rompe nada).
2. `crypto/config-hyperliquid.json` — perps, funding como costo explícito, `trading_mode: futures`.
3. `SmcSweepPerps(can_short=True)` — reutiliza `SWEEP_PARAMS`; el detector ya emite `enter_short`.
   El stop DEBE estar dentro de la distancia de liquidación (sin apalancamiento agresivo).
4. **Re-validar el lado SHORT por separado** — todos los gates de nuevo (`validate.py
   --direction short`), OOS incluido. Se registra como trial. Si el short no valida, se queda
   solo el long: la simetría del patrón NO garantiza simetría del edge.

Riesgo nuevo: liquidación. Regla: el stop (≤4%) siempre antes que el precio de liquidación;
sin esto, no se opera el short.

### D2 — Segunda estrategia: `fvg_pullback` (familia distinta)
Para diversificar de verdad hace falta un edge DESCORRELACIONADO del sweep (mean-reversion).
El FVG-pullback es continuación en tendencia → familia distinta. `fair_value_gap()` ya existe
y está testeado en `signals.py`.

Trabajo concreto:
1. `fvg_pullback_signals(df, ...)`: FVG (3 velas) EN DIRECCIÓN de la tendencia diaria (filtro
   `close > EMA200_diaria` o Donchian50 alcista) + entrada en el retest del gap (precio vuelve
   al rango del FVG) + stop bajo el origen del impulso + target por R. Presupuesto: 3-5 params.
2. Control POSITIVO propio en `synthetic.py`: serie con FVG-en-tendencia que se rellenan →
   la estrategia DEBE ganar; en random-walk DEBE perder. (Igual que el sweep.)
3. Tests de causalidad (sin look-ahead) + fills.
4. Validación con los MISMOS gates (`validate.py --strategy fvg_pullback`).
5. **Condición de inclusión**: correlación < 0.7 con el sweep (`portfolio.py` ya lo mide). Si
   corr ≥ 0.7, NO aporta diversificación → no se incluye (criterio de TESIS).

### D3 — Tercer par (probable SOL/USDT)
Correr el pipeline COMPLETO en SOL con los criterios objetivos de `TESIS.md` (top-10 volumen,
spread < 5 bps, ≥3 años de datos, pasa TODOS los gates en SOL por separado). `portfolio.py`
decide si suma o solo agrega fees. Tope duro: 4 pares.

---

## Etapa 3 — cartera de dos patas (direccional + carry)
**Se desbloquea con: Etapa 2 estable + capital ≥ $5k** (por debajo, los fees/mínimos hacen
inviable el carry — no es opinión, es aritmética).

### E1 — Carry delta-neutral (funding-rate arbitrage)
El único edge "grande" accesible sin HFT. Comprar spot + shortear el perp del mismo activo →
cobrar el funding sin exposición direccional. Descorrelaciona de la pata direccional → curva
de equity más suave (es el objetivo real de la Etapa 3, no más retorno bruto).

Diseño (a detallar recién con Etapa 2 estable):
1. **Monitor de funding multi-venue** — leer funding rate actual/histórico (Hyperliquid,
   Binance, OKX) vía ccxt; entrar solo cuando el funding anualizado supere un umbral neto de
   fees de ambas patas.
2. **Gestor del par spot-long / perp-short** — abrir/cerrar las dos patas con límites de
   *basis* (la diferencia spot-perp); rebalancear si el basis se abre; cerrar si el funding
   se da vuelta negativo de forma sostenida.
3. **Backtester de carry** — reutiliza el modelo de funding de D1; el "PnL" es funding cobrado
   − fees − costo de basis. Validar con funding histórico real.
4. **Pruebas de estrés**: depeg del spot, gap de liquidación en la pata short, caída de un
   exchange (riesgo de contraparte). Sin pasar estos stress-tests, no va a live.

Riesgos que lo pueden matar: funding negativo sostenido, basis risk, liquidación de la pata
short, riesgo de exchange. Ninguno es despreciable → por eso Etapa 3 y no antes.

---

## Nunca (guardrails permanentes)
- Sin apalancamiento en la pata direccional (perps solo para shortear/carry con stop < liquidación).
- Máximo 4 pares (en crypto todo correlaciona; más pares = fees + falsa confianza).
- Ninguna estrategia/par nuevo sin pasar TODOS los gates por separado y anotarse como trial.
- Nunca tocar el otro bot del VPS (`~/nachomarket`). El bot de crypto vive aislado.
- Si un backtest muestra 100% winrate o "demasiado bueno" → es un bug o overfitting, no un edge.

## Por qué NO está construido esto todavía
Implementar la Etapa 2/3 ahora sería sobre-ingeniería especulativa: si v1 no pasa el GATE 1
con datos reales, nada de esto se usa. El orden correcto es validar → operar chico → escalar
con evidencia. Este documento existe para que, cuando un gate se abra, la ejecución sea
mecánica y sin improvisar.
