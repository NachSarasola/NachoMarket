# Tesis de trading — NachoMarket crypto

Documento vivo. Se revisa con RESULTADOS (journal + slices por régimen), no con opiniones.
Cada cambio de reglas se anota en `REGLAS_CONGELADAS.md` y cuenta como trial (Deflated Sharpe).

> **ESTADO (2026-07-24, actualizado): CHECKPOINT RESUELTO — salidas 2+3 elegidas.** Cinco
> familias falsadas con datos reales (sweep, donchian, ma_timing, flow, funding) — todas con
> la misma firma: estructura IS 2019-2023, colapso OOS 2024+. No hay edge direccional
> validable en majors/swing/datos-gratis en el régimen actual. El usuario eligió: acumular
> capital hacia carry ($5k+) Y solo mecanismos nuevos. La investigación de bots VERIFICADOS
> (on-chain; RECURSOS §8) confirmó el meta-hallazgo — solo ~16-20% de 2.396 vaults de
> Hyperliquid es rentable y NINGUNO por señal direccional ("risk control, not win rate") —
> y re-rankeó la cola: H3b cascadas DEGRADADA a BAJO (cero evidencia pública); candidatas
> nuevas **H7 short-unlocks (MEDIO-ALTO)** y **H8 listing-fade (MEDIO)**. Con el mandato
> del usuario "invierte mi tesis: no importa el winrate sino el PnL" (2026-07-24), H7/H8
> quedaron CONGELADAS e implementadas (event-study propio: `crypto/smc/events.py` +
> `event_validate.py` + fetchers + 23 tests; el winrate se reporta pero no participa de
> ningún gate). Pendiente: correr `vps_run_eventos.sh` en el VPS y registrar el veredicto.
> Capital intacto: $0 perdidos en ~125 trials. La máquina de validación es el activo.

## La tesis en 5 puntos

1. **Los stops se agrupan en niveles obvios y su barrido revierte.** Crypto es retail-heavy y
   opera 24/7: miles de traders ponen stops bajo el mismo mínimo previo / número redondo. Las
   órdenes grandes buscan esa liquidez concentrada; el barrido + reversión (turtle soup) es un
   patrón con base microestructural documentada (Osler, NY Fed). Esta conducta no desaparece
   porque es estructural al comportamiento minorista → el edge, si valida, es *persistente*.
2. **BTC/ETH tienen momentum de series temporales documentado.** Por eso el control Donchian/BMS
   no es un adorno: es una tesis alternativa con evidencia académica. Si el sweep no lo bate
   out-of-sample, cambiamos de caballo sin ego — la tesis es "capturar un fenómeno real", no
   "tener razón sobre SMC".
3. **El edge solo existe neto de costos.** Round-trip retail: 11–20+ bps. En <1h los costos
   comen cualquier señal; en 4h/1d los targets (1–5%) los toleran. Por eso: timeframe 4h con
   filtro diario, órdenes maker cuando se pueda, y solo pares de spread mínimo.
4. **La ventaja del chico es estructural, no informativa.** No movemos el mercado, no tenemos
   presión de AUM, podemos NO operar durante semanas y podemos apagar todo en un click. La
   preservación de capital ES la estrategia: sobrevivir hasta que el interés compuesto y el
   aprendizaje acumulen. Riesgo 1%/trade, stop siempre, drawdown-kill.
5. **Proceso > predicción.** Reglas congeladas antes de mirar datos; journal por trade con
   etiqueta de régimen; revisión diaria operativa y semanal analítica; re-tuning como máximo
   trimestral y contado como trial. El enemigo n°1 no es el mercado: es el auto-engaño
   (overfitting, mover stops, agrandar size tras perder).

## Decisiones concretas

### ¿Qué estrategia? (la "mejor opción")
**v1: `SmcSweep` (sweep-reversal 4h) con `DonchianControl` como benchmark interno.** Es la única
candidata con fundamento medible + definición 100% mecánica + costos tolerables. Ya está
implementada, testeada (35+ tests) y con pipeline de validación anti-overfitting completo.
No se agrega NADA hasta que esta valide o muera con datos reales (gates de REGLAS_CONGELADAS).

### ¿Qué pool de cryptos?
**v1: BTC/USDT y ETH/USDT. Punto.** Spread 1–5 bps, liquidez máxima, 6+ años de historia.
Criterios OBJETIVOS para agregar un par (etapa 2, probablemente SOL/USDT):
- top-10 por volumen spot real y spread medio < 5 bps,
- ≥ 3 años de velas 4h para validar multi-régimen,
- la estrategia pasa TODOS los gates en ese par POR SEPARADO (no "valida en BTC, opero en X").
Tope duro: 4 pares. En crypto la correlación entre pares es altísima: más pares no diversifica,
solo multiplica fees y falsa confianza.

### ¿Qué plataforma?
- **Datos + backtest**: Binance (mejor histórico gratuito, el que usa freqtrade por defecto).
- **Spot live v1**: **Binance spot** (fees 0.10%, 0.075% con BNB; liquidez #1; mínimo 5 USDT).
  Alternativas si hay trabas de cuenta/KYC: OKX (0.08/0.10) o Kraken (más caro, muy regulado).
- **Perps (etapa 2, para shorts)**: **Hyperliquid** (maker 0.015%, mínimo $10 — el único donde
  capital chico puede fraccionar), modelando el funding (~10%/año en longs) como costo explícito.
  Bybit perps NO (mínimo 0.001 BTC ≈ inviable con cuenta chica).
- **Motor**: freqtrade (backtest + dry-run + live + Telegram). Nada de motor propio en producción.

### ¿Cómo? (pipeline de una sola dirección)
```
reglas congeladas → backtest 2019-2023 (costos pesimistas) → walk-forward + meseta de params
→ OOS 2024+ UNA pasada (+ DSR + Monte Carlo) → batir buy&hold y MA diaria → dry-run 4-8 sem
→ live con $10-20/posición → escalar solo con evidencia
```
Si un gate falla, se vuelve atrás o se descarta. Nunca se saltea.

## Pensar en grande, sin sobre-ingeniería: el roadmap por etapas

La ambición está en el ROADMAP, no en apilar indicadores. Cada etapa se desbloquea SOLO si la
anterior pasó sus gates con datos reales:

- **Etapa 1 (ahora, capital $0–500): un edge validado.** SmcSweep vs DonchianControl en BTC/ETH
  spot. Producto de la etapa: el proceso funcionando + la primera estrategia validada (o
  descartada honestamente). El PnL es secundario; el journal es el activo.
- **Etapa 2 ($500–2k): completar el setup.** Shorts en Hyperliquid perps (el sweep es simétrico;
  hoy solo operamos la mitad long por estar en spot). Evaluar SOL/USDT por los criterios de
  arriba. Segunda estrategia SOLO si es de familia distinta y no correlaciona (candidata ya
  implementada: `fvg_pullback` tendencial — está testeada pero desactivada a propósito).
- **Etapa 3 ($5k+): cartera.** Direccional (sweep/donchian) + **carry delta-neutral**
  (funding-rate arbitrage): el único edge "grande" accesible sin HFT, pero matemáticamente
  inviable por debajo de ~$5k por fees/mínimos. Dos patas descorrelacionadas = curva más suave.
- **Siempre**: máx 3–5 parámetros por estrategia, cada variante anotada, DSR como juez.

## Regímenes: identificar, no predecir

Cada señal/trade queda etiquetada con el régimen vigente (instrumentación observacional, CERO
parámetros nuevos de trading):

- **Tendencia**: close vs media de 30 días (180 velas de 4h) → `up` / `dn`.
- **Volatilidad**: ATR% vs su mediana anual → `hi` / `lo`.
- → 4 regímenes: `up_hi, up_lo, dn_hi, dn_lo`, reportados por `validate.py` (slice de PnL,
  winrate y R por régimen) y exportables a CSV (`--trades-out`) como journal.

Regla de decisión (anti-overfitting): apagar/encender la estrategia en un régimen requiere
**≥30 trades en ese régimen** y se anota como variante en REGLAS_CONGELADAS. Nada de "esta
semana el mercado está raro".

## El ciclo de revisión (día a día, como pediste)

**Diario (operativo, 5 min):**
1. ¿Fills y fees reales = esperados? (si divergen del backtest, algo miente → parar).
2. ¿Riesgo vivo ≤ 1%/trade y ≤ 2 posiciones? ¿Stop presente en TODA posición?
3. ¿Drawdown vs límites (MaxDrawdown protection)? ¿Alguna protection saltó? ¿Por qué?
4. Journal: anotar cualquier anomalía (slippage raro, vela extrema, exchange caído).

**Semanal (analítico, 30 min) — automatizado en `crypto/scripts/weekly_review.py`:**
```bash
python crypto/scripts/weekly_review.py --freqtrade-csv <export_de_trades.csv> --baseline report.json
```
El script hace mecánicamente lo que antes dependía de disciplina:
1. Slice por régimen y por exit_reason: ¿dónde gana, dónde pierde?
2. Drawdown realizado vs el p95 del Monte Carlo del backtest (cono).
3. Racha de pérdidas actual/máxima vs la esperada; winrate vivo vs backtest (edge decayendo).
4. Veredicto: `TODO_NORMAL` / `ALERTA(...)` / `KILL(...)` (los criterios de muerte de abajo).
NO toca parámetros. Un `KILL` = apagar y post-mortem antes de reencender.

**Trimestral (estructural):**
1. Re-correr validación completa con los datos nuevos (walk-forward extendido).
2. Decidir cambios de reglas/parámetros — máximo una tanda, contada como trial en el DSR.
3. Revisar la tesis: ¿el fenómeno sigue existiendo? (frecuencia de sweeps, profundidad, fills).

## Qué invalida la tesis (criterios de muerte, escritos hoy)

- El sweep no bate al Donchian ni a buy&hold/MA OOS neto de costos → se descarta el sweep.
- Ninguna de las dos pasa gates → NO se opera direccional; se acumula capital y aprendizaje
  (paper) hasta la etapa 3 (carry) o nuevas hipótesis. No operar también es una posición.
- En live: drawdown > 2× el p95 del Monte Carlo, o divergencia sistemática fills-vs-backtest
  → kill switch y post-mortem antes de reencender.
