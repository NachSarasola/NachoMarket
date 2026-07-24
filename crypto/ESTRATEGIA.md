# La estrategia en lenguaje llano

Este documento explica QUÉ hace el bot y POR QUÉ, conectando cada regla con tu ebook
(Smart Money vol 1/2) y con la evidencia académica. Es para entender el sistema, no para
convencerte de que funciona — eso lo decide la validación con datos reales.

## La idea en una frase

El mercado agrupa stop-loss justo debajo de mínimos previos (y encima de máximos). Cuando el
precio **barre** esa zona con una mecha pero **cierra de vuelta adentro**, esos stops ya se
ejecutaron y el precio suele revertir. Compramos esa reversión, con stop debajo de la mecha.

Eso es la **turtle soup / barrido de liquidez** de tu vol 1 (pp. 41-47) y el **stop hunt +
reversión** del vol 2 (pp. 28-34). Y no es "manipulación de smart money": es simplemente que
miles de traders ponen el stop en el mismo lugar obvio, y las órdenes grandes buscan esa
liquidez concentrada. Eso está **documentado con órdenes reales** (Osler, NY Fed Staff Report
150) — es el único pedazo de SMC con fundamento medible, y es lo único que mecanizamos.

## Qué dispara un trade (long, spot) — traducción de `smc_sweep_signals`

Sobre velas de 4 horas ya cerradas de BTC/USDT o ETH/USDT:

| Paso | Regla mecánica (código) | Concepto del ebook |
|------|-------------------------|--------------------|
| 1 | `SSL = mínimo de las 20 velas previas` | Sell-side liquidity / old low / EQL (vol 1) |
| 2 | `low actual < SSL` (la mecha barre) **y** `close > SSL` (el cuerpo cierra adentro) | Turtle soup: falso breakout; "mecha = liquidity grab, cuerpo = ruptura" |
| 3 | `close < medio del rango` (zona discount) | PDA array: comprar en discount (vol 1 p.65) |
| 4 | Entrada al **open de la vela siguiente** | Sin look-ahead (no operamos con info del futuro) |
| 5 | Stop = `low − 0.5×ATR`; si el riesgo > 4%, se DESCARTA | SL 3-4% en crypto (vol 1); stop obligatorio |
| 6 | Target = medio del rango; time-stop a 12 velas | "Golpes sencillos, no home runs" + regla de las 2h (vol 2) |

El short es el espejo (barrido de máximos), pero **v1 es spot long-only** (en spot no se
puede shortear; los shorts esperan a que valides y pases a perps).

## Por qué NO metimos más cosas de SMC

Tu ebook tiene mucho más (order blocks, armónicos, FVG, MMBM/MMSM, EMAs, conteo de niveles).
Los dejamos afuera **a propósito**, porque tu propia restricción fue "sin overfitting":

- **Order blocks / armónicos**: cada uno agrega decisiones subjetivas (¿qué zona?, ¿qué
  ratio?, ¿mitigación por cuerpo o mecha?) = muchos parámetros libres = curve-fitting casi
  garantizado. Con 5-6 parámetros ya estás en el límite sano.
- **FVG**: solo tiene edge como *pullback en tendencia*, no como filtro del sweep. Está
  implementado y testeado (`fair_value_gap`) pero NO cableado — queda para un v2 SOLO si el
  núcleo valida.
- **MMBM/MMSM, kill zones interpretativas, "narrativa" del día**: son discrecionales. Un bot
  no puede ejecutar "depende del contexto".

Regla de oro (que también está en tu vol 2, p.43): *"mantenlo simple, no combines mil
estrategias"*. Un setup, pocos parámetros, bien validado.

## El riesgo (la lección de la fundida de Polymarket)

- **Stop obligatorio** en toda posición, y **nunca se mueve en contra**.
- **1% del equity por trade**, calculado por la distancia al stop (no un tamaño fijo).
- Si el stop natural queda a más de 4%, no se opera (no se acerca el stop a la fuerza).
- Spot, **sin apalancamiento**. Máximo 2 posiciones.
- En freqtrade: backstop estático (-5%) + protections (StoplossGuard, MaxDrawdown, Cooldown).

El bot anterior se fundió porque una orden gigante sin tope en dólares se llenó y no había red.
Acá el tamaño está atado al stop y el stop siempre existe.

## Cómo leer la validación SIN autoengañarte

Corré los dos demos y comparalos — te enseñan a distinguir edge de ruido:

```bash
python crypto/scripts/validate.py --synthetic          # SIN edge -> todo negativo, DSR ~0
python crypto/scripts/validate.py --synthetic-positive  # CON edge -> verde en todos los gates
```

Qué mirar (en este orden), sobre **datos reales**:

1. **Trades ≥ 100-200** cruzando bull/bear/rango. Menos que eso no es validable.
2. **Walk-forward**: los folds deben rendir parecido (meseta). Si un fold salva todo y el
   resto pierde, es overfit.
3. **OOS 2024+ una sola vez**: si el Sharpe OOS < 50% del in-sample → a la basura.
4. **Deflated Sharpe**: pasale el nº de variantes que probaste (`--deflated-sharpe N`). DSR <
   0.95 = tu Sharpe no se distingue del mejor de N intentos al azar.
5. **Batir buy&hold y la media móvil** neto de costos. Si no, no aporta nada.
6. **Monte Carlo de ruina**: mirá P(ruina) y el peor maxDD del percentil 95.

Señal de alarma #1 (la que te fundió): **100% winrate / profit factor gigante**. Eso NO es
bueno, es la firma de un riesgo de cola oculto o de un backtest que miente. Por eso el
backtester siempre reporta maxDD y rachas, nunca solo el winrate.

## La expectativa honesta

Con $150-500 y un setup direccional simple bien validado, lo realista es Sharpe 0.5-1.5,
años buenos de 10-40% y años planos o negativos, con drawdowns de 15-35%. **El objetivo de
esta etapa no es recuperar lo perdido: es tener UN setup validado y no volver a fundirte.**
Si la validación honesta dice que no hay edge, ese "no" también es un resultado valioso —
es la fundida que no ocurre.
