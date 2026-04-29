# PROMPT: Estrategia de Rewards Farming para NachoMarket

## Contexto para Claude Code

NachoMarket ya está construido como bot de market making para Polymarket. Este prompt se enfoca exclusivamente en **maximizar los ingresos por Liquidity Rewards y Holding Rewards** — las dos fuentes de ingreso más predecibles y de menor riesgo en Polymarket. Todo lo que sigue está basado en documentación oficial, bots open-source reales, y experiencias publicadas por operadores reales.

**ALERTA CRÍTICA — CLOB V2 lanzado el 28 de abril de 2026:**
Polymarket acaba de migrar a CLOB v2. Los cambios que afectan directamente al bot:
- **`py-clob-client` ya no funciona.** Hay que migrar a `py-clob-client-v2`
- **pUSD reemplaza USDC.e** como token de colateral. Es un ERC-20 en Polygon, respaldado 1:1 por USDC
- **La estructura de órdenes cambió:** se eliminaron `nonce`, `feeRateBps`, y `taker` del struct. Se agregaron `timestamp` (ms), `metadata`, y `builder`
- **Fees se calculan al momento del match**, no al colocar la orden
- **EIP-712 domain version** cambió de "1" a "2" (ClobAuth sigue en "1")
- **Todos los orderbooks fueron borrados** durante la migración. Las órdenes v1 ya no existen
- SDK nuevo: `@polymarket/clob-client-v2` o `py-clob-client-v2`
- Constructor cambió: de argumentos posicionales a options object; `chainId` → `chain`

**Antes de tocar cualquier línea de código de estrategia, migrar el bot a CLOB v2.** Sin esto, el bot directamente no puede operar.

---

## Cómo funciona el sistema de rewards de Polymarket (documentación oficial, abril 2026)

### 1. Liquidity Rewards (la fuente principal de ingresos para un bot de $400)

Polymarket distribuye **más de $5 millones mensuales** en incentivos de liquidez. El programa está inspirado en el de dYdX. Rewards se pagan diariamente a medianoche UTC directamente a la dirección del maker.

**La fórmula de scoring es cuadrática** y tiene 7 pasos:

**Paso 1 — Order Scoring Function:**
```
S(v,s) = ((v - s) / v)² × b
```
Donde `v` = max spread permitido (en centavos), `s` = distancia al midpoint ajustado, `b` = multiplicador in-game.

Esto es CUADRÁTICO: una orden a 1 centavo del mid recibe **mucho** más score que una a 2 centavos. La diferencia no es lineal sino exponencial.

Ejemplo con max_spread = 3¢:
- A 1¢ del mid: ((3-1)/3)² = 0.44
- A 2¢ del mid: ((3-2)/3)² = 0.11
- A 3¢ del mid: ((3-3)/3)² = 0.00

La orden a 1¢ recibe **4x más** score que la de 2¢. Esto significa que la posición óptima es la más cercana posible al midpoint.

**Paso 2-3 — Scores por lado del mercado:**
Se calcula un score para el lado "one" (bids en YES + asks en NO) y otro para el lado "two" (asks en YES + bids en NO). Cada score suma el scoring function × size de cada orden.

**Paso 4 — Minimum Score (el paso más importante):**
- Si el midpoint está entre 0.10 y 0.90: `Qmin = max(min(Qone, Qtwo), max(Qone/c, Qtwo/c))` donde c = 3.0
- Si el midpoint está por debajo de 0.10 o encima de 0.90: `Qmin = min(Qone, Qtwo)` — OBLIGATORIO tener ambos lados

Esto significa: **tener órdenes en AMBOS lados da ~3x más rewards** que un solo lado (porque min(Qone, Qtwo) > Qone/3 cuando ambos lados son similares). Y en mercados extremos (<0.10 o >0.90), si no tenés ambos lados, tu score es CERO.

**Paso 5-7 — Normalización:**
Tu score se normaliza contra todos los otros market makers en ese mercado, se suma a lo largo de las 10,080 muestras del epoch (una muestra por minuto durante 7 días), y se normaliza otra vez para obtener tu porcentaje del reward pool.

**Parámetros configurables por mercado** (consultables vía API):
- `min_incentive_size`: tamaño mínimo de orden para calificar
- `max_incentive_spread`: distancia máxima del midpoint para calificar
- Monto total de rewards por mercado por día

**El mínimo payout es $1.** Si tu share del reward es <$1, no te pagan nada.

### 2. Holding Rewards (ingreso pasivo complementario)

Polymarket paga **4% APY** sobre posiciones elegibles en mercados de largo plazo (elecciones 2028, geopolítica). El valor de tu posición se samplea aleatoriamente cada hora, y el reward se distribuye diariamente.

Con $400 de capital 100% invertido en posiciones elegibles: 400 × 4% / 365 = **$0.044/día** — casi nada. Pero si las posiciones que mantenemos como inventario de market making están en mercados elegibles, es ingreso gratuito adicional.

### 3. Maker Rebates Program (tercer canal de ingresos)

En mercados con taker fees habilitados (crypto, deportes), los makers reciben un **rebate del 20-25%** de los fees pagados por takers. Esto es automático — si hacemos market making en esos mercados, cobramos rebates cuando takers ejecutan contra nuestras órdenes.

---

## Lecciones de bots reales de rewards farming

### Bot 1: RuneDn/polymarket-liquidity-bot (GitHub)
**Concepto:** Farm rewards SIN que te ejecuten las órdenes. Coloca órdenes que están dentro del max_spread pero nunca son las primeras en la cola — siempre hay otra orden mejor delante. Así cobra rewards sin asumir riesgo de inventario.

**Cómo funciona:**
- Monitorea el orderbook en tiempo real
- Coloca órdenes DETRÁS de la mejor orden existente pero dentro del max_spread
- Si su orden se acerca a ser la primera, la cancela y recoloca más atrás
- Cada mercado corre en un CPU core separado para minimizar latencia

**Problema:** Con la fórmula cuadrática, las órdenes más alejadas del mid reciben mucho menos rewards. Este approach es conservador pero ineficiente en rewards por dólar.

**Lección para NachoMarket:** No copiar este approach al 100%. Con $400 de capital, necesitamos maximizar rewards por dólar, lo que requiere estar CERCA del mid. La pérdida ocasional por fill es el costo de estar en la posición más rentable.

### Bot 2: warproxxx/poly-maker (GitHub, el más exitoso documentado)
**Concepto:** Market making completo con gestión de inventario y position merging.

**Características clave:**
- Configuración de parámetros vía Google Sheets (fácil de ajustar remotamente)
- `poly_merger`: módulo que combina posiciones YES+NO de vuelta a USDC para reciclar capital
- Descubrimiento automático de mercados con rewards activos
- Bands de precios con ajuste dinámico
- El creador fue entrevistado por el newsletter oficial de Polymarket

**Advertencia del propio autor:** "In today's market, this bot is not profitable and will lose money" — referido al market making puro sin rewards. Los rewards son los que hacen viable la operación.

**Lección para NachoMarket:** El position merging es CRÍTICO para capital chico. Si tenemos 50 YES shares y 50 NO shares del mismo mercado, eso son $50 de capital muerto. Merging los convierte de vuelta a $50 de USDC libre para seguir operando.

### Bot 3: terrytrl100/polymarket-automated-mm (GitHub)
**Concepto:** Selección automática de mercados basada en reward amount.

**Comando clave:**
```bash
python update_selected_markets.py --min-reward 150 --max-markets 15 --replace
```

**Lección para NachoMarket:** Filtrar mercados por reward ≥ cierto umbral. Con $400 no podemos competir en mercados donde los reward pools son $25/día (los big players nos aplastan). Debemos buscar mercados con rewards suficientes pero con menos competencia.

### Experiencia real documentada: PolyMaster (Medium, enero 2026)
**Hallazgos empíricos:**
- Con ~$10,000 de capital, los primeros LPs hacían $200-300/día en rewards en los primeros meses. Eso ya no existe — la competencia comprimió los retornos.
- Hoy los rewards son un "bonus" sobre un trading edge real, no un money printer standalone.
- Approach estable para LPs conservadores: **~10% APY** combinando holding rewards (4%) + liquidity rewards (~6%) en mercados de largo plazo como elecciones 2028.
- Mercados calmos y de largo plazo son los mejores para farming: se mueven lento, revierten mucho, y los fills son poco frecuentes.
- El "Adjusted Midpoint" filtra órdenes dust — no podés manipular el mid con órdenes chicas.
- El sampling se hace por minuto. Estar en el book 24/7 maximiza muestras pero aumenta probabilidad de fill.

**Lección clave:** "Unless you have strong, independent alpha, it is healthier to treat liquidity rewards as a BONUS, not the main profit engine."

---

## Pools de rewards por mercado (abril 2026, documentación oficial)

Los mejores pools para un bot chico que busca reward farming:

**Tier 1 — Pools grandes con buena competencia:**
- Champions League: $24,000/partido ($6,750 pre + $17,250 live)
- Premier League: $10,000/partido
- NBA: $7,700/partido
- CS2 A-Tier: $5,500/partido
- League of Legends A-Tier: $5,500/partido
- IPL Cricket: $4,500/partido
- UFC Main Card: $4,250/partido

**Tier 2 — Pools medianos con menos competencia (ideal para $400):**
- La Liga: $3,300/partido
- Serie A: $3,300/partido
- Dota 2 A-Tier: $3,500/partido
- Valorant A-Tier: $3,500/partido
- Bundesliga: $3,000/partido
- Copa Libertadores: $2,650/partido
- Ligue 1: $2,100/partido
- MLS: $1,650/partido
- MLB: $1,650/partido

**Tier 3 — Pools chicos pero con poca competencia:**
- Liga MX: $1,650/partido
- NHL: $1,500/partido
- Tennis ATP: $1,450/partido
- Esports C-Tier: $500/partido
- Ligas menores de fútbol: $75-$550/partido

**Estrategia óptima para $400:** Apuntar a Tier 2 y Tier 3. En los Tier 1 hay market makers profesionales con $100K+ que van a dominar el reward pool. En Tier 2-3, la competencia es menor y nuestro share del pool va a ser proporcionalmente mayor.

---

## Plan de implementación: Rewards Farming Optimizado

### Paso 1: Migración a CLOB v2 (URGENTE — sin esto nada funciona)

```
Migrá el bot a py-clob-client-v2:
1. Desinstalar py-clob-client, instalar py-clob-client-v2
2. Actualizar el constructor del ClobClient: de args posicionales a options object
3. Cambiar chainId por chain
4. Eliminar feeRateBps, nonce y taker de la creación de órdenes
5. Actualizar el colateral de USDC.e a pUSD
6. Actualizar EIP-712 domain version de "1" a "2"
7. Testear conexión con test_connection()
8. Verificar que place_limit_order funcione con el nuevo SDK
```

### Paso 2: Descubrimiento inteligente de mercados para farming

```
Crear src/rewards/market_scanner.py:
1. Consultar GET /rewards/markets (o endpoint equivalente en v2) para obtener
   todos los mercados con rewards activos
2. Para cada mercado, obtener: reward_amount, max_incentive_spread,
   min_incentive_size, volume_24h, number_of_makers
3. Calcular "reward_density": reward_amount / number_of_makers
   Esto estima cuánto reward te correspondería si dividieras el pool equitativamente
4. Filtrar: reward_density > $2/día (con $400 no vale la pena menos)
5. Priorizar mercados donde:
   - max_incentive_spread es amplio (≥3¢): más margen para colocar órdenes seguras
   - min_incentive_size es bajo: no necesitamos órdenes gigantes
   - El mercado tiene resolución >7 días: menos riesgo de adverse selection
   - La categoría diversifica nuestro portfolio (Hallazgo 6 del paper Akey)
6. Seleccionar top 5-8 mercados
7. Re-escanear cada 4 horas para detectar nuevos mercados o cambios en rewards
```

### Paso 3: Quoting strategy optimizada para rewards (el corazón del farming)

```
Crear src/rewards/reward_quoter.py:

La clave es la fórmula cuadrática S(v,s) = ((v-s)/v)²

Con max_spread de 3¢, el scoring es:
- 0¢ del mid: score = 1.00 (máximo, pero altísimo riesgo de fill)
- 0.5¢ del mid: score = 0.69
- 1¢ del mid: score = 0.44
- 1.5¢ del mid: score = 0.25
- 2¢ del mid: score = 0.11
- 2.5¢ del mid: score = 0.03
- 3¢ del mid: score = 0.00 (fuera del rango)

Estrategia óptima para $400 (equilibrio entre rewards y riesgo de fill):
- Colocar la orden principal a 1¢ del midpoint → score = 0.44
  (captura casi la mitad del score máximo pero con mucho menos riesgo que estar en 0)
- Colocar una orden secundaria más chica a 0.5¢ del mid → score = 0.69
  (captura más reward pero acepta más riesgo de fill)
- SIEMPRE colocar en AMBOS lados (YES y NO) → ~3x más rewards que un solo lado

Lógica de rebalanceo:
- Cada 30-60 segundos, verificar que las órdenes siguen dentro del max_spread
- Si el midpoint se movió, cancelar y recolocar
- Si una orden se ejecutó (fill), inmediatamente:
  a. Verificar inventario
  b. Si tenemos YES Y NO shares suficientes: ejecutar merge para recuperar capital
  c. Recolocar la orden en el lado que se ejecutó
```

### Paso 4: Position merging automático (crítico para capital chico)

```
Crear src/rewards/merger.py:

Cuando el bot acumula shares de ambos lados del mismo mercado,
esas shares se pueden combinar de vuelta a pUSD:
- Si tenemos 30 YES shares + 30 NO shares = se convierten en 30 pUSD
- Esto libera capital para seguir operando

Lógica:
1. Cada 5 minutos, revisar inventario por mercado
2. Si min(yes_shares, no_shares) > min_merge_size (ej: 15 shares):
   ejecutar merge vía el contrato del Exchange
3. Loguear cada merge: timestamp, market_id, shares_merged, capital_recovered
4. Alertar por Telegram: "♻️ Merged X shares → $Y pUSD recuperados"

Con $400 de capital repartido en 5-8 mercados, cada mercado tiene ~$50-80.
Si no mergeamos, el capital se congela rápidamente en posiciones opuestas.
```

### Paso 5: Tres canales de ingreso simultáneos

```
El bot debe maximizar tres fuentes de ingreso simultáneamente:

1. LIQUIDITY REWARDS (principal):
   - Órdenes resting en ambos lados, cerca del midpoint
   - Cobro diario a medianoche UTC
   - Objetivo: capturar >1% del reward pool en cada mercado

2. SPREAD CAPTURE (secundario):
   - Cuando una orden se ejecuta, el fill es a nuestro precio limit
   - Si tenemos bid a 0.48 y ask a 0.52, y ambos se ejecutan:
     spread capture = 0.04 × size
   - Complementado por position merging para reciclar capital

3. HOLDING REWARDS (bonus pasivo):
   - 4% APY sobre posiciones en mercados elegibles de largo plazo
   - Si mantenemos inventario en mercados de elecciones 2028, ganamos 4% extra
   - Es automático, no requiere acción

Prioridad: Rewards > Spread > Holding
```

### Paso 6: Monitoreo de competencia

```
Crear src/rewards/competition_monitor.py:

Para cada mercado donde operamos:
1. Observar el orderbook y contar cuántos makers hay dentro del max_spread
2. Estimar el size total de órdenes competidoras
3. Calcular nuestro share estimado: our_score / total_scores_estimated
4. Si nuestro share estimado es <0.5% (demasiada competencia), considerar
   mover capital a otro mercado con mejor ratio
5. Si nuestro share estimado es >5% (buena posición), considerar
   aumentar size en ese mercado

Reportar en cada self-review:
- Top 3 mercados por reward earned
- Bottom 3 mercados (candidatos a reemplazar)
- Reward earned total vs capital deployed = "reward yield"
```

### Paso 7: Self-review orientado a rewards

```
Agregar al self-review existente:

Métricas específicas de rewards farming:
- Rewards earned por día (en pUSD)
- Rewards earned por mercado
- Uptime de órdenes: % del tiempo que tuvimos órdenes resting calificadas
- Fill rate: % de órdenes que se ejecutaron (más bajo = menos riesgo pero OK)
- Merge rate: cuánto capital recuperamos vía merging
- Reward yield: (daily_rewards / capital_deployed) × 365 = APY de rewards
- Net PnL: rewards + spread_capture + holding_rewards - losses_from_fills

Objetivos:
- Uptime de órdenes: >95% (estar en el book lo más posible)
- Reward yield: >15% APY (rewards / capital)
- Fill rate: <10% de las órdenes colocadas
- Net PnL: positivo cada semana
```

---

## Selección de mercados — recomendación concreta para arrancar

Con $400, arrancar con 4-5 mercados deportivos de Tier 2-3 que tienen:
- Reward pool de $1,000-$3,500/partido
- Menos market makers profesionales que NBA/Premier League
- Resolución relativamente rápida (partidos diarios = reinversión rápida)

Candidatos iniciales:
1. **MLB** ($1,650/partido, ~15 partidos/día = muchas oportunidades)
2. **MLS** ($1,650/partido, menos competencia que ligas europeas)
3. **Tennis ATP** ($1,450/partido, muchos partidos simultáneos)
4. **Liga MX** ($1,650/partido, menos bots que ligas tier 1)
5. **Esports — Valorant/Dota 2** ($3,500/partido, comunidad más chica de MMs)

Diversificar: al menos 2 deportes distintos + 1 esport.

---

## Estimación realista de retorno con $400

Escenario conservador (10% del reward pool en mercados con poca competencia):
- Capital: $400 repartido en 5 mercados (~$80/mercado)
- Rewards diarios estimados: $2-5/día
- Spread capture: $0-2/día
- Holding rewards: $0.04/día
- Pérdidas por fills adversos: -$0.5-1/día
- **Net estimado: $1.5-6/día = $45-180/mes**

Escenario pesimista (mucha competencia, fills adversos):
- Net estimado: $0.5-1.5/día = $15-45/mes

Escenario optimista (mercados con poca competencia encontrados):
- Net estimado: $5-10/día = $150-300/mes

**La varianza es alta.** Los primeros 2-4 semanas son de calibración.

---

## Nota final

Este prompt se enfoca en rewards farming porque es la estrategia más apropiada para $400 de capital. Las estrategias direccionales y de arbitraje requieren más capital para absorber la varianza. Los rewards son ingreso predecible que se cobra mientras el capital está "estacionado" proveyendo liquidez.

La migración a CLOB v2 es el primer paso obligatorio. Sin ella, el bot no puede operar. Después de migrar, la prioridad es: (1) descubrir mercados con buenos reward pools y poca competencia, (2) colocar órdenes optimizadas para la fórmula cuadrática, (3) mergear posiciones constantemente para no quedar sin capital libre.
