# Recursos para aprender — validar sin autoengañarse y operar con riesgo serio

Lista curada y verificada (julio 2026). El objetivo no es "aprender a ganar": es aprender a
**no volver a fundirte**. Todo lo `[RIGUROSO]`/`[OFICIAL]` no te vende nada.

**Etiquetas de credibilidad:**
`[RIGUROSO]` académico/peer-review o estándar de industria · `[OFICIAL]` doc técnica o código
open-source · `[VENDOR]` útil pero es embudo de venta, leelo escéptico · `[BLOG]` argumento
válido pero no revisado · `[RUIDO]` evitar.

> Regla transversal: **nada que se venda con "señales", "mentoría SMC", "cuenta financiada" o
> retornos garantizados es educación. Es marketing.** Si te venden el resultado, no te enseñan
> el proceso.

---

## 1. Microestructura de mercado — la BASE REAL de tu estrategia

Lo único de "smart money" con fundamento medible: los stops se agrupan en niveles predecibles y
su ejecución en cascada mueve el precio. Es tu *turtle soup / barrido de liquidez*, pero escrito
por gente que midió el order book real.

- **Stop-Loss Orders and Price Cascades in Currency Markets** — Osler, NY Fed SR150. *Paper.* `[RIGUROSO]`
  https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr150.pdf
  El paper fundacional de tu estrategia: con el libro de órdenes real de RBS demuestra que los
  stops se agrupan y su disparo en cascada propaga tendencias. *El* sustento del liquidity sweep.
- **Support for Resistance: Technical Analysis and Intraday Exchange Rates** — Osler, NY Fed EPR. `[RIGUROSO]`
  https://www.newyorkfed.org/research/epr/00v06n2/0007osle.html
  Versión más corta y legible. **Empezá por acá** antes del SR150.
- **Currency Orders and Exchange-Rate Dynamics** — Osler, NY Fed SR125. `[RIGUROSO]`
  https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr125.pdf
- **Flow Toxicity and Liquidity in a High-Frequency World (VPIN)** — Easley, López de Prado, O'Hara. `[RIGUROSO]`
  https://www.quantresearch.org/VPIN.pdf
  Adverse selection / flujo tóxico: el mecanismo EXACTO que te fundió en el rewards farming
  single-sided (te llenaron con fill tóxico). Entenderlo te vacuna.
- **Trading and Exchanges** — Larry Harris (2003). *Libro.* `[RIGUROSO]` (la biblia del tema)
  Tipos de órdenes, stops, dealers, cascadas de liquidez. La formación que a los "SMC coaches" les falta.
- **The Wyckoff Method: A Tutorial** — StockCharts ChartSchool. `[OFICIAL]`
  https://chartschool.stockcharts.com/table-of-contents/market-analysis/wyckoff-analysis-articles/the-wyckoff-method-a-tutorial
  SMC es en gran parte Wyckoff rebrandeado (springs/shakeouts = barridos). Leé la fuente.
- **Street Smarts** — Linda Raschke & Larry Connors (1996). *Libro.* `[RIGUROSO]`
  La fuente ORIGINAL del patrón *Turtle Soup* (tu estrategia), definido con reglas mecánicas, no narrativa.

## 2. SMC/ICT con ojo crítico (mecanizable) + críticas fundadas

La detección se mecaniza; la *narrativa* de por qué funciona suele ser infalsable.

- **joshyattridge/smart-money-concepts** — repo/librería Python. `[OFICIAL]`
  https://github.com/joshyattridge/smart-money-concepts
  Referencia de detección (OB, FVG, BOS/CHoCH) para cross-checkear tu `crypto/smc/signals.py`.
  Leé el código: cada "concepto" es una regla trivial sobre highs/lows — eso desmitifica el marketing.
  (Ojo: confirma swings con velas futuras → look-ahead; tu `signals.py` ya lo corrige con delay.)
- **Smart Money Concepts — LuxAlgo** — doc de indicador. `[VENDOR]` (tomá definiciones, NO compres)
  https://www.luxalgo.com/library/indicator/smart-money-concepts-smc/
- **The Illusion of Edge: SMC, Survivorship Bias, and Market Reality** — `[BLOG]` (argumento correcto)
  https://wire.insiderfinance.io/the-illusion-of-edge-smc-survivorship-bias-and-market-reality-ae7873ef154d
  La crítica central: SMC es en buena parte infalsable, y una estrategia sin edge produce
  ganadores por varianza. Puente perfecto a la sección 3.

> Los YouTubers de ICT y los cursos de "mentoría SMC" son `[RUIDO]`: cero publican un backtest
> con fees, slippage y out-of-sample.

## 3. Validación cuantitativa y ANTI-OVERFITTING (la sección más importante)

Si solo tuvieras tiempo para una sección, es esta. Ya tenés `--deflated-sharpe` y Monte Carlo en
`validate.py`; acá están los *porqués*.

- **Advances in Financial Machine Learning** — Marcos López de Prado (2018). *Libro.* `[RIGUROSO]`
  Cap. 1 gratis: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3104847
  El estándar. Cada variante que probás es una hipótesis; el testeo secuencial "mejorando" genera
  overfitting que vuelve el resultado basura. Regla: cuanto más prolijo el desarrollo, MÁS BAJO el retorno real.
- **quantresearch.org** — LdP: papers + código Python (DSR, PBO, VPIN) + curso ORIE 5256, gratis. `[RIGUROSO/OFICIAL]`
  https://www.quantresearch.org/
- **The Deflated Sharpe Ratio** — Bailey & López de Prado (2014). `[RIGUROSO]`
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
  El fundamento de tu flag `--deflated-sharpe`. Si probaste 20 variantes, el "mejor Sharpe" está
  inflado aunque todas sean ruido. Tu escenario exacto.
- **Pseudo-Mathematics and Financial Charlatanism** — Bailey, Borwein, LdP, Zhu (2014). `[RIGUROSO]`
  PDF: https://scholarworks.wmich.edu/math_pubs/40/
  Con pocas configuraciones ya conseguís un backtest espectacular por azar, y bajo memoria el
  overfitting da retornos esperados NEGATIVOS out-of-sample. **Releelo cada vez que un backtest te enamore.**
- **The Probability of Backtest Overfitting (PBO)** — Bailey et al. `[RIGUROSO]`
  PDF: https://scholarworks.wmich.edu/math_pubs/42/
- **…and the Cross-Section of Expected Returns** — Harvey, Liu, Zhu (2016). `[RIGUROSO]`
  PDF: https://people.duke.edu/~charvey/Research/Published_Papers/P118_and_the_cross.PDF
  Por el data mining, un t-ratio > 2 no significa nada; el umbral real es > 3.0.
- **The Evaluation and Optimization of Trading Strategies** — Robert Pardo (2008). *Libro.* `[RIGUROSO]`
  El texto canónico de *walk-forward*. Directamente aplicable a tu "Sharpe OOS ≥ 50% del IS".
- **Quantitative Trading** — Ernest Chan (2ª ed., 2021). *Libro.* `[RIGUROSO]` (quant retail honesto)
  El puente entre teoría y un retail con poco capital: expectativas realistas, data-snooping, sizing.
  **El más accesible de esta sección — empezá por acá.**
- **The Kelly Criterion in Blackjack, Sports Betting, and the Stock Market** — Ed Thorp. `[RIGUROSO]`
  PDF: https://gwern.net/doc/statistics/decision/2006-thorp.pdf
  Kelly COMPLETO es demasiado agresivo; casi todos usan 1/4–1/2. Tu "1% por trade" es Kelly
  ultra-fraccionado, y este paper te dice por qué eso es lo correcto para sobrevivir.

## 4. freqtrade — documentación oficial `[OFICIAL]`

- Docs: https://www.freqtrade.io/en/stable/ · Repo: https://github.com/freqtrade/freqtrade
- **Strategy Customization** (incl. Protections + custom stoploss): https://www.freqtrade.io/en/stable/strategy-customization/
- **Backtesting**: https://www.freqtrade.io/en/stable/backtesting/ — usá `--enable-protections`.
- **Hyperopt**: https://www.freqtrade.io/en/stable/hyperopt/ — **PELIGRO: máquina de overfitting.**
  Si lo usás: 3-5 params máx, rangos acotados, y SIEMPRE OOS separado.
- **Plotting**: https://www.freqtrade.io/en/stable/plotting/ — mirá la curva *underwater* (drawdown), no el profit.
- **Deprecated**: https://www.freqtrade.io/en/stable/deprecated/ — **el módulo Edge fue ELIMINADO en 2025.6.**
  No lo uses. El sizing va por `custom_stake_amount` (tu regla del 1% por distancia al stop) — que es
  justamente lo que hace `crypto/user_data/strategies/smc_sweep.py`.

## 5. Crypto-específico (mecánica real, no señales)

- **Understanding Funding Rates in Perpetual Futures** — Coinbase Learn. `[OFICIAL]`
  https://www.coinbase.com/learn/perpetual-futures/understanding-funding-rates-in-perpetual-futures
  Aunque tu plan sea SPOT: entender el funding es lo que justifica NO dejarte tentar por perps.
- **Fee schedules oficiales** (Binance/Kraken). `[OFICIAL]`
  https://www.binance.com/en/fee/schedule · https://www.kraken.com/features/fee-schedule
  La verdad de tierra que tu backtester DEBE clavar: por qué en 4h los costos son sobrevivibles y en <1h te matan.

> "Grupos de señales", "copytrading", "bot 90% winrate" = `[RUIDO]`. Un backtest con 100% winrate MIENTE.

## 6. Psicología y riesgo de ruina (evidencia, no motivación)

Calibrá expectativas con datos duros: que "no fundirte" sea el KPI, no "generar ingresos".

- **Do Day Traders Rationally Learn About Their Ability?** — Barber, Lee, Liu, Odean (Taiwán). `[RIGUROSO]`
  PDF: https://faculty.haas.berkeley.edu/odean/papers/Day%20Traders/Day%20Trading%20and%20Learning%20110217.pdf
  Solo ~1.6% de los day traders es rentable en un año típico; 80% abandona en 2 años.
- **Day Trading for a Living?** — Chague, De-Losso, Giovannetti (Brasil). `[RIGUROSO]`
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3423101
  De quienes persistieron >300 días, **97% perdió plata**. Pegá este número en el monitor.
- **Van Tharp — position sizing / R-multiples / expectancy.** *Libros.* `[RIGUROSO en concepto / VENDOR en el instituto]`
  Los conceptos son sólidos: el "cuánto" (sizing) importa más que el "qué" (entrada). Quedate con los
  libros, ignorá los cursos carísimos.
- **Fooled by Randomness** — Nassim Taleb. *Libro.* `[RIGUROSO]`
  Suerte vs skill. Antídoto contra creerte genio tras una racha (o inútil tras una pérdida). Leelo
  justo después de fundirte una vez — es tu momento.

> Concepto extra: buscá **"risk of ruin"** y **Kelly fraccionado / optimal f (Ralph Vince)**. Incluso
> con edge positivo, un sizing agresivo te lleva a la ruina con prob alta. Por eso 1%/trade + kill-switch.

## 7. Comunidades y herramientas

**Señal:** Quantitative Finance Stack Exchange (https://quant.stackexchange.com/) · freqtrade repo +
Discord oficial · blog de Ernie Chan (https://epchan.blogspot.com/) · quantresearch.org.
**Ruido/mixto:** r/algotrading (señal enterrada bajo "miren mi equity curve") · cualquier Discord/Telegram
de "señales", gumroad "freqtrade mastery", YouTubers de ICT/SMC, "cuentas financiadas" = `[RUIDO]`.

---

## Ruta de aprendizaje (para $150-500: PRIMERO no volver a fundirte)

0. **Aceptá la base rate (1-2 días).** Chague (97% pierde) + Barber/Odean + *Fooled by Randomness*.
   El resultado por defecto es perder; tu KPI es sobrevivir. No escribas código nuevo hasta interiorizarlo.
1. **Fijá la doctrina de riesgo ANTES que la estrategia (2-3 días).** Thorp (Kelly fraccionado) + sizing de
   Van Tharp. Ya está en `REGLAS_CONGELADAS.md`; ahora vas a entender *por qué* cada número.
2. **Entendé QUÉ parte de SMC es medible (3-5 días).** Osler (EPR → SR150) + Wyckoff + order types de Harris.
3. **Blindate contra vos mismo (1 semana, la más importante).** Harvey/Liu + Bailey/Borwein (*Pseudo-Math* +
   PBO) + Deflated Sharpe + Pardo (walk-forward) + backtesting de LdP.
4. **Mecanizá UNA señal, 3-5 params (1-2 semanas).** Ya hecho: `crypto/smc/signals.py`. Cross-check con
   joshyattridge + LuxAlgo (sin comprar nada).
5. **Validá con costos y fills adversos (1-2 semanas).** Ya tenés el backtester + walk-forward + DSR + Monte
   Carlo. **Gate duro:** Sharpe OOS < 50% del IS, o no batir buy&hold/MA diaria → PARÁS. No hay live.
6. **Reproducí en freqtrade (1 semana).** Ya escrito: `SmcSweep`. Backtest con `--enable-protections`. Sizing
   por `custom_stake_amount` (Edge está eliminado). Si tocás hyperopt: 5 params máx + OOS separado.
7. **Dry-run 4-8 semanas (obligatorio).** `freqtrade trade -c crypto/config-dryrun.json`. Verificá que
   fills/fees coinciden con el backtest. Si divergen, el backtest mentía → volvé al paso 5.
8. **Live mínimo + disciplina (continuo).** Solo si pasaste todos los gates: el tamaño más chico posible.
   Journal, log de multiple-testing en `REGLAS_CONGELADAS.md`, y borrá cualquier canal de señales.

> **Regla que atraviesa todo:** cada vez que un backtest te enamore, releé *Pseudo-Mathematics and
> Financial Charlatanism*. El backtest hermoso es el síntoma, no la cura.
