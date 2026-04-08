# Review Trades

Analiza el historial de trades recientes:

1. Lee `data/trades.jsonl` y parsea las ultimas 50 operaciones
2. Calcula: PnL total, win rate, average trade size, mercados mas operados
3. Identifica patrones: mejores/peores horarios, mercados mas rentables
4. Compara con los limites de `config/risk.yaml`
5. Genera un resumen con recomendaciones accionables
