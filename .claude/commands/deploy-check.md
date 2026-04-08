# Deploy Check

Verifica que todo esta listo para deploy:

1. Corre todos los tests con `python -m pytest tests/ -v`
2. Verifica que `.env` tiene todas las keys necesarias (compara con `.env.example`)
3. Verifica que `config/settings.yaml` tiene `mode: live` o `mode: paper` segun corresponda
4. Revisa que no haya secrets hardcodeados en el codigo (`grep -rn` en src/)
5. Verifica que el circuit breaker esta configurado correctamente en `config/risk.yaml`
6. Resume los resultados y da un GO/NO-GO
