# Paper Mode

Configura y verifica el modo paper trading:

1. Verifica que `config/settings.yaml` tiene `mode: paper`
2. Revisa que el paper trading esta correctamente implementado en `src/main.py`
3. Corre el bot en modo paper: `python src/main.py --paper`
4. Monitorea los primeros 30 segundos de ejecucion
5. Reporta si hay errores o comportamiento inesperado
