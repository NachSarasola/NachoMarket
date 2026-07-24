# Despliegue en el VPS — AISLADO del otro bot

> ⚠️ **Hay otro bot corriendo en el VPS (`~/nachomarket`, servicio `polymarket-bot`). NADA de
> esto lo toca.** Todo el bot de crypto vive en un directorio y un venv separados. No corras
> `scripts/deploy.sh` ni `scripts/setup_vm.sh` para esto — esos apuntan a `~/nachomarket`,
> hacen `apt upgrade` global y tocan systemd. Para el bot de crypto: SOLO lo de abajo.

Claude no puede entrar al VPS desde su entorno (sin cliente ssh, sin tu clave, y el egress a
la VPS está bloqueado). Así que estos pasos los corrés vos con tu `ssh dublin`. Son seguros,
aislados e idempotentes.

## Reglas de seguridad (no romper el otro bot)

- Directorio del bot crypto: **`~/nacho-crypto`** (separado de `~/nachomarket`).
- venv propio: **`~/nacho-crypto/.venv-crypto`** (no toques `~/nachomarket/venv`).
- **Cero** `sudo`, `apt upgrade`, systemd o `rm` sobre nada del otro bot.
- El script `vps_setup.sh` **aborta solo** si detecta que estás dentro de `~/nachomarket`.

## Paso 1 — Traer el código (elegí A o B)

**A) Si el VPS tiene acceso a GitHub** (probá `git ls-remote`):
```bash
cd ~
git clone -b claude/profitable-bots-low-capital-ql5aw7 <URL_DEL_REPO> nacho-crypto
# si ya existe:  cd ~/nacho-crypto && git fetch origin && git checkout claude/profitable-bots-low-capital-ql5aw7 && git pull
```

**B) Si el VPS NO tiene git/GitHub** — copiá SOLO la carpeta `crypto/` desde tu PC (la carpeta
es autocontenida; sus imports solo necesitan que `crypto/` esté bajo el directorio raíz):
```bash
# desde tu máquina, con el repo checkouteado en el branch:
rsync -avz --exclude='data/' ./crypto  ubuntu@<IP-VPS>:~/nacho-crypto/
```
Queda `~/nacho-crypto/crypto/...`. (rsync sin `--delete`: no borra nada.)

## Paso 2 — Setup aislado + tests

```bash
cd ~/nacho-crypto
bash crypto/scripts/vps_setup.sh          # crea .venv-crypto, instala numpy/pandas/pytest, corre 32 tests
# para bajar datos con ccxt además:
bash crypto/scripts/vps_setup.sh --ccxt
```
Si dice que falta `python3-venv`, es lo ÚNICO que puede requerir `sudo apt install -y
python3-venv` (una vez). No instala nada más a nivel sistema.

## Paso 3 — Datos reales + validación

```bash
source ~/nacho-crypto/.venv-crypto/bin/activate
cd ~/nacho-crypto

# Bajar OHLCV (si Binance está bloqueado desde el VPS, probá --exchange kraken u okx):
python crypto/scripts/fetch_data.py --symbol BTC/USDT --timeframe 4h --since 2019-01-01 \
    --out crypto/data/BTC_USDT-4h.csv
python crypto/scripts/fetch_data.py --symbol ETH/USDT --timeframe 4h --since 2019-01-01 \
    --out crypto/data/ETH_USDT-4h.csv

# Validar (los gates): in-sample, walk-forward, OOS 2024+, benchmarks, DSR, Monte Carlo:
python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv --strategy sweep \
    --compare --deflated-sharpe 108 --out crypto/data/report_btc_sweep.json
python crypto/scripts/validate.py --data crypto/data/BTC_USDT-4h.csv --strategy donchian \
    --compare
# El sweep DEBE batir al donchian (control) y a buy&hold/MA, neto de costos, en OOS.

# Robustez de parámetros (meseta vs pico):
python crypto/scripts/param_sweep.py --data crypto/data/BTC_USDT-4h.csv
```

**Gate duro:** si el Sharpe OOS < 50% del in-sample, o no bate buy&hold ni la MA diaria, o el
DSR < 0.95 → **NO se opera**. Ese "no" es el resultado válido que evita la próxima fundida.

## Paso 4 — freqtrade (dry-run) — SOLO si el paso 3 pasó los gates

freqtrade en su PROPIO venv aislado (no mezclar con el del otro bot). Requiere TA-Lib del
sistema; si el `apt install` de TA-Lib te preocupa por el otro bot, hacelo en una ventana de
mantenimiento o usá una imagen/venv dedicada.
```bash
python3.11 -m venv ~/nacho-crypto/.venv-ft && source ~/nacho-crypto/.venv-ft/bin/activate
pip install freqtrade
freqtrade download-data -c crypto/config-backtest.json --timerange 20190101- --timeframes 4h
freqtrade backtesting -c crypto/config-backtest.json --strategy SmcSweep \
    --strategy-path crypto/user_data/strategies --timerange 20190101-20231231 --enable-protections
# Paper trading (corré en tmux/screen para que sobreviva a la desconexión SSH):
freqtrade trade -c crypto/config-dryrun.json --strategy SmcSweep \
    --strategy-path crypto/user_data/strategies
```
`config-dryrun.json` tiene `dry_run: true` y sin claves → no arriesga dinero. Telegram está
`enabled: false`; si querés control, poné token/chat en un archivo aparte, NO en el repo.

## Qué NO hacer

- ❌ `bash scripts/deploy.sh` / `scripts/setup_vm.sh` para el bot de crypto (son del otro bot).
- ❌ Instalar en `~/nachomarket/venv` ni tocar el servicio `polymarket-bot`.
- ❌ `git checkout` de este branch dentro de `~/nachomarket` (cambiaría el código del otro bot).
- ❌ Pasar a live sin: OOS creíble, batir benchmarks, y 4-8 semanas de dry-run.
