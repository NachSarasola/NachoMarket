# Despliegue en el VPS — AISLADO del otro bot

> ⚠️ **Hay otro bot corriendo en el VPS (`~/nachomarket`, servicio `polymarket-bot`). NADA de
> esto lo toca.** Todo el bot de crypto vive en un directorio y un venv separados. Los scripts
> de deploy del bot viejo (`deploy.sh`/`setup_vm.sh`, ya eliminados de este branch pero
> posiblemente presentes en el VPS) apuntan a `~/nachomarket`, hacen `apt upgrade` global y
> tocan systemd — NO los uses para esto. Para el bot de crypto: SOLO lo de abajo.

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

## Paso 3 — Datos reales + validación (UN comando)

```bash
source ~/nacho-crypto/.venv-crypto/bin/activate && cd ~/nacho-crypto
bash crypto/scripts/vps_validate_all.sh
```
Esto baja BTC+ETH 4h (con fallback binance→okx→kraken), corre TODOS los gates para sweep y
donchian en ambos pares (in-sample, walk-forward, OOS 2024+, benchmarks, DSR, Monte Carlo),
corre el barrido de parámetros, aplica el **veredicto mecánico** (`decide.py`) y empaqueta
todo en `crypto/data/reportes_<fecha>.tar.gz`. **Traeme ese .tar.gz** (o pegá el
`veredicto.txt` y los `report_*.json`).

**Veredicto** (lo calcula `decide.py`, sin interpretación humana):
- `GO_DRY_RUN` → pasar a paper (paso 4).
- `AJUSTE_UNICO(...)` → una sola tanda de ajuste (se registra como trial) y re-validar.
- `DESCARTAR_SWEEP_QUEDA_DONCHIAN` → el control tiene edge y el sweep no.
- `NO_OPERAR` → no hay edge; no hay live. Ese "no" evita la próxima fundida.

> Si preferís correrlo a mano, los comandos sueltos (`fetch_data.py`, `validate.py`,
> `param_sweep.py`, `decide.py`) están documentados en README.md.

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

- ❌ Usar los deploy scripts del bot viejo (si sobreviven en el VPS) para el bot de crypto.
- ❌ Instalar en `~/nachomarket/venv` ni tocar el servicio `polymarket-bot`.
- ❌ `git checkout` de este branch dentro de `~/nachomarket` (cambiaría el código del otro bot).
- ❌ Pasar a live sin: OOS creíble, batir benchmarks, y 4-8 semanas de dry-run.
