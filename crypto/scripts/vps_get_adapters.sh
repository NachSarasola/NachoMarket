#!/usr/bin/env bash
# Instala un snapshot del repo open-source DefiLlama/emissions-adapters (hoy privado en
# GitHub) SIN credenciales, desde: (1) forks públicos descubiertos vía grep.app, o
# (2) el archivo público Software Heritage (existe exactamente para código que desaparece).
# Solo escribe bajo crypto/data/events. Mismas guardas anti-~/nachomarket del resto.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
[ "$(id -u)" = "0" ] && { echo "❌ No corras como root." >&2; exit 1; }
case "$REPO_ROOT" in
    *nachomarket*|*/nachomarket|*/nachomarket/*)
        echo "❌ ABORTO: '$REPO_ROOT' parece el arbol del OTRO bot." >&2; exit 1;;
esac

EV="$REPO_ROOT/crypto/data/events"
ADP="$EV/emissions-adapters"
[ -d "$ADP/protocols" ] && { echo "✅ ya instalado: $ADP"; exit 0; }
mkdir -p "$EV"

install_tgz() {  # $1 = tarball
    rm -rf "$EV/ea_x"; mkdir -p "$EV/ea_x"
    tar xzf "$1" -C "$EV/ea_x" 2>/dev/null || tar xf "$1" -C "$EV/ea_x" 2>/dev/null || return 1
    local top
    top=$(find "$EV/ea_x" -mindepth 1 -maxdepth 1 -type d | head -1)
    [ -n "$top" ] || return 1
    rm -rf "$ADP" && mv "$top" "$ADP"; rm -rf "$EV/ea_x"
    [ -d "$ADP/protocols" ]
}

try_repo() {  # $1 = owner/repo
    local ref code
    for ref in master main; do
        code=$(curl -sSL --max-time 180 -o /tmp/ea.tgz -w '%{http_code}' \
            "https://codeload.github.com/$1/tar.gz/refs/heads/$ref" 2>/dev/null)
        echo "    $1 @$ref -> HTTP $code"
        [ "$code" = "200" ] && install_tgz /tmp/ea.tgz && return 0
    done
    return 1
}

echo "== 1) Buscando forks públicos del repo (grep.app) =="
CANDS=$(python3 - <<'PY'
import json
import urllib.parse
import urllib.request

repos: list[str] = []

def walk(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "raw" and isinstance(v, str) and v.count("/") == 1 and v not in repos:
                repos.append(v)
            else:
                walk(v)
    elif isinstance(o, list):
        for v in o:
            walk(v)

for q in ("manualCliff", "emissions-adapters manualLinear", "periodToSeconds manualCliff"):
    try:
        url = "https://grep.app/api/search?q=" + urllib.parse.quote(q)
        req = urllib.request.Request(url, headers={"User-Agent": "nacho-crypto/1.0"})
        walk(json.load(urllib.request.urlopen(req, timeout=30)))
    except Exception:
        pass
print(" ".join(repos[:12]))
PY
)
echo "  candidatos: ${CANDS:-ninguno}"
for R in $CANDS; do
    try_repo "$R" && { echo "✅ instalado desde fork: $R"; exit 0; }
done

echo "== 2) Software Heritage (archivo público de código abierto) =="
python3 - <<'PY'
import json
import sys
import time
import urllib.request

BASE = "https://archive.softwareheritage.org/api/1"
ORIGIN = "https://github.com/DefiLlama/emissions-adapters"
HDR = {"User-Agent": "nacho-crypto/1.0"}


def get(u):
    req = urllib.request.Request(u, headers=HDR)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def post(u):
    req = urllib.request.Request(u, method="POST", headers=HDR)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


try:
    visits = get(f"{BASE}/origin/{ORIGIN}/visits/")
except Exception as e:  # noqa: BLE001
    print(f"SWH: origen no consultable: {e}")
    sys.exit(1)
snap = next((v["snapshot"] for v in visits
             if v.get("status") == "full" and v.get("snapshot")), None)
if not snap:
    print("SWH: sin snapshot completo archivado")
    sys.exit(1)
print("  snapshot:", snap, "| última visita archivada:", visits[0].get("date"))

s = get(f"{BASE}/snapshot/{snap}/")
branches = s.get("branches", {})
b = branches.get("HEAD") or {}
if b.get("target_type") == "alias":
    b = branches.get(b.get("target"), {})
if b.get("target_type") != "revision":
    for name in ("refs/heads/master", "refs/heads/main"):
        if branches.get(name, {}).get("target_type") == "revision":
            b = branches[name]
            break
if b.get("target_type") != "revision":
    print("SWH: no pude resolver la rama; ramas:", list(branches)[:8])
    sys.exit(1)
rev = get(f"{BASE}/revision/{b['target']}/")
dir_id = rev["directory"]
print("  revisión:", b["target"][:12], rev.get("date"), "| dir:", dir_id[:12])

vurl = f"{BASE}/vault/flat/swh:1:dir:{dir_id}/"
try:
    st = post(vurl)
except Exception:  # noqa: BLE001 — quizá ya estaba cocinado/cocinando
    st = get(vurl)
for i in range(60):  # ~15 min máximo
    if st.get("status") == "done":
        break
    print(f"  cocinando tarball en SWH... ({st.get('status')}, intento {i + 1}/60)",
          flush=True)
    time.sleep(15)
    st = get(vurl)
if st.get("status") != "done":
    print("SWH: la cocción no terminó todavía.")
    sys.exit(2)
fetch = st.get("fetch_url")
print("  descargando:", fetch)
urllib.request.urlretrieve(fetch, "/tmp/ea_swh.tar.gz")
print("OK /tmp/ea_swh.tar.gz")
PY
RC=$?
if [ "$RC" -eq 0 ] && install_tgz /tmp/ea_swh.tar.gz; then
    echo "✅ instalado desde Software Heritage: $ADP"
    exit 0
fi
if [ "$RC" -eq 2 ]; then
    echo "⏳ SWH sigue cocinando el tarball: correr de nuevo en 5-10 min:"
    echo "   bash crypto/scripts/vps_get_adapters.sh && bash crypto/scripts/vps_run_eventos.sh"
    exit 2
fi
echo "❌ Sin fuente para emissions-adapters (forks y SWH fallaron)."
echo "   Siguiente paso registrado: plan C (reconstrucción por saltos de supply circulante)."
exit 1
