#!/bin/bash
set -e
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - UI-v32-crests ==="

# NÃO reinstalar pacotes aqui — já instalados pelo pip no build (requirements.txt)
# pip install ... removido para não sobrescrever versões do venv do build

# Baixar SOMENTE arquivos HTML/estáticos do GitHub (NÃO baixar .py — usa versão do build)
TS=$(date +%s)
for f in admin.html cobrar.html paypix.html paypix2.html sorteio.html home.html bot_pix.html; do
  curl -fsSL "$REPO_RAW/$f?ts=$TS" -o "$f" 2>/dev/null || echo "usando $f local"
done

echo "=== server.py versao: $(grep -m1 \"'version'\" server.py) ==="

export ASAAS_API_KEY="${ASAAS_API_KEY}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "Iniciando servidor..."
exec python3 server.py
