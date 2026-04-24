#!/bin/bash
set -e
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - UI-v32-crests ==="

pip install aiohttp telethon aiofiles psycopg2-binary requests 2>/dev/null | tail -1

# Baixar arquivos do GitHub (cache-bust via timestamp)
TS=$(date +%s)
for f in server.py admin.html paypix.html paypix2.html sorteio.html home.html mp2_api.py bot2_handler.py bot3_handler.py bot_pix.html update.py; do
  curl -fsSL "$REPO_RAW/$f?ts=$TS" -o "$f" 2>/dev/null || echo "usando $f local"
done
echo "=== server.py versao: $(grep -m1 'v2025' $REPO_RAW/server.py 2>/dev/null || grep -m1 \"'version'\" server.py) ==="

export ASAAS_API_KEY="${ASAAS_API_KEY}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "Iniciando servidor..."
python3 server.py

