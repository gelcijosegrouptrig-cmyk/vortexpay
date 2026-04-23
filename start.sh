#!/bin/bash
set -e
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - UI-v31 ==="

pip install aiohttp telethon aiofiles psycopg2-binary requests 2>/dev/null | tail -1

# Baixar arquivos do GitHub
for f in server.py admin.html paypix.html paypix2.html sorteio.html home.html mp2_api.py bot2_handler.py bot3_handler.py bot_pix.html update.py; do
  curl -fsSL "$REPO_RAW/$f" -o "$f" 2>/dev/null || echo "usando $f local"
done

export ASAAS_API_KEY="${ASAAS_API_KEY}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "Iniciando servidor..."
python3 server.py
