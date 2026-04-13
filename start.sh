#!/bin/bash
set -e
COMMIT_HASH="38eb7aa14bc978aeb33f45b1b8e79e7b1d2c8fd7"
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/${COMMIT_HASH}"
echo "=== VortexPay Deploy - UI-v29 ==="
echo "Commit: $COMMIT_HASH"

pip install aiohttp telethon aiofiles psycopg2-binary 2>/dev/null | tail -1

# Baixar arquivos do GitHub no commit fixo estável
for f in server.py admin.html paypix.html sorteio.html home.html; do
  echo "⬇ Baixando $f..."
  curl -fsSL "$REPO_RAW/$f" -o "$f" 2>/dev/null || echo "⚠ Usando $f local"
done

# ── Variáveis de ambiente garantidas (fallback se não vierem do Railway) ──
export ASAAS_API_KEY="${ASAAS_API_KEY:-\ASAAS_KEY_REMOVED}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "✅ Asaas configurado: env=$ASAAS_ENV"
echo "🚀 Iniciando servidor..."
python3 server.py
