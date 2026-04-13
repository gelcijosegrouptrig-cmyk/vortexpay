#!/bin/bash
set -e
COMMIT_HASH=d9410bac81a1646a219bce94ec716393ae46220e
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/${COMMIT_HASH}"
echo "=== VortexPay Deploy - UI-v30 ==="
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
