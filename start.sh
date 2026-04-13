#!/bin/bash
set -e
COMMIT_HASH="83ffaa8"
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - ASAAS-v13 ==="
echo "Commit: $COMMIT_HASH"

pip install aiohttp telethon aiofiles 2>/dev/null | tail -1

# Baixar arquivos do GitHub
for f in server.py admin.html paypix.html sorteio.html; do
  echo "⬇ Baixando $f..."
  curl -fsSL "$REPO_RAW/$f" -o "$f" 2>/dev/null || echo "⚠ Usando $f local"
done

# ── Variáveis de ambiente garantidas (fallback se não vierem do Railway) ──
export ASAAS_API_KEY="${ASAAS_API_KEY:-\ASAAS_KEY_REMOVED}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "✅ Asaas configurado: env=$ASAAS_ENV"
echo "🚀 Iniciando servidor..."
python3 server.py
