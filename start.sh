#!/bin/bash
set -e
# Sempre baixar do main (sem hash fixo — sempre pega versão mais recente)
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - UI-v30 ==="
echo "Branch: main (sempre atualizado)"

pip install aiohttp telethon aiofiles psycopg2-binary 2>/dev/null | tail -1

# Baixar arquivos sempre da branch main (versão mais recente)
for f in server.py admin.html paypix.html sorteio.html home.html; do
  echo "⬇ Baixando $f..."
  curl -fsSL "$REPO_RAW/$f" -o "$f" 2>/dev/null || echo "⚠ Usando $f local"
done

# ── Variáveis de ambiente garantidas (fallback se não vierem do Railway) ──
export ASAAS_API_KEY="${ASAAS_API_KEY}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "✅ Asaas configurado: env=$ASAAS_ENV (chave via Railway env var)"
echo "🚀 Iniciando servidor..."
python3 server.py
