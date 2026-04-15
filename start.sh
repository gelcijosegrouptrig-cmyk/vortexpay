#!/bin/bash
# NÃO usar set -e — queremos continuar mesmo se um arquivo não baixar
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - UI-v31 ==="
echo "Branch: main (sempre atualizado)"

pip install aiohttp telethon aiofiles psycopg2-binary requests 2>/dev/null | tail -1

# Baixar TODOS os arquivos sempre da branch main
# Continua mesmo se algum falhar (sem set -e)
for f in server.py admin.html paypix.html sorteio.html home.html mp2_api.py bot2_handler.py bot_pix.html; do
  echo "⬇ Baixando $f..."
  if curl -fsSL --max-time 30 "$REPO_RAW/$f" -o "/tmp/$f" 2>/dev/null; then
    # Só substituir se o download teve sucesso e arquivo não está vazio
    if [ -s "/tmp/$f" ]; then
      cp "/tmp/$f" "./$f"
      echo "  ✅ $f atualizado"
    else
      echo "  ⚠ $f vazio — usando versão local"
    fi
  else
    echo "  ⚠ Falha ao baixar $f — usando versão local"
  fi
done

# ── Variáveis de ambiente garantidas (fallback se não vierem do Railway) ──
export ASAAS_API_KEY="${ASAAS_API_KEY}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "✅ Asaas configurado: env=$ASAAS_ENV"
echo "🚀 Iniciando servidor..."
python3 server.py
