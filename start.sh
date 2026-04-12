#!/bin/bash
# Script de inicialização com auto-update
echo "🔄 Verificando atualização do servidor..."
# Usa commit hash fixo para evitar cache do GitHub CDN
COMMIT_HASH="7d84801"
GITHUB_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/${COMMIT_HASH}/server.py"
curl -s -f --max-time 15 -o /tmp/server_new.py "$GITHUB_RAW" \
  && mv /tmp/server_new.py server.py \
  && echo "✅ server.py atualizado (commit ${COMMIT_HASH})!" \
  || echo "⚠️ Usando server.py local"
echo "🚀 Iniciando servidor..."
exec python3 server.py
