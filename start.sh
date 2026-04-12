#!/bin/bash
# Script de inicialização com auto-update
echo "🔄 Verificando atualização do servidor..."
# Usa o branch main com cache-bust para garantir versão mais nova
GITHUB_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main/server.py"
curl -s -f --max-time 15 -H "Cache-Control: no-cache" -o /tmp/server_new.py "$GITHUB_RAW" \
  && mv /tmp/server_new.py server.py \
  && echo "✅ server.py atualizado!" \
  || echo "⚠️ Usando server.py local"
echo "🚀 Iniciando servidor..."
exec python3 server.py
