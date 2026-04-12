#!/bin/bash
# Script de inicialização com auto-update
echo "🔄 Verificando atualização do servidor..."
GITHUB_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main/server.py"
curl -s -f -o /tmp/server_new.py "$GITHUB_RAW" && mv /tmp/server_new.py server.py && echo "✅ server.py atualizado!" || echo "⚠️ Usando server.py local"
exec python3 server.py
