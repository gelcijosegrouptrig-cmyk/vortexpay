#!/bin/bash
echo "=== PaynexBet Server Start ==="

# Auto-update from GitHub (pinned commit to avoid CDN cache)
COMMIT_HASH="7a7ef902d6fffb70e238cc984261b9e952429822"
echo "Baixando server.py do commit $COMMIT_HASH..."
curl -fsSL --no-cache -H "Cache-Control: no-cache" \
  "https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/${COMMIT_HASH}/server.py" \
  -o /tmp/server_new.py 2>/dev/null

if [ -s /tmp/server_new.py ]; then
  mv /tmp/server_new.py server.py
  echo "✅ server.py atualizado (commit ${COMMIT_HASH})!"
else
  echo "⚠️ Falha no download, usando server.py local"
fi

exec python3 server.py
