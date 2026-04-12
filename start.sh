#!/bin/bash
echo "=== PaynexBet Server Start ==="

# Auto-update from GitHub (pinned commit to avoid CDN cache)
COMMIT_HASH=967bfc43d0f48ff07105b3b0ac5853d1335c3d3f
BASE_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/${COMMIT_HASH}"

echo "Baixando arquivos do commit $COMMIT_HASH..."

# Baixar server.py
curl -fsSL --no-cache -H "Cache-Control: no-cache" \
  "${BASE_RAW}/server.py" -o /tmp/server_new.py 2>/dev/null
if [ -s /tmp/server_new.py ]; then
  mv /tmp/server_new.py server.py
  echo "✅ server.py atualizado!"
else
  echo "⚠️ Falha ao baixar server.py, usando local"
fi

# Baixar sorteio.html
curl -fsSL --no-cache -H "Cache-Control: no-cache" \
  "${BASE_RAW}/sorteio.html" -o /tmp/sorteio_new.html 2>/dev/null
if [ -s /tmp/sorteio_new.html ]; then
  mv /tmp/sorteio_new.html sorteio.html
  echo "✅ sorteio.html atualizado!"
else
  echo "⚠️ Falha ao baixar sorteio.html, usando local"
fi

# Baixar admin.html
curl -fsSL --no-cache -H "Cache-Control: no-cache" \
  "${BASE_RAW}/admin.html" -o /tmp/admin_new.html 2>/dev/null
if [ -s /tmp/admin_new.html ]; then
  mv /tmp/admin_new.html admin.html
  echo "✅ admin.html atualizado!"
else
  echo "⚠️ Falha ao baixar admin.html, usando local"
fi

# Baixar paypix.html
curl -fsSL --no-cache -H "Cache-Control: no-cache" \
  "${BASE_RAW}/paypix.html" -o /tmp/paypix_new.html 2>/dev/null
if [ -s /tmp/paypix_new.html ]; then
  mv /tmp/paypix_new.html paypix.html
  echo "✅ paypix.html atualizado!"
else
  echo "⚠️ Falha ao baixar paypix.html, usando local"
fi

exec python3 server.py
