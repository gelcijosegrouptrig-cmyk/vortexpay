#!/bin/bash
set -e
COMMIT_HASH="4d805a391acbfb8be36ca1952e0b8c757ee02ffd"
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - Auto-login v9 ==="
echo "Commit: $COMMIT_HASH"

pip install aiohttp telethon aiofiles 2>/dev/null | tail -1

# Baixar arquivos do GitHub
for f in server.py admin.html paypix.html sorteio.html; do
  echo "⬇ Baixando $f..."
  curl -fsSL "$REPO_RAW/$f" -o "$f" 2>/dev/null || echo "⚠ Usando $f local"
done

echo "🚀 Iniciando servidor..."
python3 server.py
