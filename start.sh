#!/bin/bash
REPO_RAW="https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"
echo "=== VortexPay Deploy - UI-v31 ==="
echo "Branch: main (sempre atualizado)"

# Baixar TODOS os arquivos sempre da branch main
# Erros são ignorados — usa arquivo local como fallback
for f in server.py admin.html paypix.html sorteio.html home.html mp2_api.py bot2_handler.py bot_pix.html; do
  echo "⬇ Baixando $f..."
  curl -fsSL --max-time 30 "$REPO_RAW/$f" -o "$f" 2>/dev/null && echo "  ✅ $f" || echo "  ⚠ usando local $f"
done

# ── Variáveis de ambiente garantidas (fallback se não vierem do Railway) ──
export ASAAS_API_KEY="${ASAAS_API_KEY:-}"
export ASAAS_ENV="${ASAAS_ENV:-production}"

echo "✅ Arquivos atualizados"
echo "🚀 Iniciando servidor..."
exec python3 server.py
