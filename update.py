#!/usr/bin/env python3
"""Script de auto-atualização - executado pelo servidor Railway"""
import os, sys, subprocess, time

GITHUB_BASE = "https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main"

# Todos os arquivos que devem ser atualizados do GitHub
FILES_TO_UPDATE = [
    "server.py",
    "admin.html",
    "paypix.html",
    "paypix2.html",
    "bot_pix.html",
    "bot2_handler.py",
    "bot3_handler.py",
]

def update_and_restart():
    print("🔄 Iniciando atualização completa do GitHub...", flush=True)
    erros = []
    for fname in FILES_TO_UPDATE:
        url = f"{GITHUB_BASE}/{fname}"
        print(f"  ⬇️  Baixando {fname}...", flush=True)
        result = subprocess.run(
            ['curl', '-s', '-f', '-o', fname, url],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"  ✅ {fname} atualizado!", flush=True)
        else:
            print(f"  ⚠️  {fname} — não encontrado ou erro (ignorando)", flush=True)
            erros.append(fname)

    if len(erros) < len(FILES_TO_UPDATE):
        print("✅ Atualização concluída! Reiniciando...", flush=True)
        time.sleep(1)
        os._exit(1)  # Railway reinicia automaticamente
    else:
        print("❌ Nenhum arquivo foi atualizado.", flush=True)

if __name__ == '__main__':
    update_and_restart()
