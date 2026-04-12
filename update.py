#!/usr/bin/env python3
"""Script de auto-atualização - executado pelo servidor Railway"""
import os, sys, subprocess, time

GITHUB_RAW = "https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/main/server.py"

def update_and_restart():
    print("🔄 Baixando server.py mais recente do GitHub...", flush=True)
    result = subprocess.run(
        ['curl', '-s', '-o', 'server.py', GITHUB_RAW],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("✅ server.py atualizado!", flush=True)
        print("🔄 Reiniciando processo...", flush=True)
        time.sleep(1)
        os._exit(1)  # Railway reinicia automaticamente
    else:
        print(f"❌ Erro ao baixar: {result.stderr}", flush=True)

if __name__ == '__main__':
    update_and_restart()
