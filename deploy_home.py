#!/usr/bin/env python3
"""Deploy home.html para DB + verificar conteudo"""
import psycopg2, os

DB = 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'

with open('/home/user/vortex_deploy/home.html', 'r', encoding='utf-8') as f:
    html = f.read()

print(f"HTML size: {len(html)} chars")

checks = [
    ('api/bet/tabela',    'nova rota tabela TheSportsDB'),
    ('api/bet/fixtures',  'nova rota fixtures TheSportsDB'),
    ('_TSDB_LEAGUES',     'nao existe no frontend'),  # backend only, skip
    ('odd-btn',           'botoes de odds'),
    ('carregarTabela',    'funcao tabela'),
    ('carregarJogos',     'funcao jogos'),
    ('venue',             'local do jogo'),
    ('betslip',           'betslip presente'),
]

all_ok = True
for term, desc in checks:
    if term == '_TSDB_LEAGUES':
        print(f"SKIP {term} (backend only)")
        continue
    found = term in html
    status = 'OK' if found else 'FAIL'
    if not found:
        all_ok = False
    print(f"{status}  {term} — {desc}")

if not all_ok:
    print("Verificacao falhou!")
    exit(1)

try:
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute("UPDATE configuracoes SET valor=%s WHERE chave='home_html_patch'", (html,))
    conn.commit()
    print(f"\nDB atualizado: {cur.rowcount} row(s), {len(html)} chars")
    cur.close()
    conn.close()
except Exception as e:
    print(f"DB erro: {e}")
    exit(1)

print("Deploy OK!")
