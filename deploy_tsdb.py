#!/usr/bin/env python3
"""Deploy: atualiza home.html no DB e verifica server.py"""
import os, sys

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway')

print("=== DEPLOY TSDB ===")

# Checar home.html
with open('home.html', 'r', encoding='utf-8') as f:
    html = f.read()

print(f"home.html: {len(html)} chars")

checks = [
    ('/api/bet/tabela', 'rota tabela TheSportsDB'),
    ('/api/bet/fixtures', 'rota fixtures TheSportsDB'),
    ('carregarTabela', 'função carregarTabela'),
    ('liga-chip', 'chips de liga'),
    ('tabela-box', 'container tabela'),
    ('carregarTabela', 'função carregarTabela 2'),
]

ok = True
for term, name in checks:
    found = term in html
    print(f"  {'OK' if found else 'FALTA'} {name}: '{term}'")
    if not found:
        ok = False

# Checar server.py
with open('server.py', 'r', encoding='utf-8') as f:
    sv = f.read()

print(f"\nserver.py: {len(sv.splitlines())} linhas")
sv_checks = [
    ('route_tsdb_tabela', 'função route_tsdb_tabela'),
    ('route_tsdb_fixtures', 'função route_tsdb_fixtures'),
    ('TheSportsDB', 'comentário TheSportsDB'),
    ('_TSDB_LEAGUES', 'mapeamento ligas TSDB'),
    ('/api/bet/tabela', 'registro rota /api/bet/tabela'),
    ('lookuptable.php', 'endpoint lookuptable TheSportsDB'),
    ('eventsnextleague', 'endpoint eventos próximos'),
]

for term, name in sv_checks:
    found = term in sv
    print(f"  {'OK' if found else 'FALTA'} {name}")
    if not found:
        ok = False

if not ok:
    print("\nVERIFICACAO FALHOU - abortando deploy")
    sys.exit(1)

print("\n=== Atualizando banco de dados ===")
try:
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("UPDATE configuracoes SET valor=%s WHERE chave='home_html_patch'", (html,))
    rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"DB atualizado: {rows} linha(s) afetada(s), {len(html)} chars")
except Exception as e:
    print(f"ERRO DB: {e}")
    sys.exit(1)

print("Deploy concluido com sucesso!")
