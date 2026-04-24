#!/usr/bin/env python3
"""Valida producao apos deploy"""
import urllib.request, json, time

BASE = 'https://paynexbet.com'

print("=== Validacao Producao ===\n")

# 1. Verificar home page
try:
    req = urllib.request.Request(BASE + '/', headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        html = r.read().decode('utf-8', errors='replace')
        size = len(html)
        checks = ['api/bet/tabela', 'api/bet/fixtures', 'Odds em breve', 'carregarTabela', 'venue']
        print(f"Home page: {size} chars")
        for c in checks:
            print(f"  {'OK' if c in html else 'FAIL'}  {c}")
except Exception as e:
    print(f"Home page ERRO: {e}")

time.sleep(1)

# 2. Testar nova rota /api/bet/tabela (Brasileirão)
print()
try:
    req = urllib.request.Request(BASE + '/api/bet/tabela?league=71', headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=12) as r:
        d = json.loads(r.read())
        teams = d.get('standings', [])
        print(f"Tabela Brasileirao: success={d.get('success')}, times={len(teams)}")
        if teams:
            t0 = teams[0]
            print(f"  1o: {t0.get('name')} - {t0.get('points')} pts (logo={'sim' if t0.get('logo') else 'nao'})")
except Exception as e:
    print(f"Tabela Brasileirao ERRO: {e}")

time.sleep(1)

# 3. Testar /api/bet/fixtures (Brasileirão)
print()
try:
    req = urllib.request.Request(BASE + '/api/bet/fixtures?sport=soccer_brazil_campeonato',
                                 headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=12) as r:
        d = json.loads(r.read())
        jogos = d.get('jogos', [])
        print(f"Fixtures Brasileirao: success={d.get('success')}, jogos={len(jogos)}")
        for j in jogos[:3]:
            print(f"  {j.get('home_team')} x {j.get('away_team')} [{j.get('commence_time','')[:10]}]")
except Exception as e:
    print(f"Fixtures Brasileirao ERRO: {e}")

time.sleep(1)

# 4. Testar Champions
print()
try:
    req = urllib.request.Request(BASE + '/api/bet/tabela?league=2', headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=12) as r:
        d = json.loads(r.read())
        teams = d.get('standings', [])
        print(f"Tabela Champions: success={d.get('success')}, times={len(teams)}")
        if teams:
            print(f"  1o: {teams[0].get('name')} - {teams[0].get('points')} pts")
except Exception as e:
    print(f"Tabela Champions ERRO: {e}")

print("\n=== Fim ===")
