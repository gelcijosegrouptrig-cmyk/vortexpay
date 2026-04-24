#!/usr/bin/env python3
"""Testa TheSportsDB para tabelas e jogos proximos"""
import urllib.request, json

BASE = 'https://www.thesportsdb.com/api/v1/json/3'

ligas_config = [
    ('4351', 'Brasileirão Série A',  '2025'),
    ('4352', 'Brasileirão Série B',  '2025'),
    ('4529', 'Copa Libertadores',    '2025'),
    ('4480', 'UEFA Champions',       '2024-2025'),
    ('4328', 'Premier League',       '2024-2025'),
    ('4335', 'La Liga',              '2024-2025'),
    ('4331', 'Bundesliga',           '2024-2025'),
    ('4332', 'Serie A Italia',       '2024-2025'),
    ('4334', 'Ligue 1',              '2024-2025'),
]

print("=== TheSportsDB Tabelas ===")
for lid, name, season in ligas_config:
    url = f'{BASE}/lookuptable.php?l={lid}&s={season}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            content = r.read()
            if not content:
                print(f"VAZIO  {name} season={season}")
                continue
            d = json.loads(content)
            table = d.get('table', []) or []
            print(f"OK {name}: {len(table)} times season={season}")
            if table:
                t = table[0]
                print(f"   1: {t.get('strTeam')} {t.get('intPoints')} pts")
    except Exception as e:
        print(f"ERR {name}: {e}")

print("\n=== TheSportsDB Proximos Jogos ===")
for lid, name, season in ligas_config[:6]:
    url = f'{BASE}/eventsnextleague.php?id={lid}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
            events = d.get('events', []) or []
            print(f"OK {name}: {len(events)} jogos")
            for ev in events[:2]:
                print(f"  {ev.get('strHomeTeam')} vs {ev.get('strAwayTeam')} [{ev.get('dateEvent')}] {ev.get('strLeague','')[:30]}")
    except Exception as e:
        print(f"ERR {name}: {e}")
