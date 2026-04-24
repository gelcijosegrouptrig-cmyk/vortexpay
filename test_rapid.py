#!/usr/bin/env python3
"""Testa todas as APIs com a chave RapidAPI do usuario"""
import urllib.request, urllib.error, json, time

RAPID_KEY = '101853cbb1mshd61913ed76a55b6p11b79cjsn473796151d52'

def test(host, path, name):
    time.sleep(0.5)
    url = f'https://{host}{path}'
    req = urllib.request.Request(url, headers={
        'X-RapidAPI-Key': RAPID_KEY,
        'X-RapidAPI-Host': host
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            d = json.loads(raw)
            keys = list(d.keys())[:4] if isinstance(d, dict) else f'{len(d)} items'
            print(f"OK {name}: {keys}")
            return d
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')[:120]
        print(f"HTTP{e.code} {name}: {body}")
    except Exception as e:
        print(f"ERR {name}: {e}")
    return None

# APIs para tabelas de futebol
print("=== Testando APIs ===")

# 1. API-Football via RapidAPI (mais popular)
test('api-football-v1.p.rapidapi.com', '/v3/standings?league=71&season=2024', 'api-football standings BR')
test('api-football-v1.p.rapidapi.com', '/v3/leagues', 'api-football leagues')

# 2. The Odds API - Scores (pode ter standings)
test('the-odds-api.p.rapidapi.com', '/v4/sports', 'the-odds-api sports')
test('the-odds-api.p.rapidapi.com', '/v4/scores', 'the-odds-api scores')

# 3. Football Highlights
test('football-highlights-api.p.rapidapi.com', '/highlights', 'football-highlights')

# 4. Soccer
test('soccer-live-data.p.rapidapi.com', '/v2/standings/71/2024', 'soccer-live-data')

# 5. SportsOpenData
test('sportsopen.p.rapidapi.com', '/standings?leagueId=71&season=2024', 'sportsopen')

# 6. AllSports
test('allsportsapi2.p.rapidapi.com', '/football/?met=Leagues', 'allsports-leagues')

print("\n=== Done ===")
