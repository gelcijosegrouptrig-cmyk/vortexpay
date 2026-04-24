#!/usr/bin/env python3
"""
Substitui rotas TheSportsDB por ESPN API (gratuita, sem chave, 20 times por liga).
"""
import re

content = open('server.py', 'r', encoding='utf-8').read()

# ─── Novo bloco ESPN ──────────────────────────────────────────────────────────
NEW_BLOCK = '''# ── ESPN API (gratuita, sem chave, 20 times por liga) ────────────────────────
_ESPN_BASE_SITE  = 'https://site.api.espn.com/apis/site/v2/sports/soccer'
_ESPN_BASE_STAND = 'https://site.api.espn.com/apis/v2/sports/soccer'

# Mapeamento: chave frontend → (espn_slug, nome_exibição)
_ESPN_LEAGUES = {
    '71':  ('bra.1',                 'Brasileirão Série A'),
    '75':  ('bra.2',                 'Brasileirão Série B'),
    '13':  ('CONMEBOL.LIBERTADORES', 'Copa Libertadores'),
    '11':  ('CONMEBOL.SULAMERICANA', 'Copa Sul-Americana'),
    '2':   ('UEFA.CHAMPIONS',        'UEFA Champions League'),
    '39':  ('eng.1',                 'Premier League'),
    '140': ('esp.1',                 'La Liga'),
    '135': ('ita.1',                 'Serie A (Itália)'),
    '78':  ('ger.1',                 'Bundesliga'),
    '61':  ('fra.1',                 'Ligue 1'),
}

# Sport fixtures map: sport_key → espn_slug
_ESPN_FIXTURES = {
    'soccer_brazil_campeonato':          'bra.1',
    'soccer_brazil_serie_b':             'bra.2',
    'soccer_conmebol_copa_libertadores': 'CONMEBOL.LIBERTADORES',
    'soccer_conmebol_sulamericana':      'CONMEBOL.SULAMERICANA',
    'soccer_uefa_champs_league':         'UEFA.CHAMPIONS',
    'soccer_epl':                        'eng.1',
    'soccer_spain_la_liga':              'esp.1',
    'soccer_italy_serie_a':              'ita.1',
    'soccer_germany_bundesliga':         'ger.1',
    'soccer_france_ligue_one':           'fra.1',
}

_espn_table_cache = {}   # key=slug → {'ts':..., 'data':[]}
_espn_fix_cache   = {}   # key=slug → {'ts':..., 'data':[]}
_ESPN_TTL         = 1800  # 30 min


async def route_tsdb_tabela(request):
    """GET /api/bet/tabela?league=71 — tabela via ESPN API (gratuita, 20 times)"""
    import time as _time, aiohttp, json as _json
    agora = _time.time()
    league_param = request.rel_url.query.get('league', '71')

    cfg = _ESPN_LEAGUES.get(league_param)
    if not cfg:
        return web.json_response({'success': False, 'standings': [],
                                  'error': f'Liga {league_param} não mapeada'})

    slug, league_name = cfg
    cached = _espn_table_cache.get(slug)
    if cached and agora - cached['ts'] < _ESPN_TTL:
        return web.json_response({'success': True, 'standings': cached['data'],
                                  'league': league_name, 'from_cache': True})

    url = f'{_ESPN_BASE_STAND}/{slug}/standings'
    try:
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                raw = await r.read()
                d = _json.loads(raw)

        children = d.get('children', [])
        rows = []
        rank = 1
        for grp in children:
            entries = grp.get('standings', {}).get('entries', [])
            for e in entries:
                team = e.get('team', {})
                stats_list = e.get('stats', [])
                stats = {s['name']: s.get('value', 0) for s in stats_list}
                gf = int(stats.get('pointsFor', 0))
                ga = int(stats.get('pointsAgainst', 0))
                rows.append({
                    'rank':    rank,
                    'name':    team.get('displayName', ''),
                    'logo':    team.get('logo', ''),
                    'points':  int(stats.get('points', 0)),
                    'played':  int(stats.get('gamesPlayed', 0)),
                    'won':     int(stats.get('wins', 0)),
                    'draw':    int(stats.get('ties', 0)),
                    'lost':    int(stats.get('losses', 0)),
                    'gf':      gf,
                    'ga':      ga,
                    'gd':      gf - ga,
                    'form':    '',
                    'description': '',
                    'color':   '#' + (team.get('color') or '666666'),
                })
                rank += 1

        _espn_table_cache[slug] = {'ts': agora, 'data': rows}
        return web.json_response({'success': True, 'standings': rows, 'league': league_name})

    except Exception as e:
        print(f'[espn/tabela] err slug={slug}: {e}', flush=True)
        return web.json_response({'success': False, 'standings': [], 'error': str(e)})


async def route_tsdb_fixtures(request):
    """GET /api/bet/fixtures?sport=soccer_brazil_campeonato — jogos via ESPN API"""
    import time as _time, aiohttp, json as _json
    agora = _time.time()
    sport = request.rel_url.query.get('sport', '').strip()

    # Determinar quais slugs buscar
    if sport and sport in _ESPN_FIXTURES:
        slugs_to_fetch = [(sport, _ESPN_FIXTURES[sport])]
    elif not sport or sport in ('upcoming', 'all', 'destaque'):
        slugs_to_fetch = list(_ESPN_FIXTURES.items())
    else:
        return web.json_response({'success': True, 'jogos': [], 'total': 0})

    # Nome da liga para exibição
    _SPORT_LEAGUE_NAMES = {v: k2 for k2, (v, _) in _ESPN_LEAGUES.items()
                           if v in _ESPN_FIXTURES.values()}
    _SPORT_TO_NAME = {
        'bra.1':                 'Brasileirão Série A',
        'bra.2':                 'Brasileirão Série B',
        'CONMEBOL.LIBERTADORES': 'Copa Libertadores',
        'CONMEBOL.SULAMERICANA': 'Copa Sul-Americana',
        'UEFA.CHAMPIONS':        'Champions League',
        'eng.1':                 'Premier League',
        'esp.1':                 'La Liga',
        'ita.1':                 'Serie A',
        'ger.1':                 'Bundesliga',
        'fra.1':                 'Ligue 1',
    }

    fixtures_result = []

    async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
        for sp_key, slug in slugs_to_fetch:
            cached = _espn_fix_cache.get(slug)
            if cached and agora - cached['ts'] < _ESPN_TTL:
                fixtures_result.extend(cached['data'])
                continue

            url = f'{_ESPN_BASE_SITE}/{slug}/scoreboard'
            try:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    raw = await r.read()
                    d = _json.loads(raw)

                events = d.get('events', []) or []
                league_name = _SPORT_TO_NAME.get(slug, slug)
                jogos = []
                for ev in events:
                    comps = ev.get('competitions', [{}])[0]
                    teams = comps.get('competitors', [])
                    h = next((t for t in teams if t.get('homeAway') == 'home'), teams[0] if teams else {})
                    a = next((t for t in teams if t.get('homeAway') == 'away'), teams[1] if len(teams) > 1 else {})
                    h_team = h.get('team', {})
                    a_team = a.get('team', {})
                    status_type = comps.get('status', {}).get('type', {})
                    status_name = status_type.get('name', '')
                    status_desc = status_type.get('description', '')
                    venue_name  = comps.get('venue', {}).get('fullName', '')
                    h_score = h.get('score', '') or ''
                    a_score = a.get('score', '') or ''
                    score = f'{h_score} - {a_score}' if (h_score or a_score) else ''

                    jogos.append({
                        'id':            ev.get('id', '') or f"espn_{slug}_{ev.get('uid','')}",
                        'sport':         sp_key,
                        'league':        league_name,
                        'home_team':     h_team.get('displayName', ''),
                        'away_team':     a_team.get('displayName', ''),
                        'home_logo':     h_team.get('logo', ''),
                        'away_logo':     a_team.get('logo', ''),
                        'commence_time': ev.get('date', ''),
                        'venue':         venue_name,
                        'round':         '',
                        'status':        status_desc,
                        'status_code':   status_name,
                        'score':         score,
                        'odds': {'home': None, 'draw': None, 'away': None},
                        'source': 'espn',
                    })

                _espn_fix_cache[slug] = {'ts': agora, 'data': jogos}
                fixtures_result.extend(jogos)

            except Exception as e:
                print(f'[espn/fixtures] err slug={slug}: {e}', flush=True)

    return web.json_response({'success': True, 'jogos': fixtures_result, 'total': len(fixtures_result)})

'''

# ─── Localizar o bloco antigo ─────────────────────────────────────────────────
start_marker = '# ── TheSportsDB (gratuito, sem key) ──────────────────────────────────────────'
end_marker = '\nasync def route_bet_standings('

idx_start = content.find(start_marker)
idx_end   = content.find(end_marker, idx_start)

if idx_start == -1 or idx_end == -1:
    print(f'❌ Marcadores não encontrados: start={idx_start} end={idx_end}')
    exit(1)

print(f'✅ Bloco localizado: {idx_start} → {idx_end} ({idx_end - idx_start} chars)')

# Substituir
new_content = content[:idx_start] + NEW_BLOCK + content[idx_end:]
open('server.py', 'w', encoding='utf-8').write(new_content)
print(f'✅ server.py reescrito: {len(new_content)} chars')
print(f'   Bloco anterior: {idx_end - idx_start} chars → Novo: {len(NEW_BLOCK)} chars')
