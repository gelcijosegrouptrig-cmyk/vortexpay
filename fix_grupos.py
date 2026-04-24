#!/usr/bin/env python3
"""
Corrige backend (grupos Copa) e frontend (tabela por grupos + botão apostar).
"""

# ── 1. BACKEND: adicionar suporte a grupos ─────────────────────────────────
content_py = open('server.py', 'r', encoding='utf-8').read()

OLD_STANDINGS = """        rows = []
        rank = 1
        children = d.get('children', [])
        for grp in children:
            entries = grp.get('standings', {}).get('entries', [])
            for entry in entries:
                team = entry.get('team', {})
                stats_list = entry.get('stats', [])
                stats = {s['name']: s.get('value', 0) for s in stats_list}

                logo = team.get('logo', '')
                if not logo and team.get('logos'):
                    logo = team['logos'][0].get('href', '')

                gf = int(stats.get('pointsFor', 0) or 0)
                ga = int(stats.get('pointsAgainst', 0) or 0)
                rows.append({
                    'rank':    rank,
                    'name':    team.get('displayName', team.get('name', '')),
                    'logo':    logo,
                    'points':  int(stats.get('points', 0) or 0),
                    'played':  int(stats.get('gamesPlayed', 0) or 0),
                    'won':     int(stats.get('wins', 0) or 0),
                    'draw':    int(stats.get('ties', 0) or 0),
                    'lost':    int(stats.get('losses', 0) or 0),
                    'gf':      gf,
                    'ga':      ga,
                    'gd':      gf - ga,
                    'form':    '',
                    'description': '',
                })
                rank += 1

        _espn_table_cache[cache_key] = {'ts': agora, 'data': rows}
        return web.json_response({'success': True, 'standings': rows, 'league': league_name})"""

NEW_STANDINGS = """        children = d.get('children', [])
        multi_group = len(children) > 1  # Copa tem múltiplos grupos

        grupos = []
        all_rows = []

        for grp in children:
            grp_name = grp.get('name', '')
            entries = grp.get('standings', {}).get('entries', [])
            grp_rows = []
            rank = 1
            for entry in entries:
                team = entry.get('team', {})
                stats_list = entry.get('stats', [])
                stats = {s['name']: s.get('value', 0) for s in stats_list}

                logo = team.get('logo', '')
                if not logo and team.get('logos'):
                    logo = team['logos'][0].get('href', '')

                gf = int(stats.get('pointsFor', 0) or 0)
                ga = int(stats.get('pointsAgainst', 0) or 0)
                row = {
                    'rank':    rank,
                    'name':    team.get('displayName', team.get('name', '')),
                    'logo':    logo,
                    'points':  int(stats.get('points', 0) or 0),
                    'played':  int(stats.get('gamesPlayed', 0) or 0),
                    'won':     int(stats.get('wins', 0) or 0),
                    'draw':    int(stats.get('ties', 0) or 0),
                    'lost':    int(stats.get('losses', 0) or 0),
                    'gf':      gf,
                    'ga':      ga,
                    'gd':      gf - ga,
                    'form':    '',
                    'description': '',
                }
                grp_rows.append(row)
                all_rows.append(row)
                rank += 1

            if grp_rows:
                grupos.append({'name': grp_name, 'rows': grp_rows})

        result_data = {
            'success': True,
            'standings': all_rows,
            'league': league_name,
            'grupos': grupos if multi_group else [],
            'has_groups': multi_group,
        }
        _espn_table_cache[cache_key] = {'ts': agora, 'data': result_data}
        return web.json_response(result_data)"""

if OLD_STANDINGS in content_py:
    content_py = content_py.replace(OLD_STANDINGS, NEW_STANDINGS, 1)
    # Também corrigir o from_cache para retornar o dict completo
    OLD_CACHE = """    cached = _espn_table_cache.get(cache_key)
    if cached and agora - cached['ts'] < _ESPN_TTL:
        return web.json_response({'success': True, 'standings': cached['data'],
                                  'league': league_name, 'from_cache': True})"""
    NEW_CACHE = """    cached = _espn_table_cache.get(cache_key)
    if cached and agora - cached['ts'] < _ESPN_TTL:
        resp = dict(cached['data']) if isinstance(cached['data'], dict) else {'success': True, 'standings': cached['data'], 'league': league_name}
        resp['from_cache'] = True
        return web.json_response(resp)"""
    if OLD_CACHE in content_py:
        content_py = content_py.replace(OLD_CACHE, NEW_CACHE, 1)
        print('✅ Cache corrigido')
    open('server.py', 'w', encoding='utf-8').write(content_py)
    print(f'✅ Backend atualizado: {len(content_py)} chars')
else:
    print('❌ OLD_STANDINGS não encontrado no server.py')

# ── 2. FRONTEND: tabela por grupos + botão apostar ─────────────────────────
content_html = open('home.html', 'r', encoding='utf-8').read()

OLD_TABLE_JS = """    const rows=d.standings;
    if(tit)tit.textContent=(d.league||titulo||'Tabela');
    box.innerHTML='<div class=\"tabela-wrap\">'+
      '<div class=\"t-head\"><div>#</div><div>Time</div>'+
      '<div style=\"text-align:center\" title=\"Jogos\">J</div>'+
      '<div style=\"text-align:center\" title=\"Vitórias\">V</div>'+
      '<div style=\"text-align:center\" title=\"Empates\">E</div>'+
      '<div style=\"text-align:center\" title=\"Derrotas\">D</div>'+
      '<div style=\"text-align:center\" title=\"Saldo de Gols\">SG</div>'+
      '<div style=\"text-align:center\" title=\"Pontos\"><b>Pts</b></div>'+
      '</div>'+
      rows.map((t,i)=>{
        const rCls=t.rank===1?'p1':t.rank===2?'p2':t.rank===3?'p3':'';
        // Zonas de classificação inteligente por tamanho da liga
        let zc='';
        const n=rows.length;
        if(n>=20){
          // Liga grande (20 times): 4 Copa, 2 Europa, 4 rebaixados
          if(t.rank<=4) zc='z-cl';
          else if(t.rank<=6) zc='z-el';
          else if(t.rank>n-4) zc='z-rl';
        }else if(n>=16){
          // Liga média (16-19 times): 4 Copa, 2 Europa, 3 rebaixados
          if(t.rank<=4) zc='z-cl';
          else if(t.rank<=6) zc='z-el';
          else if(t.rank>n-3) zc='z-rl';
        }else if(n>=8){
          // Copa/Libertadores com grupos: top 2 avançam, 3-4 repescagem
          if(t.rank<=2) zc='z-cl';
          else if(t.rank<=4) zc='z-el';
        }
        const lo=t.logo?'<img class=\"tl\" src=\"'+t.logo+'\" alt=\"\" loading=\"lazy\" onerror=\"this.style.display=\\'none\\'\">':'';
        const sgTxt=(t.gd>=0?'+':'')+t.gd;
        return'<div class=\"t-row '+zc+'\">'+
          '<div class=\"c-rank '+rCls+'\">'+t.rank+'</div>'+
          '<div class=\"c-team\">'+lo+'<span class=\"tn\">'+t.name+'</span></div>'+
          '<div class=\"c-n\">'+t.played+'</div>'+
          '<div class=\"c-n\">'+t.won+'</div>'+
          '<div class=\"c-n\">'+t.draw+'</div>'+
          '<div class=\"c-n\">'+t.lost+'</div>'+
          '<div class=\"c-n\">'+sgTxt+'</div>'+
          '<div class=\"c-pts\">'+t.points+'</div>'+
          '</div>';
      }).join('')+'</div>'+
      '<div style=\"padding:8px 12px;font-size:10px;color:var(--muted);display:flex;gap:14px;flex-wrap:wrap\">'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#3b82f6;margin-right:4px\"></span>Classificação para Copa</span>'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#f59e0b;margin-right:4px\"></span>Zona de Perigo</span>'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#ef4444;margin-right:4px\"></span>Rebaixamento</span>'+
      '</div>';"""

NEW_TABLE_JS = """    if(tit)tit.textContent=(d.league||titulo||'Tabela');
    // Função helper para renderizar uma tabela de grupo
    function renderGrpTable(rows, isGroup){
      const n=rows.length;
      return '<div class=\"tabela-wrap\">'+
        '<div class=\"t-head\"><div>#</div><div>Time</div>'+
        '<div style=\"text-align:center\" title=\"Jogos\">J</div>'+
        '<div style=\"text-align:center\" title=\"Vitórias\">V</div>'+
        '<div style=\"text-align:center\" title=\"Empates\">E</div>'+
        '<div style=\"text-align:center\" title=\"Derrotas\">D</div>'+
        '<div style=\"text-align:center\" title=\"Saldo de Gols\">SG</div>'+
        '<div style=\"text-align:center\" title=\"Pontos\"><b>Pts</b></div>'+
        '</div>'+
        rows.map((t)=>{
          const rCls=t.rank===1?'p1':t.rank===2?'p2':t.rank===3?'p3':'';
          let zc='';
          if(isGroup){
            // Grupos de Copa: top 2 avançam, 3o repescagem
            if(t.rank<=2) zc='z-cl';
            else if(t.rank===3) zc='z-el';
          }else if(n>=20){
            if(t.rank<=4) zc='z-cl';
            else if(t.rank<=6) zc='z-el';
            else if(t.rank>n-4) zc='z-rl';
          }else if(n>=16){
            if(t.rank<=4) zc='z-cl';
            else if(t.rank<=6) zc='z-el';
            else if(t.rank>n-3) zc='z-rl';
          }
          const lo=t.logo?'<img class=\"tl\" src=\"'+t.logo+'\" alt=\"\" loading=\"lazy\" onerror=\"this.style.display=\\'none\\'\">':'';
          const sgTxt=(t.gd>=0?'+':'')+t.gd;
          return'<div class=\"t-row '+zc+'\">'+
            '<div class=\"c-rank '+rCls+'\">'+t.rank+'</div>'+
            '<div class=\"c-team\">'+lo+'<span class=\"tn\">'+t.name+'</span></div>'+
            '<div class=\"c-n\">'+t.played+'</div>'+
            '<div class=\"c-n\">'+t.won+'</div>'+
            '<div class=\"c-n\">'+t.draw+'</div>'+
            '<div class=\"c-n\">'+t.lost+'</div>'+
            '<div class=\"c-n\">'+sgTxt+'</div>'+
            '<div class=\"c-pts\">'+t.points+'</div>'+
            '</div>';
        }).join('')+'</div>';
    }

    let html='';
    if(d.has_groups && d.grupos && d.grupos.length>1){
      // Exibir grupos separados (Libertadores, Sul-Americana)
      html+='<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:10px\">';
      d.grupos.forEach(g=>{
        html+='<div>';
        html+='<div style=\"font-size:11px;font-weight:700;color:var(--blue2);padding:4px 2px;margin-bottom:4px\">'+g.name+'</div>';
        html+=renderGrpTable(g.rows, true);
        html+='</div>';
      });
      html+='</div>';
      html+='<div style=\"padding:8px 12px;font-size:10px;color:var(--muted);display:flex;gap:14px;flex-wrap:wrap;margin-top:6px\">'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#3b82f6;margin-right:4px\"></span>Avança</span>'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#f59e0b;margin-right:4px\"></span>Repescagem</span>'+
        '</div>';
    }else{
      // Liga normal — tabela única
      const rows=d.standings;
      html+=renderGrpTable(rows, false);
      html+='<div style=\"padding:8px 12px;font-size:10px;color:var(--muted);display:flex;gap:14px;flex-wrap:wrap\">'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#3b82f6;margin-right:4px\"></span>Classificação para Copa</span>'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#f59e0b;margin-right:4px\"></span>Zona de Perigo</span>'+
        '<span><span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#ef4444;margin-right:4px\"></span>Rebaixamento</span>'+
        '</div>';
    }
    box.innerHTML=html;"""

if OLD_TABLE_JS in content_html:
    content_html = content_html.replace(OLD_TABLE_JS, NEW_TABLE_JS, 1)
    open('home.html', 'w', encoding='utf-8').write(content_html)
    print(f'✅ Frontend atualizado: {len(content_html)} chars')
else:
    print('❌ OLD_TABLE_JS não encontrado')
    # debug: localizar fragmento
    idx = content_html.find("rows=d.standings")
    print(f'  rows=d.standings idx={idx}')
    idx2 = content_html.find("renderGrpTable")
    print(f'  renderGrpTable já existe: {idx2}')
