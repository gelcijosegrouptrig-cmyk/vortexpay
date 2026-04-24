#!/usr/bin/env python3
"""Substituir função carregarAoVivo no home.html"""

NEW_FUNC = r"""async function carregarAoVivo(_silent){
  const lista=document.getElementById('live-lista');
  if(!_silent) lista.innerHTML='<div class="loading-center" style="grid-column:1/-1"><div class="spinner2"></div>Buscando jogos ao vivo...</div>';
  try{
    const r=await fetch(BASE+'/api/bet/live?_t='+Date.now());
    const d=await r.json();
    const jogos=d.jogos||[];
    const source=d.source||'';
    const ageS=d.age_seconds||null;
    const cnt=document.getElementById('sb-live-cnt');
    if(cnt){cnt.textContent=jogos.length;cnt.style.display=jogos.length?'inline':'none'}
    if(!jogos.length){
      const aguardando=source==='none'||source==='';
      lista.innerHTML='<div class="loading-center" style="grid-column:1/-1">'+
        '<div style="font-size:36px;margin-bottom:10px">'+(aguardando?'⏳':'🔴')+'</div>'+
        '<div style="color:var(--text);font-size:14px;font-weight:700">'+(aguardando?'Carregando dados ao vivo...':'Nenhum jogo ao vivo agora')+'</div>'+
        '<div style="color:var(--muted);font-size:12px;margin-top:6px">'+(aguardando?'O scraper está sendo iniciado, aguarde ~1 minuto e atualize.':'Os próximos jogos aparecem aqui assim que iniciam')+'</div>'+
        '<button onclick="carregarAoVivo()" style="margin-top:14px;background:var(--blue);border:none;color:#fff;padding:7px 18px;border-radius:8px;cursor:pointer;font-size:12px">↻ Atualizar</button>'+
        '</div>';
      clearTimeout(_liveRefreshTimer);
      _liveRefreshTimer=setTimeout(()=>{if(document.getElementById('view-live')&&document.getElementById('view-live').classList.contains('active'))carregarAoVivo(true);},aguardando?30000:60000);
      return;
    }
    lista.innerHTML='';
    const srcLabel=source==='flashscore'||source==='apify_db'?'⚡ FlashScore':source==='espn'?'📡 ESPN':'📡 Ao Vivo';
    const updateTxt=ageS!=null?(ageS<60?ageS+'s':Math.floor(ageS/60)+'min atrás'):'tempo real';
    const hdr=document.createElement('div');
    hdr.style.cssText='grid-column:1/-1;display:flex;justify-content:space-between;align-items:center;padding:4px 2px 8px;font-size:11px;color:var(--muted)';
    hdr.innerHTML='<span>'+srcLabel+' — '+jogos.length+' jogo(s) ao vivo</span><span>🔄 '+updateTxt+'</span>';
    lista.appendChild(hdr);
    jogos.forEach(j=>{
      const o=j.odds||{};
      const isSoccer=!j.sport||j.sport.startsWith('soccer');
      const temEmpate=isSoccer&&o.draw!=null;
      const hL=j.home_logo||(typeof getTeamLogo==='function'?getTeamLogo(j.home_team):'');
      const aL=j.away_logo||(typeof getTeamLogo==='function'?getTeamLogo(j.away_team):'');
      const card=document.createElement('div');
      card.className='live-card2';
      const clockTxt=j.clock?j.clock:(j.elapsed!=null?(typeof j.elapsed==='number'?j.elapsed+'°':j.elapsed):'● AO VIVO');
      const hGoals=j.home_goals!=null?j.home_goals:(j.score?j.score.split('-')[0].trim():'-');
      const aGoals=j.away_goals!=null?j.away_goals:(j.score?j.score.split('-')[1]?.trim():'-');
      const hBadge=hL?'<img class="t-escudo" src="'+hL+'" alt="" onerror="this.style.display=\'none\'">':'<div class="t-ini">'+teamInitials(j.home_team)+'</div>';
      const aBadge=aL?'<img class="t-escudo" src="'+aL+'" alt="" onerror="this.style.display=\'none\'">':'<div class="t-ini">'+teamInitials(j.away_team)+'</div>';
      const sportCor=((_SPORT_LABELS[j.sport]||{}).cor)||'var(--gold)';
      card.innerHTML=
        '<div class="jc-meta">'+
          '<span class="jc-liga" style="color:'+sportCor+'">'+(j.league||'Ao Vivo')+'</span>'+
          '<span class="live-min" style="background:#ef4444;color:#fff;padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700">● '+clockTxt+'</span>'+
        '</div>'+
        '<div class="jc-times">'+
          '<div class="time-b">'+hBadge+'<span class="t-nome">'+j.home_team+'</span></div>'+
          '<div class="live-score"><span class="ls-n">'+hGoals+'</span><span class="ls-sep">-</span><span class="ls-n">'+aGoals+'</span></div>'+
          '<div class="time-b away">'+aBadge+'<span class="t-nome">'+j.away_team+'</span></div>'+
        '</div>'+
        (j.odds_simulated?'<div style="font-size:9px;color:var(--muted);text-align:center;padding:2px 0 4px">⚡ Odds estimadas</div>':'')+
        '<div class="jc-odds'+(temEmpate?'':' no-draw')+'">'+
        (o.home!=null?'<div class="odd-btn2" onclick="selOdd(\''+j.id+'_l\',\''+j.home_team+'\',\'C\','+o.home+',\''+j.home_team+' × '+j.away_team+'\')"><div class="o-lbl">'+teamShortLabel(j.home_team)+'</div><div class="o-val">'+Number(o.home).toFixed(2)+'</div></div>':'')+
        (temEmpate?'<div class="odd-btn2" onclick="selOdd(\''+j.id+'_l\',\'Empate\',\'X\','+o.draw+',\''+j.home_team+' × '+j.away_team+'\')"><div class="o-lbl">Empate</div><div class="o-val">'+Number(o.draw).toFixed(2)+'</div></div>':'')+
        (o.away!=null?'<div class="odd-btn2" onclick="selOdd(\''+j.id+'_l\',\''+j.away_team+'\',\'F\','+o.away+',\''+j.home_team+' × '+j.away_team+'\')"><div class="o-lbl">'+teamShortLabel(j.away_team)+'</div><div class="o-val">'+Number(o.away).toFixed(2)+'</div></div>':'')+
        '</div>';
      lista.appendChild(card);
    });
    clearTimeout(_liveRefreshTimer);
    _liveRefreshTimer=setTimeout(()=>{
      const liveView=document.getElementById('view-live');
      if(liveView&&liveView.classList.contains('active'))carregarAoVivo(true);
    },60000);
  }catch(e){
    lista.innerHTML='<div class="loading-center" style="grid-column:1/-1">⚠️ Erro ao carregar.<button onclick="carregarAoVivo()" style="background:var(--blue);border:none;color:#fff;padding:6px 14px;border-radius:8px;cursor:pointer;margin-left:8px;font-size:12px">Tentar novamente</button></div>';
  }
}
"""

with open('home.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Linha 1607 = índice 1606 (início), linha 1673 = índice 1672 (fim, inclusive)
start = 1606  # índice 0-based da primeira linha da função
end   = 1673  # índice 0-based da última linha (o '}' da função)

new_lines = lines[:start] + [NEW_FUNC + '\n'] + lines[end+1:]

with open('home.html', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"✅ Substituído: linhas {start+1}-{end+1} ({end-start+1} linhas) por nova função")
print(f"   Novo total de linhas: {len(new_lines)}")
