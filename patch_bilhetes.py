#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Substitui a função carregarBilhetes no home.html pela versão com filtro por data.
"""

NEW_FUNC = r"""/* ── estado global do filtro de data ── */
let _bilhetesCache={apostas:[],sorteio:[]};
let _filtroAtivo=null;

function _parseDateInput(val){
  if(!val) return null;
  const [y,m,d]=val.split('-').map(Number);
  return new Date(y,m-1,d);
}

function _dataItemEntreFiltro(dataStr){
  if(!_filtroAtivo) return true;
  if(!dataStr) return true;
  let dt;
  if(dataStr.includes('T')||dataStr.match(/^\d{4}-/)){
    dt=new Date(dataStr);
  } else if(dataStr.includes('/')){
    const parts=dataStr.split('/');
    if(parts.length===3) dt=new Date(+parts[2],+parts[1]-1,+parts[0]);
    else return true;
  } else {
    return true;
  }
  if(isNaN(dt.getTime())) return true;
  const {inicio,fim}=_filtroAtivo;
  if(inicio && dt < inicio) return false;
  if(fim){
    const fimDia=new Date(fim); fimDia.setHours(23,59,59,999);
    if(dt > fimDia) return false;
  }
  return true;
}

function _renderBilhetes(){
  const el=document.getElementById('bilhetes-lista');
  if(!el) return;
  const {apostas,sorteio}=_bilhetesCache;

  const apostasFilt=apostas.filter(a=>_dataItemEntreFiltro(a.criado_em||a.data||''));
  const sorteioFilt=_filtroAtivo
    ? sorteio.filter(b=>_dataItemEntreFiltro(b.criado_em||''))
    : sorteio;

  const btnLimpar=document.getElementById('btn-limpar-filtro');
  if(btnLimpar) btnLimpar.style.display=_filtroAtivo?'inline-block':'none';

  const stIcon=s=>s==='ganhou'?'\u{1F3C6}':s==='perdeu'?'\u274C':s==='cancelada'?'\u{1F6AB}':'\u23F3';
  const stColor=s=>s==='ganhou'?'#4ade80':s==='perdeu'?'#f87171':s==='cancelada'?'#94a3b8':'#f59e0b';
  const stLabel=s=>s==='ganhou'?'\u{1F3C6} Ganhou':s==='perdeu'?'\u274C Perdeu':s==='cancelada'?'\u{1F6AB} Cancelada':'\u23F3 Aguard. jogo';

  let html='';

  // ── Apostas esportivas ──
  if(apostasFilt.length){
    const pendentes=apostasFilt.filter(a=>(a.status||'pendente')==='pendente');
    html+='<div class="conta-card" style="margin-bottom:14px">'+
      '<div class="cc-title"><span class="ico">\u26BD</span>Apostas Esportivas ('+apostasFilt.length+
        (_filtroAtivo&&apostas.length!==apostasFilt.length?' / '+apostas.length+' total':'')+
      ')</div>'+
      (pendentes.length?'<div style="background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.25);border-radius:8px;padding:8px 12px;margin:8px 0;font-size:11px;color:#f59e0b">'+
        '\u23F3 <b>'+pendentes.length+' aposta(s) pendente(s)</b> \u2014 aguardando resultado do jogo.</div>':'')+
      apostasFilt.map(a=>{
        const st=a.status||'pendente';
        const dataExib=a.criado_em
          ? new Date(a.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'})
          : (a.data||'');
        return '<div style="border-bottom:1px solid rgba(255,255,255,0.06);padding:12px 0;display:flex;justify-content:space-between;align-items:flex-start">'+
          '<div style="flex:1;min-width:0">'+
            '<div style="font-size:13px;font-weight:700;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+a.jogo+'</div>'+
            '<div style="font-size:11px;color:#94a3b8;margin-top:3px">'+
              (a.selecao?'<span style="color:#60a5fa">'+a.selecao+'</span> \u00b7 ':'')+
              'Odd <b style="color:#e2e8f0">'+Number(a.odd).toFixed(2)+'</b>'+
            '</div>'+
            '<div style="font-size:10px;color:#475569;margin-top:2px">\u{1F4C5} '+dataExib+'</div>'+
            (st==='pendente'?'<div style="font-size:10px;color:#64748b;margin-top:2px">\u{1F504} Resolu\u00e7\u00e3o autom\u00e1tica ap\u00f3s o jogo</div>':'')+
          '</div>'+
          '<div style="text-align:right;margin-left:12px;flex-shrink:0">'+
            '<div style="font-size:11px;color:'+stColor(st)+';font-weight:700;white-space:nowrap">'+stLabel(st)+'</div>'+
            '<div style="font-size:13px;color:#e2e8f0;font-weight:700">'+_fmt(a.valor)+'</div>'+
            '<div style="font-size:11px;color:#94a3b8">retorno '+_fmt(a.retorno)+'</div>'+
          '</div>'+
        '</div>';
      }).join('')+
    '</div>';
  } else {
    const temFiltro=_filtroAtivo&&apostas.length>0;
    html+='<div class="conta-card" style="text-align:center;padding:20px;margin-bottom:14px">'+
      '<div style="font-size:2rem;margin-bottom:8px">'+(temFiltro?'\u{1F50D}':'\u26BD')+'</div>'+
      '<div style="font-weight:700;color:#94a3b8">'+(temFiltro?'Nenhuma aposta no per\u00edodo selecionado.':'Nenhuma aposta esportiva ainda.')+'</div>'+
      (temFiltro?'<button onclick="limparFiltroData()" style="margin-top:10px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:7px 16px;border-radius:8px;cursor:pointer;font-size:12px">\u{1F504} Mostrar todas</button>':
        '<button onclick="navTo(\'home\')" style="margin-top:10px;background:var(--blue);border:none;color:#fff;padding:8px 18px;border-radius:8px;cursor:pointer;font-size:13px">\u{1F3E0} Ver Jogos</button>')+
    '</div>';
  }

  // ── Bilhetes do sorteio ──
  if(sorteioFilt.length){
    const numCols=Math.min(4,sorteioFilt.length);
    html+='<div class="conta-card">'+
      '<div class="cc-title"><span class="ico">\u{1F3AB}</span>Sorteio \u2014 '+sorteioFilt.length+' bilhete(s)'+
        (_filtroAtivo&&sorteio.length!==sorteioFilt.length?' <span style="font-size:10px;color:#64748b;font-weight:400">de '+sorteio.length+' total</span>':'')+
      '</div>'+
      '<div style="display:grid;grid-template-columns:repeat('+numCols+',1fr);gap:6px;margin:10px 0">'+
        sorteioFilt.map(b=>{
          const dtB=b.criado_em?new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit'}):'';
          return '<div style="background:rgba(96,165,250,0.1);border:1px solid rgba(96,165,250,0.25);border-radius:8px;padding:8px 4px;text-align:center">'+
            '<div style="font-size:16px;font-weight:700;color:#60a5fa;letter-spacing:1px">'+(b.numero||'\u2014')+'</div>'+
            (dtB?'<div style="font-size:9px;color:#475569;margin-top:1px">'+dtB+'</div>':'<div style="font-size:9px;color:#64748b;margin-top:1px">Bilhete</div>')+
          '</div>';
        }).join('')+
      '</div>'+
      '<div style="text-align:right;margin-top:4px">'+
        '<button onclick="sbNav(\'sorteio\')" style="background:none;border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:5px 12px;border-radius:8px;cursor:pointer;font-size:12px">\u2795 Mais bilhetes</button>'+
      '</div>'+
    '</div>';
  } else {
    const temFiltroS=_filtroAtivo&&sorteio.length>0;
    html+='<div class="conta-card" style="text-align:center;padding:16px">'+
      '<div style="font-size:1.6rem;margin-bottom:6px">'+(temFiltroS?'\u{1F50D}':'\u{1F3AB}')+'</div>'+
      '<div style="font-size:13px;color:#94a3b8">'+(temFiltroS?'Nenhum bilhete no per\u00edodo selecionado.':'Sem bilhetes de sorteio ainda.')+'</div>'+
      (temFiltroS?'<button onclick="limparFiltroData()" style="margin-top:8px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:6px 14px;border-radius:8px;cursor:pointer;font-size:12px">\u{1F504} Mostrar todos</button>':
        '<button onclick="sbNav(\'sorteio\')" style="margin-top:8px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:6px 14px;border-radius:8px;cursor:pointer;font-size:12px">\u{1F3AF} Participar do Sorteio</button>')+
    '</div>';
  }

  el.innerHTML=html;
}

function aplicarFiltroData(){
  const ini=document.getElementById('filtro-data-inicio').value;
  const fim=document.getElementById('filtro-data-fim').value;
  if(!ini&&!fim){ _filtroAtivo=null; }
  else { _filtroAtivo={inicio:_parseDateInput(ini)||null,fim:_parseDateInput(fim)||null}; }
  document.querySelectorAll('.btn-filtro-rapido').forEach(b=>b.classList.remove('ativo'));
  const btnLimpar=document.getElementById('btn-limpar-filtro');
  if(btnLimpar) btnLimpar.style.display=_filtroAtivo?'inline-block':'none';
  _renderBilhetes();
}

function limparFiltroData(){
  _filtroAtivo=null;
  const di=document.getElementById('filtro-data-inicio');
  const df=document.getElementById('filtro-data-fim');
  if(di) di.value='';
  if(df) df.value='';
  document.querySelectorAll('.btn-filtro-rapido').forEach(b=>b.classList.remove('ativo'));
  const btnLimpar=document.getElementById('btn-limpar-filtro');
  if(btnLimpar) btnLimpar.style.display='none';
  _renderBilhetes();
}

function filtroRapido(tipo){
  const hoje=new Date(); hoje.setHours(0,0,0,0);
  const fimHoje=new Date(); fimHoje.setHours(23,59,59,999);
  const toIsoDate=d=>`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  document.querySelectorAll('.btn-filtro-rapido').forEach(b=>b.classList.remove('ativo'));
  if(tipo==='todos'){ limparFiltroData(); return; }
  let inicio=new Date(hoje);
  if(tipo==='semana') inicio.setDate(inicio.getDate()-6);
  else if(tipo==='mes') inicio.setDate(inicio.getDate()-29);
  const di=document.getElementById('filtro-data-inicio');
  const df=document.getElementById('filtro-data-fim');
  if(di) di.value=toIsoDate(inicio);
  if(df) df.value=toIsoDate(fimHoje);
  _filtroAtivo={inicio,fim:fimHoje};
  const btnLimpar=document.getElementById('btn-limpar-filtro');
  if(btnLimpar) btnLimpar.style.display='inline-block';
  _renderBilhetes();
}

async function carregarBilhetes(){
  const el=document.getElementById('bilhetes-lista');
  const u=loadSession();
  if(!u){
    el.innerHTML='<div class="loading-center"><a onclick="openModal(\'login\')" style="color:var(--blue2);cursor:pointer">Fa\u00e7a login</a> para ver suas apostas.</div>';
    return;
  }
  el.innerHTML='<div class="loading-center"><div class="spinner2"></div>Carregando...</div>';
  try{
    const r=await fetch(BASE+'/api/bet/saldo/'+u.id);
    const d=await r.json();
    _bilhetesCache.apostas=(d.apostas||[]);

    _bilhetesCache.sorteio=[];
    if(u.cpf){
      try{
        const rs=await fetch(BASE+'/api/sorteio/meus-bilhetes?cpf='+u.cpf.replace(/\D/g,''));
        const ds=await rs.json();
        if(ds.success) _bilhetesCache.sorteio=ds.bilhetes||[];
      }catch(_){}
    }

    _renderBilhetes();
  }catch(e){
    el.innerHTML='<div class="loading-center">\u26a0\ufe0f Erro ao carregar. Tente novamente.</div>';
  }
}
"""

def main():
    with open('home.html', 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.split('\n')

    # Encontrar linha de início (carregarBilhetes) e fim (carregarGanhadores)
    start_line = None
    end_line = None
    for i, line in enumerate(lines):
        if line.strip().startswith('async function carregarBilhetes(){'):
            start_line = i
        if start_line is not None and line.strip().startswith('async function carregarGanhadores(){'):
            end_line = i
            break

    if start_line is None or end_line is None:
        print(f"ERRO: não encontrou marcadores. start={start_line}, end={end_line}")
        return

    print(f"Substituindo linhas {start_line+1} a {end_line} ({end_line - start_line} linhas)")

    # Substituir
    lines[start_line:end_line] = [NEW_FUNC]

    with open('home.html', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"OK! Total de linhas agora: {len(lines)}")

if __name__ == '__main__':
    main()
