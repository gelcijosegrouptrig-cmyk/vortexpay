#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: apostas esportivas ocultas por padrão, igual ao sorteio.
Só aparecem ao clicar em 'Pesquisar'.
"""

OLD_APOSTAS = r"""  // ── Apostas esportivas ──
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
            '<div style="font-size:10px;color:#475569;margin-top:2px">📅 '+dataExib+'</div>'+
            (st==='pendente'?'<div style="font-size:10px;color:#64748b;margin-top:2px">🔄 Resolu\u00e7\u00e3o autom\u00e1tica ap\u00f3s o jogo</div>':'')+
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
      '<div style="font-size:2rem;margin-bottom:8px">'+(temFiltro?'🔍':'\u26BD')+'</div>'+
      '<div style="font-weight:700;color:#94a3b8">'+(temFiltro?'Nenhuma aposta no per\u00edodo selecionado.':'Nenhuma aposta esportiva ainda.')+'</div>'+
      (temFiltro?'<button onclick="limparFiltroData()" style="margin-top:10px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:7px 16px;border-radius:8px;cursor:pointer;font-size:12px">🔄 Mostrar todas</button>':
        '<button onclick="navTo(\'home\')" style="margin-top:10px;background:var(--blue);border:none;color:#fff;padding:8px 18px;border-radius:8px;cursor:pointer;font-size:13px">🏠 Ver Jogos</button>')+
    '</div>';
  }"""

NEW_APOSTAS = r"""  // ── Apostas esportivas (compacto por padrão, lista oculta) ──
  if(apostas.length){
    const pendentes=apostas.filter(a=>(a.status||'pendente')==='pendente');
    const ganhou=apostas.filter(a=>a.status==='ganhou').length;
    const canceladas=apostas.filter(a=>a.status==='cancelada').length;
    const temApostasVisiveis=_apostasVisiveis;

    html+='<div class="conta-card" style="margin-bottom:14px" id="card-apostas">'+

      // ── Linha compacta: ícone + resumo + botão Pesquisar ──
      '<div style="display:flex;align-items:center;gap:8px">'+
        '<span style="font-size:16px">\u26BD</span>'+
        '<div style="flex:1">'+
          '<span style="font-size:13px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.8px">Apostas Esportivas</span>'+
          ' <span style="font-size:13px;font-weight:700;color:#e2e8f0">\u2014 '+apostas.length+'</span>'+
          (pendentes.length?' <span style="font-size:10px;color:#f59e0b;font-weight:600">\u23F3 '+pendentes.length+' pendente(s)</span>':'')+
          (ganhou?' <span style="font-size:10px;color:#4ade80;font-weight:600">\u00b7 '+ganhou+' ganhou</span>':'')+
          (canceladas?' <span style="font-size:10px;color:#64748b">\u00b7 '+canceladas+' cancelada(s)</span>':'')+
        '</div>'+
        '<button onclick="toggleApostas()" id="btn-apostas-toggle"'+
          ' title="Ver apostas"'+
          ' style="background:'+(temApostasVisiveis?'rgba(96,165,250,0.2)':'rgba(30,41,59,0.6)')+';'+
          'border:1px solid rgba(96,165,250,'+(temApostasVisiveis?'0.6':'0.2')+');'+
          'color:'+(temApostasVisiveis?'#93c5fd':'#94a3b8')+';'+
          'padding:6px 11px;border-radius:8px;cursor:pointer;font-size:12px;'+
          'font-weight:'+(temApostasVisiveis?'700':'400')+';white-space:nowrap">'+
          (temApostasVisiveis?'\u274F Ocultar':'⚽ Ver aposta(s)')+
        '</button>'+
      '</div>'+

      // ── Hint estático quando oculto ──
      (!temApostasVisiveis?
        '<div style="margin-top:10px;padding:9px 11px;background:rgba(245,158,11,0.04);'+
          'border:1px dashed rgba(245,158,11,0.15);border-radius:8px;'+
          'display:flex;align-items:center;gap:9px">'+
          '<span style="font-size:1.1rem">\u26BD</span>'+
          '<div style="font-size:12px;color:#475569">'+
            'Clique em <b style="color:#f59e0b">⚽ Ver aposta(s)</b> para ver o detalhamento.'+
            (pendentes.length?' <span style="color:#78350f">'+pendentes.length+' aguardando jogo.</span>':'')+
          '</div>'+
        '</div>'
      :
        // ── Lista de apostas (visível) ──
        '<div id="apostas-lista-detalhe" style="margin-top:10px">'+
          (pendentes.length?
            '<div style="background:rgba(245,158,11,0.08);border:1px solid rgba(245,158,11,0.2);border-radius:8px;padding:8px 12px;margin-bottom:8px;font-size:11px;color:#f59e0b">'+
              '\u23F3 <b>'+pendentes.length+' aposta(s) pendente(s)</b> \u2014 aguardando resultado do jogo.</div>'
          :'')+
          apostasFilt.map(a=>{
            const st=a.status||'pendente';
            const dataExib=a.criado_em
              ? new Date(a.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'})
              : (a.data||'');
            return '<div style="border-bottom:1px solid rgba(255,255,255,0.06);padding:11px 0;display:flex;justify-content:space-between;align-items:flex-start">'+
              '<div style="flex:1;min-width:0">'+
                '<div style="font-size:13px;font-weight:700;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+a.jogo+'</div>'+
                '<div style="font-size:11px;color:#94a3b8;margin-top:3px">'+
                  (a.selecao?'<span style="color:#60a5fa">'+a.selecao+'</span> \u00b7 ':'')+
                  'Odd <b style="color:#e2e8f0">'+Number(a.odd).toFixed(2)+'</b>'+
                '</div>'+
                '<div style="font-size:10px;color:#475569;margin-top:2px">📅 '+dataExib+'</div>'+
                (st==='pendente'?'<div style="font-size:10px;color:#64748b;margin-top:2px">🔄 Resolu\u00e7\u00e3o autom\u00e1tica ap\u00f3s o jogo</div>':'')+
              '</div>'+
              '<div style="text-align:right;margin-left:12px;flex-shrink:0">'+
                '<div style="font-size:11px;color:'+stColor(st)+';font-weight:700;white-space:nowrap">'+stLabel(st)+'</div>'+
                '<div style="font-size:13px;color:#e2e8f0;font-weight:700">'+_fmt(a.valor)+'</div>'+
                '<div style="font-size:11px;color:#94a3b8">retorno '+_fmt(a.retorno)+'</div>'+
              '</div>'+
            '</div>';
          }).join('')+
        '</div>'
      )+

    '</div>';
  } else {
    html+='<div class="conta-card" style="text-align:center;padding:20px;margin-bottom:14px">'+
      '<div style="font-size:2rem;margin-bottom:8px">\u26BD</div>'+
      '<div style="font-weight:700;color:#94a3b8">Nenhuma aposta esportiva ainda.</div>'+
      '<button onclick="navTo(\'home\')" style="margin-top:10px;background:var(--blue);border:none;color:#fff;padding:8px 18px;border-radius:8px;cursor:pointer;font-size:13px">🏠 Ver Jogos</button>'+
    '</div>';
  }"""

TOGGLE_FUNC = """
function toggleApostas(){
  _apostasVisiveis=!_apostasVisiveis;
  _renderBilhetes();
}
"""

def main():
    with open('home.html', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Adicionar variável de estado _apostasVisiveis junto com _filtroSorteioData
    OLD_VAR = "let _filtroSorteioData=null; // filtro exclusivo para bilhetes do sorteio"
    NEW_VAR = "let _filtroSorteioData=null; // filtro exclusivo para bilhetes do sorteio\nlet _apostasVisiveis=false; // apostas ocultas por padrão"
    if OLD_VAR in content:
        content = content.replace(OLD_VAR, NEW_VAR, 1)
        print("OK: variável _apostasVisiveis adicionada")
    else:
        print("ERRO: variável _filtroSorteioData não encontrada")
        return

    # 2. Substituir bloco de apostas esportivas
    if OLD_APOSTAS in content:
        content = content.replace(OLD_APOSTAS, NEW_APOSTAS, 1)
        print("OK: bloco apostas esportivas substituído")
    else:
        print("ERRO: bloco OLD_APOSTAS não encontrado")
        # Tentar achar marcador parcial
        idx = content.find("// ── Apostas esportivas ──")
        print(f"  Marcador parcial em pos: {idx}")
        return

    # 3. Adicionar função toggleApostas antes de toggleBuscaSorteio
    OLD_TOGGLE = "function toggleBuscaSorteio(){"
    if OLD_TOGGLE in content:
        content = content.replace(OLD_TOGGLE, TOGGLE_FUNC + OLD_TOGGLE, 1)
        print("OK: função toggleApostas adicionada")
    else:
        print("ERRO: toggleBuscaSorteio não encontrado")
        return

    with open('home.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"home.html salvo: {len(content)} chars")

if __name__ == '__main__':
    main()
