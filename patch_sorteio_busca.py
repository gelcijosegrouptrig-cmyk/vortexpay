#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: adiciona busca de data INLINE no card de sorteio dentro de _renderBilhetes
"""

import re

def main():
    with open('home.html', 'r', encoding='utf-8') as f:
        content = f.read()

    # ── 1. Adicionar variável de filtro de sorteio separado ──────────────────
    OLD_VARS = "let _bilhetesCache={apostas:[],sorteio:[]};\nlet _filtroAtivo=null;"
    NEW_VARS = (
        "let _bilhetesCache={apostas:[],sorteio:[]};\n"
        "let _filtroAtivo=null;\n"
        "let _filtroSorteioData=null; // filtro exclusivo para bilhetes do sorteio"
    )
    content = content.replace(OLD_VARS, NEW_VARS, 1)

    # ── 2. Substituir bloco de renderização dos bilhetes do sorteio ──────────
    # Localizar início/fim do bloco
    SORTEIO_OLD = (
        "  // ── Bilhetes do sorteio ──\n"
        "  if(sorteioFilt.length){\n"
        "    const numCols=Math.min(4,sorteioFilt.length);\n"
        "    html+='<div class=\"conta-card\">'+\n"
        "      '<div class=\"cc-title\"><span class=\"ico\">\\u{1F3AB}</span>Sorteio \\u2014 '+sorteioFilt.length+' bilhete(s)'+\n"
        "        (_filtroAtivo&&sorteio.length!==sorteioFilt.length?' <span style=\"font-size:10px;color:#64748b;font-weight:400\">de '+sorteio.length+' total</span>':'')+\n"
        "      '</div>'+\n"
        "      '<div style=\"display:grid;grid-template-columns:repeat('+numCols+',1fr);gap:6px;margin:10px 0\">'+\n"
        "        sorteioFilt.map(b=>{\n"
        "          const dtB=b.criado_em?new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit'}):'';\n"
        "          return '<div style=\"background:rgba(96,165,250,0.1);border:1px solid rgba(96,165,250,0.25);border-radius:8px;padding:8px 4px;text-align:center\">'+\n"
        "            '<div style=\"font-size:16px;font-weight:700;color:#60a5fa;letter-spacing:1px\">'+(b.numero||'\\u2014')+'</div>'+\n"
        "            (dtB?'<div style=\"font-size:9px;color:#475569;margin-top:1px\">'+dtB+'</div>':'<div style=\"font-size:9px;color:#64748b;margin-top:1px\">Bilhete</div>')+\n"
        "          '</div>';\n"
        "        }).join('')+\n"
        "      '</div>'+\n"
        "      '<div style=\"text-align:right;margin-top:4px\">'+\n"
        "        '<button onclick=\"sbNav(\\'sorteio\\')\" style=\"background:none;border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:5px 12px;border-radius:8px;cursor:pointer;font-size:12px\">\\u2795 Mais bilhetes</button>'+\n"
        "      '</div>'+\n"
        "    '</div>';\n"
        "  } else {\n"
        "    const temFiltroS=_filtroAtivo&&sorteio.length>0;\n"
        "    html+='<div class=\"conta-card\" style=\"text-align:center;padding:16px\">'+\n"
        "      '<div style=\"font-size:1.6rem;margin-bottom:6px\">'+(temFiltroS?'\\u{1F50D}':'\\u{1F3AB}')+'</div>'+\n"
        "      '<div style=\"font-size:13px;color:#94a3b8\">'+(temFiltroS?'Nenhum bilhete no per\\u00edodo selecionado.':'Sem bilhetes de sorteio ainda.')+'</div>'+\n"
        "      (temFiltroS?'<button onclick=\"limparFiltroData()\" style=\"margin-top:8px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:6px 14px;border-radius:8px;cursor:pointer;font-size:12px\">\\u{1F504} Mostrar todos</button>':\n"
        "        '<button onclick=\"sbNav(\\'sorteio\\')\" style=\"margin-top:8px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:6px 14px;border-radius:8px;cursor:pointer;font-size:12px\">\\u{1F3AF} Participar do Sorteio</button>')+\n"
        "    '</div>';\n"
        "  }"
    )

    SORTEIO_NEW = r"""  // ── Bilhetes do sorteio ──
  // Aplicar filtro de data exclusivo do sorteio (campo inline no card)
  const sorteioFiltFinal = _filtroSorteioData
    ? sorteio.filter(b=>{
        if(!b.criado_em) return false;
        const dt=new Date(b.criado_em); dt.setHours(0,0,0,0);
        const ref=new Date(_filtroSorteioData); ref.setHours(0,0,0,0);
        return dt.getTime()===ref.getTime();
      })
    : sorteioFilt;

  // Obter datas únicas dos bilhetes para o calendário interno
  const datasUnicas=[...new Set(sorteio.map(b=>{
    if(!b.criado_em) return null;
    return new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'});
  }).filter(Boolean))].sort((a,b)=>{
    const pa=a.split('/'); const pb=b.split('/');
    return new Date(+pa[2],+pa[1]-1,+pa[0]) - new Date(+pb[2],+pb[1]-1,+pb[0]);
  });

  const toIsoVal=dtStr=>{
    if(!dtStr) return '';
    const p=dtStr.split('/');
    if(p.length===3) return `${p[2]}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`;
    return '';
  };

  if(sorteio.length){
    const numCols=Math.min(4,sorteioFiltFinal.length||1);
    const totalFiltro=_filtroAtivo&&sorteio.length!==sorteioFilt.length;
    const temBuscaData=_filtroSorteioData!==null;

    // Cabeçalho clicável com contador e pesquisa inline
    html+='<div class="conta-card" id="card-sorteio">'+

      // ── Título com botão de busca ──
      '<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">'+
        '<span style="font-size:16px">\u{1F3AB}</span>'+
        '<div style="flex:1">'+
          '<span style="font-size:13px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.8px">Sorteio</span>'+
          ' <span style="font-size:13px;font-weight:700;color:#e2e8f0">\u2014 '+
            (temBuscaData?sorteioFiltFinal.length:sorteioFilt.length)+' bilhete(s)'+
            (temBuscaData?' <span style="font-size:10px;color:#60a5fa">(filtrado)</span>':
              totalFiltro?' <span style="font-size:10px;color:#64748b">de '+sorteio.length+' total</span>':
              sorteio.length>sorteioFilt.length?' <span style="font-size:10px;color:#64748b">de '+sorteio.length+' total</span>':''
            )+
          '</span>'+
        '</div>'+
        '<button onclick="toggleBuscaSorteio()" id="btn-busca-sorteio-toggle"'+
          ' style="background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,'+(temBuscaData?'0.6)':'0.3)')+';'+
          'color:'+(temBuscaData?'#93c5fd':'#60a5fa')+';padding:5px 10px;border-radius:8px;cursor:pointer;font-size:12px;white-space:nowrap;font-weight:'+(temBuscaData?'700':'400')+'">'+
          (temBuscaData?'\u{1F50D} filtrado':'📅 Data')+
        '</button>'+
      '</div>'+

      // ── Painel de busca (inicialmente oculto) ──
      '<div id="sorteio-busca-painel" style="display:'+(temBuscaData?'block':'none')+';margin-bottom:12px;'+
        'background:rgba(15,23,42,0.8);border:1px solid rgba(96,165,250,0.25);border-radius:10px;padding:12px">'+
        '<div style="font-size:11px;color:#64748b;margin-bottom:8px;font-weight:600">🔍 Pesquisar bilhetes por data</div>'+

        // Input de data manual
        '<div style="display:flex;gap:8px;margin-bottom:10px;align-items:center">'+
          '<input type="date" id="sorteio-busca-input"'+
            ' value="'+(temBuscaData?new Date(_filtroSorteioData).toISOString().slice(0,10):'')+'\"'+
            ' style="flex:1;background:rgba(30,41,59,0.9);border:1px solid rgba(96,165,250,0.3);'+
            'border-radius:8px;color:#e2e8f0;padding:8px 10px;font-size:13px"'+
            ' onchange="aplicarBuscaSorteio(this.value)">'+
          '<button onclick="limparBuscaSorteio()"'+
            ' style="background:rgba(239,68,68,0.15);border:1px solid rgba(239,68,68,0.3);'+
            'color:#f87171;padding:8px 12px;border-radius:8px;cursor:pointer;font-size:12px">✕</button>'+
        '</div>'+

        // Botões de datas disponíveis (chips)
        (datasUnicas.length>0?
          '<div style="font-size:10px;color:#475569;margin-bottom:6px">Datas com bilhetes:</div>'+
          '<div style="display:flex;flex-wrap:wrap;gap:5px">'+
            datasUnicas.map(dt=>{
              const iso=toIsoVal(dt);
              const isAtivo=_filtroSorteioData && new Date(_filtroSorteioData).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'})===dt;
              const qtd=sorteio.filter(b=>{
                if(!b.criado_em) return false;
                return new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'})===dt;
              }).length;
              return '<button onclick="aplicarBuscaSorteio(\''+iso+'\')"'+
                ' style="background:'+(isAtivo?'rgba(96,165,250,0.3)':'rgba(30,41,59,0.8)')+';'+
                'border:1px solid rgba(96,165,250,'+(isAtivo?'0.6':'0.2')+');'+
                'color:'+(isAtivo?'#93c5fd':'#94a3b8')+';padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:'+(isAtivo?'700':'400')+'">'+
                dt.slice(0,5)+' <span style="color:'+(isAtivo?'#60a5fa':'#475569')+'">'+qtd+'</span></button>';
            }).join('')+
          '</div>'
        :'<div style="font-size:11px;color:#475569">Nenhuma data disponível.</div>')+
      '</div>'+

      // ── Grid de bilhetes ──
      (sorteioFiltFinal.length?
        '<div style="display:grid;grid-template-columns:repeat('+numCols+',1fr);gap:6px;margin:8px 0">'+
          sorteioFiltFinal.map(b=>{
            const dtB=b.criado_em?new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit'}):'';
            return '<div style="background:rgba(96,165,250,0.1);border:1px solid rgba(96,165,250,0.25);border-radius:8px;padding:8px 4px;text-align:center">'+
              '<div style="font-size:16px;font-weight:700;color:#60a5fa;letter-spacing:1px">'+(b.numero||'\u2014')+'</div>'+
              (dtB?'<div style="font-size:9px;color:#475569;margin-top:1px">'+dtB+'</div>':'<div style="font-size:9px;color:#64748b;margin-top:1px">Bilhete</div>')+
            '</div>';
          }).join('')+
        '</div>'
      :
        '<div style="text-align:center;padding:16px">'+
          '<div style="font-size:1.4rem;margin-bottom:6px">\u{1F50D}</div>'+
          '<div style="font-size:12px;color:#94a3b8">Nenhum bilhete nesta data.</div>'+
          '<button onclick="limparBuscaSorteio()" style="margin-top:8px;background:rgba(96,165,250,0.15);border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:5px 12px;border-radius:8px;cursor:pointer;font-size:11px">\u{1F504} Ver todos</button>'+
        '</div>'
      )+

      '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px">'+
        (sorteio.length>0?'<span style="font-size:11px;color:#475569">Total: '+sorteio.length+' bilhete(s)</span>':'')+
        '<button onclick="sbNav(\'sorteio\')" style="margin-left:auto;background:none;border:1px solid rgba(96,165,250,0.3);color:#60a5fa;padding:5px 12px;border-radius:8px;cursor:pointer;font-size:12px">\u2795 Mais bilhetes</button>'+
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
  }"""

    if SORTEIO_OLD in content:
        content = content.replace(SORTEIO_OLD, SORTEIO_NEW, 1)
        print("OK: bloco de sorteio substituído")
    else:
        print("ERRO: bloco original não encontrado! Verificar manualmente.")
        return

    # ── 3. Adicionar funções toggleBuscaSorteio / aplicarBuscaSorteio / limparBuscaSorteio ──
    # Inserir antes de 'function aplicarFiltroData'
    BEFORE_APLICAR = "function aplicarFiltroData(){"
    NEW_FUNCS = r"""function toggleBuscaSorteio(){
  const painel=document.getElementById('sorteio-busca-painel');
  if(!painel) return;
  const visible=painel.style.display!=='none';
  if(visible && _filtroSorteioData===null){
    painel.style.display='none';
  } else {
    painel.style.display=painel.style.display==='none'?'block':'none';
  }
}

function aplicarBuscaSorteio(isoDate){
  if(!isoDate){ limparBuscaSorteio(); return; }
  _filtroSorteioData=new Date(isoDate+'T00:00:00');
  // Atualizar input
  const inp=document.getElementById('sorteio-busca-input');
  if(inp) inp.value=isoDate;
  _renderBilhetes();
  // Garantir painel visível após re-render
  setTimeout(()=>{
    const p=document.getElementById('sorteio-busca-painel');
    if(p) p.style.display='block';
  },10);
}

function limparBuscaSorteio(){
  _filtroSorteioData=null;
  const inp=document.getElementById('sorteio-busca-input');
  if(inp) inp.value='';
  _renderBilhetes();
}

""" + BEFORE_APLICAR

    if BEFORE_APLICAR in content:
        content = content.replace(BEFORE_APLICAR, NEW_FUNCS, 1)
        print("OK: funções toggleBuscaSorteio/aplicarBuscaSorteio/limparBuscaSorteio adicionadas")
    else:
        print("AVISO: 'function aplicarFiltroData' não encontrado para injetar novas funções")

    with open('home.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"home.html salvo: {len(content)} chars")

if __name__ == '__main__':
    main()
