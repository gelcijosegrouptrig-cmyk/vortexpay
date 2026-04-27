#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: bilhetes do sorteio ocultos por padrão — substituição por índice.
"""

NEW_BLOCK = r"""if(sorteio.length){
    const temBuscaData=_filtroSorteioData!==null;

    html+='<div class="conta-card" id="card-sorteio">'+

      // ── Linha compacta: ícone + título + total + botão Pesquisar ──
      '<div style="display:flex;align-items:center;gap:8px">'+
        '<span style="font-size:16px">\u{1F3AB}</span>'+
        '<div style="flex:1">'+
          '<span style="font-size:13px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.8px">Sorteio</span>'+
          ' <span style="font-size:13px;font-weight:700;color:#e2e8f0">\u2014 '+sorteio.length+' bilhete(s)</span>'+
          (temBuscaData?
            ' <span style="font-size:10px;color:#60a5fa;font-weight:600">\u00b7 '+sorteioFiltFinal.length+' filtrado(s)</span>':
            datasUnicas.length>1?
              ' <span style="font-size:10px;color:#475569">em '+datasUnicas.length+' datas</span>':''
          )+
        '</div>'+
        '<button onclick="toggleBuscaSorteio()" id="btn-busca-sorteio-toggle"'+
          ' title="Pesquisar bilhetes por data"'+
          ' style="'+
            'background:'+(temBuscaData?'rgba(96,165,250,0.2)':'rgba(30,41,59,0.6)')+';'+
            'border:1px solid rgba(96,165,250,'+(temBuscaData?'0.6':'0.2')+');'+
            'color:'+(temBuscaData?'#93c5fd':'#94a3b8')+';'+
            'padding:6px 11px;border-radius:8px;cursor:pointer;font-size:12px;'+
            'font-weight:'+(temBuscaData?'700':'400')+';white-space:nowrap">'+
          (temBuscaData?'\u{1F50D} '+sorteioFiltFinal.length+'/'+sorteio.length:'📅 Pesquisar')+
        '</button>'+
      '</div>'+

      // ── Hint estático (só quando nenhuma data selecionada) ──
      (!temBuscaData?
        '<div style="margin-top:10px;padding:9px 11px;background:rgba(96,165,250,0.04);'+
          'border:1px dashed rgba(96,165,250,0.15);border-radius:8px;'+
          'display:flex;align-items:center;gap:9px">'+
          '<span style="font-size:1.1rem">\u{1F4C5}</span>'+
          '<div style="font-size:12px;color:#475569">'+
            'Clique em <b style="color:#60a5fa">📅 Pesquisar</b> para ver bilhetes por data.'+
            (datasUnicas.length>0?' <span style="color:#334155">('+datasUnicas.length+' data(s) disponíveis)</span>':'')+
          '</div>'+
        '</div>'
      :'')+

      // ── Painel de busca + resultado ──
      '<div id="sorteio-busca-painel" style="display:'+(temBuscaData?'block':'none')+';margin-top:12px">'+

        // Seletor de datas
        '<div style="background:rgba(15,23,42,0.85);border:1px solid rgba(96,165,250,0.2);border-radius:10px;padding:11px;margin-bottom:10px">'+
          '<div style="display:flex;gap:8px;align-items:center;margin-bottom:9px">'+
            '<input type="date" id="sorteio-busca-input"'+
              ' value="'+(temBuscaData?new Date(_filtroSorteioData).toISOString().slice(0,10):'')+'"'+
              ' style="flex:1;background:rgba(30,41,59,0.9);border:1px solid rgba(96,165,250,0.3);'+
              'border-radius:8px;color:#e2e8f0;padding:7px 10px;font-size:13px"'+
              ' onchange="aplicarBuscaSorteio(this.value)">'+
            '<button onclick="limparBuscaSorteio()"'+
              ' title="Fechar"'+
              ' style="background:rgba(239,68,68,0.12);border:1px solid rgba(239,68,68,0.25);'+
              'color:#f87171;padding:7px 11px;border-radius:8px;cursor:pointer;font-size:13px">\u2715</button>'+
          '</div>'+
          (datasUnicas.length>0?
            '<div style="font-size:10px;color:#334155;margin-bottom:6px">\u{1F4CC} Datas disponíveis:</div>'+
            '<div style="display:flex;flex-wrap:wrap;gap:5px">'+
              datasUnicas.map(dt=>{
                const iso=toIsoVal(dt);
                const isAtivo=temBuscaData &&
                  new Date(_filtroSorteioData).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'})===dt;
                const qtd=sorteio.filter(b=>{
                  if(!b.criado_em) return false;
                  return new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'})===dt;
                }).length;
                return '<button onclick="aplicarBuscaSorteio(\''+iso+'\')"'+
                  ' style="background:'+(isAtivo?'rgba(96,165,250,0.25)':'rgba(30,41,59,0.8)')+';'+
                  'border:1px solid rgba(96,165,250,'+(isAtivo?'0.6':'0.18')+');'+
                  'color:'+(isAtivo?'#e2e8f0':'#94a3b8')+';'+
                  'padding:5px 11px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:'+(isAtivo?'700':'400')+'">'+ 
                  '\u{1F4C5} '+dt.slice(0,5)+
                  ' <span style="background:rgba(96,165,250,'+(isAtivo?'0.3':'0.1')+');color:'+(isAtivo?'#93c5fd':'#60a5fa')+';'+
                  'padding:1px 6px;border-radius:10px;font-size:10px;margin-left:3px">'+qtd+'</span>'+
                '</button>';
              }).join('')+
            '</div>'
          :'<span style="font-size:11px;color:#334155">Nenhuma data.</span>')+
        '</div>'+

        // Grid de bilhetes (só quando data selecionada)
        (temBuscaData?
          sorteioFiltFinal.length?
            '<div style="display:grid;grid-template-columns:repeat('+Math.min(4,sorteioFiltFinal.length)+',1fr);gap:6px;margin-bottom:8px">'+
              sorteioFiltFinal.map(b=>{
                const dtB=b.criado_em?new Date(b.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit'}):'';
                return '<div style="background:rgba(96,165,250,0.1);border:1px solid rgba(96,165,250,0.28);'+
                  'border-radius:8px;padding:8px 4px;text-align:center">'+
                  '<div style="font-size:15px;font-weight:700;color:#60a5fa;letter-spacing:1px">'+(b.numero||'\u2014')+'</div>'+
                  (dtB?'<div style="font-size:9px;color:#475569;margin-top:1px">'+dtB+'</div>':'')+
                '</div>';
              }).join('')+
            '</div>'
          :
            '<div style="text-align:center;padding:14px;background:rgba(30,41,59,0.4);border-radius:8px;margin-bottom:8px">'+
              '<div style="font-size:1.2rem;margin-bottom:4px">\u{1F50D}</div>'+
              '<div style="font-size:12px;color:#64748b">Nenhum bilhete nesta data.</div>'+
            '</div>'
        :'')+

      '</div>'+

      // Rodapé
      '<div style="display:flex;justify-content:flex-end;margin-top:'+(temBuscaData?'4':'10')+'px">'+
        '<button onclick="sbNav(\'sorteio\')" style="background:none;border:1px solid rgba(96,165,250,0.22);'+
          'color:#60a5fa;padding:5px 12px;border-radius:8px;cursor:pointer;font-size:12px">\u2795 Comprar mais</button>'+
      '</div>'+

    '</div>';
  } else {"""

def main():
    with open('home.html', 'r', encoding='utf-8') as f:
        content = f.read()

    START_MARKER = 'if(sorteio.length){'
    END_MARKER   = '\n  } else {'

    start_idx = content.find(START_MARKER)
    if start_idx == -1:
        print("ERRO: marcador inicial não encontrado"); return

    end_idx = content.find(END_MARKER, start_idx)
    if end_idx == -1:
        print("ERRO: marcador final não encontrado"); return

    # O bloco a substituir começa em start_idx e vai até end_idx (exclusive do \n} else {)
    old_block = content[start_idx:end_idx]
    print(f"Substituindo {len(old_block)} chars (pos {start_idx}-{end_idx})")

    # Montar novo conteúdo
    new_content = content[:start_idx] + NEW_BLOCK + content[end_idx:]

    with open('home.html', 'w', encoding='utf-8') as f:
        f.write(new_content)

    print(f"OK! home.html salvo: {len(new_content)} chars")
    print(f"Verificação: {'btn-busca-sorteio-toggle' in new_content}")

if __name__ == '__main__':
    main()
