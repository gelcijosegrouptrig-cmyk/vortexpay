#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: Redesign completo e profissional da tela do Sorteio.
- Hero com prêmio em destaque + countdown
- Cards de stats animados
- Barra de progresso com label
- Seção de como funciona
- Últimos ganhadores inline
- CTA botão premium
"""

# ══════════════════════════════════════════════════════════
# CSS ANTIGO
# ══════════════════════════════════════════════════════════
OLD_CSS = """.sorteio-view{max-width:700px;margin:0 auto}
.sorteio-hero{background:linear-gradient(135deg,#1a1200,#2a1c00);
  border:1px solid #f5a62333;border-radius:16px;padding:24px;text-align:center;margin-bottom:20px}
.sh-label{font-size:11px;color:var(--gold);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px}
.sh-val{font-size:42px;font-weight:900;color:var(--gold2);line-height:1}
.sh-info{font-size:13px;color:var(--muted);margin:8px 0 20px}
.sh-stats{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:20px}
.sh-stat{background:#ffffff05;border-radius:10px;padding:12px;text-align:center}
.sh-stat .v{font-size:20px;font-weight:900;color:var(--text)}
.sh-stat .l{font-size:11px;color:var(--muted);margin-top:2px}
.sh-prog{height:8px;background:#ffffff10;border-radius:4px;overflow:hidden;margin-bottom:20px}
.sh-prog-fill{height:100%;background:linear-gradient(90deg,var(--gold),var(--gold2));border-radius:4px;transition:width .6s}
.btn-sortear{width:100%;padding:14px;border-radius:12px;
  background:linear-gradient(135deg,var(--gold),#e8950a);border:none;
  color:#1a0800;font-size:16px;font-weight:900;cursor:pointer;letter-spacing:.3px}
.btn-sortear:hover{filter:brightness(1.1);transform:translateY(-1px)}"""

# ══════════════════════════════════════════════════════════
# CSS NOVO
# ══════════════════════════════════════════════════════════
NEW_CSS = """.sorteio-view{max-width:680px;margin:0 auto;padding-bottom:24px}

/* ── Hero Principal ── */
.sorteio-hero{
  position:relative;overflow:hidden;
  background:linear-gradient(145deg,#110d00 0%,#1e1400 40%,#2a1c00 100%);
  border:1px solid rgba(245,166,35,0.25);border-radius:20px;
  padding:28px 24px 24px;text-align:center;margin-bottom:16px;
  box-shadow:0 8px 40px rgba(245,166,35,0.08),inset 0 1px 0 rgba(255,200,0,0.06)}
.sorteio-hero::before{
  content:'';position:absolute;top:-60px;left:50%;transform:translateX(-50%);
  width:320px;height:200px;
  background:radial-gradient(ellipse,rgba(245,166,35,0.12) 0%,transparent 70%);
  pointer-events:none}
.sorteio-hero::after{
  content:'';position:absolute;bottom:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,rgba(245,166,35,0.5),transparent)}

/* Badge ao topo */
.sh-badge{
  display:inline-flex;align-items:center;gap:6px;
  background:rgba(245,166,35,0.12);border:1px solid rgba(245,166,35,0.3);
  border-radius:20px;padding:4px 12px;margin-bottom:14px;
  font-size:10px;font-weight:700;color:#f5a623;text-transform:uppercase;letter-spacing:1.2px}
.sh-badge .dot{width:6px;height:6px;border-radius:50%;background:#f5a623;
  animation:pulseDot 1.5s infinite}
@keyframes pulseDot{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.7)}}

/* Prêmio */
.sh-label{font-size:11px;color:rgba(245,166,35,0.7);text-transform:uppercase;
  letter-spacing:1.5px;margin-bottom:6px;font-weight:600}
.sh-val{
  font-size:48px;font-weight:900;line-height:1;margin-bottom:4px;
  background:linear-gradient(135deg,#ffe066,#f5a623,#ffcc00);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
  text-shadow:none;filter:drop-shadow(0 2px 8px rgba(245,166,35,0.35))}
.sh-info{font-size:12px;color:#64748b;margin:8px 0 0;font-style:italic}

/* Countdown */
.sh-countdown{
  display:flex;align-items:center;justify-content:center;gap:6px;
  margin:16px 0;padding:10px 16px;
  background:rgba(0,0,0,0.3);border:1px solid rgba(245,166,35,0.1);border-radius:12px}
.sh-countdown .cd-lbl{font-size:10px;color:#64748b;margin-right:4px;text-transform:uppercase;letter-spacing:.8px}
.sh-countdown .cd-box{
  background:rgba(245,166,35,0.1);border:1px solid rgba(245,166,35,0.2);
  border-radius:7px;padding:5px 9px;text-align:center;min-width:38px}
.sh-countdown .cd-n{font-size:15px;font-weight:900;color:#f5a623;line-height:1}
.sh-countdown .cd-u{font-size:8px;color:#64748b;margin-top:1px;text-transform:uppercase}
.sh-countdown .cd-sep{font-size:14px;color:rgba(245,166,35,0.4);font-weight:900}

/* Stats */
.sh-stats{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:16px 0}
.sh-stat{
  background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);
  border-radius:12px;padding:14px 10px;text-align:center;position:relative;overflow:hidden}
.sh-stat::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,rgba(245,166,35,0.3),transparent)}
.sh-stat .v{font-size:24px;font-weight:900;color:#f1f5f9;line-height:1}
.sh-stat .l{font-size:10px;color:#475569;margin-top:3px;text-transform:uppercase;letter-spacing:.8px}
.sh-stat .st-ico{font-size:18px;margin-bottom:4px;display:block}

/* Barra de progresso */
.sh-prog-wrap{margin:16px 0 6px}
.sh-prog-label{display:flex;justify-content:space-between;
  font-size:11px;color:#475569;margin-bottom:6px}
.sh-prog-label .sp-hl{color:#f5a623;font-weight:700}
.sh-prog{height:10px;background:rgba(255,255,255,0.06);border-radius:6px;overflow:hidden}
.sh-prog-fill{
  height:100%;border-radius:6px;transition:width .8s cubic-bezier(.4,0,.2,1);
  background:linear-gradient(90deg,#b45309,#f5a623,#ffe066);
  box-shadow:0 0 8px rgba(245,166,35,0.5)}

/* Botão principal */
.btn-sortear{
  width:100%;padding:16px;border-radius:14px;border:none;cursor:pointer;
  background:linear-gradient(135deg,#d97706,#f5a623,#fbbf24);
  color:#1a0800;font-size:16px;font-weight:900;letter-spacing:.5px;
  box-shadow:0 4px 24px rgba(245,166,35,0.35),0 1px 0 rgba(255,255,255,0.2) inset;
  transition:all .2s;margin-top:14px;position:relative;overflow:hidden}
.btn-sortear::before{
  content:'';position:absolute;top:0;left:-100%;width:100%;height:100%;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,0.15),transparent);
  transition:left .5s}
.btn-sortear:hover{transform:translateY(-2px);box-shadow:0 8px 32px rgba(245,166,35,0.45)}
.btn-sortear:hover::before{left:100%}
.btn-sortear:active{transform:translateY(0)}

/* ── Como funciona ── */
.sh-como{
  background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);
  border-radius:16px;padding:18px;margin-bottom:16px}
.sh-como-title{
  font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;
  letter-spacing:1px;margin-bottom:14px;display:flex;align-items:center;gap:6px}
.sh-steps{display:flex;flex-direction:column;gap:10px}
.sh-step{display:flex;align-items:flex-start;gap:12px}
.sh-step .step-n{
  width:26px;height:26px;border-radius:50%;flex-shrink:0;
  background:linear-gradient(135deg,rgba(245,166,35,0.15),rgba(245,166,35,0.05));
  border:1px solid rgba(245,166,35,0.25);
  display:flex;align-items:center;justify-content:center;
  font-size:11px;font-weight:900;color:#f5a623}
.sh-step .step-txt{font-size:12px;color:#94a3b8;line-height:1.5;padding-top:3px}
.sh-step .step-txt b{color:#e2e8f0}

/* ── Últimos ganhadores ── */
.sh-ganhs{
  background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);
  border-radius:16px;padding:18px;margin-bottom:16px}
.sh-ganhs-title{
  font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;
  letter-spacing:1px;margin-bottom:12px;display:flex;align-items:center;gap:6px}
.sh-ganh-item{
  display:flex;align-items:center;gap:10px;padding:8px 0;
  border-bottom:1px solid rgba(255,255,255,0.04)}
.sh-ganh-item:last-child{border-bottom:none}
.sh-ganh-av{
  width:32px;height:32px;border-radius:50%;
  background:linear-gradient(135deg,rgba(245,166,35,0.2),rgba(245,166,35,0.05));
  border:1px solid rgba(245,166,35,0.2);
  display:flex;align-items:center;justify-content:center;
  font-size:13px;flex-shrink:0}
.sh-ganh-info{flex:1;min-width:0}
.sh-ganh-nome{font-size:12px;font-weight:700;color:#e2e8f0;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sh-ganh-data{font-size:10px;color:#475569;margin-top:1px}
.sh-ganh-val{font-size:13px;font-weight:900;color:#4ade80;white-space:nowrap}

/* Responsive */
@media(max-width:600px){
  .sorteio-view{padding:0}
  .sorteio-hero{padding:20px 16px 18px;border-radius:16px}
  .sh-val{font-size:36px}
  .sh-stat .v{font-size:20px}
  .sh-countdown .cd-box{min-width:32px;padding:4px 7px}
  .sh-countdown .cd-n{font-size:13px}
}"""

# ══════════════════════════════════════════════════════════
# HTML ANTIGO do view-sorteio
# ══════════════════════════════════════════════════════════
OLD_HTML = """    <div class="view" id="view-sorteio">
      <button class="btn-voltar" onclick="navBack('home')" title="Voltar"><span class="bv-ico">‹</span> Voltar</button>
      <div class="sorteio-view">
        <div class="sorteio-hero">
          <div class="sh-label">🏆 Prêmio do Sorteio</div>
          <div class="sh-val" id="sv-prize">R$ —</div>
          <div class="sh-info" id="sv-info">Carregando...</div>
          <div class="sh-stats">
            <div class="sh-stat"><div class="v" id="sv-part">--</div><div class="l">Participantes</div></div>
            <div class="sh-stat"><div class="v" id="sv-tick">--</div><div class="l">Seus bilhetes</div></div>
          </div>
          <div class="sh-prog"><div class="sh-prog-fill" id="sv-prog" style="width:65%"></div></div>
          <button class="btn-sortear" onclick="participarSorteio()">🎲 Participar do Sorteio →</button>
        </div>

        <!-- Painel inteligente de participação (injetado por JS) -->
        <div id="sorteio-participar-panel"></div>

      </div>
    </div><!-- /view-sorteio -->"""

# ══════════════════════════════════════════════════════════
# HTML NOVO do view-sorteio
# ══════════════════════════════════════════════════════════
NEW_HTML = """    <div class="view" id="view-sorteio">
      <button class="btn-voltar" onclick="navBack('home')" title="Voltar"><span class="bv-ico">&#x2039;</span> Voltar</button>
      <div class="sorteio-view">

        <!-- ── Hero com prêmio em destaque ── -->
        <div class="sorteio-hero">
          <div class="sh-badge"><span class="dot"></span> Ao Vivo &mdash; Sorteio Ativo</div>
          <div class="sh-label">🏆 Prêmio Acumulado</div>
          <div class="sh-val" id="sv-prize">R$ —</div>
          <div class="sh-info" id="sv-info">Carregando informações...</div>

          <!-- Countdown -->
          <div class="sh-countdown" id="sv-countdown-wrap">
            <span class="cd-lbl">Sorteio em</span>
            <div class="cd-box"><div class="cd-n" id="sv-cd-d">--</div><div class="cd-u">Dias</div></div>
            <span class="cd-sep">:</span>
            <div class="cd-box"><div class="cd-n" id="sv-cd-h">--</div><div class="cd-u">Hrs</div></div>
            <span class="cd-sep">:</span>
            <div class="cd-box"><div class="cd-n" id="sv-cd-m">--</div><div class="cd-u">Min</div></div>
            <span class="cd-sep">:</span>
            <div class="cd-box"><div class="cd-n" id="sv-cd-s">--</div><div class="cd-u">Seg</div></div>
          </div>

          <!-- Stats -->
          <div class="sh-stats">
            <div class="sh-stat">
              <span class="st-ico">👥</span>
              <div class="v" id="sv-part">--</div>
              <div class="l">Participantes</div>
            </div>
            <div class="sh-stat">
              <span class="st-ico">🎫</span>
              <div class="v" id="sv-tick">--</div>
              <div class="l">Seus bilhetes</div>
            </div>
          </div>

          <!-- Barra de progresso com label -->
          <div class="sh-prog-wrap">
            <div class="sh-prog-label">
              <span>Meta de participantes</span>
              <span class="sp-hl" id="sv-prog-pct">0%</span>
            </div>
            <div class="sh-prog"><div class="sh-prog-fill" id="sv-prog" style="width:0%"></div></div>
          </div>

          <!-- Botão CTA -->
          <button class="btn-sortear" onclick="participarSorteio()">
            🎲 Participar do Sorteio &#x2192;
          </button>
        </div>

        <!-- ── Como funciona ── -->
        <div class="sh-como">
          <div class="sh-como-title">📋 Como funciona</div>
          <div class="sh-steps">
            <div class="sh-step">
              <div class="step-n">1</div>
              <div class="step-txt"><b>Compre bilhetes</b> — cada R$5 dá direito a 1 bilhete numerado único.</div>
            </div>
            <div class="sh-step">
              <div class="step-n">2</div>
              <div class="step-txt"><b>Acumule bilhetes</b> — quanto mais bilhetes, maior sua chance de ganhar.</div>
            </div>
            <div class="sh-step">
              <div class="step-n">3</div>
              <div class="step-txt"><b>Sorteio ao vivo</b> — no dia marcado, 1 número é sorteado aleatoriamente.</div>
            </div>
            <div class="sh-step">
              <div class="step-n">4</div>
              <div class="step-txt"><b>Receba no Pix</b> — o ganhador recebe o prêmio direto na chave cadastrada.</div>
            </div>
          </div>
        </div>

        <!-- ── Últimos ganhadores ── -->
        <div class="sh-ganhs" id="sv-ganhs-wrap">
          <div class="sh-ganhs-title">🏅 Últimos Ganhadores</div>
          <div id="sv-ganhs-lista">
            <div style="text-align:center;padding:12px;font-size:12px;color:#475569">Carregando...</div>
          </div>
        </div>

        <!-- Painel inteligente de participação (injetado por JS) -->
        <div id="sorteio-participar-panel"></div>

      </div>
    </div><!-- /view-sorteio -->"""

# ══════════════════════════════════════════════════════════
# JS ANTIGO (loadSorteioInfo) - parte do countdown/ganhadores
# ══════════════════════════════════════════════════════════
OLD_JS_COUNTDOWN = "    // Data do próximo sorteio\n    if(s.proximo_sorteio && _el('sv-info')){\n      try{\n        const dt=new Date(s.proximo_sorteio);\n        _el('sv-info').textContent='Próximo: '+dt.toLocaleDateString('pt-BR')+' às '+dt.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});\n      }catch(_){}\n    }"

NEW_JS_COUNTDOWN = """    // Data do próximo sorteio + countdown
    if(s.proximo_sorteio){
      try{
        const dt=new Date(s.proximo_sorteio);
        if(_el('sv-info')){
          _el('sv-info').textContent='Próximo: '+dt.toLocaleDateString('pt-BR')+' às '+dt.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});
        }
        // Iniciar countdown
        _iniciarCountdown(dt);
      }catch(_){}
    } else {
      // Sem data definida: ocultar countdown
      if(_el('sv-countdown-wrap')) _el('sv-countdown-wrap').style.display='none';
    }

    // Carregar ganhadores recentes
    _carregarGanhadoresSorteio();"""

# ══════════════════════════════════════════════════════════
# JS extras: _iniciarCountdown e _carregarGanhadoresSorteio
# ══════════════════════════════════════════════════════════
EXTRA_JS = """
// ── Countdown do próximo sorteio ──────────────────────────
let _cdInterval = null;
function _iniciarCountdown(dtAlvo){
  if(_cdInterval) clearInterval(_cdInterval);
  function atualizar(){
    const agora=new Date();
    const diff=Math.max(0, dtAlvo - agora);
    const d=Math.floor(diff/86400000);
    const h=Math.floor((diff%86400000)/3600000);
    const m=Math.floor((diff%3600000)/60000);
    const s2=Math.floor((diff%60000)/1000);
    const z=n=>String(n).padStart(2,'0');
    const el=id=>document.getElementById(id);
    if(el('sv-cd-d')) el('sv-cd-d').textContent=z(d);
    if(el('sv-cd-h')) el('sv-cd-h').textContent=z(h);
    if(el('sv-cd-m')) el('sv-cd-m').textContent=z(m);
    if(el('sv-cd-s')) el('sv-cd-s').textContent=z(s2);
    if(diff===0){
      clearInterval(_cdInterval);
      const w=document.getElementById('sv-countdown-wrap');
      if(w) w.innerHTML='<span style="font-size:13px;color:#f5a623;font-weight:700">Sorteio em andamento!</span>';
    }
  }
  atualizar();
  _cdInterval=setInterval(atualizar,1000);
}

// ── Carregar últimos ganhadores do sorteio ────────────────
async function _carregarGanhadoresSorteio(){
  const lista=document.getElementById('sv-ganhs-lista');
  if(!lista) return;
  try{
    const r=await fetch(BASE+'/api/sorteio/ganhadores?limit=3');
    if(!r.ok){ lista.innerHTML='<div style="text-align:center;padding:10px;font-size:11px;color:#475569">Nenhum ganhador ainda.</div>'; return; }
    const d=await r.json();
    const items=d.ganhadores||[];
    if(!items.length){
      lista.innerHTML='<div style="text-align:center;padding:10px;font-size:11px;color:#475569">Nenhum ganhador ainda. Seja o primeiro! 🏆</div>';
      return;
    }
    const fmt=v=>Number(v||0).toLocaleString('pt-BR',{style:'currency',currency:'BRL'});
    const emos=['🥇','🥈','🥉'];
    lista.innerHTML=items.map((g,i)=>{
      const nome=(g.nome||'Jogador').substring(0,12)+(g.nome&&g.nome.length>12?'...':'');
      const data=g.criado_em?new Date(g.criado_em).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'2-digit'}):'--';
      return '<div class="sh-ganh-item">'+
        '<div class="sh-ganh-av">'+emos[i]+'</div>'+
        '<div class="sh-ganh-info">'+
          '<div class="sh-ganh-nome">'+nome+'</div>'+
          '<div class="sh-ganh-data">'+data+'</div>'+
        '</div>'+
        '<div class="sh-ganh-val">'+fmt(g.premio||0)+'</div>'+
      '</div>';
    }).join('');
  }catch(e){
    if(lista) lista.innerHTML='<div style="text-align:center;padding:10px;font-size:11px;color:#475569">Nenhum ganhador ainda.</div>';
  }
}
"""

# ══════════════════════════════════════════════════════════
# Também atualizar a barra de progresso para incluir %
# ══════════════════════════════════════════════════════════
OLD_JS_PROG = "    const pct=Math.min(100,Math.round((part/(s.min_participantes||10))*100));\n    if(_el('sv-prog')) _el('sv-prog').style.width=pct+'%';"

NEW_JS_PROG = """    const pct=Math.min(100,Math.round((part/(s.min_participantes||10))*100));
    if(_el('sv-prog')) _el('sv-prog').style.width=pct+'%';
    if(_el('sv-prog-pct')) _el('sv-prog-pct').textContent=pct+'%';"""

# CSS mobile antigo
OLD_CSS_MOBILE = """  .sorteio-view{padding:0}
  .sorteio-hero{padding:14px;border-radius:14px}
  .sh-val{font-size:30px;font-weight:900}
  .sh-stats{grid-template-columns:1fr 1fr;gap:8px}
  .sh-stat .v{font-size:18px}
  .sh-stat .l{font-size:9px}"""

NEW_CSS_MOBILE = """  .sorteio-view{padding:0 0 16px}
  .sorteio-hero{padding:18px 14px 16px;border-radius:16px}
  .sh-val{font-size:34px}
  .sh-stats{grid-template-columns:1fr 1fr;gap:8px}
  .sh-stat .v{font-size:20px}
  .sh-stat .l{font-size:9px}
  .sh-countdown{flex-wrap:wrap;gap:4px;padding:8px 10px}
  .sh-como,.sh-ganhs{border-radius:12px;padding:14px}"""


def main():
    with open('home.html', 'r', encoding='utf-8') as f:
        content = f.read()

    errors = []

    # 1. Substituir CSS antigo
    if OLD_CSS in content:
        content = content.replace(OLD_CSS, NEW_CSS, 1)
        print("OK: CSS principal substituído")
    else:
        errors.append("ERRO: OLD_CSS não encontrado")

    # 2. Substituir CSS mobile
    if OLD_CSS_MOBILE in content:
        content = content.replace(OLD_CSS_MOBILE, NEW_CSS_MOBILE, 1)
        print("OK: CSS mobile substituído")
    else:
        # Não é crítico se o mobile não existir exatamente
        print("AVISO: CSS mobile não encontrado (não crítico)")

    # 3. Substituir HTML do view-sorteio
    if OLD_HTML in content:
        content = content.replace(OLD_HTML, NEW_HTML, 1)
        print("OK: HTML view-sorteio substituído")
    else:
        errors.append("ERRO: OLD_HTML não encontrado")
        # Debug
        idx = content.find('id="view-sorteio"')
        print(f"  view-sorteio encontrado em pos: {idx}")
        print(repr(content[idx:idx+200]))

    # 4. Atualizar barra de progresso no JS
    if OLD_JS_PROG in content:
        content = content.replace(OLD_JS_PROG, NEW_JS_PROG, 1)
        print("OK: JS barra de progresso atualizada")
    else:
        errors.append("AVISO: OLD_JS_PROG não encontrado")

    # 5. Atualizar countdown no JS
    if OLD_JS_COUNTDOWN in content:
        content = content.replace(OLD_JS_COUNTDOWN, NEW_JS_COUNTDOWN, 1)
        print("OK: JS countdown e ganhadores injetado")
    else:
        errors.append("ERRO: OLD_JS_COUNTDOWN não encontrado")
        idx = content.find("// Data do próximo sorteio")
        print(f"  Marcador countdown em pos: {idx}")

    # 6. Adicionar funções extras antes de loadSorteioInfo
    INSERT_BEFORE = "async function loadSorteioInfo(){"
    if INSERT_BEFORE in content:
        content = content.replace(INSERT_BEFORE, EXTRA_JS + INSERT_BEFORE, 1)
        print("OK: funções _iniciarCountdown e _carregarGanhadoresSorteio adicionadas")
    else:
        errors.append("ERRO: loadSorteioInfo não encontrado para inserir funções extras")

    if errors:
        print("\n=== ERROS ===")
        for e in errors:
            print(e)
        if any("ERRO:" in e and "AVISO" not in e for e in errors):
            print("Abortando por erros críticos.")
            return

    with open('home.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"\nhome.html salvo: {len(content)} chars")
    print("Verificando elementos-chave...")
    checks = ['sorteio-hero', 'sh-badge', 'sh-val', 'sv-countdown-wrap', 'sh-como',
              'sh-ganhs', '_iniciarCountdown', '_carregarGanhadoresSorteio']
    for c in checks:
        found = c in content
        print(f"  {'OK' if found else 'FALTA'}: {c}")


if __name__ == '__main__':
    main()
