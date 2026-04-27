#!/usr/bin/env python3
"""
Patch: Menu lateral (drawer) mobile para home.html
- Botão hamburger ☰ no header mobile (esquerda)
- Drawer lateral deslizante com todo o conteúdo da sidebar
- Overlay escuro para fechar
- Animação suave de slide
- Swipe para fechar (touch)
"""

print("=== patch_mobile_menu.py ===")

with open('home.html', 'r', encoding='utf-8') as f:
    html = f.read()

# ─────────────────────────────────────────────────────────────────
# 1. CSS — adicionar styles do drawer e botão hamburger
#    Inserir antes do fechamento de </style> do bloco mobile
# ─────────────────────────────────────────────────────────────────

CSS_DRAWER = """
/* ══ MOBILE DRAWER ══════════════════════════════════════════════ */
.btn-hamburger {
  display: none;
  background: none;
  border: none;
  cursor: pointer;
  padding: 6px 8px;
  border-radius: 8px;
  color: var(--text);
  flex-direction: column;
  gap: 5px;
  align-items: center;
  justify-content: center;
  transition: background .15s;
  flex-shrink: 0;
}
.btn-hamburger:hover { background: #ffffff12; }
.btn-hamburger span {
  display: block;
  width: 20px;
  height: 2px;
  background: var(--text);
  border-radius: 2px;
  transition: all .25s;
}

/* Overlay escuro */
.drawer-overlay {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,.65);
  z-index: 900;
  backdrop-filter: blur(2px);
  -webkit-backdrop-filter: blur(2px);
  opacity: 0;
  transition: opacity .25s;
}
.drawer-overlay.open {
  display: block;
  opacity: 1;
}

/* Drawer lateral */
.mobile-drawer {
  position: fixed;
  top: 0;
  left: 0;
  width: 78vw;
  max-width: 300px;
  height: 100dvh;
  height: 100vh;
  background: var(--bg2);
  border-right: 1px solid var(--border);
  z-index: 901;
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 0 0 40px;
  transform: translateX(-100%);
  transition: transform .28s cubic-bezier(.4,0,.2,1);
  will-change: transform;
}
.mobile-drawer.open {
  transform: translateX(0);
}
.mobile-drawer::-webkit-scrollbar { width: 3px; }
.mobile-drawer::-webkit-scrollbar-thumb { background: #ffffff15; border-radius: 3px; }

/* Cabeçalho do drawer */
.drawer-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 16px 10px;
  border-bottom: 1px solid var(--border);
  position: sticky;
  top: 0;
  background: var(--bg2);
  z-index: 1;
}
.drawer-title {
  font-size: 16px;
  font-weight: 800;
  color: var(--gold);
  display: flex;
  align-items: center;
  gap: 7px;
}
.btn-close-drawer {
  background: #ffffff12;
  border: none;
  color: var(--text);
  font-size: 18px;
  width: 32px;
  height: 32px;
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background .15s;
}
.btn-close-drawer:hover { background: #ffffff22; }

/* Nav rápida horizontal no topo do drawer */
.drawer-quicknav {
  display: flex;
  gap: 6px;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  flex-wrap: wrap;
}
.dqn-btn {
  flex: 1;
  min-width: calc(50% - 3px);
  padding: 8px 6px;
  background: #ffffff08;
  border: 1px solid var(--border);
  border-radius: 10px;
  color: var(--text2);
  font-size: 11px;
  font-weight: 700;
  cursor: pointer;
  text-align: center;
  transition: all .15s;
}
.dqn-btn:hover, .dqn-btn.active {
  background: #FFD70018;
  border-color: var(--gold);
  color: var(--gold);
}
"""

# Inserir CSS do drawer antes do fechamento da tag </style> (primeira ocorrência)
STYLE_CLOSE_MARKER = "</style>"
idx_style = html.find(STYLE_CLOSE_MARKER)
if idx_style != -1 and '.btn-hamburger' not in html:
    html = html[:idx_style] + CSS_DRAWER + "\n" + html[idx_style:]
    print("OK: CSS do drawer inserido")
elif '.btn-hamburger' in html:
    print("OK: CSS do drawer já existe")
else:
    print("AVISO: </style> não encontrado")

# ─────────────────────────────────────────────────────────────────
# 2. CSS mobile — mostrar botão hamburger e ajustar header
# ─────────────────────────────────────────────────────────────────
OLD_MOBILE_HEADER = """  /* ── Header mobile ── */
  .sidebar{display:none}
  .header{padding:0 8px}
  .header-logo{width:auto;font-size:14px;padding:0 4px 0 2px;gap:4px}
  .header-logo .ico{font-size:17px}
  .header-nav{display:none}"""

NEW_MOBILE_HEADER = """  /* ── Header mobile ── */
  .sidebar{display:none}
  .header{padding:0 6px}
  .header-logo{width:auto;font-size:14px;padding:0 4px 0 2px;gap:4px}
  .header-logo .ico{font-size:17px}
  .header-nav{display:none}
  .btn-hamburger{display:flex}"""

if OLD_MOBILE_HEADER in html:
    html = html.replace(OLD_MOBILE_HEADER, NEW_MOBILE_HEADER)
    print("OK: CSS mobile — botão hamburger ativado")
else:
    # Tentar versão sem espaço extra
    OLD2 = "  .sidebar{display:none}\n  .header{padding:0 8px}"
    if OLD2 in html and '.btn-hamburger{display:flex}' not in html:
        html = html.replace(OLD2, "  .sidebar{display:none}\n  .header{padding:0 6px}")
        print("OK: CSS mobile sidebar ajustado (parcial)")
    else:
        print("AVISO: bloco CSS mobile header não encontrado exatamente")

# ─────────────────────────────────────────────────────────────────
# 3. HTML — adicionar botão hamburger no header (antes de header-logo)
# ─────────────────────────────────────────────────────────────────
OLD_HEADER_HTML = """<!-- ══ HEADER ══ -->
<header class=\"header\">
  <div class=\"header-logo\" onclick=\"navTo('home')\">
    <span class=\"ico\">🎰</span> PaynexBet
  </div>"""

NEW_HEADER_HTML = """<!-- ══ HEADER ══ -->
<header class=\"header\">
  <!-- Botão hamburger: só visível no mobile -->
  <button class=\"btn-hamburger\" id=\"btn-hamburger\" onclick=\"abrirDrawerMobile()\" aria-label=\"Menu\">
    <span></span><span></span><span></span>
  </button>
  <div class=\"header-logo\" onclick=\"navTo('home')\">
    <span class=\"ico\">🎰</span> PaynexBet
  </div>"""

if OLD_HEADER_HTML in html:
    html = html.replace(OLD_HEADER_HTML, NEW_HEADER_HTML)
    print("OK: botão hamburger adicionado no header")
elif 'btn-hamburger' in html:
    print("OK: botão hamburger já existe")
else:
    print("AVISO: ponto de inserção do header não encontrado")

# ─────────────────────────────────────────────────────────────────
# 4. HTML — adicionar drawer e overlay antes do </body>
# ─────────────────────────────────────────────────────────────────
DRAWER_HTML = """
<!-- ══ MOBILE DRAWER ══ -->
<div class=\"drawer-overlay\" id=\"drawer-overlay\" onclick=\"fecharDrawerMobile()\"></div>
<div class=\"mobile-drawer\" id=\"mobile-drawer\">
  <!-- Cabeçalho do drawer -->
  <div class=\"drawer-head\">
    <div class=\"drawer-title\">🎰 PaynexBet</div>
    <button class=\"btn-close-drawer\" onclick=\"fecharDrawerMobile()\">✕</button>
  </div>

  <!-- Nav rápida -->
  <div class=\"drawer-quicknav\">
    <button class=\"dqn-btn\" onclick=\"fecharDrawerMobile();navTo('apostas')\">⚽ Apostas</button>
    <button class=\"dqn-btn\" onclick=\"fecharDrawerMobile();navTo('sorteio')\">🎲 Sorteios</button>
    <button class=\"dqn-btn\" onclick=\"fecharDrawerMobile();navTo('live')\"><span class=\"dot\"></span> Ao Vivo</button>
    <button class=\"dqn-btn\" onclick=\"fecharDrawerMobile();navTo('tabela')\">🏆 Tabela</button>
  </div>

  <!-- Conteúdo da sidebar clonado -->
  <div class=\"sb-section\">
    <div class=\"sb-label\">Em Alta</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('apostas','destaque')\"><span class=\"ico\">🔥</span>Em Destaque</div>
    <div id=\"drawer-ligas-em-alta\"></div>
  </div>
  <div class=\"sb-divider\"></div>
  <div class=\"sb-section\">
    <div class=\"sb-label\">Esportes</div>
    <div id=\"drawer-ligas-esportes\"></div>
  </div>
  <div class=\"sb-divider\"></div>
  <div class=\"sb-section\">
    <div class=\"sb-label\">Sorteios</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('sorteio')\"><span class=\"ico\">🎰</span>Participar Agora <span class=\"badge\" id=\"drawer-prize\">R$750</span></div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('bilhetes')\"><span class=\"ico\">📋</span>Meus Bilhetes</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('bolao')\"><span class=\"ico\">🎯</span>Bolão Placar</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('ganhadores')\"><span class=\"ico\">🏅</span>Ganhadores</div>
  </div>
  <div class=\"sb-divider\"></div>
  <div class=\"sb-section\">
    <div class=\"sb-label\">Minha Conta</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('depositar')\"><span class=\"ico\">💳</span>Depositar (Pix)</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('sacar')\"><span class=\"ico\">💰</span>Sacar</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('minhas-apostas')\"><span class=\"ico\">🎯</span>Minhas Apostas</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('historico')\"><span class=\"ico\">📊</span>Histórico</div>
  </div>
  <div class=\"sb-divider\"></div>
  <div class=\"sb-section\">
    <div class=\"sb-label\">Navegação</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('live')\"><span class=\"ico\">🔴</span>Ao Vivo</div>
    <div class=\"sb-item\" onclick=\"fecharDrawerMobile();sbNav('tabela')\"><span class=\"ico\">📊</span>Tabela</div>
  </div>
</div>
"""

DRAWER_JS = """
<script>
// ══ MOBILE DRAWER ══
(function() {
  let _drawerOpen = false;
  let _touchStartX = 0;
  let _touchStartY = 0;

  function abrirDrawerMobile() {
    _drawerOpen = true;
    const drawer = document.getElementById('mobile-drawer');
    const overlay = document.getElementById('drawer-overlay');
    if (!drawer || !overlay) return;
    overlay.style.display = 'block';
    // Forçar reflow para animação
    void overlay.offsetWidth;
    drawer.classList.add('open');
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
    // Sincronizar prêmio do sorteio
    const prize = document.getElementById('sb-prize');
    const drawerPrize = document.getElementById('drawer-prize');
    if (prize && drawerPrize) drawerPrize.textContent = prize.textContent;
    // Sincronizar ligas no drawer
    _sincronizarLigasDrawer();
  }

  function fecharDrawerMobile() {
    _drawerOpen = false;
    const drawer = document.getElementById('mobile-drawer');
    const overlay = document.getElementById('drawer-overlay');
    if (!drawer || !overlay) return;
    drawer.classList.remove('open');
    overlay.classList.remove('open');
    document.body.style.overflow = '';
    setTimeout(() => { overlay.style.display = 'none'; }, 280);
  }

  function _sincronizarLigasDrawer() {
    // Copiar itens das ligas da sidebar para o drawer
    const srcEmAlta = document.getElementById('sb-ligas-em-alta');
    const dstEmAlta = document.getElementById('drawer-ligas-em-alta');
    if (srcEmAlta && dstEmAlta) {
      dstEmAlta.innerHTML = srcEmAlta.innerHTML;
      // Atualizar onclick para fechar drawer
      dstEmAlta.querySelectorAll('.sb-item').forEach(el => {
        const orig = el.getAttribute('onclick') || '';
        if (orig && !orig.includes('fecharDrawerMobile')) {
          el.setAttribute('onclick', 'fecharDrawerMobile();' + orig);
        }
      });
    }
    const srcEsportes = document.getElementById('sb-ligas-esportes');
    const dstEsportes = document.getElementById('drawer-ligas-esportes');
    if (srcEsportes && dstEsportes) {
      dstEsportes.innerHTML = srcEsportes.innerHTML;
      dstEsportes.querySelectorAll('.sb-item').forEach(el => {
        const orig = el.getAttribute('onclick') || '';
        if (orig && !orig.includes('fecharDrawerMobile')) {
          el.setAttribute('onclick', 'fecharDrawerMobile();' + orig);
        }
      });
    }
  }

  // Swipe para fechar (arrastar da esquerda para direita fecha)
  document.addEventListener('touchstart', function(e) {
    _touchStartX = e.touches[0].clientX;
    _touchStartY = e.touches[0].clientY;
  }, { passive: true });

  document.addEventListener('touchend', function(e) {
    if (!_drawerOpen) return;
    const dx = e.changedTouches[0].clientX - _touchStartX;
    const dy = Math.abs(e.changedTouches[0].clientY - _touchStartY);
    if (dx < -60 && dy < 80) fecharDrawerMobile(); // swipe left
  }, { passive: true });

  // Fechar com Escape
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && _drawerOpen) fecharDrawerMobile();
  });

  // Expor funções globalmente
  window.abrirDrawerMobile = abrirDrawerMobile;
  window.fecharDrawerMobile = fecharDrawerMobile;
})();
</script>
"""

BODY_CLOSE = '</body>'
if 'mobile-drawer' not in html:
    idx_body = html.rfind(BODY_CLOSE)
    if idx_body != -1:
        html = html[:idx_body] + DRAWER_HTML + DRAWER_JS + '\n' + html[idx_body:]
        print("OK: drawer HTML + JS inserido antes de </body>")
    else:
        print("AVISO: </body> não encontrado")
else:
    print("OK: drawer já existe")

# ─────────────────────────────────────────────────────────────────
# Salvar
# ─────────────────────────────────────────────────────────────────
with open('home.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"OK: home.html salvo ({len(html)} chars)")
print("=== patch concluído ===")
