#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch admin.html — Corrigir permissões de funcionários:
1. Adicionar permissão 'paypix_ver' no server.py
2. Garantir que PayPix/PayPix-Cob só aparecem com permissão paypix_ver
3. Adicionar permissão ao navMap e _abasPermStaff
4. Garantir que funcionários nunca veem tab-funcionarios
5. Melhorar UX do modal: grupos de permissão com categorias visuais
"""

# ═══════════════════════════════════
# PATCH 1 — navMap: corrigir paypix
# ═══════════════════════════════════
OLD_NAV_PAYPIX = """    'paypix':           false,
    'paypixcob':        false,
    'bot2':             false,
    'bot3':             false,"""

NEW_NAV_PAYPIX = """    'paypix':           perms.includes('paypix_ver'),
    'paypixcob':        perms.includes('paypix_ver'),
    'bot2':             perms.includes('paypix_ver'),
    'bot3':             perms.includes('paypix_ver'),"""

# ═══════════════════════════════════
# PATCH 2 — showTab: adicionar paypix nas abas permitidas
# ═══════════════════════════════════
OLD_SHOW_NOVAS = """      // Novas abas adicionadas
      'sorteio':            _permsStaff.includes('sorteio_ver'),
      'campeonatos':        _permsStaff.includes('campeonatos_ver'),
      'config-pagamento':   _permsStaff.includes('config_ver'),
      'limites':            _permsStaff.includes('limites_ver'),
      'alertas':            _permsStaff.includes('alertas_ver'),
      'jogo-responsavel':   _permsStaff.includes('jogo_responsavel_ver'),
      'afiliados':          _permsStaff.includes('afiliados_ver'),
      'notificacoes':       _permsStaff.includes('notificacoes_ver'),
    };"""

NEW_SHOW_NOVAS = """      // Novas abas adicionadas
      'sorteio':            _permsStaff.includes('sorteio_ver'),
      'campeonatos':        _permsStaff.includes('campeonatos_ver'),
      'config-pagamento':   _permsStaff.includes('config_ver'),
      'limites':            _permsStaff.includes('limites_ver'),
      'alertas':            _permsStaff.includes('alertas_ver'),
      'jogo-responsavel':   _permsStaff.includes('jogo_responsavel_ver'),
      'afiliados':          _permsStaff.includes('afiliados_ver'),
      'notificacoes':       _permsStaff.includes('notificacoes_ver'),
      'paypix':             _permsStaff.includes('paypix_ver'),
      'paypixcob':          _permsStaff.includes('paypix_ver'),
      'bot2':               _permsStaff.includes('paypix_ver'),
      'bot3':               _permsStaff.includes('paypix_ver'),
      // Funcionários: NUNCA para staff
      'funcionarios':       false,
    };"""

# ═══════════════════════════════════
# PATCH 3 — _STAFF_ABA_PERM: adicionar paypix_ver
# ═══════════════════════════════════
OLD_ABA_PERM = """  'sorteio_ver':          'sorteio',
};"""

NEW_ABA_PERM = """  'sorteio_ver':          'sorteio',
  'paypix_ver':           'paypix',
};"""

# ═══════════════════════════════════
# PATCH 4 — Modal: melhorar layout de permissões em grupos
# ═══════════════════════════════════
OLD_BUILD_PERMS = """function _buildPermsCheckboxes(selecionadas = []) {
  const container = document.getElementById('staff-perms-checkboxes');
  if (!container) return;
  const entries = Object.entries(_staffPermsMap);
  if (!entries.length) {
    container.innerHTML = '<div style=\"color:#f87171;font-size:12px;padding:8px\">❌ Nenhuma permissão disponível. Recarregue.</div>';
    return;
  }
  container.innerHTML = entries.map(([k, v]) => {
    const checked = selecionadas.includes(k) ? 'checked' : '';
    return `<label style=\"display:flex;align-items:center;gap:8px;background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:8px 10px;cursor:pointer;font-size:12px\">
      <input type=\"checkbox\" value=\"${k}\" ${checked} style=\"accent-color:#8b5cf6;width:15px;height:15px\">
      <span><strong style=\"color:#c4b5fd;display:block;font-size:11px\">${k}</strong><span style=\"color:var(--text2)\">${v}</span></span>
    </label>`;
  }).join('');
}"""

NEW_BUILD_PERMS = """const _PERM_GRUPOS = {
  '👥 Usuários':       ['usuarios_ver','usuarios_ajustar','usuarios_suspender'],
  '🎯 Apostas':        ['apostas_ver','apostas_resolver'],
  '💰 Financeiro':     ['depositos_ver','saques_ver','saques_aprovar'],
  '🎲 Sorteio':        ['sorteio_ver'],
  '🎯 Bolão':          ['bolao_ver','bolao_gerir'],
  '💸 PayPix':         ['paypix_ver'],
  '📊 Relatórios':     ['relatorios_ver','alertas_ver'],
  '🎁 Promoções':      ['bonus_criar','notificacoes_ver'],
  '🛡️ Compliance':     ['jogo_responsavel_ver','afiliados_ver'],
  '⚙️ Configurações':  ['campeonatos_ver','limites_ver','config_ver'],
};

function _buildPermsCheckboxes(selecionadas = []) {
  const container = document.getElementById('staff-perms-checkboxes');
  if (!container) return;
  const allPerms = Object.keys(_staffPermsMap);
  if (!allPerms.length) {
    container.innerHTML = '<div style=\"color:#f87171;font-size:12px;padding:8px\">❌ Nenhuma permissão disponível. Recarregue.</div>';
    return;
  }

  // Coletar permissões já atribuídas a grupos
  const jaAtribuidas = new Set(Object.values(_PERM_GRUPOS).flat());
  // Permissões sem grupo (fallback)
  const semGrupo = allPerms.filter(k => !jaAtribuidas.has(k));

  let html = '';

  // Renderizar por grupo
  for (const [grupo, permsGrupo] of Object.entries(_PERM_GRUPOS)) {
    const permsExistentes = permsGrupo.filter(k => _staffPermsMap[k]);
    if (!permsExistentes.length) continue;

    html += \`<div style="grid-column:1/-1;margin-top:12px;margin-bottom:4px">
      <span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#64748b;border-bottom:1px solid rgba(255,255,255,0.06);padding-bottom:4px;display:block">\${grupo}</span>
    </div>\`;

    for (const k of permsExistentes) {
      const v = _staffPermsMap[k] || k;
      const checked = selecionadas.includes(k) ? 'checked' : '';
      html += \`<label style="display:flex;align-items:center;gap:8px;background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:8px 10px;cursor:pointer;font-size:12px">
        <input type="checkbox" value="\${k}" \${checked} style="accent-color:#8b5cf6;width:15px;height:15px">
        <span><strong style="color:#c4b5fd;display:block;font-size:11px">\${k}</strong><span style="color:var(--text2)">\${v}</span></span>
      </label>\`;
    }
  }

  // Permissões sem grupo
  if (semGrupo.length) {
    html += \`<div style="grid-column:1/-1;margin-top:12px;margin-bottom:4px">
      <span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#64748b;border-bottom:1px solid rgba(255,255,255,0.06);padding-bottom:4px;display:block">🔧 Outras</span>
    </div>\`;
    for (const k of semGrupo) {
      const v = _staffPermsMap[k] || k;
      const checked = selecionadas.includes(k) ? 'checked' : '';
      html += \`<label style="display:flex;align-items:center;gap:8px;background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:8px 10px;cursor:pointer;font-size:12px">
        <input type="checkbox" value="\${k}" \${checked} style="accent-color:#8b5cf6;width:15px;height:15px">
        <span><strong style="color:#c4b5fd;display:block;font-size:11px">\${k}</strong><span style="color:var(--text2)">\${v}</span></span>
      </label>\`;
    }
  }

  container.style.display = 'grid';
  container.style.gridTemplateColumns = '1fr 1fr';
  container.style.gap = '6px';
  container.innerHTML = html;
}"""


# ═══════════════════════════════════
# PATCH 5 — Adicionar "Perfis Rápidos" no modal staff
# ═══════════════════════════════════
OLD_SELECT_ALL = """            <div style=\"display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap\">
              <button onclick=\"staffSelectAll(true)\"  style=\"background:var(--card2);border:1px solid #8b5cf6;border-radius:6px;color:#c4b5fd;font-size:11px;padding:4px 10px;cursor:pointer\">✅ Selecionar todas</button>
              <button onclick=\"staffSelectAll(false)\" style=\"background:var(--card2);border:1px solid var(--border);border-radius:6px;color:var(--text2);font-size:11px;padding:4px 10px;cursor:pointer\">❌ Desmarcar todas</button>
            </div>"""

NEW_SELECT_ALL = """            <!-- Perfis Rápidos -->
            <div style=\"margin-bottom:10px\">
              <div style=\"font-size:11px;color:#64748b;margin-bottom:6px;font-weight:600\">Perfis rápidos:</div>
              <div style=\"display:flex;gap:6px;flex-wrap:wrap\">
                <button onclick=\"_perfilRapido('atendente')\" style=\"background:rgba(59,130,246,0.12);border:1px solid rgba(59,130,246,0.3);border-radius:6px;color:#60a5fa;font-size:11px;padding:4px 10px;cursor:pointer\" title=\"Ver usuários, depósitos, saques\">👤 Atendente</button>
                <button onclick=\"_perfilRapido('financeiro')\" style=\"background:rgba(34,197,94,0.12);border:1px solid rgba(34,197,94,0.3);border-radius:6px;color:#4ade80;font-size:11px;padding:4px 10px;cursor:pointer\" title=\"Depósitos, saques, saldos\">💰 Financeiro</button>
                <button onclick=\"_perfilRapido('operador')\" style=\"background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.3);border-radius:6px;color:#fbbf24;font-size:11px;padding:4px 10px;cursor:pointer\" title=\"Apostas, bolão, sorteio\">🎯 Operador</button>
                <button onclick=\"_perfilRapido('gerente')\" style=\"background:rgba(139,92,246,0.12);border:1px solid rgba(139,92,246,0.3);border-radius:6px;color:#c4b5fd;font-size:11px;padding:4px 10px;cursor:pointer\" title=\"Acesso amplo exceto config\">👑 Gerente</button>
                <button onclick=\"staffSelectAll(true)\"  style=\"background:var(--card2);border:1px solid #8b5cf6;border-radius:6px;color:#c4b5fd;font-size:11px;padding:4px 10px;cursor:pointer\">✅ Tudo</button>
                <button onclick=\"staffSelectAll(false)\" style=\"background:var(--card2);border:1px solid var(--border);border-radius:6px;color:var(--text2);font-size:11px;padding:4px 10px;cursor:pointer\">❌ Nenhuma</button>
              </div>
            </div>"""


# ═══════════════════════════════════
# PATCH 6 — Adicionar função _perfilRapido
# ═══════════════════════════════════
OLD_STAFF_SELECT = """function staffSelectAll(val) {
  document.querySelectorAll('#staff-perms-checkboxes input[type=checkbox]')
    .forEach(cb => cb.checked = val);
}"""

NEW_STAFF_SELECT = """function staffSelectAll(val) {
  document.querySelectorAll('#staff-perms-checkboxes input[type=checkbox]')
    .forEach(cb => cb.checked = val);
}

const _PERFIS_RAPIDOS = {
  'atendente': ['usuarios_ver','depositos_ver','saques_ver'],
  'financeiro': ['usuarios_ver','usuarios_ajustar','depositos_ver','saques_ver','saques_aprovar','relatorios_ver'],
  'operador':   ['apostas_ver','apostas_resolver','bolao_ver','bolao_gerir','sorteio_ver','multi_ver'],
  'gerente':    ['usuarios_ver','usuarios_ajustar','usuarios_suspender',
                 'apostas_ver','apostas_resolver',
                 'depositos_ver','saques_ver','saques_aprovar',
                 'bolao_ver','bolao_gerir','sorteio_ver',
                 'relatorios_ver','alertas_ver','bonus_criar',
                 'notificacoes_ver','jogo_responsavel_ver','afiliados_ver',
                 'campeonatos_ver','limites_ver'],
};

function _perfilRapido(perfil) {
  const perms = _PERFIS_RAPIDOS[perfil] || [];
  document.querySelectorAll('#staff-perms-checkboxes input[type=checkbox]').forEach(cb => {
    cb.checked = perms.includes(cb.value);
  });
  // Flash feedback
  const btn = event && event.target;
  if (btn) {
    const orig = btn.textContent;
    btn.textContent = '✓ Aplicado!';
    setTimeout(() => { btn.textContent = orig; }, 1200);
  }
}"""


def main():
    with open('admin.html', 'r', encoding='utf-8') as f:
        content = f.read()

    errors = []

    # 1. navMap paypix
    if OLD_NAV_PAYPIX in content:
        content = content.replace(OLD_NAV_PAYPIX, NEW_NAV_PAYPIX, 1)
        print("OK: navMap — paypix/paypixcob/bot2/bot3 com permissão paypix_ver")
    else:
        errors.append("ERRO: OLD_NAV_PAYPIX não encontrado")

    # 2. showTab abas permitidas
    if OLD_SHOW_NOVAS in content:
        content = content.replace(OLD_SHOW_NOVAS, NEW_SHOW_NOVAS, 1)
        print("OK: showTab — paypix/funcionarios adicionados ao bloqueio staff")
    else:
        errors.append("ERRO: OLD_SHOW_NOVAS não encontrado")
        idx = content.find("'notificacoes':     _permsStaff")
        print(f"  Marcador showTab em pos: {idx}")

    # 3. _STAFF_ABA_PERM
    if OLD_ABA_PERM in content:
        content = content.replace(OLD_ABA_PERM, NEW_ABA_PERM, 1)
        print("OK: _STAFF_ABA_PERM — paypix_ver adicionado")
    else:
        errors.append("AVISO: _STAFF_ABA_PERM não encontrado")

    # 4. _buildPermsCheckboxes com grupos
    if OLD_BUILD_PERMS in content:
        content = content.replace(OLD_BUILD_PERMS, NEW_BUILD_PERMS, 1)
        print("OK: _buildPermsCheckboxes com grupos visuais")
    else:
        errors.append("ERRO: _buildPermsCheckboxes não encontrado")

    # 5. Perfis Rápidos no modal
    if OLD_SELECT_ALL in content:
        content = content.replace(OLD_SELECT_ALL, NEW_SELECT_ALL, 1)
        print("OK: Perfis Rápidos adicionados no modal")
    else:
        errors.append("ERRO: botões Select All no modal não encontrados")

    # 6. Função _perfilRapido
    if OLD_STAFF_SELECT in content:
        content = content.replace(OLD_STAFF_SELECT, NEW_STAFF_SELECT, 1)
        print("OK: função _perfilRapido adicionada")
    else:
        errors.append("ERRO: staffSelectAll não encontrado para inserir _perfilRapido")

    if errors:
        print("\n=== ERROS ===")
        for e in errors:
            print(e)

    with open('admin.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"\nadmin.html salvo: {len(content)} chars")

    checks = [
        ("paypix_ver", "permissão paypix_ver mapeada"),
        ("_PERM_GRUPOS", "grupos de permissões"),
        ("_perfilRapido", "função _perfilRapido"),
        ("_PERFIS_RAPIDOS", "perfis rápidos"),
        ("Atendente", "botão perfil Atendente"),
        ("Gerente", "botão perfil Gerente"),
    ]
    for key, label in checks:
        print(f"  {'OK' if key in content else 'FALTA'}: {label}")


if __name__ == '__main__':
    main()
