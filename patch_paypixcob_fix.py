#!/usr/bin/env python3
"""
Patch: corrige PayPix-Cob (/paypixcob 404 + % sem função)
1. Cria rota /paypixcob no server.py (alias para /bot)
2. Cria endpoint /api/pc/config-pct (GET + POST) para % da página pública
3. Atualiza route_pc_config_save para aceitar paypix_pct, paypix_ativo, paypix_descricao, paypix_min
4. Cria endpoint público /api/pc/config-publica (sem auth) para a página ler a %
"""

import re

print("=== patch_paypixcob_fix.py ===")

# ─────────────────────────────────────────────────────────────────
# Ler server.py
# ─────────────────────────────────────────────────────────────────
with open('server.py', 'r', encoding='utf-8') as f:
    srv = f.read()

# ─────────────────────────────────────────────────────────────────
# 1. Adicionar rota /paypixcob (alias /bot) no registro de rotas
# ─────────────────────────────────────────────────────────────────
OLD_ROUTE = "    app.router.add_get('/bot',                              route_bot_pix_page)\n    app.router.add_get('/pix',                              route_pix_page)"
NEW_ROUTE = "    app.router.add_get('/bot',                              route_bot_pix_page)\n    app.router.add_get('/paypixcob',                        route_bot_pix_page)  # alias público\n    app.router.add_get('/pix',                              route_pix_page)"

if '/paypixcob' in srv:
    print("OK: rota /paypixcob já existe")
elif OLD_ROUTE in srv:
    srv = srv.replace(OLD_ROUTE, NEW_ROUTE)
    print("OK: rota /paypixcob adicionada")
else:
    print("AVISO: ponto de inserção /bot não encontrado — pulando rota /paypixcob")

# ─────────────────────────────────────────────────────────────────
# 2. Atualizar route_pc_config_save para salvar % também
#    Substituir o trecho que define os pares de chaves
# ─────────────────────────────────────────────────────────────────
OLD_PC_SAVE = """        pares = [
            ('pc_access_token',  token_final),
            ('pc_public_key',    pubkey_final),
            ('pc_client_id',     cid_final),
            ('pc_client_secret', csec_final),
        ]
        for chave, valor in pares:
            if valor:
                cur.execute('''
                    INSERT INTO pc_config (chave, valor, atualizado_em)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                ''', (chave, valor))
        conn.commit()
        cur.close(); conn.close()

        suffix = token_final[-6:] if token_final else '???'
        print(f'✅ [pc_config] Chaves PayPix-Cob atualizadas. Token: ...{suffix}', flush=True)
        return web.json_response({'success': True, 'mensagem': 'Chaves PayPix-Cob salvas com sucesso!'})"""

NEW_PC_SAVE = """        # Campos de % (opcionais — só atualiza se enviados)
        paypix_pct_raw  = body.get('paypix_pct')
        paypix_ativo    = body.get('paypix_ativo')
        paypix_descricao = body.get('paypix_descricao')
        paypix_min_raw  = body.get('paypix_min')

        pares = [
            ('pc_access_token',  token_final),
            ('pc_public_key',    pubkey_final),
            ('pc_client_id',     cid_final),
            ('pc_client_secret', csec_final),
        ]
        # Adicionar campos de % se enviados
        if paypix_pct_raw is not None:
            pct_val = max(0.01, min(0.99, float(paypix_pct_raw) / 100.0))
            pares.append(('pc_pct', str(round(pct_val, 4))))
        if paypix_ativo is not None:
            pares.append(('pc_ativo', '1' if paypix_ativo else '0'))
        if paypix_descricao is not None:
            pares.append(('pc_descricao', str(paypix_descricao).strip()))
        if paypix_min_raw is not None:
            pares.append(('pc_min', str(max(1.0, float(paypix_min_raw)))))

        for chave, valor in pares:
            if valor is not None:
                cur.execute('''
                    INSERT INTO pc_config (chave, valor, atualizado_em)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                ''', (chave, valor))
        conn.commit()

        # Retornar configuração atualizada
        cur.execute("SELECT valor FROM pc_config WHERE chave='pc_pct'")
        r_pct = cur.fetchone()
        pct_atual = float(r_pct[0]) if r_pct else 0.6
        cur.close(); conn.close()

        suffix = token_final[-6:] if token_final else '???'
        print(f'✅ [pc_config] PayPix-Cob atualizado. Token: ...{suffix} | pct: {pct_atual:.0%}', flush=True)
        return web.json_response({
            'success': True,
            'mensagem': 'PayPix-Cob salvo com sucesso!',
            'paypix_pct': pct_atual,
            'paypix_pct_display': f'{round(pct_atual * 100)}%',
        })"""

if OLD_PC_SAVE in srv:
    srv = srv.replace(OLD_PC_SAVE, NEW_PC_SAVE)
    print("OK: route_pc_config_save atualizado para salvar %")
else:
    print("AVISO: bloco OLD_PC_SAVE não encontrado — verificar manualmente")

# ─────────────────────────────────────────────────────────────────
# 3. Adicionar endpoint público /api/pc/config-publica (sem auth)
#    Inserir logo após route_pc_testar
# ─────────────────────────────────────────────────────────────────
NEW_PC_CONFIG_PUBLICA = '''

async def route_pc_config_publica(request):
    """GET /api/pc/config-publica — Retorna % e status do PayPix-Cob para a página pública (sem auth)."""
    try:
        import psycopg2 as _pg_pc
        conn = _pg_pc.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        config = {}
        for chave in ['pc_pct', 'pc_ativo', 'pc_descricao', 'pc_min']:
            try:
                cur.execute("SELECT valor FROM pc_config WHERE chave=%s", (chave,))
                row = cur.fetchone()
                config[chave] = (row[0] or '').strip() if row else ''
            except Exception:
                config[chave] = ''
        cur.close(); conn.close()

        pct = float(config.get('pc_pct') or 0.6)
        ativo = config.get('pc_ativo', '1') not in ('0', 'false', 'False')
        descricao = config.get('pc_descricao') or f'Gere seu Pix e receba {round(pct*100)}% do valor'
        min_val = float(config.get('pc_min') or 5.0)

        return web.json_response({
            'success': True,
            'paypix_pct':      pct,
            'paypix_pct_display': f'{round(pct * 100)}%',
            'paypix_ativo':    ativo,
            'paypix_descricao': descricao,
            'paypix_min':      min_val,
        })
    except Exception as e:
        return web.json_response({
            'success': False,
            'paypix_pct': 0.6,
            'paypix_pct_display': '60%',
            'paypix_ativo': True,
            'paypix_descricao': 'Gere seu Pix e receba 60% do valor',
            'paypix_min': 5.0,
            'error': str(e),
        })

'''

if 'route_pc_config_publica' in srv:
    print("OK: route_pc_config_publica já existe")
else:
    # Inserir após route_pc_testar (antes da linha que começa o próximo async def após ela)
    marker = 'async def route_pc_testar(request):'
    idx = srv.find(marker)
    if idx != -1:
        # Encontrar o próximo 'async def' após route_pc_testar
        next_async = srv.find('\nasync def ', idx + len(marker))
        if next_async != -1:
            srv = srv[:next_async] + NEW_PC_CONFIG_PUBLICA + srv[next_async:]
            print("OK: route_pc_config_publica adicionado após route_pc_testar")
        else:
            print("AVISO: próximo async def não encontrado após route_pc_testar")
    else:
        print("AVISO: marker route_pc_testar não encontrado")

# ─────────────────────────────────────────────────────────────────
# 4. Registrar rota /api/pc/config-publica no app.router
# ─────────────────────────────────────────────────────────────────
OLD_PC_ROUTES = "    app.router.add_get('/api/pc/config',                    route_pc_config_get)\n    app.router.add_post('/api/pc/config',                   route_pc_config_save)"
NEW_PC_ROUTES = "    app.router.add_get('/api/pc/config',                    route_pc_config_get)\n    app.router.add_post('/api/pc/config',                   route_pc_config_save)\n    app.router.add_get('/api/pc/config-publica',            route_pc_config_publica)"

if 'api/pc/config-publica' in srv:
    print("OK: rota /api/pc/config-publica já registrada")
elif OLD_PC_ROUTES in srv:
    srv = srv.replace(OLD_PC_ROUTES, NEW_PC_ROUTES)
    print("OK: rota /api/pc/config-publica registrada no app.router")
else:
    print("AVISO: ponto de inserção /api/pc/config não encontrado")

# ─────────────────────────────────────────────────────────────────
# Salvar server.py
# ─────────────────────────────────────────────────────────────────
with open('server.py', 'w', encoding='utf-8') as f:
    f.write(srv)
print(f"OK: server.py salvo ({len(srv)} chars)")

# ─────────────────────────────────────────────────────────────────
# Atualizar admin.html — _legacyCarregarPaypixCobAdmin
# deve usar /api/pc/config-publica ao invés de /api/pc/config para carregar %
# ─────────────────────────────────────────────────────────────────
with open('admin.html', 'r', encoding='utf-8') as f:
    adm = f.read()

# Atualizar _legacyCarregarPaypixCobAdmin para usar config-publica para % e ativo
OLD_LEGACY = """async function _legacyCarregarPaypixCobAdmin() {
  try {
    const r = await fetch(`${BASE}/api/pc/config`);
    const d = await r.json();
    _pcCfg = d;
    const pct = Math.round((d.paypix_pct || 0.6) * 100);"""

NEW_LEGACY = """async function _legacyCarregarPaypixCobAdmin() {
  try {
    // Carregar % via endpoint público (sem auth)
    const rPub = await fetch(`${BASE}/api/pc/config-publica`);
    const dPub = await rPub.json();
    _pcCfg = dPub;
    const pct = Math.round((dPub.paypix_pct || 0.6) * 100);"""

if OLD_LEGACY in adm:
    adm = adm.replace(OLD_LEGACY, NEW_LEGACY)
    print("OK: _legacyCarregarPaypixCobAdmin usa /api/pc/config-publica")
else:
    print("AVISO: _legacyCarregarPaypixCobAdmin não encontrado — verificar")

# Corrigir também as referências a d. por dPub. nas linhas seguintes
OLD_LEGACY_REFS = """    const platPct = 100 - pct;

    // Métricas
    document.getElementById('pc-pct-display').textContent = pct + '%';
    document.getElementById('pc-plat-display').textContent = platPct + '%';
    const ativo = d.paypix_ativo !== false;"""

NEW_LEGACY_REFS = """    const platPct = 100 - pct;

    // Métricas
    document.getElementById('pc-pct-display').textContent = pct + '%';
    document.getElementById('pc-plat-display').textContent = platPct + '%';
    const ativo = dPub.paypix_ativo !== false;"""

if OLD_LEGACY_REFS in adm:
    adm = adm.replace(OLD_LEGACY_REFS, NEW_LEGACY_REFS)
    print("OK: referências d.paypix_ativo corrigidas para dPub")

OLD_LEGACY_FORM = """    atualizarSliderPaypixCob(pct);
    document.getElementById('pc-ativo').value = ativo ? '1' : '0';
    document.getElementById('pc-descricao').value = d.paypix_descricao || '';
    document.getElementById('pc-preview-desc').textContent = d.paypix_descricao || 'Gere seu Pix e receba ' + pct + '% do valor';
    document.getElementById('pc-preview-pct').textContent = pct + '%';
    // Valor mínimo
    const minEl = document.getElementById('pc-min-input');
    if (minEl) minEl.value = typeof d.paypix_min === 'number' ? d.paypix_min : 5;"""

NEW_LEGACY_FORM = """    atualizarSliderPaypixCob(pct);
    document.getElementById('pc-ativo').value = ativo ? '1' : '0';
    document.getElementById('pc-descricao').value = dPub.paypix_descricao || '';
    document.getElementById('pc-preview-desc').textContent = dPub.paypix_descricao || 'Gere seu Pix e receba ' + pct + '% do valor';
    document.getElementById('pc-preview-pct').textContent = pct + '%';
    // Valor mínimo
    const minEl = document.getElementById('pc-min-input');
    if (minEl) minEl.value = typeof dPub.paypix_min === 'number' ? dPub.paypix_min : 5;"""

if OLD_LEGACY_FORM in adm:
    adm = adm.replace(OLD_LEGACY_FORM, NEW_LEGACY_FORM)
    print("OK: formulário _legacy corrigido para dPub")

with open('admin.html', 'w', encoding='utf-8') as f:
    f.write(adm)
print(f"OK: admin.html salvo ({len(adm)} chars)")
print("=== patch concluído ===")
