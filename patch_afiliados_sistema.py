#!/usr/bin/env python3
"""
Patch completo — Sistema de Afiliados
1. Novos endpoints server.py: cadastrar, editar %, deletar, stats, buscar usuário
2. Vincular indicado no cadastro (?ref=CODIGO)
3. Creditar comissão quando indicado ganha aposta
4. Admin.html: aba completa com formulário + tabela
"""
import re, psycopg2

DB = 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'

# ─────────────────────────────────────────────────────────────────────────────
# 1. NOVOS ENDPOINTS server.py
# ─────────────────────────────────────────────────────────────────────────────
NOVOS_ENDPOINTS = '''
# ═══════════════════════════════════════════════════════════════════════════════
# 🤝  SISTEMA DE AFILIADOS — Admin gerencia, comissão automática
# ═══════════════════════════════════════════════════════════════════════════════

async def route_admin_afiliados_list(request):
    """GET /api/admin/afiliados — lista afiliados com métricas completas."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver', 'relatorios_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        # Migração: garantir colunas necessárias
        for sql in [
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS referido_por INTEGER DEFAULT NULL",
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS codigo_indicacao VARCHAR(20) DEFAULT NULL",
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS comissao_pct NUMERIC(5,2) DEFAULT 10.0",
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS afiliado_ativo BOOLEAN DEFAULT FALSE",
            """CREATE TABLE IF NOT EXISTS comissoes_afiliados (
                id SERIAL PRIMARY KEY,
                afiliado_id INTEGER NOT NULL,
                indicado_id INTEGER NOT NULL,
                aposta_id   INTEGER,
                valor_aposta NUMERIC(10,2) DEFAULT 0,
                pct_comissao NUMERIC(5,2) DEFAULT 10.0,
                valor_comissao NUMERIC(10,2) DEFAULT 0,
                pago_em TIMESTAMP DEFAULT NOW(),
                descricao TEXT
            )""",
        ]:
            try: cur.execute(sql); conn.commit()
            except Exception: conn.rollback()

        cur.execute("""
            SELECT u.id, u.nome, u.cpf, u.codigo_indicacao,
                   COALESCE(u.comissao_pct, 10.0),
                   COALESCE(u.afiliado_ativo, FALSE),
                   COUNT(DISTINCT r.id) AS indicados,
                   COALESCE(SUM(a_r.valor),0) AS volume_indicados,
                   COALESCE(SUM(c.valor_comissao),0) AS total_comissao,
                   u.criado_em
            FROM usuarios u
            LEFT JOIN usuarios r ON r.referido_por=u.id
            LEFT JOIN apostas a_r ON a_r.usuario_id=r.id
            LEFT JOIN comissoes_afiliados c ON c.afiliado_id=u.id
            WHERE u.codigo_indicacao IS NOT NULL
            GROUP BY u.id ORDER BY COUNT(DISTINCT r.id) DESC LIMIT 100
        """)
        cols = ['id','nome','cpf','codigo','comissao_pct','ativo',
                'indicados','volume_indicados','total_comissao','criado_em']
        afiliados = []
        for r in cur.fetchall():
            d = dict(zip(cols, r))
            d['comissao_pct']    = float(d['comissao_pct'] or 10)
            d['volume_indicados']= float(d['volume_indicados'] or 0)
            d['total_comissao']  = float(d['total_comissao'] or 0)
            d['criado_em']       = str(d['criado_em'])[:10] if d['criado_em'] else ''
            d['link']            = f'https://paynexbet.com/?ref={d["codigo"]}'
            afiliados.append(d)

        cur.execute("SELECT COUNT(*) FROM usuarios WHERE referido_por IS NOT NULL")
        total_indicados = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(valor_comissao),0) FROM comissoes_afiliados")
        total_comissao_pago = float(cur.fetchone()[0] or 0)
        cur.close(); conn.close()
        return web.json_response({
            'ok': True, 'afiliados': afiliados,
            'total_indicados': total_indicados,
            'total_afiliados': len(afiliados),
            'total_comissao_pago': total_comissao_pago
        })
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


async def route_admin_afiliados_cadastrar(request):
    """POST /api/admin/afiliados/cadastrar — Admin cadastra usuário como afiliado."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        usuario_id   = int(body.get('usuario_id', 0))
        comissao_pct = float(body.get('comissao_pct', 10.0))
        comissao_pct = max(0.1, min(99.0, comissao_pct))
        if not usuario_id:
            return web.json_response({'ok': False, 'error': 'usuario_id obrigatório'})

        conn = _admin_db_connect(); cur = conn.cursor()
        # Migração segura
        for sql in [
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS codigo_indicacao VARCHAR(20) DEFAULT NULL",
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS referido_por INTEGER DEFAULT NULL",
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS comissao_pct NUMERIC(5,2) DEFAULT 10.0",
            "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS afiliado_ativo BOOLEAN DEFAULT FALSE",
        ]:
            try: cur.execute(sql); conn.commit()
            except Exception: conn.rollback()

        cur.execute("SELECT id, nome, cpf, codigo_indicacao FROM usuarios WHERE id=%s", (usuario_id,))
        u = cur.fetchone()
        if not u:
            cur.close(); conn.close()
            return web.json_response({'ok': False, 'error': 'Usuário não encontrado'})

        # Gerar código único se não tem
        import random, string
        codigo = u[3]
        if not codigo:
            while True:
                codigo = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                cur.execute("SELECT 1 FROM usuarios WHERE codigo_indicacao=%s", (codigo,))
                if not cur.fetchone():
                    break

        cur.execute("""
            UPDATE usuarios SET
                codigo_indicacao = %s,
                comissao_pct     = %s,
                afiliado_ativo   = TRUE
            WHERE id = %s
        """, (codigo, comissao_pct, usuario_id))
        conn.commit()
        cur.close(); conn.close()
        return web.json_response({
            'ok': True,
            'codigo': codigo,
            'comissao_pct': comissao_pct,
            'link': f'https://paynexbet.com/?ref={codigo}',
            'nome': u[1], 'cpf': u[2]
        })
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


async def route_admin_afiliados_editar(request):
    """PATCH /api/admin/afiliados/{id} — editar % comissão e status."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        usuario_id = int(request.match_info.get('id', 0))
        body = await request.json()
        conn = _admin_db_connect(); cur = conn.cursor()
        campos, vals = [], []
        if 'comissao_pct' in body:
            pct = max(0.1, min(99.0, float(body['comissao_pct'])))
            campos.append('comissao_pct=%s'); vals.append(pct)
        if 'ativo' in body:
            campos.append('afiliado_ativo=%s'); vals.append(bool(body['ativo']))
        if not campos:
            return web.json_response({'ok': False, 'error': 'Nada para atualizar'})
        vals.append(usuario_id)
        cur.execute(f"UPDATE usuarios SET {', '.join(campos)} WHERE id=%s", vals)
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'ok': True})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


async def route_admin_afiliados_remover(request):
    """DELETE /api/admin/afiliados/{id} — desativa afiliado (mantém código)."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        usuario_id = int(request.match_info.get('id', 0))
        conn = _admin_db_connect(); cur = conn.cursor()
        cur.execute("UPDATE usuarios SET afiliado_ativo=FALSE WHERE id=%s", (usuario_id,))
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'ok': True})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


async def route_admin_afiliados_buscar_usuario(request):
    """GET /api/admin/afiliados/buscar?q=nome_ou_cpf — busca usuário para cadastrar como afiliado."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    q = request.rel_url.query.get('q', '').strip()
    if len(q) < 2:
        return web.json_response({'ok': False, 'usuarios': []})
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        q_like = f'%{q}%'
        cpf_q  = ''.join(filter(str.isdigit, q))
        cur.execute("""
            SELECT id, nome, cpf, codigo_indicacao, COALESCE(comissao_pct,10), COALESCE(afiliado_ativo,FALSE)
            FROM usuarios
            WHERE nome ILIKE %s OR cpf LIKE %s
            ORDER BY nome LIMIT 10
        """, (q_like, f'%{cpf_q}%' if cpf_q else q_like))
        usuarios = [{'id':r[0],'nome':r[1],'cpf':r[2],'codigo':r[3],
                     'comissao_pct':float(r[4] or 10),'afiliado':bool(r[5])} for r in cur.fetchall()]
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'usuarios': usuarios})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


async def route_admin_afiliados_comissoes(request):
    """GET /api/admin/afiliados/{id}/comissoes — histórico de comissões pagas."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        usuario_id = int(request.match_info.get('id', 0))
        conn = _admin_db_connect(); cur = conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comissoes_afiliados (
                    id SERIAL PRIMARY KEY,
                    afiliado_id INTEGER NOT NULL,
                    indicado_id INTEGER NOT NULL,
                    aposta_id   INTEGER,
                    valor_aposta NUMERIC(10,2) DEFAULT 0,
                    pct_comissao NUMERIC(5,2) DEFAULT 10.0,
                    valor_comissao NUMERIC(10,2) DEFAULT 0,
                    pago_em TIMESTAMP DEFAULT NOW(),
                    descricao TEXT
                )
            """); conn.commit()
        except Exception: conn.rollback()
        cur.execute("""
            SELECT c.id, u.nome, c.valor_aposta, c.pct_comissao, c.valor_comissao, c.pago_em, c.descricao
            FROM comissoes_afiliados c
            LEFT JOIN usuarios u ON u.id=c.indicado_id
            WHERE c.afiliado_id=%s
            ORDER BY c.pago_em DESC LIMIT 50
        """, (usuario_id,))
        comissoes = [{'id':r[0],'indicado':r[1],'valor_aposta':float(r[2] or 0),
                      'pct':float(r[3] or 0),'valor_comissao':float(r[4] or 0),
                      'data':str(r[5])[:16] if r[5] else '','descricao':r[6] or ''} for r in cur.fetchall()]
        cur.execute("SELECT COALESCE(SUM(valor_comissao),0) FROM comissoes_afiliados WHERE afiliado_id=%s", (usuario_id,))
        total = float(cur.fetchone()[0] or 0)
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'comissoes': comissoes, 'total': total})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

'''

# ─────────────────────────────────────────────────────────────────────────────
# 2. PATCH NO CADASTRO DE USUÁRIO — vincular ref + creditar comissão na aposta
# ─────────────────────────────────────────────────────────────────────────────
PATCH_LOGIN_OLD = '''        if nome:
            # ── CADASTRO: Upsert — cria ou retorna existente, atualizando nome se vazio
            cur.execute("""
                INSERT INTO usuarios (cpf, nome, saldo)
                VALUES (%s, %s, 0)
                ON CONFLICT (cpf) DO UPDATE SET nome = CASE
                    WHEN usuarios.nome IS NULL OR usuarios.nome='' THEN EXCLUDED.nome
                    ELSE usuarios.nome END
                RETURNING id, nome, cpf, saldo
            """, (cpf, nome))'''

PATCH_LOGIN_NEW = '''        if nome:
            # ── CADASTRO: Upsert — cria ou retorna existente, atualizando nome se vazio
            ref_code = str(body.get('ref', '')).strip().upper()
            cur.execute("""
                INSERT INTO usuarios (cpf, nome, saldo)
                VALUES (%s, %s, 0)
                ON CONFLICT (cpf) DO UPDATE SET nome = CASE
                    WHEN usuarios.nome IS NULL OR usuarios.nome='' THEN EXCLUDED.nome
                    ELSE usuarios.nome END
                RETURNING id, nome, cpf, saldo
            """, (cpf, nome))
            row_new = cur.fetchone()
            # Vincular ao afiliado se vier ref e usuário novo (saldo=0 e recém criado)
            if ref_code and row_new:
                try:
                    cur.execute("""
                        SELECT id FROM usuarios
                        WHERE codigo_indicacao=%s AND COALESCE(afiliado_ativo,FALSE)=TRUE
                    """, (ref_code,))
                    af = cur.fetchone()
                    if af:
                        cur.execute("""
                            UPDATE usuarios SET referido_por=%s
                            WHERE id=%s AND (referido_por IS NULL)
                        """, (af[0], row_new[0]))
                        conn.commit()
                except Exception: pass'''

# ─────────────────────────────────────────────────────────────────────────────
# 3. CREDITAR COMISSÃO quando aposta é resolvida como ganhadora
# ─────────────────────────────────────────────────────────────────────────────
PATCH_COMISSAO = '''
async def _creditar_comissao_afiliado(usuario_id: int, valor_aposta: float, aposta_id=None):
    """Credita comissão ao afiliado quando indicado ganha uma aposta."""
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        # Buscar se o usuário tem afiliado que o indicou
        cur.execute("""
            SELECT u_af.id, u_af.nome, COALESCE(u_af.comissao_pct,10.0)
            FROM usuarios u_ind
            JOIN usuarios u_af ON u_af.id = u_ind.referido_por
            WHERE u_ind.id=%s AND COALESCE(u_af.afiliado_ativo,FALSE)=TRUE
        """, (usuario_id,))
        af = cur.fetchone()
        if not af:
            cur.close(); conn.close()
            return
        afiliado_id, af_nome, pct = af[0], af[1], float(af[2])
        valor_comissao = round(valor_aposta * pct / 100.0, 2)
        if valor_comissao <= 0:
            cur.close(); conn.close()
            return
        # Creditar saldo ao afiliado
        cur.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (valor_comissao, afiliado_id))
        # Registrar no histórico
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comissoes_afiliados (
                    id SERIAL PRIMARY KEY,
                    afiliado_id INTEGER NOT NULL,
                    indicado_id INTEGER NOT NULL,
                    aposta_id   INTEGER,
                    valor_aposta NUMERIC(10,2) DEFAULT 0,
                    pct_comissao NUMERIC(5,2) DEFAULT 10.0,
                    valor_comissao NUMERIC(10,2) DEFAULT 0,
                    pago_em TIMESTAMP DEFAULT NOW(),
                    descricao TEXT
                )
            """); conn.commit()
        except Exception: conn.rollback()
        cur.execute("""
            INSERT INTO comissoes_afiliados
                (afiliado_id, indicado_id, aposta_id, valor_aposta, pct_comissao, valor_comissao, descricao)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (afiliado_id, usuario_id, aposta_id, valor_aposta, pct, valor_comissao,
              f'Comissão {pct}% sobre aposta de R${valor_aposta:.2f}'))
        conn.commit()
        cur.close(); conn.close()
        print(f'[afiliado] Comissão R${valor_comissao:.2f} creditada ao afiliado {af_nome} (id={afiliado_id})', flush=True)
    except Exception as e:
        print(f'[afiliado] Erro ao creditar comissão: {e}', flush=True)

'''

# ─────────────────────────────────────────────────────────────────────────────
# 4. HOOK NA RESOLUÇÃO DE APOSTAS — chamar _creditar_comissao_afiliado
# ─────────────────────────────────────────────────────────────────────────────
PATCH_APOSTA_OLD = '''            cur.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (retorno, usuario_id))
            conn.commit()
            cur.execute("UPDATE apostas SET status='ganhou', resultado='ganhou', resolvido_em=NOW() WHERE id=%s", (aposta_id,))'''

PATCH_APOSTA_NEW = '''            cur.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (retorno, usuario_id))
            conn.commit()
            cur.execute("UPDATE apostas SET status='ganhou', resultado='ganhou', resolvido_em=NOW() WHERE id=%s", (aposta_id,))
            # Creditar comissão ao afiliado que indicou este usuário
            import asyncio as _asyncio
            try: _asyncio.ensure_future(_creditar_comissao_afiliado(usuario_id, float(valor_apostado or 0), aposta_id))
            except Exception: pass'''

# ─────────────────────────────────────────────────────────────────────────────
# 5. REGISTRAR ROTAS NO ROUTER
# ─────────────────────────────────────────────────────────────────────────────
ROUTER_OLD = '''    # Afiliados
    app.router.add_get('/api/admin/afiliados',          route_admin_afiliados_list)'''

ROUTER_NEW = '''    # Afiliados — Sistema completo
    app.router.add_get('/api/admin/afiliados',                    route_admin_afiliados_list)
    app.router.add_post('/api/admin/afiliados/cadastrar',         route_admin_afiliados_cadastrar)
    app.router.add_patch('/api/admin/afiliados/{id}',             route_admin_afiliados_editar)
    app.router.add_delete('/api/admin/afiliados/{id}',            route_admin_afiliados_remover)
    app.router.add_get('/api/admin/afiliados/buscar',             route_admin_afiliados_buscar_usuario)
    app.router.add_get('/api/admin/afiliados/{id}/comissoes',     route_admin_afiliados_comissoes)'''

# ─────────────────────────────────────────────────────────────────────────────
# APLICAR PATCHES NO server.py
# ─────────────────────────────────────────────────────────────────────────────
with open('server.py', 'r', encoding='utf-8') as f:
    srv = f.read()

# Substituir bloco antigo de route_admin_afiliados_list com o novo completo
OLD_AFIL_BLOCK = '''async def route_admin_afiliados_list(request):
    """GET /api/admin/afiliados — lista afiliados com métricas."""
    _ok, _staff = _staff_auth(request, 'afiliados_ver', 'relatorios_ver')
    if not _ok:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        # Garantir coluna referido_por
        try:
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS referido_por INTEGER DEFAULT NULL")
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS codigo_indicacao VARCHAR(20) DEFAULT NULL")
            conn.commit()
        except Exception: conn.rollback()
        cur.execute("""
            SELECT u.id, u.nome, u.cpf, u.codigo_indicacao,
                   COUNT(r.id) AS indicados,
                   COALESCE(SUM(a_r.valor),0) AS volume_indicados,
                   u.criado_em
            FROM usuarios u
            LEFT JOIN usuarios r ON r.referido_por=u.id
            LEFT JOIN apostas a_r ON a_r.usuario_id=r.id
            WHERE u.codigo_indicacao IS NOT NULL
            GROUP BY u.id ORDER BY COUNT(r.id) DESC LIMIT 50
        """)
        afiliados = [{'id': r[0], 'nome': r[1] or '?', 'cpf': r[2] or '',
                      'codigo': r[3] or '', 'indicados': r[4],
                      'volume_indicados': float(r[5] or 0),
                      'criado_em': str(r[6])[:10] if r[6] else ''} for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM usuarios WHERE referido_por IS NOT NULL")
        total_indicados = cur.fetchone()[0]
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'afiliados': afiliados,
                                  'total_indicados': total_indicados,
                                  'total_afiliados': len(afiliados)})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)'''

# Adicionar _creditar_comissao_afiliado antes da substituição do bloco de afiliados
BLOCO_NOTIF_HEADER = '''# ═══════════════════════════════════════════════════════════════════════════════
# 🔔  ITEM 8 — Notificações Telegram ao Admin
# ═══════════════════════════════════════════════════════════════════════════════'''

if OLD_AFIL_BLOCK in srv:
    srv = srv.replace(OLD_AFIL_BLOCK, NOVOS_ENDPOINTS.strip())
    print('✅ Bloco route_admin_afiliados_list substituído pelo sistema completo')
else:
    print('❌ Bloco antigo não encontrado — verificar manualmente')

# Adicionar função _creditar_comissao_afiliado antes do bloco de notificações
if BLOCO_NOTIF_HEADER in srv and '_creditar_comissao_afiliado' not in srv:
    srv = srv.replace(BLOCO_NOTIF_HEADER, PATCH_COMISSAO + BLOCO_NOTIF_HEADER)
    print('✅ Função _creditar_comissao_afiliado adicionada')
else:
    print('ℹ️  _creditar_comissao_afiliado já existe ou bloco de notif não encontrado')

# Patch no cadastro de usuário (route_bet_login)
if PATCH_LOGIN_OLD in srv:
    srv = srv.replace(PATCH_LOGIN_OLD, PATCH_LOGIN_NEW)
    print('✅ route_bet_login patched — vincula ref ao cadastrar')
else:
    print('⚠️  Patch do login não aplicado — verificar')

# Patch no hook de resolução de apostas
if PATCH_APOSTA_OLD in srv and 'creditar_comissao_afiliado' not in srv[srv.find(PATCH_APOSTA_OLD)-100:srv.find(PATCH_APOSTA_OLD)+500]:
    srv = srv.replace(PATCH_APOSTA_OLD, PATCH_APOSTA_NEW)
    print('✅ Hook de comissão adicionado na resolução de apostas')
else:
    print('ℹ️  Hook de comissão já aplicado ou PATCH_APOSTA_OLD não encontrado')

# Patch no router
if ROUTER_OLD in srv:
    srv = srv.replace(ROUTER_OLD, ROUTER_NEW)
    print('✅ Rotas de afiliados registradas no router')
else:
    print('⚠️  Router patch não aplicado')

with open('server.py', 'w', encoding='utf-8') as f:
    f.write(srv)
print(f'✅ server.py salvo: {len(srv)} chars')

# ─────────────────────────────────────────────────────────────────────────────
# 6. PATCH NO home.html — capturar ?ref= e enviar no cadastro
# ─────────────────────────────────────────────────────────────────────────────
with open('home.html', 'r', encoding='utf-8') as f:
    home = f.read()

# Capturar ref da URL e salvar no localStorage
CAPTURE_REF_OLD = "window.abrirDrawerMobile = function() {"
CAPTURE_REF_NEW = """// Capturar código de afiliado da URL (?ref=CODIGO)
(function(){
  try {
    var params = new URLSearchParams(window.location.search);
    var ref = params.get('ref');
    if (ref) localStorage.setItem('afl_ref', ref.toUpperCase());
  } catch(e){}
})();

window.abrirDrawerMobile = function() {"""

if CAPTURE_REF_OLD in home and 'afl_ref' not in home:
    home = home.replace(CAPTURE_REF_OLD, CAPTURE_REF_NEW)
    print('✅ home.html — captura de ?ref= adicionada')

# Enviar ref no cadastro
CADASTRO_OLD = """fetch(`${BASE}/api/bet/login`, {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({cpf: cleanCPF(document.getElementById('cad-cpf').value), nome: document.getElementById('cad-nome').value})"""

CADASTRO_NEW = """fetch(`${BASE}/api/bet/login`, {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({cpf: cleanCPF(document.getElementById('cad-cpf').value), nome: document.getElementById('cad-nome').value, ref: localStorage.getItem('afl_ref')||''})"""

if CADASTRO_OLD in home:
    home = home.replace(CADASTRO_OLD, CADASTRO_NEW)
    print('✅ home.html — ref enviado no cadastro')
else:
    print('⚠️  Patch de cadastro no home.html não encontrado')

with open('home.html', 'w', encoding='utf-8') as f:
    f.write(home)
print(f'✅ home.html salvo: {len(home)} chars')

print('\n✅ Patches aplicados! Agora aplicar admin.html...')
