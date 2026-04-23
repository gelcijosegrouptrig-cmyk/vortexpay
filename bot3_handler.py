"""
bot3_handler.py — Bot Mercado Pago 2 (@paypix_nexbot2)  (VERSÃO 1 — Réplica do Bot2)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Arquitetura: webhook puro (sem polling, sem thread)
  POST /webhook/bot3  →  process_bot3_update(data)

Como ativar:
  1. Crie um novo bot no @BotFather e copie o token
  2. No painel admin → Bot MP2 → Cole o token → Salvar Token
  3. Clique "Registrar Webhook Bot"
  4. Pronto — o bot responde imediatamente

Comandos do bot:
  /start    — Bem-vindo + menu principal
  /carteira — Ver saldo
  /depositar — Gerar PIX para depositar
  /sacar    — Solicitar saque
  /historico — Ver transações
  /indicar  — Link de indicação
  /ajuda    — Ajuda

Tabelas: mp3_* (PostgreSQL — independentes das tabelas mp2_*)
Token:   banco mp3_config.chave='bot3_token'  ou env BOT3_TOKEN
MP:      banco mp3_config.chave='mp3_access_token' ou env MP3_ACCESS_TOKEN
"""

import os
import json
import asyncio
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime

import psycopg2
import psycopg2.extras

logger = logging.getLogger('bot3')

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'
)

BOT3_USERNAME = os.environ.get('BOT3_USERNAME', 'paypix_nexbot2')


# ─── INICIALIZAR TABELAS mp3_* ────────────────────────────────────────────────

def init_mp3_tables():
    """Cria as tabelas mp3_* se não existirem (réplica das mp2_*)."""
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur = conn.cursor()
        sqls = [
            """CREATE TABLE IF NOT EXISTS mp3_config (
                chave TEXT PRIMARY KEY,
                valor TEXT,
                atualizado_em TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS mp3_usuarios (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                nome TEXT,
                saldo NUMERIC(12,2) DEFAULT 0,
                total_depositado NUMERIC(12,2) DEFAULT 0,
                total_sacado NUMERIC(12,2) DEFAULT 0,
                referido_por BIGINT,
                criado_em TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS mp3_transacoes (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT,
                tipo TEXT DEFAULT 'deposito',
                valor NUMERIC(12,2),
                status TEXT DEFAULT 'pendente',
                mp_payment_id TEXT,
                mp_external_ref TEXT,
                pix_copia_cola TEXT,
                pix_qr_base64 TEXT,
                descricao TEXT,
                parceiro_codigo VARCHAR(50),
                comissao_valor NUMERIC(12,2) DEFAULT 0,
                comissao_status VARCHAR(20) DEFAULT 'pendente',
                criado_em TIMESTAMP DEFAULT NOW(),
                atualizado_em TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS mp3_saques (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT,
                valor NUMERIC(12,2),
                chave_pix TEXT,
                tipo_chave TEXT,
                status TEXT DEFAULT 'pendente',
                obs TEXT,
                criado_em TIMESTAMP DEFAULT NOW(),
                processado_em TIMESTAMP
            )""",
            """CREATE TABLE IF NOT EXISTS mp3_parceiros (
                id SERIAL PRIMARY KEY,
                codigo VARCHAR(50) UNIQUE NOT NULL,
                nome VARCHAR(200) NOT NULL,
                chave_pix VARCHAR(200) NOT NULL,
                tipo_chave VARCHAR(20) DEFAULT 'email',
                comissao_pct NUMERIC(5,2) DEFAULT 10.0,
                ativo BOOLEAN DEFAULT TRUE,
                total_gerado NUMERIC(12,2) DEFAULT 0,
                total_comissao NUMERIC(12,2) DEFAULT 0,
                total_pago NUMERIC(12,2) DEFAULT 0,
                criado_em TIMESTAMP DEFAULT NOW(),
                link TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS mp3_comissoes (
                id SERIAL PRIMARY KEY,
                parceiro_codigo VARCHAR(50),
                transacao_id INTEGER,
                valor_total NUMERIC(12,2),
                valor_comissao NUMERIC(12,2),
                comissao_pct NUMERIC(5,2),
                status VARCHAR(20) DEFAULT 'pendente',
                criado_em TIMESTAMP DEFAULT NOW(),
                pago_em TIMESTAMP
            )""",
        ]
        for sql in sqls:
            cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        logger.info('[bot3] Tabelas mp3_* verificadas/criadas OK')
    except Exception as e:
        logger.error(f'[bot3] init_mp3_tables: {e}')


# ─── BANCO ───────────────────────────────────────────────────────────────────

def _conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=8)


def _cfg(chave: str) -> str:
    """Lê valor de mp3_config."""
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("SELECT valor FROM mp3_config WHERE chave=%s", (chave,))
        row = cur.fetchone()
        cur.close(); conn.close()
        return (row[0] or '').strip() if row else ''
    except Exception as e:
        logger.warning(f'[bot3._cfg] {chave}: {e}')
        return ''


def _cfg_set(chave: str, valor: str):
    """Salva valor em mp3_config (upsert)."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO mp3_config (chave, valor) VALUES (%s, %s) "
        "ON CONFLICT (chave) DO UPDATE SET valor=%s, atualizado_em=NOW()",
        (chave, valor, valor)
    )
    conn.commit(); cur.close(); conn.close()


# ─── TOKEN ───────────────────────────────────────────────────────────────────

def get_token() -> str:
    t = os.environ.get('BOT3_TOKEN', '').strip()
    return t or _cfg('bot3_token')


def get_mp_token() -> str:
    t = os.environ.get('MP3_ACCESS_TOKEN', '').strip()
    return t or _cfg('mp3_access_token')


def get_mp_webhook_url() -> str:
    """URL do webhook MP3 (pode ser configurada no banco)."""
    return _cfg('mp3_webhook_url') or 'https://paynexbet.com/webhook/mp3'


# ─── TELEGRAM API ────────────────────────────────────────────────────────────

def _tg_call(method: str, payload: dict, token: str = None) -> dict:
    """Chamada síncrona à API do Telegram (para uso em run_in_executor)."""
    tok = token or get_token()
    if not tok:
        return {'ok': False, 'error': 'sem token'}
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f'https://api.telegram.org/bot{tok}/{method}',
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        logger.warning(f'[bot3] tg {method} HTTP {e.code}: {body[:200]}')
        return {'ok': False, 'error': body}
    except Exception as e:
        logger.warning(f'[bot3] tg {method}: {e}')
        return {'ok': False, 'error': str(e)}


async def tg(method: str, **kwargs) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _tg_call, method, kwargs)


async def send(chat_id: int, text: str, markup=None, parse_mode='Markdown') -> dict:
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': parse_mode,
        'disable_web_page_preview': True,
    }
    if markup:
        payload['reply_markup'] = json.dumps(markup)
    return await tg('sendMessage', **payload)


async def edit_msg(chat_id: int, msg_id: int, text: str, markup=None) -> dict:
    payload = {
        'chat_id': chat_id,
        'message_id': msg_id,
        'text': text,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True,
    }
    if markup:
        payload['reply_markup'] = json.dumps(markup)
    return await tg('editMessageText', **payload)


async def answer_cb(callback_id: str, text: str = '', alert: bool = False):
    await tg('answerCallbackQuery',
             callback_query_id=callback_id, text=text, show_alert=alert)


# ─── TECLADOS ────────────────────────────────────────────────────────────────

def kb_main():
    return {
        'inline_keyboard': [
            [
                {'text': '💰 Depositar', 'callback_data': 'depositar'},
                {'text': '💸 Sacar',     'callback_data': 'sacar'},
            ],
            [
                {'text': '👛 Carteira',  'callback_data': 'carteira'},
                {'text': '📋 Histórico', 'callback_data': 'historico'},
            ],
            [
                {'text': '🔗 Indicar',   'callback_data': 'indicar'},
                {'text': '❓ Ajuda',     'callback_data': 'ajuda'},
            ],
        ]
    }


def kb_cancelar():
    return {'inline_keyboard': [[{'text': '❌ Cancelar', 'callback_data': 'cancelar'}]]}


def kb_tipo_chave():
    return {
        'inline_keyboard': [
            [
                {'text': '📱 CPF',     'callback_data': 'chave_cpf'},
                {'text': '📧 E-mail',  'callback_data': 'chave_email'},
            ],
            [
                {'text': '📞 Telefone', 'callback_data': 'chave_telefone'},
                {'text': '🔑 Aleatória','callback_data': 'chave_aleatoria'},
            ],
            [{'text': '❌ Cancelar', 'callback_data': 'cancelar'}],
        ]
    }


def kb_confirmar_saque(valor: float):
    return {
        'inline_keyboard': [
            [
                {'text': f'✅ Confirmar R$ {valor:.2f}'.replace('.', ','),
                 'callback_data': 'confirmar_saque'},
                {'text': '❌ Cancelar', 'callback_data': 'cancelar'},
            ]
        ]
    }


# ─── ESTADO ──────────────────────────────────────────────────────────────────

_estado: dict = {}  # {telegram_id: {'step': ..., ...}}


def st_get(tid: int) -> dict:
    return _estado.get(tid, {})


def st_set(tid: int, step: str, **kw):
    _estado[tid] = {'step': step, **kw}


def st_clear(tid: int):
    _estado.pop(tid, None)


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def fmt(v) -> str:
    return f'R$ {float(v or 0):.2f}'.replace('.', ',')


def _get_ou_criar_usuario(tid: int, nome: str, username: str) -> int:
    """Garante que o usuário exista na mp3_usuarios. Retorna user_id."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM mp3_usuarios WHERE telegram_id=%s", (tid,))
    row = cur.fetchone()
    if row:
        uid = row[0]
        cur.execute(
            "UPDATE mp3_usuarios SET nome=%s, username=%s WHERE id=%s",
            (nome, username, uid)
        )
    else:
        cur.execute(
            "INSERT INTO mp3_usuarios (telegram_id, nome, username, saldo) VALUES (%s,%s,%s,0) RETURNING id",
            (tid, nome, username)
        )
        uid = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return uid


def _get_saldo(tid: int) -> float:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT saldo FROM mp3_usuarios WHERE telegram_id=%s", (tid,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return float(row[0] or 0) if row else 0.0


# ─── GERAR PIX (Mercado Pago) ────────────────────────────────────────────────

def _mp_request(method: str, path: str, body: dict = None, mp_token: str = None) -> dict:
    """Chamada à API do Mercado Pago."""
    tok = mp_token or get_mp_token()
    if not tok:
        return {'error': 'MP token não configurado'}
    url = f'https://api.mercadopago.com{path}'
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method.upper(),
        headers={
            'Authorization': f'Bearer {tok}',
            'Content-Type': 'application/json',
            'X-Idempotency-Key': f'bot3_{datetime.now().timestamp()}'
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_err = e.read().decode('utf-8', errors='replace')
        logger.warning(f'[bot3] MP {method} {path} HTTP {e.code}: {body_err[:300]}')
        try:
            return json.loads(body_err)
        except Exception:
            return {'error': f'HTTP {e.code}', 'detail': body_err[:200]}
    except Exception as e:
        return {'error': str(e)}


def _gerar_pix(tid: int, valor: float, nome: str, username: str) -> dict:
    """Cria cobrança PIX no MP e salva na tabela mp3_transacoes."""
    import uuid
    ext_ref = f'mp3_{tid}_{int(datetime.now().timestamp())}_{uuid.uuid4().hex[:8]}'
    cfg_min = float(_cfg('deposito_minimo') or '5')
    cfg_max = float(_cfg('deposito_maximo') or '10000')
    if valor < cfg_min:
        return {'error': f'Valor mínimo: {fmt(cfg_min)}'}
    if valor > cfg_max:
        return {'error': f'Valor máximo: {fmt(cfg_max)}'}

    webhook_url = get_mp_webhook_url()
    payload = {
        'transaction_amount': valor,
        'description': f'Depósito @{username or "usuario"} — VortexPay Bot2',
        'payment_method_id': 'pix',
        'payer': {
            'email': f'user3_{tid}@paynexbet.com',
            'first_name': (nome or 'Usuario').split()[0][:20],
        },
        'external_reference': ext_ref,
        'notification_url': webhook_url,
        'metadata': {'telegram_id': str(tid), 'username': username or '', 'bot': 'bot3'},
    }
    resp = _mp_request('POST', '/v1/payments', payload)

    if 'id' not in resp:
        return {'error': resp.get('message') or resp.get('error') or 'Erro MP'}

    mp_id = resp['id']
    pix_data = resp.get('point_of_interaction', {}).get('transaction_data', {})
    qr_code  = pix_data.get('qr_code', '')
    qr_b64   = pix_data.get('qr_code_base64', '')

    # Salvar no banco — usando colunas reais da tabela mp3_transacoes
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO mp3_transacoes
           (telegram_id, tipo, valor, status, mp_payment_id, mp_external_ref, pix_copia_cola, pix_qr_base64, descricao)
           VALUES (%s,'deposito',%s,'pendente',%s,%s,%s,%s,%s) RETURNING id""",
        (tid, valor, str(mp_id), ext_ref, qr_code, qr_b64,
         f'Depósito @{username or "usuario"} — VortexPay Bot2')
    )
    tx_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {'ok': True, 'tx_id': tx_id, 'mp_id': mp_id, 'qr_code': qr_code, 'qr_b64': qr_b64, 'ext_ref': ext_ref}


# ─── HANDLERS ────────────────────────────────────────────────────────────────

async def cmd_start(tid: int, nome: str, username: str, args: str = ''):
    st_clear(tid)
    try:
        _get_ou_criar_usuario(tid, nome, username)
    except Exception as e:
        logger.warning(f'[bot3] criar usuario: {e}')

    # Processar referral
    if args:
        try:
            _processar_referral(tid, args)
        except Exception:
            pass

    saldo = _get_saldo(tid)
    txt = (
        f'👋 Olá, *{nome}*! Bem-vindo ao *VortexPay Bot2* 🚀\n\n'
        f'💳 Seu saldo: *{fmt(saldo)}*\n\n'
        '═══════════════════\n'
        '📥 *Deposite* via PIX em segundos\n'
        '📤 *Saque* direto na sua chave PIX\n'
        '🔗 *Indique* e ganhe comissões\n'
        '═══════════════════\n\n'
        'Escolha uma opção:'
    )
    await send(tid, txt, kb_main())


async def cmd_carteira(tid: int):
    st_clear(tid)
    saldo = _get_saldo(tid)
    txt = (
        f'👛 *Sua Carteira*\n\n'
        f'💰 Saldo disponível: *{fmt(saldo)}*\n\n'
        '📥 Deposite para adicionar saldo\n'
        '📤 Saque para transferir para sua chave PIX'
    )
    await send(tid, txt, kb_main())


async def cmd_depositar(tid: int):
    st_clear(tid)
    cfg_min = float(_cfg('deposito_minimo') or '5')
    cfg_max = float(_cfg('deposito_maximo') or '10000')
    st_set(tid, 'aguardando_valor_dep')
    txt = (
        f'💰 *Depositar via PIX*\n\n'
        f'📊 Mínimo: *{fmt(cfg_min)}*  |  Máximo: *{fmt(cfg_max)}*\n\n'
        'Digite o *valor* que deseja depositar:\n'
        '_(ex: 50 ou 50,00)_'
    )
    await send(tid, txt, kb_cancelar())


async def handle_valor_dep(tid: int, texto: str, nome: str, username: str):
    texto = texto.strip().replace(',', '.').replace('R$', '').replace(' ', '')
    try:
        valor = float(texto)
    except ValueError:
        await send(tid, '❌ Valor inválido. Digite apenas números (ex: *50* ou *50,00*)', kb_cancelar())
        return

    st_set(tid, 'gerando_pix', valor=valor)
    await send(tid, f'⏳ Gerando QR Code PIX de *{fmt(valor)}*...')

    loop = asyncio.get_event_loop()
    resultado = await loop.run_in_executor(None, _gerar_pix, tid, valor, nome, username)

    if not resultado.get('ok'):
        st_clear(tid)
        await send(tid, f'❌ {resultado.get("error", "Erro ao gerar PIX")}', kb_main())
        return

    qr = resultado['qr_code']
    st_clear(tid)
    txt = (
        f'✅ *PIX gerado!* {fmt(valor)}\n\n'
        f'📋 *Copia e Cola:*\n`{qr}`\n\n'
        '⏱ Válido por *30 minutos*\n'
        '✅ Confirmação automática após pagamento'
    )
    await send(tid, txt, kb_main())


async def cmd_sacar(tid: int, nome: str, username: str):
    saldo = _get_saldo(tid)
    cfg_min = float(_cfg('saque_minimo') or '20')
    if saldo < cfg_min:
        await send(
            tid,
            f'❌ Saldo insuficiente para saque.\n\n'
            f'💰 Saldo atual: *{fmt(saldo)}*\n'
            f'📊 Mínimo para saque: *{fmt(cfg_min)}*',
            kb_main()
        )
        return
    st_set(tid, 'aguardando_valor_saque', saldo=saldo, min_saque=cfg_min)
    txt = (
        f'💸 *Solicitar Saque*\n\n'
        f'💰 Seu saldo: *{fmt(saldo)}*\n'
        f'📊 Mínimo: *{fmt(cfg_min)}*\n\n'
        'Digite o *valor* que deseja sacar:'
    )
    await send(tid, txt, kb_cancelar())


async def handle_valor_saque(tid: int, texto: str):
    st = st_get(tid)
    texto = texto.strip().replace(',', '.').replace('R$', '').replace(' ', '')
    try:
        valor = float(texto)
    except ValueError:
        await send(tid, '❌ Valor inválido.', kb_cancelar())
        return

    saldo   = st.get('saldo', _get_saldo(tid))
    min_saq = st.get('min_saque', 20)
    if valor > saldo:
        await send(tid, f'❌ Saldo insuficiente. Saldo: *{fmt(saldo)}*', kb_cancelar())
        return
    if valor < min_saq:
        await send(tid, f'❌ Valor mínimo para saque: *{fmt(min_saq)}*', kb_cancelar())
        return

    st_set(tid, 'aguardando_tipo_chave', valor_saque=valor)
    await send(tid, f'🔑 *Tipo da sua chave PIX?*\nValor: *{fmt(valor)}*', kb_tipo_chave())


async def handle_tipo_chave(tid: int, tipo: str, msg_id: int):
    st = st_get(tid)
    label = {'chave_cpf': 'CPF', 'chave_email': 'E-mail',
             'chave_telefone': 'Telefone', 'chave_aleatoria': 'Chave Aleatória'}.get(tipo, tipo)
    st_set(tid, 'aguardando_chave', tipo=label, valor_saque=st.get('valor_saque', 0))
    await edit_msg(tid, msg_id, f'📝 Digite sua chave PIX _{label}_:', kb_cancelar())


async def handle_chave_pix(tid: int, chave: str, nome: str, username: str):
    st = st_get(tid)
    valor = st.get('valor_saque', 0)
    tipo  = st.get('tipo', 'PIX')
    st_set(tid, 'confirmando_saque', valor_saque=valor, tipo=tipo, chave=chave)
    txt = (
        f'📤 *Confirmar Saque*\n\n'
        f'💸 Valor: *{fmt(valor)}*\n'
        f'🔑 Tipo: *{tipo}*\n'
        f'📝 Chave: `{chave}`\n\n'
        '⚠️ Verifique os dados antes de confirmar.'
    )
    await send(tid, txt, kb_confirmar_saque(valor))


async def handle_confirmar_saque(tid: int, nome: str, username: str):
    st = st_get(tid)
    valor = st.get('valor_saque', 0)
    chave = st.get('chave', '')
    tipo  = st.get('tipo', 'PIX')
    st_clear(tid)
    try:
        conn = _conn()
        cur = conn.cursor()
        # Verificar saldo
        cur.execute("SELECT saldo FROM mp3_usuarios WHERE telegram_id=%s", (tid,))
        row = cur.fetchone()
        saldo_atual = float(row[0] or 0) if row else 0.0
        if saldo_atual < valor:
            cur.close(); conn.close()
            await send(tid, f'❌ Saldo insuficiente. Saldo atual: *{fmt(saldo_atual)}*', kb_main())
            return
        # Debitar saldo e inserir saque
        cur.execute("UPDATE mp3_usuarios SET saldo=saldo-%s WHERE telegram_id=%s", (valor, tid))
        cur.execute(
            "INSERT INTO mp3_saques (telegram_id, valor, chave_pix, tipo_chave, status) VALUES (%s,%s,%s,%s,'pendente') RETURNING id",
            (tid, valor, chave, tipo)
        )
        saque_id = cur.fetchone()[0]
        conn.commit(); cur.close(); conn.close()
        await send(
            tid,
            f'✅ *Saque solicitado com sucesso!*\n\n'
            f'💸 Valor: *{fmt(valor)}*\n'
            f'🔑 Chave: `{chave}`\n\n'
            '⏱ Prazo: até 24h úteis\n'
            '📩 Você receberá confirmação aqui.',
            kb_main()
        )
    except psycopg2.Error as e:
        logger.error(f'[bot3] solicitar_saque DB: {e}')
        await send(tid, '❌ Erro ao processar saque. Tente novamente.', kb_main())
    except Exception as e:
        logger.error(f'[bot3] solicitar_saque: {e}')
        await send(tid, '❌ Erro interno. Tente novamente mais tarde.', kb_main())


async def cmd_historico(tid: int):
    st_clear(tid)
    try:
        conn = _conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT valor, status, criado_em FROM mp3_transacoes "
            "WHERE telegram_id=%s AND tipo='deposito' ORDER BY criado_em DESC LIMIT 10",
            (tid,)
        )
        deps = cur.fetchall()
        cur.execute(
            "SELECT valor, status, criado_em FROM mp3_saques "
            "WHERE telegram_id=%s ORDER BY criado_em DESC LIMIT 5",
            (tid,)
        )
        saqs = cur.fetchall()
        cur.close(); conn.close()

        lines = ['📋 *Histórico de Transações*\n']
        if deps:
            lines.append('*Depósitos:*')
            for d in deps:
                icon = '✅' if d[2] and d[1] in ('confirmado','approved') else '⏳' if d[1] == 'pendente' else '❌'
                dt = d[2].strftime('%d/%m %H:%M') if d[2] else ''
                lines.append(f'{icon} {fmt(d[0])} — {dt}')
        else:
            lines.append('_Nenhum depósito ainda_')

        if saqs:
            lines.append('\n*Saques:*')
            for s in saqs:
                icon = '✅' if s[1] == 'aprovado' else '⏳' if s[1] == 'pendente' else '❌'
                dt = s[2].strftime('%d/%m %H:%M') if s[2] else ''
                lines.append(f'{icon} {fmt(s[0])} — {dt}')

        await send(tid, '\n'.join(lines), kb_main())
    except Exception as e:
        logger.error(f'[bot3] historico: {e}')
        await send(tid, '❌ Erro ao carregar histórico.', kb_main())


async def cmd_indicar(tid: int, username: str):
    st_clear(tid)
    bot_uname = BOT3_USERNAME or 'paypix_nexbot2'
    base = f'https://t.me/{bot_uname}'
    link = f'{base}?start=ref_{tid}'
    comissao = int(_cfg('comissao_pct') or '5')
    txt = (
        f'🔗 *Seu Link de Indicação*\n\n'
        f'`{link}`\n\n'
        f'💰 Ganhe *{comissao}%* de cada depósito feito pelos seus indicados!\n\n'
        '📤 Compartilhe com amigos e parceiros.'
    )
    await send(tid, txt, kb_main())


async def cmd_ajuda(tid: int):
    st_clear(tid)
    txt = (
        '❓ *Ajuda — VortexPay Bot2*\n\n'
        '*/start* — Menu principal\n'
        '*/carteira* — Ver saldo\n'
        '*/depositar* — Depositar via PIX\n'
        '*/sacar* — Solicitar saque\n'
        '*/historico* — Ver transações\n'
        '*/indicar* — Link de afiliado\n\n'
        '📞 Suporte: @VortexPay\\_Suporte\n'
        '🌐 Site: paynexbet.com'
    )
    await send(tid, txt, kb_main())


def _processar_referral(tid: int, ref_code: str):
    """Registra referral se não existir."""
    if not ref_code.startswith('ref_'):
        return
    try:
        ref_id = int(ref_code[4:])
        if ref_id == tid:
            return
        conn = _conn()
        cur = conn.cursor()
        cur.execute("SELECT referido_por FROM mp3_usuarios WHERE telegram_id=%s", (tid,))
        row = cur.fetchone()
        if row and row[0] is None:
            cur.execute(
                "UPDATE mp3_usuarios SET referido_por=%s WHERE telegram_id=%s",
                (ref_id, tid)
            )
            conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        logger.warning(f'[bot3] referral: {e}')


# ─── PONTO DE ENTRADA PRINCIPAL ──────────────────────────────────────────────

async def process_bot3_update(data: dict):
    """
    Chamado por server.py quando POST /webhook/bot3 chega.
    data = body JSON do Telegram.
    """
    try:
        # Mensagem de texto
        if 'message' in data:
            msg     = data['message']
            chat_id = msg['chat']['id']
            nome    = msg['from'].get('first_name', 'Usuário')
            uname   = msg['from'].get('username', '')
            texto   = msg.get('text', '')

            # Comandos
            if texto.startswith('/start'):
                args = texto[7:].strip() if len(texto) > 7 else ''
                await cmd_start(chat_id, nome, uname, args)
                return
            if texto in ('/carteira', '👛 Carteira'):
                await cmd_carteira(chat_id)
                return
            if texto in ('/depositar', '💰 Depositar'):
                await cmd_depositar(chat_id)
                return
            if texto in ('/sacar', '💸 Sacar'):
                await cmd_sacar(chat_id, nome, uname)
                return
            if texto in ('/historico', '/histórico', '📋 Histórico'):
                await cmd_historico(chat_id)
                return
            if texto in ('/indicar', '🔗 Indicar'):
                await cmd_indicar(chat_id, uname)
                return
            if texto in ('/ajuda', '/help', '❓ Ajuda'):
                await cmd_ajuda(chat_id)
                return

            # Tratar texto baseado no estado atual
            st = st_get(chat_id)
            step = st.get('step', '')

            if step == 'aguardando_valor_dep':
                await handle_valor_dep(chat_id, texto, nome, uname)
            elif step == 'aguardando_valor_saque':
                await handle_valor_saque(chat_id, texto)
            elif step == 'aguardando_chave':
                await handle_chave_pix(chat_id, texto, nome, uname)
            else:
                # Mensagem fora de contexto
                await send(chat_id, '👇 Escolha uma opção:', kb_main())

        # Callback de botão inline
        elif 'callback_query' in data:
            cb      = data['callback_query']
            chat_id = cb['from']['id']
            nome    = cb['from'].get('first_name', 'Usuário')
            uname   = cb['from'].get('username', '')
            cb_id   = cb['id']
            cbd     = cb.get('data', '')
            msg_id  = cb['message']['message_id']

            await answer_cb(cb_id)

            if cbd == 'cancelar':
                st_clear(chat_id)
                await edit_msg(chat_id, msg_id, '❌ Operação cancelada.', kb_main())
            elif cbd == 'depositar':
                await cmd_depositar(chat_id)
            elif cbd == 'sacar':
                await cmd_sacar(chat_id, nome, uname)
            elif cbd == 'carteira':
                await cmd_carteira(chat_id)
            elif cbd == 'historico':
                await cmd_historico(chat_id)
            elif cbd == 'indicar':
                await cmd_indicar(chat_id, uname)
            elif cbd == 'ajuda':
                await cmd_ajuda(chat_id)
            elif cbd.startswith('chave_'):
                await handle_tipo_chave(chat_id, cbd, msg_id)
            elif cbd == 'confirmar_saque':
                await handle_confirmar_saque(chat_id, nome, uname)

    except Exception as e:
        logger.error(f'[bot3] process_update erro: {e}', exc_info=True)


# ─── NOTIFICAÇÕES ────────────────────────────────────────────────────────────

async def notificar_deposito_confirmado(telegram_id: int, valor: float, payment_id: str):
    """Chamado por server.py quando o webhook MP confirma pagamento no bot3."""
    if not telegram_id:
        return
    # Atualizar saldo
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE mp3_usuarios SET saldo=saldo+%s, total_depositado=COALESCE(total_depositado,0)+%s WHERE telegram_id=%s",
            (valor, valor, telegram_id)
        )
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.error(f'[bot3] atualizar saldo: {e}')

    saldo = _get_saldo(telegram_id)
    txt = (
        f'✅ *Pagamento confirmado!*\n\n'
        f'💰 Depósito: *{fmt(valor)}*\n'
        f'👛 Saldo atualizado: *{fmt(saldo)}*\n\n'
        f'📝 ID MP: `{payment_id}`\n'
        '🎉 Bom jogo!'
    )
    await send(telegram_id, txt, kb_main())


async def notificar_saque_processado(telegram_id: int, valor: float, aprovado: bool, saque_id: int):
    """Chamado pelo admin quando aprova/rejeita saque no bot3."""
    if not telegram_id:
        return
    if aprovado:
        txt = (
            f'✅ *Saque processado!*\n\n'
            f'💸 Valor enviado: *{fmt(valor)}*\n'
            f'🆔 Saque #{saque_id}\n\n'
            '⏱ O PIX pode levar até 30 minutos para chegar.'
        )
    else:
        txt = (
            f'❌ *Saque rejeitado*\n\n'
            f'💸 Valor: *{fmt(valor)}*\n'
            f'🆔 Saque #{saque_id}\n\n'
            '📞 Entre em contato com o suporte se tiver dúvidas.'
        )
    await send(telegram_id, txt, kb_main())


# ─── WEBHOOK MANAGEMENT ──────────────────────────────────────────────────────

def registrar_webhook(base_url: str) -> dict:
    """Registra o webhook no Telegram. Chamado por /api/bot3/set-webhook."""
    token = get_token()
    if not token:
        return {'success': False, 'error': 'bot3_token não configurado. Salve o token primeiro.'}
    webhook_url = f'{base_url}/webhook/bot3'
    result = _tg_call('setWebhook', {'url': webhook_url, 'drop_pending_updates': True}, token=token)
    if result.get('ok') or result.get('result') is True:
        logger.info(f'[bot3] Webhook registrado: {webhook_url}')
        return {'success': True, 'msg': f'Webhook ativo: {webhook_url}', 'url': webhook_url}
    return {'success': False, 'error': result.get('description') or str(result)}


def get_webhook_info() -> dict:
    """Retorna info do webhook atual do bot3."""
    token = get_token()
    if not token:
        return {'ok': False, 'error': 'sem token'}
    return _tg_call('getWebhookInfo', {}, token=token)


def get_bot_info() -> dict:
    """Retorna informações do bot via getMe."""
    token = get_token()
    if not token:
        return {'ok': False, 'error': 'sem token'}
    return _tg_call('getMe', {}, token=token)
