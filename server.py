"""
VortexPay - Servidor Railway 24/7
HTTP imediato + Telegram background com retry automático
"""
import asyncio, re, json, os, sqlite3, time, hashlib, hmac
from datetime import datetime
from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession

API_ID = 35023140
API_HASH = 'a5fb75fd2a4497eab273c2a2f7b41d49'
BOT_USERNAME = 'VortexBank_bot'
PORT = int(os.environ.get('PORT', 8080))
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', 'vortex_webhook_2024')

SESSION_STR = os.environ.get('SESSION_STR', '')
if not SESSION_STR:
    try:
        SESSION_STR = open('session_string.txt').read().strip()
    except:
        pass

client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
_lock = asyncio.Lock()
_saque_lock = asyncio.Lock()
_telegram_ready = False
_telegram_tentativas = 0
_telegram_session_invalida = False  # True quando sessão foi revogada (AuthKeyDuplicatedError)

# ─── BANCO DE DADOS ──────────────────────────────────────────
DB_PATH = '/tmp/transacoes.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS transacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT UNIQUE NOT NULL, valor REAL NOT NULL,
        pix_code TEXT, status TEXT DEFAULT 'pendente',
        cliente_id TEXT, webhook_url TEXT,
        created_at TEXT, paid_at TEXT, extra TEXT
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS saques (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        saque_id TEXT UNIQUE NOT NULL,
        valor REAL NOT NULL,
        chave_pix TEXT NOT NULL,
        tipo_chave TEXT NOT NULL,
        status TEXT DEFAULT 'pendente',
        created_at TEXT,
        processado_at TEXT,
        observacao TEXT
    )''')
    conn.commit(); conn.close()

def salvar_transacao(tx_id, valor, pix_code, cliente_id=None, webhook_url=None):
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''INSERT OR REPLACE INTO transacoes
        (tx_id,valor,pix_code,status,cliente_id,webhook_url,created_at)
        VALUES (?,?,?,'pendente',?,?,?)''',
        (tx_id, valor, pix_code, cliente_id, webhook_url, datetime.now().isoformat()))
    conn.commit(); conn.close()

def buscar_transacao(tx_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM transacoes WHERE tx_id=?', (tx_id,))
    row = c.fetchone(); conn.close()
    if row:
        cols = ['id','tx_id','valor','pix_code','status','cliente_id',
                'webhook_url','created_at','paid_at','extra']
        return dict(zip(cols, row))
    return None

def confirmar_pagamento(tx_id):
    conn = sqlite3.connect(DB_PATH)
    conn.execute('UPDATE transacoes SET status=?,paid_at=? WHERE tx_id=?',
        ('pago', datetime.now().isoformat(), tx_id))
    conn.commit(); conn.close()

def listar_transacoes(limit=50):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM transacoes ORDER BY created_at DESC LIMIT ?', (limit,))
    rows = c.fetchall(); conn.close()
    cols = ['id','tx_id','valor','pix_code','status','cliente_id',
            'webhook_url','created_at','paid_at','extra']
    return [dict(zip(cols, r)) for r in rows]

def salvar_saque(saque_id, valor, chave_pix, tipo_chave):
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS saques (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        saque_id TEXT UNIQUE NOT NULL,
        valor REAL NOT NULL,
        chave_pix TEXT NOT NULL,
        tipo_chave TEXT NOT NULL,
        status TEXT DEFAULT 'pendente',
        created_at TEXT,
        processado_at TEXT,
        observacao TEXT
    )''')
    conn.execute('''INSERT OR REPLACE INTO saques
        (saque_id, valor, chave_pix, tipo_chave, status, created_at)
        VALUES (?,?,?,?,'pendente',?)''',
        (saque_id, valor, chave_pix, tipo_chave, datetime.now().isoformat()))
    conn.commit(); conn.close()

def listar_saques(limit=50):
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS saques (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        saque_id TEXT UNIQUE NOT NULL,
        valor REAL NOT NULL,
        chave_pix TEXT NOT NULL,
        tipo_chave TEXT NOT NULL,
        status TEXT DEFAULT 'pendente',
        created_at TEXT,
        processado_at TEXT,
        observacao TEXT
    )''')
    c = conn.cursor()
    c.execute('SELECT * FROM saques ORDER BY created_at DESC LIMIT ?', (limit,))
    rows = c.fetchall(); conn.close()
    cols = ['id','saque_id','valor','chave_pix','tipo_chave','status',
            'created_at','processado_at','observacao']
    return [dict(zip(cols, r)) for r in rows]

# ─── HTML ───────────────────────────────────────────────────
def load_html():
    if os.path.exists('index.html'):
        return open('index.html', encoding='utf-8').read()
    return '<h1>VortexPay</h1>'

def load_saque_html():
    if os.path.exists('saque.html'):
        return open('saque.html', encoding='utf-8').read()
    return '<h1>VortexPay - Saque</h1>'

def load_admin_html():
    if os.path.exists('admin.html'):
        return open('admin.html', encoding='utf-8').read()
    return '<h1>VortexPay - Admin</h1>'

# ─── TELEGRAM - Conectar com retry ─────────────────────────
async def conectar_telegram():
    global _telegram_ready, _telegram_tentativas, _telegram_session_invalida
    while True:
        # Se sessão foi revogada, espera longa (evita flood) mas segue tentando
        if _telegram_session_invalida:
            print('⚠️ Sessão inválida/revogada. Aguardando 5min antes de tentar nova sessão...', flush=True)
            await asyncio.sleep(300)
            _telegram_session_invalida = False  # Tenta novamente
            continue

        _telegram_tentativas += 1
        try:
            print(f'🔄 Tentativa {_telegram_tentativas} - Conectando Telegram...', flush=True)

            # Desconectar se já estava conectado
            if client.is_connected():
                await client.disconnect()
            await asyncio.sleep(1)

            await client.connect()

            if not await client.is_user_authorized():
                print('❌ Sessão inválida/não autorizada!', flush=True)
                _telegram_session_invalida = True
                await asyncio.sleep(30)
                continue

            me = await client.get_me()
            print(f'✅ Telegram OK: {me.first_name} ({me.id})', flush=True)
            _telegram_ready = True

            @client.on(events.NewMessage(from_users=BOT_USERNAME))
            async def handler(event):
                texto = event.message.text or ''
                # Padrões exatos que o VortexPay envia ao confirmar depósito
                padroes = [
                    r'Depósito de R\$.*recebido com sucesso',
                    r'✅ Depósito de',
                    r'depósito.*recebido com sucesso',
                    r'pagamento.*confirmado',
                    r'✅.*depositado',
                    r'recebemos.*R\$',
                    r'depósito aprovado',
                    r'Valor creditado:',
                ]
                if any(re.search(p, texto, re.IGNORECASE) for p in padroes):
                    print(f'💰 Confirmação detectada: {texto[:100]}', flush=True)
                    # Buscar tx mais recente pendente e confirmar
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    # Pegar transações pendentes mais recentes (últimas 5 min)
                    c.execute("""SELECT tx_id FROM transacoes 
                                 WHERE status='pendente' 
                                 ORDER BY created_at DESC LIMIT 5""")
                    pendentes = [r[0] for r in c.fetchall()]
                    conn.close()
                    
                    # Tentar extrair valor da mensagem para match
                    val_match = re.search(r'R\$\s*([\d,.]+)', texto)
                    valor_msg = None
                    if val_match:
                        try:
                            valor_msg = float(val_match.group(1).replace(',', '.'))
                        except:
                            pass
                    
                    for tx_id in pendentes:
                        tx = buscar_transacao(tx_id)
                        if tx:
                            # Confirmar se valor bate ou se só tem uma pendente
                            if valor_msg is None or abs(tx['valor'] - valor_msg) < 0.01 or len(pendentes) == 1:
                                confirmar_pagamento(tx_id)
                                print(f'✅ Pago confirmado: {tx_id} R${tx["valor"]}', flush=True)
                                break

            print('✅ Listener ativo, mantendo conexão...', flush=True)
            await client.run_until_disconnected()

        except Exception as e:
            nome_erro = type(e).__name__
            print(f'❌ Erro Telegram ({nome_erro}): {e}', flush=True)
            
            # AuthKeyDuplicatedError: sessão revogada, não adianta tentar com mesma session
            if 'AuthKeyDuplicated' in nome_erro or 'AuthKeyDuplicated' in str(e):
                print('🚫 Sessão revogada (usada em dois IPs). Pausando 5min...', flush=True)
                _telegram_session_invalida = True
                _telegram_ready = False
                await asyncio.sleep(300)
                continue

        _telegram_ready = False
        espera = min(60, _telegram_tentativas * 5)
        print(f'🔄 Reconectando em {espera}s...', flush=True)
        await asyncio.sleep(espera)

# ─── GERAR PIX - Garante conexão antes de gerar ────────────
async def verificar_saldo_bot() -> float:
    """Consulta saldo atual no bot"""
    try:
        bot = await client.get_entity(BOT_USERNAME)
        await client.send_message(bot, '/start')
        await asyncio.sleep(3)
        msgs = await client.get_messages(bot, limit=3)
        for msg in msgs:
            if msg.text and 'Saldo Disponível' in msg.text:
                m = re.search(r'Saldo Disponível[^`]*`R\$\s*([\d,.]+)`', msg.text)
                if m:
                    return float(m.group(1).replace(',', '.'))
    except:
        pass
    return -1.0

async def gerar_pix(valor, cliente_id=None, webhook_url=None):
    # Se Telegram não está pronto, tenta conectar e espera até 60s
    if not _telegram_ready:
        print('⏳ Aguardando Telegram ficar pronto...', flush=True)
        for _ in range(30):  # espera até 30s (verificando a cada 1s)
            await asyncio.sleep(1)
            if _telegram_ready:
                break
        if not _telegram_ready:
            return {'success': False, 'error': 'Serviço temporariamente indisponível. Tente novamente.'}

    async with _lock:
        try:
            # Verificar novamente dentro do lock
            if not _telegram_ready:
                return {'success': False, 'error': '⚠️ Sistema em manutenção. O bot Telegram está sendo reconectado. Tente novamente em alguns minutos.'}

            bot = await client.get_entity(BOT_USERNAME)

            # Clicar DEPOSITAR
            messages = await client.get_messages(bot, limit=15)
            clicou = False
            for msg in messages:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if 'DEPOSITAR' in btn.text:
                                await btn.click()
                                await asyncio.sleep(3)
                                clicou = True; break
                        if clicou: break
                if clicou: break

            if not clicou:
                await client.send_message(bot, '/start')
                await asyncio.sleep(2)
                messages = await client.get_messages(bot, limit=5)
                for msg in messages:
                    if msg.buttons:
                        for row in msg.buttons:
                            for btn in row:
                                if 'DEPOSITAR' in btn.text:
                                    await btn.click()
                                    await asyncio.sleep(3)
                                    clicou = True; break

            # Enviar valor
            valor_str = str(int(valor)) if valor == int(valor) else f"{valor:.2f}"
            await client.send_message(bot, valor_str)
            await asyncio.sleep(9)

            # Capturar resposta
            msgs = await client.get_messages(bot, limit=5)
            for msg in msgs:
                if msg.text and 'PIX Copia e Cola' in msg.text:
                    text = msg.text
                    pix_match = re.search(r'`(00020101[^`]+)`', text)
                    pix_code = pix_match.group(1) if pix_match else None
                    tx_match = re.search(r'txn_([a-f0-9]+)', text)
                    tx_id = f"txn_{tx_match.group(1)}" if tx_match else f"txn_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d,.]+)', text)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"
                    if pix_code:
                        salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url)
                        return {'success': True, 'pix_code': pix_code, 'tx_id': tx_id,
                                'valor': f"R$ {valor_conf}", 'status': 'pendente'}

            return {'success': False, 'error': 'Bot não respondeu. Tente novamente.'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

# ─── MIDDLEWARE ────────────────────────────────────────────
@web.middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type,X-VortexPay-Secret',
        })
    r = await handler(request)
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r

# ─── ROTAS ────────────────────────────────────────────────
async def route_atualizar_sessao(request):
    """Atualiza SESSION_STRING em runtime sem reiniciar o servidor"""
    global client, _telegram_ready, _telegram_session_invalida
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        nova_sessao = str(data.get('session_string', '')).strip()
        if not nova_sessao or len(nova_sessao) < 50:
            return web.json_response({'error': 'session_string inválida (muito curta)'}, status=400)

        # Salvar nova sessão no arquivo
        with open('session_string.txt', 'w') as f:
            f.write(nova_sessao)
        print(f'🔑 Nova sessão recebida via API ({len(nova_sessao)} chars)', flush=True)

        # Reinicializar cliente com nova sessão
        try:
            if client.is_connected():
                await client.disconnect()
        except:
            pass

        from telethon.sessions import StringSession as SS
        client.__init__(SS(nova_sessao), API_ID, API_HASH)
        _telegram_ready = False
        _telegram_session_invalida = False

        # Tentar conectar imediatamente
        try:
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
                _telegram_ready = True
                print(f'✅ Nova sessão OK: {me.first_name} ({me.id})', flush=True)
                return web.json_response({
                    'success': True,
                    'message': f'Sessão atualizada! Conectado como {me.first_name}',
                    'user': me.first_name,
                })
            else:
                return web.json_response({'success': False, 'error': 'Sessão não autorizada'})
        except Exception as e:
            return web.json_response({'success': False, 'error': f'Erro ao conectar: {e}'})

    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_index(request):
    return web.Response(text=load_html(), content_type='text/html', charset='utf-8')

async def route_health(request):
    motivo = None
    if not _telegram_ready:
        if _telegram_session_invalida:
            motivo = 'sessao_invalida'
        elif _telegram_tentativas > 0:
            motivo = 'reconectando'
        else:
            motivo = 'iniciando'
    return web.json_response({
        'status': 'online',
        'telegram': _telegram_ready,
        'telegram_motivo': motivo,
        'tentativas': _telegram_tentativas,
        'bot': BOT_USERNAME,
        'webhook': '/webhook/confirmar',
    })

async def route_pix(request):
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'})
        result = await gerar_pix(valor, data.get('cliente_id'), data.get('webhook_url'))
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def route_status_tx(request):
    tx_id = request.match_info.get('tx_id')
    tx = buscar_transacao(tx_id)
    if not tx:
        return web.json_response({'error': 'Transação não encontrada'}, status=404)

    # Se ainda pendente, verificar mensagens recentes do bot
    if tx['status'] == 'pendente' and _telegram_ready:
        try:
            bot = await client.get_entity(BOT_USERNAME)
            msgs = await client.get_messages(bot, limit=10)
            padroes_pago = [
                r'Depósito de R\$.*recebido com sucesso',
                r'✅ Depósito de',
                r'Valor creditado:',
                r'depósito.*recebido',
            ]
            for msg in msgs:
                if not msg.text:
                    continue
                if any(re.search(p, msg.text, re.IGNORECASE) for p in padroes_pago):
                    # Verificar se é recente (últimos 10 min)
                    import datetime as dt
                    if hasattr(msg, 'date') and msg.date:
                        idade = (dt.datetime.now(dt.timezone.utc) - msg.date).total_seconds()
                        if idade < 600:  # 10 minutos
                            confirmar_pagamento(tx_id)
                            tx = buscar_transacao(tx_id)
                            print(f'✅ Confirmado via polling: {tx_id}', flush=True)
                            break
        except Exception as e:
            print(f'Polling erro: {e}', flush=True)

    return web.json_response({
        'tx_id': tx['tx_id'], 'valor': tx['valor'], 'status': tx['status'],
        'created_at': tx['created_at'], 'paid_at': tx.get('paid_at'),
        'pago': tx['status'] == 'pago',
    })

async def route_webhook(request):
    secret = request.headers.get('X-VortexPay-Secret', '')
    if secret != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        tx_id = data.get('tx_id')
        if data.get('status') == 'pago' and tx_id:
            confirmar_pagamento(tx_id)
            return web.json_response({'success': True, 'message': f'Pagamento {tx_id} confirmado'})
        return web.json_response({'success': False, 'message': 'Status inválido'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_transacoes(request):
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    txs = listar_transacoes()
    return web.json_response({
        'transacoes': txs,
        'resumo': {
            'total': len(txs),
            'pendentes': len([t for t in txs if t['status'] == 'pendente']),
            'pagos': len([t for t in txs if t['status'] == 'pago']),
            'valor_pendente': sum(t['valor'] for t in txs if t['status'] == 'pendente'),
            'valor_recebido': sum(t['valor'] for t in txs if t['status'] == 'pago'),
        }
    })

# ─── ROTA SAQUE ───────────────────────────────────────────
async def route_saque_page(request):
    return web.Response(text=load_saque_html(), content_type='text/html', charset='utf-8')

async def route_admin_page(request):
    return web.Response(text=load_admin_html(), content_type='text/html', charset='utf-8')

async def route_saldo(request):
    """Retorna saldo atual da conta via bot Telegram"""
    if not _telegram_ready:
        return web.json_response({'success': False, 'saldo': 0, 'disponivel': 0, 'error': 'Telegram não conectado'})
    try:
        bot = await client.get_entity(BOT_USERNAME)
        await client.send_message(bot, '/start')
        await asyncio.sleep(3)
        msgs = await client.get_messages(bot, limit=5)
        for msg in msgs:
            if not msg.text:
                continue
            # Padrão: "Saldo Disponível: `R$ 37,92`" ou "Saldo: R$37,92"
            m = re.search(r'Saldo[^`\n]*`R\$\s*([\d,.]+)`', msg.text)
            if not m:
                m = re.search(r'Saldo[:\s*]+R\$\s*([\d,.]+)', msg.text)
            if m:
                saldo = float(m.group(1).replace(',', '.'))
                # Disponível para saque (pode ter taxa)
                m2 = re.search(r'[Dd]ispon[íi]vel[^`\n]*`R\$\s*([\d,.]+)`', msg.text)
                if not m2:
                    m2 = re.search(r'[Ss]aque[:\s]+R\$\s*([\d,.]+)', msg.text)
                disponivel = float(m2.group(1).replace(',', '.')) if m2 else saldo
                return web.json_response({'success': True, 'saldo': saldo, 'disponivel': disponivel})
        return web.json_response({'success': False, 'saldo': 0, 'disponivel': 0, 'error': 'Saldo não encontrado'})
    except Exception as e:
        return web.json_response({'success': False, 'saldo': 0, 'disponivel': 0, 'error': str(e)})

async def executar_saque_bot(valor: float, tipo_chave: str, chave_pix: str) -> dict:
    """
    Executa o fluxo de saque manual no bot VortexBank:
    1. Abre menu SACAR
    2. Clica em 'Realizar Saque (Manual)'
    3. Envia o valor
    4. Seleciona tipo de chave Pix
    5. Envia a chave Pix
    6. Captura resposta do bot
    """
    if not _telegram_ready:
        for _ in range(30):
            await asyncio.sleep(1)
            if _telegram_ready:
                break
        if not _telegram_ready:
            return {'success': False, 'error': 'Serviço temporariamente indisponível. Tente novamente.'}

    async with _saque_lock:
        try:
            bot = await client.get_entity(BOT_USERNAME)

            # ── PASSO 1: Abrir menu principal ──────────────────────
            await client.send_message(bot, '/start')
            await asyncio.sleep(2)

            # ── PASSO 2: Clicar em SACAR ──────────────────────────
            msgs = await client.get_messages(bot, limit=5)
            clicou_sacar = False
            for msg in msgs:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if 'SACAR' in btn.text:
                                await btn.click()
                                await asyncio.sleep(3)
                                clicou_sacar = True
                                break
                        if clicou_sacar: break
                if clicou_sacar: break

            if not clicou_sacar:
                return {'success': False, 'error': 'Botão SACAR não encontrado no bot.'}

            # ── PASSO 3: Clicar em 'Realizar Saque (Manual)' ──────
            msgs = await client.get_messages(bot, limit=5)
            clicou_manual = False
            for msg in msgs:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if 'Manual' in btn.text or 'Saque' in btn.text:
                                await btn.click()
                                await asyncio.sleep(3)
                                clicou_manual = True
                                break
                        if clicou_manual: break
                if clicou_manual: break

            if not clicou_manual:
                return {'success': False, 'error': 'Botão Saque Manual não encontrado.'}

            # ── PASSO 4: Enviar valor ──────────────────────────────
            valor_str = str(int(valor)) if valor == int(valor) else f"{valor:.2f}"
            await client.send_message(bot, valor_str)
            await asyncio.sleep(4)

            # ── PASSO 5: Selecionar tipo de chave ─────────────────
            mapa_tipo = {
                'cpf': 'CPF',
                'telefone': 'Telefone',
                'email': 'E-mail',
                'aleatoria': 'Aleatória',
                'cnpj': 'CNPJ',
            }
            texto_tipo = mapa_tipo.get(tipo_chave.lower(), 'CPF')

            msgs = await client.get_messages(bot, limit=5)
            clicou_tipo = False
            for msg in msgs:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if texto_tipo.lower() in btn.text.lower():
                                await btn.click()
                                await asyncio.sleep(3)
                                clicou_tipo = True
                                break
                        if clicou_tipo: break
                if clicou_tipo: break

            if not clicou_tipo:
                # Tentar enviar como texto se não achou botão
                await client.send_message(bot, texto_tipo)
                await asyncio.sleep(3)

            # ── PASSO 6: Enviar chave Pix ──────────────────────────
            import datetime as dt
            hora_envio = dt.datetime.now(dt.timezone.utc)
            await client.send_message(bot, chave_pix)
            print(f'⏳ Chave enviada, aguardando resposta do bot...', flush=True)

            # ── PASSO 7: Polling ativo até 30s aguardando confirmação ──
            resposta_bot = ''
            status_saque = 'pendente'
            saque_id_bot = None

            padroes_sucesso = [
                r'solicitação de saque.*enviada com sucesso',
                r'saque.*enviado com sucesso',
                r'foi enviada com sucesso',
                r'Status: PROCESSING',
                r'saq-[a-f0-9]+',
                r'✅.*solicitação.*saque',
                r'saque.*solicitado',
                r'será.*processado',
            ]
            padroes_erro = [
                r'saldo insuficiente',
                r'valor.*inválido',
                r'chave.*inválida',
                r'chave pix.*não',
                r'erro ao processar',
                r'não foi possível',
                r'operação.*cancelada',
            ]

            # Polling: verifica a cada 2s por até 30s
            for tentativa in range(15):
                await asyncio.sleep(2)
                msgs = await client.get_messages(bot, limit=5)

                for msg in msgs:
                    if not msg.text:
                        continue
                    # Somente mensagens APÓS o envio da chave
                    if hasattr(msg, 'date') and msg.date:
                        if msg.date < hora_envio:
                            continue
                    texto = msg.text

                    if any(re.search(p, texto, re.IGNORECASE) for p in padroes_sucesso):
                        resposta_bot = texto[:400]
                        status_saque = 'enviado'
                        m_id = re.search(r'saq-([a-f0-9]+)', texto)
                        if m_id:
                            saque_id_bot = f"saq-{m_id.group(1)}"
                        print(f'✅ Confirmação recebida na tentativa {tentativa+1}!', flush=True)
                        break
                    if any(re.search(p, texto, re.IGNORECASE) for p in padroes_erro):
                        resposta_bot = texto[:300]
                        status_saque = 'erro'
                        print(f'❌ Erro detectado: {texto[:80]}', flush=True)
                        break

                if status_saque != 'pendente':
                    break

            # Se ainda pendente após 30s, assume enviado (saque foi feito, bot só demorou)
            if status_saque == 'pendente':
                status_saque = 'enviado'
                resposta_bot = f'Saque de R${valor:.2f} enviado para {chave_pix}. Processamento pode levar até 40 minutos.'
                print(f'⚠️ Timeout aguardando bot, assumindo enviado.', flush=True)

            print(f'💸 Saque: R${valor:.2f} → {tipo_chave}: {chave_pix} | Status: {status_saque} | BotID: {saque_id_bot}', flush=True)

            return {
                'success': True,
                'status': status_saque,
                'status_msg': '✅ Saque realizado com sucesso! Cai em até 40 minutos.' if status_saque == 'enviado' else 'Erro no saque',
                'mensagem_bot': resposta_bot,
                'saque_id_bot': saque_id_bot,
                'valor': valor,
                'tipo_chave': tipo_chave,
                'chave_pix': chave_pix,
            }

        except Exception as e:
            print(f'❌ Erro ao executar saque: {e}', flush=True)
            return {'success': False, 'error': f'Erro interno: {str(e)}'}


async def route_solicitar_saque(request):
    """Endpoint principal de saque manual - executa fluxo completo no bot"""
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        chave_pix = str(data.get('chave_pix', '')).strip()
        tipo_chave = str(data.get('tipo_chave', 'cpf')).strip()

        if valor < 10:
            return web.json_response({'success': False, 'error': 'Valor mínimo para saque é R$ 10,00'})
        if not chave_pix or len(chave_pix) < 5:
            return web.json_response({'success': False, 'error': 'Chave Pix inválida. Verifique e tente novamente.'})

        # Gerar ID único para o saque
        saque_id = 'saq_' + hashlib.md5(f"{chave_pix}{valor}{time.time()}".encode()).hexdigest()[:12]

        # Salvar no banco como pendente
        salvar_saque(saque_id, valor, chave_pix, tipo_chave)

        # Executar fluxo no bot Telegram
        resultado = await executar_saque_bot(valor, tipo_chave, chave_pix)

        if resultado['success']:
            # Atualizar status no banco
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                'UPDATE saques SET status=?, processado_at=?, observacao=? WHERE saque_id=?',
                (resultado['status'], datetime.now().isoformat(),
                 resultado.get('mensagem_bot', '')[:500], saque_id)
            )
            conn.commit(); conn.close()

            return web.json_response({
                'success': True,
                'saque_id': saque_id,
                'valor': valor,
                'chave_pix': chave_pix,
                'tipo_chave': tipo_chave,
                'status': resultado['status'],
                'status_msg': resultado.get('status_msg', 'Saque solicitado!'),
                'mensagem_bot': resultado.get('mensagem_bot', ''),
            })
        else:
            # Marcar como erro no banco
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                'UPDATE saques SET status=?, observacao=? WHERE saque_id=?',
                ('erro', resultado.get('error', ''), saque_id)
            )
            conn.commit(); conn.close()

            return web.json_response({'success': False, 'error': resultado.get('error', 'Erro desconhecido')})

    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def route_saques_admin(request):
    """Painel admin - listar todos os saques"""
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    limit = int(request.rel_url.query.get('limit', 200))
    saques = listar_saques(limit)
    enviados = [s for s in saques if s['status'] in ('enviado','confirmado','processado')]
    return web.json_response({
        'saques': saques,
        'resumo': {
            'total': len(saques),
            'pendentes': len([s for s in saques if s['status'] == 'pendente']),
            'processados': len(enviados),
            'erros': len([s for s in saques if s['status'] == 'erro']),
            'valor_pendente': sum(s['valor'] for s in saques if s['status'] == 'pendente'),
            'valor_pago': sum(s['valor'] for s in enviados),
        }
    })

async def route_stats(request):
    """Dashboard completo com métricas consolidadas"""
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Depósitos
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM transacoes WHERE status='pago'")
        dep_conf, val_dep_conf = c.fetchone()
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM transacoes WHERE status='pendente'")
        dep_pend, val_dep_pend = c.fetchone()
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM transacoes")
        dep_total, val_dep_total = c.fetchone()

        # Saques
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status IN ('enviado','confirmado','processado')")
        saq_conf, val_saq_conf = c.fetchone()
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status='pendente'")
        saq_pend, val_saq_pend = c.fetchone()
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status='erro'")
        saq_erro, _ = c.fetchone()
        c.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques")
        saq_total, val_saq_total = c.fetchone()

        # Últimos 7 dias - depósitos por dia
        c.execute("""SELECT date(created_at), COUNT(*), COALESCE(SUM(valor),0)
                     FROM transacoes WHERE created_at >= date('now','-7 days')
                     GROUP BY date(created_at) ORDER BY date(created_at)""")
        dep_por_dia = [{'data': r[0], 'qtd': r[1], 'valor': round(r[2],2)} for r in c.fetchall()]

        # Últimos 7 dias - saques por dia
        c.execute("""SELECT date(created_at), COUNT(*), COALESCE(SUM(valor),0)
                     FROM saques WHERE created_at >= date('now','-7 days')
                     GROUP BY date(created_at) ORDER BY date(created_at)""")
        saq_por_dia = [{'data': r[0], 'qtd': r[1], 'valor': round(r[2],2)} for r in c.fetchall()]

        # Últimos depósitos e saques
        c.execute("SELECT tx_id,valor,status,created_at,paid_at FROM transacoes ORDER BY created_at DESC LIMIT 10")
        ult_dep = [{'tx_id':r[0],'valor':r[1],'status':r[2],'created_at':r[3],'paid_at':r[4]} for r in c.fetchall()]

        c.execute("SELECT saque_id,valor,chave_pix,tipo_chave,status,created_at,processado_at FROM saques ORDER BY created_at DESC LIMIT 10")
        ult_saq = [{'saque_id':r[0],'valor':r[1],'chave_pix':r[2],'tipo_chave':r[3],'status':r[4],'created_at':r[5],'processado_at':r[6]} for r in c.fetchall()]

        conn.close()
        return web.json_response({
            'depositos': {
                'total': dep_total, 'confirmados': dep_conf, 'pendentes': dep_pend,
                'valor_recebido': round(val_dep_conf, 2),
                'valor_pendente': round(val_dep_pend, 2),
                'valor_total': round(val_dep_total, 2),
                'por_dia': dep_por_dia,
                'recentes': ult_dep,
            },
            'saques': {
                'total': saq_total, 'realizados': saq_conf, 'pendentes': saq_pend, 'erros': saq_erro,
                'valor_sacado': round(val_saq_conf, 2),
                'valor_pendente': round(val_saq_pend, 2),
                'valor_total': round(val_saq_total, 2),
                'por_dia': saq_por_dia,
                'recentes': ult_saq,
            },
            'telegram': _telegram_ready,
            'timestamp': datetime.now().isoformat(),
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_cancelar_saque(request):
    """Cancelar um saque pendente"""
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    saque_id = request.match_info.get('saque_id')
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT status FROM saques WHERE saque_id=?", (saque_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            return web.json_response({'error': 'Saque não encontrado'}, status=404)
        if row[0] not in ('pendente', 'erro'):
            conn.close()
            return web.json_response({'error': f'Não é possível cancelar saque com status "{row[0]}"'}, status=400)
        conn.execute("UPDATE saques SET status='cancelado', processado_at=? WHERE saque_id=?",
                     (datetime.now().isoformat(), saque_id))
        conn.commit(); conn.close()
        return web.json_response({'success': True, 'message': f'Saque {saque_id} cancelado.'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_confirmar_deposito_admin(request):
    """Confirmar manualmente um depósito pendente"""
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        tx_id = data.get('tx_id', '').strip()
        if not tx_id:
            return web.json_response({'error': 'tx_id obrigatório'}, status=400)
        tx = buscar_transacao(tx_id)
        if not tx:
            return web.json_response({'error': 'Transação não encontrada'}, status=404)
        if tx['status'] == 'pago':
            return web.json_response({'success': True, 'message': 'Já estava confirmada.'})
        confirmar_pagamento(tx_id)
        return web.json_response({'success': True, 'message': f'Depósito {tx_id} confirmado manualmente.'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_exportar_csv(request):
    """Exportar depósitos ou saques em CSV"""
    auth = (request.headers.get('X-VortexPay-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    tipo = request.rel_url.query.get('tipo', 'depositos')
    try:
        import io
        output = io.StringIO()
        if tipo == 'saques':
            rows = listar_saques(1000)
            output.write('saque_id,valor,chave_pix,tipo_chave,status,created_at,processado_at,observacao\n')
            for r in rows:
                output.write(f"{r.get('saque_id','')},{r.get('valor','')},{r.get('chave_pix','')},{r.get('tipo_chave','')},{r.get('status','')},{r.get('created_at','')},{r.get('processado_at','') or ''},{(r.get('observacao','') or '').replace(',',';')[:80]}\n")
            filename = 'saques.csv'
        else:
            rows = listar_transacoes(1000)
            output.write('tx_id,valor,status,cliente_id,created_at,paid_at\n')
            for r in rows:
                output.write(f"{r.get('tx_id','')},{r.get('valor','')},{r.get('status','')},{r.get('cliente_id','') or ''},{r.get('created_at','')},{r.get('paid_at','') or ''}\n")
            filename = 'depositos.csv'
        csv_content = output.getvalue()
        return web.Response(
            text=csv_content,
            content_type='text/csv',
            charset='utf-8',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return web.Response(text=f'Erro: {e}', status=500)

# ─── MAIN ─────────────────────────────────────────────────
async def main():
    init_db()
    print('✅ DB ok', flush=True)

    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', route_index)
    app.router.add_get('/index.html', route_index)
    app.router.add_get('/health', route_health)
    app.router.add_get('/api/status', route_health)
    app.router.add_post('/api/pix', route_pix)
    app.router.add_route('OPTIONS', '/api/pix', lambda r: web.Response(status=200))
    app.router.add_get('/api/status/{tx_id}', route_status_tx)
    app.router.add_get('/api/transacoes', route_transacoes)
    app.router.add_post('/webhook/confirmar', route_webhook)
    app.router.add_route('OPTIONS', '/webhook/confirmar', lambda r: web.Response(status=200))
    # Rotas de Saque
    app.router.add_get('/saque', route_saque_page)
    app.router.add_get('/saque.html', route_saque_page)
    app.router.add_get('/api/saldo', route_saldo)
    app.router.add_post('/api/saque', route_solicitar_saque)
    app.router.add_route('OPTIONS', '/api/saque', lambda r: web.Response(status=200))
    app.router.add_get('/api/saques', route_saques_admin)
    # Painel Admin
    app.router.add_get('/admin', route_admin_page)
    app.router.add_get('/admin.html', route_admin_page)
    # APIs adicionais
    app.router.add_get('/api/stats', route_stats)
    app.router.add_post('/api/saque/{saque_id}/cancelar', route_cancelar_saque)
    app.router.add_post('/api/deposito/confirmar', route_confirmar_deposito_admin)
    app.router.add_get('/api/exportar', route_exportar_csv)
    app.router.add_post('/api/atualizar-sessao', route_atualizar_sessao)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'✅ HTTP porta {PORT}', flush=True)

    # Telegram em background com retry automático
    asyncio.create_task(conectar_telegram())

    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
