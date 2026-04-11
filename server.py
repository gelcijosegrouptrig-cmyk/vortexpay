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
_telegram_ready = False
_telegram_tentativas = 0

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

# ─── HTML ───────────────────────────────────────────────────
def load_html():
    if os.path.exists('index.html'):
        return open('index.html', encoding='utf-8').read()
    return '<h1>VortexPay</h1>'

# ─── TELEGRAM - Conectar com retry ─────────────────────────
async def conectar_telegram():
    global _telegram_ready, _telegram_tentativas
    while True:
        _telegram_tentativas += 1
        try:
            print(f'🔄 Tentativa {_telegram_tentativas} - Conectando Telegram...', flush=True)

            # Desconectar se já estava conectado
            if client.is_connected():
                await client.disconnect()
            await asyncio.sleep(1)

            await client.connect()

            if not await client.is_user_authorized():
                print('❌ Sessão inválida!', flush=True)
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
            print(f'❌ Erro Telegram: {e}', flush=True)

        _telegram_ready = False
        espera = min(30, _telegram_tentativas * 5)
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
async def route_index(request):
    return web.Response(text=load_html(), content_type='text/html', charset='utf-8')

async def route_health(request):
    return web.json_response({
        'status': 'online',
        'telegram': _telegram_ready,
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
