"""
VortexPay - Servidor Railway 24/7
Session via env var SESSION_STR ou arquivo local
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

# Session: env var tem prioridade, depois arquivo local
SESSION_STR = os.environ.get('SESSION_STR', '')
if not SESSION_STR:
    try:
        SESSION_STR = open('session_string.txt').read().strip()
    except:
        SESSION_STR = ''

if not SESSION_STR:
    raise RuntimeError("SESSION_STR não configurado! Adicione a variável de ambiente no Railway.")

client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
_lock = asyncio.Lock()

# ─── BANCO DE DADOS ───────────────────────────────────────────
DB_PATH = os.environ.get('DB_PATH', 'transacoes.db')

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS transacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT UNIQUE NOT NULL,
        valor REAL NOT NULL,
        pix_code TEXT,
        status TEXT DEFAULT 'pendente',
        cliente_id TEXT,
        webhook_url TEXT,
        created_at TEXT,
        paid_at TEXT,
        extra TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS webhook_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT, evento TEXT, payload TEXT, sent_at TEXT, status_code INTEGER
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
        cols = ['id','tx_id','valor','pix_code','status','cliente_id','webhook_url','created_at','paid_at','extra']
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
    cols = ['id','tx_id','valor','pix_code','status','cliente_id','webhook_url','created_at','paid_at','extra']
    return [dict(zip(cols, r)) for r in rows]

# ─── CARREGAR HTML ─────────────────────────────────────────────
def load_html():
    # Tenta carregar arquivo local (index.html)
    for f in ['index.html', 'static/index.html']:
        if os.path.exists(f):
            return open(f, encoding='utf-8').read()
    return '<h1>VortexPay Online</h1>'

# ─── MONITORAR CONFIRMAÇÕES ────────────────────────────────────
async def monitorar_confirmacoes():
    @client.on(events.NewMessage(from_users=BOT_USERNAME))
    async def handler(event):
        texto = event.message.text or ''
        padroes = [r'pagamento.*confirmado', r'depósito.*recebido',
                   r'✅.*depositado', r'recebemos.*R$',
                   r'depósito aprovado', r'saldo.*atualizado']
        if any(re.search(p, texto, re.IGNORECASE) for p in padroes):
            tx_match = re.search(r'txn_([a-f0-9]+)', texto)
            if tx_match:
                tx_id = f"txn_{tx_match.group(1)}"
                confirmar_pagamento(tx_id)
                print(f'✅ Pagamento confirmado: {tx_id}')
                tx = buscar_transacao(tx_id)
                if tx and tx.get('webhook_url'):
                    await disparar_webhook(tx)
    print('✅ Listener de confirmação ativo!')

async def disparar_webhook(tx):
    import aiohttp as _aiohttp
    webhook_url = tx.get('webhook_url')
    if not webhook_url: return
    payload = {'evento':'pagamento_confirmado','tx_id':tx['tx_id'],
               'valor':tx['valor'],'status':'pago','paid_at':tx.get('paid_at')}
    body = json.dumps(payload)
    sig = hmac.new(WEBHOOK_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    try:
        async with _aiohttp.ClientSession() as s:
            async with s.post(webhook_url, data=body,
                headers={'Content-Type':'application/json','X-VortexPay-Signature':sig},
                timeout=_aiohttp.ClientTimeout(total=10)) as r:
                print(f'📨 Webhook {webhook_url}: HTTP {r.status}')
    except Exception as e:
        print(f'❌ Webhook erro: {e}')

# ─── GERAR PIX ─────────────────────────────────────────────────
async def gerar_pix(valor, cliente_id=None, webhook_url=None):
    async with _lock:
        try:
            bot = await client.get_entity(BOT_USERNAME)
            messages = await client.get_messages(bot, limit=15)
            clicou = False
            for msg in messages:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if 'DEPOSITAR' in btn.text:
                                await btn.click(); await asyncio.sleep(3)
                                clicou = True; break
                        if clicou: break
                if clicou: break
            if not clicou:
                await client.send_message(bot, '/start'); await asyncio.sleep(2)
                messages = await client.get_messages(bot, limit=5)
                for msg in messages:
                    if msg.buttons:
                        for row in msg.buttons:
                            for btn in row:
                                if 'DEPOSITAR' in btn.text:
                                    await btn.click(); await asyncio.sleep(3)
                                    clicou = True; break
            valor_str = str(int(valor)) if valor == int(valor) else f"{valor:.2f}"
            await client.send_message(bot, valor_str)
            await asyncio.sleep(8)
            msgs = await client.get_messages(bot, limit=5)
            for msg in msgs:
                if msg.text and 'PIX Copia e Cola' in msg.text:
                    text = msg.text
                    pix_match = re.search(r'`(00020101[^`]+)`', text)
                    pix_code = pix_match.group(1) if pix_match else None
                    tx_match = re.search(r'txn_([a-f0-9]+)', text)
                    tx_id = f"txn_{tx_match.group(1)}" if tx_match else f"txn_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R$\s*([\d.]+)', text)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"
                    if pix_code:
                        salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url)
                        return {'success':True,'pix_code':pix_code,'tx_id':tx_id,
                                'valor':f"R$ {valor_conf}",'status':'pendente'}
            return {'success':False,'error':'Bot não respondeu. Tente novamente.'}
        except Exception as e:
            return {'success':False,'error':str(e)}

# ─── MIDDLEWARE CORS ────────────────────────────────────────────
@web.middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers={
            'Access-Control-Allow-Origin':'*',
            'Access-Control-Allow-Methods':'GET,POST,OPTIONS',
            'Access-Control-Allow-Headers':'Content-Type,X-VortexPay-Secret',
        })
    r = await handler(request)
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r

# ─── ROTAS ─────────────────────────────────────────────────────
async def route_index(request):
    return web.Response(text=load_html(), content_type='text/html', charset='utf-8')

async def route_pix(request):
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        if valor < 5:
            return web.json_response({'success':False,'error':'Valor mínimo R$ 5,00'})
        result = await gerar_pix(valor, data.get('cliente_id'), data.get('webhook_url'))
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success':False,'error':str(e)})

async def route_status_tx(request):
    tx_id = request.match_info.get('tx_id')
    tx = buscar_transacao(tx_id)
    if not tx:
        return web.json_response({'error':'Transação não encontrada'}, status=404)
    return web.json_response({
        'tx_id':tx['tx_id'],'valor':tx['valor'],'status':tx['status'],
        'created_at':tx['created_at'],'paid_at':tx.get('paid_at'),
        'pago':tx['status']=='pago',
    })

async def route_webhook(request):
    secret = request.headers.get('X-VortexPay-Secret','')
    if secret != WEBHOOK_SECRET:
        return web.json_response({'error':'Não autorizado'}, status=401)
    try:
        data = await request.json()
        tx_id = data.get('tx_id')
        if data.get('status') == 'pago' and tx_id:
            confirmar_pagamento(tx_id)
            return web.json_response({'success':True,'message':f'Pagamento {tx_id} confirmado'})
        return web.json_response({'success':False,'message':'Status inválido'})
    except Exception as e:
        return web.json_response({'error':str(e)}, status=500)

async def route_transacoes(request):
    auth = request.headers.get('X-VortexPay-Secret','') or request.rel_url.query.get('secret','')
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error':'Não autorizado'}, status=401)
    txs = listar_transacoes()
    return web.json_response({
        'transacoes': txs,
        'resumo': {
            'total':len(txs),
            'pendentes':len([t for t in txs if t['status']=='pendente']),
            'pagos':len([t for t in txs if t['status']=='pago']),
            'valor_pendente':sum(t['valor'] for t in txs if t['status']=='pendente'),
            'valor_recebido':sum(t['valor'] for t in txs if t['status']=='pago'),
        }
    })

async def route_health(request):
    me = await client.get_me()
    return web.json_response({
        'status':'online','conta':me.first_name,'id':me.id,'bot':BOT_USERNAME,
        'webhook':'/webhook/confirmar','secret_prefix':WEBHOOK_SECRET[:8]+'...'
    })

# ─── MAIN ──────────────────────────────────────────────────────
async def main():
    init_db()
    print('✅ DB inicializado')
    await client.start()
    me = await client.get_me()
    print(f'✅ Telegram: {me.first_name} ({me.id})')
    await monitorar_confirmacoes()

    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', route_index)
    app.router.add_get('/index.html', route_index)
    app.router.add_post('/api/pix', route_pix)
    app.router.add_route('OPTIONS', '/api/pix', lambda r: web.Response(status=200))
    app.router.add_get('/api/status/{tx_id}', route_status_tx)
    app.router.add_get('/api/transacoes', route_transacoes)
    app.router.add_post('/webhook/confirmar', route_webhook)
    app.router.add_route('OPTIONS', '/webhook/confirmar', lambda r: web.Response(status=200))
    app.router.add_get('/api/status', route_health)
    app.router.add_get('/health', route_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'✅ Servidor na porta {PORT}')
    print(f'📡 Webhook: POST /webhook/confirmar  (secret: {WEBHOOK_SECRET})')
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
