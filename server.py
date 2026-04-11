"""
VortexPay Server 24/7 - Web + API Telegram + Webhook Confirmação
"""
import asyncio
import re
import json
import os
import sqlite3
import time
import hashlib
import hmac
from datetime import datetime
from aiohttp import web
from telethon import TelegramClient, events

API_ID = 35023140
API_HASH = 'a5fb75fd2a4497eab273c2a2f7b41d49'
BOT_USERNAME = 'VortexBank_bot'
SESSION_STR = open('/home/user/session_string.txt').read().strip()
PORT = int(os.environ.get('PORT', 5060))
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', 'vortex_webhook_2024')

from telethon.sessions import StringSession
client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
_lock = asyncio.Lock()

# ============================================================
# BANCO DE DADOS SQLite - Rastrear transações
# ============================================================

def init_db():
    conn = sqlite3.connect('transacoes.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS transacoes (
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
        )
    ''')
    # Tabela de eventos webhook
    c.execute('''
        CREATE TABLE IF NOT EXISTS webhook_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_id TEXT,
            evento TEXT,
            payload TEXT,
            sent_at TEXT,
            status_code INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def salvar_transacao(tx_id, valor, pix_code, cliente_id=None, webhook_url=None):
    conn = sqlite3.connect('transacoes.db')
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO transacoes (tx_id, valor, pix_code, status, cliente_id, webhook_url, created_at)
        VALUES (?, ?, ?, 'pendente', ?, ?, ?)
    ''', (tx_id, valor, pix_code, cliente_id, webhook_url, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def buscar_transacao(tx_id):
    conn = sqlite3.connect('transacoes.db')
    c = conn.cursor()
    c.execute('SELECT * FROM transacoes WHERE tx_id = ?', (tx_id,))
    row = c.fetchone()
    conn.close()
    if row:
        cols = ['id','tx_id','valor','pix_code','status','cliente_id','webhook_url','created_at','paid_at','extra']
        return dict(zip(cols, row))
    return None

def confirmar_pagamento(tx_id):
    conn = sqlite3.connect('transacoes.db')
    c = conn.cursor()
    c.execute('''
        UPDATE transacoes SET status='pago', paid_at=? WHERE tx_id=?
    ''', (datetime.now().isoformat(), tx_id))
    conn.commit()
    conn.close()

def listar_transacoes(limit=50):
    conn = sqlite3.connect('transacoes.db')
    c = conn.cursor()
    c.execute('SELECT * FROM transacoes ORDER BY created_at DESC LIMIT ?', (limit,))
    rows = c.fetchall()
    conn.close()
    cols = ['id','tx_id','valor','pix_code','status','cliente_id','webhook_url','created_at','paid_at','extra']
    return [dict(zip(cols, r)) for r in rows]

# Carregar HTML
def load_html():
    try:
        return open('index.html', 'r', encoding='utf-8').read()
    except:
        return '<h1>VortexPay</h1>'

HTML = load_html()

# ============================================================
# MONITORAR BOT - detectar confirmações de pagamento
# ============================================================

async def monitorar_confirmacoes():
    """Monitora mensagens do bot para detectar pagamentos confirmados"""
    print('🔍 Monitorando confirmações de pagamento...')
    
    @client.on(events.NewMessage(from_users=BOT_USERNAME))
    async def handler(event):
        texto = event.message.text or ''
        
        # Detectar confirmação de pagamento
        padroes_confirmacao = [
            r'pagamento.*confirmado',
            r'depósito.*recebido', 
            r'✅.*depositado',
            r'recebemos.*R\$',
            r'depósito aprovado',
            r'saldo.*atualizado',
            r'crédito.*adicionado',
        ]
        
        confirmado = any(re.search(p, texto, re.IGNORECASE) for p in padroes_confirmacao)
        
        if confirmado:
            # Extrair TX ID da mensagem
            tx_match = re.search(r'txn_([a-f0-9]+)', texto)
            if tx_match:
                tx_id = f"txn_{tx_match.group(1)}"
                print(f'✅ Pagamento confirmado automaticamente: {tx_id}')
                confirmar_pagamento(tx_id)
                
                # Disparar webhook externo se configurado
                tx = buscar_transacao(tx_id)
                if tx and tx.get('webhook_url'):
                    await disparar_webhook(tx)
    
    print('✅ Listener de confirmação ativo!')

async def disparar_webhook(tx: dict):
    """Dispara webhook para URL externa quando pagamento é confirmado"""
    import aiohttp
    
    webhook_url = tx.get('webhook_url')
    if not webhook_url:
        return
    
    payload = {
        'evento': 'pagamento_confirmado',
        'tx_id': tx['tx_id'],
        'valor': tx['valor'],
        'status': 'pago',
        'paid_at': tx.get('paid_at'),
        'cliente_id': tx.get('cliente_id'),
    }
    
    # Assinar payload
    body = json.dumps(payload)
    sig = hmac.new(WEBHOOK_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                data=body,
                headers={
                    'Content-Type': 'application/json',
                    'X-VortexPay-Signature': sig,
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                status = resp.status
                print(f'📨 Webhook enviado para {webhook_url}: HTTP {status}')
                
                # Log
                conn = sqlite3.connect('transacoes.db')
                conn.execute('''
                    INSERT INTO webhook_logs (tx_id, evento, payload, sent_at, status_code)
                    VALUES (?, 'pagamento_confirmado', ?, ?, ?)
                ''', (tx['tx_id'], body, datetime.now().isoformat(), status))
                conn.commit()
                conn.close()
    except Exception as e:
        print(f'❌ Erro ao disparar webhook: {e}')

# ============================================================
# POLLING MANUAL - verificar pagamento no bot
# ============================================================

async def verificar_pagamento_bot(tx_id: str) -> bool:
    """Verifica no bot se um TX foi pago"""
    try:
        bot = await client.get_entity(BOT_USERNAME)
        
        # Pedir extrato/histórico
        await client.send_message(bot, '/start')
        await asyncio.sleep(2)
        
        msgs = await client.get_messages(bot, limit=10)
        for msg in msgs:
            if msg.text and tx_id in msg.text:
                if any(p in msg.text.lower() for p in ['pago', 'confirmado', 'recebido', 'depositado']):
                    return True
    except:
        pass
    return False

# ============================================================
# GERAR PIX
# ============================================================

async def gerar_pix(valor: float, cliente_id: str = None, webhook_url: str = None) -> dict:
    async with _lock:
        try:
            bot = await client.get_entity(BOT_USERNAME)

            # Clicar botão DEPOSITAR
            messages = await client.get_messages(bot, limit=15)
            clicou = False
            for msg in messages:
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if 'DEPOSITAR' in btn.text:
                                await btn.click()
                                await asyncio.sleep(3)
                                clicou = True
                                break
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
                                    clicou = True
                                    break

            # Enviar valor
            valor_str = str(int(valor)) if valor == int(valor) else f"{valor:.2f}"
            await client.send_message(bot, valor_str)
            await asyncio.sleep(8)

            # Capturar resposta
            msgs = await client.get_messages(bot, limit=5)
            for msg in msgs:
                if msg.text and 'PIX Copia e Cola' in msg.text:
                    text = msg.text
                    pix_match = re.search(r'`(00020101[^`]+)`', text)
                    pix_code = pix_match.group(1) if pix_match else None
                    tx_match = re.search(r'txn_([a-f0-9]+)', text)
                    tx_id = f"txn_{tx_match.group(1)}" if tx_match else f"txn_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d.]+)', text)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"

                    if pix_code:
                        # Salvar no banco
                        salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url)
                        
                        return {
                            'success': True,
                            'pix_code': pix_code,
                            'tx_id': tx_id,
                            'valor': f"R$ {valor_conf}",
                            'status': 'pendente',
                            'webhook_ativo': bool(webhook_url),
                        }

            return {'success': False, 'error': 'Bot não respondeu. Tente novamente.'}

        except Exception as e:
            return {'success': False, 'error': str(e)}


# ============================================================
# ROTAS HTTP
# ============================================================

@web.middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, X-Webhook-URL, X-Cliente-ID',
        })
    response = await handler(request)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


async def route_index(request):
    html = load_html()
    return web.Response(text=html, content_type='text/html', charset='utf-8')


async def route_pix(request):
    """Gerar QR Code Pix"""
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        cliente_id = data.get('cliente_id')
        webhook_url = data.get('webhook_url')
        
        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'})
        
        result = await gerar_pix(valor, cliente_id, webhook_url)
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_status_tx(request):
    """Verificar status de uma transação (polling)"""
    tx_id = request.match_info.get('tx_id')
    if not tx_id:
        return web.json_response({'error': 'tx_id obrigatório'}, status=400)
    
    tx = buscar_transacao(tx_id)
    if not tx:
        return web.json_response({'error': 'Transação não encontrada'}, status=404)
    
    # Se ainda pendente, verificar no bot
    if tx['status'] == 'pendente':
        pago = await verificar_pagamento_bot(tx_id)
        if pago:
            confirmar_pagamento(tx_id)
            tx = buscar_transacao(tx_id)
    
    return web.json_response({
        'tx_id': tx['tx_id'],
        'valor': tx['valor'],
        'status': tx['status'],
        'created_at': tx['created_at'],
        'paid_at': tx.get('paid_at'),
        'pago': tx['status'] == 'pago',
    })


async def route_webhook_confirmar(request):
    """
    Endpoint Webhook - receber confirmação de pagamento externa
    
    POST /webhook/confirmar
    Headers: X-VortexPay-Secret: <seu_secret>
    Body: { "tx_id": "txn_xxx", "status": "pago" }
    """
    # Verificar secret
    secret = request.headers.get('X-VortexPay-Secret', '')
    if secret != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    
    try:
        data = await request.json()
        tx_id = data.get('tx_id')
        status = data.get('status')
        
        if not tx_id:
            return web.json_response({'error': 'tx_id obrigatório'}, status=400)
        
        if status == 'pago':
            confirmar_pagamento(tx_id)
            tx = buscar_transacao(tx_id)
            print(f'✅ Pagamento confirmado via webhook: {tx_id}')
            return web.json_response({
                'success': True,
                'message': f'Pagamento {tx_id} confirmado',
                'tx': tx
            })
        
        return web.json_response({'success': False, 'message': 'Status não reconhecido'})
        
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def route_listar_transacoes(request):
    """Listar todas as transações (painel admin)"""
    # Autenticação simples por secret
    secret = request.headers.get('X-VortexPay-Secret', '')
    auth = request.rel_url.query.get('secret', '')
    
    if secret != WEBHOOK_SECRET and auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    
    txs = listar_transacoes()
    total_pendente = sum(t['valor'] for t in txs if t['status'] == 'pendente')
    total_pago = sum(t['valor'] for t in txs if t['status'] == 'pago')
    
    return web.json_response({
        'transacoes': txs,
        'resumo': {
            'total': len(txs),
            'pendentes': len([t for t in txs if t['status'] == 'pendente']),
            'pagos': len([t for t in txs if t['status'] == 'pago']),
            'valor_pendente': total_pendente,
            'valor_recebido': total_pago,
        }
    })


async def route_status(request):
    me = await client.get_me()
    return web.json_response({
        'status': 'online',
        'conta': me.first_name,
        'id': me.id,
        'bot': BOT_USERNAME,
        'webhook_endpoint': '/webhook/confirmar',
        'webhook_secret': WEBHOOK_SECRET[:8] + '...',
    })


# ============================================================
# MAIN
# ============================================================

async def main():
    init_db()
    print('✅ Banco de dados inicializado')
    
    await client.start()
    me = await client.get_me()
    print(f'✅ Telegram: {me.first_name} (ID: {me.id})')

    # Iniciar monitoramento de confirmações
    await monitorar_confirmacoes()

    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', route_index)
    app.router.add_get('/index.html', route_index)
    
    # API de geração
    app.router.add_post('/api/pix', route_pix)
    app.router.add_route('OPTIONS', '/api/pix', lambda r: web.Response(status=200))
    
    # API de status por transação (polling)
    app.router.add_get('/api/status/{tx_id}', route_status_tx)
    
    # API admin - listar transações
    app.router.add_get('/api/transacoes', route_listar_transacoes)
    
    # Webhook - receber confirmação externa
    app.router.add_post('/webhook/confirmar', route_webhook_confirmar)
    app.router.add_route('OPTIONS', '/webhook/confirmar', lambda r: web.Response(status=200))
    
    # Status geral
    app.router.add_get('/api/status', route_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'✅ Servidor rodando na porta {PORT}')
    print(f'📡 Webhook endpoint: POST /webhook/confirmar')
    print(f'🔍 Status transação: GET /api/status/<tx_id>')
    print(f'📋 Lista transações: GET /api/transacoes?secret=<secret>')

    await client.run_until_disconnected()


if __name__ == '__main__':
    asyncio.run(main())
