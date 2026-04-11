"""
Servidor VortexPay 24/7 - Web + API Telegram
"""
import asyncio
import re
import json
import os
from aiohttp import web
from telethon import TelegramClient

API_ID = 35023140
API_HASH = 'a5fb75fd2a4497eab273c2a2f7b41d49'
BOT_USERNAME = 'VortexBank_bot'
SESSION_FILE = 'vortex_session'
PORT = int(os.environ.get('PORT', 5060))

client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
_lock = asyncio.Lock()

# Carregar HTML
HTML = open('index.html', 'r', encoding='utf-8').read()

async def gerar_pix(valor: float) -> dict:
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
            await asyncio.sleep(7)

            # Capturar resposta
            msgs = await client.get_messages(bot, limit=5)
            for msg in msgs:
                if msg.text and 'PIX Copia e Cola' in msg.text:
                    text = msg.text
                    pix_match = re.search(r'`(00020101[^`]+)`', text)
                    pix_code = pix_match.group(1) if pix_match else None
                    tx_match = re.search(r'txn_([a-f0-9]+)', text)
                    tx_id = f"txn_{tx_match.group(1)}" if tx_match else None
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d.]+)', text)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"

                    if pix_code:
                        return {
                            'success': True,
                            'pix_code': pix_code,
                            'tx_id': tx_id,
                            'valor': f"R$ {valor_conf}",
                        }

            return {'success': False, 'error': 'Bot não respondeu. Tente novamente.'}

        except Exception as e:
            return {'success': False, 'error': str(e)}


@web.middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        })
    response = await handler(request)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


async def route_index(request):
    return web.Response(text=HTML, content_type='text/html', charset='utf-8')


async def route_pix(request):
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'})
        result = await gerar_pix(valor)
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_status(request):
    me = await client.get_me()
    return web.json_response({
        'status': 'online',
        'conta': me.first_name,
        'id': me.id,
        'bot': BOT_USERNAME
    })


async def main():
    await client.start()
    me = await client.get_me()
    print(f'✅ Telegram: {me.first_name} (ID: {me.id})')

    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get('/', route_index)
    app.router.add_get('/index.html', route_index)
    app.router.add_post('/api/pix', route_pix)
    app.router.add_get('/api/status', route_status)
    app.router.add_route('OPTIONS', '/api/pix', lambda r: web.Response(status=200))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'✅ Servidor rodando na porta {PORT}')

    await asyncio.Event().wait()


if __name__ == '__main__':
    asyncio.run(main())
