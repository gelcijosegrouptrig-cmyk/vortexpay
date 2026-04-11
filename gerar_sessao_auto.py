#!/usr/bin/env python3
"""
Script para gerar nova sessão Telegram e atualizar automaticamente no Railway.
Execute após o flood expirar (~02:30 de 12/04/2026).

USO:
  python3 gerar_sessao_auto.py

  Ele pedirá o código que chegar no Telegram e atualizará via API.
"""
import asyncio, sys, os, time, json
import httpx  # pip install httpx

API_ID = 35023140
API_HASH = 'a5fb75fd2a4497eab273c2a2f7b41d49'
PHONE = '+5511970569294'
RAILWAY_URL = 'https://web-production-9f54e.up.railway.app'
WEBHOOK_SECRET = 'vortex_webhook_2024'

async def main():
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon.errors import SessionPasswordNeededError, FloodWaitError
    except ImportError:
        print('❌ Instale: pip install telethon')
        return

    print('🔄 Tentando conectar ao Telegram...')
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    try:
        sent = await client.send_code_request(PHONE)
        print(f'✅ Código enviado para {PHONE}!')
        print('📱 Abra o Telegram e copie o código (5 dígitos)')
        code = input('Digite o código: ').strip()

        try:
            await client.sign_in(PHONE, code, phone_code_hash=sent.phone_code_hash)
        except SessionPasswordNeededError:
            senha = input('🔐 Digite sua senha 2FA: ').strip()
            await client.sign_in(password=senha)

        me = await client.get_me()
        session_str = client.session.save()
        print(f'✅ Logado como: {me.first_name} ({me.id})')
        print(f'📄 Sessão gerada ({len(session_str)} chars)')

        # Salvar localmente
        with open('session_string.txt', 'w') as f:
            f.write(session_str)
        print('💾 Sessão salva em session_string.txt')

        # Atualizar Railway via API
        print(f'\n🚀 Atualizando Railway em {RAILWAY_URL}...')
        async with httpx.AsyncClient(timeout=30) as http:
            r = await http.post(
                f'{RAILWAY_URL}/api/atualizar-sessao',
                json={'session_string': session_str},
                headers={'X-VortexPay-Secret': WEBHOOK_SECRET}
            )
            result = r.json()
            if result.get('success'):
                print(f'✅ Railway atualizado! {result.get("message")}')
            else:
                print(f'⚠️ Resposta Railway: {result}')
                print(f'💡 Atualize manualmente em: railway.app → Variables → SESSION_STR')

    except FloodWaitError as e:
        secs = e.seconds
        h, m = divmod(secs, 3600)
        m //= 60
        print(f'❌ FloodWait! Aguarde {h}h {m}min antes de tentar novamente.')
        print(f'⏰ Tente após: {time.strftime("%H:%M do dia %d/%m", time.localtime(time.time() + secs))}')
    except Exception as e:
        print(f'❌ Erro: {type(e).__name__}: {e}')
    finally:
        await client.disconnect()

asyncio.run(main())
