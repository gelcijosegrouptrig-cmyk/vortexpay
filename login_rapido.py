#!/usr/bin/env python3
"""Login rápido - roda e espera o código em tempo real"""
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
import httpx

API_ID = 35023140
API_HASH = 'a5fb75fd2a4497eab273c2a2f7b41d49'
PHONE = '+5511970569294'
RAILWAY_URL = 'https://web-production-9f54e.up.railway.app'
WEBHOOK_SECRET = 'vortex_webhook_2024'

async def main():
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        print(f'📱 Enviando código para {PHONE}...')
        sent = await client.send_code_request(PHONE)
        print('✅ Código enviado! Abra o Telegram agora.')
        print()
        
        code = input('👉 Digite o código aqui: ').strip()
        
        try:
            await client.sign_in(PHONE, code, phone_code_hash=sent.phone_code_hash)
        except SessionPasswordNeededError:
            senha = input('🔐 Digite sua senha 2FA: ').strip()
            await client.sign_in(password=senha)

        me = await client.get_me()
        session_str = client.session.save()
        
        print(f'\n✅ Logado como: {me.first_name} (ID: {me.id})')
        print(f'📄 Sessão: {len(session_str)} caracteres')
        
        # Salvar localmente
        with open('session_string.txt', 'w') as f:
            f.write(session_str)
        print('💾 Sessão salva em session_string.txt')
        
        # Enviar para Railway
        print(f'\n🚀 Atualizando Railway...')
        async with httpx.AsyncClient(timeout=30) as http:
            r = await http.post(
                f'{RAILWAY_URL}/api/atualizar-sessao',
                json={'session_string': session_str},
                headers={'X-PaynexBet-Secret': WEBHOOK_SECRET}
            )
            result = r.json()
            if result.get('success'):
                print(f'✅ Railway atualizado com sucesso!')
                print(f'🎉 Telegram integrado!')
            else:
                print(f'⚠️ Resposta: {result}')
                
    except FloodWaitError as e:
        h = e.seconds // 3600
        m = (e.seconds % 3600) // 60
        print(f'❌ FloodWait: aguarde {h}h {m}min')
    except Exception as e:
        print(f'❌ Erro: {type(e).__name__}: {e}')
    finally:
        await client.disconnect()

asyncio.run(main())
