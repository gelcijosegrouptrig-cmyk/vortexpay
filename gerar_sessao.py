import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = 35023140
API_HASH = 'a5fb75fd2a4497eab273c2a2f7b41d49'

async def gerar():
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    
    phone = input('📱 Telefone (com DDI, ex: +5511999...): ').strip()
    sent = await client.send_code_request(phone)
    print(f'✅ Código enviado via {sent.type}')
    
    code = input('🔑 Código do Telegram: ').strip()
    try:
        await client.sign_in(phone, code)
    except Exception as e:
        name = type(e).__name__
        if 'SessionPasswordNeeded' in name or 'password' in str(e).lower():
            senha = input('🔐 Senha 2FA: ').strip()
            await client.sign_in(password=senha)
        else:
            raise

    session_str = client.session.save()
    print(f'\n✅ SESSION STRING GERADA:')
    print(session_str)
    with open('session_string_nova.txt', 'w') as f:
        f.write(session_str)
    print('\n✅ Salvo em session_string_nova.txt')
    
    # Testar
    me = await client.get_me()
    print(f'👤 Logado como: {me.first_name} ({me.id})')
    await client.disconnect()

asyncio.run(gerar())
