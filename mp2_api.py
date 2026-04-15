"""
mp2_api.py — Integração Mercado Pago PIX para @paypix_nexbot
Banco independente: tabelas mp2_*
"""
import os
import uuid
import json
import time
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime

# ─── CREDENCIAIS MERCADO PAGO ─────────────────────────────────────────────────
MP2_ACCESS_TOKEN = os.environ.get('MP2_ACCESS_TOKEN', '')   # Token de produção/sandbox do bot2
MP2_PUBLIC_KEY   = os.environ.get('MP2_PUBLIC_KEY', '')     # Chave pública (opcional no backend)
MP2_WEBHOOK_SECRET = os.environ.get('MP2_WEBHOOK_SECRET', 'mp2_webhook_2024')

MP2_BASE_URL = 'https://api.mercadopago.com'

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway')

# ─── CONEXÃO POSTGRESQL ───────────────────────────────────────────────────────
def _get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)

# ─── CRIAR TABELAS mp2_* ──────────────────────────────────────────────────────
def init_mp2_db():
    """Cria as tabelas mp2_* se não existirem (banco independente dentro do mesmo PostgreSQL)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS mp2_usuarios (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE NOT NULL,
        username TEXT,
        nome TEXT,
        saldo NUMERIC(12,2) DEFAULT 0.00,
        total_depositado NUMERIC(12,2) DEFAULT 0.00,
        total_sacado NUMERIC(12,2) DEFAULT 0.00,
        referido_por BIGINT,
        criado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS mp2_transacoes (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT NOT NULL,
        tipo TEXT NOT NULL,          -- 'deposito' | 'saque' | 'comissao'
        valor NUMERIC(12,2) NOT NULL,
        status TEXT DEFAULT 'pendente',   -- 'pendente' | 'confirmado' | 'cancelado'
        mp_payment_id TEXT,          -- ID pagamento MercadoPago
        mp_external_ref TEXT UNIQUE, -- referência interna (uuid)
        pix_copia_cola TEXT,         -- código Pix gerado
        pix_qr_base64 TEXT,          -- QR code base64
        descricao TEXT,
        criado_em TIMESTAMP DEFAULT NOW(),
        atualizado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS mp2_saques (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT NOT NULL,
        valor NUMERIC(12,2) NOT NULL,
        chave_pix TEXT NOT NULL,
        tipo_chave TEXT NOT NULL,    -- 'cpf' | 'cnpj' | 'email' | 'telefone' | 'aleatoria'
        status TEXT DEFAULT 'pendente',  -- 'pendente' | 'aprovado' | 'rejeitado'
        obs TEXT,
        criado_em TIMESTAMP DEFAULT NOW(),
        processado_em TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS mp2_config (
        chave TEXT PRIMARY KEY,
        valor TEXT,
        atualizado_em TIMESTAMP DEFAULT NOW()
    );

    INSERT INTO mp2_config (chave, valor) VALUES ('saque_minimo', '20') ON CONFLICT DO NOTHING;
    INSERT INTO mp2_config (chave, valor) VALUES ('deposito_minimo', '5') ON CONFLICT DO NOTHING;
    INSERT INTO mp2_config (chave, valor) VALUES ('deposito_maximo', '10000') ON CONFLICT DO NOTHING;
    INSERT INTO mp2_config (chave, valor) VALUES ('comissao_pct', '5') ON CONFLICT DO NOTHING;

    CREATE TABLE IF NOT EXISTS mp2_canais (
        tipo TEXT PRIMARY KEY,
        canal_id BIGINT NOT NULL,
        invite_link TEXT,
        nome TEXT,
        criado_em TIMESTAMP DEFAULT NOW()
    );
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
        conn.close()
        print('✅ [mp2_db] Tabelas mp2_* criadas/verificadas!', flush=True)
    except Exception as e:
        print(f'❌ [mp2_db] Erro ao criar tabelas: {e}', flush=True)

# ─── USUÁRIOS ─────────────────────────────────────────────────────────────────
def mp2_get_ou_criar_usuario(telegram_id: int, username: str = '', nome: str = '', referido_por: int = None) -> dict:
    """Busca ou cria usuário na tabela mp2_usuarios."""
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute('SELECT * FROM mp2_usuarios WHERE telegram_id = %s', (telegram_id,))
        usuario = cur.fetchone()
        if usuario:
            cur.close(); conn.close()
            return dict(usuario)
        # Criar novo
        cur.execute(
            """INSERT INTO mp2_usuarios (telegram_id, username, nome, referido_por)
               VALUES (%s, %s, %s, %s) RETURNING *""",
            (telegram_id, username, nome, referido_por)
        )
        usuario = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        # Processar comissão ao referidor
        if referido_por:
            mp2_creditar_comissao_indicacao(referido_por, telegram_id)
        return dict(usuario)
    except Exception as e:
        print(f'[mp2] Erro get_ou_criar_usuario: {e}', flush=True)
        return {}

def mp2_get_saldo(telegram_id: int) -> float:
    """Retorna saldo atual do usuário."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute('SELECT saldo FROM mp2_usuarios WHERE telegram_id = %s', (telegram_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        return float(row[0]) if row else 0.0
    except Exception as e:
        print(f'[mp2] Erro get_saldo: {e}', flush=True)
        return 0.0

def mp2_atualizar_saldo(telegram_id: int, delta: float, tipo: str = ''):
    """Incrementa ou decrementa saldo (delta positivo = crédito, negativo = débito)."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        if delta > 0 and tipo == 'deposito':
            cur.execute(
                """UPDATE mp2_usuarios
                   SET saldo = saldo + %s,
                       total_depositado = total_depositado + %s
                   WHERE telegram_id = %s""",
                (delta, delta, telegram_id)
            )
        elif delta < 0 and tipo == 'saque':
            cur.execute(
                """UPDATE mp2_usuarios
                   SET saldo = saldo + %s,
                       total_sacado = total_sacado + %s
                   WHERE telegram_id = %s""",
                (delta, abs(delta), telegram_id)
            )
        else:
            cur.execute(
                'UPDATE mp2_usuarios SET saldo = saldo + %s WHERE telegram_id = %s',
                (delta, telegram_id)
            )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f'[mp2] Erro atualizar_saldo: {e}', flush=True)

def mp2_creditar_comissao_indicacao(referidor_id: int, novo_usuario_id: int):
    """Futuramente: credita bônus por indicação (não implementado ainda)."""
    pass

# ─── GERAR PIX (MERCADO PAGO) ─────────────────────────────────────────────────
def mp2_gerar_pix(
    telegram_id: int,
    valor: float,
    nome_pagador: str = 'Cliente',
    descricao: str = '',
    email_pagador: str = '',
    cpf_pagador: str = '',
    expiracao_minutos: int = 30,
) -> dict:
    """
    Gera cobrança Pix via Mercado Pago (Checkout API).
    Conforme docs: https://www.mercadopago.com.br/developers/pt/docs/checkout-api-payments/integration-configuration/integrate-pix
    Retorna: {success, pix_copia_cola, qr_base64, ticket_url, payment_id, external_ref, valor}
    """
    if not MP2_ACCESS_TOKEN:
        return {'success': False, 'error': 'Token Mercado Pago não configurado. Contate o suporte.'}

    external_ref = f"mp2_{telegram_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"

    # Calcular data de expiração (padrão 30 minutos, máximo 30 dias)
    expiracao_minutos = max(30, min(expiracao_minutos, 43200))  # 30min a 30 dias
    from datetime import datetime, timezone, timedelta
    data_expiracao = (datetime.now(timezone.utc) + timedelta(minutes=expiracao_minutos)).strftime('%Y-%m-%dT%H:%M:%S.000-03:00')

    # Email padrão se não fornecido
    email = email_pagador if email_pagador else f"user_{telegram_id}@paypixnex.com"

    # Payer base
    payer: dict = {
        "email": email,
        "first_name": nome_pagador,
    }

    # Adicionar CPF ao payer se informado (melhora aprovação)
    cpf_limpo = ''.join(filter(str.isdigit, cpf_pagador)) if cpf_pagador else ''
    if len(cpf_limpo) == 11:
        payer["identification"] = {
            "type": "CPF",
            "number": cpf_limpo
        }

    payload = {
        "transaction_amount": float(round(valor, 2)),
        "description": descricao if descricao else f"Depósito PayPixNex - ID {telegram_id}",
        "payment_method_id": "pix",
        "external_reference": external_ref,
        "date_of_expiration": data_expiracao,
        "payer": payer,
    }

    headers = {
        'Authorization': f'Bearer {MP2_ACCESS_TOKEN}',
        'Content-Type': 'application/json',
        'X-Idempotency-Key': external_ref
    }

    try:
        resp = requests.post(
            f'{MP2_BASE_URL}/v1/payments',
            json=payload,
            headers=headers,
            timeout=30
        )
        data = resp.json()

        if resp.status_code not in (200, 201):
            erro = data.get('message', data.get('error', str(data)))
            print(f'[mp2_gerar_pix] Erro MP: {resp.status_code} — {erro}', flush=True)
            return {'success': False, 'error': f'Erro ao gerar Pix: {erro}'}

        # Extrair dados PIX conforme doc MP
        pix_info = data.get('point_of_interaction', {}).get('transaction_data', {})
        pix_copia_cola = pix_info.get('qr_code', '')
        qr_base64      = pix_info.get('qr_code_base64', '')
        ticket_url     = pix_info.get('ticket_url', '')
        payment_id     = str(data.get('id', ''))

        # Salvar transação no banco
        _mp2_salvar_transacao(
            telegram_id=telegram_id,
            tipo='deposito',
            valor=valor,
            status='pendente',
            mp_payment_id=payment_id,
            mp_external_ref=external_ref,
            pix_copia_cola=pix_copia_cola,
            pix_qr_base64=qr_base64
        )

        print(f'✅ [mp2_gerar_pix] Pix gerado: {payment_id} R${valor} exp:{expiracao_minutos}min — {external_ref}', flush=True)
        return {
            'success':        True,
            'payment_id':     payment_id,
            'external_ref':   external_ref,
            'pix_copia_cola': pix_copia_cola,
            'qr_base64':      qr_base64,
            'ticket_url':     ticket_url,
            'valor':          valor,
            'expira_em':      expiracao_minutos,
        }

    except requests.exceptions.Timeout:
        return {'success': False, 'error': 'Timeout ao conectar com Mercado Pago. Tente novamente.'}
    except Exception as e:
        print(f'[mp2_gerar_pix] Exceção: {e}', flush=True)
        return {'success': False, 'error': 'Erro interno ao gerar Pix.'}

# ─── VERIFICAR STATUS PAGAMENTO ────────────────────────────────────────────────
def mp2_verificar_pagamento(payment_id: str) -> dict:
    """Consulta status de um pagamento no Mercado Pago."""
    if not MP2_ACCESS_TOKEN:
        return {'success': False, 'status': 'unknown'}
    try:
        headers = {'Authorization': f'Bearer {MP2_ACCESS_TOKEN}'}
        resp = requests.get(f'{MP2_BASE_URL}/v1/payments/{payment_id}', headers=headers, timeout=20)
        data = resp.json()
        return {
            'success': True,
            'payment_id': payment_id,
            'status': data.get('status', 'unknown'),          # approved | pending | rejected | cancelled
            'status_detail': data.get('status_detail', ''),
            'valor': data.get('transaction_amount', 0),
            'external_ref': data.get('external_reference', ''),
            'data_aprovacao': data.get('date_approved', ''),
            'ticket_url': data.get('point_of_interaction', {}).get('transaction_data', {}).get('ticket_url', ''),
            'qr_code':    data.get('point_of_interaction', {}).get('transaction_data', {}).get('qr_code', ''),
            'qr_base64':  data.get('point_of_interaction', {}).get('transaction_data', {}).get('qr_code_base64', ''),
        }
    except Exception as e:
        print(f'[mp2_verificar_pagamento] Erro: {e}', flush=True)
        return {'success': False, 'status': 'error', 'error': str(e)}

# ─── SALVAR / BUSCAR TRANSAÇÕES ───────────────────────────────────────────────
def _mp2_salvar_transacao(telegram_id, tipo, valor, status, mp_payment_id='',
                           mp_external_ref='', pix_copia_cola='', pix_qr_base64='', descricao=''):
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO mp2_transacoes
               (telegram_id, tipo, valor, status, mp_payment_id, mp_external_ref, pix_copia_cola, pix_qr_base64, descricao)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (mp_external_ref) DO NOTHING""",
            (telegram_id, tipo, valor, status, mp_payment_id,
             mp_external_ref, pix_copia_cola, pix_qr_base64, descricao)
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f'[mp2] Erro salvar_transacao: {e}', flush=True)

def mp2_confirmar_pagamento_webhook(external_ref: str, payment_id: str) -> bool:
    """
    Confirma pagamento via webhook do MP.
    Credita saldo ao usuário e marca transação como 'confirmado'.
    Retorna True se processou, False se já processado ou não encontrado.
    """
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Buscar transação pendente
        cur.execute(
            "SELECT * FROM mp2_transacoes WHERE mp_external_ref = %s AND status = 'pendente'",
            (external_ref,)
        )
        tx = cur.fetchone()
        if not tx:
            cur.close(); conn.close()
            return False  # Já processado ou não existe

        telegram_id = tx['telegram_id']
        valor = float(tx['valor'])

        # Atualizar status da transação
        cur.execute(
            """UPDATE mp2_transacoes
               SET status = 'confirmado', mp_payment_id = %s, atualizado_em = NOW()
               WHERE mp_external_ref = %s""",
            (payment_id, external_ref)
        )

        # Verificar se usuário existe antes de creditar
        cur.execute('SELECT id FROM mp2_usuarios WHERE telegram_id = %s', (telegram_id,))
        if cur.fetchone():
            cur.execute(
                """UPDATE mp2_usuarios
                   SET saldo = saldo + %s, total_depositado = total_depositado + %s
                   WHERE telegram_id = %s""",
                (valor, valor, telegram_id)
            )

        conn.commit()
        cur.close(); conn.close()
        print(f'✅ [mp2_webhook] Pagamento confirmado: {external_ref} R${valor} → user {telegram_id}', flush=True)
        return True
    except Exception as e:
        print(f'[mp2_webhook] Erro confirmar_pagamento: {e}', flush=True)
        return False

# ─── SOLICITAR SAQUE ──────────────────────────────────────────────────────────
def mp2_solicitar_saque(telegram_id: int, valor: float, chave_pix: str, tipo_chave: str) -> dict:
    """Registra solicitação de saque manual (admin processa)."""
    try:
        saldo = mp2_get_saldo(telegram_id)
        saque_min = float(mp2_get_config('saque_minimo', '20'))
        if valor > saldo:
            return {'success': False, 'error': f'Saldo insuficiente. Saldo atual: R$ {saldo:.2f}'}
        if valor < saque_min:
            return {'success': False, 'error': f'Valor mínimo para saque: R$ {saque_min:.2f}'}

        # Bloquear saldo preventivamente
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            'UPDATE mp2_usuarios SET saldo = saldo - %s, total_sacado = total_sacado + %s WHERE telegram_id = %s',
            (valor, valor, telegram_id)
        )
        cur.execute(
            """INSERT INTO mp2_saques (telegram_id, valor, chave_pix, tipo_chave, status)
               VALUES (%s, %s, %s, %s, 'pendente') RETURNING id""",
            (telegram_id, valor, chave_pix, tipo_chave)
        )
        saque_id = cur.fetchone()[0]
        conn.commit()
        cur.close(); conn.close()

        print(f'📤 [mp2_saque] Solicitação #{saque_id}: R${valor} → {chave_pix} (user {telegram_id})', flush=True)
        return {'success': True, 'saque_id': saque_id, 'valor': valor, 'chave_pix': chave_pix}
    except Exception as e:
        print(f'[mp2_saque] Erro: {e}', flush=True)
        return {'success': False, 'error': 'Erro ao registrar saque.'}

# ─── HISTÓRICO ────────────────────────────────────────────────────────────────
def mp2_historico(telegram_id: int, limite: int = 10) -> list:
    """Retorna últimas transações do usuário."""
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT tipo, valor, status, criado_em
               FROM mp2_transacoes
               WHERE telegram_id = %s
               ORDER BY criado_em DESC LIMIT %s""",
            (telegram_id, limite)
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f'[mp2_historico] Erro: {e}', flush=True)
        return []

# ─── ESTATÍSTICAS ADMIN ───────────────────────────────────────────────────────
def mp2_stats_admin() -> dict:
    """Retorna estatísticas gerais para o painel admin."""
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute('SELECT COUNT(*) AS total FROM mp2_usuarios')
        total_usuarios = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) AS total, COALESCE(SUM(valor),0) AS soma FROM mp2_transacoes WHERE status = 'confirmado' AND tipo = 'deposito'")
        dep = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS total, COALESCE(SUM(valor),0) AS soma FROM mp2_saques WHERE status = 'pendente'")
        saques_pend = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS total, COALESCE(SUM(valor),0) AS soma FROM mp2_saques WHERE status = 'aprovado'")
        saques_aprov = cur.fetchone()

        # Totais de parceiros/afiliados
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE ativo = TRUE)  AS parceiros_ativos,
              COUNT(*)                               AS parceiros_total,
              COALESCE(SUM(total_gerado),0)          AS total_gerado_parceiros,
              COALESCE(SUM(total_comissao),0)        AS total_comissao_parceiros,
              COALESCE(SUM(total_pago),0)            AS total_pago_parceiros
            FROM mp2_parceiros
        """)
        parc = cur.fetchone()

        cur.close(); conn.close()
        return {
            'total_usuarios': total_usuarios,
            'depositos_confirmados': int(dep['total']),
            'total_depositado': float(dep['soma']),
            'saques_pendentes': int(saques_pend['total']),
            'valor_saques_pendentes': float(saques_pend['soma']),
            'saques_aprovados': int(saques_aprov['total']),
            'valor_saques_aprovados': float(saques_aprov['soma']),
            # Parceiros/afiliados
            'parceiros_ativos': int(parc['parceiros_ativos'] or 0),
            'parceiros_total': int(parc['parceiros_total'] or 0),
            'total_gerado_parceiros': float(parc['total_gerado_parceiros'] or 0),
            'total_comissao_parceiros': float(parc['total_comissao_parceiros'] or 0),
            'total_pago_parceiros': float(parc['total_pago_parceiros'] or 0),
        }
    except Exception as e:
        print(f'[mp2_stats] Erro: {e}', flush=True)
        return {}

def mp2_listar_saques_pendentes() -> list:
    """Lista saques pendentes para o admin processar."""
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT s.*, u.username, u.nome
               FROM mp2_saques s
               JOIN mp2_usuarios u ON u.telegram_id = s.telegram_id
               WHERE s.status = 'pendente'
               ORDER BY s.criado_em ASC"""
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f'[mp2_saques_pend] Erro: {e}', flush=True)
        return []

def mp2_processar_saque(saque_id: int, aprovado: bool, obs: str = '') -> bool:
    """Admin aprova ou rejeita saque. Se rejeitado, devolve saldo ao usuário."""
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute('SELECT * FROM mp2_saques WHERE id = %s', (saque_id,))
        saque = cur.fetchone()
        if not saque or saque['status'] != 'pendente':
            cur.close(); conn.close()
            return False

        status_novo = 'aprovado' if aprovado else 'rejeitado'
        cur.execute(
            "UPDATE mp2_saques SET status = %s, obs = %s, processado_em = NOW() WHERE id = %s",
            (status_novo, obs, saque_id)
        )

        if not aprovado:
            # Devolver saldo ao usuário
            cur.execute(
                'UPDATE mp2_usuarios SET saldo = saldo + %s, total_sacado = total_sacado - %s WHERE telegram_id = %s',
                (saque['valor'], saque['valor'], saque['telegram_id'])
            )

        conn.commit()
        cur.close(); conn.close()
        print(f'✅ [mp2_processar_saque] #{saque_id} → {status_novo}', flush=True)
        return True
    except Exception as e:
        print(f'[mp2_processar_saque] Erro: {e}', flush=True)
        return False

# ─── CONFIG ──────────────────────────────────────────────────────────────────
def mp2_get_config(chave: str = None, default: str = '') -> str:
    """Retorna um ou todos os valores da config."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        if chave:
            cur.execute('SELECT valor FROM mp2_config WHERE chave = %s', (chave,))
            row = cur.fetchone()
            cur.close(); conn.close()
            return row[0] if row else default
        else:
            # Retorna dict com todas as configs
            cur.execute('SELECT chave, valor FROM mp2_config')
            rows = cur.fetchall()
            cur.close(); conn.close()
            return {r[0]: r[1] for r in rows}
    except:
        return default if chave else {}

# ─── CANAIS ───────────────────────────────────────────────────────────────────
def mp2_salvar_canal(tipo: str, canal_id: int, invite_link: str, nome: str = '') -> bool:
    """Salva ou atualiza informações do canal no banco."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO mp2_canais (tipo, canal_id, invite_link, nome)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tipo) DO UPDATE
              SET canal_id = EXCLUDED.canal_id,
                  invite_link = EXCLUDED.invite_link,
                  nome = EXCLUDED.nome,
                  criado_em = NOW()
        """, (tipo, canal_id, invite_link, nome))
        conn.commit()
        cur.close(); conn.close()
        print(f'✅ [mp2_canal] Canal {tipo} salvo: {canal_id}', flush=True)
        return True
    except Exception as e:
        print(f'❌ [mp2_canal] Erro salvar canal: {e}', flush=True)
        return False

def mp2_get_canal(tipo: str = 'notificacoes') -> dict:
    """Retorna dados do canal ou None se não existir."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute('SELECT tipo, canal_id, invite_link, nome FROM mp2_canais WHERE tipo = %s', (tipo,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            return {'tipo': row[0], 'canal_id': row[1], 'invite_link': row[2], 'nome': row[3]}
        return None
    except Exception as e:
        print(f'[mp2_canal] Erro get_canal: {e}', flush=True)
        return None

def mp2_notificar_canal_deposito(telegram_id: int, valor: float, payment_id: str):
    """
    Notificação salva em fila para ser enviada pelo bot.
    (O envio assíncrono é feito pelo bot2_handler via asyncio)
    """
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        # Busca nome do usuário
        cur.execute('SELECT nome, username FROM mp2_usuarios WHERE telegram_id = %s', (telegram_id,))
        row = cur.fetchone()
        nome     = row[0] if row else 'Usuário'
        username = row[1] if row else ''
        cur.close(); conn.close()
        # Log para o bot consumir
        print(
            f'📥 [mp2_notif] DEPOSITO_CONFIRMADO | '
            f'user={telegram_id} ({username}) | '
            f'valor=R${valor:.2f} | payment_id={payment_id}',
            flush=True
        )
    except Exception as e:
        print(f'[mp2_notif] Erro: {e}', flush=True)

# ─── PARCEIROS / AFILIADOS ────────────────────────────────────────────────────
def mp2_criar_parceiro(nome: str, chave_pix: str, tipo_chave: str = 'email',
                       comissao_pct: float = None, codigo: str = None) -> dict:
    """
    Cria um novo parceiro afiliado.
    Retorna dict com {success, parceiro, link} ou {success:False, error}.
    """
    import re
    try:
        # Gerar código único se não fornecido
        if not codigo:
            base = re.sub(r'[^a-z0-9]', '', nome.lower())[:12]
            codigo = f"{base}-{str(uuid.uuid4())[:6]}"

        # Pegar % padrão do config se não especificado
        if comissao_pct is None:
            try:
                pct_str = mp2_get_config('comissao_pct', '10')
                comissao_pct = float(pct_str)
            except:
                comissao_pct = 10.0

        BASE_URL = os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'web-production-9f54e.up.railway.app')
        if not BASE_URL.startswith('http'):
            BASE_URL = 'https://' + BASE_URL
        link = f"{BASE_URL}/bot?ref={codigo}"

        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO mp2_parceiros
                (codigo, nome, chave_pix, tipo_chave, comissao_pct, ativo, link, criado_em)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, NOW())
            ON CONFLICT (codigo) DO UPDATE
              SET nome = EXCLUDED.nome,
                  chave_pix = EXCLUDED.chave_pix,
                  tipo_chave = EXCLUDED.tipo_chave,
                  comissao_pct = EXCLUDED.comissao_pct,
                  link = EXCLUDED.link
            RETURNING id, codigo, nome, chave_pix, tipo_chave, comissao_pct, link, ativo
        """, (codigo, nome, chave_pix, tipo_chave, comissao_pct, link))
        row = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()

        parceiro = {
            'id': row[0], 'codigo': row[1], 'nome': row[2],
            'chave_pix': row[3], 'tipo_chave': row[4],
            'comissao_pct': float(row[5]), 'link': row[6], 'ativo': row[7]
        }
        print(f'✅ [mp2_parceiro] Criado: {codigo} → {nome} ({comissao_pct}%) link={link}', flush=True)
        return {'success': True, 'parceiro': parceiro, 'link': link}
    except Exception as e:
        print(f'[mp2_parceiro] Erro criar: {e}', flush=True)
        return {'success': False, 'error': str(e)}


def mp2_listar_parceiros() -> list:
    """Retorna lista de todos os parceiros com estatísticas."""
    try:
        conn = _get_conn()
        cur  = conn.cursor(psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT p.id, p.codigo, p.nome, p.chave_pix, p.tipo_chave,
                   p.comissao_pct, p.ativo, p.link, p.criado_em,
                   p.total_gerado, p.total_comissao, p.total_pago,
                   COUNT(t.id) FILTER (WHERE t.status = 'confirmado') AS qtd_pagamentos
            FROM mp2_parceiros p
            LEFT JOIN mp2_transacoes t ON t.parceiro_codigo = p.codigo
            GROUP BY p.id
            ORDER BY p.criado_em DESC
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d['comissao_pct'] = float(d['comissao_pct'] or 0)
            d['total_gerado'] = float(d['total_gerado'] or 0)
            d['total_comissao'] = float(d['total_comissao'] or 0)
            d['total_pago'] = float(d['total_pago'] or 0)
            d['qtd_pagamentos'] = int(d['qtd_pagamentos'] or 0)
            if d['criado_em']:
                d['criado_em'] = d['criado_em'].strftime('%d/%m/%Y %H:%M')
            result.append(d)
        return result
    except Exception as e:
        print(f'[mp2_parceiro] Erro listar: {e}', flush=True)
        return []


def mp2_get_parceiro(codigo: str) -> dict:
    """Retorna dados de um parceiro pelo código."""
    try:
        conn = _get_conn()
        cur  = conn.cursor(psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, codigo, nome, chave_pix, tipo_chave,
                   comissao_pct, ativo, link, total_gerado, total_comissao, total_pago
            FROM mp2_parceiros WHERE codigo = %s
        """, (codigo,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            d = dict(row)
            d['comissao_pct'] = float(d['comissao_pct'] or 0)
            d['total_gerado'] = float(d['total_gerado'] or 0)
            d['total_comissao'] = float(d['total_comissao'] or 0)
            d['total_pago'] = float(d['total_pago'] or 0)
            return d
        return None
    except Exception as e:
        print(f'[mp2_parceiro] Erro get: {e}', flush=True)
        return None


def mp2_deletar_parceiro(codigo: str) -> bool:
    """Deleta (ou desativa) um parceiro."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("UPDATE mp2_parceiros SET ativo = FALSE WHERE codigo = %s", (codigo,))
        conn.commit()
        cur.close(); conn.close()
        return True
    except Exception as e:
        print(f'[mp2_parceiro] Erro deletar: {e}', flush=True)
        return False


def mp2_pagar_comissao_parceiro(parceiro_codigo: str, valor: float) -> dict:
    """
    Paga comissão ao parceiro via PIX Mercado Pago.
    USA /v1/account/bank_transfers — endpoint CORRETO para enviar dinheiro via PIX.
    /v1/payments RECEBE dinheiro (cobra). Bank_transfers ENVIA dinheiro (paga).
    Registra na tabela mp2_comissao_saques.
    """
    try:
        parceiro = mp2_get_parceiro(parceiro_codigo)
        if not parceiro:
            return {'success': False, 'error': 'Parceiro não encontrado'}
        if not parceiro.get('ativo'):
            return {'success': False, 'error': 'Parceiro inativo'}

        chave_pix  = parceiro['chave_pix']
        tipo_chave = parceiro['tipo_chave']

        # Registrar saque de comissão como pendente
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO mp2_comissao_saques
                (parceiro_codigo, valor, chave_pix, tipo_chave, status, criado_em)
            VALUES (%s, %s, %s, %s, 'pendente', NOW())
            RETURNING id
        """, (parceiro_codigo, valor, chave_pix, tipo_chave))
        saque_id = cur.fetchone()[0]
        conn.commit()
        cur.close(); conn.close()

        # Buscar token MP2
        token = MP2_ACCESS_TOKEN
        if not token:
            try:
                conn2 = _get_conn()
                c2 = conn2.cursor()
                c2.execute("SELECT valor FROM mp2_config WHERE chave='mp2_access_token'")
                r = c2.fetchone()
                conn2.close()
                if r: token = r[0]
            except: pass

        if not token:
            # Sem token → fica pendente para pagamento manual pelo admin
            return {'success': False, 'error': 'Token MP não configurado — saque pendente para pagamento manual', 'saque_id': saque_id, 'pendente': True}

        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
                'X-Idempotency-Key': str(uuid.uuid4())
            }

            # Mapa tipo_chave → tipo Mercado Pago para bank_transfers
            tipo_mp_map = {
                'cpf':       'CPF',
                'cnpj':      'CNPJ',
                'email':     'EMAIL',
                'telefone':  'PHONE',
                'aleatoria': 'RANDOM_KEY'
            }
            tipo_mp = tipo_mp_map.get(tipo_chave.lower(), 'CPF')

            # ✅ ENDPOINT CORRETO: /v1/account/bank_transfers — ENVIA dinheiro via PIX
            # NÃO usar /v1/payments (que RECEBE/cobra dinheiro)
            payload = {
                'amount':      round(valor, 2),
                'origin_id':   int(uuid.uuid4().int % 1_000_000_000),
                'description': f'Comissão parceiro {parceiro_codigo}',
                'destination': {
                    'type':      'bank_account',
                    'pix_key':   chave_pix,
                    'pix_key_type': tipo_mp
                }
            }
            resp = requests.post(
                f'{MP2_BASE_URL}/v1/account/bank_transfers',
                json=payload, headers=headers, timeout=30
            )
            resp_data = resp.json()
            mp_id = str(resp_data.get('id', resp_data.get('transfer_id', '')))

            conn3 = _get_conn()
            c3 = conn3.cursor()

            if resp.status_code in (200, 201):
                # ✅ Transferência enviada com sucesso
                c3.execute("""
                    UPDATE mp2_comissao_saques
                    SET status='pago', mp_payment_id=%s, processado_em=NOW(),
                        obs='Auto via API MP bank_transfer'
                    WHERE id=%s
                """, (mp_id, saque_id))
                c3.execute("""
                    UPDATE mp2_parceiros
                    SET total_pago = total_pago + %s
                    WHERE codigo = %s
                """, (valor, parceiro_codigo))
                conn3.commit(); conn3.close()
                print(f'✅ [comissao] Transferido R${valor:.2f} → {chave_pix} ({tipo_chave}) | MP ID: {mp_id}', flush=True)
                return {'success': True, 'mp_payment_id': mp_id, 'valor': valor, 'metodo': 'bank_transfer_pix'}

            else:
                # ❌ API retornou erro — marcar como pendente para pagamento manual
                err_msg = resp_data.get('message', '') or resp_data.get('error', resp.text[:300])
                err_cause = ''
                if resp_data.get('cause'):
                    err_cause = str(resp_data['cause'])
                obs_full = f'{err_msg} | {err_cause}'.strip(' |')

                c3.execute("""
                    UPDATE mp2_comissao_saques
                    SET status='pendente_manual', obs=%s WHERE id=%s
                """, (obs_full[:500], saque_id))
                conn3.commit(); conn3.close()
                print(f'⚠️ [comissao] API MP recusou ({resp.status_code}): {obs_full[:200]}', flush=True)
                print(f'   → Saque #{saque_id} marcado como pendente_manual para admin processar', flush=True)
                return {'success': False, 'error': obs_full, 'saque_id': saque_id, 'pendente': True}

        except Exception as ex:
            # Erro de rede/timeout → manter como pendente
            try:
                conn_err = _get_conn()
                c_err = conn_err.cursor()
                c_err.execute("UPDATE mp2_comissao_saques SET status='pendente_manual', obs=%s WHERE id=%s",
                              (f'Erro conexão: {str(ex)[:200]}', saque_id))
                conn_err.commit(); conn_err.close()
            except: pass
            print(f'[comissao] Erro rede MP: {ex}', flush=True)
            return {'success': False, 'error': str(ex), 'saque_id': saque_id, 'pendente': True}

    except Exception as e:
        print(f'[mp2_parceiro] Erro pagar_comissao: {e}', flush=True)
        return {'success': False, 'error': str(e)}


# ─── RECORRÊNCIA / ASSINATURAS ────────────────────────────────────────────────

def init_recorrentes_db():
    """Cria tabelas de recorrência e adiciona colunas de preapproval se não existirem."""
    ddl = """
    CREATE TABLE IF NOT EXISTS mp2_recorrentes (
        id SERIAL PRIMARY KEY,
        nome TEXT NOT NULL,
        email TEXT,
        telefone TEXT,
        valor NUMERIC(12,2) NOT NULL,
        descricao TEXT DEFAULT 'Mensalidade',
        parceiro_ref TEXT,
        status TEXT DEFAULT 'ativo',
        tipo TEXT DEFAULT 'manual',              -- 'manual' | 'preapproval'
        dia_vencimento INTEGER DEFAULT 1,
        proximo_vencimento DATE,
        total_cobrado INTEGER DEFAULT 0,
        total_pago NUMERIC(12,2) DEFAULT 0,
        ultimo_payment_id TEXT,
        preapproval_id TEXT,                     -- ID do preapproval no MP (débito automático)
        preapproval_plan_id TEXT,                -- ID do plano no MP
        preapproval_init_point TEXT,             -- Link de autorização que o cliente acessa
        preapproval_status TEXT DEFAULT 'pending', -- 'pending'|'authorized'|'paused'|'cancelled'
        criado_em TIMESTAMP DEFAULT NOW(),
        atualizado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS mp2_recorrentes_log (
        id SERIAL PRIMARY KEY,
        recorrente_id INTEGER REFERENCES mp2_recorrentes(id),
        payment_id TEXT,
        external_ref TEXT,
        valor NUMERIC(12,2),
        status TEXT DEFAULT 'pendente',
        pix_copia_cola TEXT,
        pix_qr_base64 TEXT,
        vencimento DATE,
        pago_em TIMESTAMP,
        criado_em TIMESTAMP DEFAULT NOW()
    );

    -- Adiciona colunas novas se a tabela já existia sem elas
    DO $$ BEGIN
        BEGIN ALTER TABLE mp2_recorrentes ADD COLUMN tipo TEXT DEFAULT 'manual'; EXCEPTION WHEN duplicate_column THEN NULL; END;
        BEGIN ALTER TABLE mp2_recorrentes ADD COLUMN preapproval_id TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
        BEGIN ALTER TABLE mp2_recorrentes ADD COLUMN preapproval_plan_id TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
        BEGIN ALTER TABLE mp2_recorrentes ADD COLUMN preapproval_init_point TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
        BEGIN ALTER TABLE mp2_recorrentes ADD COLUMN preapproval_status TEXT DEFAULT 'pending'; EXCEPTION WHEN duplicate_column THEN NULL; END;
    END $$;
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close(); conn.close()
        print('✅ [recorrentes] Tabelas mp2_recorrentes criadas/verificadas!', flush=True)
    except Exception as e:
        print(f'❌ [recorrentes] Erro criar tabelas: {e}', flush=True)


# ─── PREAPPROVAL (DÉBITO AUTOMÁTICO PIX) ──────────────────────────────────────

def mp2_criar_plano_preapproval(titulo: str, valor: float, descricao: str = '') -> dict:
    """
    Cria um preapproval_plan no Mercado Pago.
    Aceita TODOS os métodos: cartão crédito/débito (qualquer banco), PIX, saldo MP.
    Retorna: { success, plan_id, init_point }
    """
    try:
        headers = {
            'Authorization': f'Bearer {MP2_ACCESS_TOKEN}',
            'Content-Type': 'application/json',
        }
        # SEM payment_methods_allowed = aceita TODOS os métodos de pagamento
        # (cartão crédito, cartão débito de qualquer banco, PIX, saldo MP)
        payload = {
            'reason': titulo,
            'auto_recurring': {
                'frequency':          1,
                'frequency_type':     'months',
                'transaction_amount': round(float(valor), 2),
                'currency_id':        'BRL',
            },
            'back_url': 'https://paynexbet.com/assinar/obrigado',
            'status':   'active',
        }
        r = requests.post(
            f'{MP2_BASE_URL}/preapproval_plan',
            headers=headers, json=payload, timeout=12
        )
        d = r.json()
        if r.status_code in (200, 201) and d.get('id'):
            plan_id    = d['id']
            init_point = d.get('init_point', '')
            # Persiste no banco para reutilizar
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO mp2_config (chave, valor) VALUES ('preapproval_plan_id', %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (plan_id,))
            cur.execute("""
                INSERT INTO mp2_config (chave, valor) VALUES ('preapproval_init_point', %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (init_point,))
            conn.commit(); cur.close(); conn.close()
            print(f'✅ [preapproval] Plano criado: {plan_id}', flush=True)
            return {'success': True, 'plan_id': plan_id, 'init_point': init_point}
        else:
            return {'success': False, 'error': d.get('message', str(d))}
    except Exception as e:
        print(f'[preapproval] Erro criar plano: {e}', flush=True)
        return {'success': False, 'error': str(e)}


def mp2_criar_preapproval(nome: str, email: str, plano_id: str,
                           valor: float, descricao: str,
                           parceiro_ref: str = '', telefone: str = '') -> dict:
    """
    Cria um preapproval (subscription) para um cliente específico.
    O cliente recebe o init_point e autoriza o débito automático via PIX no app MP.
    Retorna: { success, preapproval_id, init_point, recorrente_id }
    """
    try:
        headers = {
            'Authorization': f'Bearer {MP2_ACCESS_TOKEN}',
            'Content-Type': 'application/json',
        }

        # Se tiver plano, vincula; senão cria sem plano
        if plano_id:
            payload = {
                'preapproval_plan_id': plano_id,
                'reason':   descricao,
                'back_url': 'https://paynexbet.com/assinar/obrigado',
                'status':   'pending',
            }
            if email:
                payload['payer_email'] = email
        else:
            # SEM payment_methods_allowed = aceita TODOS (cartão, PIX, saldo MP)
            payload = {
                'reason': descricao,
                'auto_recurring': {
                    'frequency':          1,
                    'frequency_type':     'months',
                    'transaction_amount': round(float(valor), 2),
                    'currency_id':        'BRL',
                },
                'back_url': 'https://paynexbet.com/assinar/obrigado',
                'status':   'pending',
            }
            if email:
                payload['payer_email'] = email

        r = requests.post(
            f'{MP2_BASE_URL}/preapproval',
            headers=headers, json=payload, timeout=12
        )
        d = r.json()
        if r.status_code in (200, 201) and d.get('id'):
            pre_id     = d['id']
            init_point = d.get('init_point', '')

            # Salva assinante na tabela local
            rec = mp2_criar_recorrente(
                nome=nome, valor=valor, dia_vencimento=1,
                descricao=descricao, email=email, telefone=telefone,
                parceiro_ref=parceiro_ref
            )
            rec_id = rec.get('recorrente', {}).get('id')

            # Atualiza com dados do preapproval
            if rec_id:
                conn = _get_conn()
                cur  = conn.cursor()
                cur.execute("""
                    UPDATE mp2_recorrentes
                    SET tipo                  = 'preapproval',
                        preapproval_id        = %s,
                        preapproval_plan_id   = %s,
                        preapproval_init_point= %s,
                        preapproval_status    = 'pending',
                        atualizado_em         = NOW()
                    WHERE id = %s
                """, (pre_id, plano_id or '', init_point, rec_id))
                conn.commit(); cur.close(); conn.close()

            print(f'✅ [preapproval] Criado para {nome}: {pre_id}', flush=True)
            return {
                'success':        True,
                'preapproval_id': pre_id,
                'init_point':     init_point,
                'recorrente_id':  rec_id,
            }
        else:
            return {'success': False, 'error': d.get('message', str(d))}
    except Exception as e:
        print(f'[preapproval] Erro criar: {e}', flush=True)
        return {'success': False, 'error': str(e)}


def mp2_get_plano_ativo() -> dict:
    """Retorna o plano preapproval ativo do banco (cria se não existir)."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT chave, valor FROM mp2_config
            WHERE chave IN ('preapproval_plan_id','preapproval_init_point',
                            'assinar_titulo','assinar_valor','assinar_descricao')
        """)
        cfg = dict(cur.fetchall())
        cur.close(); conn.close()
        return {
            'plan_id':    cfg.get('preapproval_plan_id', ''),
            'init_point': cfg.get('preapproval_init_point', ''),
            'titulo':     cfg.get('assinar_titulo', 'Mensalidade'),
            'valor':      float(cfg.get('assinar_valor', 10)),
            'descricao':  cfg.get('assinar_descricao', 'Mensalidade'),
        }
    except Exception as e:
        return {'plan_id': '', 'init_point': '', 'titulo': 'Mensalidade', 'valor': 10.0, 'descricao': 'Mensalidade'}


def mp2_webhook_preapproval(preapproval_id: str) -> bool:
    """
    Chamado pelo webhook quando o status de um preapproval muda.
    Busca o status no MP e atualiza o banco local.
    """
    try:
        headers = {'Authorization': f'Bearer {MP2_ACCESS_TOKEN}'}
        r = requests.get(f'{MP2_BASE_URL}/preapproval/{preapproval_id}',
                         headers=headers, timeout=8)
        d = r.json()
        novo_status = d.get('status', 'pending')
        # Atualiza no banco
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE mp2_recorrentes
            SET preapproval_status = %s,
                status = CASE WHEN %s = 'authorized' THEN 'ativo'
                              WHEN %s IN ('cancelled','paused') THEN %s
                              ELSE status END,
                atualizado_em = NOW()
            WHERE preapproval_id = %s
        """, (novo_status, novo_status, novo_status, novo_status, preapproval_id))
        conn.commit(); cur.close(); conn.close()
        print(f'✅ [preapproval webhook] {preapproval_id} → {novo_status}', flush=True)
        return True
    except Exception as e:
        print(f'[preapproval webhook] Erro: {e}', flush=True)
        return False


def mp2_criar_recorrente(nome: str, valor: float, dia_vencimento: int = 1,
                          descricao: str = 'Mensalidade', email: str = '',
                          telefone: str = '', parceiro_ref: str = '') -> dict:
    """Cadastra um novo assinante para cobrança mensal recorrente."""
    from datetime import date, timedelta
    import calendar
    try:
        dia = max(1, min(28, int(dia_vencimento)))  # Limita entre 1 e 28
        hoje = date.today()
        # Calcula próximo vencimento
        try:
            prox = date(hoje.year, hoje.month, dia)
        except ValueError:
            prox = date(hoje.year, hoje.month, calendar.monthrange(hoje.year, hoje.month)[1])
        if prox <= hoje:
            # Avança pro mês seguinte
            if hoje.month == 12:
                prox = date(hoje.year + 1, 1, dia)
            else:
                try:
                    prox = date(hoje.year, hoje.month + 1, dia)
                except ValueError:
                    prox = date(hoje.year, hoje.month + 1, 28)

        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO mp2_recorrentes
                (nome, email, telefone, valor, descricao, parceiro_ref,
                 dia_vencimento, proximo_vencimento)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """, (nome, email or None, telefone or None, valor, descricao,
              parceiro_ref or None, dia, prox))
        rec = dict(cur.fetchone())
        conn.commit()
        cur.close(); conn.close()
        print(f'✅ [recorrentes] Novo assinante: {nome} R${valor:.2f}/mês dia {dia}', flush=True)
        return {'success': True, 'recorrente': _serialize_recorrente(rec)}
    except Exception as e:
        print(f'[recorrentes] Erro criar: {e}', flush=True)
        return {'success': False, 'error': str(e)}


def mp2_listar_recorrentes(status_filtro: str = '') -> list:
    """Lista assinantes recorrentes (todos ou filtrado por status)."""
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if status_filtro:
            cur.execute("""
                SELECT * FROM mp2_recorrentes WHERE status = %s ORDER BY criado_em DESC
            """, (status_filtro,))
        else:
            cur.execute("SELECT * FROM mp2_recorrentes ORDER BY criado_em DESC")
        rows = [_serialize_recorrente(dict(r)) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f'[recorrentes] Erro listar: {e}', flush=True)
        return []


def mp2_cancelar_recorrente(recorrente_id: int) -> dict:
    """Cancela uma assinatura recorrente."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE mp2_recorrentes SET status='cancelado', atualizado_em=NOW()
            WHERE id=%s
        """, (recorrente_id,))
        conn.commit()
        cur.close(); conn.close()
        return {'success': True, 'mensagem': 'Assinatura cancelada'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def mp2_pausar_recorrente(recorrente_id: int, pausar: bool = True) -> dict:
    """Pausa ou reactiva uma assinatura recorrente."""
    novo_status = 'pausado' if pausar else 'ativo'
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE mp2_recorrentes SET status=%s, atualizado_em=NOW()
            WHERE id=%s
        """, (novo_status, recorrente_id))
        conn.commit()
        cur.close(); conn.close()
        return {'success': True, 'status': novo_status}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def mp2_cobrar_recorrente(recorrente_id: int) -> dict:
    """
    Gera um novo PIX para uma assinatura recorrente.
    Cria registro no log e retorna os dados do PIX para o cliente pagar.
    """
    from datetime import date, timedelta
    import calendar
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM mp2_recorrentes WHERE id=%s", (recorrente_id,))
        rec = cur.fetchone()
        cur.close(); conn.close()
        if not rec:
            return {'success': False, 'error': 'Assinante não encontrado'}
        rec = dict(rec)
        if rec['status'] == 'cancelado':
            return {'success': False, 'error': 'Assinatura cancelada'}

        valor = float(rec['valor'])
        desc = rec.get('descricao') or 'Mensalidade'
        nome = rec.get('nome') or 'Assinante'

        # Gerar PIX
        resultado = mp2_gerar_pix(
            telegram_id=0,
            valor=valor,
            descricao=f'{desc} — {nome}'
        )
        if not resultado.get('success'):
            return {'success': False, 'error': resultado.get('error', 'Erro ao gerar PIX')}

        # Calcular próximo vencimento (mês seguinte)
        hoje = date.today()
        dia = int(rec.get('dia_vencimento') or 1)
        mes_atual = hoje.month
        ano_atual = hoje.year
        if mes_atual == 12:
            prox_mes, prox_ano = 1, ano_atual + 1
        else:
            prox_mes, prox_ano = mes_atual + 1, ano_atual
        try:
            prox_venc = date(prox_ano, prox_mes, dia)
        except ValueError:
            prox_venc = date(prox_ano, prox_mes, 28)

        # Salvar no log
        conn2 = _get_conn()
        cur2 = conn2.cursor()
        cur2.execute("""
            INSERT INTO mp2_recorrentes_log
                (recorrente_id, payment_id, external_ref, valor, status,
                 pix_copia_cola, pix_qr_base64, vencimento)
            VALUES (%s, %s, %s, %s, 'pendente', %s, %s, %s)
        """, (
            recorrente_id,
            str(resultado.get('payment_id', '')),
            resultado.get('external_ref', ''),
            valor,
            resultado.get('pix_copia_cola', ''),
            resultado.get('qr_base64', ''),
            hoje
        ))
        # Atualizar próximo vencimento e último payment_id
        cur2.execute("""
            UPDATE mp2_recorrentes
            SET proximo_vencimento = %s,
                ultimo_payment_id = %s,
                total_cobrado = total_cobrado + 1,
                atualizado_em = NOW()
            WHERE id = %s
        """, (prox_venc, str(resultado.get('payment_id', '')), recorrente_id))
        conn2.commit()
        cur2.close(); conn2.close()

        print(f'✅ [recorrentes] PIX gerado para {nome} R${valor:.2f} | venc={prox_venc}', flush=True)
        return {
            'success': True,
            'payment_id':     resultado.get('payment_id', ''),
            'external_ref':   resultado.get('external_ref', ''),
            'pix_copia_cola': resultado.get('pix_copia_cola', ''),
            'qr_base64':      resultado.get('qr_base64', ''),
            'valor':          valor,
            'proximo_vencimento': str(prox_venc),
        }
    except Exception as e:
        print(f'[recorrentes] Erro cobrar: {e}', flush=True)
        return {'success': False, 'error': str(e)}


def mp2_processar_webhook_recorrente(payment_id: str, status_mp: str) -> bool:
    """
    Chamado pelo webhook quando um pagamento recorrente é confirmado.
    Atualiza o log e o total_pago da assinatura.
    """
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM mp2_recorrentes_log WHERE payment_id=%s", (str(payment_id),))
        log = cur.fetchone()
        if not log:
            cur.close(); conn.close()
            return False
        log = dict(log)
        if status_mp == 'approved':
            cur.execute("""
                UPDATE mp2_recorrentes_log
                SET status='pago', pago_em=NOW() WHERE payment_id=%s
            """, (str(payment_id),))
            cur.execute("""
                UPDATE mp2_recorrentes
                SET total_pago = total_pago + %s, atualizado_em=NOW()
                WHERE id=%s
            """, (float(log['valor']), log['recorrente_id']))
            conn.commit()
            cur.close(); conn.close()
            print(f'✅ [recorrentes] Pagamento confirmado: {payment_id} R${log["valor"]:.2f}', flush=True)
            return True
        cur.close(); conn.close()
        return False
    except Exception as e:
        print(f'[recorrentes] Erro webhook: {e}', flush=True)
        return False


def mp2_vencimentos_hoje() -> list:
    """Retorna assinantes com vencimento hoje (para disparo automático)."""
    from datetime import date
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM mp2_recorrentes
            WHERE status = 'ativo'
              AND proximo_vencimento <= %s
            ORDER BY id
        """, (date.today(),))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f'[recorrentes] Erro vencimentos_hoje: {e}', flush=True)
        return []


def mp2_stats_recorrentes() -> dict:
    """Retorna estatísticas resumidas das recorrências."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status='ativo') as ativos,
                COUNT(*) FILTER (WHERE status='pausado') as pausados,
                COUNT(*) FILTER (WHERE status='cancelado') as cancelados,
                COALESCE(SUM(valor) FILTER (WHERE status='ativo'), 0) as mrr,
                COALESCE(SUM(total_pago), 0) as total_arrecadado
            FROM mp2_recorrentes
        """)
        row = cur.fetchone()
        cur.close(); conn.close()
        return {
            'ativos':           int(row[0] or 0),
            'pausados':         int(row[1] or 0),
            'cancelados':       int(row[2] or 0),
            'mrr':              float(row[3] or 0),  # Monthly Recurring Revenue
            'total_arrecadado': float(row[4] or 0),
        }
    except Exception as e:
        print(f'[recorrentes] Erro stats: {e}', flush=True)
        return {'ativos': 0, 'pausados': 0, 'cancelados': 0, 'mrr': 0, 'total_arrecadado': 0}


def _serialize_recorrente(r: dict) -> dict:
    """Converte campos especiais do recorrente para JSON serializável."""
    from datetime import date
    out = {}
    for k, v in r.items():
        if isinstance(v, date):
            out[k] = v.isoformat()
        else:
            try:
                out[k] = float(v) if isinstance(v, __import__('decimal').Decimal) else v
            except Exception:
                out[k] = str(v) if v is not None else None
    return out
