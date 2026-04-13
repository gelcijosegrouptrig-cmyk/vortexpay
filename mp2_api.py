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
def mp2_gerar_pix(telegram_id: int, valor: float, nome_pagador: str = 'Cliente') -> dict:
    """
    Gera cobrança Pix via Mercado Pago.
    Retorna: {success, pix_copia_cola, qr_base64, payment_id, external_ref, valor}
    """
    if not MP2_ACCESS_TOKEN:
        return {'success': False, 'error': 'Token Mercado Pago não configurado. Contate o suporte.'}

    external_ref = f"mp2_{telegram_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"

    payload = {
        "transaction_amount": float(round(valor, 2)),
        "description": f"Depósito PayPixNex - ID {telegram_id}",
        "payment_method_id": "pix",
        "external_reference": external_ref,
        "payer": {
            "email": f"user_{telegram_id}@paypixnex.com",
            "first_name": nome_pagador
        }
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

        pix_info = data.get('point_of_interaction', {}).get('transaction_data', {})
        pix_copia_cola = pix_info.get('qr_code', '')
        qr_base64 = pix_info.get('qr_code_base64', '')
        payment_id = str(data.get('id', ''))

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

        print(f'✅ [mp2_gerar_pix] Pix gerado: {payment_id} R${valor} — {external_ref}', flush=True)
        return {
            'success': True,
            'payment_id': payment_id,
            'external_ref': external_ref,
            'pix_copia_cola': pix_copia_cola,
            'qr_base64': qr_base64,
            'valor': valor
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
            'data_aprovacao': data.get('date_approved', '')
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

        cur.close(); conn.close()
        return {
            'total_usuarios': total_usuarios,
            'depositos_confirmados': int(dep['total']),
            'total_depositado': float(dep['soma']),
            'saques_pendentes': int(saques_pend['total']),
            'valor_saques_pendentes': float(saques_pend['soma']),
            'saques_aprovados': int(saques_aprov['total']),
            'valor_saques_aprovados': float(saques_aprov['soma'])
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
def mp2_get_config(chave: str, default: str = '') -> str:
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute('SELECT valor FROM mp2_config WHERE chave = %s', (chave,))
        row = cur.fetchone()
        cur.close(); conn.close()
        return row[0] if row else default
    except:
        return default
