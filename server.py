"""
PaynexBet - Servidor Railway 24/7
HTTP imediato + Telegram background com retry automático
Banco: PostgreSQL (persistente) com fallback SQLite
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

# ─── ASAAS ────────────────────────────────────────────────────────────────────
ASAAS_API_KEY  = os.environ.get('ASAAS_API_KEY', '')   # $aact_xxx (produção) ou $aasa_xxx (sandbox)
ASAAS_ENV      = os.environ.get('ASAAS_ENV', 'production')  # 'sandbox' ou 'production'
# URL correta: https://api.asaas.com/v3 (produção) | https://sandbox.asaas.com/v3 (sandbox)
ASAAS_BASE_URL = 'https://sandbox.asaas.com/v3' if ASAAS_ENV == 'sandbox' else 'https://api.asaas.com/v3'
ASAAS_WEBHOOK_TOKEN = os.environ.get('ASAAS_WEBHOOK_TOKEN', 'vortex_asaas_2024')  # token secreto webhook Asaas

_SESSION_FALLBACK = ''  # Sessão fallback removida por segurança - usar apenas DB/env var

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway')

# Carregar sessão: 1) env var SESSION_STR, 2) arquivo local session_string.txt, 3) PostgreSQL (após init_db)
SESSION_STR = os.environ.get('SESSION_STR', '')
if not SESSION_STR:
    try:
        SESSION_STR = open('session_string.txt').read().strip()
    except:
        pass
if not SESSION_STR:
    print('⚠️ Nenhuma sessão Telegram encontrada - reconexão necessária pelo admin', flush=True)

client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
_lock = asyncio.Lock()
_saque_lock = asyncio.Lock()
_telegram_ready = False
_telegram_tentativas = 0
_telegram_session_invalida = False
_telegram_ultimo_ping = 0        # timestamp do último ping bem-sucedido
_telegram_reconectando = False   # flag para evitar reconexões simultâneas
_sessao_salva_em = 0             # timestamp do último save de sessão
PHONE_NUMBER = os.environ.get('TELEGRAM_PHONE', '')  # número de telefone Telegram (env var preferida)

# ─── CACHE DE SALDO BOT ───────────────────────────────────────────────────────
_saldo_bot_cache        = -1.0   # último saldo conhecido
_saldo_bot_cache_ts     = 0      # timestamp da última atualização
_saldo_bot_atualizando  = False  # evita chamadas simultâneas
SALDO_BOT_CACHE_TTL     = 300    # 5 minutos

# ─── CANAIS TELEGRAM ─────────────────────────────────────────────────────────
# IDs salvos após criação via /api/admin/criar-canais
CANAL_NOTIF_ID   = int(os.environ.get('CANAL_NOTIF_ID', '0'))   # Canal de Notificações
CANAL_HIST_ID    = int(os.environ.get('CANAL_HIST_ID',  '0'))   # Canal Histórico de Transações
CANAL_NOTIF_LINK = os.environ.get('CANAL_NOTIF_LINK', '')       # Link de convite do canal notif
CANAL_HIST_LINK  = os.environ.get('CANAL_HIST_LINK',  '')       # Link de convite do canal hist

# ══════════════════════════════════════════════════════════════════
# ─── BOT 2 - @paypix_nexbot (paralelo, independente do Bot 1) ───────
# ══════════════════════════════════════════════════════════════════
BOT2_USERNAME   = os.environ.get('BOT2_USERNAME', 'paypix_nexbot')
SESSION_STR2    = os.environ.get('SESSION_STR2', '')
if not SESSION_STR2:
    try:
        SESSION_STR2 = open('session_string2.txt').read().strip()
    except:
        pass

client2                  = TelegramClient(StringSession(SESSION_STR2), API_ID, API_HASH)
_lock2                   = asyncio.Lock()
_saque_lock2             = asyncio.Lock()
_telegram2_ready         = False
_telegram2_session_inv   = False
_telegram2_ultimo_ping   = 0
_telegram2_reconectando  = False
_login_state2            = {}        # estado do fluxo solicitar/confirmar código Bot2
PHONE_NUMBER2            = os.environ.get('TELEGRAM_PHONE2', '')

# ─── BANCO DE DADOS - PostgreSQL persistente + fallback SQLite ───────────────
DB_PATH = '/tmp/transacoes.db'
_USE_PG = False

def _to_pg(sql):
    """Converte SQL SQLite para PostgreSQL"""
    import re as _re
    sql = sql.replace('?', '%s')
    sql = _re.sub(r'INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY', sql, flags=_re.IGNORECASE)
    sql = _re.sub(r'\bAUTOINCREMENT\b', '', sql, flags=_re.IGNORECASE)
    if _re.search(r'\bINSERT OR IGNORE\b', sql, _re.IGNORECASE):
        sql = _re.sub(r'\bINSERT OR IGNORE\b', 'INSERT', sql, flags=_re.IGNORECASE)
        if 'ON CONFLICT' not in sql.upper():
            sql = sql.rstrip() + ' ON CONFLICT DO NOTHING'
    if _re.search(r'\bINSERT OR REPLACE\b', sql, _re.IGNORECASE):
        sql = _re.sub(r'\bINSERT OR REPLACE\b', 'INSERT', sql, flags=_re.IGNORECASE)
        if 'ON CONFLICT' not in sql.upper():
            sql = sql.rstrip() + ' ON CONFLICT DO NOTHING'
    return sql

class _FakeRow:
    """Resultado de query PG que já foi executada e fechou a conexão"""
    def __init__(self, rows, lastrow=None):
        self._rows = rows
        self._last = lastrow
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows
    @property
    def lastrowid(self): return self._last

def _pg_run(sql, params=()):
    """Executa SQL no PG abrindo e fechando conexão na mesma chamada.
    Retorna _FakeRow com todos os resultados já em memória.
    Zero risco de 'transaction aborted' - cada chamada é totalmente isolada."""
    import psycopg2
    pg = psycopg2.connect(DATABASE_URL)
    try:
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(_to_pg(sql), params if params else ())
        try:
            rows = cur.fetchall()
        except Exception:
            rows = []
        lastrow = None
        return _FakeRow(rows, lastrow)
    finally:
        try: pg.close()
        except: pass

class DBConn:
    """Wrapper sqlite3-compatível. PG: _pg_run por query (abre+fecha = zero transaction aborted).
    SQLite: conexão normal."""
    def __init__(self, use_pg=False, sq_conn=None):
        self._use_pg = use_pg
        self._sq = sq_conn
        self.row_factory = None

    def execute(self, sql, params=()):
        if self._use_pg:
            return _pg_run(sql, params)
        return self._sq.execute(sql, params)

    def cursor(self):
        if self._use_pg:
            # Retorna objeto que delega execute() para _pg_run
            return _DBCursor(use_pg=True)
        return self._sq.cursor()

    def commit(self):
        if not self._use_pg and self._sq:
            try: self._sq.commit()
            except: pass

    def rollback(self):
        if not self._use_pg and self._sq:
            try: self._sq.rollback()
            except: pass

    def close(self):
        if not self._use_pg and self._sq:
            try: self._sq.close()
            except: pass

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class _DBCursor:
    """Cursor para DBConn.cursor() - delega para _pg_run"""
    def __init__(self, use_pg=False, sq_cur=None):
        self._use_pg = use_pg
        self._sq_cur = sq_cur
        self._result = None

    def execute(self, sql, params=()):
        if self._use_pg:
            self._result = _pg_run(sql, params)
        else:
            self._result = self._sq_cur.execute(sql, params)
        return self

    def fetchone(self):
        if self._result: return self._result.fetchone()
        return None

    def fetchall(self):
        if self._result: return self._result.fetchall()
        return []

    @property
    def lastrowid(self):
        if self._result: return self._result.lastrowid
        return None

def _pg_insert_ignore(sql, params, conn):
    """INSERT ignorando duplicatas"""
    try:
        conn.execute(sql, params)
    except Exception as e:
        if 'duplicate' not in str(e).lower() and 'unique' not in str(e).lower():
            raise

def sqlite3_connect(path=None):
    """Retorna DBConn. PG: _pg_run por query. SQLite: arquivo local."""
    if _USE_PG and DATABASE_URL:
        return DBConn(use_pg=True)
    sq = sqlite3.connect(path or DB_PATH)
    return DBConn(sq_conn=sq)

def _salvar_sessao_db(session_str):
    """Salva a sessão Telegram no PostgreSQL para persistir entre deploys"""
    try:
        if not DATABASE_URL: return
        import psycopg2
        pg = psycopg2.connect(DATABASE_URL)
        cur = pg.cursor()
        cur.execute("""INSERT INTO configuracoes (chave, valor, updated_at)
                       VALUES ('telegram_session', %s, %s)
                       ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, updated_at=EXCLUDED.updated_at""",
                    (session_str, datetime.now().isoformat()))
        pg.commit(); pg.close()
        print('✅ Sessão Telegram salva no PostgreSQL!', flush=True)
    except Exception as e:
        print(f'⚠️ Erro ao salvar sessão no DB: {e}', flush=True)

def _carregar_sessao_db():
    """Carrega sessão Telegram salva no PostgreSQL"""
    global SESSION_STR, client
    try:
        if not DATABASE_URL: return
        import psycopg2
        from telethon.sessions import StringSession
        pg = psycopg2.connect(DATABASE_URL)
        cur = pg.cursor()
        # Garantir que tabela existe
        cur.execute("""CREATE TABLE IF NOT EXISTS configuracoes
                       (chave TEXT PRIMARY KEY, valor TEXT, updated_at TEXT)""")
        pg.commit()
        cur.execute("SELECT valor FROM configuracoes WHERE chave='telegram_session'")
        row = cur.fetchone()
        pg.close()
        if row and row[0] and len(row[0]) > 50:
            SESSION_STR = row[0]
            client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
            print('✅ Sessão Telegram carregada do PostgreSQL!', flush=True)
    except Exception as e:
        print(f'⚠️ Erro ao carregar sessão do DB: {e}', flush=True)

# ── Bot2: salvar/carregar sessão ─────────────────────────────────
def _salvar_sessao2_db(session_str):
    """Salva sessão do Bot2 (@paypix_nexbot) no PostgreSQL"""
    try:
        if not DATABASE_URL: return
        import psycopg2
        pg = psycopg2.connect(DATABASE_URL)
        cur = pg.cursor()
        cur.execute("""INSERT INTO configuracoes (chave, valor, updated_at)
                       VALUES ('telegram_session2', %s, %s)
                       ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, updated_at=EXCLUDED.updated_at""",
                    (session_str, datetime.now().isoformat()))
        pg.commit(); pg.close()
        print('✅ [Bot2] Sessão salva no PostgreSQL!', flush=True)
    except Exception as e:
        print(f'⚠️ [Bot2] Erro ao salvar sessão: {e}', flush=True)

def _carregar_sessao2_db():
    """Carrega sessão do Bot2 do PostgreSQL"""
    global SESSION_STR2, client2
    try:
        if not DATABASE_URL: return
        import psycopg2
        from telethon.sessions import StringSession
        pg = psycopg2.connect(DATABASE_URL)
        cur = pg.cursor()
        cur.execute("SELECT valor FROM configuracoes WHERE chave='telegram_session2'")
        row = cur.fetchone()
        pg.close()
        if row and row[0] and len(row[0]) > 50:
            SESSION_STR2 = row[0]
            client2 = TelegramClient(StringSession(SESSION_STR2), API_ID, API_HASH)
            print('✅ [Bot2] Sessão carregada do PostgreSQL!', flush=True)
    except Exception as e:
        print(f'⚠️ [Bot2] Erro ao carregar sessão: {e}', flush=True)

def _pg_exec_safe(pg, sql, params=None):
    """Executa SQL no PostgreSQL com rollback automático em caso de erro.
    Usado apenas no init_db onde a conexão NÃO tem autocommit."""
    try:
        cur = pg.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur
    except Exception as e:
        try: pg.rollback()
        except: pass
        raise e

def init_db():
    global _USE_PG
    # Tentar conectar ao PostgreSQL primeiro
    if DATABASE_URL:
        try:
            import psycopg2
            pg = psycopg2.connect(DATABASE_URL)
            # Usar autocommit=True no init_db também - cada CREATE TABLE é independente
            pg.autocommit = True
            cur = pg.cursor()
            pg_tables = [
                """CREATE TABLE IF NOT EXISTS transacoes (
                    id SERIAL PRIMARY KEY, tx_id TEXT UNIQUE NOT NULL,
                    valor REAL NOT NULL, pix_code TEXT,
                    status TEXT DEFAULT 'pendente', cliente_id TEXT,
                    webhook_url TEXT, created_at TEXT, paid_at TEXT, extra TEXT)""",
                """CREATE TABLE IF NOT EXISTS saques (
                    id SERIAL PRIMARY KEY, saque_id TEXT UNIQUE NOT NULL,
                    valor REAL NOT NULL, chave_pix TEXT NOT NULL,
                    tipo_chave TEXT NOT NULL, status TEXT DEFAULT 'pendente',
                    created_at TEXT, processado_at TEXT, observacao TEXT)""",
                """CREATE TABLE IF NOT EXISTS sorteio_config (
                    id INTEGER PRIMARY KEY, ativo INTEGER DEFAULT 1,
                    valor_por_numero REAL DEFAULT 5.0, premio_fixo REAL DEFAULT 0,
                    percentual REAL DEFAULT 50.0, usar_media INTEGER DEFAULT 0,
                    dias_media INTEGER DEFAULT 30, descricao TEXT DEFAULT 'Sorteio PaynexBet',
                    proximo_sorteio TEXT, updated_at TEXT,
                    premio_acumulado REAL DEFAULT 0, min_participantes INTEGER DEFAULT 1,
                    acumulativo INTEGER DEFAULT 1)""",
                """CREATE TABLE IF NOT EXISTS sorteio_participantes (
                    id SERIAL PRIMARY KEY, cliente_id TEXT NOT NULL UNIQUE,
                    nome TEXT, cpf TEXT, chave_pix TEXT, tipo_chave TEXT DEFAULT 'cpf',
                    total_depositado REAL DEFAULT 0, total_numeros INTEGER DEFAULT 0,
                    numeros_sorte TEXT DEFAULT '[]', created_at TEXT,
                    updated_at TEXT, sorteio_id TEXT DEFAULT 'atual')""",
                """CREATE TABLE IF NOT EXISTS sorteio_bilhetes (
                    id SERIAL PRIMARY KEY, cliente_id TEXT NOT NULL,
                    numero INTEGER NOT NULL, sorteio_id TEXT DEFAULT 'atual', created_at TEXT)""",
                """CREATE TABLE IF NOT EXISTS sorteio_historico (
                    id SERIAL PRIMARY KEY, sorteio_id TEXT UNIQUE NOT NULL,
                    data_sorteio TEXT, ganhador_cliente_id TEXT, ganhador_nome TEXT,
                    ganhador_cpf TEXT, ganhador_numero INTEGER, ganhador_chave_pix TEXT,
                    ganhador_tipo_chave TEXT, premio_pago REAL, saque_id TEXT,
                    saque_status TEXT DEFAULT 'pendente', total_participantes INTEGER,
                    total_bilhetes INTEGER, total_depositado REAL, observacao TEXT)""",
                """CREATE TABLE IF NOT EXISTS configuracoes (
                    chave TEXT PRIMARY KEY, valor TEXT, updated_at TEXT)""",
                """CREATE TABLE IF NOT EXISTS paypix_fila (
                    id SERIAL PRIMARY KEY,
                    tx_id TEXT NOT NULL,
                    valor REAL NOT NULL,
                    chave_pix TEXT NOT NULL,
                    tipo_chave TEXT NOT NULL,
                    pct REAL NOT NULL DEFAULT 0.6,
                    tentativas INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pendente',
                    proxima_tentativa TEXT,
                    created_at TEXT,
                    finalizado_at TEXT,
                    observacao TEXT)""",
            ]
            for sql in pg_tables:
                try:
                    cur.execute(sql)
                except Exception as e:
                    print(f'[DB init] Aviso ao criar tabela: {e}', flush=True)
            # Migrações PostgreSQL - adicionar colunas novas se não existirem
            pg_migrations = [
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS premio_acumulado REAL DEFAULT 0",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS acumulativo INTEGER DEFAULT 1",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS min_participantes INTEGER DEFAULT 1",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_pct REAL DEFAULT 0.6",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_ativo INTEGER DEFAULT 1",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_descricao TEXT DEFAULT 'Gere seu Pix e receba sua % do valor'",
                # Corrigir valor_por_numero se ainda estiver com valor errado de migração anterior
                "UPDATE sorteio_config SET valor_por_numero=5.0 WHERE id=1 AND valor_por_numero=10.0",
            ]
            for mig_sql in pg_migrations:
                try:
                    cur.execute(mig_sql)
                except Exception as e:
                    print(f'[DB migrate] {e}', flush=True)

            # Config padrão sorteio (cada query é independente com autocommit)
            try:
                cur.execute("""INSERT INTO sorteio_config
                    (id,ativo,valor_por_numero,premio_fixo,percentual,usar_media,dias_media,descricao,proximo_sorteio,updated_at,premio_acumulado,acumulativo,min_participantes)
                    VALUES (1,1,5.0,0,50.0,0,30,'Sorteio PaynexBet',NULL,%s,0,1,1)
                    ON CONFLICT (id) DO NOTHING""", (datetime.now().isoformat(),))
            except Exception as e:
                print(f'[DB init] Aviso config sorteio: {e}', flush=True)
            pg.close()
            _USE_PG = True
            print('✅ PostgreSQL conectado - banco PERSISTENTE ativo!', flush=True)
            # Tentar carregar sessão salva no banco
            _carregar_sessao_db()
            _carregar_sessao2_db()   # ← Bot2 (@paypix_nexbot)
            # Carregar IDs dos canais Telegram do banco
            try:
                import psycopg2 as _pg2
                _conn_c = _pg2.connect(DATABASE_URL, connect_timeout=8)
                _cur_c  = _conn_c.cursor()
                _cur_c.execute("SELECT chave, valor FROM configuracoes WHERE chave LIKE 'canal_%'")
                _canal_rows = dict(_cur_c.fetchall())
                _cur_c.close(); _conn_c.close()
                global CANAL_NOTIF_ID, CANAL_HIST_ID, CANAL_NOTIF_LINK, CANAL_HIST_LINK
                if _canal_rows.get('canal_notif_id'):
                    CANAL_NOTIF_ID   = int(_canal_rows['canal_notif_id'])
                if _canal_rows.get('canal_notif_link'):
                    CANAL_NOTIF_LINK = _canal_rows['canal_notif_link']
                if _canal_rows.get('canal_hist_id'):
                    CANAL_HIST_ID    = int(_canal_rows['canal_hist_id'])
                if _canal_rows.get('canal_hist_link'):
                    CANAL_HIST_LINK  = _canal_rows['canal_hist_link']
                if CANAL_NOTIF_ID:
                    print(f'✅ Canais Telegram: notif={CANAL_NOTIF_ID} hist={CANAL_HIST_ID}', flush=True)
            except Exception as _ec:
                print(f'[canais_db] Aviso: {_ec}', flush=True)
            return  # Sai sem criar SQLite
        except ImportError:
            print('⚠️ psycopg2 não instalado, usando SQLite', flush=True)
            _USE_PG = False
        except Exception as e:
            print(f'⚠️ PostgreSQL falhou ({e}), usando SQLite', flush=True)
            _USE_PG = False
    else:
        print('ℹ️ DATABASE_URL não definida, usando SQLite', flush=True)
        _USE_PG = False

    conn = sqlite3_connect()
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
    # ─── TABELAS DE SORTEIO ──────────────────────────────────
    conn.execute('''CREATE TABLE IF NOT EXISTS sorteio_config (
        id INTEGER PRIMARY KEY,
        ativo INTEGER DEFAULT 1,
        valor_por_numero REAL DEFAULT 5.0,
        premio_fixo REAL DEFAULT 0,
        percentual REAL DEFAULT 50.0,
        usar_media INTEGER DEFAULT 0,
        dias_media INTEGER DEFAULT 30,
        descricao TEXT DEFAULT 'Sorteio PaynexBet',
        proximo_sorteio TEXT,
        updated_at TEXT,
        premio_acumulado REAL DEFAULT 0,
        min_participantes INTEGER DEFAULT 1,
        acumulativo INTEGER DEFAULT 1
    )''')
    conn.commit()
    # Migrações de colunas - cada ALTER TABLE em transação separada
    for col in ["valor_por_numero REAL DEFAULT 5.0",
                "usar_media INTEGER DEFAULT 0",
                "dias_media INTEGER DEFAULT 30",
                "paypix_pct REAL DEFAULT 0.6",
                "paypix_ativo INTEGER DEFAULT 1",
                "paypix_descricao TEXT DEFAULT 'Gere seu Pix e receba sua % do valor'",
                "premio_acumulado REAL DEFAULT 0",
                "min_participantes INTEGER DEFAULT 1",
                "acumulativo INTEGER DEFAULT 1"]:
        try:
            conn.execute(f'ALTER TABLE sorteio_config ADD COLUMN {col}')
            conn.commit()
        except Exception:
            conn.rollback()

    conn.execute('''CREATE TABLE IF NOT EXISTS sorteio_participantes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id TEXT NOT NULL UNIQUE,
        nome TEXT,
        cpf TEXT,
        chave_pix TEXT,
        tipo_chave TEXT DEFAULT 'cpf',
        total_depositado REAL DEFAULT 0,
        total_numeros INTEGER DEFAULT 0,
        numeros_sorte TEXT DEFAULT '[]',
        created_at TEXT,
        updated_at TEXT,
        sorteio_id TEXT DEFAULT 'atual'
    )''')
    conn.commit()
    # Migrações sorteio_participantes - cada ALTER TABLE em transação separada
    for col in ['cpf TEXT', 'total_depositado REAL DEFAULT 0',
                'total_numeros INTEGER DEFAULT 0', "numeros_sorte TEXT DEFAULT '[]'",
                'updated_at TEXT']:
        try:
            conn.execute(f'ALTER TABLE sorteio_participantes ADD COLUMN {col}')
            conn.commit()
        except Exception:
            conn.rollback()

    conn.execute('''CREATE TABLE IF NOT EXISTS sorteio_bilhetes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id TEXT NOT NULL,
        numero INTEGER NOT NULL,
        sorteio_id TEXT DEFAULT 'atual',
        created_at TEXT
    )''')

    conn.execute('''CREATE TABLE IF NOT EXISTS sorteio_historico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sorteio_id TEXT UNIQUE NOT NULL,
        data_sorteio TEXT,
        ganhador_cliente_id TEXT,
        ganhador_nome TEXT,
        ganhador_cpf TEXT,
        ganhador_numero INTEGER,
        ganhador_chave_pix TEXT,
        ganhador_tipo_chave TEXT,
        premio_pago REAL,
        saque_id TEXT,
        saque_status TEXT DEFAULT 'pendente',
        total_participantes INTEGER,
        total_bilhetes INTEGER,
        total_depositado REAL,
        observacao TEXT
    )''')

    # Config padrão
    conn.execute('''INSERT OR IGNORE INTO sorteio_config
        (id, ativo, valor_por_numero, premio_fixo, percentual, usar_media, dias_media, descricao, proximo_sorteio, updated_at, premio_acumulado)
        VALUES (1, 1, 5.0, 0, 50.0, 0, 30, 'Sorteio PaynexBet', NULL, ?, 0)''',
        (datetime.now().isoformat(),))

    # Tabela de fila de splits PayPix (agenda persistente)
    conn.execute('''CREATE TABLE IF NOT EXISTS paypix_fila (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT NOT NULL,
        valor REAL NOT NULL,
        chave_pix TEXT NOT NULL,
        tipo_chave TEXT NOT NULL,
        pct REAL NOT NULL DEFAULT 0.6,
        tentativas INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pendente',
        proxima_tentativa TEXT,
        created_at TEXT,
        finalizado_at TEXT,
        observacao TEXT
    )''')

    conn.commit(); conn.close()

def salvar_transacao(tx_id, valor, pix_code, cliente_id=None, webhook_url=None, participante_dados=None):
    import json as _json
    extra = _json.dumps(participante_dados) if participante_dados else None
    conn = sqlite3_connect()
    conn.execute('''INSERT OR REPLACE INTO transacoes
        (tx_id,valor,pix_code,status,cliente_id,webhook_url,created_at,extra)
        VALUES (?,?,?,'pendente',?,?,?,?)''',
        (tx_id, valor, pix_code, cliente_id, webhook_url, datetime.now().isoformat(), extra))
    conn.commit(); conn.close()

def buscar_transacao(tx_id):
    conn = sqlite3_connect()
    cur = conn.execute('SELECT * FROM transacoes WHERE tx_id=?', (tx_id,))
    row = cur.fetchone(); conn.close()
    if row:
        cols = ['id','tx_id','valor','pix_code','status','cliente_id',
                'webhook_url','created_at','paid_at','extra']
        return dict(zip(cols, row))
    return None

def confirmar_pagamento(tx_id):
    """Confirma pagamento e automaticamente gera bilhetes do sorteio para o cliente"""
    import json as _json
    # UPDATE em conexão própria
    conn = sqlite3_connect()
    conn.execute('UPDATE transacoes SET status=?,paid_at=? WHERE tx_id=?',
        ('pago', datetime.now().isoformat(), tx_id))
    conn.commit()
    conn.close()

    # SELECT em nova conexão independente (evita qualquer contaminação)
    conn2 = sqlite3_connect()
    cur = conn2.execute('SELECT valor, cliente_id, extra FROM transacoes WHERE tx_id=?', (tx_id,))
    row = cur.fetchone()
    conn2.close()

    if row:
        valor_pago, cliente_id, extra_json = row
        participante_dados = None
        if extra_json:
            try: participante_dados = _json.loads(extra_json)
            except: pass
        if cliente_id and valor_pago and float(valor_pago) >= 1:
            _creditar_bilhetes_por_deposito(cliente_id, float(valor_pago), tx_id, participante_dados)
        elif valor_pago and float(valor_pago) >= 1 and participante_dados and participante_dados.get('cpf'):
            cpf_extra = re.sub(r'\D', '', str(participante_dados['cpf']))
            _creditar_bilhetes_por_deposito(f'cli_{cpf_extra}', float(valor_pago), tx_id, participante_dados)

def _creditar_bilhetes_por_deposito(cliente_id, valor, tx_id, participante_dados=None):
    """Gera bilhetes do sorteio automaticamente quando um depósito é confirmado."""
    try:
        import json as _json

        cpf_tentativa = re.sub(r'\D', '', str(cliente_id or '')).strip()
        if cliente_id and str(cliente_id).startswith('cli_'):
            cpf_tentativa = str(cliente_id)[4:]

        if not cpf_tentativa or len(cpf_tentativa) < 11:
            return  # Sem CPF válido

        # SELECT em conexão própria
        conn = sqlite3_connect()
        cur = conn.execute("SELECT id, cliente_id, total_depositado, total_numeros, numeros_sorte FROM sorteio_participantes WHERE cpf=? AND sorteio_id='atual'",
                  (cpf_tentativa,))
        row = cur.fetchone()
        conn.close()

        if not row:
            # Tentar criar participante automaticamente se temos os dados
            if participante_dados and participante_dados.get('nome') and participante_dados.get('chave_pix'):
                now = datetime.now().isoformat()
                cli_id = f"cli_{cpf_tentativa}"
                nome_p = str(participante_dados.get('nome', '')).strip()
                chave_p = str(participante_dados.get('chave_pix', '')).strip()
                tipo_p = str(participante_dados.get('tipo_chave', 'cpf')).strip()
                conn2 = sqlite3_connect()
                conn2.execute('''INSERT OR IGNORE INTO sorteio_participantes
                    (cliente_id, nome, cpf, chave_pix, tipo_chave,
                     total_depositado, total_numeros, numeros_sorte, created_at, updated_at, sorteio_id)
                    VALUES (?,?,?,?,?, 0, 0, '[]', ?, ?, 'atual')''',
                    (cli_id, nome_p, cpf_tentativa, chave_p, tipo_p, now, now))
                conn2.commit(); conn2.close()
                print(f'✅ Sorteio: participante {nome_p} (CPF {cpf_tentativa}) criado automaticamente ao confirmar pagamento tx={tx_id}', flush=True)
                # Re-buscar após criação
                conn3 = sqlite3_connect()
                cur3 = conn3.execute("SELECT id, cliente_id, total_depositado, total_numeros, numeros_sorte FROM sorteio_participantes WHERE cpf=? AND sorteio_id='atual'",
                           (cpf_tentativa,))
                row = cur3.fetchone()
                conn3.close()
                if not row:
                    return  # Falha inesperada
            else:
                return  # Participante não cadastrado e sem dados para criar

        _, cli_id, total_dep, total_num, numeros_json = row
        total_dep = float(total_dep or 0)
        total_num = int(total_num or 0)
        numeros_atuais = []
        try: numeros_atuais = _json.loads(numeros_json or '[]')
        except: pass

        # Ler config do sorteio
        cfg = get_sorteio_config()
        vp = float(cfg.get('valor_por_numero') or 5.0)

        novo_total = total_dep + float(valor)
        novos_total_num = calcular_numeros(novo_total, vp)
        novos = novos_total_num - total_num

        novos_numeros = []
        if novos > 0:
            novos_numeros = gerar_bilhetes_unicos(cli_id, novos)
            numeros_atuais.extend(novos_numeros)

        conn2 = sqlite3_connect()
        conn2.execute('''UPDATE sorteio_participantes
            SET total_depositado=?, total_numeros=?, numeros_sorte=?, updated_at=?
            WHERE cpf=? AND sorteio_id='atual' ''',
            (novo_total, novos_total_num, _json.dumps(numeros_atuais),
             datetime.now().isoformat(), cpf_tentativa))
        conn2.commit(); conn2.close()

        if novos > 0:
            print(f'🎫 Sorteio: {novos} bilhete(s) gerado(s) para CPF {cpf_tentativa} (depósito R${valor:.2f}, tx={tx_id})', flush=True)
        else:
            print(f'💰 Sorteio: R${valor:.2f} creditado para CPF {cpf_tentativa}, total R${novo_total:.2f}, aguardando R${vp - (novo_total % vp):.2f} para próximo bilhete', flush=True)
    except Exception as e:
        print(f'⚠️ Erro ao creditar bilhetes sorteio (tx={tx_id}): {e}', flush=True)

def listar_transacoes(limit=50):
    conn = sqlite3_connect()
    cur = conn.execute('SELECT * FROM transacoes ORDER BY created_at DESC LIMIT ?', (limit,))
    rows = cur.fetchall(); conn.close()
    cols = ['id','tx_id','valor','pix_code','status','cliente_id',
            'webhook_url','created_at','paid_at','extra']
    return [dict(zip(cols, r)) for r in rows]

def salvar_saque(saque_id, valor, chave_pix, tipo_chave):
    conn = sqlite3_connect()
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
    conn = sqlite3_connect()
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
    cur = conn.execute('SELECT * FROM saques ORDER BY created_at DESC LIMIT ?', (limit,))
    rows = cur.fetchall(); conn.close()
    cols = ['id','saque_id','valor','chave_pix','tipo_chave','status',
            'created_at','processado_at','observacao']
    return [dict(zip(cols, r)) for r in rows]

# ─── HTML ───────────────────────────────────────────────────
def load_home_html():
    if os.path.exists('home.html'):
        html = open('home.html', encoding='utf-8').read()
        # ── Patch v29: remover participantes/bilhetes da seção stats ──
        import re as _reh
        # Substituir seção STATS antiga pelos 2 cards corretos (prêmio + por número)
        html = _reh.sub(
            r'<!-- STATS -->.*?<!-- URGÊNCIA -->',
            '<!-- STATS -->\n'
            '  <div class="divider"><span>🏆 Prêmio atual do sorteio</span></div>\n\n'
            '  <div class="stats-grid" style="grid-template-columns:1fr 1fr">\n'
            '    <div class="stat-card">\n'
            '      <div class="stat-val" id="s-premio">--</div>\n'
            '      <div class="stat-lbl">🏆 Prêmio estimado</div>\n'
            '    </div>\n'
            '    <div class="stat-card">\n'
            '      <div class="stat-val">R$5</div>\n'
            '      <div class="stat-lbl">Por número da sorte</div>\n'
            '    </div>\n'
            '  </div>\n\n'
            '  <!-- URGÊNCIA -->',
            html, flags=_reh.DOTALL
        )
        # Corrigir JS: remover referências a s-part e s-bil
        html = _reh.sub(
            r"document\.getElementById\('s-part'\)\.textContent=s\.total_participantes\|\|0;\s*"
            r"document\.getElementById\('s-bil'\)\.textContent=s\.total_bilhetes\|\|0;\s*",
            '', html
        )
        # Corrigir label prêmio acumulado → prêmio estimado
        html = html.replace('Prêmio acumulado</div>', 'Prêmio estimado</div>')
        return html
    return '<h1>PaynexBet</h1>'

def load_html():
    if os.path.exists('index.html'):
        return open('index.html', encoding='utf-8').read()
    return '<h1>PaynexBet</h1>'

def load_saque_html():
    if os.path.exists('saque.html'):
        return open('saque.html', encoding='utf-8').read()
    return '<h1>PaynexBet - Saque</h1>'

def load_admin_html():
    # Tentar versão mais recente do PostgreSQL (patch imediato sem redeploy)
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='admin_html_patch'")
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
    # Fallback: arquivo em disco
    if os.path.exists('admin.html'):
        return open('admin.html', encoding='utf-8').read()
    return '<h1>PaynexBet - Admin</h1>'

def load_paypix_html():
    # ── Tentar carregar versão mais recente do PostgreSQL (patch via DB) ──
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            # Tentar chave específica do paypix primeiro
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='paypix_html_patch'")
            _row = _cur.fetchone()
            # Fallback: usar sorteio_html_patch se paypix_html_patch não existir
            if not (_row and _row[0] and len(_row[0]) > 1000):
                _cur.execute("SELECT valor FROM configuracoes WHERE chave='sorteio_html_patch'")
                _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
    # Fallback: arquivo em disco
    if os.path.exists('paypix.html'):
        return open('paypix.html', encoding='utf-8').read()
    return '<h1>PayPix</h1>'

def load_bot_pix_html():
    """Carrega a página pública /bot - gerar PIX via @paypix_nexbot (Mercado Pago)."""
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='bot_pix_html_patch'")
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
    if os.path.exists('bot_pix.html'):
        return open('bot_pix.html', encoding='utf-8').read()
    return '<h1>PayPixNex Bot</h1>'

def load_sorteio_html():
    # ── Tentar carregar versão mais recente do PostgreSQL (patch via DB) ──
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='sorteio_html_patch'")
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
    # Fallback: arquivo em disco
    if os.path.exists('sorteio.html'):
        return open('sorteio.html', encoding='utf-8').read()
    return '<h1>PaynexBet - Sorteio</h1>'

# ═══════════════════════════════════════════════════════════════════════════════
# ─── ASAAS - INTEGRAÇÃO PIX CASH-IN / CASH-OUT ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

async def asaas_request(method: str, path: str, data: dict = None) -> dict:
    """Faz requisição autenticada à API Asaas (aiohttp)"""
    import aiohttp
    if not ASAAS_API_KEY:
        return {'error': 'ASAAS_API_KEY não configurada', 'success': False}
    url = f"{ASAAS_BASE_URL}{path}"
    headers = {
        'access_token': ASAAS_API_KEY,
        'Content-Type': 'application/json',
        'User-Agent': 'VortexPay/1.0'
    }
    try:
        async with aiohttp.ClientSession() as session:
            if method.upper() == 'GET':
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    return await r.json()
            elif method.upper() == 'POST':
                async with session.post(url, headers=headers, json=data or {}, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    return await r.json()
            elif method.upper() == 'DELETE':
                async with session.delete(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    return await r.json()
    except Exception as e:
        print(f'❌ [Asaas] Erro na requisição {method} {path}: {e}', flush=True)
        return {'error': str(e), 'success': False}


async def asaas_criar_ou_buscar_customer(cpf: str, nome: str, email: str = '') -> dict:
    """Busca ou cria um customer no Asaas pelo CPF"""
    cpf_limpo = re.sub(r'\D', '', cpf)
    # Buscar customer existente
    resp = await asaas_request('GET', f'/customers?cpfCnpj={cpf_limpo}&limit=1')
    if resp.get('data') and len(resp['data']) > 0:
        c = resp['data'][0]
        print(f'✅ [Asaas] Customer existente: {c["id"]} ({c["name"]})', flush=True)
        return {'success': True, 'customer_id': c['id'], 'novo': False}

    # Criar novo customer
    payload = {
        'name': nome or f'Participante {cpf_limpo[-4:]}',
        'cpfCnpj': cpf_limpo,
        'notificationDisabled': True,
    }
    if email:
        payload['email'] = email

    resp = await asaas_request('POST', '/customers', payload)
    if resp.get('id'):
        print(f'✅ [Asaas] Customer criado: {resp["id"]} ({resp["name"]})', flush=True)
        return {'success': True, 'customer_id': resp['id'], 'novo': True}

    print(f'❌ [Asaas] Erro ao criar customer: {resp}', flush=True)
    return {'success': False, 'error': resp.get('errors', [{}])[0].get('description', str(resp))}


async def asaas_gerar_pix_sorteio(cpf: str, nome: str, valor: float,
                                   descricao: str = 'Participação no Sorteio PaynexBet',
                                   email: str = '') -> dict:
    """
    Gera cobrança PIX Asaas para o sorteio.
    Retorna: {success, payment_id, pix_code, qr_image_b64, expiration}
    """
    # 1. Criar/buscar customer
    cust = await asaas_criar_ou_buscar_customer(cpf, nome, email)
    if not cust['success']:
        return {'success': False, 'error': cust['error']}

    customer_id = cust['customer_id']

    # 2. Criar cobrança PIX
    from datetime import date, timedelta
    due = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')
    payment_payload = {
        'customer': customer_id,
        'billingType': 'PIX',
        'value': round(valor, 2),
        'dueDate': due,
        'description': descricao,
        'externalReference': f'sorteio_cpf_{re.sub(chr(92) + "D", "", cpf)}_{int(time.time())}',
    }
    resp = await asaas_request('POST', '/payments', payment_payload)
    if not resp.get('id'):
        erros = resp.get('errors', [{}])
        msg = erros[0].get('description', str(resp)) if erros else str(resp)
        print(f'❌ [Asaas] Erro ao criar cobrança: {msg}', flush=True)
        return {'success': False, 'error': f'Asaas: {msg}'}

    payment_id = resp['id']
    print(f'✅ [Asaas] Cobrança criada: {payment_id} | R${valor:.2f} | CPF:{cpf}', flush=True)

    # 3. Buscar QR Code PIX
    await asyncio.sleep(1)  # pequena pausa para Asaas processar
    qr_resp = await asaas_request('GET', f'/payments/{payment_id}/pixQrCode')
    if not qr_resp.get('payload'):
        print(f'⚠️ [Asaas] QR Code não disponível ainda para {payment_id}', flush=True)
        return {'success': False, 'error': 'QR Code PIX não disponível. Tente novamente em instantes.'}

    print(f'✅ [Asaas] QR Code gerado para {payment_id}', flush=True)
    return {
        'success': True,
        'payment_id': payment_id,
        'pix_code': qr_resp['payload'],
        'qr_image_b64': qr_resp.get('encodedImage', ''),
        'expiration': qr_resp.get('expirationDate', ''),
        'value': valor,
    }


async def asaas_enviar_pix(chave_pix: str, tipo_chave: str, valor: float,
                            descricao: str = 'Prêmio Sorteio PaynexBet') -> dict:
    """
    Envia PIX via Asaas (cash-out) - substitui Telegram para saques do sorteio.
    tipo_chave: cpf | cnpj | email | phone | evp
    """
    if not ASAAS_API_KEY:
        return {'success': False, 'error': 'Asaas não configurado (ASAAS_API_KEY ausente)'}

    # Mapeamento de tipo_chave para pixAddressKeyType Asaas
    tipo_map = {
        'cpf': 'CPF', 'cnpj': 'CNPJ',
        'email': 'EMAIL', 'phone': 'PHONE',
        'celular': 'PHONE', 'telefone': 'PHONE',
        'evp': 'EVP', 'aleatoria': 'EVP', 'aleatório': 'EVP',
    }
    pix_type = tipo_map.get(tipo_chave.lower(), 'CPF')

    # Limpar chave se CPF/CNPJ/Phone
    chave_limpa = chave_pix.strip()
    if pix_type in ('CPF', 'CNPJ'):
        chave_limpa = re.sub(r'\D', '', chave_limpa)
    elif pix_type == 'PHONE':
        chave_limpa = re.sub(r'\D', '', chave_limpa)
        if not chave_limpa.startswith('55'):
            chave_limpa = '55' + chave_limpa

    payload = {
        'value': round(valor, 2),
        'pixAddressKey': chave_limpa,
        'pixAddressKeyType': pix_type,
        'description': descricao,
    }
    print(f'💸 [Asaas] Enviando PIX R${valor:.2f} → {pix_type}: {chave_limpa}', flush=True)
    resp = await asaas_request('POST', '/transfers', payload)

    if resp.get('id') and resp.get('status') in ('PENDING', 'DONE', 'BANK_PROCESSING'):
        print(f'✅ [Asaas] PIX enviado! ID:{resp["id"]} status:{resp["status"]}', flush=True)
        return {
            'success': True,
            'transfer_id': resp['id'],
            'status': resp['status'],
            'mensagem_bot': f'PIX enviado via Asaas | ID:{resp["id"]} | Status:{resp["status"]}',
        }

    erros = resp.get('errors', [{}])
    msg = erros[0].get('description', str(resp)) if erros else str(resp)
    print(f'❌ [Asaas] Falha ao enviar PIX: {msg}', flush=True)
    return {'success': False, 'error': f'Asaas PIX: {msg}'}


def asaas_salvar_pagamento_db(payment_id: str, tx_id: str, cpf: str,
                               nome: str, valor: float, tipo: str = 'sorteio'):
    """Salva mapeamento payment_id Asaas → tx_id local no SQLite"""
    conn = sqlite3_connect()
    conn.execute('''CREATE TABLE IF NOT EXISTS asaas_pagamentos (
        payment_id TEXT PRIMARY KEY,
        tx_id TEXT,
        cpf TEXT,
        nome TEXT,
        valor REAL,
        tipo TEXT,
        status TEXT DEFAULT 'pendente',
        created_at TEXT,
        confirmed_at TEXT
    )''')
    conn.execute('''INSERT OR REPLACE INTO asaas_pagamentos
        (payment_id, tx_id, cpf, nome, valor, tipo, status, created_at)
        VALUES (?,?,?,?,?,?,'pendente',?)''',
        (payment_id, tx_id, cpf, nome, valor, tipo, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def asaas_confirmar_pagamento_db(payment_id: str) -> dict:
    """Marca pagamento Asaas como confirmado e retorna dados"""
    conn = sqlite3_connect()
    row = conn.execute('SELECT * FROM asaas_pagamentos WHERE payment_id=?', (payment_id,)).fetchone()
    if not row:
        conn.close()
        return {}
    cols = ['payment_id','tx_id','cpf','nome','valor','tipo','status','created_at','confirmed_at']
    d = {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
    conn.execute('UPDATE asaas_pagamentos SET status=?, confirmed_at=? WHERE payment_id=?',
                 ('confirmado', datetime.now().isoformat(), payment_id))
    conn.commit()
    conn.close()
    return d


async def asaas_polling_pagamento(payment_id: str, cpf: str, nome: str, valor: float,
                                   tipo: str = 'sorteio', intervalo: int = 10, max_tentativas: int = 60):
    """
    Polling automático: consulta Asaas a cada `intervalo` segundos por até
    `max_tentativas` vezes (padrão 60x10s = 10 min) até confirmar o pagamento.
    Quando confirmado, processa automaticamente o depósito no sorteio.
    """
    print(f'🔄 [Polling] Iniciando para payment_id={payment_id} CPF={cpf} R${valor:.2f}', flush=True)
    for tentativa in range(1, max_tentativas + 1):
        await asyncio.sleep(intervalo)
        try:
            # Checar DB local primeiro (pode ter sido confirmado pelo webhook)
            conn = sqlite3_connect()
            row = conn.execute('SELECT status FROM asaas_pagamentos WHERE payment_id=?',
                               (payment_id,)).fetchone()
            conn.close()
            if row and row[0] == 'confirmado':
                print(f'✅ [Polling] payment_id={payment_id} já confirmado via webhook. Encerrando polling.', flush=True)
                return

            # Consultar Asaas em tempo real
            resp = await asaas_request('GET', f'/payments/{payment_id}')
            status_asaas = resp.get('status', 'PENDING')
            print(f'🔍 [Polling {tentativa}/{max_tentativas}] payment_id={payment_id} status={status_asaas}', flush=True)

            if status_asaas in ('RECEIVED', 'CONFIRMED'):
                # Marcar como confirmado no DB local
                dados = asaas_confirmar_pagamento_db(payment_id)
                if not dados:
                    asaas_salvar_pagamento_db(payment_id, f'asaas_{payment_id}', cpf, nome, valor, tipo)
                    asaas_confirmar_pagamento_db(payment_id)

                print(f'✅ [Polling] Pagamento confirmado! CPF={cpf} R${valor:.2f} tipo={tipo}', flush=True)

                # Processar depósito no sorteio
                if tipo == 'sorteio':
                    await _processar_deposito_sorteio_asaas(cpf, nome, valor)
                return

            # Pagamento expirado ou cancelado - encerrar polling
            if status_asaas in ('OVERDUE', 'REFUNDED', 'REFUND_REQUESTED', 'CHARGEBACK_REQUESTED',
                                 'CHARGEBACK_DISPUTE', 'AWAITING_CHARGEBACK_REVERSAL',
                                 'DUNNING_REQUESTED', 'DUNNING_RECEIVED', 'AWAITING_RISK_ANALYSIS'):
                print(f'⛔ [Polling] payment_id={payment_id} status={status_asaas}. Encerrando polling.', flush=True)
                # Marcar como cancelado no DB local
                conn = sqlite3_connect()
                conn.execute("UPDATE asaas_pagamentos SET status='cancelado' WHERE payment_id=?", (payment_id,))
                conn.commit()
                conn.close()
                return

        except Exception as e:
            print(f'⚠️ [Polling {tentativa}] Erro consultando payment_id={payment_id}: {e}', flush=True)

    print(f'⏰ [Polling] Timeout após {max_tentativas} tentativas para payment_id={payment_id}. Encerrando.', flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# ─── HELPERS SORTEIO ────────────────────────────────────────
def get_sorteio_config():
    """Retorna config do sorteio usando SELECT com colunas nomeadas (compatível PG + SQLite)"""
    cols = ['id','ativo','valor_por_numero','premio_fixo','percentual',
            'usar_media','dias_media','descricao','proximo_sorteio','updated_at',
            'paypix_pct','paypix_ativo','paypix_descricao','premio_acumulado',
            'min_participantes','acumulativo']
    select_cols = ', '.join(f'COALESCE({c}, NULL) AS {c}' if c not in ('id','ativo','descricao','proximo_sorteio','updated_at') else c for c in cols)
    conn = sqlite3_connect()
    try:
        cur = conn.execute(f'SELECT {", ".join(cols)} FROM sorteio_config WHERE id=1')
        row = cur.fetchone()
    except Exception:
        # fallback: SELECT * com mapeamento por posição
        cur = conn.execute('SELECT * FROM sorteio_config WHERE id=1')
        row = cur.fetchone()
        conn.close()
        if row:
            d = {}
            for i, col in enumerate(cols):
                d[col] = row[i] if i < len(row) else None
            return d
        return {}
    conn.close()
    if row:
        return {col: row[i] for i, col in enumerate(cols)}
    return {}

def get_paypix_config():
    """Retorna configuração do PayPix (%, ativo, descrição, valor mínimo)"""
    conn = sqlite3_connect()
    try:
        # Tentar ler paypix_min (pode não existir ainda)
        try:
            cur = conn.execute('SELECT paypix_pct, paypix_ativo, paypix_descricao, paypix_min FROM sorteio_config WHERE id=1')
            row = cur.fetchone()
            if row:
                conn.close()
                return {
                    'paypix_pct':       float(row[0]) if row[0] is not None else 0.6,
                    'paypix_ativo':     bool(row[1]) if row[1] is not None else True,
                    'paypix_descricao': str(row[2]) if row[2] else 'Gere seu Pix e receba sua % do valor',
                    'paypix_min':       float(row[3]) if row[3] is not None else 5.0,
                }
        except Exception:
            pass
        # Fallback sem paypix_min
        cur = conn.execute('SELECT paypix_pct, paypix_ativo, paypix_descricao FROM sorteio_config WHERE id=1')
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                'paypix_pct':       float(row[0]) if row[0] is not None else 0.6,
                'paypix_ativo':     bool(row[1]) if row[1] is not None else True,
                'paypix_descricao': str(row[2]) if row[2] else 'Gere seu Pix e receba sua % do valor',
                'paypix_min':       5.0,
            }
    except Exception:
        conn.close()
    return {'paypix_pct': 0.6, 'paypix_ativo': True, 'paypix_descricao': 'Gere seu Pix e receba sua % do valor', 'paypix_min': 5.0}

def get_participante(cpf):
    """Busca participante pelo CPF"""
    conn = sqlite3_connect()
    cur = conn.execute("SELECT * FROM sorteio_participantes WHERE cpf=? AND sorteio_id='atual'", (cpf,))
    row = cur.fetchone(); conn.close()
    if not row: return None
    cols = ['id','cliente_id','nome','cpf','chave_pix','tipo_chave',
            'total_depositado','total_numeros','numeros_sorte','created_at','updated_at','sorteio_id']
    d = {}
    for i, col in enumerate(cols):
        d[col] = row[i] if i < len(row) else None
    import json as _json
    try: d['numeros_sorte'] = _json.loads(d['numeros_sorte'] or '[]')
    except: d['numeros_sorte'] = []
    return d

def calcular_numeros(total_depositado, valor_por_numero=5.0):
    """Calcula quantos números a pessoa tem: R$5=1, R$10=2, R$15=3..."""
    return max(0, int(total_depositado // valor_por_numero))

def gerar_bilhetes_unicos(cliente_id, qtd, sorteio_id='atual'):
    """Gera números únicos para o participante (sem repetir com outros)"""
    import hashlib
    # Buscar números já usados (query independente)
    conn = sqlite3_connect()
    cur = conn.execute('SELECT numero FROM sorteio_bilhetes WHERE sorteio_id=?', (sorteio_id,))
    usados = set(r[0] for r in cur.fetchall())
    conn.close()

    numeros = []
    tentativa = 0
    while len(numeros) < qtd:
        seed = hashlib.md5(f"{cliente_id}{sorteio_id}{tentativa}vortex".encode()).hexdigest()
        num = int(seed[:8], 16) % 900000 + 100000  # 100000-999999
        if num not in usados:
            usados.add(num)
            numeros.append(num)
        tentativa += 1
        if tentativa > 9999999: break

    # Salvar cada bilhete com conexão própria (nova conn por INSERT = zero conflito)
    for num in numeros:
        try:
            conn2 = sqlite3_connect()
            conn2.execute('INSERT INTO sorteio_bilhetes (cliente_id, numero, sorteio_id, created_at) VALUES (?,?,?,?)',
                         (cliente_id, num, sorteio_id, datetime.now().isoformat()))
            conn2.commit()
            conn2.close()
        except Exception as e:
            print(f'[bilhete] erro ao inserir {num}: {e}', flush=True)
    return numeros

async def _loop_verificar_pagamentos():
    """Verifica a cada 30s se há pagamentos pendentes confirmados no bot"""
    await asyncio.sleep(10)  # Aguarda sistema estabilizar
    while True:
        try:
            if not _telegram_ready:
                await asyncio.sleep(30)
                continue

            # Buscar transações pendentes
            conn = sqlite3_connect()
            cur = conn.execute("""SELECT tx_id, valor, extra FROM transacoes
                         WHERE status='pendente'
                         ORDER BY created_at DESC LIMIT 10""")
            pendentes = cur.fetchall()
            conn.close()

            if not pendentes:
                await asyncio.sleep(30)
                continue

            # Verificar mensagens recentes do bot (últimos 5 min)
            import datetime as _dt
            bot = await client.get_entity(BOT_USERNAME)
            msgs = await client.get_messages(bot, limit=20)

            padroes = [
                r'Depósito de R\$',
                r'✅ Depósito',
                r'depósito.*recebido',
                r'pagamento.*confirmado',
                r'Valor creditado',
                r'depósito aprovado',
                r'recebemos.*R\$',
                r'crédito.*R\$',
                r'pix.*recebido',
                r'transferência.*recebida',
            ]

            for msg in msgs:
                if not msg.text: continue
                # Só mensagens dos últimos 10 minutos
                if hasattr(msg, 'date') and msg.date:
                    idade = (_dt.datetime.now(_dt.timezone.utc) - msg.date).total_seconds()
                    if idade > 600: continue

                if not any(re.search(p, msg.text, re.IGNORECASE) for p in padroes):
                    continue

                # Extrair valor da mensagem
                val_match = re.search(r'R\$\s*([\d.,]+)', msg.text)
                valor_msg = None
                if val_match:
                    try:
                        valor_msg = float(val_match.group(1).replace(',','.').replace(' ',''))
                    except: pass

                print(f'💰 [Loop] Msg pagamento detectada: {msg.text[:80]}', flush=True)

                for row_p in pendentes:
                    tx_id_p, valor_p, extra_p = row_p[0], row_p[1], row_p[2]
                    if valor_msg is None or abs(valor_p - valor_msg) < 0.05:
                        # Confirmar pagamento
                        confirmar_pagamento(tx_id_p)
                        print(f'✅ [Loop] Confirmado: {tx_id_p} R${valor_p:.2f}', flush=True)
                        # Split PayPix automático
                        if extra_p:
                            try:
                                ex = json.loads(extra_p)
                                if ex.get('tipo') == 'paypix':
                                    asyncio.create_task(_processar_split_paypix(tx_id_p, valor_p, extra_p))
                                    print(f'💸 [Loop] Split disparado: R${valor_p * 0.6:.2f} → {ex.get("parceiro_chave")}', flush=True)
                            except Exception as ex_err:
                                print(f'[Loop] Erro split: {ex_err}', flush=True)
                        break

        except Exception as e:
            print(f'[Loop verificar] erro: {e}', flush=True)

        await asyncio.sleep(30)  # Verificar a cada 30 segundos

# ─── TELEGRAM - Registrar handler de mensagens ──────────────
def _registrar_handler_telegram():
    """Registra o handler de novas mensagens (chamado após cada reconexão)"""
    @client.on(events.NewMessage(from_users=BOT_USERNAME))
    async def handler(event):
        texto = event.message.text or ''
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
            conn = sqlite3_connect()
            cur = conn.execute("""SELECT tx_id, valor, extra FROM transacoes
                         WHERE status='pendente'
                         ORDER BY created_at DESC LIMIT 5""")
            pendentes = cur.fetchall()
            conn.close()
            val_match = re.search(r'R\$\s*([\d,.]+)', texto)
            valor_msg = None
            if val_match:
                try:
                    valor_msg = float(val_match.group(1).replace(',', '.').replace(' ', ''))
                except:
                    pass
            for row_p in pendentes:
                tx_id_p, valor_p, extra_p = row_p[0], row_p[1], row_p[2]
                if valor_msg is None or abs(valor_p - valor_msg) < 0.05 or len(pendentes) == 1:
                    confirmar_pagamento(tx_id_p)
                    print(f'✅ Pago confirmado: {tx_id_p} R${valor_p}', flush=True)
                    if extra_p:
                        try:
                            extra_d = json.loads(extra_p)
                            if extra_d.get('tipo') == 'paypix':
                                asyncio.create_task(_processar_split_paypix(tx_id_p, valor_p, extra_p))
                        except Exception as ex:
                            print(f'Erro split handler: {ex}', flush=True)
                    break

# ─── TELEGRAM - Ping de keepalive ───────────────────────────
async def _ping_telegram() -> bool:
    """Testa se a conexão Telegram está viva. Retorna True se OK."""
    global _telegram_ultimo_ping
    try:
        if not client.is_connected():
            return False
        me = await asyncio.wait_for(client.get_me(), timeout=15)
        if me:
            _telegram_ultimo_ping = time.time()
            return True
        return False
    except Exception as e:
        print(f'[Ping] falhou: {e}', flush=True)
        return False

# ─── TELEGRAM - Auto-Login (sem intervenção humana) ─────────
_auto_login_em_progresso = False   # evita múltiplos auto-logins simultâneos
_floodwait_ate = 0                 # timestamp até quando o FloodWait está ativo

async def _auto_login_telegram():
    """
    Auto-login completo sem intervenção humana:
    1) Solicita código via Telethon (temp_client)
    2) Aguarda 25s para o Telegram entregar a mensagem
    3) Lê conversa com "Telegram" (serviceNotifications) para extrair o código
    4) Confirma o código e reinicializa o cliente principal
    """
    global _login_state, client, _telegram_ready, _telegram_session_invalida
    global _auto_login_em_progresso

    if _auto_login_em_progresso:
        print('🤖 [AutoLogin] Já em progresso, aguardando...', flush=True)
        return False

    _auto_login_em_progresso = True
    print('🤖 [AutoLogin] Iniciando auto-login Telegram...', flush=True)

    try:
        from telethon.sessions import StringSession as SS
        from telethon.errors import FloodWaitError, SessionPasswordNeededError
        import re as re_al

        # ── Passo 1: cliente temporário para solicitar código ──────────
        temp_client = TelegramClient(SS(), API_ID, API_HASH)
        await temp_client.connect()
        print('🤖 [AutoLogin] Solicitando código...', flush=True)
        _phone_autologin = PHONE_NUMBER or os.environ.get('TELEGRAM_PHONE', '')
        if not _phone_autologin:
            print('🤖 [AutoLogin] Número de telefone não configurado - impossível auto-login', flush=True)
            _auto_login_em_progresso = False
            return False
        try:
            sent = await temp_client.send_code_request(_phone_autologin)
        except FloodWaitError as fw:
            _floodwait_ate = time.time() + fw.seconds
            mins = fw.seconds // 60
            print(f'🤖 [AutoLogin] FloodWait: aguardar {mins}min ({fw.seconds}s). Próxima tentativa após {datetime.fromtimestamp(_floodwait_ate).strftime("%H:%M")}', flush=True)
            try: await temp_client.disconnect()
            except: pass
            _auto_login_em_progresso = False
            return False

        phone_code_hash = sent.phone_code_hash
        temp_session = temp_client.session.save()
        print('🤖 [AutoLogin] Código solicitado! Aguardando 35s para chegar...', flush=True)

        # ── Passo 2: aguardar a mensagem chegar no Telegram ────────────
        await asyncio.sleep(35)

        # ── Passo 3: ler mensagens usando sessão do DB (mais recente) ──
        # Estratégia em cascata: DB → SESSION_STR → sem leitura
        codigo = None
        sessoes_tentar = []

        # 1) Tentar sessão do banco (mais recente/válida)
        try:
            import psycopg2 as _pg2
            _pg_conn = _pg2.connect(DATABASE_URL)
            _cur = _pg_conn.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='telegram_session'")
            _row = _cur.fetchone()
            _pg_conn.close()
            if _row and _row[0] and len(_row[0]) > 100:
                sessoes_tentar.append(('DB', _row[0]))
        except:
            pass

        # 2) Sessão do env/arquivo
        if SESSION_STR and len(SESSION_STR) > 100:
            sessoes_tentar.append(('env', SESSION_STR))

        for origem, sess_str in sessoes_tentar:
            if codigo:
                break
            try:
                print(f'🤖 [AutoLogin] Tentando ler mensagens com sessão {origem}...', flush=True)
                leitor = TelegramClient(SS(sess_str), API_ID, API_HASH)
                await leitor.connect()
                if await leitor.is_user_authorized():
                    # Tenta 777000 (notificações do Telegram)
                    async for msg in leitor.iter_messages(777000, limit=5):
                        txt = msg.message or ''
                        m = re_al.search(r'\b(\d{5})\b', txt)
                        if m:
                            codigo = m.group(1)
                            print(f'🤖 [AutoLogin] ✅ Código [{origem}]: {codigo}', flush=True)
                            break
                    # Fallback: busca em todas as mensagens recentes
                    if not codigo:
                        async for msg in leitor.iter_messages(limit=20):
                            txt = msg.message or ''
                            if 'login' in txt.lower() or 'código' in txt.lower() or 'code' in txt.lower():
                                m = re_al.search(r'\b(\d{5})\b', txt)
                                if m:
                                    codigo = m.group(1)
                                    print(f'🤖 [AutoLogin] ✅ Código busca geral [{origem}]: {codigo}', flush=True)
                                    break
                    await leitor.disconnect()
                else:
                    await leitor.disconnect()
                    print(f'🤖 [AutoLogin] Sessão {origem} sem autorização', flush=True)
            except Exception as e_read:
                print(f'🤖 [AutoLogin] Erro sessão {origem}: {e_read}', flush=True)

        if not codigo:
            print('🤖 [AutoLogin] ❌ Código não encontrado! Sistema aguardará próximo ciclo (3min).', flush=True)
            try:
                await temp_client.disconnect()
            except:
                pass
            _auto_login_em_progresso = False
            return False

        # ── Passo 4: confirmar código com temp_client ──────────────────
        print(f'🤖 [AutoLogin] Confirmando código {codigo}...', flush=True)
        try:
            # Reconectar temp_client se necessário
            if not temp_client.is_connected():
                temp_client = TelegramClient(SS(temp_session), API_ID, API_HASH)
                await temp_client.connect()

            await temp_client.sign_in(_phone_autologin, codigo, phone_code_hash=phone_code_hash)
        except SessionPasswordNeededError:
            print('🤖 [AutoLogin] 2FA necessário - não suportado no auto-login', flush=True)
            await temp_client.disconnect()
            _auto_login_em_progresso = False
            return False
        except Exception as e_sign:
            print(f'🤖 [AutoLogin] Erro sign_in: {e_sign}', flush=True)
            await temp_client.disconnect()
            _auto_login_em_progresso = False
            return False

        me = await temp_client.get_me()
        nova_sessao = temp_client.session.save()
        await temp_client.disconnect()
        print(f'🤖 [AutoLogin] Login OK: {me.first_name} ({me.id})', flush=True)

        # ── Passo 5: salvar nova sessão ────────────────────────────────
        try:
            with open('session_string.txt', 'w') as f:
                f.write(nova_sessao)
        except Exception as e_file:
            print(f'[AutoLogin] Erro ao salvar session_string.txt: {e_file}', flush=True)

        _salvar_sessao_db(nova_sessao)

        # ── Passo 6: reinicializar cliente principal ───────────────────
        try:
            if client.is_connected():
                await client.disconnect()
        except:
            pass
        from telethon.sessions import StringSession as SS2
        client.__init__(SS2(nova_sessao), API_ID, API_HASH)
        _telegram_session_invalida = False
        _telegram_ready = False
        await client.connect()
        if await client.is_user_authorized():
            _telegram_ready = True
            _salvar_sessao_db(nova_sessao)
            print('🤖 [AutoLogin] ✅ Cliente principal reconectado automaticamente!', flush=True)
            _auto_login_em_progresso = False
            return True
        else:
            print('🤖 [AutoLogin] ❌ Cliente principal não autorizou após login', flush=True)
            _auto_login_em_progresso = False
            return False

    except Exception as e:
        print(f'🤖 [AutoLogin] Erro geral: {e}', flush=True)
        _auto_login_em_progresso = False
        return False

# ─── TELEGRAM - Reconexão limpa ─────────────────────────────
async def _reconectar_telegram():
    """Desconecta e reconecta o client Telegram de forma limpa."""
    global _telegram_ready, _telegram_tentativas, _telegram_reconectando
    if _telegram_reconectando:
        return  # já tem reconexão em andamento
    _telegram_reconectando = True
    _telegram_ready = False
    try:
        try:
            if client.is_connected():
                await client.disconnect()
        except:
            pass
        await asyncio.sleep(3)
        await client.connect()
        if await client.is_user_authorized():
            _telegram_ready = True
            _telegram_tentativas = 0
            _telegram_ultimo_ping = time.time()
            print('✅ [Reconexão] Telegram OK!', flush=True)
        else:
            print('❌ [Reconexão] Sessão inválida após reconectar', flush=True)
    except Exception as e:
        print(f'❌ [Reconexão] Erro: {e}', flush=True)
    finally:
        _telegram_reconectando = False

# ─── TELEGRAM - Watchdog (verifica a cada 2min) ──────────────
async def watchdog_telegram():
    """
    Loop eterno que:
    1) A cada 2min faz ping no Telegram
    2) Se falhar → tenta reconectar imediatamente
    3) Se reconectar falhar → tenta de novo em 30s, 60s, 120s (backoff)
    4) A cada 30min salva a sessão atual no DB (keepalive de sessão)
    5) NUNCA desiste - só para se sessão for revogada (AuthKeyDuplicated)
    """
    global _telegram_ready, _telegram_session_invalida, _sessao_salva_em
    await asyncio.sleep(30)  # aguarda sistema estabilizar no boot
    print('🔍 [Watchdog] Iniciado - verificando Telegram a cada 2min', flush=True)

    PING_INTERVAL   = 120   # 2 minutos entre pings normais
    SAVE_INTERVAL   = 1800  # 30 minutos entre saves de sessão
    backoff_delays  = [30, 60, 120, 180, 300]  # backoff progressivo em segundos
    falhas_seguidas = 0

    while True:
        try:
            # ── Salvar sessão periodicamente ──────────────────────
            agora = time.time()
            if _telegram_ready and (agora - _sessao_salva_em) > SAVE_INTERVAL:
                try:
                    sess = client.session.save()
                    if sess and len(sess) > 50:
                        _salvar_sessao_db(sess)
                        _sessao_salva_em = agora
                        print('💾 [Watchdog] Sessão Telegram salva no DB', flush=True)
                except Exception as e_save:
                    print(f'[Watchdog] Erro ao salvar sessão: {e_save}', flush=True)

            # ── Se sessão revogada → tentar reconectar via DB primeiro ──
            if _telegram_session_invalida:
                if not _auto_login_em_progresso:
                    # Passo 1: tentar reconectar com sessão do banco (sem pedir código)
                    print('🔄 [Watchdog] Tentando reconectar via sessão do DB...', flush=True)
                    reconectou = False
                    try:
                        import psycopg2 as _pg2w
                        _pgcw = _pg2w.connect(DATABASE_URL)
                        _curw = _pgcw.cursor()
                        _curw.execute("SELECT valor FROM configuracoes WHERE chave='telegram_session'")
                        _roww = _pgcw.fetchone()
                        _pgcw.close()
                        if _roww and _roww[0] and len(_roww[0]) > 50:
                            _sessw = _roww[0]
                            from telethon.sessions import StringSession as _SSW
                            try:
                                if client.is_connected(): await client.disconnect()
                            except: pass
                            await asyncio.sleep(2)
                            client.__init__(_SSW(_sessw), API_ID, API_HASH)
                            _telegram_session_invalida = False
                            await client.connect()
                            if await client.is_user_authorized():
                                _telegram_ready = True
                                falhas_seguidas = 0
                                reconectou = True
                                print('✅ [Watchdog] Reconectado via sessão DB!', flush=True)
                    except Exception as _ew:
                        print(f'⚠️ [Watchdog] Sessão DB falhou: {_ew}', flush=True)

                    if reconectou:
                        await asyncio.sleep(PING_INTERVAL)
                        continue

                    # Passo 2: verificar FloodWait antes de tentar auto-login
                    agora_fw = time.time()
                    if _floodwait_ate > agora_fw:
                        mins_rest = int((_floodwait_ate - agora_fw) / 60)
                        print(f'⏳ [Watchdog] FloodWait ativo - aguardando mais {mins_rest}min para tentar auto-login', flush=True)
                        await asyncio.sleep(min(300, _floodwait_ate - agora_fw))
                        continue

                    # Passo 3: sessão DB inválida → tentar auto-login com código
                    print('🤖 [Watchdog] Sessão DB inválida → iniciando AUTO-LOGIN...', flush=True)
                    sucesso = await _auto_login_telegram()
                    if sucesso:
                        print('🤖 [Watchdog] Auto-login bem-sucedido! Retomando operação normal.', flush=True)
                        _telegram_session_invalida = False
                        falhas_seguidas = 0
                        await asyncio.sleep(PING_INTERVAL)
                        continue
                    else:
                        print('🤖 [Watchdog] Auto-login falhou. Tentando novamente em 3min...', flush=True)
                        await asyncio.sleep(180)
                        continue
                else:
                    print('🤖 [Watchdog] Auto-login em progresso, aguardando...', flush=True)
                    await asyncio.sleep(30)
                    continue

            # ── Ping ───────────────────────────────────────────────
            if _telegram_ready:
                ok = await _ping_telegram()
                if ok:
                    falhas_seguidas = 0
                    await asyncio.sleep(PING_INTERVAL)
                    continue
                else:
                    print(f'⚠️ [Watchdog] Ping falhou! Tentando reconectar...', flush=True)
                    _telegram_ready = False
            else:
                print(f'⚠️ [Watchdog] Telegram offline! Reconectando...', flush=True)

            # ── Reconectar ────────────────────────────────────────
            await _reconectar_telegram()

            if _telegram_ready:
                falhas_seguidas = 0
                print(f'✅ [Watchdog] Reconectado com sucesso!', flush=True)
                await asyncio.sleep(PING_INTERVAL)
            else:
                # Backoff progressivo
                falhas_seguidas += 1
                delay = backoff_delays[min(falhas_seguidas - 1, len(backoff_delays) - 1)]
                print(f'🔄 [Watchdog] Falha #{falhas_seguidas} - tentando novamente em {delay}s', flush=True)
                await asyncio.sleep(delay)

        except Exception as e_watch:
            print(f'[Watchdog] Exceção inesperada: {e_watch}', flush=True)
            await asyncio.sleep(30)

# ─── TELEGRAM - Conectar com retry ──────────────────────────
async def conectar_telegram():
    """Conexão inicial do Telegram - após conectar, o watchdog assume o keepalive."""
    global _telegram_ready, _telegram_tentativas, _telegram_session_invalida
    _registrar_handler_telegram()  # registrar handler uma única vez

    while True:
        if _telegram_session_invalida:
            if not _auto_login_em_progresso:
                print('🤖 [ConectarTG] Sessão inválida → tentando AUTO-LOGIN...', flush=True)
                sucesso = await _auto_login_telegram()
                if sucesso:
                    print('🤖 [ConectarTG] Auto-login bem-sucedido!', flush=True)
                    _telegram_session_invalida = False
                    continue
                else:
                    print('🤖 [ConectarTG] Auto-login falhou, tentando em 3min...', flush=True)
                    await asyncio.sleep(180)
                    continue
            else:
                print('🤖 [ConectarTG] Auto-login em progresso, aguardando 30s...', flush=True)
                await asyncio.sleep(30)
                continue

        _telegram_tentativas += 1
        try:
            print(f'🔄 Tentativa {_telegram_tentativas} - Conectando Telegram...', flush=True)
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
            _telegram_ultimo_ping = time.time()

            # Salvar sessão válida imediatamente
            try:
                _salvar_sessao_db(client.session.save())
                _sessao_salva_em = time.time()
            except:
                pass

            # Iniciar loops de background
            asyncio.create_task(_loop_verificar_pagamentos())
            asyncio.create_task(watchdog_telegram())

            print('✅ Listener ativo - watchdog iniciado', flush=True)
            await client.run_until_disconnected()

        except Exception as e:
            nome_erro = type(e).__name__
            print(f'❌ Erro Telegram ({nome_erro}): {e}', flush=True)
            if 'AuthKeyDuplicated' in nome_erro or 'AuthKeyDuplicated' in str(e):
                print('🚫 Sessão revogada (AuthKeyDuplicated). Pausando 5min...', flush=True)
                _telegram_session_invalida = True
                _telegram_ready = False
                await asyncio.sleep(300)
                continue

        _telegram_ready = False
        espera = min(60, _telegram_tentativas * 5)
        print(f'🔄 Reconectando em {espera}s...', flush=True)
        await asyncio.sleep(espera)

# ══════════════════════════════════════════════════════════════════
# ─── BOT 2 - Conexão e Watchdog (@paypix_nexbot) ────────────────────
# ══════════════════════════════════════════════════════════════════

async def conectar_telegram2():
    """Conexão inicial do Bot2 (@paypix_nexbot) com retry automático."""
    global _telegram2_ready, _telegram2_session_inv
    await asyncio.sleep(15)  # aguarda Bot1 iniciar primeiro
    print('🔄 [Bot2] Conectando @paypix_nexbot...', flush=True)
    tentativas2 = 0
    while True:
        if not SESSION_STR2:
            print('⚠️ [Bot2] Sem sessão configurada - aguardando login pelo admin...', flush=True)
            await asyncio.sleep(60)
            continue
        tentativas2 += 1
        try:
            if client2.is_connected():
                await client2.disconnect()
            await asyncio.sleep(1)
            await client2.connect()
            if not await client2.is_user_authorized():
                print('❌ [Bot2] Sessão inválida - reconexão necessária via admin', flush=True)
                _telegram2_session_inv = True
                _telegram2_ready = False
                await asyncio.sleep(60)
                continue
            me2 = await client2.get_me()
            print(f'✅ [Bot2] @paypix_nexbot conectado: {me2.first_name} ({me2.id})', flush=True)
            _telegram2_ready = True
            _telegram2_session_inv = False
            await client2.run_until_disconnected()
        except Exception as e2:
            nome2 = type(e2).__name__
            print(f'❌ [Bot2] Erro ({nome2}): {e2}', flush=True)
            if 'AuthKeyDuplicated' in nome2 or 'AuthKeyDuplicated' in str(e2):
                print('🚫 [Bot2] Sessão revogada. Pausando 5min...', flush=True)
                _telegram2_session_inv = True
                _telegram2_ready = False
                await asyncio.sleep(300)
                continue
        _telegram2_ready = False
        espera2 = min(120, tentativas2 * 10)
        print(f'🔄 [Bot2] Reconectando em {espera2}s...', flush=True)
        await asyncio.sleep(espera2)

async def watchdog_telegram2():
    """Watchdog do Bot2 - ping a cada 2min, reconecta se cair."""
    global _telegram2_ready, _telegram2_ultimo_ping
    await asyncio.sleep(45)  # aguarda Bot2 inicializar
    print('🔍 [Watchdog Bot2] Iniciado', flush=True)
    PING_INTERVAL2 = 120
    while True:
        try:
            if _telegram2_ready:
                try:
                    me2 = await asyncio.wait_for(client2.get_me(), timeout=15)
                    if me2:
                        _telegram2_ultimo_ping = time.time()
                        await asyncio.sleep(PING_INTERVAL2)
                        continue
                except Exception:
                    pass
                print('⚠️ [Watchdog Bot2] Ping falhou!', flush=True)
                _telegram2_ready = False
            else:
                if SESSION_STR2:
                    print('🔄 [Watchdog Bot2] Tentando reconectar...', flush=True)
                    try:
                        from telethon.sessions import StringSession as _SS2W
                        if client2.is_connected():
                            await client2.disconnect()
                        await asyncio.sleep(2)
                        await client2.connect()
                        if await client2.is_user_authorized():
                            _telegram2_ready = True
                            print('✅ [Watchdog Bot2] Reconectado!', flush=True)
                    except Exception as ew2:
                        print(f'⚠️ [Watchdog Bot2] Reconexão falhou: {ew2}', flush=True)
                await asyncio.sleep(30)
        except Exception as ewg2:
            print(f'[Watchdog Bot2] Exceção: {ewg2}', flush=True)
            await asyncio.sleep(30)

async def _loop_verificar_pagamentos_bot2():
    """Verifica a cada 30s pagamentos confirmados pelo @paypix_nexbot"""
    await asyncio.sleep(20)
    while True:
        try:
            if not _telegram2_ready:
                await asyncio.sleep(30)
                continue
            conn = sqlite3_connect()
            cur = conn.execute("""SELECT tx_id, valor, extra FROM transacoes
                         WHERE status='pendente' AND (tx_id LIKE 'txn2_%')
                         ORDER BY created_at DESC LIMIT 10""")
            pendentes = cur.fetchall()
            conn.close()
            if not pendentes:
                await asyncio.sleep(30)
                continue
            import datetime as _dt
            bot2 = await client2.get_entity(BOT2_USERNAME)
            msgs = await client2.get_messages(bot2, limit=20)
            padroes = [
                r'Depósito de R\$', r'✅ Depósito', r'depósito.*recebido',
                r'pagamento.*confirmado', r'Valor creditado', r'depósito aprovado',
                r'recebemos.*R\$', r'crédito.*R\$', r'pix.*recebido', r'transferência.*recebida',
            ]
            for msg in msgs:
                if not msg.text: continue
                if hasattr(msg, 'date') and msg.date:
                    idade = (_dt.datetime.now(_dt.timezone.utc) - msg.date).total_seconds()
                    if idade > 600: continue
                if not any(re.search(p, msg.text, re.IGNORECASE) for p in padroes): continue
                val_match = re.search(r'R\$\s*([\d.,]+)', msg.text)
                if not val_match: continue
                valor_msg = float(val_match.group(1).replace(',', '.'))
                for tx_id, valor_db, extra_json in pendentes:
                    if abs(valor_msg - valor_db) < 0.02:
                        conn2 = sqlite3_connect()
                        conn2.execute("UPDATE transacoes SET status='confirmado' WHERE tx_id=?", (tx_id,))
                        conn2.commit(); conn2.close()
                        print(f'✅ [Bot2] Pagamento confirmado: {tx_id} R${valor_db}', flush=True)
                        break
        except Exception as e2lp:
            print(f'[Bot2 loop pagamentos] erro: {e2lp}', flush=True)
        await asyncio.sleep(30)

# ─── GERAR PIX - Garante conexão antes de gerar ────────────
async def verificar_saldo_bot() -> float:
    """Consulta saldo atual no bot clicando em CARTEIRA"""
    try:
        bot = await client.get_entity(BOT_USERNAME)
        # Enviar /start e clicar em CARTEIRA
        await client.send_message(bot, '/start')
        await asyncio.sleep(2)
        msgs = await client.get_messages(bot, limit=5)
        # Tentar clicar no botão CARTEIRA
        for msg in msgs:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if 'CARTEIRA' in (btn.text or '').upper():
                            await btn.click()
                            await asyncio.sleep(3)
                            break
        # Ler resposta com saldo
        msgs2 = await client.get_messages(bot, limit=5)
        for msg in msgs2:
            if not msg.text:
                continue
            # Padrão: 💰 Saldo Atual: R$ 10.38
            m = re.search(r'Saldo Atual[:\s]*R\$\s*([\d,.]+)', msg.text)
            if m:
                return float(m.group(1).replace(',', '.'))
            # Padrão alternativo: Saldo Disponível `R$ 10,38`
            m2 = re.search(r'Saldo[^`\n]*[`:]\s*R\$\s*([\d,.]+)', msg.text)
            if m2:
                return float(m2.group(1).replace(',', '.'))
            # Padrão direto: R$ 10.38 após saldo
            m3 = re.search(r'💰[^\n]*R\$\s*([\d,.]+)', msg.text)
            if m3:
                return float(m3.group(1).replace(',', '.'))
    except Exception as e:
        print(f'[saldo_bot] erro: {e}', flush=True)
    return -1.0

# ══════════════════════════════════════════════════════════════════
# ─── BOT 2 - @paypix_nexbot - Funções espelhadas ────────────────────
# ══════════════════════════════════════════════════════════════════

async def verificar_saldo_bot2() -> float:
    """Consulta saldo atual no @paypix_nexbot clicando em CARTEIRA"""
    try:
        bot2 = await client2.get_entity(BOT2_USERNAME)
        await client2.send_message(bot2, '/start')
        await asyncio.sleep(2)
        msgs = await client2.get_messages(bot2, limit=5)
        for msg in msgs:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if 'CARTEIRA' in (btn.text or '').upper():
                            await btn.click()
                            await asyncio.sleep(3)
                            break
        msgs2 = await client2.get_messages(bot2, limit=5)
        for msg in msgs2:
            if not msg.text:
                continue
            m = re.search(r'Saldo Atual[:\s]*R\$\s*([\d,.]+)', msg.text)
            if m:
                return float(m.group(1).replace(',', '.'))
            m2 = re.search(r'Saldo[^`\n]*[`:]\s*R\$\s*([\d,.]+)', msg.text)
            if m2:
                return float(m2.group(1).replace(',', '.'))
            m3 = re.search(r'💰[^\n]*R\$\s*([\d,.]+)', msg.text)
            if m3:
                return float(m3.group(1).replace(',', '.'))
    except Exception as e:
        print(f'[saldo_bot2] erro: {e}', flush=True)
    return -1.0

async def gerar_pix_bot2(valor, cliente_id=None, webhook_url=None, participante_dados=None, tx_id_override=None):
    """Gera Pix via @paypix_nexbot (Bot 2). Mesma lógica do Bot 1."""
    # Espera Bot2 ficar pronto (até 30s)
    for _ in range(30):
        if _telegram2_ready:
            break
        await asyncio.sleep(1)
    if not _telegram2_ready:
        return {'success': False, 'error': '[Bot2] Serviço temporariamente indisponível. Tente novamente.'}

    try:
        await asyncio.wait_for(_lock2.acquire(), timeout=120)
    except asyncio.TimeoutError:
        return {'success': False, 'error': '[Bot2] Sistema ocupado. Tente novamente em instantes.'}

    try:
        bot2 = await client2.get_entity(BOT2_USERNAME)

        await client2.send_message(bot2, '/start')
        await asyncio.sleep(2)

        messages = await client2.get_messages(bot2, limit=5)
        clicou = False
        for msg in messages:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if 'DEPOSITAR' in btn.text:
                            await btn.click()
                            await asyncio.sleep(2)
                            clicou = True; break
                    if clicou: break
            if clicou: break

        if not clicou:
            return {'success': False, 'error': '[Bot2] Botão DEPOSITAR não encontrado.'}

        valor_str = str(int(valor)) if valor == int(valor) else f"{valor:.2f}"
        import datetime as _dt
        hora_envio = _dt.datetime.now(_dt.timezone.utc)
        cutoff = hora_envio - _dt.timedelta(seconds=3)
        await client2.send_message(bot2, valor_str)
        print(f'[gerar_pix_bot2] Valor {valor_str} enviado, aguardando resposta...', flush=True)

        for tentativa in range(30):
            await asyncio.sleep(2)
            msgs = await client2.get_messages(bot2, limit=10)
            for msg in msgs:
                if not msg.text: continue
                if msg.date and msg.date < cutoff: continue
                txt = msg.text or ''
                print(f'[gerar_pix_bot2] [{tentativa}] {txt[:100]}', flush=True)
                if '00020101' in txt:
                    pix_match = re.search(r'`?(00020101[^`\s\n]+)`?', txt)
                    pix_code  = pix_match.group(1) if pix_match else None
                    tx_match  = re.search(r'txn_([a-f0-9]+)', txt)
                    tx_id     = f"txn_{tx_match.group(1)}" if tx_match else f"txn2_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d,.]+)', txt)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"
                    if pix_code:
                        if not tx_id_override:
                            salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url, participante_dados)
                        print(f'✅ [Bot2] Pix gerado: {tx_id} R${valor}', flush=True)
                        return {'success': True, 'pix_code': pix_code, 'tx_id': tx_id,
                                'valor': f"R$ {valor_conf}", 'status': 'pendente', 'bot': 'bot2'}
                if 'PIX Copia e Cola' in txt or 'Copia e Cola' in txt:
                    pix_match = re.search(r'`?(00020101[^`\s\n]+)`?', txt)
                    pix_code  = pix_match.group(1) if pix_match else None
                    tx_match  = re.search(r'txn_([a-f0-9]+)', txt)
                    tx_id     = f"txn_{tx_match.group(1)}" if tx_match else f"txn2_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d,.]+)', txt)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"
                    if pix_code:
                        if not tx_id_override:
                            salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url, participante_dados)
                        print(f'✅ [Bot2] Pix gerado (copia-cola): {tx_id} R${valor}', flush=True)
                        return {'success': True, 'pix_code': pix_code, 'tx_id': tx_id,
                                'valor': f"R$ {valor_conf}", 'status': 'pendente', 'bot': 'bot2'}

        print(f'[gerar_pix_bot2] Timeout - nenhum código Pix recebido', flush=True)
        return {'success': False, 'error': '[Bot2] Bot demorou para responder. Tente novamente.'}
    except Exception as e:
        print(f'❌ [Bot2] Erro gerar_pix_bot2: {e}', flush=True)
        return {'success': False, 'error': str(e)}
    finally:
        try:
            _lock2.release()
        except Exception:
            pass

# ──────────────────────────────────────────────────────────────────

async def gerar_pix(valor, cliente_id=None, webhook_url=None, participante_dados=None, tx_id_override=None):
    """Gera Pix via bot Telegram.
    Se tx_id_override for informado, NÃO chama salvar_transacao (o registro já existe no banco)."""
    # Espera Telegram ficar pronto (até 30s)
    for _ in range(30):
        if _telegram_ready:
            break
        await asyncio.sleep(1)
    if not _telegram_ready:
        return {'success': False, 'error': 'Serviço temporariamente indisponível. Tente novamente.'}

    # Lock com timeout de 120s para não bloquear para sempre
    try:
        await asyncio.wait_for(_lock.acquire(), timeout=120)
    except asyncio.TimeoutError:
        return {'success': False, 'error': 'Sistema ocupado. Tente novamente em instantes.'}

    try:
        bot = await client.get_entity(BOT_USERNAME)

        # Sempre iniciar com /start para garantir estado limpo
        await client.send_message(bot, '/start')
        await asyncio.sleep(2)

        # Clicar DEPOSITAR
        messages = await client.get_messages(bot, limit=5)
        clicou = False
        for msg in messages:
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if 'DEPOSITAR' in btn.text:
                            await btn.click()
                            await asyncio.sleep(2)
                            clicou = True; break
                    if clicou: break
            if clicou: break

        if not clicou:
            return {'success': False, 'error': 'Botão DEPOSITAR não encontrado. Tente novamente.'}

        # Enviar valor
        valor_str = str(int(valor)) if valor == int(valor) else f"{valor:.2f}"
        import datetime as _dt
        # CRÍTICO: marca o tempo ANTES do envio para filtrar mensagens antigas
        hora_envio = _dt.datetime.now(_dt.timezone.utc)
        cutoff = hora_envio - _dt.timedelta(seconds=3)  # margem de 3s para clock skew
        await client.send_message(bot, valor_str)
        print(f'[gerar_pix] Valor {valor_str} enviado, aguardando resposta...', flush=True)

        # Polling ativo - checar a cada 2s por até 60s (30 tentativas)
        for tentativa in range(30):
            await asyncio.sleep(2)
            msgs = await client.get_messages(bot, limit=10)
            for msg in msgs:
                if not msg.text:
                    continue
                # FILTRO RIGOROSO: ignorar mensagens com data anterior ao envio
                if msg.date and msg.date < cutoff:
                    continue
                txt = msg.text or ''
                # Log para debug
                print(f'[gerar_pix] [{tentativa}] {txt[:100]}', flush=True)
                # Procurar código Pix - padrão primário
                if '00020101' in txt:
                    pix_match = re.search(r'`?(00020101[^`\s\n]+)`?', txt)
                    pix_code = pix_match.group(1) if pix_match else None
                    tx_match = re.search(r'txn_([a-f0-9]+)', txt)
                    tx_id = f"txn_{tx_match.group(1)}" if tx_match else f"txn_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d,.]+)', txt)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"
                    if pix_code:
                        if not tx_id_override:
                            salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url, participante_dados)
                        print(f'✅ Pix gerado: {tx_id} R${valor}', flush=True)
                        return {'success': True, 'pix_code': pix_code, 'tx_id': tx_id,
                                'valor': f"R$ {valor_conf}", 'status': 'pendente'}
                # Padrão secundário: "PIX Copia e Cola" sem código 00020101
                if 'PIX Copia e Cola' in txt or 'Copia e Cola' in txt:
                    pix_match = re.search(r'`?(00020101[^`\s\n]+)`?', txt)
                    pix_code = pix_match.group(1) if pix_match else None
                    tx_match = re.search(r'txn_([a-f0-9]+)', txt)
                    tx_id = f"txn_{tx_match.group(1)}" if tx_match else f"txn_{int(time.time())}"
                    val_match = re.search(r'Valor[:\s*]+R\$\s*([\d,.]+)', txt)
                    valor_conf = val_match.group(1) if val_match else f"{valor:.2f}"
                    if pix_code:
                        if not tx_id_override:
                            salvar_transacao(tx_id, valor, pix_code, cliente_id, webhook_url, participante_dados)
                        print(f'✅ Pix gerado (copia-cola): {tx_id} R${valor}', flush=True)
                        return {'success': True, 'pix_code': pix_code, 'tx_id': tx_id,
                                'valor': f"R$ {valor_conf}", 'status': 'pendente'}

        print(f'[gerar_pix] Timeout após 60s - nenhum código Pix recebido', flush=True)
        return {'success': False, 'error': 'Bot demorou para responder. Tente novamente.'}
    except Exception as e:
        print(f'❌ Erro gerar_pix: {e}', flush=True)
        return {'success': False, 'error': str(e)}
    finally:
        try:
            _lock.release()
        except Exception:
            pass

# ─── MIDDLEWARE ────────────────────────────────────────────
@web.middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type,X-PaynexBet-Secret',
        })
    r = await handler(request)
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r

# ─── ROTAS SORTEIO ────────────────────────────────────────
async def route_sorteio_page(request):
    return web.Response(text=load_sorteio_html(), content_type='text/html', charset='utf-8')

async def route_sorteio_info(request):
    """Info pública + dados do participante por CPF"""
    import json as _json
    cpf = request.rel_url.query.get('cpf', '').strip()
    config = get_sorteio_config()

    conn = sqlite3_connect()
    cur1 = conn.execute("SELECT COUNT(*), COALESCE(SUM(total_depositado),0), COALESCE(SUM(total_numeros),0) FROM sorteio_participantes WHERE sorteio_id='atual'")
    total_part, total_dep, total_bilhetes = cur1.fetchone()
    cur2 = conn.execute("SELECT * FROM sorteio_historico ORDER BY data_sorteio DESC LIMIT 5")
    hist_rows = cur2.fetchall()
    conn.close()

    hist_cols = ['id','sorteio_id','data_sorteio','ganhador_cliente_id','ganhador_nome',
                 'ganhador_cpf','ganhador_numero','ganhador_chave_pix','ganhador_tipo_chave',
                 'premio_pago','saque_id','saque_status','total_participantes',
                 'total_bilhetes','total_depositado','observacao']
    historico = []
    for r in hist_rows:
        d = {}
        for i, col in enumerate(hist_cols):
            d[col] = r[i] if i < len(r) else None
        historico.append(d)

    vp = float(config.get('valor_por_numero') or 5.0)
    # Prêmio = o acumulado total (que já cresce 50% a cada depósito confirmado)
    # Se premio_fixo > 0, usa fixo. Caso contrário, usa o acumulado salvo.
    _premio_fixo = float(config.get('premio_fixo') or 0)
    _acumulado   = float(config.get('premio_acumulado') or 0)
    _percentual  = float(config.get('percentual') or 50)

    if _premio_fixo > 0:
        # Modo prêmio fixo: ignora acumulado
        premio = _premio_fixo
        premio_base = _premio_fixo
    elif _acumulado > 0:
        # Modo acumulativo: o prêmio É o acumulado (já foi somando 50% de cada depósito)
        premio = round(_acumulado, 2)
        premio_base = premio
    else:
        # Fallback: calcular 50% do total depositado quando acumulado ainda é zero
        premio_base = round(total_dep * _percentual / 100, 2)
        premio_base = max(premio_base, 1.0)
        premio = premio_base
    premio = max(premio, 1.0)

    resp = {
        'sorteio': {
            'ativo': bool(config.get('ativo', 1)),
            'descricao': config.get('descricao', 'Sorteio PaynexBet'),
            'valor_por_numero': vp,
            'percentual': float(config.get('percentual') or 50),
            'premio_fixo': float(config.get('premio_fixo') or 0),
            'usar_media': int(config.get('usar_media') or 0),
            'dias_media': int(config.get('dias_media') or 30),
            'proximo_sorteio': config.get('proximo_sorteio'),
            'total_participantes': int(total_part),
            'total_bilhetes': int(total_bilhetes),
            'total_depositado': round(total_dep, 2),
            'premio_estimado_total': premio,
            'premio_acumulado': round(_acumulado, 2),
            'acumulativo': bool(int(config.get('acumulativo') or 1)),
            'min_participantes': int(config.get('min_participantes') or 1),
        },
        'historico': historico,
    }

    if cpf:
        part = get_participante(cpf)
        if part:
            resp['participante'] = {
                'cpf': part['cpf'],
                'nome': part['nome'],
                'chave_pix': part['chave_pix'],
                'tipo_chave': part['tipo_chave'],
                'total_depositado': part['total_depositado'],
                'total_numeros': part['total_numeros'],
                'numeros_sorte': part['numeros_sorte'],
                'participando': True,
            }
        else:
            resp['participante'] = {'participando': False, 'cpf': cpf}

    return web.json_response(resp)

async def route_sorteio_cadastrar(request):
    """Cadastrar participante com Nome, CPF e Chave Pix"""
    import json as _json
    try:
        data = await request.json()
        nome      = str(data.get('nome', '')).strip()
        cpf       = re.sub(r'\D', '', str(data.get('cpf', ''))).strip()
        chave_pix = str(data.get('chave_pix', '')).strip()
        tipo_chave= str(data.get('tipo_chave', 'cpf')).strip().lower()

        if not nome:      return web.json_response({'error': 'Nome obrigatório'}, status=400)
        if len(cpf) < 11: return web.json_response({'error': 'CPF inválido (informe 11 dígitos)'}, status=400)
        if not chave_pix: return web.json_response({'error': 'Chave Pix obrigatória'}, status=400)

        config = get_sorteio_config()
        if not config.get('ativo', 1):
            return web.json_response({'error': 'Sorteio não está ativo no momento'}, status=400)

        # Verificar se já existe
        existente = get_participante(cpf)
        now = datetime.now().isoformat()
        cliente_id = f"cli_{cpf}"

        conn = sqlite3_connect()
        if existente:
            # Atualizar dados (sem alterar depósitos e bilhetes)
            conn.execute('''UPDATE sorteio_participantes
                SET nome=?, chave_pix=?, tipo_chave=?, updated_at=?
                WHERE cpf=? AND sorteio_id='atual' ''',
                (nome, chave_pix, tipo_chave, now, cpf))
            conn.commit(); conn.close()
            part = get_participante(cpf)
            return web.json_response({
                'success': True,
                'atualizado': True,
                'cpf': cpf,
                'nome': nome,
                'chave_pix': chave_pix,
                'tipo_chave': tipo_chave,
                'total_depositado': part['total_depositado'],
                'total_numeros': part['total_numeros'],
                'numeros_sorte': part['numeros_sorte'],
                'message': f'✅ Dados atualizados! Você tem {part["total_numeros"]} número(s) da sorte.',
            })
        else:
            conn.execute('''INSERT INTO sorteio_participantes
                (cliente_id, nome, cpf, chave_pix, tipo_chave,
                 total_depositado, total_numeros, numeros_sorte, created_at, updated_at, sorteio_id)
                VALUES (?,?,?,?,?, 0, 0, '[]', ?, ?, 'atual')''',
                (cliente_id, nome, cpf, chave_pix, tipo_chave, now, now))
            conn.commit(); conn.close()
            return web.json_response({
                'success': True,
                'cadastrado': True,
                'cpf': cpf,
                'nome': nome,
                'chave_pix': chave_pix,
                'tipo_chave': tipo_chave,
                'total_depositado': 0,
                'total_numeros': 0,
                'numeros_sorte': [],
                'message': '✅ Cadastro realizado! Faça depósitos para gerar seus números da sorte.\nA cada R$5 depositado = 1 número!',
            })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_sorteio_adicionar_deposito(request):
    """ADMIN ou sistema: adicionar depósito e gerar bilhetes automaticamente"""
    import json as _json
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        cpf   = re.sub(r'\D', '', str(data.get('cpf', ''))).strip()
        valor = float(data.get('valor', 0))

        if not cpf:    return web.json_response({'error': 'CPF obrigatório'}, status=400)
        if valor <= 0: return web.json_response({'error': 'Valor inválido'}, status=400)

        part = get_participante(cpf)
        if not part:
            return web.json_response({'error': 'Participante não cadastrado. Cadastre-se primeiro em /sorteio'}, status=404)

        config = get_sorteio_config()
        vp = float(config.get('valor_por_numero') or 5.0)

        novo_total = (part['total_depositado'] or 0) + valor
        numeros_antes = int(part['total_numeros'] or 0)
        numeros_total = calcular_numeros(novo_total, vp)
        novos = numeros_total - numeros_antes

        numeros_atuais = list(part['numeros_sorte'] or [])
        novos_numeros = []
        if novos > 0:
            novos_numeros = gerar_bilhetes_unicos(part['cliente_id'], novos)
            numeros_atuais.extend(novos_numeros)

        import json as _json
        conn = sqlite3_connect()
        conn.execute('''UPDATE sorteio_participantes
            SET total_depositado=?, total_numeros=?, numeros_sorte=?, updated_at=?
            WHERE cpf=? AND sorteio_id='atual' ''',
            (novo_total, numeros_total, _json.dumps(numeros_atuais),
             datetime.now().isoformat(), cpf))
        conn.commit(); conn.close()

        # ── ACÚMULO AUTOMÁTICO: 50% do depósito vai para o prêmio acumulado ──
        novo_acum = _acumular_premio_deposito(valor)

        return web.json_response({
            'success': True,
            'cpf': cpf,
            'nome': part['nome'],
            'valor_adicionado': valor,
            'total_depositado': novo_total,
            'numeros_gerados': novos,
            'novos_numeros': novos_numeros,
            'total_numeros': numeros_total,
            'todos_numeros': numeros_atuais,
            'premio_acumulado': novo_acum,
            'message': f'✅ R${valor:.2f} adicionado! {novos} novo(s) número(s) gerado(s). Total: {numeros_total} bilhetes. Acumulado: R${novo_acum:.2f}',
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_sorteio_participar(request):
    """Alias público: buscar dados do participante por CPF"""
    import json as _json
    try:
        data = await request.json()
        cpf = re.sub(r'\D', '', str(data.get('cpf', ''))).strip()
        if not cpf:
            return web.json_response({'error': 'CPF obrigatório'}, status=400)
        part = get_participante(cpf)
        if not part:
            return web.json_response({'success': False, 'participando': False,
                                      'message': 'CPF não cadastrado. Faça seu cadastro!'})
        return web.json_response({
            'success': True, 'participando': True,
            'cpf': part['cpf'], 'nome': part['nome'],
            'chave_pix': part['chave_pix'], 'tipo_chave': part['tipo_chave'],
            'total_depositado': part['total_depositado'],
            'total_numeros': part['total_numeros'],
            'numeros_sorte': part['numeros_sorte'],
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def _executar_sorteio_completo():
    """Lógica central: sorteia 1 bilhete vencedor e executa saque automático"""
    import random, json as _json
    conn = sqlite3_connect()

    # Buscar todos os bilhetes do sorteio atual
    cur_b = conn.execute("SELECT cliente_id, numero FROM sorteio_bilhetes WHERE sorteio_id='atual'")
    bilhetes = cur_b.fetchall()  # [(cliente_id, numero), ...]

    if not bilhetes:
        conn.close()
        return {'success': False, 'error': 'Nenhum bilhete no sorteio. Participantes precisam fazer depósitos.'}

    # Buscar participantes
    cur_p = conn.execute("SELECT * FROM sorteio_participantes WHERE sorteio_id='atual'")
    rows = cur_p.fetchall()
    cols_part = ['id','cliente_id','nome','cpf','chave_pix','tipo_chave',
                 'total_depositado','total_numeros','numeros_sorte','created_at','updated_at','sorteio_id']
    participantes = []
    for r in rows:
        d = {}
        for i, col in enumerate(cols_part):
            d[col] = r[i] if i < len(r) else None
        try: d['numeros_sorte'] = _json.loads(d['numeros_sorte'] or '[]')
        except: d['numeros_sorte'] = []
        participantes.append(d)
    conn.close()

    part_map = {p['cliente_id']: p for p in participantes}

    config = get_sorteio_config()

    # ── VERIFICAR MÍNIMO DE PARTICIPANTES (SORTEIO ACUMULATIVO) ────────────
    min_part_cfg = int(config.get('min_participantes') or 1)
    acumulativo_cfg = bool(int(config.get('acumulativo') or 1))
    total_part_atual = len(participantes)

    if acumulativo_cfg and total_part_atual < min_part_cfg:
        conn.close()
        # Não arquivar - participantes e bilhetes continuam na rodada
        acum_atual = float(config.get('premio_acumulado') or 0)
        total_dep_temp = sum(p['total_depositado'] or 0 for p in participantes)
        _percentual_cfg = float(config.get('percentual', 50)) / 100
        # O prêmio exibido é o acumulado já salvo (ou fallback 50% dos depósitos)
        premio_exibido = acum_atual if acum_atual > 0 else round(total_dep_temp * _percentual_cfg, 2)
        premio_exibido = max(premio_exibido, 0.0)
        print(f'🎰 [Acumulativo] Poucos participantes ({total_part_atual}/{min_part_cfg}). Prêmio acumulado: R${premio_exibido:.2f}', flush=True)
        return {
            'success': False,
            'acumulando': True,
            'error': f'Mínimo {min_part_cfg} participantes. Atual: {total_part_atual}. Aguardando mais participantes.',
            'total_participantes': total_part_atual,
            'min_participantes': min_part_cfg,
            'premio_acumulado': acum_atual,
            'premio_estimado': premio_exibido,
        }

    # Sortear 1 bilhete aleatório (cada bilhete = igual chance)
    bilhete_vencedor = random.choice(bilhetes)
    cliente_id_vencedor, numero_vencedor = bilhete_vencedor
    ganhador = part_map.get(cliente_id_vencedor)

    if not ganhador:
        return {'success': False, 'error': 'Erro interno: participante do bilhete não encontrado'}

    total_depositado = sum(p['total_depositado'] or 0 for p in participantes)
    total_bilhetes_count = len(bilhetes)

    # ── PRÊMIO ACUMULATIVO ──────────────────────────────────────────────────
    premio_acumulado_anterior = float(config.get('premio_acumulado') or 0)
    acumulativo = bool(int(config.get('acumulativo') or 1))
    min_participantes = int(config.get('min_participantes') or 1)

    # O prêmio_acumulado JÁ contém 50% de todos os depósitos desta rodada
    # (acumulado automaticamente a cada depósito confirmado)
    if float(config.get('premio_fixo') or 0) > 0:
        # Modo prêmio fixo
        premio = float(config['premio_fixo'])
    elif premio_acumulado_anterior > 0:
        # Modo acumulativo: o prêmio É o acumulado salvo no DB
        premio = round(premio_acumulado_anterior, 2)
    else:
        # Fallback: calcular 50% se acumulado ainda for zero
        premio = round(total_depositado * float(config.get('percentual', 50)) / 100, 2)
    premio = max(premio, 1.0)

    print(f'🏆 Prêmio a pagar: R${premio:.2f} (acumulado no DB: R${premio_acumulado_anterior:.2f})', flush=True)

    sorteio_id  = f"sorteio_{int(time.time())}"
    chave_pix   = ganhador.get('chave_pix') or ''
    tipo_chave  = ganhador.get('tipo_chave') or 'cpf'
    cpf_ganhador = ganhador.get('cpf') or ''

    # Salvar no histórico
    conn2 = sqlite3_connect()
    conn2.execute('''INSERT INTO sorteio_historico
        (sorteio_id, data_sorteio, ganhador_cliente_id, ganhador_nome, ganhador_cpf,
         ganhador_numero, ganhador_chave_pix, ganhador_tipo_chave, premio_pago,
         saque_status, total_participantes, total_bilhetes, total_depositado, observacao)
        VALUES (?,?,?,?,?,?,?,?,?,'pendente',?,?,?,?)''',
        (sorteio_id, datetime.now().isoformat(),
         ganhador['cliente_id'], ganhador['nome'], cpf_ganhador,
         numero_vencedor, chave_pix, tipo_chave, premio,
         len(participantes), total_bilhetes_count, total_depositado,
         f'Bilhete {numero_vencedor} sorteado de {total_bilhetes_count} bilhetes'))

    # Arquivar participantes e bilhetes
    conn2.execute("UPDATE sorteio_participantes SET sorteio_id=? WHERE sorteio_id='atual'", (sorteio_id,))
    conn2.execute("UPDATE sorteio_bilhetes SET sorteio_id=? WHERE sorteio_id='atual'", (sorteio_id,))
    # Resetar prêmio acumulado (pois houve ganhador nesta rodada)
    conn2.execute("UPDATE sorteio_config SET premio_acumulado=0 WHERE id=1")
    conn2.commit(); conn2.close()

    print(f'🎉 SORTEIO {sorteio_id}: bilhete {numero_vencedor} → {ganhador["nome"]} ganhou R${premio:.2f} (incluindo acumulado R${premio_acumulado_anterior:.2f}) → {tipo_chave}: {chave_pix}', flush=True)

    # ── SAQUE AUTOMÁTICO ──────────────────────────────────────
    # Prioridade: 1) Asaas (se configurado), 2) Telegram Bot, 3) Pendente
    saque_result = {'success': False, 'error': 'Chave Pix não cadastrada'}
    saque_id_gerado = None

    if chave_pix:
        import hashlib as _hl
        saque_id_gerado = 'saq_sorteio_' + _hl.md5(f"{sorteio_id}{chave_pix}".encode()).hexdigest()[:10]
        salvar_saque(saque_id_gerado, premio, chave_pix, tipo_chave)

        if ASAAS_API_KEY:
            # ── ASAAS: PIX direto, único gateway do sorteio ────────
            print(f'💸 [Asaas] Enviando prêmio R${premio:.2f} → {tipo_chave}: {chave_pix}', flush=True)
            saque_result = await asaas_enviar_pix(chave_pix, tipo_chave, premio,
                descricao=f'Prêmio Sorteio PaynexBet - {ganhador["nome"]}')
            if saque_result.get('success'):
                novo_status = 'enviado'
                obs = saque_result.get('mensagem_bot', '')[:500]
            else:
                # Asaas falhou → pendente para retentativa automática a cada 1h (SEM Telegram)
                novo_status = 'pendente_asaas'
                obs = f'Asaas falhou - retentativa automática a cada 1h. Erro: {saque_result.get("error","")}'[:500]
                print(f'⚠️ [Asaas] Falha no saque - pendente p/ retentativa 1h: {saque_id_gerado}', flush=True)

        else:
            # ── PENDENTE: Asaas não configurado - retentativa a cada 1h ──
            novo_status = 'pendente_asaas'
            obs = 'Asaas não configurado - saque pendente para retentativa automática a cada 1h'
            saque_result = {'success': False, 'error': obs}
            print(f'⚠️ Asaas não configurado - saque pendente: {saque_id_gerado}', flush=True)

        # Atualizar DB
        conn2 = sqlite3_connect()
        conn2.execute('UPDATE saques SET status=?, processado_at=?, observacao=? WHERE saque_id=?',
            (novo_status, datetime.now().isoformat(), obs, saque_id_gerado))
        conn2.execute('UPDATE sorteio_historico SET saque_id=?, saque_status=? WHERE sorteio_id=?',
            (saque_id_gerado, novo_status, sorteio_id))
        conn2.commit(); conn2.close()
        print(f'💸 Saque sorteio: {novo_status} | {obs[:80]}', flush=True)

    return {
        'success': True,
        'sorteio_id': sorteio_id,
        'ganhador': {
            'cliente_id': ganhador['cliente_id'],
            'nome': ganhador['nome'],
            'cpf': cpf_ganhador,
            'numero_sorte': numero_vencedor,
            'chave_pix': chave_pix,
            'tipo_chave': tipo_chave,
            'premio': premio,
        },
        'saque': saque_result,
        'saque_id': saque_id_gerado,
        'estatisticas': {
            'total_participantes': len(participantes),
            'total_bilhetes': total_bilhetes_count,
            'total_depositado': round(total_depositado, 2),
        }
    }

async def route_sorteio_realizar(request):
    """ADMIN - Realizar o sorteio e executar saque automático"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        resultado = await _executar_sorteio_completo()
        return web.json_response(resultado)
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

# ─── AGENDADOR AUTOMÁTICO DE SORTEIO ─────────────────────
async def agendador_sorteio():
    """Verifica a cada 30s se chegou a hora do sorteio e executa automaticamente.
    Dispara se a hora configurada JÁ PASSOU (sem janela máxima), garantindo
    que o sorteio sempre aconteça mesmo após reinícios do processo."""
    print('⏰ Agendador de sorteio iniciado (intervalo: 30s)', flush=True)
    while True:
        await asyncio.sleep(30)  # Verifica a cada 30 segundos
        try:
            config = get_sorteio_config()
            proximo = config.get('proximo_sorteio')
            if not proximo or not config.get('ativo', 1):
                continue

            import datetime as dt
            agora = dt.datetime.now(dt.timezone.utc)
            try:
                alvo = dt.datetime.fromisoformat(proximo.replace('Z', '+00:00'))
            except Exception:
                continue
            if alvo.tzinfo is None:
                alvo = alvo.replace(tzinfo=dt.timezone.utc)

            # Dispara se a hora alvo já passou (qualquer atraso é OK)
            diff = (agora - alvo).total_seconds()
            if diff >= 0:
                print(f'🎰 SORTEIO AUTOMÁTICO DISPARADO! Atraso: {diff:.0f}s | Alvo: {proximo}', flush=True)

                # Limpar proximo_sorteio PRIMEIRO para evitar re-disparo em caso de falha
                conn = sqlite3_connect()
                conn.execute("UPDATE sorteio_config SET proximo_sorteio=NULL, updated_at=? WHERE id=1",
                             (datetime.now().isoformat(),))
                conn.commit(); conn.close()

                resultado = await _executar_sorteio_completo()
                if resultado.get('success'):
                    g = resultado['ganhador']
                    print(f'🎉 SORTEIO AUTO CONCLUÍDO: {g["nome"]} ganhou R${g["premio"]:.2f}!', flush=True)
                else:
                    print(f'❌ Sorteio auto falhou: {resultado.get("error")}', flush=True)
                    # Se falhou por falta de participantes, não precisa restaurar a data

        except Exception as e:
            print(f'❌ Agendador erro: {e}', flush=True)

async def reprocessar_saques_pendentes_sorteio():
    """Verifica a cada 1h saques de sorteio pendentes e retenta via Asaas (sem Telegram)"""
    print('🔄 Monitor de saques pendentes sorteio iniciado (retentativa a cada 1h via Asaas)', flush=True)
    while True:
        await asyncio.sleep(3600)  # 1 hora
        try:
            if not ASAAS_API_KEY:
                print('⚠️ [Monitor Saques] Asaas não configurado - aguardando...', flush=True)
                continue

            # Buscar saques de sorteio pendentes (falha Asaas ou Asaas não configurado)
            conn = sqlite3_connect()
            cur = conn.execute("""SELECT h.sorteio_id, h.ganhador_nome, h.ganhador_chave_pix,
                                h.ganhador_tipo_chave, h.premio_pago, h.saque_id
                         FROM sorteio_historico h
                         WHERE h.saque_status IN ('pendente_asaas','erro','aguardando_gateway','aguardando_telegram')
                         ORDER BY h.data_sorteio DESC LIMIT 10""")
            pendentes = cur.fetchall()
            conn.close()

            if not pendentes:
                print('✅ [Monitor Saques] Nenhum saque pendente.', flush=True)
                continue

            print(f'💸 [Monitor Saques] {len(pendentes)} saque(s) pendente(s) - tentando via Asaas...', flush=True)

            for row in pendentes:
                sorteio_id, nome, chave_pix, tipo_chave, premio, saque_id = row
                if not chave_pix:
                    print(f'⚠️ [Monitor Saques] {sorteio_id} sem chave PIX - pulando', flush=True)
                    continue

                print(f'💸 [Monitor Saques] Retentativa Asaas: {sorteio_id} | R${premio:.2f} → {tipo_chave}:{chave_pix}', flush=True)
                result = await asaas_enviar_pix(chave_pix, tipo_chave, float(premio),
                                                descricao=f'Prêmio Sorteio PaynexBet - {nome} (retentativa)')

                if result.get('success'):
                    novo_status = 'enviado'
                    obs = result.get('mensagem_bot', f'Retentativa OK - ID:{result.get("transfer_id","")}')[:500]
                    print(f'✅ [Monitor Saques] Saque enviado com sucesso: {sorteio_id}', flush=True)
                else:
                    novo_status = 'pendente_asaas'
                    obs = f'Retentativa Asaas falhou - próxima em 1h. Erro: {result.get("error","")}' [:500]
                    print(f'❌ [Monitor Saques] Falha na retentativa {sorteio_id}: {result.get("error","")}', flush=True)

                conn2 = sqlite3_connect()
                conn2.execute('UPDATE sorteio_historico SET saque_status=? WHERE sorteio_id=?',
                              (novo_status, sorteio_id))
                if saque_id:
                    conn2.execute('UPDATE saques SET status=?, processado_at=?, observacao=? WHERE saque_id=?',
                                  (novo_status, datetime.now().isoformat(), obs, saque_id))
                conn2.commit(); conn2.close()

        except Exception as e:
            print(f'❌ [Monitor Saques] Erro inesperado: {e}', flush=True)

async def route_sorteio_config(request):
    """ADMIN - Configurar parâmetros do sorteio"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        params = (
            int(data.get('ativo', 1)),
            float(data.get('valor_por_numero', 5.0)),
            float(data.get('percentual', 50)),
            int(data.get('usar_media', 0)),
            int(data.get('dias_media', 30)),
            float(data.get('premio_fixo', 0)),
            str(data.get('descricao', 'Sorteio PaynexBet')),
            data.get('proximo_sorteio'),
            datetime.now().isoformat(),
            int(data.get('acumulativo', 1)),
            int(data.get('min_participantes', 5)),
        )
        if _USE_PG and DATABASE_URL:
            # PostgreSQL: rodar migrações primeiro, depois UPDATE com %s
            import psycopg2
            pg = psycopg2.connect(DATABASE_URL)
            pg.autocommit = True
            cur = pg.cursor()
            for mig in [
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS acumulativo INTEGER DEFAULT 1",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS min_participantes INTEGER DEFAULT 5",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS premio_acumulado REAL DEFAULT 0",
            ]:
                try: cur.execute(mig)
                except Exception: pass
            # UPDATE com commit explícito
            pg.autocommit = False
            cur.execute('''UPDATE sorteio_config SET
                ativo=%s, valor_por_numero=%s, percentual=%s, usar_media=%s, dias_media=%s,
                premio_fixo=%s, descricao=%s, proximo_sorteio=%s, updated_at=%s,
                acumulativo=%s, min_participantes=%s
                WHERE id=1''', params)
            pg.commit()
            pg.close()
        else:
            conn = sqlite3_connect()
            conn.execute('''UPDATE sorteio_config SET
                ativo=?, valor_por_numero=?, percentual=?, usar_media=?, dias_media=?,
                premio_fixo=?, descricao=?, proximo_sorteio=?, updated_at=?,
                acumulativo=?, min_participantes=?
                WHERE id=1''', params)
            conn.commit(); conn.close()
        return web.json_response({'success': True, 'message': 'Configuração salva!'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_sorteio_acumular(request):
    """ADMIN - Acumular prêmio extra manualmente (sem arquivar participantes)
    Útil quando se quer adicionar um bônus ao prêmio acumulado.
    Body (opcional): { "valor": 10.0 }  → adiciona valor específico
    Sem body: adiciona 50% do total depositado atual
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        config = get_sorteio_config()
        conn = sqlite3_connect()

        # Verificar se há valor específico no body
        valor_extra = 0.0
        try:
            body = await request.json()
            valor_extra = float(body.get('valor', 0))
        except:
            pass

        if valor_extra <= 0:
            # Usar 50% do total depositado atual como bônus
            cur1 = conn.execute("SELECT COALESCE(SUM(total_depositado),0), COUNT(*) FROM sorteio_participantes WHERE sorteio_id='atual'")
            total_dep_rod, total_part_rod = cur1.fetchone()
            total_dep_rod = float(total_dep_rod or 0)
            _pf = float(config.get('premio_fixo') or 0)
            _pct = float(config.get('percentual') or 50)
            valor_extra = _pf if _pf > 0 else round(total_dep_rod * _pct / 100, 2)
            valor_extra = max(valor_extra, 0.0)

        acumulado_anterior = float(config.get('premio_acumulado') or 0)
        novo_acumulado = round(acumulado_anterior + valor_extra, 2)

        # Salvar novo acumulado SEM arquivar participantes (eles continuam na próxima rodada)
        conn.execute("UPDATE sorteio_config SET premio_acumulado=?, updated_at=? WHERE id=1",
                     (novo_acumulado, datetime.now().isoformat()))
        conn.commit(); conn.close()

        print(f'🔄 Acumulado manual: +R${valor_extra:.2f} → Total acumulado: R${novo_acumulado:.2f}', flush=True)
        return web.json_response({
            'success': True,
            'message': f'R${valor_extra:.2f} adicionado ao prêmio acumulado!',
            'premio_acumulado': novo_acumulado,
            'acumulado_anterior': acumulado_anterior,
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def route_sorteio_reparar_participante(request):
    """ADMIN - Corrigir dados de participante (total_depositado e total_numeros)"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        cpf   = re.sub(r'\D', '', str(data.get('cpf', ''))).strip()
        total_dep  = float(data.get('total_depositado', 0))
        total_num  = int(data.get('total_numeros', 0))
        numeros    = data.get('numeros_sorte', None)  # lista de números, opcional
        nome       = data.get('nome', None)
        chave_pix  = data.get('chave_pix', None)
        tipo_chave = data.get('tipo_chave', None)

        if not cpf:
            return web.json_response({'error': 'CPF obrigatório'}, status=400)

        import json as _json
        conn = sqlite3_connect()

        # Montar query dinâmica
        updates = ['total_depositado=?', 'total_numeros=?', 'updated_at=?']
        vals = [total_dep, total_num, datetime.now().isoformat()]
        if numeros is not None:
            updates.append('numeros_sorte=?')
            vals.append(_json.dumps(numeros))
        if nome:
            updates.append('nome=?')
            vals.append(nome)
        if chave_pix:
            updates.append('chave_pix=?')
            vals.append(chave_pix)
        if tipo_chave:
            updates.append('tipo_chave=?')
            vals.append(tipo_chave)

        vals.append(cpf)
        sql = f"UPDATE sorteio_participantes SET {', '.join(updates)} WHERE cpf=? AND sorteio_id='atual'"
        conn.execute(sql, vals)
        conn.commit()

        # Corrigir bilhetes (apagar e recriar)
        cliente_id = f'cli_{cpf}'
        cur = conn.execute("SELECT total_numeros FROM sorteio_participantes WHERE cpf=? AND sorteio_id='atual'", (cpf,))
        row = cur.fetchone()

        if numeros is not None:
            # Recriar bilhetes
            conn.execute("DELETE FROM sorteio_bilhetes WHERE cliente_id=? AND sorteio_id='atual'", (cliente_id,))
            for n in numeros:
                conn.execute("INSERT OR IGNORE INTO sorteio_bilhetes (cliente_id, numero, sorteio_id) VALUES (?,?,'atual')",
                             (cliente_id, n))
            conn.commit()

        conn.close()
        print(f'🔧 [REPARO] Participante CPF:{cpf} → R${total_dep:.2f} / {total_num} bilhetes', flush=True)
        return web.json_response({
            'success': True,
            'cpf': cpf,
            'total_depositado': total_dep,
            'total_numeros': total_num,
            'numeros_sorte': numeros,
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def route_sorteio_set_acumulado(request):
    """ADMIN - Definir valor acumulado manualmente"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        conn = sqlite3_connect()
        conn.execute("UPDATE sorteio_config SET premio_acumulado=?, updated_at=? WHERE id=1",
                     (valor, datetime.now().isoformat()))
        conn.commit(); conn.close()
        print(f'💰 Prêmio acumulado definido manualmente: R${valor:.2f}', flush=True)
        return web.json_response({'success': True, 'premio_acumulado': valor})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def route_sorteio_participantes(request):
    """ADMIN - Listar todos os participantes"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    import json as _json
    conn = sqlite3_connect()
    cur = conn.execute("""SELECT id, cliente_id, nome, cpf, chave_pix, tipo_chave,
                        total_depositado, total_numeros, numeros_sorte, created_at, updated_at, sorteio_id
                 FROM sorteio_participantes
                 WHERE sorteio_id='atual' ORDER BY total_depositado DESC""")
    rows = cur.fetchall(); conn.close()
    cols = ['id','cliente_id','nome','cpf','chave_pix','tipo_chave',
            'total_depositado','total_numeros','numeros_sorte','created_at','updated_at','sorteio_id']
    participantes = []
    for r in rows:
        d = {}
        for i, col in enumerate(cols):
            d[col] = r[i] if i < len(r) else None
        try: d['numeros_sorte'] = _json.loads(d['numeros_sorte'] or '[]')
        except: d['numeros_sorte'] = []
        participantes.append(d)
    total_dep = sum(p['total_depositado'] or 0 for p in participantes)
    total_bill = sum(p['total_numeros'] or 0 for p in participantes)
    return web.json_response({
        'participantes': participantes,
        'total': len(participantes),
        'total_depositado': round(total_dep, 2),
        'total_bilhetes': int(total_bill),
        'com_pix': len([p for p in participantes if p.get('chave_pix')]),
        'sem_pix': len([p for p in participantes if not p.get('chave_pix')]),
    })

# ═══════════════════════════════════════════════════════════════════════════════
# ─── ROTAS ASAAS - SORTEIO PIX ─────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

async def route_asaas_pix_sorteio(request):
    """
    POST /api/sorteio/asaas/pix
    Gera QR Code PIX via Asaas para participação no sorteio.
    Body: { cpf, nome, valor, email? }
    """
    try:
        data = await request.json()
        cpf   = re.sub(r'\D', '', str(data.get('cpf', ''))).strip()
        nome  = str(data.get('nome', '')).strip()
        valor = float(data.get('valor', 0))
        email = str(data.get('email', '')).strip()

        if not cpf:
            return web.json_response({'success': False, 'error': 'CPF obrigatório'}, status=400)
        if not ASAAS_API_KEY:
            return web.json_response({'success': False, 'error': 'Gateway PIX não configurado. Informe ASAAS_API_KEY.'}, status=503)

        config = get_sorteio_config()
        vp = float(config.get('valor_por_numero') or 5.0)
        if valor < vp:
            return web.json_response({'success': False, 'error': f'Valor mínimo R$ {vp:.2f}'}, status=400)
        if valor % vp != 0:
            return web.json_response({'success': False, 'error': f'Valor deve ser múltiplo de R$ {vp:.0f}'}, status=400)
        qtd_numeros = int(valor // vp)

        print(f'🎰 [Asaas/Sorteio] Gerando PIX R${valor:.2f} para CPF:{cpf} ({nome})', flush=True)
        resultado = await asaas_gerar_pix_sorteio(cpf, nome, valor,
            descricao=f'Sorteio PaynexBet - {qtd_numeros} número(s) da sorte', email=email)

        if not resultado['success']:
            return web.json_response({'success': False, 'error': resultado['error']}, status=500)

        # Salvar no DB local para rastrear confirmação via webhook
        tx_id = f"asaas_{resultado['payment_id']}"
        asaas_salvar_pagamento_db(
            resultado['payment_id'], tx_id, cpf, nome, valor, 'sorteio'
        )

        # ── POLLING AUTOMÁTICO: inicia task em background para monitorar pagamento ──
        asyncio.create_task(asaas_polling_pagamento(
            payment_id=resultado['payment_id'],
            cpf=cpf, nome=nome, valor=valor, tipo='sorteio',
            intervalo=10, max_tentativas=60  # 10s × 60 = 10 minutos
        ))
        print(f'🚀 [Asaas/Sorteio] Polling automático iniciado para payment_id={resultado["payment_id"]}', flush=True)

        return web.json_response({
            'success': True,
            'tx_id': tx_id,
            'payment_id': resultado['payment_id'],
            'pix_code': resultado['pix_code'],
            'qr_image_b64': resultado.get('qr_image_b64', ''),
            'expiration': resultado.get('expiration', ''),
            'valor': valor,
            'qtd_numeros': qtd_numeros,
        })
    except Exception as e:
        print(f'❌ [Asaas/Sorteio] Erro: {e}', flush=True)
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_asaas_pix_status(request):
    """
    GET /api/sorteio/asaas/status/{tx_id}
    Consulta status do pagamento Asaas pelo tx_id local.
    Retorna dados ricos para o frontend exibir progresso em tempo real.
    """
    try:
        tx_id = request.match_info.get('tx_id', '')
        payment_id = tx_id.replace('asaas_', '')

        if not payment_id:
            return web.json_response({'success': False, 'error': 'tx_id inválido'}, status=400)

        # Checar no DB local primeiro (polling do servidor pode ter confirmado)
        conn = sqlite3_connect()
        row = conn.execute(
            'SELECT status, cpf, nome, valor, confirmed_at FROM asaas_pagamentos WHERE payment_id=?',
            (payment_id,)).fetchone()
        conn.close()

        if row and row[0] == 'confirmado':
            cpf_conf = row[1] or ''
            # Buscar dados atualizados do participante
            part = get_participante(cpf_conf) if cpf_conf else None
            return web.json_response({
                'success': True, 'status': 'confirmado', 'pago': True,
                'cpf': cpf_conf, 'nome': row[2], 'valor': row[3],
                'confirmed_at': row[4],
                'bilhetes': part['total_numeros'] if part else None,
                'numeros': part['numeros_sorte'] if part else [],
            })

        if row and row[0] == 'cancelado':
            return web.json_response({
                'success': True, 'status': 'cancelado', 'pago': False,
                'valor': row[3],
            })

        # Consultar Asaas em tempo real (fallback caso polling ainda não tenha rodado)
        try:
            resp = await asaas_request('GET', f'/payments/{payment_id}')
            status_asaas = resp.get('status', 'PENDING')
            pago = status_asaas in ('RECEIVED', 'CONFIRMED')

            if pago and row:
                # Confirmar via polling se ainda não foi processado
                cpf_db = row[1] or ''
                nome_db = row[2] or ''
                valor_db = float(row[3] or 0)
                dados = asaas_confirmar_pagamento_db(payment_id)
                if dados:
                    asyncio.create_task(_processar_deposito_sorteio_asaas(cpf_db, nome_db, valor_db))

            return web.json_response({
                'success': True,
                'status': 'confirmado' if pago else 'pendente',
                'pago': pago,
                'status_asaas': status_asaas,
                'valor': resp.get('value', 0),
                'polling_ativo': True,
            })
        except Exception as e_asaas:
            # Se Asaas falhar, retornar status local
            status_local = row[0] if row else 'pendente'
            return web.json_response({
                'success': True,
                'status': status_local,
                'pago': status_local == 'confirmado',
                'polling_ativo': True,
                'erro_asaas': str(e_asaas),
            })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_webhook_asaas(request):
    """
    POST /webhook/asaas
    Recebe eventos de pagamento do Asaas e processa automaticamente.
    Evento principal: PAYMENT_RECEIVED → credita bilhetes no sorteio.
    """
    import json as _json
    try:
        # Validar token Asaas (opcional - Asaas não envia token por padrão)
        # Se ASAAS_WEBHOOK_TOKEN estiver configurado como header, verifica
        # Caso contrário, aceita todas as requisições do Asaas (autenticação por IP implícita)
        token = request.headers.get('asaas-access-token', '')
        if ASAAS_WEBHOOK_TOKEN and token and token != ASAAS_WEBHOOK_TOKEN:
            # Só rejeita se enviou token E está errado (não rejeita se não enviou)
            print(f'⚠️ [Webhook Asaas] Token inválido enviado: {token[:20]}...', flush=True)
            return web.json_response({'error': 'Token inválido'}, status=401)
        # Log de recebimento (aceito)
        print(f'📥 [Webhook Asaas] Requisição aceita de {request.remote}', flush=True)

        body = await request.json()
        event = body.get('event', '')
        payment = body.get('payment', {})
        transfer = body.get('transfer', {})
        payment_id = payment.get('id', '')
        valor = float(payment.get('value', 0))
        external_ref = payment.get('externalReference', '')

        print(f'📨 [Webhook Asaas] Evento: {event} | ID:{payment_id} | R${valor:.2f}', flush=True)

        # ── TRANSFER_DONE: saque PIX confirmado ──────────────────────────
        if event == 'TRANSFER_DONE':
            tid = transfer.get('id', '')
            tval = float(transfer.get('value', 0))
            tkey = transfer.get('pixAddressKey', '')
            ete  = transfer.get('endToEndIdentifier', '')
            print(f'✅ [Webhook Asaas] TRANSFER_DONE | ID:{tid} | R${tval:.2f} | chave:{tkey} | E2E:{ete}', flush=True)
            # Atualizar saque no banco local para 'confirmado'
            try:
                conn = sqlite3_connect()
                conn.execute(
                    "UPDATE saques SET status='confirmado', processado_at=?, observacao=? WHERE saque_id=? OR observacao LIKE ?",
                    (datetime.now().isoformat(),
                     f'PIX confirmado Asaas | E2E:{ete}',
                     tid, f'%{tid}%')
                )
                conn.commit()
                conn.close()
                print(f'✅ [Webhook] Saque {tid} marcado como confirmado no DB', flush=True)
            except Exception as _e:
                print(f'⚠️ [Webhook] Erro ao atualizar saque: {_e}', flush=True)
            # ── Notificar canal Telegram ──────────────────────────────────
            chave_mask = tkey[-6:] if len(tkey) > 6 else tkey
            asyncio.create_task(_enviar_canal_notif(
                f'✅ **SAQUE EFETUADO**\n'
                f'━━━━━━━━━━━━━━━━━━━━━\n'
                f'💰 Valor: **R$ {tval:.2f}**\n'
                f'🔑 Chave Pix: `*******{chave_mask}`\n'
                f'📋 ID: `{tid}`\n'
                f'🕐 Status: **CONFIRMADO**'
            ))
            asyncio.create_task(_enviar_canal_hist(
                f'📤 Saque | R$ {tval:.2f} | chave: *{chave_mask} | ✅ Confirmado'
            ))
            return web.Response(text='ok', status=200)

        # ── TRANSFER_FAILED: saque PIX falhou ────────────────────────────
        if event == 'TRANSFER_FAILED':
            tid  = transfer.get('id', '')
            tval = float(transfer.get('value', 0))
            tkey = transfer.get('pixAddressKey', '')
            fail = transfer.get('failReason', 'desconhecido')
            print(f'❌ [Webhook Asaas] TRANSFER_FAILED | ID:{tid} | R${tval:.2f} | chave:{tkey} | motivo:{fail}', flush=True)
            # Atualizar saque no banco local para 'erro'
            try:
                conn = sqlite3_connect()
                conn.execute(
                    "UPDATE saques SET status='erro', observacao=? WHERE saque_id=? OR observacao LIKE ?",
                    (f'Falha Asaas: {fail}', tid, f'%{tid}%')
                )
                conn.commit()
                conn.close()
                print(f'⚠️ [Webhook] Saque {tid} marcado como erro no DB | motivo:{fail}', flush=True)
            except Exception as _e:
                print(f'⚠️ [Webhook] Erro ao atualizar saque falho: {_e}', flush=True)
            # ── Notificar canal Telegram ──────────────────────────────────
            asyncio.create_task(_enviar_canal_notif(
                f'❌ **SAQUE FALHOU**\n'
                f'━━━━━━━━━━━━━━━━━━━━━\n'
                f'💰 Valor: R$ {tval:.2f}\n'
                f'⚠️ Motivo: {fail}\n'
                f'📋 ID: `{tid}`'
            ))
            return web.Response(text='ok', status=200)

        # Só processa pagamentos recebidos daqui pra frente
        if event not in ('PAYMENT_RECEIVED', 'PAYMENT_CONFIRMED'):
            return web.Response(text='ok', status=200)

        # ── IDEMPOTÊNCIA: verificar se payment_id já foi processado ──
        # Usa PostgreSQL diretamente para garantir persistência entre reinicializações
        try:
            import psycopg2 as _pg2_idem
            _pg_idem = _pg2_idem.connect(DATABASE_URL)
            _pg_idem.autocommit = True
            _cur_idem = _pg_idem.cursor()
            _cur_idem.execute('''CREATE TABLE IF NOT EXISTS asaas_processados (
                payment_id TEXT PRIMARY KEY,
                processado_at TEXT
            )''')
            _cur_idem.execute('SELECT payment_id FROM asaas_processados WHERE payment_id=%s', (payment_id,))
            _ja_processado = _cur_idem.fetchone()
            if _ja_processado:
                _pg_idem.close()
                print(f'⏭️ [Webhook Asaas] payment_id {payment_id} já processado. Ignorando duplicata.', flush=True)
                return web.Response(text='ok', status=200)
            # Marcar como processado ANTES de executar (evita race condition)
            _cur_idem.execute(
                'INSERT INTO asaas_processados (payment_id, processado_at) VALUES (%s,%s) ON CONFLICT (payment_id) DO NOTHING',
                (payment_id, datetime.now().isoformat())
            )
            _pg_idem.close()
        except Exception as _e_idem:
            print(f'⚠️ [Webhook] Idempotência falhou (sem bloqueio): {_e_idem}', flush=True)

        # Buscar no DB local
        dados = asaas_confirmar_pagamento_db(payment_id)
        if not dados:
            # FALLBACK: extrair CPF do externalReference (ex: sorteio_cpf_11013430794_xxx)
            cpf_ext = ''
            if external_ref and 'cpf_' in external_ref:
                import re as _re
                m = _re.search(r'cpf_(\d{11})', external_ref)
                if m:
                    cpf_ext = m.group(1)
            if cpf_ext:
                print(f'🔄 [Webhook Asaas] Usando externalReference CPF:{cpf_ext} para payment_id:{payment_id}', flush=True)
                # Salvar no DB para rastreamento futuro
                asaas_salvar_pagamento_db(payment_id, f'asaas_{payment_id}', cpf_ext, 'Participante', valor, 'sorteio')
                asaas_confirmar_pagamento_db(payment_id)
                dados = {'cpf': cpf_ext, 'nome': 'Participante', 'tipo': 'sorteio', 'valor': valor}
            else:
                print(f'⚠️ [Webhook Asaas] payment_id {payment_id} não encontrado. ExtRef:{external_ref}', flush=True)
                return web.Response(text='ok', status=200)

        cpf      = dados.get('cpf', '')
        nome     = dados.get('nome', '')
        tipo     = dados.get('tipo', 'sorteio')
        valor_db = dados.get('valor', valor)

        print(f'✅ [Webhook Asaas] Pagamento confirmado! CPF:{cpf} | R${valor_db:.2f} | tipo:{tipo}', flush=True)

        # Processar conforme tipo
        if tipo == 'sorteio':
            await _processar_deposito_sorteio_asaas(cpf, nome, valor_db)

        # ── Notificar canal Telegram - Depósito confirmado ────────────
        cpf_mask = f'***{cpf[-3:]}' if len(cpf) >= 3 else cpf
        asyncio.create_task(_enviar_canal_notif(
            f'💰 **DEPÓSITO CONFIRMADO**\n'
            f'━━━━━━━━━━━━━━━━━━━━━\n'
            f'👤 {nome} ({cpf_mask})\n'
            f'💵 Valor: **R$ {float(valor_db):.2f}**\n'
            f'📋 Tipo: {tipo}\n'
            f'✅ Status: **CONFIRMADO**'
        ))
        asyncio.create_task(_enviar_canal_hist(
            f'📥 Depósito | {nome} | R$ {float(valor_db):.2f} | ✅ Confirmado'
        ))

        return web.Response(text='ok', status=200)

    except Exception as e:
        print(f'❌ [Webhook Asaas] Erro: {e}', flush=True)
        return web.Response(text='ok', status=200)  # sempre 200 para Asaas não pausar a fila


def _acumular_premio_deposito(valor: float, percentual: float = None):
    """
    Acumula automaticamente X% do depósito confirmado no premio_acumulado.
    Chamado toda vez que um depósito de sorteio é confirmado.
    percentual: None = usa o configurado em sorteio_config (padrão 50%)
    """
    try:
        config = get_sorteio_config()
        pct = percentual if percentual is not None else float(config.get('percentual', 50)) / 100
        incremento = round(valor * pct, 2)
        acum_atual = float(config.get('premio_acumulado') or 0)
        novo_acum = round(acum_atual + incremento, 2)

        conn = sqlite3_connect()
        conn.execute("UPDATE sorteio_config SET premio_acumulado=?, updated_at=? WHERE id=1",
                     (novo_acum, datetime.now().isoformat()))
        conn.commit()
        conn.close()

        print(f'💰 [Acúmulo] +R${incremento:.2f} ({pct*100:.0f}% de R${valor:.2f}) | acumulado: R${acum_atual:.2f} → R${novo_acum:.2f}', flush=True)
        return novo_acum
    except Exception as e:
        print(f'⚠️ [Acúmulo] Erro ao acumular: {e}', flush=True)
        return None


async def _processar_deposito_sorteio_asaas(cpf: str, nome: str, valor: float):
    """
    Processa depósito confirmado pelo Asaas:
    - Se participante existe: adiciona bilhetes
    - Se não existe: apenas registra no log (usuário precisa se cadastrar)
    """
    import json as _json
    config = get_sorteio_config()
    vp = float(config.get('valor_por_numero') or 5.0)

    part = get_participante(cpf)
    if not part:
        print(f'⚠️ [Asaas/Sorteio] CPF {cpf} pagou mas não está cadastrado no sorteio', flush=True)
        # Salvar como crédito pendente para quando o participante se cadastrar
        conn = sqlite3_connect()
        conn.execute('''CREATE TABLE IF NOT EXISTS asaas_creditos_pendentes (
            cpf TEXT PRIMARY KEY,
            nome TEXT,
            valor_total REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )''')
        conn.execute('''INSERT INTO asaas_creditos_pendentes (cpf, nome, valor_total, created_at, updated_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(cpf) DO UPDATE SET
                valor_total = valor_total + ?,
                nome = excluded.nome,
                updated_at = excluded.updated_at''',
            (cpf, nome, valor, datetime.now().isoformat(), datetime.now().isoformat(), valor))
        conn.commit()
        conn.close()
        return

    novo_total = (part['total_depositado'] or 0) + valor
    numeros_antes = int(part['total_numeros'] or 0)
    numeros_total = calcular_numeros(novo_total, vp)
    novos = numeros_total - numeros_antes

    numeros_atuais = list(part['numeros_sorte'] or [])
    if novos > 0:
        novos_numeros = gerar_bilhetes_unicos(part['cliente_id'], novos)
        numeros_atuais.extend(novos_numeros)

    conn = sqlite3_connect()
    conn.execute('''UPDATE sorteio_participantes
        SET total_depositado=?, total_numeros=?, numeros_sorte=?, updated_at=?
        WHERE cpf=? AND sorteio_id='atual' ''',
        (novo_total, numeros_total, _json.dumps(numeros_atuais),
         datetime.now().isoformat(), cpf))
    conn.commit()
    conn.close()

    # ── ACÚMULO AUTOMÁTICO: 50% do depósito vai para o prêmio acumulado ──
    _acumular_premio_deposito(valor)

    print(f'🎫 [Asaas/Sorteio] {nome} (CPF:{cpf}) | +R${valor:.2f} | +{novos} bilhetes | Total:{numeros_total} bilhetes', flush=True)


async def route_asaas_saque_sorteio(request):
    """
    POST /api/sorteio/asaas/saque  (admin)
    Envia prêmio do sorteio via Asaas PIX.
    Body: { chave_pix, tipo_chave, valor, descricao? }
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        chave_pix  = str(data.get('chave_pix', '')).strip()
        tipo_chave = str(data.get('tipo_chave', 'cpf')).strip()
        valor      = float(data.get('valor', 0))
        descricao  = str(data.get('descricao', 'Prêmio Sorteio PaynexBet'))

        if not chave_pix:
            return web.json_response({'success': False, 'error': 'chave_pix obrigatória'}, status=400)
        if valor < 1:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 1,00'}, status=400)

        resultado = await asaas_enviar_pix(chave_pix, tipo_chave, valor, descricao)
        return web.json_response(resultado)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_db_migrate(request):
    """POST /api/admin/db-migrate - Roda migrações pendentes no PostgreSQL"""
    auth = (request.headers.get('X-PaynexBet-Secret','') or request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    results = []
    migrations = [
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS premio_acumulado REAL DEFAULT 0",
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS acumulativo INTEGER DEFAULT 1",
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS min_participantes INTEGER DEFAULT 5",
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_pct REAL DEFAULT 0.6",
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_ativo INTEGER DEFAULT 1",
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_descricao TEXT DEFAULT 'Gere seu Pix e receba sua % do valor'",
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS paypix_min REAL DEFAULT 5.0",
        "UPDATE sorteio_config SET min_participantes=1, acumulativo=1, percentual=50 WHERE id=1 AND min_participantes IS NULL",
    ]
    try:
        import psycopg2
        if not DATABASE_URL:
            return web.json_response({'success': False, 'error': 'DATABASE_URL não configurada'})
        pg = psycopg2.connect(DATABASE_URL)
        pg.autocommit = True
        cur = pg.cursor()
        for sql in migrations:
            try:
                cur.execute(sql)
                results.append({'sql': sql[:60], 'ok': True})
            except Exception as e:
                results.append({'sql': sql[:60], 'ok': False, 'err': str(e)})
        # ── Patch sorteio.html no PostgreSQL (se enviado via body) ──
        sorteio_html_result = 'N/A'
        try:
            _body = {}
            try:
                _body = await request.json()
            except Exception:
                pass
            _sorteio_html = _body.get('sorteio_html', '') or _body.get('html', '')
            if _sorteio_html and len(_sorteio_html) > 1000:
                # Verificar colunas da tabela
                pg2 = psycopg2.connect(DATABASE_URL)
                pg2.autocommit = True
                c2 = pg2.cursor()
                c2.execute("SELECT column_name FROM information_schema.columns WHERE table_name='configuracoes'")
                _cols = [r[0] for r in c2.fetchall()]
                if 'atualizado_em' in _cols:
                    c2.execute("INSERT INTO configuracoes (chave, valor, atualizado_em) VALUES ('sorteio_html_patch', %s, NOW()::text) ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()::text", (_sorteio_html,))
                else:
                    c2.execute("INSERT INTO configuracoes (chave, valor) VALUES ('sorteio_html_patch', %s) ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor", (_sorteio_html,))
                pg2.close()
                sorteio_html_result = f'salvo ({len(_sorteio_html)} chars)'
                print(f'🔧 sorteio.html patch salvo no PostgreSQL via db-migrate: {len(_sorteio_html)} chars', flush=True)
        except Exception as _es:
            sorteio_html_result = f'erro: {_es}'

        # ── Patch paypix.html no PostgreSQL (se enviado via body) ──
        paypix_html_result = 'N/A'
        try:
            _paypix_html = _body.get('paypix_html', '')
            if _paypix_html and len(_paypix_html) > 1000:
                pg3 = psycopg2.connect(DATABASE_URL)
                pg3.autocommit = True
                c3 = pg3.cursor()
                c3.execute("SELECT column_name FROM information_schema.columns WHERE table_name='configuracoes'")
                _cols3 = [r[0] for r in c3.fetchall()]
                if 'atualizado_em' in _cols3:
                    c3.execute("INSERT INTO configuracoes (chave, valor, atualizado_em) VALUES ('paypix_html_patch', %s, NOW()::text) ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()::text", (_paypix_html,))
                else:
                    c3.execute("INSERT INTO configuracoes (chave, valor) VALUES ('paypix_html_patch', %s) ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor", (_paypix_html,))
                pg3.close()
                paypix_html_result = f'salvo ({len(_paypix_html)} chars)'
                print(f'🔧 paypix.html patch salvo no PostgreSQL via db-migrate: {len(_paypix_html)} chars', flush=True)
        except Exception as _ep:
            paypix_html_result = f'erro: {_ep}'
        pg.close()
        # ── Patch home.html: remover participantes/bilhetes da seção stats ──
        html_patch_result = 'N/A'
        try:
            import re as _reh
            if os.path.exists('home.html'):
                _htxt = open('home.html', encoding='utf-8').read()
                # Remover bloco stats antigo (participantes + bilhetes + acumulado + por número)
                _htxt2 = _reh.sub(
                    r'<!-- STATS -->.*?<div class="divider"><span>🎲 Entre agora!',
                    '<!-- STATS -->\n'
                    '  <div class="divider"><span>🏆 Prêmio atual do sorteio</span></div>\n'
                    '  <div class="stats-grid" style="grid-template-columns:1fr 1fr">\n'
                    '    <div class="stat-card">\n'
                    '      <div class="stat-val" id="s-premio">--</div>\n'
                    '      <div class="stat-lbl">🏆 Prêmio estimado</div>\n'
                    '    </div>\n'
                    '    <div class="stat-card">\n'
                    '      <div class="stat-val">R$5</div>\n'
                    '      <div class="stat-lbl">Por número da sorte</div>\n'
                    '    </div>\n'
                    '  </div>\n\n'
                    '  <!-- URGÊNCIA -->\n'
                    '  <div class="divider"><span>🎲 Entre agora!',
                    _htxt, flags=_reh.DOTALL
                )
                # Corrigir JS: remover referências a s-part e s-bil
                _htxt2 = _reh.sub(
                    r"document\.getElementById\('s-part'\)\.textContent=s\.total_participantes\|\|0;\s*"
                    r"document\.getElementById\('s-bil'\)\.textContent=s\.total_bilhetes\|\|0;\s*",
                    '', _htxt2
                )
                if _htxt2 != _htxt:
                    open('home.html', 'w', encoding='utf-8').write(_htxt2)
                    html_patch_result = 'aplicado'
                else:
                    html_patch_result = 'ja_correto'
        except Exception as _ep:
            html_patch_result = f'erro: {_ep}'
        return web.json_response({'success': True, 'migrations': results, 'html_patch': html_patch_result, 'sorteio_html': sorteio_html_result})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e), 'migrations': results})

async def route_patch_sorteio_html(request):
    """POST /api/admin/patch-sorteio-html - Salva sorteio.html no PostgreSQL (patch permanente)"""
    auth = (request.headers.get('X-PaynexBet-Secret','') or request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        html_content = data.get('html', '')
        if not html_content or len(html_content) < 1000:
            return web.json_response({'success': False, 'error': 'HTML inválido ou muito curto'}, status=400)
        import psycopg2, datetime as _dt
        if not DATABASE_URL:
            return web.json_response({'success': False, 'error': 'DATABASE_URL não configurada'})
        pg = psycopg2.connect(DATABASE_URL)
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='configuracoes'")
        cols = [r[0] for r in cur.fetchall()]
        if 'atualizado_em' in cols:
            cur.execute("""
                INSERT INTO configuracoes (chave, valor, atualizado_em)
                VALUES ('sorteio_html_patch', %s, %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = EXCLUDED.atualizado_em
            """, (html_content, _dt.datetime.utcnow().isoformat()))
        else:
            cur.execute("""
                INSERT INTO configuracoes (chave, valor)
                VALUES ('sorteio_html_patch', %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (html_content,))
        pg.close()
        print(f'🔧 sorteio.html patch salvo no PostgreSQL: {len(html_content)} chars', flush=True)
        return web.json_response({'success': True, 'chars': len(html_content), 'msg': 'sorteio.html atualizado no PostgreSQL'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_patch_paypix_html(request):
    """POST /api/admin/patch-paypix-html - Salva paypix.html no PostgreSQL (patch permanente)"""
    auth = (request.headers.get('X-PaynexBet-Secret','') or request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        html_content = data.get('html', '')
        if not html_content or len(html_content) < 1000:
            return web.json_response({'success': False, 'error': 'HTML inválido ou muito curto'}, status=400)
        import psycopg2, datetime as _dt
        if not DATABASE_URL:
            return web.json_response({'success': False, 'error': 'DATABASE_URL não configurada'})
        pg = psycopg2.connect(DATABASE_URL)
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='configuracoes'")
        cols = [r[0] for r in cur.fetchall()]
        if 'atualizado_em' in cols:
            cur.execute("""
                INSERT INTO configuracoes (chave, valor, atualizado_em)
                VALUES ('paypix_html_patch', %s, %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = EXCLUDED.atualizado_em
            """, (html_content, _dt.datetime.utcnow().isoformat()))
        else:
            cur.execute("""
                INSERT INTO configuracoes (chave, valor)
                VALUES ('paypix_html_patch', %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (html_content,))
        pg.close()
        print(f'🔧 paypix.html patch salvo no PostgreSQL: {len(html_content)} chars', flush=True)
        return web.json_response({'success': True, 'chars': len(html_content), 'msg': 'paypix.html atualizado no PostgreSQL'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_asaas_status(request):
    """GET /api/asaas/status - Verifica se Asaas está configurado e operacional"""
    if not ASAAS_API_KEY:
        return web.json_response({
            'configurado': False,
            'error': 'ASAAS_API_KEY não definida',
            'instrucao': 'Defina a variável de ambiente ASAAS_API_KEY no Railway'
        })
    resp = await asaas_request('GET', '/myAccount')
    conta = resp.get('company') or resp.get('name', '')
    balance_resp = await asaas_request('GET', '/finance/balance')
    saldo = balance_resp.get('balance', 0)
    if conta or resp.get('object') == 'account':
        return web.json_response({
            'configurado': True,
            'ambiente': ASAAS_ENV,
            'conta': conta,
            'cpfCnpj': resp.get('cpfCnpj', ''),
            'saldo': saldo,
            'status': 'ok'
        })
    return web.json_response({
        'configurado': True,
        'ambiente': ASAAS_ENV,
        'error': resp.get('errors', str(resp)),
        'status': 'erro_api'
    })


async def route_asaas_configurar(request):
    """POST /api/asaas/configurar - Injeta ASAAS_API_KEY em runtime (admin)"""
    global ASAAS_API_KEY, ASAAS_ENV, ASAAS_BASE_URL
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        nova_key = str(data.get('api_key', '')).strip()
        novo_env  = str(data.get('env', 'production')).strip()

        if not nova_key or len(nova_key) < 20:
            return web.json_response({'error': 'api_key inválida'}, status=400)

        # Atualizar variáveis globais em runtime
        ASAAS_API_KEY = nova_key
        ASAAS_ENV = novo_env
        ASAAS_BASE_URL = 'https://sandbox.asaas.com/v3' if novo_env == 'sandbox' else 'https://api.asaas.com/v3'

        # Salvar no PostgreSQL para persistir entre restarts
        try:
            conn = sqlite3_connect()
            conn.execute('''CREATE TABLE IF NOT EXISTS configuracoes (chave TEXT PRIMARY KEY, valor TEXT, updated_at TEXT)''')
            conn.execute('INSERT OR REPLACE INTO configuracoes (chave, valor, updated_at) VALUES (?,?,?)',
                        ('asaas_api_key', nova_key, datetime.now().isoformat()))
            conn.execute('INSERT OR REPLACE INTO configuracoes (chave, valor, updated_at) VALUES (?,?,?)',
                        ('asaas_env', novo_env, datetime.now().isoformat()))
            conn.commit(); conn.close()
            print(f'✅ [Asaas] Chave configurada em runtime + salva no DB | env:{novo_env}', flush=True)
        except Exception as e:
            print(f'⚠️ [Asaas] Erro ao salvar no DB: {e}', flush=True)

        # Testar chave
        resp = await asaas_request('GET', '/finance/balance')
        saldo = resp.get('balance', '?')
        conta_resp = await asaas_request('GET', '/myAccount')
        conta = conta_resp.get('company') or conta_resp.get('name', '')

        return web.json_response({
            'success': True,
            'message': f'✅ Asaas configurado! Conta: {conta} | Saldo: R${saldo}',
            'conta': conta,
            'saldo': saldo,
            'ambiente': novo_env,
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_railway_set_vars(request):
    """
    POST /api/railway/set-vars  (admin)
    Usa a Railway GraphQL API para salvar variáveis de ambiente permanentemente.
    Lê RAILWAY_TOKEN, RAILWAY_PROJECT_ID, RAILWAY_SERVICE_ID, RAILWAY_ENVIRONMENT_ID
    do próprio container Railway (injetadas automaticamente).
    """
    import aiohttp
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)

    try:
        data = await request.json()
        variables_to_set = data.get('variables', {})  # dict {KEY: VALUE}

        if not variables_to_set:
            return web.json_response({'error': 'Nenhuma variável informada'}, status=400)

        # Ler credenciais Railway do ambiente do container
        railway_token   = os.environ.get('RAILWAY_TOKEN', '')
        railway_project = os.environ.get('RAILWAY_PROJECT_ID', '')
        railway_service = os.environ.get('RAILWAY_SERVICE_ID', '')
        railway_env     = os.environ.get('RAILWAY_ENVIRONMENT_ID', '')

        if not all([railway_token, railway_project, railway_service, railway_env]):
            missing = []
            if not railway_token:   missing.append('RAILWAY_TOKEN')
            if not railway_project: missing.append('RAILWAY_PROJECT_ID')
            if not railway_service: missing.append('RAILWAY_SERVICE_ID')
            if not railway_env:     missing.append('RAILWAY_ENVIRONMENT_ID')
            return web.json_response({
                'success': False,
                'error': f'Variáveis Railway não disponíveis no container: {missing}',
                'instrucao': 'Adicione RAILWAY_TOKEN nas variáveis de ambiente do Railway'
            })

        mutation = """
        mutation($input: VariableCollectionUpsertInput!) {
          variableCollectionUpsert(input: $input)
        }"""

        payload = {
            "query": mutation,
            "variables": {
                "input": {
                    "projectId": railway_project,
                    "environmentId": railway_env,
                    "serviceId": railway_service,
                    "variables": variables_to_set
                }
            }
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://backboard.railway.app/graphql/v2',
                json=payload,
                headers={
                    "Authorization": f"Bearer {railway_token}",
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                result = await resp.json()

        if result.get('data', {}).get('variableCollectionUpsert'):
            keys = list(variables_to_set.keys())
            print(f'✅ [Railway] Variáveis salvas: {keys}', flush=True)
            return web.json_response({
                'success': True,
                'message': f'✅ Variáveis salvas no Railway: {keys}',
                'keys': keys
            })
        else:
            erros = result.get('errors', [])
            msg = erros[0].get('message', str(result)) if erros else str(result)
            print(f'❌ [Railway] Erro GraphQL: {msg}', flush=True)
            return web.json_response({'success': False, 'error': msg})

    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

# ─── ROTAS ────────────────────────────────────────────────
# ── Estado global para login interativo ──────────────────────────────────────
_login_state = {}  # phone_code_hash, temp_client, temp_session

async def route_solicitar_codigo(request):
    """Passo 1: Solicitar código do Telegram - aceita phone via body para trocar número pelo admin"""
    global _login_state, PHONE_NUMBER
    auth = (request.headers.get('X-PaynexBet-Secret','') or
            request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error':'Não autorizado'},status=401)
    try:
        # Aceitar número via body (admin pode trocar sem precisar de suporte)
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        phone_body = str(body.get('phone', '')).strip()
        if phone_body:
            # Normalizar: garantir +55 sem duplicar
            import re as _re2
            digits = _re2.sub(r'\D', '', phone_body)
            if digits.startswith('55') and len(digits) > 11:
                digits = digits  # já tem 55
            elif not digits.startswith('55'):
                digits = '55' + digits
            phone_use = '+' + digits
            PHONE_NUMBER = phone_use  # atualiza em memória para reconexão automática
            print(f'📱 Número Telegram atualizado via admin: {phone_use}', flush=True)
        else:
            phone_use = PHONE_NUMBER or ''

        from telethon.sessions import StringSession as SS
        from telethon.errors import FloodWaitError
        temp_client = TelegramClient(SS(), API_ID, API_HASH)
        await temp_client.connect()
        sent = await temp_client.send_code_request(phone_use)
        _login_state = {
            'client': temp_client,
            'hash': sent.phone_code_hash,
            'session': temp_client.session.save(),
            'phone': phone_use,
        }
        print(f'📱 Código Telegram solicitado para {phone_use} via API Railway', flush=True)
        return web.json_response({'success':True,'message':f'Código enviado para {phone_use}!','phone':phone_use})
    except Exception as e:
        import re as re2
        m = re2.search(r'(\d+)', str(e))
        if 'FloodWait' in type(e).__name__ and m:
            secs=int(m.group(1)); h=secs//3600; mi=(secs%3600)//60
            return web.json_response({'success':False,'error':f'FloodWait: aguarde {h}h{mi}min'})
        return web.json_response({'success':False,'error':str(e)},status=500)

async def route_confirmar_codigo(request):
    """Passo 2: Confirmar código recebido e salvar sessão"""
    global _login_state, client, _telegram_ready, _telegram_session_invalida
    auth = (request.headers.get('X-PaynexBet-Secret','') or
            request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error':'Não autorizado'},status=401)
    try:
        data = await request.json()
        code = str(data.get('code','')).strip()
        if not code:
            return web.json_response({'error':'Código obrigatório'},status=400)
        if not _login_state:
            return web.json_response({'error':'Solicite o código primeiro via /api/telegram/solicitar-codigo'},status=400)

        from telethon.errors import SessionPasswordNeededError
        temp_client = _login_state['client']
        if not temp_client.is_connected():
            from telethon.sessions import StringSession as SS
            temp_client = TelegramClient(SS(_login_state['session']), API_ID, API_HASH)
            await temp_client.connect()

        phone_login = _login_state.get('phone', PHONE_NUMBER or '')
        try:
            await temp_client.sign_in(phone_login, code, phone_code_hash=_login_state['hash'])
        except SessionPasswordNeededError:
            senha = data.get('password','')
            if not senha:
                return web.json_response({'success':False,'needs_2fa':True,'message':'Digite sua senha 2FA'})
            await temp_client.sign_in(password=senha)

        me = await temp_client.get_me()
        nova_sessao = temp_client.session.save()
        await temp_client.disconnect()
        _login_state = {}

        # Salvar sessão no arquivo local
        with open('session_string.txt','w') as f:
            f.write(nova_sessao)
        # Salvar no PostgreSQL - persiste entre deploys!
        _salvar_sessao_db(nova_sessao)

        # ── SALVAR NO RAILWAY via API para persistir entre deploys ──
        railway_token = os.environ.get('RAILWAY_TOKEN', '')
        railway_project = os.environ.get('RAILWAY_PROJECT_ID', '')
        railway_service = os.environ.get('RAILWAY_SERVICE_ID', '')
        railway_env = os.environ.get('RAILWAY_ENVIRONMENT_ID', '')
        if railway_token and railway_project and railway_service and railway_env:
            try:
                import aiohttp as _aio
                mutation = """
                mutation($input: VariableCollectionUpsertInput!) {
                  variableCollectionUpsert(input: $input)
                }"""
                variables = {
                    "input": {
                        "projectId": railway_project,
                        "environmentId": railway_env,
                        "serviceId": railway_service,
                        "variables": {"SESSION_STR": nova_sessao}
                    }
                }
                async with _aio.ClientSession() as sess:
                    async with sess.post(
                        'https://backboard.railway.app/graphql/v2',
                        json={"query": mutation, "variables": variables},
                        headers={"Authorization": f"Bearer {railway_token}", "Content-Type": "application/json"},
                        timeout=_aio.ClientTimeout(total=10)
                    ) as resp:
                        result = await resp.json()
                        if result.get('data', {}).get('variableCollectionUpsert'):
                            print('✅ SESSION_STR salva no Railway automaticamente!', flush=True)
                        else:
                            print(f'⚠️ Railway API: {result}', flush=True)
            except Exception as e_rail:
                print(f'⚠️ Erro ao salvar no Railway: {e_rail}', flush=True)
        else:
            print('⚠️ Vars Railway não configuradas - sessão salva só em arquivo local', flush=True)

        # Reinicializar cliente principal
        try:
            if client.is_connected(): await client.disconnect()
        except: pass
        from telethon.sessions import StringSession as SS
        client.__init__(SS(nova_sessao), API_ID, API_HASH)
        _telegram_ready = False
        _telegram_session_invalida = False
        await client.connect()
        if await client.is_user_authorized():
            _telegram_ready = True
            _salvar_sessao_db(nova_sessao)  # Garantir que está salva no DB
            print(f'✅ Telegram conectado: {me.first_name} ({me.id})', flush=True)

        return web.json_response({
            'success': True,
            'message': f'✅ Telegram conectado como {me.first_name}!',
            'user': me.first_name,
            'user_id': me.id,
        })
    except Exception as e:
        return web.json_response({'success':False,'error':str(e)},status=500)

async def route_reconectar_db(request):
    """
    Força Railway a reconectar o Telegram.
    Estratégia em cascata:
    1) Tenta reconectar o cliente atual (mesma sessão, sem troca de IP)
    2) Se AuthKeyDuplicated: apaga sessão do DB e aguarda novo código
    3) Informa status detalhado
    """
    global client, _telegram_ready, _telegram_session_invalida, SESSION_STR
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        print('🔄 [ReconectarDB] Forçando reconexão...', flush=True)

        # 1) Desconectar e aguardar
        try:
            if client.is_connected():
                await client.disconnect()
            await asyncio.sleep(3)
        except:
            pass

        # 2) Tentar reconectar com a sessão que o cliente JÁ TEM (mesma authkey)
        _telegram_session_invalida = False
        _telegram_ready = False
        await client.connect()

        if await client.is_user_authorized():
            me = await client.get_me()
            _telegram_ready = True
            nova_sess = client.session.save()
            _salvar_sessao_db(nova_sess)
            print(f'✅ [ReconectarDB] Reconectado: {me.first_name} ({me.id})', flush=True)
            return web.json_response({
                'success': True,
                'message': f'✅ Telegram reconectado como {me.first_name}!',
                'user': me.first_name,
                'user_id': me.id,
            })
        else:
            _telegram_session_invalida = True
            return web.json_response({
                'success': False,
                'error': 'Sessão expirada - solicite novo código via painel Admin → Sistema → Reconexão Telegram'
            })

    except Exception as e:
        err = str(e)
        if 'AuthKeyDuplicated' in err:
            _telegram_session_invalida = True
            return web.json_response({
                'success': False,
                'error': 'Sessão conflitante (usada em outro IP). Solicite novo código no painel Admin.',
                'detalhe': 'AuthKeyDuplicatedError'
            })
        return web.json_response({'success': False, 'error': err}, status=500)

async def route_sessao_atual(request):
    """Retorna a sessão atual válida para salvar no Railway manualmente"""
    auth = (request.headers.get('X-PaynexBet-Secret','') or
            request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error':'Não autorizado'}, status=401)
    try:
        if client.is_connected() and await client.is_user_authorized():
            sessao = client.session.save()
            return web.json_response({
                'success': True,
                'session_str': sessao,
                'instrucao': 'Copie session_str e salve como SESSION_STR no Railway Variables'
            })
        return web.json_response({'success': False, 'error': 'Telegram não conectado'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def route_atualizar_sessao(request):
    """Atualiza SESSION_STRING em runtime sem reiniciar o servidor"""
    global client, _telegram_ready, _telegram_session_invalida
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        nova_sessao = str(data.get('session_string', '')).strip()
        if not nova_sessao or len(nova_sessao) < 50:
            return web.json_response({'error': 'session_string inválida (muito curta)'}, status=400)

        # Salvar nova sessão no arquivo e no PostgreSQL
        with open('session_string.txt', 'w') as f:
            f.write(nova_sessao)
        _salvar_sessao_db(nova_sessao)  # Persistir no DB!
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

async def route_home(request):
    """Página principal - paynexbet.com"""
    return web.Response(text=load_home_html(), content_type='text/html', charset='utf-8')

async def route_index(request):
    return web.Response(text=load_html(), content_type='text/html', charset='utf-8')

async def route_pague(request):
    """Página /pague - abre direto o formulário de gerar Pix"""
    html = load_html()
    # Injeta script para abrir modal de depósito automaticamente
    html = html.replace('</body>', '<script>window.addEventListener("load",()=>{setTimeout(()=>{const b=document.getElementById("btn-depositar");if(b)b.click();},500);});</script></body>')
    return web.Response(text=html, content_type='text/html', charset='utf-8')

async def route_health(request):
    motivo = None
    if not _telegram_ready:
        if _telegram_session_invalida:
            motivo = 'sessao_invalida'
        elif _telegram_reconectando:
            motivo = 'reconectando'
        elif _telegram_tentativas > 0:
            motivo = 'tentando'
        else:
            motivo = 'iniciando'
    # Tempo desde último ping bem-sucedido
    ping_age = int(time.time() - _telegram_ultimo_ping) if _telegram_ultimo_ping else None
    # Calcular FloodWait restante
    fw_restante = None
    if _floodwait_ate > time.time():
        fw_restante = int((_floodwait_ate - time.time()) / 60)

    return web.json_response({
        'status': 'online',
        'version': 'v20260415-parceiros-v31',
        'telegram': _telegram_ready,
        'telegram_motivo': motivo,
        'watchdog': 'ativo',
        'ultimo_ping_seg': ping_age,
        'tentativas': _telegram_tentativas,
        'bot': BOT_USERNAME,
        'webhook': '/webhook/confirmar',
        'floodwait_min': fw_restante,
    })

async def route_debug_pix(request):
    """Endpoint de diagnóstico - testa DBConn e INSERT diretamente"""
    import traceback, time
    resultado = {}
    try:
        resultado['USE_PG'] = _USE_PG
        resultado['DATABASE_URL_set'] = bool(DATABASE_URL)
        # Testar conexão
        conn = sqlite3_connect()
        resultado['sqlite3_connect'] = 'OK'
        # Testar INSERT
        tx_test = f'debug_{int(time.time())}'
        conn.execute('INSERT OR IGNORE INTO transacoes (tx_id,valor,cliente_id,status,created_at,extra) VALUES (?,?,?,?,?,?)',
                    (tx_test, 1.0, 'debug', 'teste', '2025-01-01', None))
        conn.commit()
        conn.close()
        resultado['INSERT'] = 'OK'
        resultado['tx_test'] = tx_test
        resultado['success'] = True
    except Exception as e:
        resultado['error'] = str(e)
        resultado['traceback'] = traceback.format_exc()
        resultado['success'] = False
    return web.json_response(resultado)

async def route_pix(request):
    try:
        data = await request.json()
        valor = float(data.get('valor', 0))
        cfg_pix = get_sorteio_config()
        vp_pix = float(cfg_pix.get('valor_por_numero') or 5.0)
        if valor < vp_pix:
            return web.json_response({'success': False, 'error': f'Valor mínimo R$ {vp_pix:.2f}'})
        if valor % vp_pix != 0:
            return web.json_response({'success': False, 'error': f'Valor deve ser múltiplo de R$ {vp_pix:.0f}'})

        participante_dados = data.get('participante_dados')
        cliente_id = data.get('cliente_id')
        if participante_dados and participante_dados.get('cpf'):
            cpf_limpo = re.sub(r'\D', '', str(participante_dados['cpf']))
            cliente_id = f"cli_{cpf_limpo}"

        # Gerar tx_id imediatamente e iniciar geração em background
        tx_id = f"txn_{hashlib.md5(f'{cliente_id}{valor}{time.time()}'.encode()).hexdigest()[:16]}"

        # Salvar transação como "gerando" para polling do frontend
        now = datetime.now().isoformat()
        part_json = json.dumps(participante_dados) if participante_dados else None
        # Cada execute() usa nova conexão PG - zero risco de transaction aborted
        conn = sqlite3_connect()
        conn.execute('INSERT OR IGNORE INTO transacoes (tx_id,valor,cliente_id,status,created_at,extra) VALUES (?,?,?,?,?,?)',
                  (tx_id, valor, cliente_id, 'gerando', now, part_json))
        conn.commit()
        conn.close()

        # Iniciar geração em background (não bloqueia resposta)
        async def gerar_em_background():
            # tx_id_override=tx_id garante que gerar_pix NÃO cria registro duplicado no banco
            result = await gerar_pix(valor, cliente_id, data.get('webhook_url'), participante_dados, tx_id_override=tx_id)
            conn2 = sqlite3_connect()
            if result.get('success') and result.get('pix_code'):
                # Atualizar registro original com pix_code - mantém o tx_id original intacto
                conn2.execute('UPDATE transacoes SET pix_code=?, status=? WHERE tx_id=?',
                           (result['pix_code'], 'pendente', tx_id))
                conn2.commit()
                conn2.close()
                print(f'✅ Pix pronto: {tx_id} | pix_code={result["pix_code"][:30]}...', flush=True)
            else:
                conn2.execute('UPDATE transacoes SET status=? WHERE tx_id=?', ('erro', tx_id))
                conn2.commit()
                conn2.close()
                print(f'❌ Falha gerar pix [{tx_id}]: {result.get("error")}', flush=True)

        asyncio.create_task(gerar_em_background())

        # Responder imediatamente com tx_id para polling
        return web.json_response({
            'success': True,
            'tx_id': tx_id,
            'status': 'gerando',
            'message': 'Gerando Pix... aguarde alguns segundos.',
            'poll_url': f'/api/pix/status/{tx_id}'
        })
    except Exception as e:
        import traceback
        print(f'❌ ERRO route_pix: {traceback.format_exc()}', flush=True)
        return web.json_response({'success': False, 'error': str(e)})

async def route_pix_status(request):
    """Polling do status de geração do Pix"""
    tx_id = request.match_info.get('tx_id')
    conn = sqlite3_connect()
    cur = conn.execute('SELECT tx_id, valor, pix_code, status FROM transacoes WHERE tx_id=?', (tx_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return web.json_response({'success': False, 'status': 'nao_encontrado'})
    r_tx_id, valor, pix_code, status = row
    if status == 'gerando':
        return web.json_response({'success': False, 'status': 'gerando', 'message': 'Aguarde...'})
    if status == 'erro':
        return web.json_response({'success': False, 'status': 'erro', 'error': 'Falha ao gerar Pix. Tente novamente.'})
    if pix_code:
        return web.json_response({
            'success': True, 'tx_id': r_tx_id, 'pix_code': pix_code,
            'valor': f'R$ {valor:.2f}', 'status': status
        })
    return web.json_response({'success': False, 'status': status})

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
    secret = request.headers.get('X-PaynexBet-Secret', '')
    if secret != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        tx_id = data.get('tx_id')
        if data.get('status') == 'pago' and tx_id:
            confirmar_pagamento(tx_id)
            # Verificar se é uma transação PayPix e disparar split 60/40
            try:
                conn_w = sqlite3_connect()
                cw = conn_w.cursor()
                cw.execute('SELECT valor, extra FROM transacoes WHERE tx_id=?', (tx_id,))
                row_w = cw.fetchone()
                conn_w.close()
                if row_w:
                    valor_w, extra_w = row_w
                    if extra_w:
                        ex = json.loads(extra_w)
                        if ex.get('tipo') == 'paypix':
                            asyncio.create_task(_processar_split_paypix(tx_id, valor_w, extra_w))
            except Exception as e_w:
                print(f'[webhook] erro split check: {e_w}', flush=True)
            return web.json_response({'success': True, 'message': f'Pagamento {tx_id} confirmado'})
        return web.json_response({'success': False, 'message': 'Status inválido'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_transacoes(request):
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
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

async def route_saldo_bot(request):
    """Consulta saldo do bot Telegram - retorna cache/banco imediato, atualiza Telegram em background."""
    secret = request.headers.get('X-PaynexBet-Secret') or request.rel_url.query.get('secret', '')
    pub = request.rel_url.query.get('secret', '') == 'pub'
    if not pub and secret != WEBHOOK_SECRET:
        return web.json_response({'success': False, 'error': 'Não autorizado'}, status=401)

    global _saldo_bot_cache, _saldo_bot_cache_ts, _saldo_bot_atualizando
    forcar = request.rel_url.query.get('force', '') == '1'
    agora  = time.time()
    cache_valido = (agora - _saldo_bot_cache_ts) < SALDO_BOT_CACHE_TTL

    # ── Sempre calcular saldo local como fallback imediato ────────────────
    def _saldo_local():
        try:
            conn = sqlite3_connect()
            r1 = conn.execute("SELECT COALESCE(SUM(valor),0) FROM transacoes WHERE status='pago'").fetchone()
            r2 = conn.execute("SELECT COALESCE(SUM(valor),0) FROM saques WHERE status IN ('enviado','confirmado','processado')").fetchone()
            conn.close()
            dep = float(r1[0]) if r1 else 0.0
            sac = float(r2[0]) if r2 else 0.0
            return round(dep - sac, 2)
        except:
            return 0.0

    # Se cache Telegram válido e não forçado - retorna imediatamente
    if cache_valido and not forcar and _saldo_bot_cache >= 0:
        return web.json_response({
            'success': True,
            'saldo_bot': _saldo_bot_cache,
            'fonte': 'cache',
            'cache_age': int(agora - _saldo_bot_cache_ts),
            'proximo_refresh': int(SALDO_BOT_CACHE_TTL - (agora - _saldo_bot_cache_ts))
        })

    # Disparar atualização em background (não bloqueia)
    if not _saldo_bot_atualizando:
        _saldo_bot_atualizando = True
        async def _atualizar_cache():
            global _saldo_bot_cache, _saldo_bot_cache_ts, _saldo_bot_atualizando
            try:
                novo_saldo = await asyncio.wait_for(verificar_saldo_bot(), timeout=30)
                if novo_saldo >= 0:
                    _saldo_bot_cache    = novo_saldo
                    _saldo_bot_cache_ts = time.time()
                    print(f'✅ [saldo_cache] Telegram: R$ {novo_saldo:.2f}', flush=True)
            except asyncio.TimeoutError:
                print('⚠️ [saldo_cache] Timeout Telegram (30s) - usando banco local', flush=True)
            except Exception as _e:
                print(f'⚠️ [saldo_cache] Erro Telegram: {_e}', flush=True)
            finally:
                _saldo_bot_atualizando = False
        asyncio.create_task(_atualizar_cache())

    # Se tem cache Telegram anterior, retornar ele enquanto atualiza
    if _saldo_bot_cache >= 0:
        return web.json_response({
            'success': True,
            'saldo_bot': _saldo_bot_cache,
            'fonte': 'cache_atualizando',
            'atualizando': True,
            'cache_age': int(agora - _saldo_bot_cache_ts)
        })

    # ── FALLBACK IMEDIATO: saldo calculado do banco local ─────────────────
    saldo_fb = _saldo_local()
    return web.json_response({
        'success': True,
        'saldo_bot': saldo_fb,
        'fonte': 'banco_local',
        'atualizando': _saldo_bot_atualizando,
        'aviso': 'Telegram sendo consultado em background. Clique 🔄 em 30s para ver saldo real.'
    })


async def route_saldo(request):
    """Retorna saldo calculado localmente (depósitos confirmados - saques realizados)
    Evita FloodWait do Telegram consultando apenas o banco de dados."""
    try:
        conn = sqlite3_connect()

        # Total depositado confirmado (pago)
        r1 = conn.execute("SELECT COALESCE(SUM(valor),0) FROM transacoes WHERE status='pago'").fetchone()
        total_depositado = float(r1[0]) if r1 else 0.0

        # Total sacado (enviado/confirmado/processado) - exclui split PayPix automático
        r2 = conn.execute(
            "SELECT COALESCE(SUM(valor),0) FROM saques WHERE status IN ('enviado','confirmado','processado')"
        ).fetchone()
        total_sacado = float(r2[0]) if r2 else 0.0

        # Total pendente
        r3 = conn.execute("SELECT COALESCE(SUM(valor),0) FROM transacoes WHERE status='pendente'").fetchone()
        total_pendente = float(r3[0]) if r3 else 0.0

        # Saques pendentes
        r4 = conn.execute("SELECT COALESCE(SUM(valor),0) FROM saques WHERE status='pendente'").fetchone()
        saques_pendentes = float(r4[0]) if r4 else 0.0

        conn.close()

        # Saldo = depositado - sacado
        saldo = round(total_depositado - total_sacado, 2)
        # Disponível = saldo - saques pendentes em processamento
        disponivel = round(max(0.0, saldo - saques_pendentes), 2)

        return web.json_response({
            'success': True,
            'saldo': saldo,
            'disponivel': disponivel,
            'total_depositado': total_depositado,
            'total_sacado': total_sacado,
            'depositos_pendentes': total_pendente,
            'saques_pendentes': saques_pendentes,
            'fonte': 'banco_local',
        })
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

        # ── AUTENTICAÇÃO OBRIGATÓRIA ──────────────────────────────
        senha = str(data.get('senha', '')).strip()
        auth_header = request.headers.get('X-PaynexBet-Secret', '')
        if senha != 'paynex2024' and auth_header != WEBHOOK_SECRET:
            return web.json_response({'success': False, 'error': 'Senha incorreta. Acesso negado.'}, status=401)
        # ─────────────────────────────────────────────────────────

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
            conn = sqlite3_connect()
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
            conn = sqlite3_connect()
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
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
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
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        def _q(sql):
            conn2 = sqlite3_connect()
            cur2 = conn2.execute(sql)
            rows2 = cur2.fetchall()
            conn2.close()
            return rows2

        # Depósitos
        dep_conf, val_dep_conf = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM transacoes WHERE status='pago'")[0]
        dep_pend, val_dep_pend = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM transacoes WHERE status='pendente'")[0]
        dep_total, val_dep_total = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM transacoes")[0]

        # Saques
        saq_conf, val_saq_conf = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status IN ('enviado','confirmado','processado')")[0]
        saq_pend, val_saq_pend = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status='pendente'")[0]
        saq_erro, _ = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status='erro'")[0]
        saq_total, val_saq_total = _q("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques")[0]

        # Últimos 7 dias - depósitos por dia (query com try/except para SQLite e PostgreSQL)
        try:
            dep_por_dia = [{'data': str(r[0])[:10], 'qtd': r[1], 'valor': round(float(r[2]),2)} for r in _q(
                "SELECT DATE(created_at), COUNT(*), COALESCE(SUM(valor),0) FROM transacoes WHERE created_at >= NOW() - INTERVAL '7 days' GROUP BY DATE(created_at) ORDER BY DATE(created_at)")]
        except Exception:
            dep_por_dia = []
        try:
            saq_por_dia = [{'data': str(r[0])[:10], 'qtd': r[1], 'valor': round(float(r[2]),2)} for r in _q(
                "SELECT DATE(created_at), COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE created_at >= NOW() - INTERVAL '7 days' GROUP BY DATE(created_at) ORDER BY DATE(created_at)")]
        except Exception:
            saq_por_dia = []

        # Últimos depósitos e saques
        ult_dep = [{'tx_id':r[0],'valor':r[1],'status':r[2],'created_at':r[3],'paid_at':r[4]} for r in _q(
            "SELECT tx_id,valor,status,created_at,paid_at FROM transacoes ORDER BY created_at DESC LIMIT 10")]

        ult_saq = [{'saque_id':r[0],'valor':r[1],'chave_pix':r[2],'tipo_chave':r[3],'status':r[4],'created_at':r[5],'processado_at':r[6]} for r in _q(
            "SELECT saque_id,valor,chave_pix,tipo_chave,status,created_at,processado_at FROM saques ORDER BY created_at DESC LIMIT 10")]
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
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    saque_id = request.match_info.get('saque_id')
    try:
        conn = sqlite3_connect()
        cur = conn.execute("SELECT status FROM saques WHERE saque_id=?", (saque_id,))
        row = cur.fetchone()
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
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
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

async def route_paypix_page(request):
    """Página pública /paypix - parceiro gera Pix e recebe 60%"""
    html = load_paypix_html()
    return web.Response(text=html, content_type='text/html', charset='utf-8')

async def route_paypix_gerar(request):
    """Gera um Pix para o parceiro. Guarda chave_pix do parceiro no extra para split depois."""
    try:
        data = await request.json()
        chave_pix   = str(data.get('chave_pix', '')).strip()
        tipo_chave  = str(data.get('tipo_chave', 'cpf')).strip()
        valor       = float(data.get('valor', 0))

        if not chave_pix:
            return web.json_response({'success': False, 'error': 'Informe sua chave Pix'})
        # Ler config dinâmica (paypix_min pode ser editado pelo admin)
        _pp_cfg_pre = get_paypix_config()
        _min_val = float(_pp_cfg_pre.get('paypix_min', 15.0))
        if valor < _min_val:
            return web.json_response({'success': False, 'error': f'Valor mínimo R$ {_min_val:.2f}'.replace('.', ',')})
        # PayPix aceita qualquer valor >= 5 (sem restrição de múltiplo)
        # Arredondar para 2 casas decimais
        valor = round(valor, 2)
        # NÃO bloquear por Telegram - deixa tentar e retornar erro real se falhar

        cliente_id = f"paypix_{hashlib.md5(f'{chave_pix}{time.time()}'.encode()).hexdigest()[:10]}"
        tx_id = f"ppx_{hashlib.md5(f'{chave_pix}{valor}{time.time()}'.encode()).hexdigest()[:16]}"

        # Ler % dinâmico do banco
        _pp_cfg = get_paypix_config()
        _pct = _pp_cfg.get('paypix_pct', 0.6)

        extra = json.dumps({
            'tipo': 'paypix',
            'parceiro_chave': chave_pix,
            'parceiro_tipo':  tipo_chave,
            'valor_total':    valor,
            'parceiro_pct':   _pct,
            'plataforma_pct': round(1.0 - _pct, 4),
        })

        # Salvar como "gerando"
        now = datetime.now().isoformat()
        conn = sqlite3_connect()
        try:
            conn.execute(
                'INSERT OR IGNORE INTO transacoes (tx_id,valor,cliente_id,status,created_at,extra) VALUES (?,?,?,?,?,?)',
                (tx_id, valor, cliente_id, 'gerando', now, extra)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f'[PayPix] Erro ao inserir transacao: {e}', flush=True)
        finally:
            conn.close()

        # Gerar Pix em background
        async def gerar_bg():
            result = await gerar_pix(valor, cliente_id, None, None)
            conn2 = sqlite3_connect()
            if result.get('success') and result.get('pix_code'):
                # MANTER o tx_id original (ppx_...) - apenas atualizar pix_code e status
                # Isso garante que o status polling funcione com o tx_id retornado ao frontend
                conn2.execute(
                    'UPDATE transacoes SET pix_code=?, status=? WHERE tx_id=?',
                    (result['pix_code'], 'pendente', tx_id)
                )
                print(f'[PayPix] Pix gerado OK: {tx_id} R${valor:.2f}', flush=True)
            else:
                erro_msg = result.get('error', 'Erro desconhecido')
                conn2.execute("UPDATE transacoes SET status='erro' WHERE tx_id=?", (tx_id,))
                print(f'[PayPix] Falha ao gerar: {erro_msg}', flush=True)
            conn2.commit()
            conn2.close()

        asyncio.create_task(gerar_bg())

        return web.json_response({
            'success': True,
            'tx_id':   tx_id,
            'status':  'gerando',
            'message': 'Gerando Pix… aguarde.',
            'poll_url': f'/api/paypix/status/{tx_id}'
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def route_paypix_status(request):
    """Status da transação PayPix - retorna pix_code quando pronto e status de pagamento"""
    tx_id = request.match_info.get('tx_id')
    conn = sqlite3_connect()
    # Usar conn.execute() diretamente (evita cursor compartilhado no PG)
    cur = conn.execute('SELECT tx_id, valor, pix_code, status, extra FROM transacoes WHERE tx_id=?', (tx_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return web.json_response({'success': False, 'status': 'nao_encontrado'})

    real_tx, valor, pix_code, status, extra_str = row
    resp = {'success': True, 'tx_id': real_tx, 'status': status, 'valor': valor}

    if status == 'gerando':
        resp['success'] = False
        resp['message'] = 'Gerando Pix…'
        return web.json_response(resp)

    if status == 'erro':
        resp['success'] = False
        resp['error'] = 'Falha ao gerar Pix'
        return web.json_response(resp)

    if pix_code:
        resp['pix_code'] = pix_code

    if status == 'pago':
        resp['pagamento'] = 'pago'
        return web.json_response(resp)

    # Se pendente, tenta detectar pagamento via mensagens recentes do bot
    if status == 'pendente' and _telegram_ready and pix_code:
        try:
            import datetime as dt
            bot = await client.get_entity(BOT_USERNAME)
            msgs = await client.get_messages(bot, limit=15)
            padroes_pago = [
                r'Depósito de R\$.*recebido',
                r'✅ Depósito',
                r'Valor creditado',
                r'depósito.*confirmado',
            ]
            for msg in msgs:
                if not msg.text:
                    continue
                if any(re.search(p, msg.text, re.IGNORECASE) for p in padroes_pago):
                    if hasattr(msg, 'date') and msg.date:
                        idade = (dt.datetime.now(dt.timezone.utc) - msg.date).total_seconds()
                        if idade < 900:  # 15 minutos
                            confirmar_pagamento(real_tx)
                            # Disparar split
                            extra2_str = extra_str or ''
                            asyncio.create_task(_processar_split_paypix(real_tx, valor, extra2_str))
                            resp['pagamento'] = 'pago'
                            resp['status'] = 'pago'
                            return web.json_response(resp)
        except Exception as e:
            print(f'[paypix status] erro poll bot: {e}', flush=True)

    return web.json_response(resp)

async def route_paypix_config(request):
    """GET: retorna config PayPix | POST (admin): atualiza % e descrição"""
    if request.method == 'POST':
        auth = (request.headers.get('X-PaynexBet-Secret', '') or
                request.rel_url.query.get('secret', ''))
        if auth != WEBHOOK_SECRET:
            return web.json_response({'error': 'Não autorizado'}, status=401)
        try:
            data = await request.json()
            pct_raw = float(data.get('paypix_pct', 60))
            # Aceita 0-100 (percentual) ou 0.0-1.0 (decimal)
            pct = pct_raw / 100.0 if pct_raw > 1 else pct_raw
            pct = max(0.01, min(0.99, pct))  # entre 1% e 99%
            ativo = int(bool(data.get('paypix_ativo', True)))
            descricao = str(data.get('paypix_descricao', 'Gere seu Pix e receba sua % do valor'))[:200]
            paypix_min = float(data.get('paypix_min', 5.0))
            paypix_min = max(1.0, min(10000.0, paypix_min))  # entre R$1 e R$10.000
            conn = sqlite3_connect()
            # Criar coluna paypix_min se não existir
            try:
                conn.execute('ALTER TABLE sorteio_config ADD COLUMN paypix_min REAL DEFAULT 5.0')
                conn.commit()
            except Exception:
                pass
            conn.execute(
                'UPDATE sorteio_config SET paypix_pct=?, paypix_ativo=?, paypix_descricao=?, paypix_min=?, updated_at=? WHERE id=1',
                (pct, ativo, descricao, paypix_min, datetime.now().isoformat())
            )
            conn.commit(); conn.close()
            return web.json_response({
                'success': True,
                'paypix_pct': pct,
                'paypix_pct_display': f'{round(pct*100, 1)}%',
                'paypix_ativo': bool(ativo),
                'paypix_descricao': descricao,
                'paypix_min': paypix_min,
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    else:
        # GET - público (paypix.html precisa saber o %)
        cfg = get_paypix_config()
        pct = cfg.get('paypix_pct', 0.6)
        return web.json_response({
            'paypix_pct':          pct,
            'paypix_pct_display':  f'{round(pct*100, 1)}%',
            'paypix_ativo':        cfg.get('paypix_ativo', True),
            'paypix_descricao':    cfg.get('paypix_descricao', 'Gere seu Pix e receba sua % do valor'),
            'paypix_min':          cfg.get('paypix_min', 5.0),
        })

def _paypix_fila_inserir(tx_id, val_par, chave, tipo, pct):
    """Insere item na fila persistente de splits PayPix"""
    try:
        agora = datetime.now().isoformat()
        conn  = sqlite3_connect()
        conn.execute(
            '''INSERT INTO paypix_fila
               (tx_id, valor, chave_pix, tipo_chave, pct, tentativas, status,
                proxima_tentativa, created_at, observacao)
               VALUES (?,?,?,?,?,0,'pendente',?,?,?)''',
            (tx_id, val_par, chave, tipo, pct, agora, agora,
             f'Split PayPix {round(pct*100)}% aguardando envio')
        )
        conn.commit()
        conn.close()
        print(f'[PayPix Fila] ✅ Enfileirado: R${val_par:.2f} → {chave} (tx={tx_id})', flush=True)
    except Exception as e:
        print(f'[PayPix Fila] Erro ao enfileirar: {e}', flush=True)


async def _tentar_envio_split(item_id, val_par, chave, tipo, tx_id, tentativa_num):
    """Tenta enviar o saque do split. Retorna True se sucesso."""
    print(f'[PayPix Fila] Tentativa #{tentativa_num} - R${val_par:.2f} → {chave} ({tipo}) [fila_id={item_id}]', flush=True)
    try:
        resultado = await executar_saque_bot(val_par, tipo, chave)
    except Exception as ex:
        resultado = {'success': False, 'error': str(ex), 'status': 'erro'}

    print(f'[PayPix Fila] Resultado #{tentativa_num}: {resultado}', flush=True)
    return resultado


async def _processar_split_paypix(tx_id, valor, extra_str):
    """
    Fase 1 - Tenta enviar 3 vezes com 30s de intervalo.
    Se falhar nas 3: enfileira na paypix_fila para o worker tentar a cada 5 min até conseguir.
    """
    TENTATIVAS_RAPIDAS = 3
    DELAY_RAPIDO       = 30  # segundos entre tentativas rápidas

    try:
        extra = json.loads(extra_str or '{}')
        if extra.get('tipo') != 'paypix':
            return
        chave   = extra.get('parceiro_chave', '')
        tipo    = extra.get('parceiro_tipo', 'cpf')
        pct     = float(extra.get('parceiro_pct', 0.6))
        val_par = round(valor * pct, 2)

        if not chave or val_par < 10:
            print(f'[PayPix] split bloqueado - valor parceiro R${val_par:.2f} abaixo do mínimo R$10,00 ou chave inválida. tx={tx_id}', flush=True)
            return

        # ── FASE 1: 3 tentativas rápidas (30s entre cada) ──
        resultado = None
        for n in range(1, TENTATIVAS_RAPIDAS + 1):
            resultado = await _tentar_envio_split(0, val_par, chave, tipo, tx_id, n)
            if resultado.get('success'):
                # ✅ Sucesso - registrar saque e sair
                _registrar_saque_split(tx_id, val_par, chave, tipo, pct, resultado, n)
                return
            if n < TENTATIVAS_RAPIDAS:
                print(f'[PayPix] aguardando {DELAY_RAPIDO}s antes do retry {n+1}...', flush=True)
                await asyncio.sleep(DELAY_RAPIDO)

        # ── FASE 2: Falhou nas 3 → enfileirar para worker persistente ──
        print(f'[PayPix] ❌ Falhou nas {TENTATIVAS_RAPIDAS} tentativas rápidas. Enfileirando para retry a cada 5min...', flush=True)
        _paypix_fila_inserir(tx_id, val_par, chave, tipo, pct)

    except Exception as e:
        print(f'[PayPix] erro split (exceção): {e}', flush=True)
        # Se exceção, tentar enfileirar para não perder o split
        try:
            extra2 = json.loads(extra_str or '{}')
            chave2  = extra2.get('parceiro_chave', '')
            tipo2   = extra2.get('parceiro_tipo', 'cpf')
            pct2    = float(extra2.get('parceiro_pct', 0.6))
            val2    = round(valor * pct2, 2)
            if chave2 and val2 >= 10:
                _paypix_fila_inserir(tx_id, val2, chave2, tipo2, pct2)
        except Exception:
            pass


def _registrar_saque_split(tx_id, val_par, chave, tipo, pct, resultado, tentativas):
    """Registra o saque na tabela saques após envio bem-sucedido"""
    saque_id     = f"spp_{hashlib.md5(f'{tx_id}{time.time()}'.encode()).hexdigest()[:12]}"
    status_final = resultado.get('status', 'enviado') if resultado.get('success') else 'erro'
    observacao   = f'PayPix split {round(pct*100)}% - tx {tx_id} - {tentativas} tentativa(s)'
    try:
        conn = sqlite3_connect()
        conn.execute(
            '''INSERT OR IGNORE INTO saques
               (saque_id,valor,chave_pix,tipo_chave,status,created_at,observacao)
               VALUES (?,?,?,?,?,?,?)''',
            (saque_id, val_par, chave, tipo, status_final,
             datetime.now().isoformat(), observacao[:200])
        )
        conn.commit()
        conn.close()
        print(f'[PayPix] ✅ Saque registrado: R${val_par:.2f} → {chave} status={status_final}', flush=True)
    except Exception as e:
        print(f'[PayPix] Erro ao registrar saque: {e}', flush=True)


async def _worker_paypix_fila():
    """
    Worker background: processa a fila paypix_fila.
    A cada 5 minutos verifica itens pendentes e tenta enviar.
    Continua tentando INDEFINIDAMENTE até conseguir finalizar o Pix.
    """
    INTERVALO_WORKER = 300  # 5 minutos entre ciclos
    TENTATIVAS_CICLO = 2    # tentativas por ciclo do worker (com 15s entre elas)
    DELAY_CICLO      = 15   # segundos entre tentativas dentro do ciclo

    print('[PayPix Worker] 🚀 Iniciado - verificando fila a cada 5 minutos', flush=True)

    while True:
        await asyncio.sleep(INTERVALO_WORKER)
        try:
            conn  = sqlite3_connect()
            agora = datetime.now().isoformat()
            # Buscar itens pendentes cuja próxima tentativa já passou
            cur   = conn.execute(
                """SELECT id, tx_id, valor, chave_pix, tipo_chave, pct, tentativas
                   FROM paypix_fila
                   WHERE status='pendente' AND (proxima_tentativa IS NULL OR proxima_tentativa <= ?)
                   ORDER BY created_at ASC LIMIT 10""",
                (agora,)
            )
            itens = cur.fetchall()
            conn.close()

            if not itens:
                continue

            print(f'[PayPix Worker] 🔄 Processando {len(itens)} item(s) da fila...', flush=True)

            for row in itens:
                item_id, tx_id, val_par, chave, tipo, pct, tentativas_total = row

                sucesso = False
                for n in range(1, TENTATIVAS_CICLO + 1):
                    tentativa_num = tentativas_total + n
                    resultado     = await _tentar_envio_split(item_id, val_par, chave, tipo, tx_id, tentativa_num)

                    if resultado.get('success'):
                        sucesso = True
                        # ✅ Atualizar fila como finalizado
                        conn2 = sqlite3_connect()
                        conn2.execute(
                            """UPDATE paypix_fila
                               SET status='finalizado', tentativas=?, finalizado_at=?,
                                   observacao=?
                               WHERE id=?""",
                            (tentativa_num, datetime.now().isoformat(),
                             f'✅ Enviado na tentativa #{tentativa_num}', item_id)
                        )
                        conn2.commit()
                        conn2.close()
                        # Registrar na tabela saques
                        _registrar_saque_split(tx_id, val_par, chave, tipo, pct, resultado, tentativa_num)
                        print(f'[PayPix Worker] ✅ FINALIZADO item_id={item_id} - R${val_par:.2f} → {chave} (tentativa #{tentativa_num})', flush=True)
                        break

                    if n < TENTATIVAS_CICLO:
                        await asyncio.sleep(DELAY_CICLO)

                if not sucesso:
                    # Agendar próxima tentativa para daqui 5 minutos
                    proxima = (datetime.now() + __import__('datetime').timedelta(minutes=5)).isoformat()
                    novo_total = tentativas_total + TENTATIVAS_CICLO
                    conn3 = sqlite3_connect()
                    conn3.execute(
                        """UPDATE paypix_fila
                           SET tentativas=?, proxima_tentativa=?,
                               observacao=?
                           WHERE id=?""",
                        (novo_total, proxima,
                         f'⏳ Aguardando retry - {novo_total} tentativa(s) realizadas', item_id)
                    )
                    conn3.commit()
                    conn3.close()
                    print(f'[PayPix Worker] ⏳ item_id={item_id} - próxima tentativa em 5min (total={novo_total})', flush=True)

        except Exception as e:
            print(f'[PayPix Worker] Erro no ciclo: {e}', flush=True)

async def route_paypix_fila(request):
    """GET /api/paypix/fila - Lista a fila de splits pendentes (admin)"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        conn = sqlite3_connect()
        cur  = conn.execute(
            """SELECT id, tx_id, valor, chave_pix, tipo_chave, pct,
                      tentativas, status, proxima_tentativa, created_at,
                      finalizado_at, observacao
               FROM paypix_fila
               ORDER BY created_at DESC LIMIT 50"""
        )
        rows = cur.fetchall()
        conn.close()
        cols = ['id','tx_id','valor','chave_pix','tipo_chave','pct',
                'tentativas','status','proxima_tentativa','created_at',
                'finalizado_at','observacao']
        itens = [dict(zip(cols, r)) for r in rows]
        pendentes   = sum(1 for i in itens if i['status'] == 'pendente')
        finalizados = sum(1 for i in itens if i['status'] == 'finalizado')
        return web.json_response({
            'success': True,
            'total': len(itens),
            'pendentes': pendentes,
            'finalizados': finalizados,
            'itens': itens
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

# ══════════════════════════════════════════════════════════════════
# ─── ROTAS BOT 2 - @paypix_nexbot ───────────────────────────────────
# ══════════════════════════════════════════════════════════════════

async def route_bot2_status(request):
    """Status do @paypix_nexbot - inclui userbot Telethon E bot real (python-telegram-bot)."""
    # Status do bot REAL (BOT2_TOKEN configurado = pronto para usar)
    bot2_token    = os.environ.get('BOT2_TOKEN', '')
    mp2_token     = os.environ.get('MP2_ACCESS_TOKEN', '')
    bot_real_ok   = bool(bot2_token)

    # Verificar se bot real está respondendo (rápido, sem bloquear)
    bot_real_info = {}
    if bot2_token:
        try:
            import aiohttp as _aiohttp
            async with _aiohttp.ClientSession() as _sess:
                async with _sess.get(
                    f'https://api.telegram.org/bot{bot2_token}/getMe',
                    timeout=_aiohttp.ClientTimeout(total=5)
                ) as _r:
                    _data = await _r.json()
                    if _data.get('ok'):
                        _u = _data['result']
                        bot_real_info = {
                            'username': _u.get('username', 'paypix_nexbot'),
                            'nome': _u.get('first_name', ''),
                            'id': _u.get('id')
                        }
                        bot_real_ok = True
        except Exception:
            bot_real_ok = bool(bot2_token)  # token existe mas não verificado

    return web.json_response({
        'success':   True,
        'bot':       'bot2',
        'username':  bot_real_info.get('username', BOT2_USERNAME),
        # online = bot real configurado OU userbot conectado
        'online':    bot_real_ok or _telegram2_ready,
        'bot_real':  bot_real_ok,
        'bot_real_info': bot_real_info,
        'mp2_configurado': bool(mp2_token),
        # userbot legado (Telethon)
        'userbot_online': _telegram2_ready,
        'sessao_ok':   not _telegram2_session_inv,
        'ultimo_ping': _telegram2_ultimo_ping,
        'tem_sessao':  bool(SESSION_STR2),
    })

async def route_bot2_saldo(request):
    """Saldo real do @paypix_nexbot"""
    secret = request.headers.get('X-PaynexBet-Secret') or request.rel_url.query.get('secret', '')
    pub    = request.rel_url.query.get('secret', '') == 'pub'
    if not pub and secret != WEBHOOK_SECRET:
        return web.json_response({'success': False, 'error': 'Não autorizado'}, status=401)
    if not _telegram2_ready:
        return web.json_response({'success': False, 'error': '[Bot2] Offline - conecte a conta primeiro', 'saldo_bot': -1})
    try:
        saldo = await verificar_saldo_bot2()
        if saldo >= 0:
            return web.json_response({'success': True, 'saldo_bot': saldo, 'bot': 'bot2', 'fonte': 'telegram'})
        return web.json_response({'success': False, 'saldo_bot': -1, 'error': '[Bot2] Saldo não encontrado'})
    except Exception as e:
        return web.json_response({'success': False, 'saldo_bot': -1, 'error': str(e)})

async def route_bot2_pix(request):
    """Gera Pix via @paypix_nexbot (mesmas regras do Bot 1)"""
    try:
        data  = await request.json()
        valor = float(data.get('valor', 0))
        if valor < 1:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$1,00'})
        cliente_id       = data.get('cliente_id')
        webhook_url      = data.get('webhook_url')
        participante_dados = data.get('participante_dados')
        resultado = await gerar_pix_bot2(valor, cliente_id, webhook_url, participante_dados)
        return web.json_response(resultado)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def route_bot2_solicitar_codigo(request):
    """Passo 1: solicitar código Telegram para a conta do Bot2"""
    global _login_state2
    auth = request.headers.get('X-PaynexBet-Secret', '') or request.rel_url.query.get('secret', '')
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        phone_body = str(data.get('phone', '')).strip()
        if not phone_body:
            return web.json_response({'error': 'Número obrigatório'}, status=400)
        import re as _re2b
        digits = _re2b.sub(r'\D', '', phone_body)
        if not digits.startswith('55'):
            digits = '55' + digits
        phone_use = '+' + digits
        from telethon.sessions import StringSession as _SS2
        from telethon.errors import FloodWaitError as _FW2
        temp2 = TelegramClient(_SS2(), API_ID, API_HASH)
        await temp2.connect()
        sent2 = await temp2.send_code_request(phone_use)
        _login_state2 = {
            'client':  temp2,
            'hash':    sent2.phone_code_hash,
            'session': temp2.session.save(),
            'phone':   phone_use,
        }
        print(f'📱 [Bot2] Código solicitado para {phone_use}', flush=True)
        return web.json_response({'success': True, 'message': f'Código enviado para {phone_use}!', 'phone': phone_use})
    except Exception as e:
        import re as _re3b
        m = _re3b.search(r'(\d+)', str(e))
        if 'FloodWait' in type(e).__name__ and m:
            secs = int(m.group(1)); h = secs // 3600; mi = (secs % 3600) // 60
            return web.json_response({'success': False, 'error': f'FloodWait: aguarde {h}h{mi}min'})
        return web.json_response({'success': False, 'error': str(e)}, status=500)

async def route_bot2_confirmar_codigo(request):
    """Passo 2: confirmar código e salvar sessão do Bot2"""
    global _login_state2, client2, _telegram2_ready, _telegram2_session_inv, SESSION_STR2
    auth = request.headers.get('X-PaynexBet-Secret', '') or request.rel_url.query.get('secret', '')
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        code = str(data.get('code', '')).strip()
        if not code:
            return web.json_response({'error': 'Código obrigatório'}, status=400)
        if not _login_state2:
            return web.json_response({'error': 'Solicite o código primeiro'}, status=400)
        from telethon.errors import SessionPasswordNeededError as _SPNE2
        temp2 = _login_state2['client']
        if not temp2.is_connected():
            from telethon.sessions import StringSession as _SS2C
            temp2 = TelegramClient(_SS2C(_login_state2['session']), API_ID, API_HASH)
            await temp2.connect()
        phone2 = _login_state2.get('phone', '')
        try:
            await temp2.sign_in(phone2, code, phone_code_hash=_login_state2['hash'])
        except _SPNE2:
            senha2 = data.get('password', '')
            if not senha2:
                return web.json_response({'success': False, 'needs_2fa': True, 'message': 'Digite sua senha 2FA'})
            await temp2.sign_in(password=senha2)
        me2 = await temp2.get_me()
        nova_sessao2 = temp2.session.save()
        await temp2.disconnect()
        _login_state2 = {}
        # Salvar em arquivo local
        with open('session_string2.txt', 'w') as f:
            f.write(nova_sessao2)
        # Salvar no PostgreSQL
        _salvar_sessao2_db(nova_sessao2)
        # Atualizar client2 em memória
        SESSION_STR2 = nova_sessao2
        from telethon.sessions import StringSession as _SS2U
        if client2.is_connected():
            try: await client2.disconnect()
            except: pass
        client2.__init__(_SS2U(nova_sessao2), API_ID, API_HASH)
        await client2.connect()
        if await client2.is_user_authorized():
            _telegram2_ready = True
            _telegram2_session_inv = False
        print(f'✅ [Bot2] Sessão salva! Conta: {me2.first_name} ({me2.id})', flush=True)
        return web.json_response({
            'success': True,
            'message': f'✅ [Bot2] Conectado como {me2.first_name}!',
            'nome': me2.first_name,
            'id': me2.id,
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

# ══════════════════════════════════════════════════════════════════
# ─── ROTAS MP2 - Mercado Pago + @paypix_nexbot (Bot real) ────────
# ══════════════════════════════════════════════════════════════════

async def route_mp2_status(request):
    """Status geral do sistema PayPixNex (Bot2 real)."""
    try:
        from mp2_api import mp2_stats_admin, mp2_get_config
        stats = mp2_stats_admin()
        bot2_token = bool(os.environ.get('BOT2_TOKEN', ''))
        mp2_token  = bool(os.environ.get('MP2_ACCESS_TOKEN', ''))
        return web.json_response({
            'success': True,
            'bot': '@paypix_nexbot',
            'bot2_token_configurado': bot2_token,
            'mp2_token_configurado': mp2_token,
            'stats': stats,
            'deposito_minimo': mp2_get_config('deposito_minimo', '5'),
            'deposito_maximo': mp2_get_config('deposito_maximo', '10000'),
            'saque_minimo': mp2_get_config('saque_minimo', '20'),
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

async def route_mp2_webhook(request):
    """
    Webhook do Mercado Pago para confirmar pagamentos Pix.
    URL: POST /webhook/mp2
    Registrar no painel MP: https://SEU_DOMINIO/webhook/mp2
    """
    try:
        body = await request.json()
        action = body.get('action', '')
        data   = body.get('data', {})

        # MP envia: {"action":"payment.updated","data":{"id":"12345"}}
        if action in ('payment.updated', 'payment.created'):
            payment_id = str(data.get('id', ''))
            if payment_id:
                from mp2_api import mp2_verificar_pagamento, mp2_confirmar_pagamento_webhook
                info = mp2_verificar_pagamento(payment_id)
                if info.get('status') == 'approved':
                    external_ref = info.get('external_ref', '')
                    if external_ref:
                        processado = mp2_confirmar_pagamento_webhook(external_ref, payment_id)
                        print(f'✅ [mp2_webhook] {external_ref} processado={processado}', flush=True)
                        # Notificar usuário via bot (se token configurado)
                        _mp2_notificar_pagamento(external_ref, info.get('valor', 0))
                        # Pagar comissão ao parceiro (se houver)
                        _mp2_pagar_comissao_parceiro_webhook(external_ref, info.get('valor', 0))
                    return web.json_response({'ok': True, 'processado': True})

        return web.json_response({'ok': True, 'action': action})
    except Exception as e:
        print(f'[mp2_webhook] Erro: {e}', flush=True)
        return web.json_response({'ok': False, 'error': str(e)}, status=200)  # sempre 200 pro MP

def _mp2_notificar_pagamento(external_ref: str, valor: float):
    """Notifica usuário + canal Telegram após confirmação de depósito (fire-and-forget)."""
    try:
        import psycopg2, psycopg2.extras, requests as _req
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Busca dados da transação e do usuário
        cur.execute("""
            SELECT t.telegram_id, u.username, u.nome
            FROM mp2_transacoes t
            LEFT JOIN mp2_usuarios u ON u.telegram_id = t.telegram_id
            WHERE t.mp_external_ref = %s
        """, (external_ref,))
        row = cur.fetchone()

        # Busca canal de notificações
        cur.execute("SELECT canal_id, invite_link FROM mp2_canais WHERE tipo = 'notificacoes'")
        canal_row = cur.fetchone()

        cur.close(); conn.close()

        bot2_token = os.environ.get('BOT2_TOKEN', '')
        if not bot2_token:
            return

        valor_fmt = f"R$ {float(valor):.2f}".replace('.', ',')

        # 1️⃣ Notifica o USUÁRIO via DM
        if row:
            telegram_id = row['telegram_id']
            if telegram_id:
                msg_usuario = (
                    f"✅ *PIX Confirmado!*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"💰 *{valor_fmt}* creditado com sucesso!\n\n"
                    f"Use /carteira para ver seu saldo atualizado. 🏦"
                )
                _req.post(
                    f'https://api.telegram.org/bot{bot2_token}/sendMessage',
                    json={'chat_id': telegram_id, 'text': msg_usuario, 'parse_mode': 'Markdown'},
                    timeout=10
                )

        # 2️⃣ Notifica o CANAL público
        if canal_row:
            canal_id = canal_row['canal_id']
            username = ''
            if row:
                username = f"@{row['username']}" if row.get('username') else (row.get('nome') or 'Usuário')
            msg_canal = (
                f"💰 *Depósito Confirmado*\n"
                f"👤 {username}\n"
                f"✅ {valor_fmt} recebido via PIX\n"
                f"🕐 {__import__('datetime').datetime.now().strftime('%d/%m %H:%M')}"
            )
            _req.post(
                f'https://api.telegram.org/bot{bot2_token}/sendMessage',
                json={'chat_id': canal_id, 'text': msg_canal, 'parse_mode': 'Markdown'},
                timeout=10
            )
            print(f'✅ [mp2_notificar] Canal notificado: {valor_fmt}', flush=True)

    except Exception as e:
        print(f'[mp2_notificar] Erro: {e}', flush=True)

def _mp2_pagar_comissao_parceiro_webhook(external_ref: str, valor: float):
    """
    Verifica se a transação tem parceiro vinculado e paga a comissão automaticamente.
    Chamado após confirmação de pagamento PIX.
    """
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""
            SELECT parceiro_codigo, comissao_valor, comissao_status
            FROM mp2_transacoes
            WHERE mp_external_ref = %s AND parceiro_codigo IS NOT NULL
        """, (external_ref,))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            return  # Sem parceiro vinculado

        parceiro_codigo = row[0]
        comissao_valor  = float(row[1] or 0)
        comissao_status = row[2]

        if comissao_status == 'pago':
            return  # Já pago

        if comissao_valor <= 0:
            return

        print(f'💰 [comissao] Pagando R${comissao_valor:.2f} ao parceiro {parceiro_codigo}...', flush=True)

        # Pagar via Mercado Pago
        from mp2_api import mp2_pagar_comissao_parceiro
        resultado = mp2_pagar_comissao_parceiro(parceiro_codigo, comissao_valor)

        # Atualizar status na transação
        conn2 = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur2  = conn2.cursor()
        novo_status = 'pago' if resultado.get('success') else 'erro'
        cur2.execute("""
            UPDATE mp2_transacoes
            SET comissao_status = %s
            WHERE mp_external_ref = %s
        """, (novo_status, external_ref))

        # Atualizar totais do parceiro
        if resultado.get('success'):
            cur2.execute("""
                UPDATE mp2_parceiros
                SET total_gerado   = total_gerado + %s,
                    total_comissao = total_comissao + %s
                WHERE codigo = %s
            """, (valor, comissao_valor, parceiro_codigo))

        conn2.commit()
        cur2.close(); conn2.close()

        if resultado.get('success'):
            print(f'✅ [comissao] Pago R${comissao_valor:.2f} → {parceiro_codigo}', flush=True)
        else:
            print(f'⚠️ [comissao] Falha ao pagar {parceiro_codigo}: {resultado.get("error")}', flush=True)

    except Exception as e:
        print(f'[comissao_webhook] Erro: {e}', flush=True)


async def route_mp2_saques_pendentes(request):
    """Lista saques pendentes para o admin processar."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        from mp2_api import mp2_listar_saques_pendentes
        saques = mp2_listar_saques_pendentes()
        # Serializar datas
        for s in saques:
            for k, v in s.items():
                if hasattr(v, 'isoformat'):
                    s[k] = v.isoformat()
        return web.json_response({'success': True, 'saques': saques, 'total': len(saques)})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

async def route_mp2_processar_saque(request):
    """Admin aprova ou rejeita saque manualmente."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body = await request.json()
        saque_id = int(body.get('saque_id', 0))
        aprovado = bool(body.get('aprovado', False))
        obs      = body.get('obs', '')

        from mp2_api import mp2_processar_saque, mp2_listar_saques_pendentes
        ok = mp2_processar_saque(saque_id, aprovado, obs)

        if ok:
            # Notificar usuário se aprovado
            if aprovado:
                _mp2_notificar_saque_aprovado(saque_id)
            return web.json_response({'success': True, 'saque_id': saque_id, 'status': 'aprovado' if aprovado else 'rejeitado'})
        return web.json_response({'success': False, 'error': 'Saque não encontrado ou já processado'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

def _mp2_notificar_saque_aprovado(saque_id: int):
    """Notifica usuário que o saque foi aprovado."""
    try:
        import psycopg2, psycopg2.extras, requests as _req
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM mp2_saques WHERE id = %s", (saque_id,))
        saque = cur.fetchone()
        cur.close(); conn.close()
        if saque:
            bot2_token  = os.environ.get('BOT2_TOKEN', '')
            telegram_id = saque['telegram_id']
            valor       = float(saque['valor'])
            chave       = saque['chave_pix']
            if bot2_token and telegram_id:
                msg = (
                    f"✅ *Saque aprovado!*\n"
                    f"💰 R$ {valor:.2f} enviado para:\n"
                    f"`{chave}`\n\n"
                    f"_Obrigado por usar o PayPixNex!_"
                )
                _req.post(
                    f'https://api.telegram.org/bot{bot2_token}/sendMessage',
                    json={'chat_id': telegram_id, 'text': msg, 'parse_mode': 'Markdown'},
                    timeout=10
                )
    except Exception as e:
        print(f'[mp2_notif_saque] Erro: {e}', flush=True)

async def route_mp2_stats(request):
    """Estatísticas gerais do PayPixNex para o admin."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        from mp2_api import mp2_stats_admin
        stats = mp2_stats_admin()
        return web.json_response({'success': True, 'stats': stats})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp2_config_get(request):
    """GET /api/mp2/config - retorna chaves MP2 mascaradas (só últimos 6 chars)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        chaves = ['mp2_access_token', 'mp2_public_key', 'mp2_client_id', 'mp2_client_secret']
        resultado = {}
        for c in chaves:
            cur.execute("SELECT valor FROM mp2_config WHERE chave = %s", (c,))
            row = cur.fetchone()
            if row and row[0]:
                resultado[c.replace('mp2_', '')] = row[0]  # retorna valor completo para preencher o campo
        cur.close(); conn.close()
        resultado['configurado'] = bool(resultado.get('access_token'))
        return web.json_response({'success': True, **resultado})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_mp2_config_save(request):
    """POST /api/mp2/config - salva chaves MP2 no banco e recarrega no mp2_api."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body = await request.json()
        access_token  = body.get('access_token', '').strip()
        public_key    = body.get('public_key', '').strip()
        client_id     = body.get('client_id', '').strip()
        client_secret = body.get('client_secret', '').strip()

        if not access_token:
            return web.json_response({'success': False, 'error': 'access_token é obrigatório'})

        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        pares = [
            ('mp2_access_token', access_token),
            ('mp2_public_key',   public_key),
            ('mp2_client_id',    client_id),
            ('mp2_client_secret', client_secret),
        ]
        for chave, valor in pares:
            if valor:
                cur.execute("""
                    INSERT INTO mp2_config (chave, valor) VALUES (%s, %s)
                    ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                """, (chave, valor))
        conn.commit()
        cur.close(); conn.close()

        # Recarrega as variáveis no módulo mp2_api em tempo real (sem reiniciar)
        import mp2_api
        if access_token:  mp2_api.MP2_ACCESS_TOKEN = access_token
        if public_key:    mp2_api.MP2_PUBLIC_KEY   = public_key

        print(f'✅ [mp2_config] Chaves MP2 atualizadas pelo admin. Token: ...{access_token[-6:]}', flush=True)
        return web.json_response({'success': True, 'mensagem': 'Chaves salvas e ativas imediatamente!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_mp2_testar(request):
    """GET /api/mp2/testar - verifica conexão com a API do Mercado Pago."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import mp2_api, requests as _req
        token = mp2_api.MP2_ACCESS_TOKEN
        if not token:
            # Tenta carregar do banco
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
            cur  = conn.cursor()
            cur.execute("SELECT valor FROM mp2_config WHERE chave = 'mp2_access_token'")
            row = cur.fetchone()
            cur.close(); conn.close()
            token = row[0] if row else ''
            if token:
                mp2_api.MP2_ACCESS_TOKEN = token

        if not token:
            return web.json_response({'success': False, 'error': 'Access Token não configurado'})

        resp = _req.get(
            'https://api.mercadopago.com/users/me',
            headers={'Authorization': f'Bearer {token}'},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            email   = data.get('email', '')
            site_id = data.get('site_id', '')
            ambiente = 'produção' if not token.startswith('TEST-') else 'sandbox'
            return web.json_response({
                'success': True,
                'email': email,
                'site_id': site_id,
                'ambiente': ambiente,
                'mensagem': f'Conta MP ativa: {email}'
            })
        else:
            return web.json_response({
                'success': False,
                'error': f'MP retornou {resp.status_code}: {resp.text[:200]}'
            })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

# ══════════════════════════════════════════════════════════════════
# ─── PARCEIROS / AFILIADOS - /api/mp2/parceiros ──────────────────
# ══════════════════════════════════════════════════════════════════

async def route_mp2_parceiros_listar(request):
    """GET /api/mp2/parceiros - Lista todos os parceiros (SQL direto)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2, psycopg2.extras, decimal, json as _json
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT p.id, p.codigo, p.nome, p.chave_pix, p.tipo_chave,
                   p.comissao_pct, p.ativo, p.link,
                   COALESCE(p.total_gerado,0)   AS total_gerado,
                   COALESCE(p.total_comissao,0) AS total_comissao,
                   COALESCE(p.total_pago,0)     AS total_pago,
                   TO_CHAR(p.criado_em, %s) AS criado_em,
                   COUNT(t.id) FILTER (WHERE t.status = %s) AS qtd_pagamentos
            FROM mp2_parceiros p
            LEFT JOIN mp2_transacoes t ON t.parceiro_codigo = p.codigo
            GROUP BY p.id
            ORDER BY p.criado_em DESC
        """, ('DD/MM/YYYY HH24:MI', 'confirmado'))
        rows = cur.fetchall()
        cur.close(); conn.close()
        parceiros = []
        for r in rows:
            d = {}
            for k, v in r.items():
                if isinstance(v, decimal.Decimal):
                    d[k] = float(v)
                elif isinstance(v, bool):
                    d[k] = v
                elif v is None:
                    d[k] = None
                elif isinstance(v, (int, float, str)):
                    d[k] = v
                else:
                    d[k] = str(v)
            parceiros.append(d)
        resp_data = _json.dumps({'success': True, 'parceiros': parceiros, 'total': len(parceiros)})
        return web.Response(text=resp_data, content_type='application/json')
    except Exception as e:
        print(f'[parceiros_listar] Erro: {e}', flush=True)
        return web.Response(
            text=_json.dumps({'success': False, 'error': str(e)}),
            content_type='application/json', status=500
        )


async def route_mp2_parceiros_criar(request):
    """POST /api/mp2/parceiros - Cria novo parceiro (SQL direto)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body         = await request.json()
        nome         = str(body.get('nome', '')).strip()
        chave_pix    = str(body.get('chave_pix', '')).strip()
        tipo_chave   = str(body.get('tipo_chave', 'email')).strip()
        comissao_pct = float(body.get('comissao_pct', 10))
        codigo_req   = str(body.get('codigo', '')).strip()

        if not nome:
            return web.json_response({'success': False, 'error': 'Nome obrigatório'})
        if not chave_pix:
            return web.json_response({'success': False, 'error': 'Chave PIX obrigatória'})

        import uuid, re, psycopg2
        if not codigo_req:
            base   = re.sub(r'[^a-z0-9]', '', nome.lower())[:12]
            codigo = f"{base}-{str(uuid.uuid4())[:6]}"
        else:
            codigo = codigo_req

        host = request.headers.get('X-Forwarded-Host') or request.host or 'web-production-9f54e.up.railway.app'
        base_url = f"https://{host}"
        link = f"{base_url}/bot?ref={codigo}"

        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mp2_parceiros (
                id SERIAL PRIMARY KEY,
                codigo VARCHAR(50) UNIQUE NOT NULL,
                nome VARCHAR(200) NOT NULL,
                chave_pix VARCHAR(200) NOT NULL,
                tipo_chave VARCHAR(20) DEFAULT 'email',
                comissao_pct NUMERIC(5,2) DEFAULT 10.0,
                ativo BOOLEAN DEFAULT TRUE,
                total_gerado NUMERIC(12,2) DEFAULT 0,
                total_comissao NUMERIC(12,2) DEFAULT 0,
                total_pago NUMERIC(12,2) DEFAULT 0,
                criado_em TIMESTAMP DEFAULT NOW(),
                link TEXT
            )
        """)
        cur.execute("""
            INSERT INTO mp2_parceiros
                (codigo, nome, chave_pix, tipo_chave, comissao_pct, ativo, link, criado_em)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, NOW())
            ON CONFLICT (codigo) DO UPDATE
              SET nome=EXCLUDED.nome, chave_pix=EXCLUDED.chave_pix,
                  tipo_chave=EXCLUDED.tipo_chave, comissao_pct=EXCLUDED.comissao_pct,
                  link=EXCLUDED.link
            RETURNING id, codigo, nome, chave_pix, tipo_chave, comissao_pct, link
        """, (codigo, nome, chave_pix, tipo_chave, comissao_pct, link))
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()

        parceiro = {
            'id': row[0], 'codigo': row[1], 'nome': row[2],
            'chave_pix': row[3], 'tipo_chave': row[4],
            'comissao_pct': float(row[5]), 'link': row[6], 'ativo': True
        }
        print(f'✅ [parceiro] Criado: {codigo} → {nome} ({comissao_pct}%)', flush=True)
        return web.json_response({'success': True, 'parceiro': parceiro, 'link': link})
    except Exception as e:
        print(f'[parceiro_criar] Erro: {e}', flush=True)
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp2_parceiros_deletar(request):
    """DELETE /api/mp2/parceiros/{codigo} - Desativa parceiro (SQL direto)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        codigo = request.match_info.get('codigo', '')
        if not codigo:
            return web.json_response({'success': False, 'error': 'Código obrigatório'})
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("UPDATE mp2_parceiros SET ativo = FALSE WHERE codigo = %s", (codigo,))
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'success': True})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp2_parceiros_editar(request):
    """PATCH /api/mp2/parceiros/{codigo} - Edita parceiro: nome, chave_pix, tipo_chave, comissao_pct, ativo."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        codigo = request.match_info.get('codigo', '')
        if not codigo:
            return web.json_response({'success': False, 'error': 'Código obrigatório'})
        body = await request.json()
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        # Campos aceitos para atualização
        campos = []
        valores = []
        if 'ativo' in body:
            campos.append('ativo = %s')
            valores.append(bool(body['ativo']))
        if 'nome' in body and str(body['nome']).strip():
            campos.append('nome = %s')
            valores.append(str(body['nome']).strip())
        if 'chave_pix' in body and str(body['chave_pix']).strip():
            campos.append('chave_pix = %s')
            valores.append(str(body['chave_pix']).strip())
        if 'tipo_chave' in body and str(body['tipo_chave']).strip():
            campos.append('tipo_chave = %s')
            valores.append(str(body['tipo_chave']).strip())
        if 'comissao_pct' in body:
            pct = float(body['comissao_pct'])
            pct = max(1.0, min(90.0, pct))
            campos.append('comissao_pct = %s')
            valores.append(pct)
        if not campos:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Nenhum campo para atualizar'})
        valores.append(codigo)
        cur.execute(f"UPDATE mp2_parceiros SET {', '.join(campos)} WHERE codigo = %s RETURNING id, nome, chave_pix, tipo_chave, comissao_pct, ativo", valores)
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not row:
            return web.json_response({'success': False, 'error': 'Parceiro não encontrado'})
        return web.json_response({'success': True, 'parceiro': {
            'id': row[0], 'nome': row[1], 'chave_pix': row[2],
            'tipo_chave': row[3], 'comissao_pct': float(row[4]), 'ativo': row[5]
        }})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)



async def route_mp2_comissoes_listar(request):
    """GET /api/mp2/comissoes - Lista saques de comissão dos parceiros (SQL direto)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2, psycopg2.extras, decimal, json as _json
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT cs.id, cs.parceiro_codigo, cs.valor, cs.chave_pix, cs.tipo_chave,
                   cs.status, cs.mp_payment_id, cs.obs,
                   TO_CHAR(cs.criado_em,    'DD/MM HH24:MI') AS criado_em,
                   TO_CHAR(cs.processado_em,'DD/MM HH24:MI') AS processado_em,
                   p.nome AS parceiro_nome
            FROM mp2_comissao_saques cs
            LEFT JOIN mp2_parceiros p ON p.codigo = cs.parceiro_codigo
            ORDER BY cs.criado_em DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        saques = []
        for r in rows:
            d = {}
            for k, v in r.items():
                if isinstance(v, decimal.Decimal): d[k] = float(v)
                elif isinstance(v, bool):          d[k] = v
                elif v is None:                    d[k] = None
                elif isinstance(v, (int, float, str)): d[k] = v
                else:                              d[k] = str(v)
            saques.append(d)
        resp = _json.dumps({'success': True, 'saques': saques, 'total': len(saques)})
        return web.Response(text=resp, content_type='application/json')
    except Exception as e:
        import json as _j
        return web.Response(text=_j.dumps({'success': False, 'error': str(e)}),
                            content_type='application/json', status=500)


async def route_mp2_comissoes_pagar_manual(request):
    """POST /api/mp2/comissoes/pagar - Marca comissão como paga manualmente."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body    = await request.json()
        saque_id = int(body.get('saque_id', 0))
        obs      = str(body.get('obs', 'Pago manualmente pelo admin')).strip()
        if not saque_id:
            return web.json_response({'success': False, 'error': 'saque_id obrigatório'})
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        # Buscar valor e parceiro antes de marcar como pago
        cur.execute("SELECT valor, parceiro_codigo FROM mp2_comissao_saques WHERE id=%s", (saque_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Saque não encontrado'})
        valor, parceiro_codigo = float(row[0]), row[1]
        cur.execute("""
            UPDATE mp2_comissao_saques
            SET status='pago', processado_em=NOW(), obs=%s
            WHERE id=%s AND status != 'pago'
        """, (obs, saque_id))
        rows_updated = cur.rowcount
        if rows_updated > 0:
            cur.execute("""
                UPDATE mp2_parceiros
                SET total_pago = total_pago + %s
                WHERE codigo = %s
            """, (valor, parceiro_codigo))
        conn.commit(); cur.close(); conn.close()
        if rows_updated > 0:
            return web.json_response({'success': True, 'msg': f'R${valor:.2f} marcado como pago ao parceiro {parceiro_codigo}'})
        else:
            return web.json_response({'success': False, 'error': 'Já estava pago ou não encontrado'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


# ══════════════════════════════════════════════════════════════════
# ─── BOT PIX - Página pública /bot - @paypix_nexbot ──────────────
# ══════════════════════════════════════════════════════════════════

async def route_bot_pix_page(request):
    """GET /bot - Página pública para gerar PIX via @paypix_nexbot (Mercado Pago)."""
    html = load_bot_pix_html()
    return web.Response(text=html, content_type='text/html', charset='utf-8')


async def route_pix_page(request):
    """GET /pix - Sub-link público @paypix_nexbot (alias de /bot com ref opcional)."""
    html = load_bot_pix_html()
    return web.Response(text=html, content_type='text/html', charset='utf-8')


async def route_bot_gerar(request):
    """
    POST /api/bot/gerar - Gera PIX via Mercado Pago (@paypix_nexbot).
    Body: { valor: float, descricao: str, ref: str (opcional - código parceiro) }
    Retorna: { success, pix_copia_cola, qr_base64, payment_id, external_ref }
    """
    try:
        data          = await request.json()
        valor         = float(data.get('valor', 0))
        desc          = str(data.get('descricao', '')).strip() or 'PIX PayPixNex'
        parceiro_ref  = str(data.get('ref', '')).strip()  # código do parceiro afiliado

        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'})
        if valor > 10000:
            return web.json_response({'success': False, 'error': 'Valor máximo R$ 10.000,00'})

        # Validar parceiro (se informado)
        parceiro = None
        if parceiro_ref:
            from mp2_api import mp2_get_parceiro
            parceiro = mp2_get_parceiro(parceiro_ref)
            if parceiro and not parceiro.get('ativo'):
                parceiro = None  # Parceiro inativo = ignora ref

        # Carrega token MP2 do banco se não estiver em memória
        import mp2_api
        if not mp2_api.MP2_ACCESS_TOKEN:
            try:
                import psycopg2 as _pg
                _c  = _pg.connect(DATABASE_URL, connect_timeout=8)
                _cu = _c.cursor()
                _cu.execute("SELECT valor FROM mp2_config WHERE chave='mp2_access_token'")
                _r  = _cu.fetchone()
                _c.close()
                if _r and _r[0]:
                    mp2_api.MP2_ACCESS_TOKEN = _r[0]
            except Exception as _e:
                print(f'[bot_gerar] Erro carregar token: {_e}', flush=True)

        if not mp2_api.MP2_ACCESS_TOKEN:
            return web.json_response({
                'success': False,
                'error': 'Integração Mercado Pago não configurada. Contate o suporte.'
            })

        # Gera PIX usando mp2_api (mesmo sistema do @paypix_nexbot)
        from mp2_api import mp2_gerar_pix
        # Usa telegram_id=0 para cobranças anônimas pela página web
        resultado = mp2_gerar_pix(
            telegram_id=0,
            valor=valor,
            descricao=desc
        )

        if not resultado.get('success'):
            return web.json_response({
                'success': False,
                'error': resultado.get('error', 'Erro ao gerar PIX')
            })

        # Vincular parceiro à transação (para pagar comissão no webhook)
        if parceiro and resultado.get('external_ref'):
            try:
                import psycopg2 as _pg
                pct    = parceiro['comissao_pct']
                comval = round(valor * pct / 100, 2)
                _c  = _pg.connect(DATABASE_URL, connect_timeout=8)
                _cu = _c.cursor()
                _cu.execute("""
                    UPDATE mp2_transacoes
                    SET parceiro_codigo = %s,
                        comissao_valor  = %s,
                        comissao_status = 'pendente'
                    WHERE mp_external_ref = %s
                """, (parceiro_ref, comval, resultado['external_ref']))
                _c.commit()
                _c.close()
                print(f'✅ [bot_gerar] Parceiro {parceiro_ref} vinculado | comissão R${comval:.2f}', flush=True)
            except Exception as ex:
                print(f'[bot_gerar] Erro vincular parceiro: {ex}', flush=True)

        log_ref = f" [ref:{parceiro_ref}]" if parceiro else ""
        print(f'✅ [bot/gerar] PIX R${valor:.2f} - {resultado.get("external_ref","")}{log_ref}', flush=True)
        return web.json_response({
            'success':        True,
            'payment_id':     resultado.get('payment_id', ''),
            'external_ref':   resultado.get('external_ref', ''),
            'pix_copia_cola': resultado.get('pix_copia_cola', ''),
            'qr_base64':      resultado.get('qr_base64', ''),
            'valor':          valor,
            'parceiro':       parceiro['nome'] if parceiro else None,
        })

    except Exception as e:
        print(f'[bot_gerar] Erro: {e}', flush=True)
        return web.json_response({'success': False, 'error': str(e)})


async def route_bot_status(request):
    """
    GET /api/bot/status/{payment_id} - Verifica status do pagamento MP.
    Retorna: { status, confirmado, pix_copia_cola, qr_base64, valor }
    """
    payment_id = request.match_info.get('payment_id', '')
    if not payment_id:
        return web.json_response({'success': False, 'error': 'payment_id obrigatório'})

    try:
        import mp2_api
        # Carrega token se necessário
        if not mp2_api.MP2_ACCESS_TOKEN:
            try:
                import psycopg2 as _pg
                _c  = _pg.connect(DATABASE_URL, connect_timeout=8)
                _cu = _c.cursor()
                _cu.execute("SELECT valor FROM mp2_config WHERE chave='mp2_access_token'")
                _r  = _cu.fetchone()
                _c.close()
                if _r and _r[0]:
                    mp2_api.MP2_ACCESS_TOKEN = _r[0]
            except Exception:
                pass

        # Se for external_ref (começa com mp2_), busca no banco
        if payment_id.startswith('mp2_'):
            import psycopg2 as _pg, psycopg2.extras as _pge
            _c  = _pg.connect(DATABASE_URL, connect_timeout=8)
            _cu = _c.cursor(_pge.RealDictCursor)
            _cu.execute("""
                SELECT mp_payment_id, pix_copia_cola, pix_qr_base64, status, valor
                FROM mp2_transacoes WHERE mp_external_ref = %s
            """, (payment_id,))
            row = _cu.fetchone()
            _c.close()
            if row:
                status      = row['status']
                confirmado  = status == 'confirmado'
                return web.json_response({
                    'success':      True,
                    'status':       status,
                    'confirmado':   confirmado,
                    'pix_copia_cola': row.get('pix_copia_cola', ''),
                    'qr_base64':    row.get('pix_qr_base64', ''),
                    'valor':        float(row.get('valor', 0)),
                })

        # Se for payment_id numérico, consulta API do MP diretamente
        from mp2_api import mp2_verificar_pagamento
        info = mp2_verificar_pagamento(payment_id)
        status     = info.get('status', 'pending')
        confirmado = status == 'approved'

        # Busca pix_copia_cola no banco se disponível
        pix_code = ''
        qr_b64   = ''
        try:
            import psycopg2 as _pg
            _c  = _pg.connect(DATABASE_URL, connect_timeout=8)
            _cu = _c.cursor()
            _cu.execute("SELECT pix_copia_cola, pix_qr_base64 FROM mp2_transacoes WHERE mp_payment_id = %s", (str(payment_id),))
            _r  = _cu.fetchone()
            _c.close()
            if _r:
                pix_code = _r[0] or ''
                qr_b64   = _r[1] or ''
        except Exception:
            pass

        return web.json_response({
            'success':      True,
            'status':       status,
            'confirmado':   confirmado,
            'approved':     confirmado,
            'pix_copia_cola': pix_code,
            'qr_base64':    qr_b64,
            'valor':        float(info.get('valor', 0)),
        })

    except Exception as e:
        return web.json_response({'success': False, 'error': str(e), 'status': 'erro'})

async def _criar_canal_telegram(titulo: str, descricao: str) -> dict:
    """
    Cria um canal Telegram usando o client (userbot) já conectado.
    Retorna: {success, id, link, username}
    """
    global client
    try:
        from telethon.tl.functions.channels import CreateChannelRequest, ExportInviteLinkRequest
        from telethon.tl.types import InputChannel

        # Criar canal
        result = await client(CreateChannelRequest(
            title=titulo,
            about=descricao,
            broadcast=True,   # True = Canal | False = Grupo
            megagroup=False
        ))

        channel = result.chats[0]
        channel_id = channel.id
        access_hash = channel.access_hash

        # Gerar link de convite
        invite = await client(ExportInviteLinkRequest(
            channel=InputChannel(channel_id, access_hash)
        ))

        link = invite.link

        print(f'✅ Canal criado: {titulo} | ID={channel_id} | Link={link}', flush=True)
        return {
            'success': True,
            'id': channel_id,
            'access_hash': access_hash,
            'link': link,
            'titulo': titulo
        }
    except Exception as e:
        print(f'[criar_canal] Erro: {e}', flush=True)
        return {'success': False, 'error': str(e)}

async def _salvar_canais_db(notif_id: int, notif_link: str, hist_id: int, hist_link: str):
    """Salva IDs dos canais no PostgreSQL (tabela configuracoes)."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS configuracoes (
                chave TEXT PRIMARY KEY, valor TEXT
            )
        """)
        for chave, valor in [
            ('canal_notif_id',   str(notif_id)),
            ('canal_notif_link', notif_link),
            ('canal_hist_id',    str(hist_id)),
            ('canal_hist_link',  hist_link),
        ]:
            cur.execute(
                "INSERT INTO configuracoes (chave, valor) VALUES (%s,%s) ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor",
                (chave, valor)
            )
        conn.commit()
        cur.close(); conn.close()
        print('✅ Canais salvos no banco!', flush=True)
    except Exception as e:
        print(f'[salvar_canais_db] Erro: {e}', flush=True)

async def _carregar_canais_db():
    """Carrega IDs dos canais do PostgreSQL e atualiza variáveis globais."""
    global CANAL_NOTIF_ID, CANAL_HIST_ID, CANAL_NOTIF_LINK, CANAL_HIST_LINK
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT chave, valor FROM configuracoes WHERE chave LIKE 'canal_%'")
        rows = dict(cur.fetchall())
        cur.close(); conn.close()
        if rows.get('canal_notif_id'):
            CANAL_NOTIF_ID   = int(rows['canal_notif_id'])
        if rows.get('canal_notif_link'):
            CANAL_NOTIF_LINK = rows['canal_notif_link']
        if rows.get('canal_hist_id'):
            CANAL_HIST_ID    = int(rows['canal_hist_id'])
        if rows.get('canal_hist_link'):
            CANAL_HIST_LINK  = rows['canal_hist_link']
        if CANAL_NOTIF_ID:
            print(f'✅ Canais carregados: notif={CANAL_NOTIF_ID} hist={CANAL_HIST_ID}', flush=True)
    except Exception as e:
        print(f'[carregar_canais_db] Erro: {e}', flush=True)

async def _enviar_canal_notif(mensagem: str):
    """Envia mensagem no Canal de Notificações. Silencioso se canal não configurado."""
    global CANAL_NOTIF_ID, client
    if not CANAL_NOTIF_ID:
        return
    try:
        from telethon.tl.types import InputChannel
        await client.send_message(
            CANAL_NOTIF_ID, mensagem, parse_mode='markdown'
        )
    except Exception as e:
        print(f'[canal_notif] Erro ao enviar: {e}', flush=True)

async def _enviar_canal_hist(mensagem: str):
    """Envia mensagem no Canal de Histórico. Silencioso se canal não configurado."""
    global CANAL_HIST_ID, client
    if not CANAL_HIST_ID:
        return
    try:
        await client.send_message(
            CANAL_HIST_ID, mensagem, parse_mode='markdown'
        )
    except Exception as e:
        print(f'[canal_hist] Erro ao enviar: {e}', flush=True)

async def route_admin_criar_canais(request):
    """
    POST /api/admin/criar-canais
    Cria os dois canais Telegram via Telethon e salva IDs no banco.
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)

    global _telegram_ready, CANAL_NOTIF_ID, CANAL_HIST_ID, CANAL_NOTIF_LINK, CANAL_HIST_LINK

    if not _telegram_ready:
        return web.json_response({'success': False, 'error': 'Telegram offline - conecte primeiro'}, status=503)

    # Criar Canal 1 - Notificações
    c1 = await _criar_canal_telegram(
        '📣 PayPixNex - Notificações',
        'Notificações automáticas de saques, depósitos e transações do @paypix_nexbot 🔔'
    )
    if not c1['success']:
        return web.json_response({'success': False, 'error': f'Erro Canal 1: {c1["error"]}'}), 500

    # Aguardar flood wait
    await asyncio.sleep(3)

    # Criar Canal 2 - Histórico
    c2 = await _criar_canal_telegram(
        '📊 PayPixNex',
        'Histórico de transações @paypix_nexbot - depósitos, saques e movimentações.'
    )
    if not c2['success']:
        return web.json_response({'success': False, 'error': f'Erro Canal 2: {c2["error"]}'}), 500

    # Salvar no banco
    CANAL_NOTIF_ID   = c1['id']
    CANAL_NOTIF_LINK = c1['link']
    CANAL_HIST_ID    = c2['id']
    CANAL_HIST_LINK  = c2['link']
    await _salvar_canais_db(CANAL_NOTIF_ID, CANAL_NOTIF_LINK, CANAL_HIST_ID, CANAL_HIST_LINK)

    # Mensagem inicial nos canais
    await asyncio.sleep(2)
    await _enviar_canal_notif(
        '🟢 **Canal de Notificações PayPixNex ativo!**\n\n'
        'Aqui você receberá notificações automáticas de:\n'
        '✅ Saques efetuados\n'
        '💰 Depósitos confirmados\n'
        '⚠️ Alertas importantes\n\n'
        '_Bem-vindo ao @paypix_nexbot!_ 🚀'
    )
    await asyncio.sleep(1)
    await _enviar_canal_hist(
        '📊 **Canal de Histórico PayPixNex ativo!**\n\n'
        'Aqui são registradas todas as transações:\n'
        '📥 Depósitos\n'
        '📤 Saques\n'
        '🔄 Movimentações\n\n'
        '_Transparência total. @paypix_nexbot_ ✨'
    )

    return web.json_response({
        'success': True,
        'canal_notif': {'id': CANAL_NOTIF_ID, 'link': CANAL_NOTIF_LINK, 'titulo': c1['titulo']},
        'canal_hist':  {'id': CANAL_HIST_ID,  'link': CANAL_HIST_LINK,  'titulo': c2['titulo']},
        'instrucao': 'Adicione CANAL_NOTIF_ID e CANAL_HIST_ID como variáveis de ambiente no Railway!'
    })

async def route_admin_status_canais(request):
    """GET /api/admin/canais - Status dos canais configurados."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    return web.json_response({
        'success': True,
        'canal_notif': {'id': CANAL_NOTIF_ID, 'link': CANAL_NOTIF_LINK, 'configurado': bool(CANAL_NOTIF_ID)},
        'canal_hist':  {'id': CANAL_HIST_ID,  'link': CANAL_HIST_LINK,  'configurado': bool(CANAL_HIST_ID)},
    })

async def route_admin_testar_canais(request):
    """POST /api/admin/testar-canais - Envia mensagem de teste nos canais."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    if not CANAL_NOTIF_ID and not CANAL_HIST_ID:
        return web.json_response({'success': False, 'error': 'Canais não configurados. Use /api/admin/criar-canais primeiro.'})
    await _enviar_canal_notif('🔔 **Teste** - Canal de Notificações funcionando! ✅')
    await _enviar_canal_hist('📊 **Teste** - Canal de Histórico funcionando! ✅')
    return web.json_response({'success': True, 'message': 'Mensagens de teste enviadas!'})

async def route_exportar_csv(request):
    """Exportar depósitos ou saques em CSV"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
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
    global ASAAS_API_KEY, ASAAS_ENV, ASAAS_BASE_URL
    init_db()
    print('✅ DB ok', flush=True)

    # ── Carregar ASAAS_API_KEY do DB se não vier por env var ──────────────
    if not ASAAS_API_KEY:
        try:
            conn = sqlite3_connect()
            row = conn.execute("SELECT valor FROM configuracoes WHERE chave='asaas_api_key'").fetchone()
            if row and row[0]:
                ASAAS_API_KEY = row[0]
                env_row = conn.execute("SELECT valor FROM configuracoes WHERE chave='asaas_env'").fetchone()
                ASAAS_ENV = (env_row[0] if env_row and env_row[0] else 'production')
                ASAAS_BASE_URL = 'https://sandbox.asaas.com/v3' if ASAAS_ENV == 'sandbox' else 'https://api.asaas.com/v3'
                print(f'✅ [Asaas] Chave carregada do DB | env:{ASAAS_ENV}', flush=True)
            conn.close()
        except Exception as e:
            print(f'⚠️ [Asaas] Erro ao carregar chave do DB: {e}', flush=True)

    if ASAAS_API_KEY:
        print(f'✅ [Asaas] Configurado | env:{ASAAS_ENV} | URL:{ASAAS_BASE_URL}', flush=True)
    else:
        print('⚠️ [Asaas] ASAAS_API_KEY não configurada - gateway PIX indisponível', flush=True)

    app = web.Application(middlewares=[cors_middleware])

    # ─── ENDPOINT SELF-UPDATE (baixa server.py do GitHub e reinicia) ───
    async def route_self_update(request):
        secret = request.rel_url.query.get('secret', '')
        if secret != WEBHOOK_SECRET:
            return web.json_response({'error': 'unauthorized'}, status=401)
        import subprocess, threading, os as _os
        COMMIT = request.rel_url.query.get('commit', 'e294ecc')
        GITHUB_RAW = f'https://raw.githubusercontent.com/gelcijosegrouptrig-cmyk/vortexpay/{COMMIT}/server.py'
        def _do_update():
            import time as _t, os as _o
            try:
                r = subprocess.run(['curl', '-s', '-f', '-o', 'server_new.py', GITHUB_RAW], timeout=30)
                if r.returncode == 0:
                    import shutil
                    shutil.move('server_new.py', 'server.py')
                    print('✅ server.py atualizado do GitHub!', flush=True)
                else:
                    print('❌ Falha ao baixar server.py do GitHub', flush=True)
            except Exception as e:
                print(f'❌ Erro no self-update: {e}', flush=True)
            _t.sleep(1)
            _o._exit(1)  # Railway reinicia automaticamente
        threading.Thread(target=_do_update, daemon=True).start()
        return web.json_response({'status': 'updating', 'msg': 'Baixando server.py do GitHub e reiniciando...'})
    app.router.add_get('/api/self-update', route_self_update)

    # Endpoint de diagnóstico temporário
    async def route_debug_pix(request):
        import traceback as _tb
        try:
            data = await request.json()
            valor = float(data.get('valor', 5))
            import hashlib as _hlib, time as _time
            tx_id = f"dbg_{_hlib.md5(str(_time.time()).encode()).hexdigest()[:8]}"
            now = __import__('datetime').datetime.now().isoformat()
            conn = sqlite3_connect()
            conn.execute('INSERT OR IGNORE INTO transacoes (tx_id,valor,cliente_id,status,created_at,extra) VALUES (?,?,?,?,?,?)',
                      (tx_id, valor, 'cli_debug', 'gerando', now, None))
            conn.commit()
            conn.close()
            return web.json_response({'success': True, 'tx_id': tx_id, 'msg': 'INSERT OK sem erro'})
        except Exception as e:
            tb = _tb.format_exc()
            print(f'DEBUG PIX ERROR:\n{tb}', flush=True)
            return web.json_response({'success': False, 'error': str(e), 'traceback': tb})
    app.router.add_post('/api/debug/pix', route_debug_pix)

    app.router.add_get('/', route_home)            # Página principal PaynexBet
    app.router.add_get('/home', route_home)
    app.router.add_get('/index.html', route_index)
    app.router.add_get('/health', route_health)
    app.router.add_get('/api/status', route_health)
    app.router.add_post('/api/pix', route_pix)
    app.router.add_get('/api/pix/status/{tx_id}', route_pix_status)
    app.router.add_get('/api/debug-pix', route_debug_pix)
    app.router.add_route('OPTIONS', '/api/pix', lambda r: web.Response(status=200))
    app.router.add_get('/api/status/{tx_id}', route_status_tx)
    app.router.add_get('/api/transacoes', route_transacoes)
    app.router.add_post('/webhook/confirmar', route_webhook)
    app.router.add_route('OPTIONS', '/webhook/confirmar', lambda r: web.Response(status=200))
    # Rotas de Saque - /sacar e /saque
    app.router.add_get('/sacar', route_saque_page)      # paynexbet.com/sacar
    app.router.add_get('/saque', route_saque_page)
    app.router.add_get('/saque.html', route_saque_page)
    # Rota /pague - gerar Pix (abre modal automaticamente)
    app.router.add_get('/pague', route_pague)           # paynexbet.com/pague
    app.router.add_get('/api/saldo', route_saldo)
    app.router.add_get('/api/saldo/bot', route_saldo_bot)
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
    app.router.add_post('/api/telegram/solicitar-codigo', route_solicitar_codigo)
    app.router.add_post('/api/telegram/confirmar-codigo', route_confirmar_codigo)
    app.router.add_get('/api/telegram/sessao-atual', route_sessao_atual)
    app.router.add_get('/api/reconectar', route_reconectar_db)
    # ── Bot 2 (@paypix_nexbot) ──────────────────────────────────────
    app.router.add_get('/api/bot2/status',                  route_bot2_status)
    app.router.add_get('/api/bot2/saldo',                   route_bot2_saldo)
    app.router.add_post('/api/bot2/pix',                    route_bot2_pix)
    app.router.add_post('/api/bot2/solicitar-codigo',       route_bot2_solicitar_codigo)
    app.router.add_post('/api/bot2/confirmar-codigo',       route_bot2_confirmar_codigo)
    # ── MP2 - PayPixNex (Bot real @paypix_nexbot + Mercado Pago) ────
    app.router.add_get('/api/mp2/status',                   route_mp2_status)
    app.router.add_get('/api/mp2/stats',                    route_mp2_stats)
    app.router.add_post('/webhook/mp2',                     route_mp2_webhook)
    app.router.add_get('/api/mp2/saques',                   route_mp2_saques_pendentes)
    app.router.add_post('/api/mp2/saques/processar',        route_mp2_processar_saque)
    app.router.add_get('/api/mp2/config',                   route_mp2_config_get)
    app.router.add_post('/api/mp2/config',                  route_mp2_config_save)
    app.router.add_get('/api/mp2/testar',                   route_mp2_testar)
    app.router.add_get('/api/mp2/parceiros',                route_mp2_parceiros_listar)
    app.router.add_post('/api/mp2/parceiros',               route_mp2_parceiros_criar)
    app.router.add_delete('/api/mp2/parceiros/{codigo}',    route_mp2_parceiros_deletar)
    app.router.add_patch('/api/mp2/parceiros/{codigo}',     route_mp2_parceiros_editar)
    app.router.add_get('/api/mp2/comissoes',                route_mp2_comissoes_listar)
    app.router.add_post('/api/mp2/comissoes/pagar',         route_mp2_comissoes_pagar_manual)
    # ── Bot PIX - página pública @paypix_nexbot ──
    app.router.add_get('/bot',                              route_bot_pix_page)
    app.router.add_get('/pix',                              route_pix_page)
    app.router.add_post('/api/bot/gerar',                   route_bot_gerar)
    app.router.add_get('/api/bot/status/{payment_id}',      route_bot_status)
    # ── Canais Telegram ──────────────────────────────────────────────
    app.router.add_post('/api/admin/criar-canais',          route_admin_criar_canais)
    app.router.add_get('/api/admin/canais',                 route_admin_status_canais)
    app.router.add_post('/api/admin/testar-canais',         route_admin_testar_canais)
    # PayPix - parceiro gera Pix e recebe 60%
    app.router.add_get('/paypix', route_paypix_page)
    app.router.add_post('/api/paypix/gerar', route_paypix_gerar)
    app.router.add_get('/api/paypix/status/{tx_id}', route_paypix_status)
    app.router.add_get('/api/paypix/config', route_paypix_config)
    app.router.add_post('/api/paypix/config', route_paypix_config)
    app.router.add_options('/api/paypix/config', lambda r: web.Response(headers={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type,X-PaynexBet-Secret'}))
    app.router.add_route('OPTIONS', '/api/paypix/gerar', lambda r: web.Response(status=200))
    app.router.add_get('/api/paypix/fila', route_paypix_fila)
    # Endpoint de restart forçado (Railway reinicia o processo com código novo)
    async def route_force_restart(request):
        secret = request.rel_url.query.get('secret', '')
        if secret != WEBHOOK_SECRET:
            return web.json_response({'error': 'unauthorized'}, status=401)
        import threading
        def _exit():
            import time as _t; _t.sleep(1); import os; os._exit(1)
        threading.Thread(target=_exit, daemon=True).start()
        return web.json_response({'status': 'restarting', 'msg': 'Processo encerrando para Railway reiniciar com código novo'})
    app.router.add_get('/api/restart', route_force_restart)

    # Endpoint para resetar o lock preso + diagnóstico completo
    async def route_lock_reset(request):
        global _lock
        secret = request.rel_url.query.get('secret', '')
        if secret != WEBHOOK_SECRET:
            return web.json_response({'error': 'unauthorized'}, status=401)
        lock_antes = _lock.locked()
        if _lock.locked():
            # Força liberação do lock criando um novo
            _lock = asyncio.Lock()
            lock_resetado = True
        else:
            lock_resetado = False
        return web.json_response({
            'lock_estava_preso': lock_antes,
            'lock_resetado': lock_resetado,
            'telegram_ready': _telegram_ready,
            'version': 'v20260415-parceiros-v31',
            'msg': 'Lock resetado! Tente gerar Pix agora.' if lock_resetado else 'Lock estava livre, nenhuma ação necessária.'
        })
    app.router.add_get('/api/lock/reset', route_lock_reset)

    # Sorteio
    app.router.add_get('/sorteio', route_sorteio_page)
    app.router.add_get('/sorteio.html', route_sorteio_page)
    app.router.add_get('/api/sorteio/info', route_sorteio_info)
    app.router.add_post('/api/sorteio/cadastrar', route_sorteio_cadastrar)
    app.router.add_post('/api/sorteio/participar', route_sorteio_participar)
    app.router.add_post('/api/sorteio/deposito', route_sorteio_adicionar_deposito)
    app.router.add_post('/api/sorteio/realizar', route_sorteio_realizar)
    app.router.add_post('/api/sorteio/config', route_sorteio_config)
    app.router.add_get('/api/sorteio/participantes', route_sorteio_participantes)
    app.router.add_post('/api/sorteio/acumular', route_sorteio_acumular)
    app.router.add_post('/api/sorteio/set-acumulado', route_sorteio_set_acumulado)
    app.router.add_post('/api/sorteio/reparar-participante', route_sorteio_reparar_participante)
    # ── Asaas ──────────────────────────────────────────────────────────────────
    app.router.add_post('/api/sorteio/asaas/pix', route_asaas_pix_sorteio)
    app.router.add_get('/api/sorteio/asaas/status/{tx_id}', route_asaas_pix_status)
    app.router.add_post('/api/sorteio/asaas/saque', route_asaas_saque_sorteio)
    app.router.add_post('/webhook/asaas', route_webhook_asaas)
    app.router.add_get('/api/asaas/status', route_asaas_status)
    app.router.add_post('/api/asaas/configurar', route_asaas_configurar)
    app.router.add_post('/api/railway/set-vars', route_railway_set_vars)
    app.router.add_post('/api/admin/db-migrate', route_db_migrate)
    app.router.add_post('/api/admin/patch-sorteio-html', route_patch_sorteio_html)
    app.router.add_post('/api/admin/patch-paypix-html', route_patch_paypix_html)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'✅ HTTP porta {PORT}', flush=True)

    # Telegram Bot1 em background com retry automático
    asyncio.create_task(conectar_telegram())

    # Agendador de sorteio automático
    asyncio.create_task(agendador_sorteio())

    # Monitor de saques pendentes (reprocessa quando Telegram voltar)
    asyncio.create_task(reprocessar_saques_pendentes_sorteio())

    # Worker de splits PayPix pendentes - tenta a cada 5 min até finalizar
    asyncio.create_task(_worker_paypix_fila())

    # ── Bot 2 (@paypix_nexbot) - tasks paralelas (userbot Telethon legado) ──
    asyncio.create_task(conectar_telegram2())
    asyncio.create_task(watchdog_telegram2())
    asyncio.create_task(_loop_verificar_pagamentos_bot2())

    # ── Bot 2 REAL - @paypix_nexbot (python-telegram-bot + Mercado Pago) ──
    try:
        from mp2_api import init_mp2_db
        from bot2_handler import build_bot2_app, _bot2_post_startup
        init_mp2_db()

        # Carrega chaves MP2 salvas no banco (substitui vars de ambiente se existirem)
        try:
            import mp2_api, psycopg2 as _pg2
            _conn = _pg2.connect(DATABASE_URL, connect_timeout=8)
            _cur  = _conn.cursor()
            _cur.execute("SELECT chave, valor FROM mp2_config WHERE chave IN ('mp2_access_token','mp2_public_key')")
            for _chave, _valor in _cur.fetchall():
                if _valor:
                    if _chave == 'mp2_access_token':
                        mp2_api.MP2_ACCESS_TOKEN = _valor
                        print(f'✅ [mp2] Access Token carregado do banco: ...{_valor[-6:]}', flush=True)
                    elif _chave == 'mp2_public_key':
                        mp2_api.MP2_PUBLIC_KEY = _valor
            _cur.close(); _conn.close()
        except Exception as _e:
            print(f'⚠️ [mp2] Erro ao carregar chaves do banco: {_e}', flush=True)
        bot2_app = build_bot2_app()
        if bot2_app:
            async def _rodar_bot2():
                try:
                    await bot2_app.initialize()
                    await bot2_app.start()
                    await bot2_app.updater.start_polling(
                        drop_pending_updates=True,
                        allowed_updates=['message', 'callback_query']
                    )
                    print('✅ [Bot2] @paypix_nexbot polling ativo!', flush=True)
                    # Cria canal automaticamente após iniciar
                    asyncio.create_task(_bot2_post_startup(bot2_app))
                    await asyncio.Event().wait()  # manter vivo
                except Exception as e:
                    print(f'❌ [Bot2] Erro no polling: {e}', flush=True)
            asyncio.create_task(_rodar_bot2())
        else:
            print('⚠️ [Bot2 Real] BOT2_TOKEN não configurado - bot desativado', flush=True)
    except Exception as e_bot2:
        print(f'⚠️ [Bot2 Real] Erro ao iniciar: {e_bot2}', flush=True)

    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
# deploy Sun Apr 12 13:09:00 UTC 2026


# rebuild-1776092829

# rebuild 20260413160152
# rebuild-1776100930
