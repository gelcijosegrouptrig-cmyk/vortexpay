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
    # ── v31: carregar do banco primeiro (deploy instantâneo sem redeploy) ──
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='home_html_patch'")
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
    # Fallback: arquivo em disco
    if os.path.exists('home.html'):
        return open('home.html', encoding='utf-8').read()
    return '<h1>PaynexBet</h1>'

def load_html():
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='index_html_patch'")
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
    if os.path.exists('index.html'):
        return open('index.html', encoding='utf-8').read()
    return '<h1>PaynexBet</h1>'

def load_saque_html():
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave='saque_html_patch'")
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 1000:
                return _row[0]
    except Exception:
        pass
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
            # Tenta a chave principal (_patch) e também a chave legada (sem _patch)
            _cur.execute("SELECT chave, valor FROM configuracoes WHERE chave IN ('bot_pix_html_patch','bot_pix_html') ORDER BY (chave='bot_pix_html_patch') DESC")
            rows = _cur.fetchall()
            _c.close()
            for _chave, _val in rows:
                if _val and len(_val) > 1000:
                    return _val
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
            'min_participantes','acumulativo',
            'part_manual','bilhetes_manual','deposito_manual','premio_manual']
    cols_base = cols[:16]  # colunas sem os campos manuais (fallback)
    conn = sqlite3_connect()
    try:
        cur = conn.execute(f'SELECT {", ".join(cols)} FROM sorteio_config WHERE id=1')
        row = cur.fetchone()
        conn.close()
        if row:
            return {col: row[i] for i, col in enumerate(cols)}
        return {}
    except Exception:
        # Colunas manuais ainda não existem — tentar sem elas
        try:
            cur = conn.execute(f'SELECT {", ".join(cols_base)} FROM sorteio_config WHERE id=1')
            row = cur.fetchone()
            conn.close()
            if row:
                d = {col: row[i] for i, col in enumerate(cols_base)}
                for c in ['part_manual','bilhetes_manual','deposito_manual','premio_manual']:
                    d[c] = None
                return d
        except Exception:
            pass
        conn.close()
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
            # Ajustes manuais dos cards (None = usar calculado)
            'part_manual':     int(config['part_manual'])     if config.get('part_manual')     is not None else None,
            'bilhetes_manual': int(config['bilhetes_manual']) if config.get('bilhetes_manual') is not None else None,
            'deposito_manual': float(config['deposito_manual']) if config.get('deposito_manual') is not None else None,
            'premio_manual':   float(config['premio_manual'])   if config.get('premio_manual')   is not None else None,
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
            int(data.get('min_participantes', 1)),
        )
        # Campos de ajuste manual (None = usar valor calculado automaticamente)
        def _opt_float(key):
            v = data.get(key)
            return float(v) if v is not None else None
        def _opt_int(key):
            v = data.get(key)
            return int(v) if v is not None else None

        part_manual     = _opt_int('part_manual')
        bilhetes_manual = _opt_int('bilhetes_manual')
        deposito_manual = _opt_float('deposito_manual')
        premio_manual   = _opt_float('premio_manual')

        if _USE_PG and DATABASE_URL:
            import psycopg2
            pg = psycopg2.connect(DATABASE_URL)
            pg.autocommit = True
            cur = pg.cursor()
            for mig in [
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS acumulativo INTEGER DEFAULT 1",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS min_participantes INTEGER DEFAULT 1",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS premio_acumulado REAL DEFAULT 0",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS part_manual INTEGER DEFAULT NULL",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS bilhetes_manual INTEGER DEFAULT NULL",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS deposito_manual REAL DEFAULT NULL",
                "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS premio_manual REAL DEFAULT NULL",
            ]:
                try: cur.execute(mig)
                except Exception: pass
            pg.autocommit = False
            # COALESCE: preserva valor existente quando frontend envia null (campo deixado em branco)
            cur.execute('''UPDATE sorteio_config SET
                ativo=%s, valor_por_numero=%s, percentual=%s, usar_media=%s, dias_media=%s,
                premio_fixo=%s, descricao=%s, proximo_sorteio=%s, updated_at=%s,
                acumulativo=%s, min_participantes=%s,
                part_manual=COALESCE(%s, part_manual),
                bilhetes_manual=COALESCE(%s, bilhetes_manual),
                deposito_manual=COALESCE(%s, deposito_manual),
                premio_manual=COALESCE(%s, premio_manual)
                WHERE id=1''', params + (part_manual, bilhetes_manual, deposito_manual, premio_manual))
            pg.commit()
            pg.close()
        else:
            conn = sqlite3_connect()
            for mig in [
                "ALTER TABLE sorteio_config ADD COLUMN part_manual INTEGER",
                "ALTER TABLE sorteio_config ADD COLUMN bilhetes_manual INTEGER",
                "ALTER TABLE sorteio_config ADD COLUMN deposito_manual REAL",
                "ALTER TABLE sorteio_config ADD COLUMN premio_manual REAL",
            ]:
                try: conn.execute(mig)
                except Exception: pass
            # COALESCE: preserva valor existente quando frontend envia null (campo deixado em branco)
            conn.execute('''UPDATE sorteio_config SET
                ativo=?, valor_por_numero=?, percentual=?, usar_media=?, dias_media=?,
                premio_fixo=?, descricao=?, proximo_sorteio=?, updated_at=?,
                acumulativo=?, min_participantes=?,
                part_manual=COALESCE(?, part_manual),
                bilhetes_manual=COALESCE(?, bilhetes_manual),
                deposito_manual=COALESCE(?, deposito_manual),
                premio_manual=COALESCE(?, premio_manual)
                WHERE id=1''', params + (part_manual, bilhetes_manual, deposito_manual, premio_manual))
            conn.commit(); conn.close()
        return web.json_response({'success': True, 'message': 'Configuração salva!'})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def route_sorteio_limpar_manuais(request):
    """ADMIN - Limpar todos os valores manuais dos cards (volta ao cálculo automático)"""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        if _USE_PG and DATABASE_URL:
            import psycopg2
            pg = psycopg2.connect(DATABASE_URL)
            pg.autocommit = False
            cur = pg.cursor()
            cur.execute('''UPDATE sorteio_config SET
                part_manual=NULL, bilhetes_manual=NULL, deposito_manual=NULL, premio_manual=NULL
                WHERE id=1''')
            pg.commit(); pg.close()
        else:
            conn = sqlite3_connect()
            conn.execute('''UPDATE sorteio_config SET
                part_manual=NULL, bilhetes_manual=NULL, deposito_manual=NULL, premio_manual=NULL
                WHERE id=1''')
            conn.commit(); conn.close()
        return web.json_response({'success': True})
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
        "ALTER TABLE sorteio_config ADD COLUMN IF NOT EXISTS min_participantes INTEGER DEFAULT 1",
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


async def route_patch_admin_html(request):
    """POST /api/admin/patch-admin-html - Salva admin.html no PostgreSQL (patch permanente sem redeploy)"""
    auth = (request.headers.get('X-PaynexBet-Secret','') or request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        html_content = data.get('html', '')
        if not html_content or len(html_content) < 5000:
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
                VALUES ('admin_html_patch', %s, %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = EXCLUDED.atualizado_em
            """, (html_content, _dt.datetime.utcnow().isoformat()))
        else:
            cur.execute("""
                INSERT INTO configuracoes (chave, valor)
                VALUES ('admin_html_patch', %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (html_content,))
        pg.close()
        print(f'🔧 admin.html patch salvo no PostgreSQL: {len(html_content)} chars', flush=True)
        return web.json_response({'success': True, 'chars': len(html_content), 'msg': 'admin.html atualizado no PostgreSQL — recarregue o painel'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


# ═══════════════════════════════════════════════════════════════════
# ADMIN - GERENCIADOR DE CAMPEONATOS (LIGAS)
# ═══════════════════════════════════════════════════════════════════

# Mapa completo de ligas gerenciáveis
_ADMIN_LIGAS_MAP = {
    'soccer_brazil_campeonato':          {'nome': '🇧🇷 Brasileirão Série A', 'espn': 'bra.1',                  'categoria': 'soccer'},
    'soccer_brazil_serie_b':             {'nome': '🇧🇷 Série B',             'espn': 'bra.2',                  'categoria': 'soccer'},
    'soccer_conmebol_copa_libertadores': {'nome': '🏆 Libertadores',          'espn': 'CONMEBOL.LIBERTADORES',  'categoria': 'soccer'},
    'soccer_conmebol_sudamericana':      {'nome': '🌎 Sul-Americana',         'espn': 'CONMEBOL.SUDAMERICANA',  'categoria': 'soccer'},
    'soccer_uefa_champs_league':         {'nome': '⭐ Champions League',      'espn': 'UEFA.CHAMPIONS',         'categoria': 'soccer'},
    'soccer_epl':                        {'nome': '🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League',   'espn': 'eng.1',                  'categoria': 'soccer'},
    'soccer_spain_la_liga':              {'nome': '🇪🇸 La Liga',              'espn': 'esp.1',                  'categoria': 'soccer'},
    'soccer_italy_serie_a':              {'nome': '🇮🇹 Serie A',              'espn': 'ita.1',                  'categoria': 'soccer'},
    'soccer_germany_bundesliga':         {'nome': '🇩🇪 Bundesliga',           'espn': 'ger.1',                  'categoria': 'soccer'},
    'soccer_france_ligue_one':           {'nome': '🇫🇷 Ligue 1',              'espn': 'fra.1',                  'categoria': 'soccer'},
    'basketball_nba':                    {'nome': '🏀 NBA',                   'espn': 'nba',                    'categoria': 'basketball'},
    'americanfootball_nfl':              {'nome': '🏈 NFL',                   'espn': 'nfl',                    'categoria': 'football'},
    'mma_mixed_martial_arts':            {'nome': '🥊 MMA / UFC',             'espn': 'ufc',                    'categoria': 'mma'},
}

def _admin_auth(request):
    """Verifica autenticação admin via header ou query param."""
    auth = (request.headers.get('X-PaynexBet-Secret','') or
            request.headers.get('x-paynexbet-secret','') or
            request.rel_url.query.get('secret',''))
    return auth == WEBHOOK_SECRET

def _admin_db_connect():
    """Conecta ao PostgreSQL para operações admin."""
    import psycopg2 as _pg2
    url = DATABASE_URL or _BET_DB_URL_FALLBACK
    return _pg2.connect(url)

def _admin_get_liga_config(liga_key):
    """Retorna config salva da liga no DB (suspensa, api_pagamento)."""
    try:
        conn = _admin_db_connect()
        cur = conn.cursor()
        chave = f'liga_cfg_{liga_key}'
        cur.execute("SELECT valor FROM configuracoes WHERE chave=%s", (chave,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            import json as _j
            return _j.loads(row[0])
    except Exception:
        pass
    return {}

def _admin_save_liga_config(liga_key, cfg):
    """Salva config da liga no DB."""
    import json as _j
    conn = _admin_db_connect()
    cur = conn.cursor()
    chave = f'liga_cfg_{liga_key}'
    val = _j.dumps(cfg)
    cur.execute("""
        INSERT INTO configuracoes (chave, valor)
        VALUES (%s, %s)
        ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor
    """, (chave, val))
    conn.commit(); cur.close(); conn.close()

async def route_admin_ligas_list(request):
    """GET /api/admin/ligas — lista todas as ligas com status e config."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    ligas = []
    for key, info in _ADMIN_LIGAS_MAP.items():
        cfg = _admin_get_liga_config(key)
        ligas.append({
            'key': key,
            'nome': info['nome'],
            'categoria': info['categoria'],
            'espn': info['espn'],
            'suspensa': cfg.get('suspensa', False),
            'api_pagamento': cfg.get('api_pagamento', 'suitpay'),
            'odds_habilitadas': cfg.get('odds_habilitadas', True),
            'nota': cfg.get('nota', ''),
        })
    return web.json_response({'ok': True, 'ligas': ligas})

async def route_admin_ligas_suspender(request):
    """POST /api/admin/ligas/suspender — suspende ou ativa uma liga."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        liga_key = body.get('liga_key', '')
        suspensa = bool(body.get('suspensa', False))
        if liga_key not in _ADMIN_LIGAS_MAP:
            return web.json_response({'ok': False, 'error': 'Liga não encontrada'}, status=400)
        cfg = _admin_get_liga_config(liga_key)
        cfg['suspensa'] = suspensa
        _admin_save_liga_config(liga_key, cfg)
        status = 'SUSPENSA' if suspensa else 'ATIVA'
        print(f'[admin/ligas] {liga_key} → {status}', flush=True)
        return web.json_response({'ok': True, 'liga_key': liga_key, 'suspensa': suspensa,
                                  'msg': f'Liga {_ADMIN_LIGAS_MAP[liga_key]["nome"]} → {status}'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_ligas_config(request):
    """POST /api/admin/ligas/config — configura api_pagamento, odds, nota de uma liga."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        liga_key = body.get('liga_key', '')
        if liga_key not in _ADMIN_LIGAS_MAP:
            return web.json_response({'ok': False, 'error': 'Liga não encontrada'}, status=400)
        cfg = _admin_get_liga_config(liga_key)
        if 'api_pagamento' in body:
            cfg['api_pagamento'] = body['api_pagamento']  # 'suitpay' | 'asaas' | 'manual'
        if 'odds_habilitadas' in body:
            cfg['odds_habilitadas'] = bool(body['odds_habilitadas'])
        if 'nota' in body:
            cfg['nota'] = str(body['nota'])[:200]
        _admin_save_liga_config(liga_key, cfg)
        return web.json_response({'ok': True, 'liga_key': liga_key, 'config': cfg})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


# ═══════════════════════════════════════════════════════════════════
# ADMIN - GERENCIADOR DE USUÁRIOS
# ═══════════════════════════════════════════════════════════════════

async def route_admin_usuarios_list(request):
    """GET /api/admin/usuarios — lista usuários com saldo, apostas, status."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        busca = request.rel_url.query.get('busca', '').strip()
        limit = int(request.rel_url.query.get('limit', 50))
        offset = int(request.rel_url.query.get('offset', 0))
        conn = _admin_db_connect()
        cur = conn.cursor()
        # Contar total
        if busca:
            cur.execute("""
                SELECT COUNT(*) FROM usuarios
                WHERE nome ILIKE %s OR cpf ILIKE %s
            """, (f'%{busca}%', f'%{busca}%'))
        else:
            cur.execute("SELECT COUNT(*) FROM usuarios")
        total = cur.fetchone()[0]
        # Listar usuários com contagem de apostas
        if busca:
            cur.execute("""
                SELECT u.id, u.nome, u.cpf, u.saldo,
                       COALESCE(u.suspendido, false) as suspendido,
                       COUNT(a.id) as total_apostas,
                       COALESCE(SUM(CASE WHEN a.status='WON' THEN 1 ELSE 0 END),0) as apostas_won,
                       COALESCE(SUM(a.valor),0) as volume_apostado
                FROM usuarios u
                LEFT JOIN apostas a ON a.usuario_id = u.id
                WHERE u.nome ILIKE %s OR u.cpf ILIKE %s
                GROUP BY u.id, u.nome, u.cpf, u.saldo, u.suspendido
                ORDER BY u.id DESC LIMIT %s OFFSET %s
            """, (f'%{busca}%', f'%{busca}%', limit, offset))
        else:
            cur.execute("""
                SELECT u.id, u.nome, u.cpf, u.saldo,
                       COALESCE(u.suspendido, false) as suspendido,
                       COUNT(a.id) as total_apostas,
                       COALESCE(SUM(CASE WHEN a.status='WON' THEN 1 ELSE 0 END),0) as apostas_won,
                       COALESCE(SUM(a.valor),0) as volume_apostado
                FROM usuarios u
                LEFT JOIN apostas a ON a.usuario_id = u.id
                GROUP BY u.id, u.nome, u.cpf, u.saldo, u.suspendido
                ORDER BY u.id DESC LIMIT %s OFFSET %s
            """, (limit, offset))
        rows = cur.fetchall()
        cur.close(); conn.close()
        usuarios = []
        for r in rows:
            usuarios.append({
                'id': r[0], 'nome': r[1] or '',
                'cpf': r[2] or '',
                'saldo': float(r[3] or 0),
                'suspendido': bool(r[4]),
                'total_apostas': int(r[5] or 0),
                'apostas_won': int(r[6] or 0),
                'volume_apostado': float(r[7] or 0),
            })
        return web.json_response({'ok': True, 'total': total, 'usuarios': usuarios})
    except Exception as e:
        # Tentar sem coluna suspendido (retrocompatibilidade)
        try:
            conn = _admin_db_connect()
            cur = conn.cursor()
            busca = request.rel_url.query.get('busca', '').strip()
            if busca:
                cur.execute("""
                    SELECT u.id, u.nome, u.cpf, u.saldo, false,
                           COUNT(a.id), COALESCE(SUM(a.valor),0)
                    FROM usuarios u LEFT JOIN apostas a ON a.usuario_id=u.id
                    WHERE u.nome ILIKE %s OR u.cpf ILIKE %s
                    GROUP BY u.id ORDER BY u.id DESC LIMIT 50
                """, (f'%{busca}%', f'%{busca}%'))
            else:
                cur.execute("""
                    SELECT u.id, u.nome, u.cpf, u.saldo, false,
                           COUNT(a.id), COALESCE(SUM(a.valor),0)
                    FROM usuarios u LEFT JOIN apostas a ON a.usuario_id=u.id
                    GROUP BY u.id ORDER BY u.id DESC LIMIT 50
                """)
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM usuarios"); total = cur.fetchone()[0]
            cur.close(); conn.close()
            usuarios = [{'id':r[0],'nome':r[1]or'','cpf':r[2]or'','saldo':float(r[3]or 0),
                         'suspendido':bool(r[4]),'total_apostas':int(r[5]or 0),
                         'apostas_won':0,'volume_apostado':float(r[6]or 0)} for r in rows]
            return web.json_response({'ok': True, 'total': total, 'usuarios': usuarios})
        except Exception as e2:
            return web.json_response({'ok': False, 'error': str(e2)}, status=500)

async def route_admin_usuarios_ajustar(request):
    """POST /api/admin/usuarios/ajustar — ajusta saldo de um usuário (crédito ou débito)."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        usuario_id = int(body.get('usuario_id', 0))
        ajuste = float(body.get('ajuste', 0))
        motivo = str(body.get('motivo', 'Ajuste manual admin'))[:200]
        if not usuario_id:
            return web.json_response({'ok': False, 'error': 'usuario_id obrigatório'}, status=400)
        conn = _admin_db_connect()
        cur = conn.cursor()
        cur.execute("SELECT id, nome, saldo FROM usuarios WHERE id=%s", (usuario_id,))
        user = cur.fetchone()
        if not user:
            cur.close(); conn.close()
            return web.json_response({'ok': False, 'error': 'Usuário não encontrado'}, status=404)
        novo_saldo = float(user[2] or 0) + ajuste
        if novo_saldo < 0:
            cur.close(); conn.close()
            return web.json_response({'ok': False, 'error': f'Saldo insuficiente (atual: R${user[2]:.2f})'}, status=400)
        cur.execute("UPDATE usuarios SET saldo=%s WHERE id=%s", (novo_saldo, usuario_id))
        conn.commit(); cur.close(); conn.close()
        print(f'[admin/usuarios] ID {usuario_id} ({user[1]}) ajuste={ajuste:+.2f} → saldo={novo_saldo:.2f} | {motivo}', flush=True)
        return web.json_response({'ok': True, 'usuario_id': usuario_id, 'nome': user[1],
                                  'saldo_anterior': float(user[2] or 0), 'saldo_novo': novo_saldo,
                                  'ajuste': ajuste, 'motivo': motivo})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_usuarios_suspender(request):
    """POST /api/admin/usuarios/suspender — suspende ou ativa conta de usuário."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        usuario_id = int(body.get('usuario_id', 0))
        suspendido = bool(body.get('suspendido', False))
        if not usuario_id:
            return web.json_response({'ok': False, 'error': 'usuario_id obrigatório'}, status=400)
        conn = _admin_db_connect()
        cur = conn.cursor()
        # Adicionar coluna se não existir
        try:
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS suspendido BOOLEAN DEFAULT false")
            conn.commit()
        except Exception:
            conn.rollback()
        cur.execute("SELECT id, nome FROM usuarios WHERE id=%s", (usuario_id,))
        user = cur.fetchone()
        if not user:
            cur.close(); conn.close()
            return web.json_response({'ok': False, 'error': 'Usuário não encontrado'}, status=404)
        cur.execute("UPDATE usuarios SET suspendido=%s WHERE id=%s", (suspendido, usuario_id))
        conn.commit(); cur.close(); conn.close()
        status = 'SUSPENSO' if suspendido else 'ATIVO'
        print(f'[admin/usuarios] ID {usuario_id} ({user[1]}) → {status}', flush=True)
        return web.json_response({'ok': True, 'usuario_id': usuario_id, 'nome': user[1],
                                  'suspendido': suspendido, 'msg': f'Conta {status}'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)


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


async def route_railway_add_domain(request):
    """
    POST /api/railway/add-domain
    Configura um domínio customizado no Railway via GraphQL API.
    Aceita railway_token no body (pois pode não estar como var de ambiente).
    Body: { railway_token, domain, environment_id, service_id, project_id, target_port? }
    """
    import aiohttp

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)

    try:
        data = await request.json()

        # Token pode vir do body OU das variáveis de ambiente
        railway_token   = data.get('railway_token') or os.environ.get('RAILWAY_TOKEN', '')
        domain          = data.get('domain', '').strip()
        environment_id  = data.get('environment_id') or os.environ.get('RAILWAY_ENVIRONMENT_ID', '')
        service_id      = data.get('service_id')     or os.environ.get('RAILWAY_SERVICE_ID', '')
        project_id      = data.get('project_id')     or os.environ.get('RAILWAY_PROJECT_ID', '')
        target_port     = data.get('target_port', 8080)

        # Validações
        if not railway_token:
            return web.json_response({
                'success': False,
                'error': 'railway_token obrigatório. Gere em: https://railway.com/account/tokens',
                'instrucao': 'Acesse railway.com → Account → Tokens → New Token'
            }, status=400)

        if not domain:
            return web.json_response({'success': False, 'error': 'domain obrigatório'}, status=400)

        if not all([environment_id, service_id, project_id]):
            # Tentar buscar automaticamente via API Railway
            missing = []
            if not environment_id: missing.append('environment_id')
            if not service_id:     missing.append('service_id')
            if not project_id:     missing.append('project_id')
            return web.json_response({
                'success': False,
                'error': f'Parâmetros faltando: {missing}',
                'instrucao': 'Informe environment_id, service_id e project_id (encontrados em Settings > General do projeto Railway)'
            }, status=400)

        # Mutation GraphQL para criar domínio customizado
        mutation = """
        mutation($input: CustomDomainCreateInput!) {
          customDomainCreate(input: $input) {
            id
            domain
            status {
              dns {
                hostname
                type
                value
                status
              }
            }
          }
        }
        """

        payload = {
            "query": mutation,
            "variables": {
                "input": {
                    "domain": domain,
                    "environmentId": environment_id,
                    "serviceId": service_id,
                    "projectId": project_id,
                    "targetPort": target_port
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
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                result = await resp.json()

        if result.get('errors'):
            msg = result['errors'][0].get('message', str(result['errors']))
            print(f'❌ [Railway] Erro ao adicionar domínio {domain}: {msg}', flush=True)
            return web.json_response({'success': False, 'error': msg})

        domain_data = result.get('data', {}).get('customDomainCreate', {})
        dns_records = []
        if domain_data.get('status', {}).get('dns'):
            dns_records = domain_data['status']['dns']

        print(f'✅ [Railway] Domínio {domain} adicionado! ID: {domain_data.get("id")}', flush=True)
        return web.json_response({
            'success': True,
            'message': f'✅ Domínio {domain} adicionado ao Railway!',
            'domain_id': domain_data.get('id'),
            'domain': domain_data.get('domain'),
            'dns_records': dns_records,
            'instrucao': f'Configure o DNS do domínio {domain} com os registros acima'
        })

    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_railway_domain_status(request):
    """
    GET /api/railway/domain-status?domain=paynexbet.com.br&secret=...
    Verifica status de domínios customizados no Railway.
    """
    import aiohttp

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)

    try:
        railway_token  = request.rel_url.query.get('railway_token')  or os.environ.get('RAILWAY_TOKEN', '')
        service_id     = request.rel_url.query.get('service_id')     or os.environ.get('RAILWAY_SERVICE_ID', '')
        environment_id = request.rel_url.query.get('environment_id') or os.environ.get('RAILWAY_ENVIRONMENT_ID', '')
        project_id     = request.rel_url.query.get('project_id')     or os.environ.get('RAILWAY_PROJECT_ID', '')

        if not railway_token:
            return web.json_response({
                'success': False,
                'error': 'railway_token obrigatório',
                'instrucao': 'Passe ?railway_token=xxx ou configure RAILWAY_TOKEN nas env vars'
            })

        if not all([service_id, environment_id, project_id]):
            missing = []
            if not service_id:     missing.append('service_id')
            if not environment_id: missing.append('environment_id')
            if not project_id:     missing.append('project_id')
            return web.json_response({
                'success': False,
                'error': f'Parâmetros faltando: {missing}',
                'instrucao': 'Informe service_id, environment_id e project_id'
            })

        # Buscar nome do serviço
        svc_query = """
        query($id: String!) {
          service(id: $id) {
            id
            name
          }
        }
        """

        # Buscar domínios via query separada (API Railway v2)
        dom_query = """
        query($serviceId: String!, $environmentId: String!, $projectId: String!) {
          domains(serviceId: $serviceId, environmentId: $environmentId, projectId: $projectId) {
            customDomains {
              id
              domain
              updatedAt
              status {
                dns {
                  hostname
                  type
                  value
                  status
                }
              }
            }
            serviceDomains {
              id
              domain
            }
          }
        }
        """

        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {railway_token}",
                "Content-Type": "application/json"
            }
            timeout = aiohttp.ClientTimeout(total=15)

            # Buscar nome do serviço
            svc_resp = await session.post(
                'https://backboard.railway.app/graphql/v2',
                json={"query": svc_query, "variables": {"id": service_id}},
                headers=headers, timeout=timeout
            )
            svc_result = await svc_resp.json()

            # Buscar domínios
            dom_resp = await session.post(
                'https://backboard.railway.app/graphql/v2',
                json={"query": dom_query, "variables": {
                    "serviceId": service_id,
                    "environmentId": environment_id,
                    "projectId": project_id
                }},
                headers=headers, timeout=timeout
            )
            dom_result = await dom_resp.json()

        if dom_result.get('errors'):
            msg = dom_result['errors'][0].get('message', str(dom_result['errors']))
            return web.json_response({'success': False, 'error': msg})

        service_name = svc_result.get('data', {}).get('service', {}).get('name', 'N/A')
        domains = dom_result.get('data', {}).get('domains', {})

        return web.json_response({
            'success': True,
            'service_name': service_data.get('name'),
            'custom_domains': domains.get('customDomains', []),
            'service_domains': domains.get('serviceDomains', [])
        })

    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_railway_get_info(request):
    """
    GET /api/railway/info?secret=...&railway_token=...
    Busca automaticamente project_id, service_id e environment_id do projeto atual.
    """
    import aiohttp

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)

    try:
        railway_token = (request.rel_url.query.get('railway_token') or
                         os.environ.get('RAILWAY_TOKEN', ''))

        if not railway_token:
            return web.json_response({
                'success': False,
                'error': 'railway_token obrigatório',
                'como_obter': '1. Acesse https://railway.com/account/tokens\n2. Clique em New Token\n3. Cole o token aqui'
            })

        # Buscar projetos do usuário
        query = """
        {
          me {
            id
            email
            projects {
              edges {
                node {
                  id
                  name
                  environments {
                    edges {
                      node {
                        id
                        name
                      }
                    }
                  }
                  services {
                    edges {
                      node {
                        id
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://backboard.railway.app/graphql/v2',
                json={"query": query},
                headers={
                    "Authorization": f"Bearer {railway_token}",
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                result = await resp.json()

        if result.get('errors'):
            msg = result['errors'][0].get('message', str(result['errors']))
            return web.json_response({'success': False, 'error': msg})

        me = result.get('data', {}).get('me', {})
        projects = []
        for edge in me.get('projects', {}).get('edges', []):
            p = edge['node']
            envs = [e['node'] for e in p.get('environments', {}).get('edges', [])]
            svcs = [s['node'] for s in p.get('services', {}).get('edges', [])]
            projects.append({
                'id': p['id'],
                'name': p['name'],
                'environments': envs,
                'services': svcs
            })

        return web.json_response({
            'success': True,
            'email': me.get('email'),
            'projects': projects,
            'dica': 'Use os IDs acima para chamar /api/railway/add-domain'
        })

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
    # Verifica se o token MP2 está configurado (busca direto do banco)
    mp2_ativo = False
    mp2_ambiente = 'produção'
    try:
        from mp2_api import mp2_get_config
        token = mp2_get_config('mp2_access_token', '')
        mp2_ativo = bool(token and len(token) > 10)
        mp2_ambiente = mp2_get_config('mp2_ambiente', 'produção') or 'produção'
    except Exception:
        pass

    return web.json_response({
        'status': 'online',
        'version': 'v20250428n-crests-pix-fix',
        'gateway': 'mercado_pago',
        'mp2_ativo': mp2_ativo,
        'mp2_token_configurado': mp2_ativo,
        'mp2_ambiente': mp2_ambiente,
        'watchdog': 'ativo',
        'webhook': '/webhook/mp2',
        # Bet system — mostra primeiros 6 chars para debug (sem expor chave)
        'bet_routes': ['/apostas', '/conta', '/api/bet/jogos', '/api/bet/deposito', '/webhook/suitpay'],
        'odds_configurado': bool(os.environ.get('ODDS_API_KEY','')),
        'odds_key_preview': (os.environ.get('ODDS_API_KEY','')[:6] + '...' if os.environ.get('ODDS_API_KEY','') else 'NÃO CONFIGURADA'),
        'suitpay_configurado': bool(os.environ.get('SUITPAY_CI','')),
        'suitpay_ci_preview': (os.environ.get('SUITPAY_CI','')[:8] + '...' if os.environ.get('SUITPAY_CI','') else 'NÃO CONFIGURADA'),
        'suit_ci_parsed': _SUIT_CI[:4] + '...' if _SUIT_CI else 'vazio',
        'suit_cs_parsed': _SUIT_CS[:6] + '...' if _SUIT_CS else 'vazio',
        # Mantém compatibilidade retroativa
        'telegram': False,
        'telegram_motivo': None,
        'tentativas': 0,
        'ultimo_ping_seg': None,
        'bot': '@paypix_nexbot',
    })

async def route_check_auth(request):
    """GET /api/check-auth?secret=... — verifica se a senha está correta (sem revelar a senha real)"""

    secret = request.headers.get('X-PaynexBet-Secret') or request.rel_url.query.get('secret', '')
    ok = (secret == WEBHOOK_SECRET)
    return web.json_response({
        'ok': ok,
        'message': 'Senha correta ✅' if ok else 'Senha incorreta ❌',
        'hint': 'Use a variável WEBHOOK_SECRET do Railway ou o valor padrão do server.py' if not ok else None,
        'secret_len': len(WEBHOOK_SECRET),  # tamanho da senha real (ajuda a conferir)
        'provided_len': len(secret),         # tamanho do que foi enviado
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
    return web.Response(
        text=load_admin_html(),
        content_type='text/html',
        charset='utf-8',
        headers={
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
    )

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
    """Painel admin - listar todos os saques do Mercado Pago"""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        from mp2_api import _get_conn
        import psycopg2.extras
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT s.id, s.telegram_id, s.valor, s.chave_pix, s.tipo_chave,
                   s.status, s.obs, s.criado_em, s.processado_em,
                   u.username, u.nome
            FROM mp2_saques s
            LEFT JOIN mp2_usuarios u ON u.telegram_id = s.telegram_id
            ORDER BY s.criado_em DESC
            LIMIT 500
        """)
        from decimal import Decimal as _Dec
        rows = cur.fetchall()
        saques = []
        for r in rows:
            row = dict(r)
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()
                elif isinstance(v, _Dec):
                    row[k] = float(v)
                elif v is None:
                    row[k] = ''
            saques.append(row)
        cur.close(); conn.close()
        pendentes   = [s for s in saques if s['status'] == 'pendente']
        aprovados   = [s for s in saques if s['status'] == 'aprovado']
        rejeitados  = [s for s in saques if s['status'] == 'rejeitado']
        return web.json_response({
            'saques': saques,
            'resumo': {
                'total':          len(saques),
                'pendentes':      len(pendentes),
                'aprovados':      len(aprovados),
                'rejeitados':     len(rejeitados),
                'valor_pendente': round(sum(s['valor'] for s in pendentes if isinstance(s['valor'], float)), 2),
                'valor_pago':     round(sum(s['valor'] for s in aprovados  if isinstance(s['valor'], float)), 2),
            }
        })
    except Exception as e:
        return web.json_response({'error': str(e), 'saques': [], 'resumo': {}}, status=500)

async def route_stats(request):
    """Dashboard completo com métricas consolidadas — Mercado Pago (mp2_*)"""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        from mp2_api import _get_conn as mp2_conn
        import psycopg2.extras

        conn = mp2_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ── Depósitos MP2 ─────────────────────────────────────────────────────
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE status = 'confirmado') AS dep_conf,
              COALESCE(SUM(valor) FILTER (WHERE status = 'confirmado'), 0) AS val_dep_conf,
              COUNT(*) FILTER (WHERE status = 'pendente')   AS dep_pend,
              COALESCE(SUM(valor) FILTER (WHERE status = 'pendente'), 0)   AS val_dep_pend,
              COUNT(*) AS dep_total,
              COALESCE(SUM(valor), 0) AS val_dep_total
            FROM mp2_transacoes WHERE tipo = 'deposito'
        """)
        dep_row = cur.fetchone()
        dep_conf      = int(dep_row['dep_conf'])
        val_dep_conf  = float(dep_row['val_dep_conf'])
        dep_pend      = int(dep_row['dep_pend'])
        val_dep_pend  = float(dep_row['val_dep_pend'])
        dep_total     = int(dep_row['dep_total'])

        # ── Saques MP2 ────────────────────────────────────────────────────────
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE status = 'aprovado')   AS saq_conf,
              COALESCE(SUM(valor) FILTER (WHERE status = 'aprovado'), 0)   AS val_saq_conf,
              COUNT(*) FILTER (WHERE status = 'pendente')   AS saq_pend,
              COALESCE(SUM(valor) FILTER (WHERE status = 'pendente'), 0)   AS val_saq_pend,
              COUNT(*) AS saq_total,
              COALESCE(SUM(valor), 0) AS val_saq_total
            FROM mp2_saques
        """)
        saq_row = cur.fetchone()
        saq_conf      = int(saq_row['saq_conf'])
        val_saq_conf  = float(saq_row['val_saq_conf'])
        saq_pend      = int(saq_row['saq_pend'])
        val_saq_pend  = float(saq_row['val_saq_pend'])
        saq_total     = int(saq_row['saq_total'])

        # ── Depósitos por dia (últimos 7 dias) ────────────────────────────────
        try:
            cur.execute("""
                SELECT DATE(criado_em) AS dia, COUNT(*) AS qtd, COALESCE(SUM(valor),0) AS valor
                FROM mp2_transacoes
                WHERE tipo = 'deposito' AND criado_em >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(criado_em) ORDER BY DATE(criado_em)
            """)
            dep_por_dia = [{'data': str(r['dia']), 'qtd': r['qtd'], 'valor': round(float(r['valor']),2)}
                           for r in cur.fetchall()]
        except Exception:
            dep_por_dia = []

        # ── Saques por dia (últimos 7 dias) ───────────────────────────────────
        try:
            cur.execute("""
                SELECT DATE(criado_em) AS dia, COUNT(*) AS qtd, COALESCE(SUM(valor),0) AS valor
                FROM mp2_saques
                WHERE criado_em >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(criado_em) ORDER BY DATE(criado_em)
            """)
            saq_por_dia = [{'data': str(r['dia']), 'qtd': r['qtd'], 'valor': round(float(r['valor']),2)}
                           for r in cur.fetchall()]
        except Exception:
            saq_por_dia = []

        # ── Recentes ──────────────────────────────────────────────────────────
        cur.execute("""
            SELECT t.id, t.telegram_id, t.valor, t.status, t.criado_em, t.atualizado_em,
                   t.mp_payment_id, t.pix_copia_cola, u.username, u.nome
            FROM mp2_transacoes t
            LEFT JOIN mp2_usuarios u ON u.telegram_id = t.telegram_id
            WHERE t.tipo = 'deposito'
            ORDER BY t.criado_em DESC LIMIT 10
        """)
        from decimal import Decimal as _Dec
        def _clean_row(r):
            row = dict(r)
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()
                elif isinstance(v, _Dec):
                    row[k] = float(v)
                elif v is None:
                    row[k] = ''
            return row

        ult_dep = [_clean_row(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT s.id, s.telegram_id, s.valor, s.chave_pix, s.tipo_chave,
                   s.status, s.criado_em, s.processado_em, u.username, u.nome
            FROM mp2_saques s
            LEFT JOIN mp2_usuarios u ON u.telegram_id = s.telegram_id
            ORDER BY s.criado_em DESC LIMIT 10
        """)
        ult_saq = [_clean_row(r) for r in cur.fetchall()]

        cur.close(); conn.close()

        return web.json_response({
            'depositos': {
                'total':          dep_total,
                'confirmados':    dep_conf,
                'pendentes':      dep_pend,
                'valor_recebido': round(val_dep_conf, 2),
                'valor_pendente': round(val_dep_pend, 2),
                'por_dia':        dep_por_dia,
                'recentes':       ult_dep,
            },
            'saques': {
                'total':          saq_total,
                'realizados':     saq_conf,
                'pendentes':      saq_pend,
                'erros':          0,
                'valor_sacado':   round(val_saq_conf, 2),
                'valor_pendente': round(val_saq_pend, 2),
                'por_dia':        saq_por_dia,
                'recentes':       ult_saq,
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
# ─── PAYPIX VORTEX — Credenciais, Gateway, Afiliados ────────────
# ══════════════════════════════════════════════════════════════════

def _paypix_vortex_ensure_table(conn):
    """Garante que as tabelas do PayPix VORTEX existam"""

    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS paypix_vortex_config (
            chave TEXT PRIMARY KEY,
            valor TEXT,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS paypix_afiliados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE NOT NULL,
            nome TEXT,
            chave_pix TEXT NOT NULL,
            tipo_chave TEXT DEFAULT 'aleatoria',
            comissao_pct REAL DEFAULT 0.0,
            ativo INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
    except Exception:
        pass

async def route_paypix_vortex_config_get(request):
    """GET /api/paypix/vortex/config — retorna config do gateway PayPix VORTEX"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)
        chaves = ['pp_gateway', 'pp_mp_token', 'pp_asaas_key', 'pp_propria_chave', 'pp_propria_tipo', 'pp_propria_nome']
        resultado = {}
        for c in chaves:
            cur = conn.execute("SELECT valor FROM paypix_vortex_config WHERE chave=?", (c,))
            row = cur.fetchone()
            resultado[c] = row[0] if row and row[0] else ''
        conn.close()
        # Mascarar tokens
        mp_token_raw = resultado.get('pp_mp_token', '')
        resultado['mp_token_configurado'] = bool(mp_token_raw)
        resultado['pp_mp_token'] = ''  # não expor token
        resultado['gateway'] = resultado.get('pp_gateway') or 'mercadopago'
        resultado['configurado'] = bool(mp_token_raw or resultado.get('pp_asaas_key') or resultado.get('pp_propria_chave'))
        resultado['success'] = True
        return web.json_response(resultado)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_paypix_vortex_config_save(request):
    """POST /api/paypix/vortex/config — salva credenciais do gateway PayPix VORTEX"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        body = await request.json()
        gateway       = body.get('gateway', 'mercadopago').strip()
        mp_token      = body.get('mp_token', '').strip()
        asaas_key     = body.get('asaas_key', '').strip()
        propria_chave = body.get('propria_chave', '').strip()
        propria_tipo  = body.get('propria_tipo', 'aleatoria').strip()
        propria_nome  = body.get('propria_nome', '').strip()

        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)

        def _upsert(chave, valor):
            if valor:
                conn.execute("""INSERT INTO paypix_vortex_config (chave, valor) VALUES (?, ?)
                    ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor, atualizado_em=CURRENT_TIMESTAMP""",
                    (chave, valor))

        _upsert('pp_gateway', gateway)
        if mp_token:      _upsert('pp_mp_token', mp_token)
        if asaas_key:     _upsert('pp_asaas_key', asaas_key)
        if propria_chave: _upsert('pp_propria_chave', propria_chave)
        if propria_tipo:  _upsert('pp_propria_tipo', propria_tipo)
        if propria_nome:  _upsert('pp_propria_nome', propria_nome)
        conn.commit()
        conn.close()
        return web.json_response({'success': True, 'mensagem': 'Credenciais salvas!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_paypix_vortex_testar(request):
    """GET /api/paypix/vortex/testar — testa conexão com o gateway configurado"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)
        cur = conn.execute("SELECT valor FROM paypix_vortex_config WHERE chave='pp_gateway'")
        row = cur.fetchone()
        gateway = row[0] if row else 'mercadopago'

        if gateway == 'mercadopago':
            # Tentar usar token do PayPix VORTEX primeiro, senão usar do mp2
            cur2 = conn.execute("SELECT valor FROM paypix_vortex_config WHERE chave='pp_mp_token'")
            r2 = cur2.fetchone()
            token = r2[0] if r2 and r2[0] else ''
            conn.close()
            # Fallback: usar mp2_config
            if not token:
                try:
                    import psycopg2
                    pg = psycopg2.connect(DATABASE_URL, connect_timeout=5)
                    c = pg.cursor()
                    c.execute("SELECT valor FROM mp2_config WHERE chave='mp2_access_token'")
                    row_mp = c.fetchone()
                    token = row_mp[0] if row_mp else ''
                    c.close(); pg.close()
                except Exception:
                    pass
            if not token:
                return web.json_response({'success': False, 'gateway': 'mercadopago', 'error': 'Access Token não configurado'})
            # Testar via API MP
            try:
                import aiohttp as _aiohttp
                async with _aiohttp.ClientSession() as sess:
                    async with sess.get(
                        'https://api.mercadopago.com/v1/account/bank_report/config',
                        headers={'Authorization': f'Bearer {token}'},
                        timeout=_aiohttp.ClientTimeout(total=8)
                    ) as resp:
                        ok = resp.status in (200, 404)  # 404 = conta existe mas sem relatório
                        return web.json_response({'success': ok, 'gateway': 'mercadopago',
                                                   'status': resp.status,
                                                   'mensagem': '✅ Mercado Pago conectado!' if ok else f'HTTP {resp.status}'})
            except Exception as ex:
                return web.json_response({'success': False, 'gateway': 'mercadopago', 'error': str(ex)})

        elif gateway == 'asaas':
            cur2 = conn.execute("SELECT valor FROM paypix_vortex_config WHERE chave='pp_asaas_key'")
            r2 = cur2.fetchone()
            key = r2[0] if r2 and r2[0] else ''
            conn.close()
            if not key:
                return web.json_response({'success': False, 'gateway': 'asaas', 'error': 'API Key Asaas não configurada'})
            try:
                import aiohttp as _aiohttp
                async with _aiohttp.ClientSession() as sess:
                    async with sess.get(
                        'https://api.asaas.com/v3/myAccount',
                        headers={'access_token': key},
                        timeout=_aiohttp.ClientTimeout(total=8)
                    ) as resp:
                        ok = resp.status == 200
                        return web.json_response({'success': ok, 'gateway': 'asaas',
                                                   'mensagem': '✅ Asaas conectado!' if ok else f'HTTP {resp.status}'})
            except Exception as ex:
                return web.json_response({'success': False, 'gateway': 'asaas', 'error': str(ex)})

        elif gateway == 'propria':
            cur2 = conn.execute("SELECT valor FROM paypix_vortex_config WHERE chave='pp_propria_chave'")
            r2 = cur2.fetchone()
            chave = r2[0] if r2 and r2[0] else ''
            conn.close()
            if not chave:
                return web.json_response({'success': False, 'gateway': 'propria', 'error': 'Chave PIX não configurada'})
            return web.json_response({'success': True, 'gateway': 'propria',
                                       'mensagem': f'✅ Chave PIX própria configurada: {chave[:6]}...'})
        else:
            conn.close()
            return web.json_response({'success': False, 'error': f'Gateway desconhecido: {gateway}'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_paypix_afiliados_listar(request):
    """GET /api/paypix/afiliados — lista afiliados PayPix VORTEX"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)
        cur = conn.execute(
            "SELECT id, codigo, nome, chave_pix, tipo_chave, comissao_pct, ativo, created_at FROM paypix_afiliados ORDER BY created_at DESC"
        )
        rows = cur.fetchall()
        conn.close()
        cols = ['id', 'codigo', 'nome', 'chave_pix', 'tipo_chave', 'comissao_pct', 'ativo', 'created_at']
        afiliados = [dict(zip(cols, r)) for r in rows]
        for af in afiliados:
            af['ativo'] = bool(af['ativo'])
        return web.json_response({'success': True, 'afiliados': afiliados, 'total': len(afiliados)})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_paypix_afiliados_criar(request):
    """POST /api/paypix/afiliados — cria novo afiliado PayPix VORTEX"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        body = await request.json()
        codigo    = (body.get('codigo', '') or '').strip().lower()
        nome      = (body.get('nome', '') or '').strip()[:100]
        chave_pix = (body.get('chave_pix', '') or '').strip()
        tipo_chave = (body.get('tipo_chave', 'aleatoria') or '').strip()
        comissao_pct = float(body.get('comissao_pct', 0.6))
        comissao_pct = max(0.0, min(0.99, comissao_pct))

        if not codigo:
            return web.json_response({'success': False, 'error': 'Código obrigatório'})
        if not chave_pix:
            return web.json_response({'success': False, 'error': 'Chave PIX obrigatória'})
        import re
        if not re.match(r'^[a-z0-9_-]{2,30}$', codigo):
            return web.json_response({'success': False, 'error': 'Código inválido — use apenas letras minúsculas, números, _ e -'})

        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)
        try:
            conn.execute(
                "INSERT INTO paypix_afiliados (codigo, nome, chave_pix, tipo_chave, comissao_pct, ativo) VALUES (?,?,?,?,?,1)",
                (codigo, nome, chave_pix, tipo_chave, comissao_pct)
            )
            conn.commit()
        except Exception as ex:
            conn.close()
            if 'UNIQUE' in str(ex).upper():
                return web.json_response({'success': False, 'error': f'Código "{codigo}" já existe!'})
            raise
        conn.close()
        return web.json_response({'success': True, 'codigo': codigo, 'mensagem': f'Afiliado {codigo} criado!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_paypix_afiliados_editar(request):
    """PATCH /api/paypix/afiliados/{codigo} — edita afiliado PayPix VORTEX"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        codigo = request.match_info.get('codigo', '')
        body   = await request.json()
        campos = []
        valores = []
        if 'nome' in body:
            campos.append('nome=?'); valores.append(str(body['nome'])[:100])
        if 'chave_pix' in body:
            campos.append('chave_pix=?'); valores.append(str(body['chave_pix']))
        if 'tipo_chave' in body:
            campos.append('tipo_chave=?'); valores.append(str(body['tipo_chave']))
        if 'comissao_pct' in body:
            pct = max(0.0, min(0.99, float(body['comissao_pct'])))
            campos.append('comissao_pct=?'); valores.append(pct)
        if 'ativo' in body:
            campos.append('ativo=?'); valores.append(1 if body['ativo'] else 0)
        if not campos:
            return web.json_response({'success': False, 'error': 'Nenhum campo para atualizar'})
        valores.append(codigo)
        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)
        conn.execute(f"UPDATE paypix_afiliados SET {', '.join(campos)} WHERE codigo=?", valores)
        conn.commit()
        conn.close()
        return web.json_response({'success': True, 'codigo': codigo})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_paypix_afiliados_deletar(request):
    """DELETE /api/paypix/afiliados/{codigo} — desativa afiliado PayPix VORTEX"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'não autorizado'}, status=401)
    try:
        codigo = request.match_info.get('codigo', '')
        conn = sqlite3_connect()
        _paypix_vortex_ensure_table(conn)
        conn.execute("UPDATE paypix_afiliados SET ativo=0 WHERE codigo=?", (codigo,))
        conn.commit()
        conn.close()
        return web.json_response({'success': True, 'codigo': codigo})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


# ══════════════════════════════════════════════════════════════════
# ─── ROTAS BOT 2 - @paypix_nexbot ───────────────────────────────────
# ══════════════════════════════════════════════════════════════════

async def route_bot2_status(request):
    """Status do @paypix_nexbot - inclui userbot Telethon E bot real (python-telegram-bot)."""

    # Status do bot REAL — verifica env E banco
    bot2_token = os.environ.get('BOT2_TOKEN', '').strip()
    if not bot2_token:
        # Tentar do banco
        try:
            import psycopg2 as _pg2
            _c = _pg2.connect(DATABASE_URL, connect_timeout=5)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM mp2_config WHERE chave='bot2_token'")
            _row = _cur.fetchone()
            _cur.close(); _c.close()
            bot2_token = (_row[0] or '').strip() if _row else ''
        except Exception:
            bot2_token = ''

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
                        # Notificar usuário via novo bot2_handler (webhook)
                        try:
                            import psycopg2 as _pg2, psycopg2.extras as _pge2
                            _wc = _pg2.connect(DATABASE_URL, connect_timeout=8)
                            _wcu = _wc.cursor(cursor_factory=_pge2.RealDictCursor)
                            _wcu.execute("SELECT telegram_id FROM mp2_transacoes WHERE mp_external_ref=%s", (external_ref,))
                            _tx = _wcu.fetchone()
                            _wcu.close(); _wc.close()
                            if _tx and _tx.get('telegram_id'):
                                from bot2_handler import notificar_deposito_confirmado
                                asyncio.create_task(notificar_deposito_confirmado(
                                    int(_tx['telegram_id']), float(info.get('valor', 0)), payment_id
                                ))
                        except Exception as _ne:
                            print(f'[mp2_wh_notif] {_ne} — usando fallback', flush=True)
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

        bot2_token = os.environ.get('BOT2_TOKEN', '').strip()
        if not bot2_token:
            # Tentar buscar do banco
            try:
                _conn2 = psycopg2.connect(DATABASE_URL, connect_timeout=5)
                _cur2 = _conn2.cursor()
                _cur2.execute("SELECT valor FROM mp2_config WHERE chave='bot2_token'")
                _row2 = _cur2.fetchone()
                _cur2.close(); _conn2.close()
                bot2_token = (_row2[0] or '').strip() if _row2 else ''
            except Exception:
                pass
        if not bot2_token:
            print('[mp2_notif] bot2_token não configurado — pulando notificação Telegram', flush=True)
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


async def route_mp2_depositos_admin(request):
    """Lista todos os depósitos MP2 para o painel admin."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        from mp2_api import mp2_listar_depositos_admin
        result = mp2_listar_depositos_admin(limit=500)
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'error': str(e), 'depositos': [], 'resumo': {}}, status=500)


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
            # Notificar usuário via novo bot2_handler (webhook)
            try:
                import psycopg2 as _pg, psycopg2.extras as _pge
                _c = _pg.connect(DATABASE_URL, connect_timeout=8)
                _cur = _c.cursor(cursor_factory=_pge.RealDictCursor)
                _cur.execute("SELECT telegram_id, valor FROM mp2_saques WHERE id=%s", (saque_id,))
                _saque = _cur.fetchone()
                _cur.close(); _c.close()
                if _saque:
                    from bot2_handler import notificar_saque_processado
                    asyncio.create_task(notificar_saque_processado(
                        int(_saque['telegram_id']), float(_saque['valor']), aprovado, saque_id
                    ))
            except Exception as _en:
                print(f'[notif_saque] {_en}', flush=True)
                _mp2_notificar_saque_aprovado(saque_id)  # fallback legado
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
            bot2_token = os.environ.get('BOT2_TOKEN', '').strip()
            if not bot2_token:
                try:
                    _c2 = psycopg2.connect(DATABASE_URL, connect_timeout=5)
                    _c2cur = _c2.cursor()
                    _c2cur.execute("SELECT valor FROM mp2_config WHERE chave='bot2_token'")
                    _r2 = _c2cur.fetchone()
                    _c2cur.close(); _c2.close()
                    bot2_token = (_r2[0] or '').strip() if _r2 else ''
                except Exception: pass
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

# ─── BOT2 WEBHOOK (novo — sem polling, sem Telethon) ────────────────────────

async def route_bot2_webhook(request):
    """
    POST /webhook/bot2
    Telegram envia updates aqui. Processa e responde.
    Não requer autenticação — valida pelo token na URL internamente.
    """
    try:
        data = await request.json()
        from bot2_handler import process_bot2_update
        asyncio.create_task(process_bot2_update(data))
        return web.Response(text='OK', status=200)
    except Exception as e:
        print(f'[bot2_wh] Erro: {e}', flush=True)
        return web.Response(text='OK', status=200)  # sempre 200 para o Telegram


async def route_bot2_set_webhook(request):
    """
    POST /api/bot2/set-webhook
    Admin chama para registrar o webhook no Telegram.
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        base_url = str(request.url).split('/api/')[0]  # https://paynexbet.com
        from bot2_handler import registrar_webhook
        resultado = registrar_webhook(base_url)
        return web.json_response(resultado)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_bot2_webhook_info(request):
    """
    GET /api/bot2/webhook-info
    Retorna informações do webhook atual no Telegram.
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        from bot2_handler import get_webhook_info, get_bot_info
        wh   = get_webhook_info()
        info = get_bot_info()
        return web.json_response({
            'success': True,
            'bot':     info,
            'webhook': wh,
            'token_configurado': bool(os.environ.get('BOT2_TOKEN', '')),
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_bot2_config_get(request):
    """
    GET /api/bot2/config
    Retorna configuração básica do bot2 (token_configurado, webhook, etc).
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT valor FROM mp2_config WHERE chave='bot2_token'")
        row = cur.fetchone()
        bot2_token_db = bool(row and row[0] and len((row[0] or '').strip()) > 10)
        cur.close(); conn.close()

        bot2_token_env = bool(os.environ.get('BOT2_TOKEN', '').strip())
        token_configurado = bot2_token_env or bot2_token_db

        base_url = str(request.url).split('/api/')[0]
        return web.json_response({
            'success': True,
            'token_configurado': token_configurado,
            'token_origem': 'env' if bot2_token_env else ('banco' if bot2_token_db else 'nenhum'),
            'webhook_url': f'{base_url}/webhook/bot2',
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_bot2_salvar_token(request):
    """
    POST /api/bot2/token
    Salva BOT2_TOKEN no banco (mp2_config) sem precisar de variável Railway.
    """
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body  = await request.json()
        token = (body.get('token') or '').strip()
        if not token:
            return web.json_response({'success': False, 'error': 'Token vazio'})

        # Validar token antes de salvar
        req = urllib.request.Request(
            f'https://api.telegram.org/bot{token}/getMe',
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            resp = json.loads(r.read())
        if not resp.get('ok'):
            return web.json_response({'success': False, 'error': 'Token inválido no Telegram'})

        # Salvar no banco
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO mp2_config (chave, valor) VALUES ('bot2_token', %s) "
            "ON CONFLICT (chave) DO UPDATE SET valor=%s, atualizado_em=NOW()",
            (token, token)
        )
        conn.commit(); cur.close(); conn.close()

        bot_info = resp.get('result', {})
        return web.json_response({
            'success': True,
            'username': bot_info.get('username', ''),
            'nome': bot_info.get('first_name', ''),
            'msg': f"Token do @{bot_info.get('username','?')} salvo com sucesso!"
        })
    except urllib.error.HTTPError as e:
        return web.json_response({'success': False, 'error': f'Token inválido: {e.code}'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


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
    """POST /api/mp2/config - salva chaves MP2 no banco e recarrega no mp2_api.
    
    Sentinela '__keep__': se o front-end enviar '__keep__' (ou string vazia) para
    qualquer campo, o valor já armazenado no banco é mantido — o token não é apagado
    nem sobrescrito por uma string vazia quando o campo está mascarado.
    """

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

        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()

        # Resolve sentinela '__keep__': substitui pelo valor atual do banco
        def _resolve(campo_db, valor_novo):
            """Retorna o valor a gravar: novo se não for sentinela, senão o atual do banco."""

            if valor_novo and valor_novo != '__keep__':
                return valor_novo          # novo valor fornecido — usar
            # vazio ou sentinel → buscar o que já está salvo
            cur.execute("SELECT valor FROM mp2_config WHERE chave = %s", (campo_db,))
            row = cur.fetchone()
            return row[0] if row else None

        token_final  = _resolve('mp2_access_token', access_token)
        pubkey_final = _resolve('mp2_public_key',   public_key)
        cid_final    = _resolve('mp2_client_id',    client_id)
        csec_final   = _resolve('mp2_client_secret', client_secret)

        # access_token é obrigatório (mas pode vir do banco via _resolve)
        if not token_final:
            cur.close(); conn.close()
            return web.json_response({'success': False,
                                      'error': 'Access Token é obrigatório! Cole o token no campo e clique em Salvar.'})

        pares = [
            ('mp2_access_token',  token_final),
            ('mp2_public_key',    pubkey_final),
            ('mp2_client_id',     cid_final),
            ('mp2_client_secret', csec_final),
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
        if token_final:  mp2_api.MP2_ACCESS_TOKEN = token_final
        if pubkey_final: mp2_api.MP2_PUBLIC_KEY   = pubkey_final

        suffix = token_final[-6:] if token_final else '???'
        print(f'✅ [mp2_config] Chaves MP2 atualizadas pelo admin. Token: ...{suffix}', flush=True)
        return web.json_response({'success': True, 'mensagem': 'Chaves salvas e ativas imediatamente!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


# ═══════════════════════════════════════════════════════════════════════════
# ███  PAYPIX-COB (pc) — Config ISOLADA (tabela pc_config)  ███
# ═══════════════════════════════════════════════════════════════════════════

async def route_pc_config_get(request):
    """GET /api/pc/config  ou  GET /api/pc/chaves — retorna credenciais PayPix-Cob."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        chaves = ['pc_access_token', 'pc_public_key', 'pc_client_id', 'pc_client_secret']
        resultado = {}
        for c in chaves:
            cur.execute("SELECT valor FROM pc_config WHERE chave = %s", (c,))
            row = cur.fetchone()
            if row and row[0]:
                resultado[c.replace('pc_', '')] = row[0]
        cur.close(); conn.close()
        resultado['configurado'] = bool(resultado.get('access_token'))
        return web.json_response({'success': True, **resultado})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_pc_config_save(request):
    """POST /api/pc/config — salva credenciais PayPix-Cob na tabela pc_config (ISOLADA)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body          = await request.json()
        access_token  = body.get('access_token', '').strip()
        public_key    = body.get('public_key',   '').strip()
        client_id     = body.get('client_id',    '').strip()
        client_secret = body.get('client_secret','').strip()

        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()

        # Garante que a tabela existe
        cur.execute('''
            CREATE TABLE IF NOT EXISTS pc_config (
                chave TEXT PRIMARY KEY,
                valor TEXT,
                atualizado_em TIMESTAMPTZ DEFAULT NOW()
            )
        ''')

        def _resolve_pc(campo_db, valor_novo):
            if valor_novo and valor_novo != '__keep__':
                return valor_novo
            cur.execute("SELECT valor FROM pc_config WHERE chave = %s", (campo_db,))
            row = cur.fetchone()
            return row[0] if row else None

        token_final  = _resolve_pc('pc_access_token',  access_token)
        pubkey_final = _resolve_pc('pc_public_key',    public_key)
        cid_final    = _resolve_pc('pc_client_id',     client_id)
        csec_final   = _resolve_pc('pc_client_secret', client_secret)

        if not token_final:
            cur.close(); conn.close()
            return web.json_response({'success': False,
                                      'error': 'Access Token do PayPix-Cob é obrigatório!'})

        pares = [
            ('pc_access_token',  token_final),
            ('pc_public_key',    pubkey_final),
            ('pc_client_id',     cid_final),
            ('pc_client_secret', csec_final),
        ]
        for chave, valor in pares:
            if valor:
                cur.execute('''
                    INSERT INTO pc_config (chave, valor, atualizado_em)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                ''', (chave, valor))
        conn.commit()
        cur.close(); conn.close()

        suffix = token_final[-6:] if token_final else '???'
        print(f'✅ [pc_config] Chaves PayPix-Cob atualizadas. Token: ...{suffix}', flush=True)
        return web.json_response({'success': True, 'mensagem': 'Chaves PayPix-Cob salvas com sucesso!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_pc_testar(request):
    """GET /api/pc/testar — testa credenciais PayPix-Cob com a API do Mercado Pago."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2, requests as _req
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT valor FROM pc_config WHERE chave = 'pc_access_token'")
        row = cur.fetchone()
        cur.close(); conn.close()
        token = row[0] if row else ''

        if not token:
            return web.json_response({'success': False, 'error': 'Access Token PayPix-Cob não configurado'})

        resp = _req.get(
            'https://api.mercadopago.com/users/me',
            headers={'Authorization': f'Bearer {token}'},
            timeout=10
        )
        if resp.status_code == 200:
            d = resp.json()
            email   = d.get('email', '')
            site_id = d.get('site_id', 'MLB')
            # Determina ambiente pelo prefixo do token (TEST = sandbox, APP_USR = produção)
            pc_token_prefix = token[:4] if token else ''
            ambiente = 'sandbox' if pc_token_prefix == 'TEST' else 'produção'
            return web.json_response({
                'success':  True,
                'email':    email,
                'site_id':  site_id,
                'ambiente': ambiente,
                'conta':    'PayPix-Cob (pc)',
                'mensagem': f'✅ Conectado: {email} · {ambiente}'
            })
        else:
            return web.json_response({'success': False, 'error': f'MP retornou {resp.status_code}: {resp.text[:200]}'})
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

async def route_parceiro_info_publico(request):
    """GET /api/parceiro/{codigo} - Info pública do parceiro (sem secret): nome, modo_pagamento, valor_fixo_parceiro."""
    try:
        codigo = request.match_info.get('codigo', '')
        if not codigo:
            return web.json_response({'success': False, 'error': 'Código obrigatório'})
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""
            SELECT nome, modo_pagamento, valor_fixo_parceiro, ativo, COALESCE(permitir_assinatura, false)
            FROM mp2_parceiros WHERE codigo = %s
        """, (codigo,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row or not row[3]:
            return web.json_response({'success': False, 'error': 'Parceiro não encontrado'})
        return web.json_response({
            'success': True,
            'nome': row[0],
            'modo_pagamento': row[1] or 'livre',
            'valor_fixo_parceiro': float(row[2] or 0),
            'permitir_assinatura': bool(row[4])
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

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
                   COUNT(t.id) FILTER (WHERE t.status = %s) AS qtd_pagamentos,
                   COALESCE(p.modo_pagamento, 'livre') AS modo_pagamento,
                   COALESCE(p.valor_fixo_parceiro, 0)  AS valor_fixo_parceiro,
                   COALESCE(p.permitir_assinatura, false) AS permitir_assinatura
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


async def route_mp2_parceiros_excluir(request):
    """POST /api/mp2/parceiros/{codigo}/excluir - Remove permanentemente o parceiro."""

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
        cur.execute("DELETE FROM mp2_parceiros WHERE codigo = %s RETURNING id", (codigo,))
        deleted = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not deleted:
            return web.json_response({'success': False, 'error': 'Parceiro não encontrado'})
        return web.json_response({'success': True, 'deleted': codigo})
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
        if 'modo_pagamento' in body:
            modo = str(body['modo_pagamento']).strip().lower()
            if modo not in ('fixo', 'livre'): modo = 'livre'
            campos.append('modo_pagamento = %s')
            valores.append(modo)
        if 'valor_fixo_parceiro' in body:
            vfp = float(body.get('valor_fixo_parceiro') or 0)
            vfp = max(0.0, vfp)
            campos.append('valor_fixo_parceiro = %s')
            valores.append(vfp)
        if 'permitir_assinatura' in body:
            campos.append('permitir_assinatura = %s')
            valores.append(bool(body['permitir_assinatura']))
        if not campos:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Nenhum campo para atualizar'})
        valores.append(codigo)
        cur.execute(f"UPDATE mp2_parceiros SET {', '.join(campos)} WHERE codigo = %s RETURNING id, nome, chave_pix, tipo_chave, comissao_pct, ativo, modo_pagamento, valor_fixo_parceiro", valores)
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not row:
            return web.json_response({'success': False, 'error': 'Parceiro não encontrado'})
        return web.json_response({'success': True, 'parceiro': {
            'id': row[0], 'nome': row[1], 'chave_pix': row[2],
            'tipo_chave': row[3], 'comissao_pct': float(row[4]), 'ativo': row[5],
            'modo_pagamento': row[6] or 'livre', 'valor_fixo_parceiro': float(row[7] or 0)
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
# ─── BOT3 / MP3 — Réplica do Bot2 (@paypix_nexbot2) ──────────────
# ══════════════════════════════════════════════════════════════════

async def route_bot3_webhook(request):
    """POST /webhook/bot3 — Telegram envia updates aqui."""
    try:
        data = await request.json()
        from bot3_handler import process_bot3_update
        asyncio.create_task(process_bot3_update(data))
        return web.Response(text='OK', status=200)
    except Exception as e:
        print(f'[bot3_wh] Erro: {e}', flush=True)
        return web.Response(text='OK', status=200)


async def route_bot3_set_webhook(request):
    """POST /api/bot3/set-webhook — Registra webhook no Telegram."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        base_url = str(request.url).split('/api/')[0]
        from bot3_handler import registrar_webhook
        resultado = registrar_webhook(base_url)
        return web.json_response(resultado)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_bot3_webhook_info(request):
    """GET /api/bot3/webhook-info — Retorna informações do webhook atual."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        from bot3_handler import get_webhook_info, get_bot_info
        wh   = get_webhook_info()
        info = get_bot_info()
        return web.json_response({
            'success': True,
            'bot':     info,
            'webhook': wh,
            'token_configurado': bool(os.environ.get('BOT3_TOKEN', '')),
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_bot3_config_get(request):
    """GET /api/bot3/config — Retorna configuração básica do bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3
        conn = _pg3.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        try:
            cur.execute("SELECT valor FROM mp3_config WHERE chave='bot3_token'")
            row = cur.fetchone()
            bot3_token_db = bool(row and row[0] and len((row[0] or '').strip()) > 10)
        except Exception:
            bot3_token_db = False
        cur.close(); conn.close()

        bot3_token_env = bool(os.environ.get('BOT3_TOKEN', '').strip())
        token_configurado = bot3_token_env or bot3_token_db

        base_url = str(request.url).split('/api/')[0]
        return web.json_response({
            'success': True,
            'token_configurado': token_configurado,
            'token_origem': 'env' if bot3_token_env else ('banco' if bot3_token_db else 'nenhum'),
            'webhook_url': f'{base_url}/webhook/bot3',
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_bot3_salvar_token(request):
    """POST /api/bot3/token — Salva BOT3_TOKEN no banco (mp3_config)."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body  = await request.json()
        token = (body.get('token') or '').strip()
        if not token:
            return web.json_response({'success': False, 'error': 'Token vazio'})

        # Validar token antes de salvar
        req_tg = urllib.request.Request(
            f'https://api.telegram.org/bot{token}/getMe',
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req_tg, timeout=8) as r:
            resp = json.loads(r.read())
        if not resp.get('ok'):
            return web.json_response({'success': False, 'error': 'Token inválido no Telegram'})

        # Garantir tabela mp3_config existe e salvar
        import psycopg2 as _pg3b
        conn = _pg3b.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS mp3_config (
            chave TEXT PRIMARY KEY, valor TEXT, atualizado_em TIMESTAMP DEFAULT NOW()
        )""")
        cur.execute(
            "INSERT INTO mp3_config (chave, valor) VALUES ('bot3_token', %s) "
            "ON CONFLICT (chave) DO UPDATE SET valor=%s, atualizado_em=NOW()",
            (token, token)
        )
        conn.commit(); cur.close(); conn.close()

        bot_info = resp.get('result', {})
        return web.json_response({
            'success': True,
            'username': bot_info.get('username', ''),
            'nome': bot_info.get('first_name', ''),
            'msg': f"Token do @{bot_info.get('username','?')} salvo com sucesso!"
        })
    except urllib.error.HTTPError as e:
        return web.json_response({'success': False, 'error': f'Token inválido: {e.code}'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_status(request):
    """GET /api/mp3/status — Status geral do Bot3."""
    try:
        import psycopg2 as _pg3s
        conn = _pg3s.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        # Verificar token
        try:
            cur.execute("SELECT valor FROM mp3_config WHERE chave='bot3_token'")
            row = cur.fetchone()
            bot3_token_db = bool(row and row[0] and len((row[0] or '').strip()) > 10)
        except Exception:
            bot3_token_db = False
        # Verificar MP token
        try:
            cur.execute("SELECT valor FROM mp3_config WHERE chave='mp3_access_token'")
            row2 = cur.fetchone()
            mp3_token_db = bool(row2 and row2[0])
        except Exception:
            mp3_token_db = False
        cur.close(); conn.close()

        bot3_token = bool(os.environ.get('BOT3_TOKEN', '')) or bot3_token_db
        mp3_token  = bool(os.environ.get('MP3_ACCESS_TOKEN', '')) or mp3_token_db

        # Verificar bot no Telegram se tiver token
        bot_real = False
        bot_real_info = {}
        username = 'paypix_nexbot2'
        if bot3_token:
            from bot3_handler import get_bot_info
            info = get_bot_info()
            if info.get('ok'):
                bot_real = True
                bot_real_info = info.get('result', {})
                username = bot_real_info.get('username', username)

        return web.json_response({
            'success': True,
            'bot': 'bot3',
            'username': username,
            'online': bot3_token,
            'bot_real': bot_real,
            'bot_real_info': bot_real_info,
            'mp3_configurado': mp3_token,
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_webhook(request):
    """POST /webhook/mp3 — Webhook do Mercado Pago para Bot3."""
    try:
        body = await request.json()
        action = body.get('action', '')
        data   = body.get('data', {})

        if action in ('payment.updated', 'payment.created'):
            payment_id = str(data.get('id', ''))
            if payment_id:
                from mp2_api import mp2_verificar_pagamento
                info = mp2_verificar_pagamento(payment_id, use_mp3=True)
                if info.get('status') == 'approved':
                    external_ref = info.get('external_ref', '')
                    if external_ref and external_ref.startswith('mp3_'):
                        # Confirmar pagamento na tabela mp3_transacoes
                        try:
                            import psycopg2 as _pg3w
                            conn = _pg3w.connect(DATABASE_URL, connect_timeout=8)
                            cur  = conn.cursor()
                            cur.execute(
                                "UPDATE mp3_transacoes SET status='confirmado', atualizado_em=NOW() "
                                "WHERE mp_external_ref=%s AND status='pendente'",
                                (external_ref,)
                            )
                            # Buscar telegram_id
                            cur.execute("SELECT telegram_id, valor FROM mp3_transacoes WHERE mp_external_ref=%s", (external_ref,))
                            tx_row = cur.fetchone()
                            conn.commit(); cur.close(); conn.close()

                            if tx_row and tx_row[0]:
                                from bot3_handler import notificar_deposito_confirmado
                                asyncio.create_task(notificar_deposito_confirmado(
                                    int(tx_row[0]), float(tx_row[1] or 0), payment_id
                                ))
                        except Exception as _e3w:
                            print(f'[mp3_wh] {_e3w}', flush=True)
                        return web.json_response({'ok': True, 'processado': True})

        return web.json_response({'ok': True, 'action': action})
    except Exception as e:
        print(f'[mp3_webhook] Erro: {e}', flush=True)
        return web.json_response({'ok': False, 'error': str(e)}, status=200)


async def route_mp3_config_get(request):
    """GET /api/mp3/config — Retorna chaves MP3 mascaradas."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3c
        conn = _pg3c.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        chaves = ['mp3_access_token', 'mp3_public_key', 'mp3_client_id', 'mp3_client_secret']
        result = {}
        for c in chaves:
            try:
                cur.execute("SELECT valor FROM mp3_config WHERE chave=%s", (c,))
                row = cur.fetchone()
                v = (row[0] or '').strip() if row else ''
                result[c] = ('__keep__' if len(v) > 6 else '') if v else ''
                result[f'{c}_configurado'] = bool(v)
                result[f'{c}_preview'] = (v[-6:] if len(v) > 6 else v) if v else ''
            except Exception:
                result[c] = ''
                result[f'{c}_configurado'] = False
        cur.close(); conn.close()
        base_url = str(request.url).split('/api/')[0]
        result['webhook_url'] = f'{base_url}/webhook/mp3'
        result['configurado'] = result.get('mp3_access_token_configurado', False)
        return web.json_response({'success': True, **result})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_config_save(request):
    """POST /api/mp3/config — Salva chaves Mercado Pago do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body = await request.json()
        import psycopg2 as _pg3cs
        conn = _pg3cs.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS mp3_config (
            chave TEXT PRIMARY KEY, valor TEXT, atualizado_em TIMESTAMP DEFAULT NOW()
        )""")
        chaves_map = {
            'access_token': 'mp3_access_token',
            'public_key':   'mp3_public_key',
            'client_id':    'mp3_client_id',
            'client_secret':'mp3_client_secret',
        }
        for body_key, db_key in chaves_map.items():
            v = (body.get(body_key) or '').strip()
            if v and v != '__keep__':
                cur.execute(
                    "INSERT INTO mp3_config (chave, valor) VALUES (%s, %s) "
                    "ON CONFLICT (chave) DO UPDATE SET valor=%s, atualizado_em=NOW()",
                    (db_key, v, v)
                )
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'success': True, 'msg': 'Chaves MP3 salvas com sucesso!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_testar(request):
    """GET /api/mp3/testar — Testa conexão com Mercado Pago do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3t
        conn = _pg3t.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        try:
            cur.execute("SELECT valor FROM mp3_config WHERE chave='mp3_access_token'")
            row = cur.fetchone()
            mp3_token = (row[0] or '').strip() if row else ''
        except Exception:
            mp3_token = ''
        cur.close(); conn.close()

        if not mp3_token:
            mp3_token = os.environ.get('MP3_ACCESS_TOKEN', '').strip()

        if not mp3_token:
            return web.json_response({'success': False, 'error': 'MP3 Access Token não configurado'})

        # Testar via API do MP
        req_mp = urllib.request.Request(
            'https://api.mercadopago.com/v1/account',
            headers={'Authorization': f'Bearer {mp3_token}'}
        )
        with urllib.request.urlopen(req_mp, timeout=10) as r:
            info = json.loads(r.read())

        email = info.get('email', 'N/A')
        site_id = info.get('site_id', 'N/A')
        return web.json_response({
            'success': True,
            'email': email,
            'site_id': site_id,
            'ambiente': 'produção' if not mp3_token.startswith('TEST-') else 'teste',
            'mensagem': f'Conta MP ativa: {email}',
        })
    except urllib.error.HTTPError as e:
        return web.json_response({'success': False, 'error': f'Erro MP: {e.code}'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_stats(request):
    """GET /api/mp3/stats — Estatísticas do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3st, decimal as _dec3
        conn = _pg3st.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        stats = {}
        queries = [
            ('total_depositos', "SELECT COUNT(*) FROM mp3_transacoes WHERE tipo='deposito'"),
            ('total_confirmados', "SELECT COUNT(*) FROM mp3_transacoes WHERE status='confirmado'"),
            ('total_valor', "SELECT COALESCE(SUM(valor),0) FROM mp3_transacoes WHERE status='confirmado'"),
            ('total_usuarios', "SELECT COUNT(*) FROM mp3_usuarios"),
            ('saques_pendentes', "SELECT COUNT(*) FROM mp3_saques WHERE status='pendente'"),
            ('saques_valor_pendente', "SELECT COALESCE(SUM(valor),0) FROM mp3_saques WHERE status='pendente'"),
        ]
        for key, sql in queries:
            try:
                cur.execute(sql)
                row = cur.fetchone()
                v = row[0] if row else 0
                stats[key] = float(v) if isinstance(v, _dec3.Decimal) else (v or 0)
            except Exception:
                stats[key] = 0
        cur.close(); conn.close()
        return web.json_response({'success': True, 'stats': stats})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_saques_pendentes(request):
    """GET /api/mp3/saques — Lista saques pendentes do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3sp, psycopg2.extras as _pge3sp, decimal as _dec3sp
        conn = _pg3sp.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor(cursor_factory=_pge3sp.RealDictCursor)
        try:
            cur.execute("""
                SELECT s.id, s.telegram_id, s.valor, s.chave_pix, s.tipo_chave, s.status,
                       s.obs, s.criado_em,
                       u.nome, u.username
                FROM mp3_saques s
                LEFT JOIN mp3_usuarios u ON u.telegram_id = s.telegram_id
                WHERE s.status = 'pendente'
                ORDER BY s.criado_em ASC
            """)
            rows = cur.fetchall()
        except Exception:
            rows = []
        cur.close(); conn.close()
        saques = []
        for r in rows:
            d = {}
            for k, v in r.items():
                if isinstance(v, _dec3sp.Decimal):
                    d[k] = float(v)
                elif v is None:
                    d[k] = None
                else:
                    d[k] = str(v) if not isinstance(v, (int, float, bool, str)) else v
            saques.append(d)
        return web.json_response({'success': True, 'saques': saques, 'total': len(saques)})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_processar_saque(request):
    """POST /api/mp3/saques/processar — Aprova ou rejeita saque do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body     = await request.json()
        saque_id = int(body.get('saque_id', 0))
        acao     = str(body.get('acao', 'aprovar')).lower()
        obs      = str(body.get('obs', '')).strip()

        if not saque_id:
            return web.json_response({'success': False, 'error': 'saque_id obrigatório'})

        import psycopg2 as _pg3proc
        conn = _pg3proc.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT telegram_id, valor, chave_pix FROM mp3_saques WHERE id=%s", (saque_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Saque não encontrado'})

        telegram_id, valor, chave = row[0], float(row[1] or 0), row[2]
        novo_status = 'aprovado' if acao == 'aprovar' else 'rejeitado'

        cur.execute(
            "UPDATE mp3_saques SET status=%s, obs=%s, processado_em=NOW() WHERE id=%s",
            (novo_status, obs, saque_id)
        )
        # Se rejeitado, devolver saldo
        if acao != 'aprovar':
            cur.execute(
                "UPDATE mp3_usuarios SET saldo=saldo+%s WHERE telegram_id=%s",
                (valor, telegram_id)
            )
        conn.commit(); cur.close(); conn.close()

        # Notificar usuário via bot3
        if telegram_id:
            try:
                from bot3_handler import notificar_saque_processado
                asyncio.create_task(notificar_saque_processado(
                    int(telegram_id), valor, acao == 'aprovar', saque_id
                ))
            except Exception as _ne3:
                print(f'[mp3_proc_saque] notif: {_ne3}', flush=True)

        return web.json_response({
            'success': True,
            'msg': f'Saque #{saque_id} {novo_status} com sucesso',
            'saque_id': saque_id,
            'novo_status': novo_status,
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_parceiros_listar(request):
    """GET /api/mp3/parceiros — Lista parceiros do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3pl, psycopg2.extras as _pge3pl, decimal as _dec3pl, json as _j3pl
        conn = _pg3pl.connect(DATABASE_URL)
        cur  = conn.cursor(cursor_factory=_pge3pl.RealDictCursor)
        try:
            cur.execute("""
                SELECT p.id, p.codigo, p.nome, p.chave_pix, p.tipo_chave,
                       p.comissao_pct, p.ativo, p.link,
                       COALESCE(p.total_gerado,0)   AS total_gerado,
                       COALESCE(p.total_comissao,0) AS total_comissao,
                       COALESCE(p.total_pago,0)     AS total_pago,
                       TO_CHAR(p.criado_em, 'DD/MM/YYYY HH24:MI') AS criado_em,
                       COUNT(t.id) FILTER (WHERE t.status = 'confirmado') AS qtd_pagamentos
                FROM mp3_parceiros p
                LEFT JOIN mp3_transacoes t ON t.parceiro_codigo = p.codigo
                GROUP BY p.id ORDER BY p.criado_em DESC
            """)
            rows = cur.fetchall()
        except Exception:
            rows = []
        cur.close(); conn.close()
        parceiros = []
        for r in rows:
            d = {}
            for k, v in r.items():
                if isinstance(v, _dec3pl.Decimal): d[k] = float(v)
                elif isinstance(v, bool): d[k] = v
                elif v is None: d[k] = None
                elif isinstance(v, (int, float, str)): d[k] = v
                else: d[k] = str(v)
            parceiros.append(d)
        return web.Response(text=_j3pl.dumps({'success': True, 'parceiros': parceiros, 'total': len(parceiros)}), content_type='application/json')
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_parceiros_criar(request):
    """POST /api/mp3/parceiros — Cria parceiro no Bot3."""
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
        if not nome: return web.json_response({'success': False, 'error': 'Nome obrigatório'})
        if not chave_pix: return web.json_response({'success': False, 'error': 'Chave PIX obrigatória'})
        import uuid, re, psycopg2 as _pg3pc
        if not codigo_req:
            base   = re.sub(r'[^a-z0-9]', '', nome.lower())[:12]
            codigo = f"{base}-{str(uuid.uuid4())[:6]}"
        else:
            codigo = codigo_req
        host = request.headers.get('X-Forwarded-Host') or request.host or 'paynexbet.com'
        base_url = f"https://{host}"
        link = f"{base_url}/paypix2?ref={codigo}"
        conn = _pg3pc.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS mp3_parceiros (
            id SERIAL PRIMARY KEY, codigo VARCHAR(50) UNIQUE NOT NULL,
            nome VARCHAR(200) NOT NULL, chave_pix VARCHAR(200) NOT NULL,
            tipo_chave VARCHAR(20) DEFAULT 'email', comissao_pct NUMERIC(5,2) DEFAULT 10.0,
            ativo BOOLEAN DEFAULT TRUE, total_gerado NUMERIC(12,2) DEFAULT 0,
            total_comissao NUMERIC(12,2) DEFAULT 0, total_pago NUMERIC(12,2) DEFAULT 0,
            criado_em TIMESTAMP DEFAULT NOW(), link TEXT
        )""")
        cur.execute("""
            INSERT INTO mp3_parceiros (codigo, nome, chave_pix, tipo_chave, comissao_pct, ativo, link, criado_em)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, NOW())
            ON CONFLICT (codigo) DO UPDATE
              SET nome=EXCLUDED.nome, chave_pix=EXCLUDED.chave_pix,
                  tipo_chave=EXCLUDED.tipo_chave, comissao_pct=EXCLUDED.comissao_pct, link=EXCLUDED.link
            RETURNING id, codigo, nome, chave_pix, tipo_chave, comissao_pct, link
        """, (codigo, nome, chave_pix, tipo_chave, comissao_pct, link))
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        parceiro = {'id': row[0], 'codigo': row[1], 'nome': row[2], 'chave_pix': row[3],
                    'tipo_chave': row[4], 'comissao_pct': float(row[5]), 'link': row[6], 'ativo': True}
        return web.json_response({'success': True, 'parceiro': parceiro, 'link': link})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_parceiros_deletar(request):
    """DELETE /api/mp3/parceiros/{codigo} — Desativa parceiro Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        codigo = request.match_info.get('codigo', '')
        import psycopg2 as _pg3pd
        conn = _pg3pd.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("UPDATE mp3_parceiros SET ativo=FALSE WHERE codigo=%s", (codigo,))
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'success': True, 'msg': f'Parceiro {codigo} desativado'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_parceiros_excluir(request):
    """POST /api/mp3/parceiros/{codigo}/excluir - Remove permanentemente o parceiro Bot3."""

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
        cur.execute("DELETE FROM mp3_parceiros WHERE codigo = %s RETURNING id", (codigo,))
        deleted = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not deleted:
            return web.json_response({'success': False, 'error': 'Parceiro não encontrado'})
        return web.json_response({'success': True, 'deleted': codigo})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_parceiros_editar(request):
    """PATCH /api/mp3/parceiros/{codigo} — Edita parceiro Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        codigo = request.match_info.get('codigo', '')
        body   = await request.json()
        import psycopg2 as _pg3pe
        conn = _pg3pe.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        fields = []
        vals   = []
        for f in ['nome', 'chave_pix', 'tipo_chave']:
            if f in body:
                fields.append(f'{f}=%s')
                vals.append(str(body[f]).strip())
        if 'comissao_pct' in body:
            fields.append('comissao_pct=%s')
            vals.append(float(body['comissao_pct']))
        if 'ativo' in body:
            fields.append('ativo=%s')
            vals.append(bool(body['ativo']))
        if 'modo_pagamento' in body:
            modo = str(body['modo_pagamento']).strip().lower()
            if modo not in ('fixo', 'livre'): modo = 'livre'
            fields.append('modo_pagamento=%s')
            vals.append(modo)
        if 'valor_fixo_parceiro' in body:
            vfp = float(body.get('valor_fixo_parceiro') or 0)
            fields.append('valor_fixo_parceiro=%s')
            vals.append(max(0.0, vfp))
        if 'permitir_assinatura' in body:
            fields.append('permitir_assinatura=%s')
            vals.append(bool(body['permitir_assinatura']))
        if fields:
            vals.append(codigo)
            cur.execute(f"UPDATE mp3_parceiros SET {', '.join(fields)} WHERE codigo=%s", vals)
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'success': True, 'msg': f'Parceiro {codigo} atualizado'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_comissoes_listar(request):
    """GET /api/mp3/comissoes — Lista comissões do Bot3."""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import psycopg2 as _pg3cl, psycopg2.extras as _pge3cl, decimal as _dec3cl, json as _j3cl
        conn = _pg3cl.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor(cursor_factory=_pge3cl.RealDictCursor)
        try:
            cur.execute("""
                SELECT t.id, t.parceiro_codigo, t.valor AS valor_total,
                       t.comissao_valor, t.comissao_status AS status, t.criado_em,
                       p.nome AS parceiro_nome, p.chave_pix
                FROM mp3_transacoes t
                LEFT JOIN mp3_parceiros p ON p.codigo = t.parceiro_codigo
                WHERE t.parceiro_codigo IS NOT NULL
                ORDER BY t.criado_em DESC LIMIT 50
            """)
            rows = cur.fetchall()
        except Exception:
            rows = []
        cur.close(); conn.close()
        comissoes = []
        for r in rows:
            d = {}
            for k, v in r.items():
                if isinstance(v, _dec3cl.Decimal): d[k] = float(v)
                elif v is None: d[k] = None
                elif isinstance(v, (int, float, bool, str)): d[k] = v
                else: d[k] = str(v)
            comissoes.append(d)
        return web.Response(text=_j3cl.dumps({'success': True, 'comissoes': comissoes, 'total': len(comissoes)}), content_type='application/json')
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


# ══════════════════════════════════════════════════════════════════
# ─── MP3 — Rotas públicas /paypix2 (sem Telegram, só MP) ─────────
# ══════════════════════════════════════════════════════════════════

def _mp3_get_access_token() -> str:
    """Retorna o Access Token do MP3 (env ou banco)."""
    t = os.environ.get('MP3_ACCESS_TOKEN', '').strip()
    if t:
        return t
    try:
        import psycopg2 as _pg3tok
        conn = _pg3tok.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT valor FROM mp3_config WHERE chave='mp3_access_token'")
        row = cur.fetchone()
        cur.close(); conn.close()
        return (row[0] or '').strip() if row else ''
    except Exception:
        return ''


async def route_paypix2_page(request):
    """GET /paypix2 — Página pública para gerar PIX via MP3 (sem Telegram)."""
    try:
        import psycopg2 as _pg3p2
        conn = _pg3p2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        try:
            cur.execute("SELECT valor FROM configuracoes WHERE chave='paypix2_html_patch'")
            row = cur.fetchone()
            if row and row[0] and len(row[0]) > 500:
                cur.close(); conn.close()
                return web.Response(text=row[0], content_type='text/html', charset='utf-8')
        except Exception:
            pass
        cur.close(); conn.close()
    except Exception:
        pass
    if os.path.exists('paypix2.html'):
        html = open('paypix2.html', encoding='utf-8').read()
        return web.Response(text=html, content_type='text/html', charset='utf-8')
    return web.Response(text='<h1>PayPix2</h1>', content_type='text/html')


async def route_mp3_config_publica(request):
    """GET /api/mp3/config-publica — Retorna config pública do MP3 (sem autenticação)."""
    try:
        import psycopg2 as _pg3cp
        conn = _pg3cp.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        config = {}
        for chave in ['bot_titulo', 'bot_descricao', 'bot_min_valor', 'bot_max_valor',
                      'bot_pct_parceiro', 'bot_ativo', 'deposito_minimo', 'deposito_maximo']:
            try:
                cur.execute("SELECT valor FROM mp3_config WHERE chave=%s", (chave,))
                row = cur.fetchone()
                config[chave] = (row[0] or '').strip() if row else ''
            except Exception:
                config[chave] = ''

        # Verificar se parceiro existe (ref na URL)
        ref = request.rel_url.query.get('ref', '').strip()
        parceiro = None
        if ref:
            try:
                cur.execute("SELECT nome, comissao_pct FROM mp3_parceiros WHERE codigo=%s AND ativo=TRUE", (ref,))
                pr = cur.fetchone()
                if pr:
                    parceiro = {'codigo': ref, 'nome': pr[0], 'comissao_pct': float(pr[1] or 10)}
            except Exception:
                pass

        cur.close(); conn.close()
        host = request.headers.get('X-Forwarded-Host') or request.host or 'paynexbet.com'
        return web.json_response({
            'success': True,
            'titulo': config.get('bot_titulo') or 'PayPix2 — Pague via PIX',
            'descricao': config.get('bot_descricao') or 'Pagamento via PIX instantâneo',
            'min_valor': float(config.get('deposito_minimo') or config.get('bot_min_valor') or '5'),
            'max_valor': float(config.get('deposito_maximo') or config.get('bot_max_valor') or '10000'),
            'ativo': config.get('bot_ativo', 'true') not in ('false', '0', 'False'),
            'parceiro': parceiro,
            'webhook_mp3': f'https://{host}/webhook/mp3',
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)


async def route_mp3_gerar_pix(request):
    """POST /api/mp3/gerar — Gera PIX Mercado Pago via MP3 (público, sem Telegram)."""
    # CORS headers
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    }
    try:
        data       = await request.json()
        valor      = float(data.get('valor', 0))
        nome_pag   = str(data.get('nome', 'Cliente')).strip() or 'Cliente'
        email_pag  = str(data.get('email', '')).strip()
        ref        = str(data.get('ref', '')).strip()   # código parceiro (opcional)

        # Validações básicas
        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'}, headers=headers)
        if valor > 10000:
            return web.json_response({'success': False, 'error': 'Valor máximo R$ 10.000,00'}, headers=headers)

        mp3_token = _mp3_get_access_token()
        if not mp3_token:
            return web.json_response({'success': False, 'error': 'Mercado Pago não configurado'}, headers=headers)

        import uuid, psycopg2 as _pg3g
        ext_ref = f'mp3_pub_{int(datetime.now().timestamp())}_{uuid.uuid4().hex[:8]}'

        # Verificar parceiro e comissão
        parceiro_codigo = None
        comissao_pct    = 0.0
        comissao_valor  = 0.0
        if ref:
            try:
                conn_p = _pg3g.connect(DATABASE_URL, connect_timeout=8)
                cur_p  = conn_p.cursor()
                cur_p.execute("SELECT comissao_pct FROM mp3_parceiros WHERE codigo=%s AND ativo=TRUE", (ref,))
                rp = cur_p.fetchone()
                if rp:
                    parceiro_codigo = ref
                    comissao_pct    = float(rp[0] or 10)
                    comissao_valor  = round(valor * comissao_pct / 100, 2)
                cur_p.close(); conn_p.close()
            except Exception:
                pass

        # Gerar PIX no Mercado Pago
        host = request.headers.get('X-Forwarded-Host') or request.host or 'paynexbet.com'
        notif_url = f'https://{host}/webhook/mp3'
        email_real = email_pag or f'cliente_{int(datetime.now().timestamp())}@paynexbet.com'

        payload_mp = {
            'transaction_amount': valor,
            'description': f'Pagamento via PayPix2 — {nome_pag}',
            'payment_method_id': 'pix',
            'payer': {
                'email': email_real,
                'first_name': nome_pag.split()[0][:20],
            },
            'external_reference': ext_ref,
            'notification_url': notif_url,
            'metadata': {'ref': ref, 'parceiro': parceiro_codigo or '', 'origem': 'paypix2'},
        }
        req_mp = urllib.request.Request(
            'https://api.mercadopago.com/v1/payments',
            data=json.dumps(payload_mp).encode(),
            method='POST',
            headers={
                'Authorization': f'Bearer {mp3_token}',
                'Content-Type': 'application/json',
                'X-Idempotency-Key': ext_ref,
            }
        )
        with urllib.request.urlopen(req_mp, timeout=15) as r:
            resp_mp = json.loads(r.read())

        mp_id    = resp_mp.get('id')
        pix_data = resp_mp.get('point_of_interaction', {}).get('transaction_data', {})
        qr_code  = pix_data.get('qr_code', '')
        qr_b64   = pix_data.get('qr_code_base64', '')

        if not mp_id or not qr_code:
            msg_err = resp_mp.get('message') or resp_mp.get('error') or 'Erro ao gerar PIX'
            return web.json_response({'success': False, 'error': msg_err}, headers=headers)

        # Salvar transação na tabela mp3_transacoes
        conn_t = _pg3g.connect(DATABASE_URL, connect_timeout=8)
        cur_t  = conn_t.cursor()
        # Garantir tabelas existem
        cur_t.execute("""CREATE TABLE IF NOT EXISTS mp3_transacoes (
            id SERIAL PRIMARY KEY, telegram_id BIGINT, tipo TEXT DEFAULT 'deposito',
            valor NUMERIC(12,2), status TEXT DEFAULT 'pendente',
            mp_payment_id TEXT, mp_external_ref TEXT, pix_copia_cola TEXT, pix_qr_base64 TEXT,
            descricao TEXT, parceiro_codigo VARCHAR(50), comissao_valor NUMERIC(12,2) DEFAULT 0,
            comissao_status VARCHAR(20) DEFAULT 'pendente',
            criado_em TIMESTAMP DEFAULT NOW(), atualizado_em TIMESTAMP DEFAULT NOW()
        )""")
        cur_t.execute(
            """INSERT INTO mp3_transacoes
               (telegram_id, tipo, valor, status, mp_payment_id, mp_external_ref,
                pix_copia_cola, pix_qr_base64, descricao, parceiro_codigo, comissao_valor)
               VALUES (NULL,'deposito',%s,'pendente',%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (valor, str(mp_id), ext_ref, qr_code, qr_b64,
             f'Pagamento PayPix2 — {nome_pag}', parceiro_codigo, comissao_valor)
        )
        tx_db_id = cur_t.fetchone()[0]
        conn_t.commit(); cur_t.close(); conn_t.close()

        print(f'✅ [mp3_gerar] PIX gerado: {ext_ref} R${valor:.2f} mp_id={mp_id}', flush=True)
        return web.json_response({
            'success':  True,
            'tx_id':    ext_ref,
            'db_id':    tx_db_id,
            'mp_id':    mp_id,
            'pix_code': qr_code,
            'qr_base64': qr_b64,
            'valor':    valor,
            'poll_url': f'/api/mp3/status/{ext_ref}',
        }, headers=headers)

    except urllib.error.HTTPError as e:
        body_err = e.read().decode('utf-8', errors='replace')
        print(f'[mp3_gerar] MP HTTP {e.code}: {body_err[:300]}', flush=True)
        try:
            err_json = json.loads(body_err)
            msg = err_json.get('message') or err_json.get('error') or f'Erro MP {e.code}'
        except Exception:
            msg = f'Erro Mercado Pago: {e.code}'
        return web.json_response({'success': False, 'error': msg}, headers=headers)
    except Exception as e:
        print(f'[mp3_gerar] Erro: {e}', flush=True)
        return web.json_response({'success': False, 'error': str(e)}, headers=headers, status=500)


async def route_mp3_status_tx(request):
    """GET /api/mp3/status/{tx_id} — Status do PIX gerado via paypix2."""
    headers = {'Access-Control-Allow-Origin': '*'}
    tx_id = request.match_info.get('tx_id', '')
    if not tx_id:
        return web.json_response({'success': False, 'status': 'nao_encontrado'}, headers=headers)
    try:
        import psycopg2 as _pg3st2
        conn = _pg3st2.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute(
            "SELECT id, valor, status, mp_payment_id, pix_copia_cola FROM mp3_transacoes WHERE mp_external_ref=%s",
            (tx_id,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            return web.json_response({'success': False, 'status': 'nao_encontrado'}, headers=headers)

        db_id, valor, status, mp_id, pix_code = row

        # Se ainda pendente, consultar MP para checar se foi pago
        if status == 'pendente' and mp_id:
            mp3_token = _mp3_get_access_token()
            if mp3_token:
                try:
                    req_chk = urllib.request.Request(
                        f'https://api.mercadopago.com/v1/payments/{mp_id}',
                        headers={'Authorization': f'Bearer {mp3_token}'}
                    )
                    with urllib.request.urlopen(req_chk, timeout=10) as r:
                        info = json.loads(r.read())
                    if info.get('status') == 'approved':
                        # Confirmar no banco
                        conn2 = _pg3st2.connect(DATABASE_URL, connect_timeout=8)
                        cur2  = conn2.cursor()
                        cur2.execute(
                            "UPDATE mp3_transacoes SET status='confirmado', atualizado_em=NOW() "
                            "WHERE mp_external_ref=%s AND status='pendente'",
                            (tx_id,)
                        )
                        conn2.commit(); cur2.close(); conn2.close()
                        status = 'confirmado'
                        print(f'✅ [mp3_status] PIX confirmado via polling: {tx_id}', flush=True)
                except Exception as _ep:
                    pass  # Silencioso

        resp = {
            'success': True,
            'tx_id':   tx_id,
            'status':  status,
            'valor':   float(valor or 0),
        }
        if pix_code:
            resp['pix_code'] = pix_code
        if status in ('confirmado', 'approved'):
            resp['pago'] = True
        return web.json_response(resp, headers=headers)

    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, headers=headers, status=500)


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
        nome_pag      = str(data.get('nome', 'Cliente')).strip() or 'Cliente'
        email_pag     = str(data.get('email', '')).strip()
        cpf_pag       = str(data.get('cpf', '')).strip()
        expira_min    = int(data.get('expiracao_minutos', 30))

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
            nome_pagador=nome_pag,
            descricao=desc,
            email_pagador=email_pag,
            cpf_pagador=cpf_pag,
            expiracao_minutos=expira_min,
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
            'ticket_url':     resultado.get('ticket_url', ''),
            'valor':          valor,
            'expira_em':      resultado.get('expira_em', 30),
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


async def route_bot_config_get(request):
    """GET /api/bot/config - retorna configurações públicas da página /bot e /pix."""

    try:
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        campos = ['bot_titulo', 'bot_descricao', 'bot_valor_fixo', 'bot_modo_fixo',
                  'bot_min_valor', 'bot_max_valor', 'bot_ativo']
        cfg = {}
        for c in campos:
            cur.execute("SELECT valor FROM mp2_config WHERE chave = %s", (c,))
            row = cur.fetchone()
            cfg[c.replace('bot_', '')] = row[0] if row else None
        cur.close(); conn.close()
        # Defaults
        return web.json_response({
            'success':      True,
            'titulo':       cfg.get('titulo')      or 'PayPixNex — Pague via PIX',
            'descricao':    cfg.get('descricao')   or 'Pagamento seguro via Mercado Pago',
            'valor_fixo':   float(cfg.get('valor_fixo') or 0),
            'modo_fixo':    (cfg.get('modo_fixo') or 'false').lower() == 'true',
            'min_valor':    float(cfg.get('min_valor') or 5),
            'max_valor':    float(cfg.get('max_valor') or 10000),
            'ativo':        (cfg.get('ativo') or 'true').lower() == 'true',
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_bot_config_save(request):
    """POST /api/bot/config - salva configurações do bot de pagamento."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body = await request.json()
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        pares = [
            ('bot_titulo',      str(body.get('titulo', 'PayPixNex — Pague via PIX'))),
            ('bot_descricao',   str(body.get('descricao', 'Pagamento seguro via Mercado Pago'))),
            ('bot_valor_fixo',  str(float(body.get('valor_fixo', 0)))),
            ('bot_modo_fixo',   'true' if body.get('modo_fixo') else 'false'),
            ('bot_min_valor',   str(float(body.get('min_valor', 5)))),
            ('bot_max_valor',   str(float(body.get('max_valor', 10000)))),
            ('bot_ativo',       'true' if body.get('ativo', True) else 'false'),
        ]
        for chave, valor in pares:
            cur.execute("""

                INSERT INTO mp2_config (chave, valor) VALUES (%s, %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (chave, valor))
        conn.commit()
        cur.close(); conn.close()
        print(f'✅ [bot_config] Configurações salvas: modo_fixo={body.get("modo_fixo")}, valor_fixo={body.get("valor_fixo")}', flush=True)
        return web.json_response({'success': True, 'mensagem': 'Configurações salvas!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_bot_config_save_html(request):
    """POST /api/bot/config/save-html - persiste bot_pix.html no banco (para servir sem arquivo local)."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        if not os.path.exists('bot_pix.html'):
            return web.json_response({'success': False, 'error': 'bot_pix.html não encontrado'})
        html = open('bot_pix.html', encoding='utf-8').read()
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("""

            INSERT INTO configuracoes (chave, valor) VALUES ('bot_pix_html_patch', %s)
            ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
        """, (html,))
        conn.commit()
        cur.close(); conn.close()
        return web.json_response({'success': True, 'chars': len(html)})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


# ═══════════════════════════════════════════════════════════════
#  ROTAS — RECORRÊNCIA / ASSINATURAS MENSAIS
# ═══════════════════════════════════════════════════════════════

async def route_recorrente_criar(request):
    """POST /api/recorrente/criar — Cadastra novo assinante recorrente."""

    try:
        data = await request.json()
        nome    = str(data.get('nome', '')).strip()
        valor   = float(data.get('valor', 0))
        dia     = int(data.get('dia_vencimento', 1))
        desc    = str(data.get('descricao', 'Mensalidade')).strip()
        email   = str(data.get('email', '')).strip()
        tel     = str(data.get('telefone', '')).strip()
        ref     = str(data.get('ref', '')).strip()
        if not nome:
            return web.json_response({'success': False, 'error': 'Informe o nome do assinante'})
        if valor < 1:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 1,00'})
        from mp2_api import mp2_criar_recorrente
        result = mp2_criar_recorrente(nome=nome, valor=valor, dia_vencimento=dia,
                                      descricao=desc, email=email, telefone=tel,
                                      parceiro_ref=ref)
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_recorrente_listar(request):
    """GET /api/recorrente/listar — Lista assinantes recorrentes."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    status_filtro = request.rel_url.query.get('status', '')
    from mp2_api import mp2_listar_recorrentes, mp2_stats_recorrentes
    lista = mp2_listar_recorrentes(status_filtro)
    stats = mp2_stats_recorrentes()
    return web.json_response({'success': True, 'recorrentes': lista, 'stats': stats})


async def route_recorrente_cancelar(request):
    """POST /api/recorrente/cancelar — Cancela uma assinatura."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        data = await request.json()
        rid = int(data.get('id', 0))
        from mp2_api import mp2_cancelar_recorrente
        return web.json_response(mp2_cancelar_recorrente(rid))
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_recorrente_pausar(request):
    """POST /api/recorrente/pausar — Pausa ou reactiva uma assinatura."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        data = await request.json()
        rid   = int(data.get('id', 0))
        pausar = bool(data.get('pausar', True))
        from mp2_api import mp2_pausar_recorrente
        return web.json_response(mp2_pausar_recorrente(rid, pausar))
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_recorrente_cobrar(request):
    """POST /api/recorrente/cobrar — Gera PIX agora para um assinante específico."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        data = await request.json()
        rid = int(data.get('id', 0))
        # Garante token MP carregado
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
            except Exception: pass
        result = mp2_api.mp2_cobrar_recorrente(rid)
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_recorrente_pagar_link(request):
    """GET /pagar/{id} — Página pública de pagamento da mensalidade recorrente."""

    try:
        rid = int(request.match_info.get('id', 0))
        import mp2_api, psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor(cursor_factory=_pg.extras.RealDictCursor)
        cur.execute("SELECT * FROM mp2_recorrentes WHERE id=%s", (rid,))
        rec = cur.fetchone()
        cur.close(); conn.close()
        if not rec:
            return web.Response(text='<h1>Assinatura não encontrada</h1>', content_type='text/html')
        rec = dict(rec)
        nome  = rec.get('nome', 'Assinante')
        valor = float(rec.get('valor', 0))
        desc  = rec.get('descricao', 'Mensalidade')
        prox  = str(rec.get('proximo_vencimento', ''))
        # Serve a bot_pix.html com parâmetros no URL — o JS vai preencher automaticamente
        html = load_bot_pix_html()
        if html:
            return web.Response(
                text=html, content_type='text/html',
                headers={'X-Recorrente-Id': str(rid)}
            )
        return web.Response(
            text=f'<h1>Pague sua mensalidade</h1><p>{nome} — R${valor:.2f} — {desc}</p>',
            content_type='text/html'
        )
    except Exception as e:
        return web.Response(text=f'<h1>Erro: {e}</h1>', content_type='text/html')


async def route_recorrente_stats(request):
    """GET /api/recorrente/stats — Estatísticas públicas + admin."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    from mp2_api import mp2_stats_recorrentes
    return web.json_response({'success': True, **mp2_stats_recorrentes()})


async def _job_cobrar_vencimentos():
    """

    Job automático: roda diariamente e gera PIX para todos os assinantes
    com vencimento hoje ou em atraso. Chamado pelo background task.
    """

    import mp2_api
    print('🔄 [job_recorrente] Verificando vencimentos...', flush=True)
    # Garante token MP
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
            print(f'[job_recorrente] Erro token: {_e}', flush=True)
            return 0

    vencidos = mp2_api.mp2_vencimentos_hoje()
    total = 0
    for rec in vencidos:
        rid   = rec['id']
        nome  = rec.get('nome', '?')
        valor = float(rec.get('valor', 0))
        try:
            resultado = mp2_api.mp2_cobrar_recorrente(rid)
            if resultado.get('success'):
                total += 1
                print(f'  ✅ PIX gerado: {nome} R${valor:.2f} | {resultado.get("payment_id","")}', flush=True)
            else:
                print(f'  ⚠️ Falha PIX {nome}: {resultado.get("error","")}', flush=True)
        except Exception as ex:
            print(f'  ❌ Erro ao cobrar {nome}: {ex}', flush=True)

    print(f'✅ [job_recorrente] Finalizado: {total}/{len(vencidos)} PIX gerados', flush=True)
    return total


async def _background_job_recorrente(app):
    """Background task aiohttp: verifica vencimentos a cada 6 horas."""

    import asyncio
    await asyncio.sleep(30)  # Aguarda 30s após o startup
    while True:
        try:
            await _job_cobrar_vencimentos()
        except Exception as e:
            print(f'[bg_recorrente] Erro: {e}', flush=True)
        await asyncio.sleep(6 * 3600)  # A cada 6 horas


async def route_recorrente_job_manual(request):
    """POST /api/recorrente/job — Dispara o job manualmente (admin)."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    total = await _job_cobrar_vencimentos()
    return web.json_response({'success': True, 'pix_gerados': total})


async def route_assinar_page(request):
    """GET /assinar — Página pública de autocadastro via QR Code."""

    try:
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT valor FROM mp2_config WHERE chave='assinar_html_patch'")
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row[0]:
            return web.Response(text=row[0], content_type='text/html',
                                headers={'Cache-Control': 'no-store'})
    except Exception: pass
    if os.path.exists('assinar.html'):
        return web.Response(text=open('assinar.html', encoding='utf-8').read(),
                            content_type='text/html',
                            headers={'Cache-Control': 'no-store'})
    return web.Response(text='<h1>Página de assinatura não configurada</h1>',
                        content_type='text/html')


async def route_assinar_config_get(request):
    """GET /api/assinar/config — config pública do plano de assinatura."""

    try:
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        campos = ['assinar_titulo', 'assinar_descricao', 'assinar_valor',
                  'assinar_dia', 'assinar_ativo', 'assinar_cor', 'assinar_beneficios']
        cur.execute("SELECT chave, valor FROM mp2_config WHERE chave = ANY(%s)", (campos,))
        cfg = dict(cur.fetchall())
        cur.close(); conn.close()
        return web.json_response({
            'success':     True,
            'titulo':      cfg.get('assinar_titulo')      or 'Assine e pague todo mês via PIX',
            'descricao':   cfg.get('assinar_descricao')   or 'Assinatura mensal automática',
            'valor':       float(cfg.get('assinar_valor') or 10),
            'dia':         int(cfg.get('assinar_dia')     or 1),
            'ativo':       (cfg.get('assinar_ativo') or 'true').lower() == 'true',
            'cor':         cfg.get('assinar_cor')         or '#9c27b0',
            'beneficios':  cfg.get('assinar_beneficios')  or '',
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_assinar_config_save(request):
    """POST /api/assinar/config — salva config do plano."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        body = await request.json()
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        pares = [
            ('assinar_titulo',      str(body.get('titulo', 'Assine agora'))),
            ('assinar_descricao',   str(body.get('descricao', 'Assinatura mensal'))),
            ('assinar_valor',       str(float(body.get('valor', 10)))),
            ('assinar_dia',         str(int(body.get('dia', 1)))),
            ('assinar_ativo',       'true' if body.get('ativo', True) else 'false'),
            ('assinar_cor',         str(body.get('cor', '#9c27b0'))),
            ('assinar_beneficios',  str(body.get('beneficios', ''))),
        ]
        for chave, valor in pares:
            cur.execute("""

                INSERT INTO mp2_config (chave, valor) VALUES (%s, %s)
                ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor
            """, (chave, valor))
        conn.commit()
        cur.close(); conn.close()
        return web.json_response({'success': True, 'mensagem': 'Configurações salvas!'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_autocadastro(request):
    """

    POST /api/recorrente/autocadastro
    Cliente preenche nome + dia → sistema cadastra + gera 1º PIX.
    Retorna: { success, recorrente_id, pix_copia_cola, qr_base64, payment_id, proximo_vencimento }
    """

    try:
        data  = await request.json()
        nome  = str(data.get('nome', '')).strip()
        tel   = str(data.get('telefone', '')).strip()
        email = str(data.get('email', '')).strip()
        dia   = int(data.get('dia', 1))
        ref   = str(data.get('ref', '')).strip()

        if not nome:
            return web.json_response({'success': False, 'error': 'Informe seu nome'})

        # Busca config do plano
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT chave, valor FROM mp2_config WHERE chave IN "
                    "('assinar_valor','assinar_dia','assinar_descricao','assinar_ativo')")
        cfg = dict(cur.fetchall())
        cur.close(); conn.close()

        if (cfg.get('assinar_ativo') or 'true').lower() != 'true':
            return web.json_response({'success': False, 'error': 'Assinaturas temporariamente desativadas'})

        valor = float(cfg.get('assinar_valor') or 10)
        desc  = cfg.get('assinar_descricao') or 'Mensalidade'
        # dia do plano prevalece se não veio no body
        if dia <= 0:
            dia = int(cfg.get('assinar_dia') or 1)
        dia = max(1, min(28, dia))

        # Garante token MP
        import mp2_api
        if not mp2_api.MP2_ACCESS_TOKEN:
            try:
                conn2 = _pg.connect(DATABASE_URL, connect_timeout=8)
                cur2  = conn2.cursor()
                cur2.execute("SELECT valor FROM mp2_config WHERE chave='mp2_access_token'")
                row = cur2.fetchone()
                cur2.close(); conn2.close()
                if row and row[0]:
                    mp2_api.MP2_ACCESS_TOKEN = row[0]
            except Exception: pass

        if not mp2_api.MP2_ACCESS_TOKEN:
            return web.json_response({'success': False,
                                      'error': 'Pagamentos não configurados. Contate o suporte.'})

        # 1) Cadastra assinante
        rec_result = mp2_api.mp2_criar_recorrente(
            nome=nome, valor=valor, dia_vencimento=dia,
            descricao=desc, email=email, telefone=tel, parceiro_ref=ref
        )
        if not rec_result.get('success'):
            return web.json_response({'success': False,
                                      'error': rec_result.get('error', 'Erro ao cadastrar')})

        rec_id = rec_result['recorrente']['id']

        # 2) Gera primeiro PIX
        pix_result = mp2_api.mp2_cobrar_recorrente(rec_id)
        if not pix_result.get('success'):
            return web.json_response({'success': False,
                                      'error': pix_result.get('error', 'Erro ao gerar PIX'),
                                      'recorrente_id': rec_id})

        print(f'✅ [autocadastro] {nome} | R${valor:.2f}/mês dia {dia} | PIX {pix_result.get("payment_id","")}', flush=True)

        return web.json_response({
            'success':            True,
            'recorrente_id':      rec_id,
            'nome':               nome,
            'valor':              valor,
            'dia':                dia,
            'proximo_vencimento': pix_result.get('proximo_vencimento', ''),
            'payment_id':         pix_result.get('payment_id', ''),
            'external_ref':       pix_result.get('external_ref', ''),
            'pix_copia_cola':     pix_result.get('pix_copia_cola', ''),
            'qr_base64':          pix_result.get('qr_base64', ''),
        })

    except Exception as e:
        print(f'[autocadastro] Erro: {e}', flush=True)
        return web.json_response({'success': False, 'error': str(e)})


async def route_assinar_qr_gerado(request):
    """GET /api/assinar/qr — gera QR Code PNG do link /assinar para o admin imprimir."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    try:
        import qrcode, io
        ref   = request.rel_url.query.get('ref', '')
        valor = request.rel_url.query.get('valor', '')
        base  = str(request.url.origin())
        link  = f'{base}/assinar'
        if ref:   link += f'?ref={ref}'
        if valor: link += ('&' if ref else '?') + f'valor={valor}'
        qr  = qrcode.QRCode(box_size=10, border=4)
        qr.add_data(link)
        qr.make(fit=True)
        img = qr.make_image(fill_color='#9c27b0', back_color='white')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return web.Response(body=buf.read(), content_type='image/png',
                            headers={'Content-Disposition': 'inline; filename="qr-assinar.png"'})
    except ImportError:
        # fallback: retorna URL para QR externo
        ref   = request.rel_url.query.get('ref', '')
        base  = str(request.url.origin())
        link  = f'{base}/assinar' + (f'?ref={ref}' if ref else '')
        import urllib.parse
        qr_url = f'https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={urllib.parse.quote(link)}'
        return web.Response(body=b'', status=302,
                            headers={'Location': qr_url})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


# ═══════════════════════════════════════════════════════════════
#  PREAPPROVAL — DÉBITO AUTOMÁTICO PIX (MERCADO PAGO)
# ═══════════════════════════════════════════════════════════════

async def _garantir_token_mp():
    """Garante que mp2_api.MP2_ACCESS_TOKEN está carregado do banco."""

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
        except Exception: pass
    return bool(mp2_api.MP2_ACCESS_TOKEN)


async def route_preapproval_criar_plano(request):
    """

    POST /api/assinar/criar-plano
    Cria (ou recria) o preapproval_plan no MP com PIX obrigatório.
    Deve ser chamado 1x pelo admin após configurar o plano.
    """

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)

    if not await _garantir_token_mp():
        return web.json_response({'success': False, 'error': 'Access Token do Mercado Pago não configurado'})

    import mp2_api
    # Lê config do plano
    try:
        import psycopg2 as _pg
        conn = _pg.connect(DATABASE_URL, connect_timeout=8)
        cur  = conn.cursor()
        cur.execute("SELECT chave, valor FROM mp2_config WHERE chave IN "
                    "('assinar_titulo','assinar_valor','assinar_descricao')")
        cfg = dict(cur.fetchall())
        cur.close(); conn.close()
    except Exception:
        cfg = {}

    titulo = cfg.get('assinar_titulo') or 'Mensalidade'
    valor  = float(cfg.get('assinar_valor') or 10)
    result = mp2_api.mp2_criar_plano_preapproval(titulo=titulo, valor=valor)
    return web.json_response(result)


async def route_preapproval_assinar(request):
    """

    POST /api/assinar/preapproval
    Registra o interesse do cliente e retorna o init_point do plano ativo no MP.
    O cliente acessa o init_point e autoriza o débito automático via PIX no app MP.
    Body: { nome, email, telefone, ref }
    Retorna: { success, preapproval_id, init_point, recorrente_id }
    """

    try:
        data  = await request.json()
        nome  = str(data.get('nome', '')).strip()
        email = str(data.get('email', '')).strip()
        tel   = str(data.get('telefone', '')).strip()
        ref   = str(data.get('ref', '')).strip()

        if not nome:
            return web.json_response({'success': False, 'error': 'Informe seu nome'})

        if not await _garantir_token_mp():
            return web.json_response({'success': False,
                                      'error': 'Pagamentos não configurados. Contate o suporte.'})

        import mp2_api
        # Pega configurações do plano
        plano   = mp2_api.mp2_get_plano_ativo()
        plan_id = plano.get('plan_id', '')
        valor   = plano.get('valor', 10.0)
        desc    = plano.get('descricao', 'Mensalidade')
        titulo  = plano.get('titulo', 'Mensalidade')

        # Se não tem plano criado ainda, cria agora
        if not plan_id:
            pr = mp2_api.mp2_criar_plano_preapproval(titulo=titulo, valor=valor)
            if not pr.get('success'):
                return web.json_response({'success': False,
                                          'error': f"Erro ao criar plano: {pr.get('error')}"})
            plan_id    = pr['plan_id']
            init_point = pr['init_point']
        else:
            # Recupera o init_point do plano existente
            init_point = plano.get('init_point', '')
            if not init_point:
                # Busca o init_point do plano no MP
                try:
                    import requests as _req
                    _r = _req.get(
                        f'https://api.mercadopago.com/preapproval_plan/{plan_id}',
                        headers={'Authorization': f'Bearer {mp2_api.MP2_ACCESS_TOKEN}'},
                        timeout=8
                    )
                    _d = _r.json()
                    init_point = _d.get('init_point', '')
                except Exception:
                    pass

        # Pré-registra o assinante no banco com status 'pendente_auth'
        # (status muda para 'ativo' quando o webhook confirmar)
        rec = mp2_api.mp2_criar_recorrente(
            nome=nome, valor=valor, dia_vencimento=1,
            descricao=desc, email=email, telefone=tel, parceiro_ref=ref
        )
        rec_id = rec.get('recorrente', {}).get('id')

        # Marca como tipo preapproval com status pendente
        if rec_id:
            try:
                import psycopg2 as _pg
                conn = _pg.connect(DATABASE_URL, connect_timeout=8)
                cur  = conn.cursor()
                cur.execute("""

                    UPDATE mp2_recorrentes
                    SET tipo                  = 'preapproval',
                        preapproval_plan_id   = %s,
                        preapproval_init_point= %s,
                        preapproval_status    = 'pending',
                        status                = 'pendente_auth',
                        atualizado_em         = NOW()
                    WHERE id = %s
                """, (plan_id, init_point, rec_id))
                conn.commit(); cur.close(); conn.close()
            except Exception as _e:
                print(f'[preapproval_assinar] Erro update: {_e}', flush=True)

        # Gera um preapproval_id temporário baseado no rec_id para polling
        # O ID real virá do webhook quando o cliente autorizar
        tmp_pre_id = f'pending_rec_{rec_id}'

        print(f'✅ [preapproval] Pré-registro: {nome} → plano {plan_id} | link enviado', flush=True)
        return web.json_response({
            'success':        True,
            'preapproval_id': tmp_pre_id,
            'init_point':     init_point,
            'recorrente_id':  rec_id,
            'plan_id':        plan_id,
        })

    except Exception as e:
        print(f'[preapproval_assinar] Erro: {e}', flush=True)
        return web.json_response({'success': False, 'error': str(e)})


async def route_preapproval_status(request):
    """

    GET /api/assinar/status/{preapproval_id}
    Verifica status do preapproval no MP.
    Aceita:
      - IDs reais do MP (ex: dca1eca6...)
      - IDs temporários no formato 'pending_rec_{rec_id}'
    """

    pre_id = request.match_info.get('preapproval_id', '')
    if not pre_id:
        return web.json_response({'success': False, 'error': 'ID não informado'})

    # Caso seja ID temporário (pending_rec_X) — verifica o status pelo rec_id no banco
    if pre_id.startswith('pending_rec_'):
        try:
            rec_id = int(pre_id.replace('pending_rec_', ''))
            import psycopg2 as _pg
            conn = _pg.connect(DATABASE_URL, connect_timeout=8)
            cur  = conn.cursor()
            cur.execute("""

                SELECT preapproval_id, preapproval_status, status
                FROM mp2_recorrentes WHERE id = %s
            """, (rec_id,))
            row = cur.fetchone(); cur.close(); conn.close()

            if not row:
                return web.json_response({'success': True, 'status': 'pending', 'autorizado': False})

            real_pre_id, pre_status, rec_status = row
            # Se o webhook já registrou um preapproval_id real, verifica no MP
            if real_pre_id and not real_pre_id.startswith('pending_'):
                if await _garantir_token_mp():
                    import mp2_api, requests as _req
                    r = _req.get(
                        f'https://api.mercadopago.com/preapproval/{real_pre_id}',
                        headers={'Authorization': f'Bearer {mp2_api.MP2_ACCESS_TOKEN}'},
                        timeout=8
                    )
                    d = r.json()
                    status = d.get('status', 'pending')
                    autorizado = status == 'authorized'
                    if autorizado:
                        mp2_api.mp2_webhook_preapproval(real_pre_id)
                    return web.json_response({'success': True, 'status': status, 'autorizado': autorizado})

            # Caso contrário, verifica o status local
            autorizado = pre_status == 'authorized' or rec_status == 'ativo'
            return web.json_response({
                'success':   True,
                'status':    pre_status or 'pending',
                'autorizado': autorizado,
            })
        except Exception as e:
            return web.json_response({'success': False, 'error': str(e)})

    # ID real do MP — consulta direto na API
    if not await _garantir_token_mp():
        return web.json_response({'success': False, 'error': 'Token MP não configurado'})
    try:
        import mp2_api, requests as _req
        r = _req.get(
            f'https://api.mercadopago.com/preapproval/{pre_id}',
            headers={'Authorization': f'Bearer {mp2_api.MP2_ACCESS_TOKEN}'},
            timeout=8
        )
        d = r.json()
        status = d.get('status', 'pending')
        autorizado = status == 'authorized'
        if autorizado:
            mp2_api.mp2_webhook_preapproval(pre_id)
        return web.json_response({
            'success':    True,
            'status':     status,
            'autorizado': autorizado,
            'next_payment_date': d.get('next_payment_date', ''),
        })
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def route_preapproval_webhook(request):
    """

    POST /webhook/preapproval
    Recebe notificações do MP quando um preapproval muda de status.
    Também atualiza o banco local com o preapproval_id real quando o cliente autoriza.
    """

    try:
        data = await request.json()
        topic  = data.get('type') or data.get('topic', '')
        res_id = (data.get('data', {}) or {}).get('id') or data.get('id', '')

        print(f'[webhook preapproval] topic={topic} id={res_id}', flush=True)

        if topic in ('subscription_preapproval', 'preapproval') and res_id:
            if await _garantir_token_mp():
                import mp2_api, requests as _req
                # Busca detalhes do preapproval no MP
                r = _req.get(
                    f'https://api.mercadopago.com/preapproval/{res_id}',
                    headers={'Authorization': f'Bearer {mp2_api.MP2_ACCESS_TOKEN}'},
                    timeout=8
                )
                d = r.json()
                status     = d.get('status', 'pending')
                payer_email = d.get('payer_email', '') or d.get('payer', {}).get('email', '')
                plan_id    = d.get('preapproval_plan_id', '')

                print(f'✅ [webhook preapproval] {res_id} → {status} | email={payer_email}', flush=True)

                # Tenta vincular ao assinante pré-registrado (por e-mail ou pelo plano pendente)
                try:
                    import psycopg2 as _pg
                    conn = _pg.connect(DATABASE_URL, connect_timeout=8)
                    cur  = conn.cursor()

                    # Primeiro: tenta por preapproval_id já registrado
                    cur.execute("""

                        UPDATE mp2_recorrentes
                        SET preapproval_id     = %s,
                            preapproval_status = %s,
                            status = CASE WHEN %s = 'authorized' THEN 'ativo'
                                          WHEN %s IN ('cancelled','paused') THEN %s
                                          ELSE status END,
                            atualizado_em = NOW()
                        WHERE preapproval_id = %s
                    """, (res_id, status, status, status, status, res_id))

                    # Segundo: tenta por e-mail do pagador (pré-registro com status pendente_auth)
                    if payer_email and cur.rowcount == 0:
                        cur.execute("""

                            UPDATE mp2_recorrentes
                            SET preapproval_id     = %s,
                                preapproval_status = %s,
                                status = CASE WHEN %s = 'authorized' THEN 'ativo'
                                              WHEN %s IN ('cancelled','paused') THEN %s
                                              ELSE status END,
                                atualizado_em = NOW()
                            WHERE email = %s AND tipo = 'preapproval'
                              AND (preapproval_id IS NULL OR preapproval_id LIKE 'pending_%%')
                            ORDER BY criado_em DESC LIMIT 1
                        """, (res_id, status, status, status, status, payer_email))

                    # Terceiro: o mais recente pendente_auth do mesmo plano
                    if cur.rowcount == 0 and plan_id:
                        cur.execute("""

                            UPDATE mp2_recorrentes
                            SET preapproval_id     = %s,
                                preapproval_status = %s,
                                status = CASE WHEN %s = 'authorized' THEN 'ativo'
                                              WHEN %s IN ('cancelled','paused') THEN %s
                                              ELSE status END,
                                atualizado_em = NOW()
                            WHERE tipo = 'preapproval'
                              AND preapproval_plan_id = %s
                              AND status = 'pendente_auth'
                            ORDER BY criado_em DESC LIMIT 1
                        """, (res_id, status, status, status, status, plan_id))

                    conn.commit(); cur.close(); conn.close()
                    print(f'✅ [webhook] Banco atualizado para preapproval {res_id}', flush=True)
                except Exception as _e:
                    print(f'[webhook preapproval] Erro DB: {_e}', flush=True)

        return web.json_response({'status': 'ok'})
    except Exception as e:
        print(f'[webhook preapproval] Erro: {e}', flush=True)
        return web.json_response({'status': 'ok'})  # sempre 200 para o MP


async def route_preapproval_plano_info(request):
    """GET /api/assinar/plano — retorna dados do plano ativo (plan_id + init_point)."""

    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.Response(text='Não autorizado', status=401)
    import mp2_api
    plano = mp2_api.mp2_get_plano_ativo()
    return web.json_response({'success': True, **plano})


async def route_assinar_obrigado(request):
    """GET /assinar/obrigado — Página de retorno após autorização no MP."""

    status  = request.rel_url.query.get('status', '')
    pre_id  = request.rel_url.query.get('preapproval_id', '')
    nome    = request.rel_url.query.get('nome', 'Assinante')

    # Atualiza status no banco se veio preapproval_id
    if pre_id:
        try:
            import mp2_api
            if await _garantir_token_mp():
                mp2_api.mp2_webhook_preapproval(pre_id)
        except Exception: pass

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Assinatura Confirmada — PayPixNex</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0a0a0f;color:#f0f0f0;font-family:'Segoe UI',system-ui,sans-serif;
     display:flex;flex-direction:column;align-items:center;justify-content:center;
     min-height:100vh;padding:30px 20px;text-align:center}}
.card{{background:#13131e;border:1px solid #9c27b030;border-radius:24px;
       padding:40px 30px;max-width:420px;width:100%;
       box-shadow:0 8px 40px rgba(156,39,176,.2)}}
.icon{{font-size:72px;margin-bottom:20px;animation:pop .5s ease}}
@keyframes pop{{0%{{transform:scale(0);opacity:0}}70%{{transform:scale(1.2)}}100%{{transform:scale(1);opacity:1}}}}
h1{{font-size:24px;font-weight:900;color:#ce93d8;margin-bottom:10px}}
p{{font-size:14px;color:#aaa;line-height:1.6;margin-bottom:20px}}
.badge{{display:inline-flex;align-items:center;gap:8px;background:#9c27b020;
        border:1px solid #9c27b050;border-radius:12px;padding:12px 20px;
        font-size:14px;font-weight:700;color:#ce93d8;margin-bottom:24px}}
.info-box{{background:#0a0a1a;border-radius:14px;padding:16px;text-align:left;margin-bottom:20px}}
.info-row{{display:flex;gap:10px;padding:6px 0;font-size:13px;border-bottom:1px solid #ffffff0a}}
.info-row:last-child{{border-bottom:none}}
.info-icon{{font-size:18px;flex-shrink:0}}
.info-txt{{color:#ccc;line-height:1.4}}
.btn{{display:block;width:100%;padding:16px;background:linear-gradient(135deg,#9c27b0,#6a0080);
      border:none;border-radius:14px;color:#fff;font-size:15px;font-weight:800;
      cursor:pointer;text-decoration:none;margin-top:6px}}
</style>
</head>
<body>
<div class="card">
  <div class="icon">{'🎉' if status != 'failure' else '⚠️'}</div>
  <h1>{'Assinatura Autorizada!' if status != 'failure' else 'Autorização Pendente'}</h1>
  <p>{'Parabéns! Você autorizou o débito automático mensal via PIX. A cobrança será processada automaticamente todo mês.' if status != 'failure' else 'A autorização não foi concluída. Você pode tentar novamente na página de assinatura.'}</p>
  <div class="badge">🔄 Débito Automático Ativo</div>
  <div class="info-box">
    <div class="info-row"><span class="info-icon">✅</span><span class="info-txt"><strong>1ª cobrança:</strong> Será processada pelo Mercado Pago em breve</span></div>
    <div class="info-row"><span class="info-icon">🔄</span><span class="info-txt"><strong>Próximas mensalidades:</strong> Debitadas automaticamente todo mês do seu saldo MP</span></div>
    <div class="info-row"><span class="info-icon">📱</span><span class="info-txt"><strong>Notificação:</strong> Você receberá aviso no app Mercado Pago a cada cobrança</span></div>
    <div class="info-row"><span class="info-icon">🚫</span><span class="info-txt"><strong>Para cancelar:</strong> Acesse o app Mercado Pago → Assinaturas</span></div>
  </div>
  <a href="/assinar" class="btn">← Voltar</a>
</div>
</body>
</html>"""

    return web.Response(text=html, content_type='text/html',
                        headers={'Cache-Control': 'no-store'})


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

# ═══════════════════════════════════════════════════════════════════════════
# ███  BET — Apostas Esportivas (The Odds API + SuitPay)  ███
# ═══════════════════════════════════════════════════════════════════════════

import hashlib as _hashlib

_odds_cache       = {'ts': 0, 'data': []}
_ODDS_CACHE_TTL   = 60   # segundos

# ── API-Football (api-sports.io) ─────────────────────────────────────────────
_APIFOOTBALL_KEY  = '9f32a39147f38ec092f55c39abb14517'
_APIFOOTBALL_BASE = 'https://v3.football.api-sports.io'
_live_cache       = {'ts': 0, 'data': []}          # /odds/live — cache 30s
_LIVE_CACHE_TTL   = 30

# ── Apify Live Cache ─────────────────────────────────────────────────────────
_APIFY_TOKEN      = os.environ.get('APIFY_TOKEN', '')  # Configurar via Railway vars ou apify_config DB
_APIFY_ACTOR_ID   = 'rdOwbSNd2e3a5nTug'   # FlashScore Scraper Live (93k runs)
_APIFY_INTERVAL   = 120                    # segundos entre execuções (2 min)
_apify_live_cache = {'ts': 0, 'jogos': [], 'run_id': None, 'status': 'idle'}
_pred_cache       = {}                              # fixture_id → prediction
_PRED_CACHE_TTL   = 3600                            # 1 hora
_standings_cache  = {'ts': 0, 'data': {}}          # league_id → standings
_STANDINGS_TTL    = 1800                            # 30 min
_APISPORTS_HDR    = {'x-apisports-key': _APIFOOTBALL_KEY}

# ── SuitPay: suporte ao formato "CI|CS" numa variável só
def _suit_parse_keys():
    raw_ci = os.environ.get('SUITPAY_CI', '')
    raw_cs = os.environ.get('SUITPAY_CS', '')
    if '|' in raw_ci and (not raw_cs or raw_cs == raw_ci):
        parts = raw_ci.split('|', 1)
        return parts[0].strip(), parts[1].strip()
    return raw_ci.strip(), raw_cs.strip()

# ── Carregar chaves bet do PostgreSQL (fallback quando Railway não injeta env vars)
def _bet_load_keys_from_db():
    """Lê ODDS_API_KEY, SUITPAY_CI, SUITPAY_CS do PostgreSQL configuracoes"""
    try:
        import psycopg2 as _pg2
        if not DATABASE_URL:
            return {}
        conn = _pg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        cur.execute("""
            SELECT chave, valor FROM configuracoes
            WHERE chave IN ('bet_odds_api_key','bet_suitpay_ci','bet_suitpay_cs')
        """)
        rows = {r[0]: r[1] for r in cur.fetchall()}
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f'[bet_load_keys] erro DB: {e}', flush=True)
        return {}

def _get_odds_key():
    """Retorna ODDS_API_KEY: DB tem prioridade (permite troca sem redeploy), depois env var"""
    db = _bet_load_keys_from_db()
    k = db.get('bet_odds_api_key', '').strip()
    if k:
        return k
    return os.environ.get('ODDS_API_KEY', '').strip()

def _get_suit_keys():
    """Retorna (CI, CS): DB tem prioridade sobre env var (para permitir troca sem redeploy)"""
    # 1. Tentar DB primeiro — permite atualização via /api/bet/config sem redeploy
    db = _bet_load_keys_from_db()
    db_ci = db.get('bet_suitpay_ci', '').strip()
    db_cs = db.get('bet_suitpay_cs', '').strip()
    # Se o DB tem CI no formato pipe "ID|SECRET", separar
    if '|' in db_ci and (not db_cs or db_cs == db_ci):
        parts = db_ci.split('|', 1)
        db_ci, db_cs = parts[0].strip(), parts[1].strip()
    if db_ci and db_cs:
        return db_ci, db_cs
    # 2. Fallback: env vars
    ci, cs = _suit_parse_keys()
    return ci, cs

_SUIT_CI, _SUIT_CS = _suit_parse_keys()   # inicialização; funções acima recarregam em runtime
_SUIT_HOST        = os.environ.get('SUITPAY_HOST', 'https://ws.suitpay.app')
_SUIT_WEBHOOK_URL = os.environ.get('SUITPAY_WEBHOOK_URL', 'https://paynexbet.com/webhook/suitpay')

# ═══════════════════════════════════════════════════════════════════
# SISTEMA DE MARGEM DE LUCRO NAS ODDS
# ═══════════════════════════════════════════════════════════════════
_margem_cache = {}        # liga_key → margem_pct (cache em memória)
_margem_cache_ts = 0.0   # timestamp do último refresh

def _get_margem_liga(liga_key: str) -> float:
    """Retorna margem (%) configurada para uma liga. Default 7%."""
    import time as _t
    global _margem_cache, _margem_cache_ts
    # Recarregar do DB a cada 60s
    if _t.time() - _margem_cache_ts > 60:
        try:
            import psycopg2 as _pg2
            url = DATABASE_URL or _BET_DB_URL_FALLBACK
            conn = _pg2.connect(url)
            cur = conn.cursor()
            cur.execute("SELECT liga_key, margem_pct FROM bet_margem_ligas WHERE ativa=true")
            _margem_cache = {r[0]: float(r[1]) for r in cur.fetchall()}
            cur.close(); conn.close()
            _margem_cache_ts = _t.time()
        except Exception as _e:
            print(f'[margem] erro ao carregar DB: {_e}', flush=True)
    # Busca específica → fallback upcoming → fallback 7%
    return _margem_cache.get(liga_key) or _margem_cache.get('upcoming') or 7.0

def _aplicar_margem_odds(odds: dict, liga_key: str) -> tuple:
    """
    Aplica margem de overround nas odds.
    Retorna (odds_com_margem, margem_pct_usada).
    Fórmula: prob_imp = 1/odd; prob_ajust = prob_imp * (1 + margem/100);
             odd_final = 1 / prob_ajust
    """
    margem_pct = _get_margem_liga(liga_key)
    if margem_pct <= 0:
        return odds, 0.0

    fator = 1.0 + (margem_pct / 100.0)
    resultado = {}
    for k, v in odds.items():
        if v is None or v <= 1.0:
            resultado[k] = v
        else:
            nova_odd = round(v / fator, 2)
            resultado[k] = max(nova_odd, 1.01)  # nunca abaixo de 1.01
    return resultado, margem_pct

def _salvar_margens_db(margens: dict):
    """Salva dict {liga_key: margem_pct} no PostgreSQL."""
    import psycopg2 as _pg2
    url = DATABASE_URL or _BET_DB_URL_FALLBACK
    conn = _pg2.connect(url)
    cur = conn.cursor()
    for liga_key, pct in margens.items():
        cur.execute("""
            INSERT INTO bet_margem_ligas (liga_key, margem_pct, ativa, updated_at)
            VALUES (%s, %s, true, NOW())
            ON CONFLICT (liga_key) DO UPDATE
            SET margem_pct=EXCLUDED.margem_pct, updated_at=NOW()
        """, (liga_key, float(pct)))
    conn.commit(); cur.close(); conn.close()
    global _margem_cache_ts
    _margem_cache_ts = 0.0  # forçar reload

# ─── Rotas Admin: Margem ──────────────────────────────────────────────────────
async def route_admin_margem_get(request):
    """GET /api/admin/bet/margem — lista margens por liga."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect()
        cur = conn.cursor()
        cur.execute("SELECT liga_key, margem_pct, ativa, updated_at FROM bet_margem_ligas ORDER BY liga_key")
        rows = cur.fetchall()
        cur.close(); conn.close()
        ligas_info = []
        for r in rows:
            info = _ADMIN_LIGAS_MAP.get(r[0], {})
            ligas_info.append({
                'liga_key': r[0],
                'nome': info.get('nome', r[0]),
                'margem_pct': float(r[1]),
                'ativa': bool(r[2]),
                'updated_at': r[3].strftime('%d/%m/%Y %H:%M') if r[3] else '',
            })
        return web.json_response({'ok': True, 'ligas': ligas_info})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_margem_set(request):
    """POST /api/admin/bet/margem — define margem % para uma ou mais ligas."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        # Aceita {liga_key, margem_pct} ou {margens: {key: pct, ...}}
        if 'margens' in body:
            margens = {k: float(v) for k, v in body['margens'].items()}
        else:
            liga_key = body.get('liga_key', '')
            margem_pct = float(body.get('margem_pct', 7))
            if not liga_key:
                return web.json_response({'ok': False, 'error': 'liga_key obrigatório'}, status=400)
            margens = {liga_key: margem_pct}
        # Validar 0–50%
        for k, v in margens.items():
            if not (0 <= v <= 50):
                return web.json_response({'ok': False, 'error': f'Margem {v}% inválida — use entre 0% e 50%'}, status=400)
        _salvar_margens_db(margens)
        print(f'[admin/margem] Margens salvas: {margens}', flush=True)
        return web.json_response({'ok': True, 'saved': list(margens.keys()),
                                  'msg': f'✅ {len(margens)} margem(ns) salva(s) — ativas imediatamente'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ─── Rotas Admin: Dashboard financeiro de apostas ─────────────────────────────
async def route_admin_bet_dashboard(request):
    """GET /api/admin/bet/dashboard — GGR, volume, apostas por status."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect()
        cur = conn.cursor()
        # Totais por status
        cur.execute("""
            SELECT status,
                   COUNT(*) as qtd,
                   COALESCE(SUM(valor),0) as total_apostado,
                   COALESCE(SUM(retorno_potencial),0) as total_potencial
            FROM apostas GROUP BY status
        """)
        por_status = {r[0]: {'qtd': r[1], 'apostado': float(r[2]), 'potencial': float(r[3])}
                      for r in cur.fetchall()}
        # GGR = total apostado (LOST+pendente encerrado) - total pago (WON)
        cur.execute("SELECT COALESCE(SUM(valor),0) FROM apostas")
        total_apostado = float(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(retorno_potencial),0) FROM apostas WHERE status='WON'")
        total_pago_won = float(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(valor),0) FROM apostas WHERE status='LOST'")
        total_perdido_casa = float(cur.fetchone()[0])  # o que a casa ganhou
        ggr = total_perdido_casa - total_pago_won + total_perdido_casa  # simplificado
        ggr = total_perdido_casa  # lost → foi para a casa
        # Por liga (últimos 30 dias)
        cur.execute("""
            SELECT COALESCE(league_key,'outros'), COUNT(*), COALESCE(SUM(valor),0),
                   COALESCE(SUM(CASE WHEN status='LOST' THEN valor ELSE 0 END),0) as lucro,
                   COALESCE(SUM(CASE WHEN status='WON' THEN retorno_potencial ELSE 0 END),0) as pago
            FROM apostas
            WHERE criado_em >= NOW() - INTERVAL '30 days'
            GROUP BY league_key ORDER BY SUM(valor) DESC LIMIT 15
        """)
        por_liga = [{'liga': r[0], 'qtd': r[1], 'apostado': float(r[2]),
                     'lucro_bruto': float(r[3]), 'pago_won': float(r[4])} for r in cur.fetchall()]
        # Depósitos
        cur.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM depositos_suit WHERE status='PAID'")
        dep = cur.fetchone()
        # Saques
        cur.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM saques WHERE status='PAGO'")
        saq = cur.fetchone()
        # Últimas apostas pendentes
        cur.execute("""
            SELECT a.id, u.nome, a.jogo_nome, a.selecao, a.odd, a.valor,
                   a.retorno_potencial, a.status, a.criado_em, a.league_key
            FROM apostas a LEFT JOIN usuarios u ON u.id=a.usuario_id
            ORDER BY a.criado_em DESC LIMIT 20
        """)
        ultimas = []
        for r in cur.fetchall():
            ultimas.append({
                'id': r[0], 'usuario': r[1] or '?', 'jogo': r[2] or '', 'selecao': r[3] or '',
                'odd': float(r[4] or 0), 'valor': float(r[5] or 0),
                'retorno': float(r[6] or 0), 'status': r[7],
                'data': r[8].strftime('%d/%m %H:%M') if r[8] else '',
                'liga': r[9] or '',
            })
        cur.close(); conn.close()
        return web.json_response({
            'ok': True,
            'por_status': por_status,
            'total_apostado': total_apostado,
            'total_pago_won': total_pago_won,
            'ggr': total_perdido_casa,   # lucro real da casa
            'risco_atual': por_status.get('pendente', {}).get('potencial', 0),
            'depositos': {'qtd': dep[0], 'total': float(dep[1])},
            'saques': {'qtd': saq[0] if saq else 0, 'total': float(saq[1]) if saq else 0},
            'por_liga': por_liga,
            'ultimas_apostas': ultimas,
        })
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ─── Resolução de apostas ──────────────────────────────────────────────────────
async def route_admin_bet_resolver(request):
    """POST /api/admin/bet/resolver — resolve aposta manualmente (WON/LOST/cancelada)."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        aposta_id = int(body.get('aposta_id', 0))
        resultado = body.get('resultado', '').upper()  # 'WON' | 'LOST' | 'CANCELADA'
        if not aposta_id or resultado not in ('WON', 'LOST', 'CANCELADA'):
            return web.json_response({'ok': False, 'error': 'aposta_id e resultado (WON/LOST/CANCELADA) obrigatórios'}, status=400)
        conn = _admin_db_connect()
        cur = conn.cursor()
        cur.execute("SELECT id, usuario_id, valor, retorno_potencial, status FROM apostas WHERE id=%s", (aposta_id,))
        aposta = cur.fetchone()
        if not aposta:
            cur.close(); conn.close()
            return web.json_response({'ok': False, 'error': 'Aposta não encontrada'}, status=404)
        if aposta[4] not in ('pendente',):
            cur.close(); conn.close()
            return web.json_response({'ok': False, 'error': f'Aposta já resolvida: {aposta[4]}'}, status=400)
        usuario_id = aposta[1]
        valor_apostado = float(aposta[2])
        retorno = float(aposta[3])
        # Atualizar status da aposta
        cur.execute("""
            UPDATE apostas SET status=%s, resultado=%s, resolvido_em=NOW()
            WHERE id=%s
        """, (resultado, resultado, aposta_id))
        credito = 0.0
        if resultado == 'WON':
            cur.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (retorno, usuario_id))
            credito = retorno
        elif resultado == 'CANCELADA':
            cur.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (valor_apostado, usuario_id))
            credito = valor_apostado
        conn.commit(); cur.close(); conn.close()
        msg_credito = f' | R${credito:.2f} creditado' if credito > 0 else ''
        print(f'[admin/resolver] Aposta #{aposta_id} → {resultado}{msg_credito}', flush=True)
        return web.json_response({'ok': True, 'aposta_id': aposta_id, 'resultado': resultado,
                                  'credito': credito, 'msg': f'Aposta #{aposta_id} → {resultado}{msg_credito}'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_bet_resolver_auto(request):
    """POST /api/admin/bet/resolver-auto — tenta resolver todas pendentes via ESPN."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    resolved = await _auto_resolver_apostas_pendentes()
    return web.json_response({'ok': True, 'resolved': resolved})

async def _auto_resolver_apostas_pendentes():
    """Worker: busca apostas pendentes e tenta resolver pelo placar final ESPN."""
    import aiohttp, datetime as _dt
    resolved_count = 0
    try:
        conn = _admin_db_connect()
        cur = conn.cursor()
        # Só apostas com mais de 2h (tempo para jogo terminar)
        cur.execute("""
            SELECT id, usuario_id, jogo_id, jogo_nome, selecao, odd, valor, retorno_potencial, league_key
            FROM apostas
            WHERE status='pendente'
            AND criado_em < NOW() - INTERVAL '2 hours'
            LIMIT 50
        """)
        pendentes = cur.fetchall()
        cur.close(); conn.close()

        if not pendentes:
            return 0

        async with aiohttp.ClientSession(headers={'User-Agent': 'PaynexBet/1.0'}) as sess:
            for ap in pendentes:
                ap_id, user_id, jogo_id, jogo_nome, selecao, odd, valor, retorno, liga_key = ap
                valor = float(valor); retorno = float(retorno)
                resultado = await _verificar_resultado_espn(sess, jogo_id, jogo_nome, selecao, liga_key)
                if resultado in ('WON', 'LOST'):
                    conn2 = _admin_db_connect()
                    cur2 = conn2.cursor()
                    cur2.execute("""
                        UPDATE apostas SET status=%s, resultado=%s, resolvido_em=NOW()
                        WHERE id=%s AND status='pendente'
                    """, (resultado, resultado, ap_id))
                    if resultado == 'WON':
                        cur2.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (retorno, user_id))
                        print(f'[auto-resolver] ✅ Aposta #{ap_id} WON → R${retorno:.2f} creditado a user {user_id}', flush=True)
                    else:
                        print(f'[auto-resolver] ❌ Aposta #{ap_id} LOST — R${valor:.2f} retido', flush=True)
                    conn2.commit(); cur2.close(); conn2.close()
                    resolved_count += 1
    except Exception as e:
        print(f'[auto-resolver] erro: {e}', flush=True)
    return resolved_count

async def _verificar_resultado_espn(sess, jogo_id, jogo_nome, selecao, liga_key):
    """
    Verifica resultado de um jogo no ESPN scoreboard.
    Retorna 'WON', 'LOST' ou None (jogo ainda não finalizado).
    """
    import aiohttp
    try:
        # Determinar categoria ESPN
        cat = 'soccer'
        if liga_key and 'nba' in liga_key: cat = 'basketball'
        elif liga_key and 'nfl' in liga_key: cat = 'football'
        elif liga_key and 'mma' in liga_key: cat = 'mma'
        # Buscar fixture ESPN pelo jogo_nome (fallback: buscar em todas as ligas)
        cfg = _ESPN_FIXTURES.get(liga_key or '')
        if not cfg:
            return None
        espn_slug, league_name, category = cfg
        if cat == 'soccer':
            url = f'https://site.api.espn.com/apis/site/v2/sports/soccer/{espn_slug}/scoreboard'
        else:
            url = f'https://site.api.espn.com/apis/site/v2/sports/{category}/{espn_slug}/scoreboard'
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return None
            data = await r.json(content_type=None)
        eventos = data.get('events', [])
        for ev in eventos:
            comp = ev.get('competitions', [{}])[0]
            status_type = comp.get('status', {}).get('type', {})
            completed = status_type.get('completed', False)
            if not completed: continue  # jogo não encerrado
            # Tentar pelo ID
            ev_id = str(ev.get('id', ''))
            if jogo_id and str(jogo_id) == ev_id:
                return _extrair_resultado_competidor(comp, selecao)
            # Tentar pelo nome do jogo
            nome_ev = ev.get('name', ev.get('shortName', ''))
            if jogo_nome and (jogo_nome.lower() in nome_ev.lower() or nome_ev.lower() in jogo_nome.lower()):
                return _extrair_resultado_competidor(comp, selecao)
    except Exception as e:
        print(f'[verificar-resultado] err: {e}', flush=True)
    return None

def _extrair_resultado_competidor(comp, selecao):
    """
    Dado um competition ESPN finalizado, determina se a seleção apostada ganhou.
    selecao: 'home' | 'away' | 'draw' | nome do time
    """
    competidores = comp.get('competitors', [])
    if len(competidores) < 2: return None
    home = next((c for c in competidores if c.get('homeAway') == 'home'), None)
    away = next((c for c in competidores if c.get('homeAway') == 'away'), None)
    if not home or not away: return None
    try:
        score_h = int(home.get('score', 0) or 0)
        score_a = int(away.get('score', 0) or 0)
    except Exception: return None
    winner = 'home' if score_h > score_a else ('away' if score_a > score_h else 'draw')
    sel = (selecao or '').lower()
    home_name = (home.get('team', {}).get('displayName') or home.get('team', {}).get('name') or '').lower()
    away_name = (away.get('team', {}).get('displayName') or away.get('team', {}).get('name') or '').lower()
    if sel in ('home', '1', 'casa') or (home_name and sel in home_name) or (home_name and home_name in sel):
        return 'WON' if winner == 'home' else 'LOST'
    elif sel in ('away', '2', 'fora') or (away_name and sel in away_name) or (away_name and away_name in sel):
        return 'WON' if winner == 'away' else 'LOST'
    elif sel in ('draw', 'x', 'empate'):
        return 'WON' if winner == 'draw' else 'LOST'
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# 🔢  ITEM 1 — Charts / Gráficos  (GGR por dia, Dep vs Saq, pizza por liga)
# ═══════════════════════════════════════════════════════════════════════════════
async def route_admin_bet_charts(request):
    """GET /api/admin/bet/charts — dados para gráficos Chart.js."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        dias = int(request.rel_url.query.get('dias', 30))
        conn = _admin_db_connect(); cur = conn.cursor()
        # GGR por dia (últimos N dias)
        cur.execute("""
            SELECT DATE(criado_em) AS dia,
                   COALESCE(SUM(valor),0) AS apostado,
                   COALESCE(SUM(CASE WHEN status='LOST' THEN valor ELSE 0 END),0) AS lucro,
                   COALESCE(SUM(CASE WHEN status='WON'  THEN retorno_potencial ELSE 0 END),0) AS pago,
                   COUNT(*) AS qtd
            FROM apostas
            WHERE criado_em >= NOW() - INTERVAL '%s days'
            GROUP BY dia ORDER BY dia
        """ % int(dias))
        ggr_dias = [{'dia': str(r[0]), 'apostado': float(r[1]), 'lucro': float(r[2]),
                     'pago': float(r[3]), 'qtd': r[4]} for r in cur.fetchall()]
        # Depósitos por dia
        try:
            cur.execute("""
                SELECT DATE(criado_em) AS dia, COALESCE(SUM(valor),0), COUNT(*)
                FROM depositos_suit WHERE status='PAID'
                AND criado_em >= NOW() - INTERVAL '%s days'
                GROUP BY dia ORDER BY dia
            """ % int(dias))
            dep_dias = [{'dia': str(r[0]), 'valor': float(r[1]), 'qtd': r[2]} for r in cur.fetchall()]
        except Exception: dep_dias = []
        # Saques por dia
        try:
            cur.execute("""
                SELECT DATE(criado_em) AS dia, COALESCE(SUM(valor),0), COUNT(*)
                FROM saques WHERE status='PAGO'
                AND criado_em >= NOW() - INTERVAL '%s days'
                GROUP BY dia ORDER BY dia
            """ % int(dias))
            saq_dias = [{'dia': str(r[0]), 'valor': float(r[1]), 'qtd': r[2]} for r in cur.fetchall()]
        except Exception: saq_dias = []
        # Pizza por liga
        cur.execute("""
            SELECT COALESCE(league_key,'outros'), COUNT(*), COALESCE(SUM(valor),0)
            FROM apostas GROUP BY league_key ORDER BY SUM(valor) DESC LIMIT 10
        """)
        pizza_liga = [{'liga': r[0], 'qtd': r[1], 'apostado': float(r[2])} for r in cur.fetchall()]
        # Status pie
        cur.execute("SELECT status, COUNT(*), COALESCE(SUM(valor),0) FROM apostas GROUP BY status")
        pizza_status = [{'status': r[0], 'qtd': r[1], 'apostado': float(r[2])} for r in cur.fetchall()]
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'ggr_dias': ggr_dias, 'dep_dias': dep_dias,
                                  'saq_dias': saq_dias, 'pizza_liga': pizza_liga, 'pizza_status': pizza_status})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# 📅  ITEM 7 — P&L por período com filtro de data
# ═══════════════════════════════════════════════════════════════════════════════
async def route_admin_bet_pnl(request):
    """GET /api/admin/bet/pnl?inicio=YYYY-MM-DD&fim=YYYY-MM-DD — P&L detalhado."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        inicio = request.rel_url.query.get('inicio', '')
        fim    = request.rel_url.query.get('fim', '')
        conn = _admin_db_connect(); cur = conn.cursor()
        where = "1=1"
        params = []
        if inicio:
            where += " AND criado_em >= %s"; params.append(inicio + ' 00:00:00')
        if fim:
            where += " AND criado_em <= %s"; params.append(fim + ' 23:59:59')
        cur.execute(f"""
            SELECT status, COUNT(*), COALESCE(SUM(valor),0), COALESCE(SUM(retorno_potencial),0)
            FROM apostas WHERE {where} GROUP BY status
        """, params)
        por_status = {r[0]: {'qtd': r[1], 'apostado': float(r[2]), 'potencial': float(r[3])} for r in cur.fetchall()}
        cur.execute(f"SELECT COALESCE(SUM(valor),0) FROM apostas WHERE {where}", params)
        total_apostado = float(cur.fetchone()[0])
        cur.execute(f"SELECT COALESCE(SUM(retorno_potencial),0) FROM apostas WHERE status='WON' AND {where}", params)
        total_pago = float(cur.fetchone()[0])
        cur.execute(f"SELECT COALESCE(SUM(valor),0) FROM apostas WHERE status='LOST' AND {where}", params)
        ggr = float(cur.fetchone()[0])
        # Por liga no período
        cur.execute(f"""
            SELECT COALESCE(league_key,'outros'), COUNT(*), COALESCE(SUM(valor),0),
                   COALESCE(SUM(CASE WHEN status='LOST' THEN valor ELSE 0 END),0),
                   COALESCE(SUM(CASE WHEN status='WON'  THEN retorno_potencial ELSE 0 END),0)
            FROM apostas WHERE {where} GROUP BY league_key ORDER BY SUM(valor) DESC
        """, params)
        por_liga = [{'liga': r[0], 'qtd': r[1], 'apostado': float(r[2]),
                     'lucro_bruto': float(r[3]), 'pago_won': float(r[4])} for r in cur.fetchall()]
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'periodo': {'inicio': inicio, 'fim': fim},
                                  'total_apostado': total_apostado, 'total_pago_won': total_pago,
                                  'ggr': ggr, 'ggr_liquido': ggr - total_pago,
                                  'por_status': por_status, 'por_liga': por_liga})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# 📁  ITEM 3 — Exportar CSV
# ═══════════════════════════════════════════════════════════════════════════════
async def route_admin_export_apostas_csv(request):
    """GET /api/admin/export/apostas.csv — exporta apostas em CSV."""
    if not _admin_auth(request):
        return web.Response(text='Não autorizado', status=401)
    try:
        import io, csv as _csv
        inicio = request.rel_url.query.get('inicio', '')
        fim    = request.rel_url.query.get('fim', '')
        conn = _admin_db_connect(); cur = conn.cursor()
        where = "1=1"; params = []
        if inicio: where += " AND a.criado_em >= %s"; params.append(inicio + ' 00:00:00')
        if fim:    where += " AND a.criado_em <= %s"; params.append(fim + ' 23:59:59')
        cur.execute(f"""
            SELECT a.id, u.nome, u.cpf, a.jogo_nome, a.selecao, a.odd,
                   a.valor, a.retorno_potencial, a.status, a.league_key, a.criado_em
            FROM apostas a LEFT JOIN usuarios u ON u.id=a.usuario_id
            WHERE {where} ORDER BY a.criado_em DESC
        """, params)
        rows = cur.fetchall(); cur.close(); conn.close()
        out = io.StringIO()
        w = _csv.writer(out)
        w.writerow(['ID','Usuário','CPF','Jogo','Seleção','Odd','Apostado','Retorno','Status','Liga','Data'])
        for r in rows:
            w.writerow([r[0], r[1] or '', r[2] or '', r[3] or '', r[4] or '',
                        float(r[5] or 0), float(r[6] or 0), float(r[7] or 0),
                        r[8] or '', r[9] or '', str(r[10])[:16] if r[10] else ''])
        return web.Response(text=out.getvalue(), content_type='text/csv',
                            headers={'Content-Disposition': 'attachment; filename="apostas.csv"'})
    except Exception as e:
        return web.Response(text=f'Erro: {e}', status=500)

async def route_admin_export_usuarios_csv(request):
    """GET /api/admin/export/usuarios.csv — exporta usuários em CSV."""
    if not _admin_auth(request):
        return web.Response(text='Não autorizado', status=401)
    try:
        import io, csv as _csv
        conn = _admin_db_connect(); cur = conn.cursor()
        cur.execute("""
            SELECT u.id, u.nome, u.cpf, u.saldo,
                   COUNT(a.id) AS apostas, COALESCE(SUM(a.valor),0) AS volume,
                   u.suspendido, u.criado_em
            FROM usuarios u LEFT JOIN apostas a ON a.usuario_id=u.id
            GROUP BY u.id ORDER BY u.criado_em DESC
        """)
        rows = cur.fetchall(); cur.close(); conn.close()
        out = io.StringIO()
        w = _csv.writer(out)
        w.writerow(['ID','Nome','CPF','Saldo','Apostas','Volume','Suspenso','Cadastro'])
        for r in rows:
            w.writerow([r[0], r[1] or '', r[2] or '', float(r[3] or 0),
                        r[4], float(r[5] or 0), 'Sim' if r[6] else 'Não',
                        str(r[7])[:16] if r[7] else ''])
        return web.Response(text=out.getvalue(), content_type='text/csv',
                            headers={'Content-Disposition': 'attachment; filename="usuarios.csv"'})
    except Exception as e:
        return web.Response(text=f'Erro: {e}', status=500)

async def route_admin_export_depositos_csv(request):
    """GET /api/admin/export/depositos.csv — exporta depósitos em CSV."""
    if not _admin_auth(request):
        return web.Response(text='Não autorizado', status=401)
    try:
        import io, csv as _csv
        conn = _admin_db_connect(); cur = conn.cursor()
        try:
            cur.execute("""
                SELECT d.id, u.nome, u.cpf, d.valor, d.status, d.criado_em
                FROM depositos_suit d LEFT JOIN usuarios u ON u.cpf=d.cpf
                ORDER BY d.criado_em DESC LIMIT 5000
            """)
            rows = cur.fetchall()
        except Exception:
            rows = []
        cur.close(); conn.close()
        out = io.StringIO()
        w = _csv.writer(out)
        w.writerow(['ID','Nome','CPF','Valor','Status','Data'])
        for r in rows:
            w.writerow([r[0], r[1] or '', r[2] or '', float(r[3] or 0), r[4] or '', str(r[5])[:16] if r[5] else ''])
        return web.Response(text=out.getvalue(), content_type='text/csv',
                            headers={'Content-Disposition': 'attachment; filename="depositos.csv"'})
    except Exception as e:
        return web.Response(text=f'Erro: {e}', status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# 🎯  ITEM 2 — Limites máx de aposta por liga e por usuário
# ═══════════════════════════════════════════════════════════════════════════════
def _ensure_limites_table(cur, conn):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bet_limites (
            liga_key VARCHAR(100) PRIMARY KEY,
            limite_aposta REAL DEFAULT 500.0,
            limite_usuario_dia REAL DEFAULT 1000.0,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    # Inserir defaults se vazio
    cur.execute("SELECT COUNT(*) FROM bet_limites")
    if cur.fetchone()[0] == 0:
        for k in ['soccer_brazil_campeonato','soccer_brazil_serie_b','soccer_conmebol_copa_libertadores',
                  'soccer_conmebol_sudamericana','soccer_uefa_champs_league','soccer_epl',
                  'soccer_spain_la_liga','soccer_italy_serie_a','soccer_germany_bundesliga',
                  'soccer_france_ligue_one','basketball_nba','americanfootball_nfl','mma_mixed_martial_arts']:
            cur.execute("""
                INSERT INTO bet_limites (liga_key, limite_aposta, limite_usuario_dia)
                VALUES (%s, 500.0, 1000.0) ON CONFLICT DO NOTHING
            """, (k,))
    conn.commit()

async def route_admin_limites_get(request):
    """GET /api/admin/bet/limites — lista limites por liga."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        _ensure_limites_table(cur, conn)
        cur.execute("SELECT liga_key, limite_aposta, limite_usuario_dia, updated_at FROM bet_limites ORDER BY liga_key")
        rows = cur.fetchall()
        cur.close(); conn.close()
        ligas = []
        for r in rows:
            info = _ADMIN_LIGAS_MAP.get(r[0], {})
            ligas.append({'liga_key': r[0], 'nome': info.get('nome', r[0]),
                          'limite_aposta': float(r[1]), 'limite_usuario_dia': float(r[2]),
                          'updated_at': r[3].strftime('%d/%m %H:%M') if r[3] else ''})
        return web.json_response({'ok': True, 'ligas': ligas})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_limites_set(request):
    """POST /api/admin/bet/limites — define limites por liga."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        liga_key = body.get('liga_key', '')
        limite_aposta = float(body.get('limite_aposta', 500))
        limite_dia    = float(body.get('limite_usuario_dia', 1000))
        if not liga_key:
            return web.json_response({'ok': False, 'error': 'liga_key obrigatório'}, status=400)
        conn = _admin_db_connect(); cur = conn.cursor()
        _ensure_limites_table(cur, conn)
        cur.execute("""
            INSERT INTO bet_limites (liga_key, limite_aposta, limite_usuario_dia, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (liga_key) DO UPDATE
            SET limite_aposta=%s, limite_usuario_dia=%s, updated_at=NOW()
        """, (liga_key, limite_aposta, limite_dia, limite_aposta, limite_dia))
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'ok': True, 'msg': f'Limites de {liga_key} salvos'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# 🎁  ITEM 4 — Sistema de Bônus (boas-vindas, free bet, cashback, indicação)
# ═══════════════════════════════════════════════════════════════════════════════
def _ensure_bonus_tables(cur, conn):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bet_bonus_config (
            id SERIAL PRIMARY KEY,
            tipo VARCHAR(50) NOT NULL,
            nome VARCHAR(200) NOT NULL,
            descricao TEXT,
            valor REAL DEFAULT 0,
            percentual REAL DEFAULT 0,
            deposito_minimo REAL DEFAULT 10,
            rollover REAL DEFAULT 1,
            validade_dias INTEGER DEFAULT 30,
            ativo BOOLEAN DEFAULT TRUE,
            criado_em TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bet_bonus_usuarios (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER,
            bonus_id INTEGER,
            tipo VARCHAR(50),
            valor_bonus REAL DEFAULT 0,
            valor_usado REAL DEFAULT 0,
            status VARCHAR(30) DEFAULT 'ativo',
            criado_em TIMESTAMP DEFAULT NOW(),
            expira_em TIMESTAMP
        )
    """)
    conn.commit()

async def route_admin_bonus_list(request):
    """GET /api/admin/bonus — lista bônus configurados."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        _ensure_bonus_tables(cur, conn)
        cur.execute("SELECT id, tipo, nome, descricao, valor, percentual, deposito_minimo, rollover, validade_dias, ativo, criado_em FROM bet_bonus_config ORDER BY criado_em DESC")
        bonus = [{'id': r[0], 'tipo': r[1], 'nome': r[2], 'descricao': r[3] or '',
                  'valor': float(r[4] or 0), 'percentual': float(r[5] or 0),
                  'deposito_minimo': float(r[6] or 0), 'rollover': float(r[7] or 1),
                  'validade_dias': r[8], 'ativo': bool(r[9]),
                  'criado_em': r[10].strftime('%d/%m/%Y') if r[10] else ''} for r in cur.fetchall()]
        # Estatísticas de uso
        cur.execute("SELECT COUNT(*), COALESCE(SUM(valor_bonus),0) FROM bet_bonus_usuarios WHERE status='ativo'")
        stats = cur.fetchone()
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'bonus': bonus,
                                  'stats': {'ativos': stats[0], 'valor_total': float(stats[1])}})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_bonus_criar(request):
    """POST /api/admin/bonus/criar — cria novo bônus."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        tipo        = body.get('tipo', 'free_bet')  # boas_vindas|free_bet|cashback|indicacao
        nome        = body.get('nome', '')
        descricao   = body.get('descricao', '')
        valor       = float(body.get('valor', 0))
        percentual  = float(body.get('percentual', 0))
        dep_min     = float(body.get('deposito_minimo', 10))
        rollover    = float(body.get('rollover', 1))
        validade    = int(body.get('validade_dias', 30))
        if not nome:
            return web.json_response({'ok': False, 'error': 'nome obrigatório'}, status=400)
        conn = _admin_db_connect(); cur = conn.cursor()
        _ensure_bonus_tables(cur, conn)
        cur.execute("""
            INSERT INTO bet_bonus_config (tipo, nome, descricao, valor, percentual, deposito_minimo, rollover, validade_dias)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """, (tipo, nome, descricao, valor, percentual, dep_min, rollover, validade))
        new_id = cur.fetchone()[0]
        conn.commit(); cur.close(); conn.close()
        print(f'[admin/bonus] Novo bônus #{new_id}: {tipo} — {nome}', flush=True)
        return web.json_response({'ok': True, 'id': new_id, 'msg': f'Bônus "{nome}" criado com sucesso'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_bonus_cancelar(request):
    """POST /api/admin/bonus/cancelar — desativa bônus."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        bonus_id = int(body.get('id', 0))
        conn = _admin_db_connect(); cur = conn.cursor()
        cur.execute("UPDATE bet_bonus_config SET ativo=false WHERE id=%s", (bonus_id,))
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'ok': True, 'msg': f'Bônus #{bonus_id} desativado'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# ⚠️  ITEM 5 — Alertas de Risco
# ═══════════════════════════════════════════════════════════════════════════════
async def route_admin_alertas(request):
    """GET /api/admin/alertas — detecta apostadores suspeitos."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        alertas = []
        # 1. Win rate acima de 70%
        cur.execute("""
            SELECT u.id, u.nome, u.cpf,
                   COUNT(a.id) AS total,
                   SUM(CASE WHEN a.status='WON' THEN 1 ELSE 0 END) AS won,
                   COALESCE(SUM(a.valor),0) AS volume
            FROM apostas a JOIN usuarios u ON u.id=a.usuario_id
            WHERE a.status IN ('WON','LOST')
            GROUP BY u.id, u.nome, u.cpf
            HAVING COUNT(a.id) >= 3
            ORDER BY (SUM(CASE WHEN a.status='WON' THEN 1.0 ELSE 0 END)/COUNT(a.id)) DESC
            LIMIT 20
        """)
        for r in cur.fetchall():
            total, won = r[3], r[4] or 0
            wr = (won / total * 100) if total > 0 else 0
            if wr >= 70:
                alertas.append({'tipo': 'win_rate_alto', 'nivel': 'alto' if wr >= 85 else 'medio',
                                 'usuario_id': r[0], 'nome': r[1] or '?', 'cpf': r[2] or '',
                                 'detalhe': f'Win rate {wr:.0f}% ({won}/{total} apostas)',
                                 'volume': float(r[5])})
        # 2. Saque logo após depósito sem apostar (lavagem)
        try:
            cur.execute("""
                SELECT u.id, u.nome, u.cpf,
                       COALESCE(SUM(d.valor),0) AS dep_total,
                       COUNT(DISTINCT a.id) AS apostas
                FROM usuarios u
                JOIN depositos_suit d ON d.cpf=u.cpf AND d.status='PAID'
                LEFT JOIN apostas a ON a.usuario_id=u.id
                GROUP BY u.id, u.nome, u.cpf
                HAVING COALESCE(SUM(d.valor),0) > 50 AND COUNT(DISTINCT a.id) = 0
                LIMIT 10
            """)
            for r in cur.fetchall():
                alertas.append({'tipo': 'saque_sem_aposta', 'nivel': 'alto',
                                 'usuario_id': r[0], 'nome': r[1] or '?', 'cpf': r[2] or '',
                                 'detalhe': f'Depositou R${float(r[3]):.2f} mas não apostou nada',
                                 'volume': float(r[3])})
        except Exception: pass
        # 3. Usuários com aposta única muito alta (>80% do saldo)
        cur.execute("""
            SELECT u.id, u.nome, u.cpf, a.valor, u.saldo
            FROM apostas a JOIN usuarios u ON u.id=a.usuario_id
            WHERE a.valor > 100 AND u.saldo > 0 AND a.status='pendente'
            AND a.valor > u.saldo * 0.8
            ORDER BY a.valor DESC LIMIT 10
        """)
        for r in cur.fetchall():
            alertas.append({'tipo': 'aposta_concentrada', 'nivel': 'medio',
                             'usuario_id': r[0], 'nome': r[1] or '?', 'cpf': r[2] or '',
                             'detalhe': f'Apostou R${float(r[3]):.2f} (>80% do saldo R${float(r[4]):.2f})',
                             'volume': float(r[3])})
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'alertas': alertas,
                                  'total': len(alertas),
                                  'altos': sum(1 for a in alertas if a['nivel'] == 'alto'),
                                  'medios': sum(1 for a in alertas if a['nivel'] == 'medio')})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_alertas_ignorar(request):
    """POST /api/admin/alertas/ignorar — registra que alerta foi revisado."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    return web.json_response({'ok': True, 'msg': 'Alerta marcado como revisado'})

# ═══════════════════════════════════════════════════════════════════════════════
# 🛡️  ITEM 6 — Jogo Responsável (autoexclusão, limite dep, limite perda)
# ═══════════════════════════════════════════════════════════════════════════════
def _ensure_jogo_resp_table(cur, conn):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bet_jogo_responsavel (
            usuario_id INTEGER PRIMARY KEY,
            autoexcluido BOOLEAN DEFAULT FALSE,
            autoexclusao_ate TIMESTAMP,
            limite_deposito_mes REAL DEFAULT 0,
            limite_perda_mes REAL DEFAULT 0,
            limite_aposta_unica REAL DEFAULT 0,
            pausa_ate TIMESTAMP,
            motivo TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()

async def route_admin_jogo_resp_list(request):
    """GET /api/admin/jogo-responsavel — lista usuários com restrições ativas."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        _ensure_jogo_resp_table(cur, conn)
        cur.execute("""
            SELECT jr.usuario_id, u.nome, u.cpf, jr.autoexcluido, jr.autoexclusao_ate,
                   jr.limite_deposito_mes, jr.limite_perda_mes, jr.limite_aposta_unica,
                   jr.pausa_ate, jr.motivo, jr.updated_at
            FROM bet_jogo_responsavel jr
            JOIN usuarios u ON u.id=jr.usuario_id
            ORDER BY jr.updated_at DESC
        """)
        rows = cur.fetchall()
        # Estatísticas globais
        cur.execute("SELECT COUNT(*) FROM bet_jogo_responsavel WHERE autoexcluido=true")
        total_excluidos = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM bet_jogo_responsavel WHERE pausa_ate > NOW()")
        total_pausados = cur.fetchone()[0]
        cur.close(); conn.close()
        usuarios = [{'usuario_id': r[0], 'nome': r[1] or '?', 'cpf': r[2] or '',
                     'autoexcluido': bool(r[3]),
                     'autoexclusao_ate': str(r[4])[:16] if r[4] else None,
                     'limite_deposito_mes': float(r[5] or 0), 'limite_perda_mes': float(r[6] or 0),
                     'limite_aposta_unica': float(r[7] or 0),
                     'pausa_ate': str(r[8])[:16] if r[8] else None,
                     'motivo': r[9] or '', 'updated_at': str(r[10])[:16] if r[10] else ''} for r in rows]
        return web.json_response({'ok': True, 'usuarios': usuarios,
                                  'total_excluidos': total_excluidos, 'total_pausados': total_pausados})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_admin_jogo_resp_set(request):
    """POST /api/admin/jogo-responsavel — define restrições para usuário."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        usuario_id = int(body.get('usuario_id', 0))
        if not usuario_id:
            return web.json_response({'ok': False, 'error': 'usuario_id obrigatório'}, status=400)
        conn = _admin_db_connect(); cur = conn.cursor()
        _ensure_jogo_resp_table(cur, conn)
        import datetime as _dt
        autoexcluido = bool(body.get('autoexcluido', False))
        dias_excl = int(body.get('dias_exclusao', 30))
        excl_ate = (_dt.datetime.now() + _dt.timedelta(days=dias_excl)) if autoexcluido else None
        lim_dep  = float(body.get('limite_deposito_mes', 0))
        lim_perd = float(body.get('limite_perda_mes', 0))
        lim_apos = float(body.get('limite_aposta_unica', 0))
        dias_pausa = int(body.get('dias_pausa', 0))
        pausa_ate = (_dt.datetime.now() + _dt.timedelta(days=dias_pausa)) if dias_pausa > 0 else None
        motivo = body.get('motivo', '')
        cur.execute("""
            INSERT INTO bet_jogo_responsavel
                (usuario_id, autoexcluido, autoexclusao_ate, limite_deposito_mes,
                 limite_perda_mes, limite_aposta_unica, pausa_ate, motivo, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (usuario_id) DO UPDATE SET
                autoexcluido=%s, autoexclusao_ate=%s, limite_deposito_mes=%s,
                limite_perda_mes=%s, limite_aposta_unica=%s, pausa_ate=%s, motivo=%s, updated_at=NOW()
        """, (usuario_id, autoexcluido, excl_ate, lim_dep, lim_perd, lim_apos, pausa_ate, motivo,
              autoexcluido, excl_ate, lim_dep, lim_perd, lim_apos, pausa_ate, motivo))
        # Se autoexcluído, também suspender a conta
        if autoexcluido:
            try:
                cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS suspendido BOOLEAN DEFAULT false")
                cur.execute("UPDATE usuarios SET suspendido=true WHERE id=%s", (usuario_id,))
            except Exception: pass
        conn.commit(); cur.close(); conn.close()
        print(f'[jogo-responsavel] User {usuario_id} → autoexcl={autoexcluido}, lim_dep={lim_dep}, lim_perd={lim_perd}', flush=True)
        return web.json_response({'ok': True, 'msg': 'Configurações de jogo responsável salvas'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# 🤝  ITEM 9 — Afiliados / Indicação
# ═══════════════════════════════════════════════════════════════════════════════
async def route_admin_afiliados_list(request):
    """GET /api/admin/afiliados — lista afiliados com métricas."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        # Garantir coluna referido_por
        try:
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS referido_por INTEGER DEFAULT NULL")
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS codigo_indicacao VARCHAR(20) DEFAULT NULL")
            conn.commit()
        except Exception: conn.rollback()
        cur.execute("""
            SELECT u.id, u.nome, u.cpf, u.codigo_indicacao,
                   COUNT(r.id) AS indicados,
                   COALESCE(SUM(a_r.valor),0) AS volume_indicados,
                   u.criado_em
            FROM usuarios u
            LEFT JOIN usuarios r ON r.referido_por=u.id
            LEFT JOIN apostas a_r ON a_r.usuario_id=r.id
            WHERE u.codigo_indicacao IS NOT NULL
            GROUP BY u.id ORDER BY COUNT(r.id) DESC LIMIT 50
        """)
        afiliados = [{'id': r[0], 'nome': r[1] or '?', 'cpf': r[2] or '',
                      'codigo': r[3] or '', 'indicados': r[4],
                      'volume_indicados': float(r[5] or 0),
                      'criado_em': str(r[6])[:10] if r[6] else ''} for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM usuarios WHERE referido_por IS NOT NULL")
        total_indicados = cur.fetchone()[0]
        cur.close(); conn.close()
        return web.json_response({'ok': True, 'afiliados': afiliados,
                                  'total_indicados': total_indicados,
                                  'total_afiliados': len(afiliados)})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════════════
# 🔔  ITEM 8 — Notificações Telegram ao Admin
# ═══════════════════════════════════════════════════════════════════════════════
_NOTIF_CONFIG = {'bot_token': '', 'chat_id': '', 'limite_saque': 200.0,
                 'limite_aposta': 500.0, 'alertas_risco': True}

async def _carregar_notif_config():
    """Carrega config de notificações do DB."""
    try:
        conn = _admin_db_connect(); cur = conn.cursor()
        cur.execute("SELECT valor FROM configuracoes WHERE chave='notif_admin_config'")
        row = cur.fetchone(); cur.close(); conn.close()
        if row:
            import json as _j
            _NOTIF_CONFIG.update(_j.loads(row[0]))
    except Exception: pass

async def _notif_telegram_admin(msg: str):
    """Envia mensagem de alerta ao admin via Telegram."""
    import aiohttp as _aio
    token = _NOTIF_CONFIG.get('bot_token', '')
    chat  = _NOTIF_CONFIG.get('chat_id', '')
    if not token or not chat:
        return
    try:
        async with _aio.ClientSession() as sess:
            await sess.post(f'https://api.telegram.org/bot{token}/sendMessage',
                            json={'chat_id': chat, 'text': msg, 'parse_mode': 'HTML'},
                            timeout=_aio.ClientTimeout(total=5))
    except Exception: pass

async def route_admin_notif_config_get(request):
    """GET /api/admin/notif-config — retorna config de notificações."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    await _carregar_notif_config()
    cfg = dict(_NOTIF_CONFIG)
    if cfg.get('bot_token'):
        cfg['bot_token'] = cfg['bot_token'][:8] + '***'  # mascarar token
    return web.json_response({'ok': True, 'config': cfg})

async def route_admin_notif_config(request):
    """POST /api/admin/notif-config — salva config de notificações."""
    if not _admin_auth(request):
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        body = await request.json()
        import json as _j
        # Atualizar config em memória
        if 'bot_token' in body and body['bot_token'] and '***' not in body['bot_token']:
            _NOTIF_CONFIG['bot_token'] = body['bot_token']
        if 'chat_id' in body:
            _NOTIF_CONFIG['chat_id'] = str(body['chat_id'])
        if 'limite_saque'  in body: _NOTIF_CONFIG['limite_saque']  = float(body['limite_saque'])
        if 'limite_aposta' in body: _NOTIF_CONFIG['limite_aposta'] = float(body['limite_aposta'])
        if 'alertas_risco' in body: _NOTIF_CONFIG['alertas_risco'] = bool(body['alertas_risco'])
        # Salvar no DB
        conn = _admin_db_connect(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO configuracoes (chave, valor) VALUES ('notif_admin_config', %s)
            ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor
        """, (_j.dumps(_NOTIF_CONFIG),))
        conn.commit(); cur.close(); conn.close()
        # Testar envio se solicitado
        if body.get('testar'):
            await _notif_telegram_admin('🔔 <b>PaynexBet Admin</b>\nNotificações configuradas com sucesso! ✅')
            return web.json_response({'ok': True, 'msg': 'Configuração salva e mensagem de teste enviada!'})
        return web.json_response({'ok': True, 'msg': 'Configuração de notificações salva!'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)}, status=500)

async def route_bet_jogos(request):
    """GET /api/bet/jogos — retorna jogos com odds reais (odds-api) ou ESPN como fallback.
    Suporta ?sport=<key> para filtrar por campeonato específico.
    """
    import time as _time, aiohttp
    agora = _time.time()
    sport_filter = request.rel_url.query.get('sport', '').strip()

    # Cache válido
    cache_valido = agora - _odds_cache['ts'] < _ODDS_CACHE_TTL and _odds_cache['data']
    if cache_valido:
        jogos = _odds_cache['data']
        if sport_filter and sport_filter not in ('upcoming', 'destaque', 'all'):
            jogos = [j for j in jogos if j.get('sport') == sport_filter]
        return web.json_response({'success': True, 'jogos': jogos, 'from_cache': True, 'total': len(jogos)})

    all_jogos = []

    # ── 1. Tentar odds-api (se tiver chave) ────────────────────────────────────
    key = _get_odds_key()
    if key:
        sports_odds = [
            'soccer_brazil_campeonato', 'soccer_conmebol_copa_libertadores',
            'soccer_uefa_champs_league', 'soccer_epl', 'soccer_spain_la_liga',
            'basketball_nba', 'americanfootball_nfl',
            'tennis_atp_aus_open_singles', 'mma_mixed_martial_arts',
        ]
        async with aiohttp.ClientSession() as sess:
            for sport in sports_odds:
                try:
                    url = f'https://api.the-odds-api.com/v4/sports/{sport}/odds'
                    params = {'apiKey': key, 'regions': 'eu', 'markets': 'h2h',
                              'oddsFormat': 'decimal', 'dateFormat': 'iso'}
                    async with sess.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            eventos = await r.json()
                            for ev in (eventos or [])[:6]:
                                h_odds, d_odds, a_odds = [], [], []
                                for bk in ev.get('bookmakers', [])[:5]:
                                    for mkt in bk.get('markets', []):
                                        if mkt.get('key') == 'h2h':
                                            for oc in mkt.get('outcomes', []):
                                                n = oc.get('name', '')
                                                v = float(oc.get('price', 0))
                                                if n == ev.get('home_team'):   h_odds.append(v)
                                                elif n == ev.get('away_team'): a_odds.append(v)
                                                else:                          d_odds.append(v)
                                def avg(lst): return round(sum(lst)/len(lst), 2) if lst else None
                                odds_bruta = {'home': avg(h_odds), 'draw': avg(d_odds), 'away': avg(a_odds)}
                                odds_com_margem, margem_pct = _aplicar_margem_odds(odds_bruta, sport)
                                all_jogos.append({
                                    'id': ev.get('id'), 'sport': sport,
                                    'league': ev.get('sport_title', sport),
                                    'home_team': ev.get('home_team', ''),
                                    'away_team': ev.get('away_team', ''),
                                    'commence_time': ev.get('commence_time', ''),
                                    'home_logo': '', 'away_logo': '',
                                    'status': 'Agendado', 'is_live': False, 'is_finished': False,
                                    'odds': odds_com_margem,
                                    'odds_bruta': odds_bruta,
                                    'margem_pct': margem_pct,
                                    'source': 'odds-api',
                                })
                except Exception as e_odds:
                    print(f'[bet/jogos/odds] sport={sport}: {e_odds}', flush=True)

    # ── 2. Fallback ESPN (gratuito — sempre completa os jogos) ──────────────────
    existing_pairs = set((j['home_team'] + '|' + j['away_team']) for j in all_jogos)
    sports_to_fetch = []
    if sport_filter and sport_filter not in ('upcoming', 'destaque', 'all'):
        cfg = _ESPN_FIXTURES.get(sport_filter)
        if cfg:
            sports_to_fetch = [(sport_filter, cfg)]
    else:
        seen_slugs = set()
        for k, v in _ESPN_FIXTURES.items():
            if v[0] not in seen_slugs:
                sports_to_fetch.append((k, v))
                seen_slugs.add(v[0])

    async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
        for sp_key, cfg in sports_to_fetch:
            espn_slug   = cfg[0]
            league_name = cfg[1]
            category    = cfg[2] if len(cfg) > 2 else 'soccer'
            cached_fix  = _espn_fix_cache.get(espn_slug)
            if cached_fix and agora - cached_fix['ts'] < _ESPN_FIX_TTL:
                items = cached_fix['data']
            else:
                items = await _espn_fetch_scoreboard(sess, espn_slug, league_name, sp_key, category)
                _espn_fix_cache[espn_slug] = {'ts': agora, 'data': items}
            for item in items:
                pair = item['home_team'] + '|' + item['away_team']
                if pair not in existing_pairs:
                    all_jogos.append(item)
                    existing_pairs.add(pair)

    # Salvar cache completo
    _odds_cache['ts']   = agora
    _odds_cache['data'] = all_jogos

    jogos_resp = all_jogos
    if sport_filter and sport_filter not in ('upcoming', 'destaque', 'all'):
        jogos_resp = [j for j in all_jogos if j.get('sport') == sport_filter]

    return web.json_response({'success': True, 'jogos': jogos_resp, 'total': len(jogos_resp)})


async def route_bet_live(request):
    """GET /api/bet/live — jogos ao vivo via Apify FlashScore (cache DB) + fallback ESPN."""
    import time as _time
    agora = _time.time()

    # 1) Tentar cache em memória (< 60s)
    if agora - _apify_live_cache['ts'] < 60 and _apify_live_cache['jogos']:
        return web.json_response({
            'success': True,
            'jogos': _apify_live_cache['jogos'],
            'total': len(_apify_live_cache['jogos']),
            'source': 'apify_mem',
            'updated_at': _apify_live_cache['ts']
        })

    # 2) Tentar cache no banco PostgreSQL
    try:
        import psycopg2 as _pg2, json as _json
        _db_url = DATABASE_URL or _BET_DB_URL_FALLBACK
        _conn = _pg2.connect(_db_url, connect_timeout=5)
        _cur  = _conn.cursor()
        _cur.execute("SELECT jogos, total, updated_at FROM live_cache WHERE id=1")
        row = _cur.fetchone()
        _cur.close(); _conn.close()
        if row:
            jogos_db  = row[0] if isinstance(row[0], list) else _json.loads(row[0] or '[]')
            total_db  = row[1] or 0
            updated   = row[2]
            import datetime as _dt
            age = ((_dt.datetime.now(_dt.timezone.utc) - updated.replace(tzinfo=_dt.timezone.utc)).total_seconds()
                   if updated else 9999)
            # Cache do DB válido por 5 minutos
            if age < 300 and jogos_db:
                _apify_live_cache['ts']    = agora
                _apify_live_cache['jogos'] = jogos_db
                return web.json_response({
                    'success': True,
                    'jogos': jogos_db,
                    'total': total_db,
                    'source': 'apify_db',
                    'age_seconds': int(age)
                })
    except Exception as e_db:
        print(f'[live] erro DB: {e_db}', flush=True)

    # 3) Fallback: ESPN ao vivo (síncrono, rápido)
    try:
        import aiohttp
        jogos_live = []
        seen_slugs = set()
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
            for sp_key, cfg in _ESPN_FIXTURES.items():
                espn_slug   = cfg[0]
                league_name = cfg[1]
                category    = cfg[2] if len(cfg) > 2 else 'soccer'
                if espn_slug in seen_slugs:
                    continue
                seen_slugs.add(espn_slug)
                items = await _espn_fetch_scoreboard(sess, espn_slug, league_name, sp_key, category)
                for item in items:
                    if item.get('is_live'):
                        seed = sum(ord(c) for c in (item['home_team'] + item['away_team']))
                        r1   = ((seed * 17 + 3) % 100) / 100
                        r2   = ((seed * 31 + 7) % 100) / 100
                        item['odds'] = {
                            'home': round(1.4 + r1 * 2.2, 2),
                            'draw': round(2.8 + (seed % 20) / 10, 2),
                            'away': round(1.4 + r2 * 2.2, 2),
                        }
                        item['odds_simulated'] = True
                        item['source'] = 'espn'
                        jogos_live.append(item)
        _live_cache['ts']   = agora
        _live_cache['data'] = jogos_live
        return web.json_response({'success': True, 'jogos': jogos_live, 'total': len(jogos_live), 'source': 'espn'})
    except Exception as e:
        print(f'[bet/live] erro ESPN fallback: {e}', flush=True)
        return web.json_response({'success': True, 'jogos': [], 'total': 0, 'source': 'none', 'error': str(e)})

        # Indexar fixtures por id
        fix_by_id = {}
        for f in fix_raw.get('response', []):
            fid = f.get('fixture', {}).get('id')
            if fid:
                fix_by_id[fid] = f

        jogos = []
        for item in live_raw.get('response', []):
            fid = item.get('fixture', {}).get('id')
            fix = fix_by_id.get(fid, {})
            teams = fix.get('teams', {})
            goals = fix.get('goals', {})
            status = fix.get('fixture', {}).get('status', {})
            league = fix.get('league', {})

            home_name = teams.get('home', {}).get('name', f"Time {item.get('teams',{}).get('home',{}).get('id','?')}")
            away_name = teams.get('away', {}).get('name', f"Time {item.get('teams',{}).get('away',{}).get('id','?')}")
            home_logo = teams.get('home', {}).get('logo', '')
            away_logo = teams.get('away', {}).get('logo', '')
            home_goals = goals.get('home')
            away_goals = goals.get('away')
            elapsed    = status.get('elapsed', 0) or item.get('fixture', {}).get('status', {}).get('elapsed', 0)
            league_name = league.get('name', 'Ao Vivo')
            league_logo = league.get('logo', '')

            # Extrair odds do mercado "Fulltime Result" (id 59) ou 1X2
            home_odd = away_odd = draw_odd = None
            for mkt in item.get('odds', []):
                mkt_id = mkt.get('id')
                mkt_name = mkt.get('name', '').lower()
                if mkt_id in (1, 59) or 'fulltime' in mkt_name or 'match winner' in mkt_name or '1x2' in mkt_name:
                    for v in mkt.get('values', []):
                        val = v.get('value', '')
                        odd_f = float(v.get('odd', 0) or 0)
                        suspended = v.get('suspended', False)
                        if suspended or odd_f <= 1.0:
                            continue
                        if val in ('Home', '1'):
                            home_odd = odd_f
                        elif val in ('Draw', 'X'):
                            draw_odd = odd_f
                        elif val in ('Away', '2'):
                            away_odd = odd_f
                    if home_odd:
                        break

            if not home_odd:
                continue  # pular jogos sem mercado 1X2 disponível

            jogos.append({
                'id':          fid,
                'fixture_id':  fid,
                'home_team':   home_name,
                'away_team':   away_name,
                'home_logo':   home_logo,
                'away_logo':   away_logo,
                'home_goals':  home_goals,
                'away_goals':  away_goals,
                'elapsed':     elapsed,
                'league':      league_name,
                'league_logo': league_logo,
                'odds': {
                    'home': round(home_odd, 2) if home_odd else None,
                    'draw': round(draw_odd, 2) if draw_odd else None,
                    'away': round(away_odd, 2) if away_odd else None,
                }
            })

        _live_cache['ts']   = agora
        _live_cache['data'] = jogos
        return web.json_response({'success': True, 'jogos': jogos, 'total': len(jogos)})
    except Exception as e:
        print(f'[bet/live] erro: {e}', flush=True)
        return web.json_response({'success': False, 'jogos': [], 'error': str(e)})


async def route_bet_predictions(request):
    """GET /api/bet/predictions?fixture=ID — previsão IA para um fixture"""
    import time as _time, aiohttp
    fixture_id = request.rel_url.query.get('fixture', '')
    if not fixture_id:
        return web.json_response({'error': 'fixture obrigatório'}, status=400)
    agora = _time.time()
    cached = _pred_cache.get(fixture_id)
    if cached and agora - cached['ts'] < _PRED_CACHE_TTL:
        return web.json_response({'success': True, 'prediction': cached['data']})
    try:
        async with aiohttp.ClientSession(headers=_APISPORTS_HDR) as sess:
            async with sess.get(f'{_APIFOOTBALL_BASE}/predictions',
                                params={'fixture': fixture_id},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
                d = await r.json() if r.status == 200 else {}
        resp = d.get('response', [])
        if not resp:
            return web.json_response({'success': False, 'error': 'sem dados'})
        pred = resp[0].get('predictions', {})
        teams = resp[0].get('teams', {})
        result = {
            'winner':    pred.get('winner', {}).get('name'),
            'advice':    pred.get('advice', ''),
            'percent':   pred.get('percent', {}),
            'under_over': pred.get('under_over'),
            'home_form': teams.get('home', {}).get('last_5', {}).get('form', ''),
            'away_form': teams.get('away', {}).get('last_5', {}).get('form', ''),
            'home_name': teams.get('home', {}).get('name', ''),
            'away_name': teams.get('away', {}).get('name', ''),
        }
        _pred_cache[fixture_id] = {'ts': agora, 'data': result}
        return web.json_response({'success': True, 'prediction': result})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


# ── ESPN API (gratuita, sem key, dados COMPLETOS) ─────────────────────────────
_ESPN_BASE = 'https://site.api.espn.com/apis/site/v2/sports/soccer'
_ESPN_STANDINGS_BASE = 'https://site.api.espn.com/apis/v2/sports/soccer'

# Mapeamento: chave frontend → (espn_slug, nome_liga)
_ESPN_LEAGUES = {
    '71':  ('bra.1',               'Brasileirão Série A'),
    '75':  ('bra.2',               'Brasileirão Série B'),
    '13':  ('CONMEBOL.LIBERTADORES','Copa Libertadores'),
    '11':  ('CONMEBOL.SUDAMERICANA', 'Copa Sul-Americana'),
    '2':   ('UEFA.CHAMPIONS',      'UEFA Champions League'),
    '39':  ('eng.1',               'Premier League'),
    '140': ('esp.1',               'La Liga'),
    '135': ('ita.1',               'Serie A (Itália)'),
    '78':  ('ger.1',               'Bundesliga'),
    '61':  ('fra.1',               'Ligue 1'),
}

# Sport fixtures map: sport_key → (espn_slug, nome_liga, espn_category)
# espn_category: 'soccer'(default), 'basketball', 'football', 'mma', 'tennis'
_ESPN_FIXTURES = {
    'soccer_brazil_campeonato':          ('bra.1',                'Brasileirão Série A',  'soccer'),
    'soccer_brazil_serie_b':             ('bra.2',                'Brasileirão Série B',  'soccer'),
    'soccer_conmebol_copa_libertadores': ('CONMEBOL.LIBERTADORES', 'Copa Libertadores',   'soccer'),
    'soccer_conmebol_sudamericana':      ('CONMEBOL.SUDAMERICANA', 'Copa Sul-Americana',  'soccer'),
    'soccer_conmebol_sulamericana':      ('CONMEBOL.SUDAMERICANA', 'Copa Sul-Americana',  'soccer'),
    'soccer_uefa_champs_league':         ('UEFA.CHAMPIONS',        'Champions League',    'soccer'),
    'soccer_epl':                        ('eng.1',                 'Premier League',      'soccer'),
    'soccer_spain_la_liga':              ('esp.1',                 'La Liga',             'soccer'),
    'soccer_italy_serie_a':              ('ita.1',                 'Serie A',             'soccer'),
    'soccer_germany_bundesliga':         ('ger.1',                 'Bundesliga',          'soccer'),
    'soccer_france_ligue_one':           ('fra.1',                 'Ligue 1',             'soccer'),
    'basketball_nba':                    ('nba',                   'NBA',                 'basketball'),
    'americanfootball_nfl':              ('nfl',                   'NFL',                 'football'),
    'mma_mixed_martial_arts':            ('ufc',                   'UFC / MMA',           'mma'),
    'tennis_atp_aus_open_singles':       ('atp',                   'Tênis ATP',           'tennis'),
}

_espn_table_cache = {}   # key=league_key → {'ts':..., 'data':[]}
_espn_fix_cache   = {}   # key=espn_slug → {'ts':..., 'data':[]}
_ESPN_TTL         = 1800  # 30 min
_ESPN_FIX_TTL     = 60    # 1 min para jogos (detectar ao vivo mais rápido)


def _gerar_odds_espn(home: str, away: str, sport_key: str) -> dict:
    """
    Gera odds sintéticas (brutas, sem margem) quando ESPN não fornece mercado.
    Usa hash do nome dos times para determinismo (mesmo jogo = mesmas odds).
    """
    import hashlib
    seed = int(hashlib.md5(f'{home}{away}{sport_key}'.encode()).hexdigest(), 16)
    rng = seed % 1000
    if 'nba' in sport_key or 'basketball' in sport_key:
        # NBA: sem empate, odds próximas
        h = round(1.60 + (rng % 80) / 100, 2)
        a = round(2.60 - (rng % 60) / 100, 2)
        return {'home': h, 'draw': None, 'away': a}
    elif 'nfl' in sport_key or 'football' in sport_key:
        h = round(1.70 + (rng % 90) / 100, 2)
        a = round(2.40 - (rng % 50) / 100, 2)
        return {'home': h, 'draw': None, 'away': a}
    elif 'mma' in sport_key:
        h = round(1.40 + (rng % 120) / 100, 2)
        a = round(2.80 - (rng % 80) / 100, 2)
        return {'home': h, 'draw': None, 'away': a}
    else:
        # Futebol: 3 mercados
        fav = rng % 3  # 0=home, 1=draw, 2=away
        if fav == 0:
            h = round(1.60 + (rng % 60) / 100, 2)
            d = round(3.20 + (rng % 40) / 100, 2)
            a = round(4.50 + (rng % 80) / 100, 2)
        elif fav == 1:
            h = round(2.40 + (rng % 50) / 100, 2)
            d = round(2.80 + (rng % 30) / 100, 2)
            a = round(2.90 + (rng % 50) / 100, 2)
        else:
            h = round(4.20 + (rng % 80) / 100, 2)
            d = round(3.50 + (rng % 40) / 100, 2)
            a = round(1.65 + (rng % 50) / 100, 2)
        return {'home': h, 'draw': d, 'away': a}

async def _espn_fetch_scoreboard(sess, espn_slug, league_name, sport_key, category='soccer'):
    """Helper: busca scoreboard ESPN e retorna lista de jogos normalizados."""
    import json as _json, aiohttp as _aio
    if category == 'soccer':
        url = f'https://site.api.espn.com/apis/site/v2/sports/soccer/{espn_slug}/scoreboard'
    else:
        url = f'https://site.api.espn.com/apis/site/v2/sports/{category}/{espn_slug}/scoreboard'
    try:
        async with sess.get(url, timeout=_aio.ClientTimeout(total=10)) as r:
            raw = await r.read()
            if not raw or r.status != 200:
                return []
            d = _json.loads(raw)
        events = d.get('events', []) or []
        result = []
        for ev in events:
            comps = ev.get('competitions', [{}])[0]
            teams = comps.get('competitors', [])
            h = next((t for t in teams if t.get('homeAway') == 'home'), teams[0] if teams else {})
            a = next((t for t in teams if t.get('homeAway') == 'away'), teams[-1] if teams else {})
            h_team = h.get('team', {})
            a_team = a.get('team', {})
            status_obj  = ev.get('status', {})
            status_type = status_obj.get('type', {})
            state       = status_type.get('state', 'pre')
            status_desc = status_type.get('description', 'Agendado')
            status_code = status_type.get('name', 'STATUS_SCHEDULED')
            is_live     = state == 'in'
            is_finished = state == 'post'
            h_score = h.get('score', '')
            a_score = a.get('score', '')
            score = f'{h_score} - {a_score}' if (is_live or is_finished) and h_score != '' else ''
            h_logo = h_team.get('logo', '')
            if not h_logo and h_team.get('logos'):
                h_logo = h_team['logos'][0].get('href', '')
            a_logo = a_team.get('logo', '')
            if not a_logo and a_team.get('logos'):
                a_logo = a_team['logos'][0].get('href', '')
            clock   = status_obj.get('displayClock', '') if is_live else ''
            elapsed = status_obj.get('period', 0)       if is_live else 0
            venue_obj = comps.get('venue', {})
            venue = venue_obj.get('fullName', '')
            commence_time = ev.get('date', '')
            h_name = h_team.get('displayName', h_team.get('name', '?'))
            a_name = a_team.get('displayName', a_team.get('name', '?'))
            if h_name == '?' or a_name == '?':
                continue
            # Aplicar margem nas odds geradas
            odds_bruta = _gerar_odds_espn(h_name, a_name, sport_key)
            odds_com_margem, margem_pct = _aplicar_margem_odds(odds_bruta, sport_key)
            result.append({
                'id':            ev.get('id', ''),
                'sport':         sport_key,
                'league':        league_name,
                'home_team':     h_name,
                'away_team':     a_name,
                'home_logo':     h_logo,
                'away_logo':     a_logo,
                'home_goals':    int(h_score) if str(h_score).isdigit() else None,
                'away_goals':    int(a_score) if str(a_score).isdigit() else None,
                'score':         score,
                'commence_time': commence_time,
                'venue':         venue,
                'status':        status_desc,
                'status_code':   status_code,
                'is_live':       is_live,
                'is_finished':   is_finished,
                'clock':         clock,
                'elapsed':       elapsed,
                'odds':          odds_com_margem,
                'odds_bruta':    odds_bruta,
                'margem_pct':    margem_pct,
                'source':        'espn',
            })
        return result
    except Exception as _ex:
        print(f'[espn_scoreboard] {league_name} err: {_ex}', flush=True)
        return []


async def route_tsdb_tabela(request):
    """GET /api/bet/tabela?league=71 — tabela de classificação via ESPN API (gratuita, 20 times)"""
    import time as _time, aiohttp, json as _json
    agora = _time.time()
    league_param = request.rel_url.query.get('league', '71')

    cfg = _ESPN_LEAGUES.get(league_param)
    if not cfg:
        return web.json_response({'success': False, 'standings': [], 'error': f'Liga {league_param} não mapeada'})

    espn_slug, league_name = cfg
    cache_key = league_param

    cached = _espn_table_cache.get(cache_key)
    if cached and agora - cached['ts'] < _ESPN_TTL:
        resp = dict(cached['data']) if isinstance(cached['data'], dict) else {'success': True, 'standings': cached['data'], 'league': league_name}
        resp['from_cache'] = True
        return web.json_response(resp)

    url = f'{_ESPN_STANDINGS_BASE}/{espn_slug}/standings'
    try:
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                raw = await r.read()
                if not raw:
                    return web.json_response({'success': False, 'standings': [], 'error': 'Sem dados'})
                d = _json.loads(raw)

        children = d.get('children', [])
        multi_group = len(children) > 1  # Copa tem múltiplos grupos

        grupos = []
        all_rows = []

        for grp in children:
            grp_name = grp.get('name', '')
            entries = grp.get('standings', {}).get('entries', [])
            grp_rows = []
            rank = 1
            for entry in entries:
                team = entry.get('team', {})
                stats_list = entry.get('stats', [])
                stats = {s['name']: s.get('value', 0) for s in stats_list}

                logo = team.get('logo', '')
                if not logo and team.get('logos'):
                    logo = team['logos'][0].get('href', '')

                gf = int(stats.get('pointsFor', 0) or 0)
                ga = int(stats.get('pointsAgainst', 0) or 0)
                row = {
                    'rank':    rank,
                    'name':    team.get('displayName', team.get('name', '')),
                    'logo':    logo,
                    'points':  int(stats.get('points', 0) or 0),
                    'played':  int(stats.get('gamesPlayed', 0) or 0),
                    'won':     int(stats.get('wins', 0) or 0),
                    'draw':    int(stats.get('ties', 0) or 0),
                    'lost':    int(stats.get('losses', 0) or 0),
                    'gf':      gf,
                    'ga':      ga,
                    'gd':      gf - ga,
                    'form':    '',
                    'description': '',
                }
                grp_rows.append(row)
                all_rows.append(row)
                rank += 1

            if grp_rows:
                grupos.append({'name': grp_name, 'rows': grp_rows})

        result_data = {
            'success': True,
            'standings': all_rows,
            'league': league_name,
            'grupos': grupos if multi_group else [],
            'has_groups': multi_group,
        }
        _espn_table_cache[cache_key] = {'ts': agora, 'data': result_data}
        return web.json_response(result_data)
    except Exception as e:
        print(f'[espn/tabela] err league={league_param} slug={espn_slug}: {e}', flush=True)
        return web.json_response({'success': False, 'standings': [], 'error': str(e)})


async def route_tsdb_fixtures(request):
    """GET /api/bet/fixtures?sport=soccer_brazil_campeonato — jogos via ESPN API (gratuita).
    Usa helper _espn_fetch_scoreboard para suportar futebol, NBA, NFL, MMA e Tênis.
    """
    import time as _time, aiohttp
    agora = _time.time()
    sport = request.rel_url.query.get('sport', '').strip()

    fixtures_result = []
    sports_to_fetch = []

    if sport and sport in _ESPN_FIXTURES:
        sports_to_fetch = [(sport, _ESPN_FIXTURES[sport])]
    elif not sport or sport in ('upcoming', 'all', 'destaque'):
        # Retornar jogos de todas as ligas principais (evitar duplicar conmebol_sulamericana)
        seen_slugs = set()
        for k, v in _ESPN_FIXTURES.items():
            if v[0] not in seen_slugs:
                sports_to_fetch.append((k, v))
                seen_slugs.add(v[0])
    else:
        return web.json_response({'success': True, 'jogos': [], 'total': 0})

    async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
        for sp_key, cfg in sports_to_fetch:
            espn_slug  = cfg[0]
            league_name = cfg[1]
            category   = cfg[2] if len(cfg) > 2 else 'soccer'
            cached = _espn_fix_cache.get(espn_slug)
            if cached and agora - cached['ts'] < _ESPN_FIX_TTL:
                fixtures_result.extend(cached['data'])
                continue
            items = await _espn_fetch_scoreboard(sess, espn_slug, league_name, sp_key, category)
            _espn_fix_cache[espn_slug] = {'ts': agora, 'data': items}
            fixtures_result.extend(items)

    return web.json_response({'success': True, 'jogos': fixtures_result, 'total': len(fixtures_result)})


async def route_bet_standings(request):
    """GET /api/bet/standings?league=71&season=2024 — tabela de classificação
    Usa TheSportsDB (gratuito) como fonte primária (api-sports.io suspenso).
    """
    import time as _time, aiohttp
    agora = _time.time()
    league = request.rel_url.query.get('league', '71')
    season = request.rel_url.query.get('season', '')
    cache_key = f'{league}_{season}'
    cached = _standings_cache['data'].get(cache_key)
    if cached and agora - _standings_cache['ts'] < _STANDINGS_TTL:
        return web.json_response({'success': True, 'standings': cached, 'from_cache': True})

    # Mapeamento: ID api-football → (TheSportsDB ID, season format, nome)
    _TSDB_MAP = {
        '71':  ('4351', '2025',       'Brasileirão Série A'),
        '75':  ('4352', '2025',       'Brasileirão Série B'),
        '13':  ('4529', '2025',       'Copa Libertadores'),
        '11':  ('4528', '2024-2025',  'Copa Sul-Americana'),
        '2':   ('4480', '2024-2025',  'UEFA Champions League'),
        '39':  ('4328', '2024-2025',  'Premier League'),
        '140': ('4335', '2024-2025',  'La Liga'),
        '135': ('4332', '2024-2025',  'Serie A Italia'),
        '78':  ('4331', '2024-2025',  'Bundesliga'),
        '61':  ('4334', '2024-2025',  'Ligue 1'),
        '4351': ('4351', '2025',      'Brasileirão Série A'),
        '4352': ('4352', '2025',      'Brasileirão Série B'),
        '4480': ('4480', '2024-2025', 'UEFA Champions League'),
        '4328': ('4328', '2024-2025', 'Premier League'),
        '4335': ('4335', '2024-2025', 'La Liga'),
        '4332': ('4332', '2024-2025', 'Serie A Italia'),
        '4331': ('4331', '2024-2025', 'Bundesliga'),
        '4334': ('4334', '2024-2025', 'Ligue 1'),
    }

    tsdb_id, tsdb_season, league_name = _TSDB_MAP.get(league, (league, season or '2025', 'Liga'))
    if season:
        tsdb_season = season

    _TSDB_BASE = 'https://www.thesportsdb.com/api/v1/json/3'
    url = f'{_TSDB_BASE}/lookuptable.php?l={tsdb_id}&s={tsdb_season}'

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url,
                                headers={'User-Agent': 'PaynexBet/1.0'},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 429:
                    old = _standings_cache['data'].get(cache_key)
                    if old:
                        return web.json_response({'success': True, 'standings': old, 'from_cache': True})
                    return web.json_response({'success': False, 'standings': [],
                                             'error': 'Rate limit. Tente novamente em instantes.'})
                if r.status != 200:
                    return web.json_response({'success': False, 'standings': [],
                                             'error': f'API retornou status {r.status}'})
                content = await r.read()
                if not content or content.strip() in (b'', b'null', b'[]'):
                    return web.json_response({'success': False, 'standings': [],
                                             'error': 'Tabela não disponível para esta temporada.'})
                d = await r.json(content_type=None)

        table_raw = d.get('table', []) or []
        if not table_raw:
            return web.json_response({'success': False, 'standings': [],
                                     'error': 'Tabela sem dados para esta temporada.'})

        data = []
        for t in table_raw:
            rank   = int(t.get('intRank', 0) or 0)
            pts    = int(t.get('intPoints', 0) or 0)
            played = int(t.get('intPlayed', 0) or 0)
            won    = int(t.get('intWin', 0) or 0)
            draw   = int(t.get('intDraw', 0) or 0)
            lost   = int(t.get('intLoss', 0) or 0)
            gf     = int(t.get('intGoalsFor', 0) or 0)
            ga     = int(t.get('intGoalsAgainst', 0) or 0)
            gd     = gf - ga
            logo   = (t.get('strBadge', '') or '').replace('/tiny', '/medium')
            data.append({
                'rank':        rank,
                'name':        t.get('strTeam', ''),
                'logo':        logo,
                'points':      pts,
                'played':      played,
                'won':         won,
                'draw':        draw,
                'lost':        lost,
                'gf':          gf,
                'ga':          ga,
                'gd':          gd,
                'form':        t.get('strForm', '') or '',
                'description': '',
            })

        _standings_cache['data'][cache_key] = data
        _standings_cache['ts'] = agora
        return web.json_response({'success': True, 'standings': data, 'league': league_name})

    except Exception as e:
        old = _standings_cache['data'].get(cache_key)
        if old:
            return web.json_response({'success': True, 'standings': old, 'from_cache': True})
        return web.json_response({'success': False, 'standings': [], 'error': str(e)})


async def route_tabela_page(request):
    """GET /tabela — página de classificação (tabela de times)"""
    return web.Response(text=_page_tabela_html(), content_type='text/html', charset='utf-8')


async def route_webhook_suitpay(request):
    """POST /webhook/suitpay — recebe notificação de pagamento da SuitPay"""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'ok': False, 'error': 'JSON inválido'}, status=400)

    # Validar hash SHA-256 conforme doc SuitPay:
    # 1) Concatenar todos os valores (exceto 'hash') na ORDEM RECEBIDA no JSON
    # 2) Concatenar o ClientSecret (cs) ao final
    # 3) SHA-256 do resultado
    _, cs = _get_suit_keys()
    hash_recv = data.get('hash', '')
    if cs and hash_recv:
        concat = ''.join(str(v) for k, v in data.items() if k != 'hash')
        concat += cs
        hash_calc = _hashlib.sha256(concat.encode()).hexdigest()
        if hash_recv != hash_calc:
            # Tentar também com ordem fixa (fallback p/ retrocompatibilidade)
            campos_fixos = ['idTransaction','typeTransaction','statusTransaction',
                            'value','payerName','payerTaxId','paymentDate',
                            'paymentCode','requestNumber']
            concat2 = ''.join(str(data.get(c, '')) for c in campos_fixos if c in data)
            concat2 += cs
            hash_calc2 = _hashlib.sha256(concat2.encode()).hexdigest()
            if hash_recv != hash_calc2:
                print(f'[webhook/suitpay] ⚠️ hash inválido! recv={hash_recv} calc={hash_calc}', flush=True)
                return web.json_response({'ok': False, 'error': 'hash inválido'}, status=403)

    status     = data.get('statusTransaction', '')
    valor      = float(data.get('value', 0))
    req_number = data.get('requestNumber', '')
    id_tx      = data.get('idTransaction', '')

    print(f'[webhook/suitpay] status={status} valor={valor} req={req_number}', flush=True)

    if status == 'PAID_OUT' and DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur  = conn.cursor()
            # Buscar usuário pelo requestNumber (formato: userid_uuid)
            cur.execute("""
                UPDATE depositos_suit
                SET status='PAID', pago_em=NOW(), id_transaction=%s
                WHERE request_number=%s AND status='PENDING'
                RETURNING usuario_id, valor
            """, (id_tx, req_number))
            row = cur.fetchone()
            if row:
                usuario_id, valor_db = row
                # Creditar saldo
                cur.execute("""
                    UPDATE usuarios SET saldo = saldo + %s WHERE id = %s
                """, (valor_db, usuario_id))
                conn.commit()
                print(f'[webhook/suitpay] ✅ Saldo +{valor_db} para usuario {usuario_id}', flush=True)
            else:
                # Tentar como deposito de apostas esportivas genérico
                cur.execute("""
                    UPDATE depositos_suit SET status='PAID', pago_em=NOW()
                    WHERE request_number=%s
                """, (req_number,))
                conn.commit()
                print(f'[webhook/suitpay] ℹ️ req_number {req_number} marcado como PAID (sem usuario vinculado)', flush=True)
            cur.close()
            conn.close()
        except Exception as e_db:
            print(f'[webhook/suitpay] erro DB: {e_db}', flush=True)

    elif status == 'CHARGEBACK':
        print(f'[webhook/suitpay] ⚠️ CHARGEBACK id_tx={id_tx}', flush=True)

    return web.json_response({'ok': True})


async def route_bet_deposito(request):
    """POST /api/bet/deposito — gera QR Code PIX via SuitPay"""
    import aiohttp
    try:
        body = await request.json()
    except Exception:
        return web.json_response({'success': False, 'error': 'JSON inválido'}, status=400)

    valor      = float(body.get('valor', 0))
    usuario_id = body.get('usuario_id', '')
    nome       = body.get('nome', 'Cliente')
    cpf_raw    = re.sub(r'\D', '', body.get('cpf', ''))  # apenas dígitos

    # Se CPF não fornecido no body, buscar no banco pelo usuario_id
    if not cpf_raw and usuario_id:
        try:
            import psycopg2 as _pg2
            _db_url = DATABASE_URL or _BET_DB_URL_FALLBACK
            if _db_url:
                _conn = _pg2.connect(_db_url)
                _cur  = _conn.cursor()
                _cur.execute("SELECT cpf, nome FROM usuarios WHERE id=%s", (usuario_id,))
                _row = _cur.fetchone()
                if _row:
                    cpf_raw = re.sub(r'\D', '', _row[0] or '')
                    nome = nome if nome != 'Cliente' else (_row[1] or nome)
                _cur.close(); _conn.close()
        except Exception as e_cpf:
            print(f'[bet/deposito] erro ao buscar CPF: {e_cpf}', flush=True)

    # Formatar CPF no padrão que SuitPay aceita: NNN.NNN.NNN-NN
    def _fmt_cpf(c):
        c = re.sub(r'\D', '', c)
        if len(c) == 11:
            return f'{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:11]}'
        if len(c) == 14:  # CNPJ: NN.NNN.NNN/NNNN-NN
            return f'{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:14]}'
        return c
    cpf = _fmt_cpf(cpf_raw) if cpf_raw else '000.000.000-00'

    if valor < 5:
        return web.json_response({'success': False, 'error': 'Valor mínimo R$5,00'})
    suit_ci, suit_cs = _get_suit_keys()
    if not suit_ci or not suit_cs:
        return web.json_response({'success': False, 'error': 'SuitPay não configurado — adicione via /api/bet/config'})

    import uuid as _uuid, datetime as _dt
    req_number = f'{usuario_id}_{_uuid.uuid4().hex[:12]}'
    due_date   = (_dt.datetime.utcnow() + _dt.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')

    payload = {
        'requestNumber': req_number,
        'dueDate':       due_date,
        'amount':        valor,
        'callbackUrl':   _SUIT_WEBHOOK_URL,
        'client': {
            'name':     nome,
            'document': cpf or '00000000000',  # SuitPay usa 'document', não 'taxNumber'
        }
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                f'{_SUIT_HOST}/api/v1/gateway/request-qrcode',  # endpoint correto conforme SDK oficial
                json=payload,
                headers={'ci': suit_ci, 'cs': suit_cs, 'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                resp = await r.json()
    except Exception as e_suit:
        return web.json_response({'success': False, 'error': f'Erro SuitPay: {e_suit}'})

    # SuitPay retorna response='OK' e idTransaction quando sucesso (não 'success': true)
    suit_ok = resp.get('response') == 'OK' or resp.get('success') or bool(resp.get('idTransaction'))
    if not suit_ok:
        return web.json_response({'success': False, 'error': resp.get('message', resp.get('response', 'Erro desconhecido SuitPay'))})

    # Salvar deposito pendente no banco
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO depositos_suit (request_number, usuario_id, valor, status, criado_em)
                VALUES (%s, %s, %s, 'PENDING', NOW())
                ON CONFLICT (request_number) DO NOTHING
            """, (req_number, usuario_id, valor))
            conn.commit(); cur.close(); conn.close()
        except Exception as e_db:
            print(f'[bet/deposito] erro DB: {e_db}', flush=True)

    return web.json_response({
        'success':    True,
        'qrCode':     resp.get('paymentCode', ''),
        'qrImage':    resp.get('paymentCodeBase64', resp.get('qrCodeImage', '')),  # SuitPay usa paymentCodeBase64
        'requestNumber': req_number,
        'valor':      valor,
        'expira':     due_date,
    })


# ═══════════════════════════════════════════════════════════════════════════
# ███  BET — Fases 3-7: Apostar / Sacar / Auth / Páginas  ███
# ═══════════════════════════════════════════════════════════════════════════

# URL de fallback hardcoded para garantir funcionamento
_BET_DB_URL_FALLBACK = 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'

async def _bet_db():
    """Retorna conexão psycopg2 ou None — tenta DATABASE_URL env var e fallback hardcoded"""
    import psycopg2 as _pg2
    # Tenta 1: DATABASE_URL do ambiente (Railway injeta automaticamente)
    for db_url in [DATABASE_URL, _BET_DB_URL_FALLBACK]:
        if not db_url:
            continue
        try:
            conn = _pg2.connect(db_url, connect_timeout=10)
            conn.autocommit = False
            return conn
        except Exception as e:
            print(f'[bet_db] ERRO ({db_url[:30]}...): {type(e).__name__}: {e}', flush=True)
    print('[bet_db] ERRO: todas as tentativas de conexão falharam', flush=True)
    return None

# ── FASE 3: Auth / Login por CPF ──────────────────────────────────────────

async def route_bet_login(request):
    """POST /api/bet/login — login/cadastro por CPF
    - Se vier nome: cadastro (cria ou retorna existente, atualizando nome se vazio)
    - Se só CPF: login (retorna usuário existente ou erro se não encontrado)
    """
    try:
        body = await request.json()
    except Exception:
        return web.json_response({'success': False, 'error': 'JSON inválido'}, status=400)

    nome = (body.get('nome') or '').strip()
    cpf  = ''.join(filter(str.isdigit, body.get('cpf') or ''))

    if not cpf or len(cpf) < 11:
        return web.json_response({'success': False, 'error': 'CPF inválido'})

    conn = await _bet_db()
    if not conn:
        return web.json_response({'success': False, 'error': 'DB indisponível'})

    try:
        cur = conn.cursor()

        if nome:
            # ── CADASTRO: Upsert — cria ou retorna existente, atualizando nome se vazio
            cur.execute("""
                INSERT INTO usuarios (cpf, nome, saldo)
                VALUES (%s, %s, 0)
                ON CONFLICT (cpf) DO UPDATE SET nome = CASE
                    WHEN usuarios.nome IS NULL OR usuarios.nome='' THEN EXCLUDED.nome
                    ELSE usuarios.nome END
                RETURNING id, nome, cpf, saldo
            """, (cpf, nome))
        else:
            # ── LOGIN: só CPF — busca usuário existente
            cur.execute("SELECT id, nome, cpf, saldo FROM usuarios WHERE cpf=%s", (cpf,))

        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'CPF não encontrado. Crie sua conta primeiro.'})

        conn.commit()
        cur.close(); conn.close()
        return web.json_response({
            'success': True,
            'usuario': {'id': row[0], 'nome': row[1], 'cpf': row[2], 'saldo': float(row[3] or 0)}
        })
    except Exception as e:
        try: conn.close()
        except: pass
        return web.json_response({'success': False, 'error': str(e)})


async def route_bet_saldo(request):
    """GET /api/bet/saldo/{usuario_id} — consulta saldo e apostas recentes"""
    usuario_id = request.match_info.get('usuario_id', '')
    conn = await _bet_db()
    if not conn:
        return web.json_response({'success': False, 'error': 'DB indisponível'})
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, nome, cpf, saldo FROM usuarios WHERE id=%s", (usuario_id,))
        u = cur.fetchone()
        if not u:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Usuário não encontrado'})
        # Últimas 10 apostas
        cur.execute("""
            SELECT jogo_nome, selecao, odd, valor, retorno_potencial, status, resultado, criado_em
            FROM apostas WHERE usuario_id=%s ORDER BY criado_em DESC LIMIT 10
        """, (usuario_id,))
        apostas = [{'jogo': r[0], 'selecao': r[1], 'odd': float(r[2] or 0),
                    'valor': float(r[3] or 0), 'retorno': float(r[4] or 0),
                    'status': r[5], 'resultado': r[6],
                    'data': r[7].strftime('%d/%m/%Y %H:%M') if r[7] else ''} for r in cur.fetchall()]
        # Últimos depósitos
        cur.execute("""
            SELECT valor, status, pago_em, criado_em FROM depositos_suit
            WHERE usuario_id=%s ORDER BY criado_em DESC LIMIT 5
        """, (usuario_id,))
        deps = [{'valor': float(r[0] or 0), 'status': r[1],
                 'pago_em': r[2].strftime('%d/%m/%Y %H:%M') if r[2] else '',
                 'criado': r[3].strftime('%d/%m/%Y %H:%M') if r[3] else ''} for r in cur.fetchall()]
        cur.close(); conn.close()
        return web.json_response({
            'success': True,
            'usuario': {'id': u[0], 'nome': u[1], 'cpf': u[2], 'saldo': float(u[3] or 0)},
            'apostas': apostas,
            'depositos': deps,
        })
    except Exception as e:
        conn.close()
        return web.json_response({'success': False, 'error': str(e)})


# ── FASE 5: Apostar ────────────────────────────────────────────────────────

async def route_bet_apostar(request):
    """POST /api/bet/apostar — registra aposta debitando saldo"""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({'success': False, 'error': 'JSON inválido'}, status=400)

    usuario_id   = body.get('usuario_id')
    jogo_id      = body.get('jogo_id', '')
    jogo_nome    = body.get('jogo_nome', '')
    selecao      = body.get('selecao', '')
    odd          = float(body.get('odd', 0))
    valor        = float(body.get('valor', 0))
    league_key   = body.get('league_key', '') or body.get('sport', '')
    odd_bruta    = float(body.get('odd_bruta', 0) or odd)
    margem_apl   = float(body.get('margem_pct', 0) or _get_margem_liga(league_key))

    if not usuario_id:
        return web.json_response({'success': False, 'error': 'Faça login primeiro'})
    if valor < 2:
        return web.json_response({'success': False, 'error': 'Valor mínimo R$2,00'})
    if odd < 1.01:
        return web.json_response({'success': False, 'error': 'Odd inválida'})

    retorno = round(valor * odd, 2)

    conn = await _bet_db()
    if not conn:
        return web.json_response({'success': False, 'error': 'DB indisponível'})
    try:
        cur = conn.cursor()
        # Verificar saldo
        cur.execute("SELECT saldo FROM usuarios WHERE id=%s FOR UPDATE", (usuario_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Usuário não encontrado'})
        saldo_atual = float(row[0] or 0)
        if saldo_atual < valor:
            cur.close(); conn.close()
            return web.json_response({
                'success': False,
                'error': f'Saldo insuficiente. Saldo atual: R$ {saldo_atual:.2f}'
            })
        # Debitar saldo
        cur.execute("UPDATE usuarios SET saldo = saldo - %s WHERE id=%s", (valor, usuario_id))
        # Registrar aposta com league_key, odd_bruta e margem
        cur.execute("""
            INSERT INTO apostas (usuario_id, jogo_id, jogo_nome, selecao, odd, valor,
                                 retorno_potencial, status, league_key, odd_bruta, margem_aplicada)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'pendente',%s,%s,%s) RETURNING id
        """, (usuario_id, jogo_id, jogo_nome, selecao, odd, valor, retorno,
              league_key or None, odd_bruta or None, margem_apl))
        aposta_id = cur.fetchone()[0]
        conn.commit()
        # Saldo atualizado
        cur.execute("SELECT saldo FROM usuarios WHERE id=%s", (usuario_id,))
        novo_saldo = float(cur.fetchone()[0] or 0)
        cur.close(); conn.close()
        return web.json_response({
            'success': True,
            'aposta_id': aposta_id,
            'saldo': novo_saldo,
            'retorno_potencial': retorno,
            'margem_pct': margem_apl,
            'msg': f'✅ Aposta de R$ {valor:.2f} registrada! Retorno potencial: R$ {retorno:.2f}'
        })
    except Exception as e:
        conn.close()
        return web.json_response({'success': False, 'error': str(e)})


# ── FASE 6: Sacar ──────────────────────────────────────────────────────────

async def route_bet_sacar(request):
    """POST /api/bet/sacar — saque via SuitPay PIX"""
    import aiohttp
    try:
        body = await request.json()
    except Exception:
        return web.json_response({'success': False, 'error': 'JSON inválido'}, status=400)

    usuario_id  = body.get('usuario_id')
    valor       = float(body.get('valor', 0))
    chave_pix   = (body.get('chave_pix') or '').strip()
    tipo_chave  = (body.get('tipo_chave') or 'cpf').strip()

    if not usuario_id:
        return web.json_response({'success': False, 'error': 'Faça login primeiro'})
    if valor < 20:
        return web.json_response({'success': False, 'error': 'Valor mínimo de saque: R$20,00'})
    if not chave_pix:
        return web.json_response({'success': False, 'error': 'Informe sua chave PIX'})
    suit_ci, suit_cs = _get_suit_keys()
    if not suit_ci or not suit_cs:
        return web.json_response({'success': False, 'error': 'Gateway de pagamento não configurado — adicione via /api/bet/config'})

    conn = await _bet_db()
    if not conn:
        return web.json_response({'success': False, 'error': 'DB indisponível'})
    try:
        cur = conn.cursor()
        cur.execute("SELECT saldo, nome FROM usuarios WHERE id=%s FOR UPDATE", (usuario_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': 'Usuário não encontrado'})
        saldo_atual = float(row[0] or 0)
        nome_usuario = row[1] or ''
        if saldo_atual < valor:
            cur.close(); conn.close()
            return web.json_response({'success': False, 'error': f'Saldo insuficiente (R$ {saldo_atual:.2f})'})
        # Reservar saldo (debitar)
        cur.execute("UPDATE usuarios SET saldo = saldo - %s WHERE id=%s", (valor, usuario_id))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        conn.close()
        return web.json_response({'success': False, 'error': str(e)})

    # Chamar SuitPay Cash-out
    import uuid as _uuid
    payload_suit = {
        'requestNumber': f'saque_{usuario_id}_{_uuid.uuid4().hex[:8]}',
        'value': valor,
        'key': chave_pix,
        'typeKey': tipo_chave,
        'callbackUrl': _SUIT_WEBHOOK_URL,
        'client': {'name': nome_usuario}
    }
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                f'{_SUIT_HOST}/api/v1/gateway/pix-payment',  # endpoint correto (prefixo api/)
                json=payload_suit,
                headers={'ci': suit_ci, 'cs': suit_cs, 'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                resp = await r.json()
    except Exception as e_suit:
        # Devolver saldo em caso de erro
        conn2 = await _bet_db()
        if conn2:
            try:
                c2 = conn2.cursor()
                c2.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (valor, usuario_id))
                conn2.commit(); c2.close(); conn2.close()
            except Exception: pass
        return web.json_response({'success': False, 'error': f'Erro ao processar saque: {e_suit}'})

    if not resp.get('success'):
        # Devolver saldo
        conn2 = await _bet_db()
        if conn2:
            try:
                c2 = conn2.cursor()
                c2.execute("UPDATE usuarios SET saldo = saldo + %s WHERE id=%s", (valor, usuario_id))
                conn2.commit(); c2.close(); conn2.close()
            except Exception: pass
        return web.json_response({'success': False, 'error': resp.get('message', 'Erro no saque')})

    return web.json_response({
        'success': True,
        'msg': f'✅ Saque de R$ {valor:.2f} enviado para {chave_pix}!'
    })


# ── FASE 7: Páginas HTML /apostas e /conta ─────────────────────────────────

def _page_tabela_html():
    return r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="theme-color" content="#0a0a0f">
<title>PaynexBet — Tabela de Classificação</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--gold:#FFD700;--blue:#3b82f6;--green:#22c55e;--red:#ef4444;--bg:#0a0a0f;--card:#111118;--card2:#13131e}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:#fff;min-height:100vh}
.topbar{background:#0d0d16;border-bottom:1px solid #ffffff0c;padding:0 20px;height:56px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.topbar-logo{font-size:20px;font-weight:900;background:linear-gradient(135deg,#FFD700,#FFA500);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;text-decoration:none}
.topbar-nav{display:flex;gap:8px;align-items:center}
.nav-link{color:#aaa;text-decoration:none;font-size:13px;font-weight:600;padding:5px 10px;border-radius:8px;transition:all .15s}
.nav-link:hover,.nav-link.active{background:#1e3a8a;color:#fff}
.page{max-width:1100px;margin:0 auto;padding:20px 16px 60px}
/* Liga tabs */
.liga-tabs{display:flex;gap:8px;overflow-x:auto;margin-bottom:20px;padding-bottom:4px;scrollbar-width:none}
.liga-tabs::-webkit-scrollbar{display:none}
.liga-tab{flex-shrink:0;display:flex;align-items:center;gap:6px;background:var(--card);border:1px solid #ffffff10;border-radius:999px;padding:7px 14px;font-size:12px;font-weight:700;color:#888;cursor:pointer;transition:all .15s;white-space:nowrap}
.liga-tab img{width:18px;height:18px;object-fit:contain}
.liga-tab.active{background:#1e3a8a;border-color:#3b82f6;color:#fff}
/* Tabela */
.tabela-wrap{background:var(--card);border:1px solid #ffffff0c;border-radius:14px;overflow:hidden}
.tabela-header{display:grid;grid-template-columns:36px 1fr 36px 36px 36px 36px 36px 40px 60px;gap:0;padding:10px 12px;border-bottom:1px solid #ffffff0c;font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.5px;font-weight:700}
@media(max-width:600px){
  .tabela-header{grid-template-columns:28px 1fr 28px 28px 28px 28px 28px 36px 50px;padding:8px 8px}
  .col-gd,.col-jg{display:none}
}
.tabela-row{display:grid;grid-template-columns:36px 1fr 36px 36px 36px 36px 36px 40px 60px;gap:0;padding:10px 12px;border-bottom:1px solid #ffffff06;align-items:center;transition:background .15s}
.tabela-row:hover{background:#ffffff04}
.tabela-row:last-child{border-bottom:none}
@media(max-width:600px){
  .tabela-row{grid-template-columns:28px 1fr 28px 28px 28px 28px 28px 36px 50px;padding:8px 8px}
}
.col-rank{font-size:12px;font-weight:700;color:#555;text-align:center}
.col-rank.pos1{color:var(--gold)}
.col-rank.pos2{color:#aaa}
.col-rank.pos3{color:#cd7f32}
.col-team{display:flex;align-items:center;gap:8px;min-width:0}
.team-logo{width:24px;height:24px;object-fit:contain;flex-shrink:0}
.team-name{font-size:13px;font-weight:700;color:#fff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.team-ini{width:24px;height:24px;border-radius:50%;background:linear-gradient(135deg,#1e3a8a,#3b82f6);color:#fff;font-size:9px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.col-num{font-size:12px;font-weight:700;color:#ccc;text-align:center}
.col-pts{font-size:14px;font-weight:900;color:var(--blue);text-align:center}
.col-form{display:flex;gap:2px;justify-content:flex-end}
.form-c{width:14px;height:14px;border-radius:3px;font-size:8px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.form-c.W{background:#22c55e22;color:#22c55e;border:1px solid #22c55e44}
.form-c.D{background:#55555522;color:#888;border:1px solid #55555544}
.form-c.L{background:#ef444422;color:#ef4444;border:1px solid #ef444444}
/* Zone colors */
.tabela-row.zone-cl{border-left:3px solid #3b82f6}
.tabela-row.zone-el{border-left:3px solid #22c55e}
.tabela-row.zone-rl{border-left:3px solid #ef4444}
/* Legend */
.legend{display:flex;gap:12px;margin-top:12px;flex-wrap:wrap}
.legend-item{display:flex;align-items:center;gap:4px;font-size:11px;color:#666}
.legend-dot{width:10px;height:10px;border-radius:2px}
.secao-titulo{font-size:12px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;margin-top:4px}
.loading-msg{text-align:center;padding:40px 20px;color:#555}
.spinner{width:28px;height:28px;border:3px solid #ffffff0c;border-top-color:var(--blue);border-radius:50%;animation:spin .8s linear infinite;margin:0 auto 12px}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<nav class="topbar">
  <a class="topbar-logo" href="/">PaynexBet</a>
  <div class="topbar-nav">
    <a class="nav-link" href="/apostas">⚽ Apostas</a>
    <a class="nav-link active" href="/tabela">🏆 Tabela</a>
    <a class="nav-link" href="/conta">👤 Conta</a>
  </div>
</nav>

<div class="page">
  <div class="liga-tabs" id="liga-tabs">
    <div class="liga-tab active" data-league="71" data-season="2024" onclick="trocarLiga(this)">🇧🇷 Brasileirão Série A</div>
    <div class="liga-tab" data-league="75" data-season="2024" onclick="trocarLiga(this)">🇧🇷 Série B</div>
    <div class="liga-tab" data-league="13" data-season="2024" onclick="trocarLiga(this)">🏆 Libertadores</div>
    <div class="liga-tab" data-league="11" data-season="2024" onclick="trocarLiga(this)">🌎 Sul-Americana</div>
    <div class="liga-tab" data-league="2" data-season="2024" onclick="trocarLiga(this)">⭐ Champions League</div>
    <div class="liga-tab" data-league="39" data-season="2024" onclick="trocarLiga(this)">🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League</div>
    <div class="liga-tab" data-league="140" data-season="2024" onclick="trocarLiga(this)">🇪🇸 La Liga</div>
    <div class="liga-tab" data-league="135" data-season="2024" onclick="trocarLiga(this)">🇮🇹 Serie A</div>
    <div class="liga-tab" data-league="78" data-season="2024" onclick="trocarLiga(this)">🇩🇪 Bundesliga</div>
    <div class="liga-tab" data-league="61" data-season="2024" onclick="trocarLiga(this)">🇫🇷 Ligue 1</div>
  </div>

  <div class="secao-titulo" id="tabela-titulo">Brasileirão Série A 2024</div>

  <div id="tabela-container">
    <div class="loading-msg"><div class="spinner"></div>Carregando tabela...</div>
  </div>

  <div class="legend" id="tabela-legend" style="display:none">
    <div class="legend-item"><div class="legend-dot" style="background:#3b82f6"></div> Libertadores</div>
    <div class="legend-item"><div class="legend-dot" style="background:#22c55e"></div> Sul-Americana</div>
    <div class="legend-item"><div class="legend-dot" style="background:#ef4444"></div> Rebaixamento</div>
  </div>
</div>

<script>
const BASE = '';

function trocarLiga(el) {
  document.querySelectorAll('.liga-tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  const league = el.dataset.league;
  const season = el.dataset.season;
  carregarTabela(league, season, el.textContent.trim());
}

function zoneClass(desc) {
  if (!desc) return '';
  const d = desc.toLowerCase();
  if (d.includes('libertadores') || d.includes('champions') || d.includes('promotion')) return 'zone-cl';
  if (d.includes('sul-americana') || d.includes('europa') || d.includes('play')) return 'zone-el';
  if (d.includes('rebaixamento') || d.includes('relegation')) return 'zone-rl';
  return '';
}

function formDots(form) {
  if (!form) return '';
  return form.split('').slice(-5).map(c => {
    const cls = c === 'W' ? 'W' : c === 'D' ? 'D' : 'L';
    return `<span class="form-c ${cls}">${c}</span>`;
  }).join('');
}

async function carregarTabela(league, season, titulo) {
  const container = document.getElementById('tabela-container');
  const tituloEl = document.getElementById('tabela-titulo');
  const legend = document.getElementById('tabela-legend');
  container.innerHTML = '<div class="loading-msg"><div class="spinner"></div>Carregando tabela...</div>';
  legend.style.display = 'none';
  if (tituloEl) tituloEl.textContent = titulo + ' ' + season;
  try {
    const r = await fetch(`${BASE}/api/bet/standings?league=${league}&season=${season}`);
    const d = await r.json();
    if (!d.success || !d.standings || !d.standings.length) {
      container.innerHTML = '<div class="loading-msg">⚠️ Dados não disponíveis para esta liga.</div>';
      return;
    }
    const rows = d.standings;
    legend.style.display = 'flex';
    container.innerHTML = `
      <div class="tabela-wrap">
        <div class="tabela-header">
          <div style="text-align:center">#</div>
          <div>Time</div>
          <div class="col-jg" style="text-align:center" title="Jogos">J</div>
          <div style="text-align:center" title="Vitórias">V</div>
          <div style="text-align:center" title="Empates">E</div>
          <div style="text-align:center" title="Derrotas">D</div>
          <div class="col-gd" style="text-align:center" title="Saldo de gols">SG</div>
          <div style="text-align:center" title="Pontos">Pts</div>
          <div style="text-align:right" title="Forma (últimos 5)">Forma</div>
        </div>
        ${rows.map(t => {
          const rankCls = t.rank === 1 ? 'pos1' : t.rank === 2 ? 'pos2' : t.rank === 3 ? 'pos3' : '';
          const zone = zoneClass(t.description || '');
          const logoHtml = t.logo
            ? `<img class="team-logo" src="${t.logo}" alt="" onerror="this.style.display='none';">`
            : `<div class="team-ini">${(t.name||'?').substring(0,2).toUpperCase()}</div>`;
          return `
          <div class="tabela-row ${zone}">
            <div class="col-rank ${rankCls}">${t.rank}</div>
            <div class="col-team">${logoHtml}<span class="team-name">${t.name}</span></div>
            <div class="col-num col-jg">${t.played}</div>
            <div class="col-num">${t.won}</div>
            <div class="col-num">${t.draw}</div>
            <div class="col-num">${t.lost}</div>
            <div class="col-num col-gd">${t.gd >= 0 ? '+' : ''}${t.gd}</div>
            <div class="col-pts">${t.points}</div>
            <div class="col-form">${formDots(t.form)}</div>
          </div>`;
        }).join('')}
      </div>`;
  } catch(e) {
    container.innerHTML = '<div class="loading-msg">⚠️ Erro ao carregar. Tente novamente.</div>';
  }
}

document.addEventListener('DOMContentLoaded', () => {
  carregarTabela('71', '2024', 'Brasileirão Série A');
});
</script>
</body>
</html>"""

def _page_apostas_html():
    return r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="theme-color" content="#0a0a0f">
<title>PaynexBet — Apostas Esportivas</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{--gold:#FFD700;--blue:#3b82f6;--green:#22c55e;--red:#ef4444;--bg:#0a0a0f;--card:#111118;--card2:#13131e}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:#fff;min-height:100vh}

/* TOPBAR */
.topbar{background:#0d0d16;border-bottom:1px solid #ffffff0c;padding:0 20px;height:56px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;width:100%}
.topbar-logo{font-size:20px;font-weight:900;background:linear-gradient(135deg,#FFD700,#FFA500);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;text-decoration:none;letter-spacing:-0.5px}
.topbar-right{display:flex;align-items:center;gap:10px}
.saldo-badge{background:#00d68f14;border:1px solid #22c55e33;border-radius:8px;padding:5px 14px;font-size:14px;font-weight:700;color:#4ade80}
.btn-conta{background:#1e3a8a;border:none;border-radius:8px;padding:7px 14px;color:#fff;font-size:13px;font-weight:700;cursor:pointer;text-decoration:none}

/* LAYOUT FULL-WIDTH */
.page-layout{display:flex;min-height:calc(100vh - 52px);width:100%;overflow-x:hidden}
.main{flex:1;min-width:0;padding:14px 16px 80px;width:100%;overflow-x:hidden}
@media(min-width:768px){
  .main{padding:16px 24px 80px}
}
@media(min-width:1024px){
  .page-layout{gap:0}
  .main{padding:20px 32px 24px}
  .bet-slip-sidebar{width:340px;flex-shrink:0;background:#0d0d16;border-left:1px solid #ffffff0c;padding:16px;height:calc(100vh - 52px);position:sticky;top:52px;overflow-y:auto;display:flex;flex-direction:column}
}
@media(min-width:1440px){
  .main{padding:24px 48px 24px}
}

/* FILTROS ESPORTES */
.sport-tabs{display:flex;gap:6px;overflow-x:auto;padding-bottom:4px;margin-bottom:14px;-webkit-overflow-scrolling:touch;scrollbar-width:none}
.sport-tabs::-webkit-scrollbar{display:none}
.sport-tab{flex-shrink:0;background:var(--card);border:1px solid #ffffff10;border-radius:999px;padding:6px 14px;font-size:12px;font-weight:600;color:#888;cursor:pointer;white-space:nowrap;transition:all .15s}
.sport-tab.active{background:#1e3a8a;border-color:#3b82f6;color:#fff}

/* CARD JOGO */
.secao-titulo{font-size:12px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;display:flex;align-items:center;gap:6px}
.live-dot{width:6px;height:6px;background:#ef4444;border-radius:50%;animation:blink 1.2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.jogos-lista{display:grid;grid-template-columns:1fr;gap:10px;margin-bottom:20px}
@media(min-width:640px){.jogos-lista{grid-template-columns:1fr 1fr}}
@media(min-width:1024px){.jogos-lista{grid-template-columns:1fr 1fr}}
@media(min-width:1280px){.jogos-lista{grid-template-columns:1fr 1fr 1fr}}
.jogo-card{background:var(--card);border:1px solid #ffffff0c;border-radius:14px;padding:14px;cursor:pointer;transition:border-color .2s}
.jogo-card:hover{border-color:#3b82f633}
.jogo-card.live{border-left:3px solid #ef4444}
.jogo-meta{display:flex;align-items:center;gap:6px;margin-bottom:8px}
.jogo-liga{font-size:11px;color:#555;flex:1}
.jogo-hora{font-size:11px;color:#666}
.live-tag{background:#ef444418;color:#ef4444;border:1px solid #ef444433;border-radius:4px;padding:1px 7px;font-size:10px;font-weight:700}
.jogo-times{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;gap:6px}
.time-bloco{display:flex;flex-direction:column;align-items:center;gap:4px;flex:1;min-width:0}
.time-bloco.away{align-items:flex-end}
.time-escudo{width:40px;height:40px;border-radius:50%;object-fit:contain;background:#1e2035;padding:4px;border:1.5px solid #ffffff15;flex-shrink:0}
.time-initials{width:40px;height:40px;border-radius:50%;background:linear-gradient(135deg,#1e3a8a,#3b82f6);color:#fff;font-size:12px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0;border:1.5px solid #3b82f633}
.time-nome{font-size:13px;font-weight:800;color:#fff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%;text-align:center}
.time-bloco.away .time-nome{text-align:right}
.placar{font-size:11px;font-weight:700;color:#555;text-align:center;flex-shrink:0;padding:0 4px}
.jogo-odds{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}
.jogo-odds.no-draw{grid-template-columns:1fr 1fr}
.odd-btn{background:#0d1120;border:1px solid #3b82f622;border-radius:10px;padding:8px 6px;text-align:center;cursor:pointer;transition:all .15s}
.odd-btn:hover,.odd-btn.sel{background:#1e3a8a;border-color:#3b82f6}
.odd-lbl{font-size:10px;color:#666;margin-bottom:3px}
.odd-val{font-size:15px;font-weight:900;color:var(--blue)}
.odd-btn.sel .odd-val{color:#fff}

/* PLACAR AO VIVO */
.placar-live{display:flex;align-items:center;gap:6px;flex-shrink:0;padding:0 8px}
.placar-num{font-size:22px;font-weight:900;color:#fff;line-height:1}
.placar-sep{font-size:16px;color:#444;font-weight:700}

/* BARRA DE PREVISÃO IA */
.pred-bar{display:flex;height:5px;border-radius:3px;overflow:hidden;margin:8px 0 4px;gap:1px}
.pred-home{background:#3b82f6;display:flex;align-items:center;justify-content:center;font-size:9px;color:#fff;font-weight:700;min-width:16px}
.pred-draw{background:#555;display:flex;align-items:center;justify-content:center;font-size:9px;color:#fff;font-weight:700;min-width:12px}
.pred-away{background:#ef4444;display:flex;align-items:center;justify-content:center;font-size:9px;color:#fff;font-weight:700;min-width:16px}
.pred-advice{font-size:10px;color:#888;margin-bottom:8px;font-style:italic}

/* CARRINHO FIXO — mobile: barra inferior | desktop: coluna lateral */
.bet-slip-bar{position:fixed;bottom:0;left:0;right:0;background:#0d1a0d;border-top:1px solid #22c55e33;z-index:200;transition:max-height .3s;max-height:56px;overflow:hidden}
.bet-slip-bar.open{max-height:90vh;overflow:auto}
.bet-slip-toggle{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;cursor:pointer}
.bst-left{display:flex;align-items:center;gap:10px}
.bst-count{background:#22c55e;color:#000;border-radius:50%;width:22px;height:22px;font-size:11px;font-weight:900;display:flex;align-items:center;justify-content:center}
.bst-title{font-size:14px;font-weight:700;color:#4ade80}
.bst-odd{font-size:12px;color:#888}
.bst-confirmar{background:linear-gradient(135deg,#16a34a,#22c55e);border:none;border-radius:8px;padding:8px 16px;color:#000;font-size:13px;font-weight:900;cursor:pointer}
.bet-slip-body{padding:0 16px 16px}
.bs-item{background:#0a140a;border:1px solid #22c55e1a;border-radius:10px;padding:10px 12px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:flex-start}
.bs-item-info{flex:1}
.bs-item-jogo{font-size:11px;color:#666;margin-bottom:2px}
.bs-item-sel{font-size:13px;font-weight:700;color:#4ade80}
.bs-item-odd{font-size:12px;color:#22c55e}
.bs-item-remove{background:none;border:none;color:#666;font-size:18px;cursor:pointer;padding:0 0 0 8px;line-height:1}
.bs-valor-row{display:flex;gap:8px;margin-top:8px;align-items:center}
.bs-input{flex:1;background:#0a0a14;border:1px solid #22c55e33;border-radius:8px;padding:10px;color:#fff;font-size:16px;font-weight:700}
.bs-chips{display:flex;gap:6px;margin-top:6px;flex-wrap:wrap}
.bs-chip{background:#0d1a0d;border:1px solid #22c55e22;border-radius:6px;padding:5px 10px;font-size:12px;font-weight:700;color:#4ade80;cursor:pointer}
.bs-chip:hover{background:#1a2e1a}
.bs-retorno{font-size:12px;color:#555;margin-top:8px;text-align:right}
.bs-retorno span{color:#4ade80;font-weight:700;font-size:14px}
.bs-btn-apostar{width:100%;background:linear-gradient(135deg,#16a34a,#22c55e);border:none;border-radius:10px;padding:14px;color:#000;font-size:15px;font-weight:900;cursor:pointer;margin-top:10px}
/* Sidebar desktop */
.bet-slip-sidebar{display:none}
.sidebar-title{font-size:14px;font-weight:900;color:#4ade80;margin-bottom:12px;display:flex;align-items:center;gap:8px}
.sidebar-empty{color:#555;font-size:13px;text-align:center;padding:30px 0}
.sidebar-items{flex:1;overflow-y:auto;margin-bottom:12px}
.sidebar-footer{border-top:1px solid #22c55e22;padding-top:12px}
@media(min-width:1024px){
  .bet-slip-bar{display:none !important}
  .bet-slip-sidebar{display:flex;flex-direction:column;width:340px;flex-shrink:0;background:#0d0d16;border-left:1px solid #ffffff0c;padding:20px 16px;height:calc(100vh - 52px);position:sticky;top:52px;overflow-y:auto}
  .main{padding-bottom:24px}
}
@media(min-width:1440px){
  .bet-slip-sidebar{width:380px}
}

/* LOGIN MODAL */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:500;align-items:flex-end;justify-content:center}
.modal-overlay.show{display:flex}
.modal-box{background:#13131e;border-radius:20px 20px 0 0;padding:24px 20px 32px;width:100%;max-width:480px;border-top:1px solid #3b82f633}
.modal-title{font-size:18px;font-weight:900;color:#fff;margin-bottom:6px}
.modal-sub{font-size:13px;color:#666;margin-bottom:20px;line-height:1.5}
.modal-input{width:100%;background:#0a0a14;border:1px solid #3b82f633;border-radius:10px;padding:12px 14px;color:#fff;font-size:15px;margin-bottom:10px}
.modal-btn{width:100%;background:linear-gradient(135deg,#1e3a8a,#3b82f6);border:none;border-radius:10px;padding:14px;color:#fff;font-size:15px;font-weight:900;cursor:pointer}
.modal-close{float:right;background:none;border:none;color:#666;font-size:22px;cursor:pointer;margin-top:-4px}

/* TOAST */
.toast{position:fixed;top:20px;left:50%;transform:translateX(-50%);background:#1a2e1a;border:1px solid #22c55e44;border-radius:10px;padding:10px 18px;font-size:13px;font-weight:600;color:#4ade80;z-index:9999;display:none;white-space:nowrap}
.toast.err{background:#2e1a1a;border-color:#ef444444;color:#f87171}
.toast.show{display:block;animation:fadeInOut 2.5s ease}
@keyframes fadeInOut{0%{opacity:0;transform:translateX(-50%) translateY(-10px)}15%,85%{opacity:1;transform:translateX(-50%) translateY(0)}100%{opacity:0}}

/* LOADING */
.loading-spinner{display:flex;align-items:center;justify-content:center;padding:40px;color:#555;font-size:14px;gap:10px}
.spinner{width:20px;height:20px;border:2px solid #333;border-top-color:#3b82f6;border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

@media(min-width:600px) and (max-width:1023px){.bet-slip-bar{border-radius:16px 16px 0 0}}
</style>
</head>
<body>

<!-- TOPBAR -->
<header class="topbar">
  <a href="/" class="topbar-logo">🎰 PaynexBet</a>
  <div class="topbar-right">
    <a href="/tabela" style="background:#1e3a8a;border:none;border-radius:8px;padding:6px 12px;color:#fff;font-size:12px;font-weight:700;text-decoration:none">🏆 Tabela</a>
    <div class="saldo-badge" id="saldo-display">💰 R$ 0,00</div>
    <a href="/conta" class="btn-conta" id="btn-conta-link">👤 Conta</a>
  </div>
</header>

<div class="page-layout">
<div class="main">
  <!-- Filtros de esporte -->
  <div style="margin-bottom:12px">
    <h2 style="font-size:13px;color:#555;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;display:flex;align-items:center;gap:8px">
      <span style="width:8px;height:8px;background:#22c55e;border-radius:50%;display:inline-block;animation:blink 1.2s infinite"></span>
      Apostar Agora
    </h2>
    <div class="sport-tabs" id="sport-tabs">
      <div class="sport-tab active" data-sport="upcoming" onclick="trocarSport(this)">🔥 Em destaque</div>
      <div class="sport-tab" data-sport="soccer_brazil_campeonato" onclick="trocarSport(this)">🇧🇷 Brasileirão</div>
      <div class="sport-tab" data-sport="soccer_conmebol_copa_libertadores" onclick="trocarSport(this)">🏆 Libertadores</div>
      <div class="sport-tab" data-sport="soccer_uefa_champs_league" onclick="trocarSport(this)">⭐ Champions</div>
      <div class="sport-tab" data-sport="soccer_epl" onclick="trocarSport(this)">🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League</div>
      <div class="sport-tab" data-sport="basketball_nba" onclick="trocarSport(this)">🏀 NBA</div>
    </div>
  </div>

  <div class="secao-titulo"><span class="live-dot"></span> Jogos disponíveis</div>
  <div class="jogos-lista" id="jogos-lista">
    <div class="loading-spinner"><div class="spinner"></div> Carregando jogos...</div>
  </div>

  <!-- SEÇÃO AO VIVO (api-sports.io) -->
  <div id="sec-live" style="display:none">
    <div class="secao-titulo" style="margin-top:8px">
      <span class="live-dot"></span>
      <span style="color:#ef4444;font-weight:800">AO VIVO AGORA</span>
      <span style="font-size:10px;color:#555;font-weight:400;margin-left:4px">atualiza a cada 30s</span>
    </div>
    <div class="jogos-lista" id="jogos-live"></div>
  </div>
</div><!-- /main -->

<!-- SIDEBAR CARRINHO (desktop ≥1024px) -->
<aside class="bet-slip-sidebar" id="bet-slip-sidebar">
  <div class="sidebar-title">
    <span id="sidebar-count" style="background:#22c55e;color:#000;border-radius:50%;width:22px;height:22px;font-size:11px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0">0</span>
    🎯 Minha Aposta
  </div>
  <div class="sidebar-items" id="sidebar-items">
    <div class="sidebar-empty" id="sidebar-empty">Clique nas odds para adicionar apostas</div>
  </div>
  <div class="sidebar-footer" id="sidebar-footer" style="display:none">
    <div style="font-size:11px;color:#666;margin-bottom:6px">Odd total: <strong id="sidebar-odd-total" style="color:#4ade80">—</strong></div>
    <input class="bs-input" type="number" id="sidebar-valor" placeholder="R$ valor" min="2" step="1" oninput="calcRetorno()" style="width:100%;margin-bottom:8px">
    <div class="bs-chips">
      <div class="bs-chip" onclick="setValor(5)">R$5</div>
      <div class="bs-chip" onclick="setValor(10)">R$10</div>
      <div class="bs-chip" onclick="setValor(20)">R$20</div>
      <div class="bs-chip" onclick="setValor(50)">R$50</div>
      <div class="bs-chip" onclick="setValor(100)">R$100</div>
    </div>
    <div class="bs-retorno" style="margin-bottom:10px">Retorno potencial: <span id="sidebar-retorno">R$ 0,00</span></div>
    <button class="bs-btn-apostar" onclick="confirmarAposta()">🎯 Confirmar Aposta</button>
  </div>
</aside>

</div><!-- /page-layout -->

<!-- CARRINHO (bet slip) -->
<div class="bet-slip-bar" id="bet-slip-bar">
  <div class="bet-slip-toggle" onclick="toggleSlip()">
    <div class="bst-left">
      <div class="bst-count" id="slip-count">0</div>
      <div class="bst-title">Minha aposta</div>
      <div class="bst-odd" id="slip-odd-total">Odd total: —</div>
    </div>
    <button class="bst-confirmar" onclick="event.stopPropagation();confirmarAposta()">Apostar ▶</button>
  </div>
  <div class="bet-slip-body" id="slip-body" style="display:none">
    <div id="slip-items"></div>
    <div class="bs-valor-row">
      <input class="bs-input" type="number" id="slip-valor" placeholder="R$ valor" min="2" step="1" oninput="calcRetorno()">
    </div>
    <div class="bs-chips">
      <div class="bs-chip" onclick="setValor(5)">R$5</div>
      <div class="bs-chip" onclick="setValor(10)">R$10</div>
      <div class="bs-chip" onclick="setValor(20)">R$20</div>
      <div class="bs-chip" onclick="setValor(50)">R$50</div>
      <div class="bs-chip" onclick="setValor(100)">R$100</div>
    </div>
    <div class="bs-retorno">Retorno potencial: <span id="slip-retorno">R$ 0,00</span></div>
    <button class="bs-btn-apostar" onclick="confirmarAposta()">🎯 Confirmar Aposta</button>
  </div>
</div>

<!-- LOGIN MODAL -->
<div class="modal-overlay" id="login-modal">
  <div class="modal-box">
    <button class="modal-close" onclick="fecharModal()">✕</button>
    <div class="modal-title">👤 Entre na sua conta</div>
    <div class="modal-sub">Informe seu CPF para acessar. Se for novo, sua conta é criada automaticamente!</div>
    <input class="modal-input" id="modal-nome" type="text" placeholder="Seu nome (opcional)">
    <input class="modal-input" id="modal-cpf" type="tel" placeholder="CPF (somente números)" maxlength="14" oninput="maskCPF(this)">
    <button class="modal-btn" onclick="fazerLogin()">🚀 Entrar / Cadastrar</button>
  </div>
</div>

<!-- TOAST -->
<div class="toast" id="toast"></div>

<script>
const BASE = '';
const _fmt = n => 'R$ ' + Number(n||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
let _usuario = JSON.parse(localStorage.getItem('bet_usuario') || 'null');
let _selecoes = [];
let _sportAtual = 'upcoming';

// ── Inicialização ──
document.addEventListener('DOMContentLoaded', () => {
  atualizarSaldo();
  carregarJogos(_sportAtual);
  carregarAoVivo();
  // Atualizar ao vivo a cada 30s
  if (_liveInterval) clearInterval(_liveInterval);
  _liveInterval = setInterval(carregarAoVivo, 30000);
});

function atualizarSaldo() {
  const el = document.getElementById('saldo-display');
  if (_usuario) {
    el.textContent = _fmt(_usuario.saldo);
    // Sincronizar com servidor
    fetch(`${BASE}/api/bet/saldo/${_usuario.id}`)
      .then(r => r.json())
      .then(d => {
        if (d.success) {
          _usuario.saldo = d.usuario.saldo;
          localStorage.setItem('bet_usuario', JSON.stringify(_usuario));
          el.textContent = _fmt(_usuario.saldo);
        }
      }).catch(() => {});
  } else {
    el.textContent = 'R$ 0,00';
  }
}

// ── Logos oficiais media.api-sports.io (alta qualidade, cobertura mundial) ──
const _LOGOS = {
  // ── Brasileirão Série A (IDs oficiais api-sports) ──
  'EC Bahia':              'https://media.api-sports.io/football/teams/118.png',
  'Bahia':                 'https://media.api-sports.io/football/teams/118.png',
  'SC Internacional':      'https://media.api-sports.io/football/teams/119.png',
  'Internacional':         'https://media.api-sports.io/football/teams/119.png',
  'Botafogo FR':           'https://media.api-sports.io/football/teams/120.png',
  'Botafogo':              'https://media.api-sports.io/football/teams/120.png',
  'SE Palmeiras':          'https://media.api-sports.io/football/teams/121.png',
  'Palmeiras':             'https://media.api-sports.io/football/teams/121.png',
  'Fluminense FC':         'https://media.api-sports.io/football/teams/124.png',
  'Fluminense':            'https://media.api-sports.io/football/teams/124.png',
  'São Paulo FC':          'https://media.api-sports.io/football/teams/126.png',
  'Sao Paulo':             'https://media.api-sports.io/football/teams/126.png',
  'São Paulo':             'https://media.api-sports.io/football/teams/126.png',
  'CR Flamengo':           'https://media.api-sports.io/football/teams/127.png',
  'Flamengo':              'https://media.api-sports.io/football/teams/127.png',
  'Grêmio FBPA':           'https://media.api-sports.io/football/teams/130.png',
  'Gremio':                'https://media.api-sports.io/football/teams/130.png',
  'Grêmio':                'https://media.api-sports.io/football/teams/130.png',
  'SC Corinthians Paulista':'https://media.api-sports.io/football/teams/131.png',
  'Corinthians':           'https://media.api-sports.io/football/teams/131.png',
  'CR Vasco da Gama':      'https://media.api-sports.io/football/teams/133.png',
  'Vasco da Gama':         'https://media.api-sports.io/football/teams/133.png',
  'Vasco':                 'https://media.api-sports.io/football/teams/133.png',
  'CA Paranaense':         'https://media.api-sports.io/football/teams/134.png',
  'Athletico Paranaense':  'https://media.api-sports.io/football/teams/134.png',
  'Atletico Paranaense':   'https://media.api-sports.io/football/teams/134.png',
  'Cruzeiro EC':           'https://media.api-sports.io/football/teams/135.png',
  'Cruzeiro':              'https://media.api-sports.io/football/teams/135.png',
  'EC Vitória':            'https://media.api-sports.io/football/teams/136.png',
  'Vitória':               'https://media.api-sports.io/football/teams/136.png',
  'Vitoria':               'https://media.api-sports.io/football/teams/136.png',
  'Criciúma EC':           'https://media.api-sports.io/football/teams/140.png',
  'Criciuma':              'https://media.api-sports.io/football/teams/140.png',
  'Atletico Goianiense':   'https://media.api-sports.io/football/teams/144.png',
  'Atlético Goianiense':   'https://media.api-sports.io/football/teams/144.png',
  'Juventude':             'https://media.api-sports.io/football/teams/152.png',
  'Fortaleza EC':          'https://media.api-sports.io/football/teams/154.png',
  'Fortaleza':             'https://media.api-sports.io/football/teams/154.png',
  'RB Bragantino':         'https://media.api-sports.io/football/teams/794.png',
  'Red Bull Bragantino':   'https://media.api-sports.io/football/teams/794.png',
  'Bragantino':            'https://media.api-sports.io/football/teams/794.png',
  'Atletico-MG':           'https://media.api-sports.io/football/teams/1062.png',
  'CA Mineiro':            'https://media.api-sports.io/football/teams/1062.png',
  'Atletico Mineiro':      'https://media.api-sports.io/football/teams/1062.png',
  'Atlético Mineiro':      'https://media.api-sports.io/football/teams/1062.png',
  'Cuiabá EC':             'https://media.api-sports.io/football/teams/1193.png',
  'Cuiaba':                'https://media.api-sports.io/football/teams/1193.png',
  // ── Série B / outros times brasileiros ──
  'Santos FC':             'https://media.api-sports.io/football/teams/137.png',
  'Santos':                'https://media.api-sports.io/football/teams/137.png',
  'Clube do Remo':         'https://media.api-sports.io/football/teams/18309.png',
  'Remo':                  'https://media.api-sports.io/football/teams/18309.png',
  'Mirassol FC':           'https://media.api-sports.io/football/teams/20006.png',
  'Mirassol':              'https://media.api-sports.io/football/teams/20006.png',
  // ── Copa Libertadores ──
  'Club Libertad Asuncion':'https://media.api-sports.io/football/teams/1843.png',
  'Libertad Asuncion':     'https://media.api-sports.io/football/teams/1843.png',
  'Libertad':              'https://media.api-sports.io/football/teams/1843.png',
  'CAR Independiente del Valle': 'https://media.api-sports.io/football/teams/1952.png',
  'Independiente del Valle': 'https://media.api-sports.io/football/teams/1952.png',
  'CA Lanús':              'https://media.api-sports.io/football/teams/468.png',
  'Lanus':                 'https://media.api-sports.io/football/teams/468.png',
  'LDU de Quito':          'https://media.api-sports.io/football/teams/1937.png',
  'LDU Quito':             'https://media.api-sports.io/football/teams/1937.png',
  'CA Rosario Central':    'https://media.api-sports.io/football/teams/462.png',
  'Rosario Central':       'https://media.api-sports.io/football/teams/462.png',
  'CA Boca Juniors':       'https://media.api-sports.io/football/teams/405.png',
  'Boca Juniors':          'https://media.api-sports.io/football/teams/405.png',
  'CA Peñarol':            'https://media.api-sports.io/football/teams/1938.png',
  'Penarol':               'https://media.api-sports.io/football/teams/1938.png',
  'Nacional':              'https://media.api-sports.io/football/teams/2016.png',
  'Club Nacional de Football': 'https://media.api-sports.io/football/teams/2016.png',
  'CA River Plate':        'https://media.api-sports.io/football/teams/442.png',
  'River Plate':           'https://media.api-sports.io/football/teams/442.png',
  'Club Bolívar':          'https://media.api-sports.io/football/teams/1899.png',
  'Barcelona SC':          'https://media.api-sports.io/football/teams/1961.png',
  'Club Guaraní':          'https://media.api-sports.io/football/teams/1847.png',
  'Club Cerro Porteño':    'https://media.api-sports.io/football/teams/1840.png',
  'Estudiantes de La Plata':'https://media.api-sports.io/football/teams/451.png',
  // ── UEFA Champions League ──
  'Real Madrid CF':        'https://media.api-sports.io/football/teams/541.png',
  'Real Madrid':           'https://media.api-sports.io/football/teams/541.png',
  'FC Barcelona':          'https://media.api-sports.io/football/teams/529.png',
  'Barcelona':             'https://media.api-sports.io/football/teams/529.png',
  'FC Bayern München':     'https://media.api-sports.io/football/teams/157.png',
  'Bayern Munich':         'https://media.api-sports.io/football/teams/157.png',
  'Bayern München':        'https://media.api-sports.io/football/teams/157.png',
  'Paris Saint-Germain FC':'https://media.api-sports.io/football/teams/85.png',
  'Paris Saint Germain':   'https://media.api-sports.io/football/teams/85.png',
  'PSG':                   'https://media.api-sports.io/football/teams/85.png',
  'Club Atlético de Madrid':'https://media.api-sports.io/football/teams/530.png',
  'Atletico Madrid':       'https://media.api-sports.io/football/teams/530.png',
  'Atlético Madrid':       'https://media.api-sports.io/football/teams/530.png',
  'Chelsea FC':            'https://media.api-sports.io/football/teams/49.png',
  'Chelsea':               'https://media.api-sports.io/football/teams/49.png',
  'Arsenal FC':            'https://media.api-sports.io/football/teams/42.png',
  'Arsenal':               'https://media.api-sports.io/football/teams/42.png',
  'Liverpool FC':          'https://media.api-sports.io/football/teams/40.png',
  'Liverpool':             'https://media.api-sports.io/football/teams/40.png',
  'Manchester City FC':    'https://media.api-sports.io/football/teams/50.png',
  'Manchester City':       'https://media.api-sports.io/football/teams/50.png',
  'Manchester United FC':  'https://media.api-sports.io/football/teams/33.png',
  'Manchester United':     'https://media.api-sports.io/football/teams/33.png',
  'Tottenham Hotspur FC':  'https://media.api-sports.io/football/teams/47.png',
  'Tottenham':             'https://media.api-sports.io/football/teams/47.png',
  'Juventus FC':           'https://media.api-sports.io/football/teams/496.png',
  'Juventus':              'https://media.api-sports.io/football/teams/496.png',
  'FC Internazionale Milano': 'https://media.api-sports.io/football/teams/505.png',
  'Inter Milan':           'https://media.api-sports.io/football/teams/505.png',
  'Internazionale':        'https://media.api-sports.io/football/teams/505.png',
  'Borussia Dortmund':     'https://media.api-sports.io/football/teams/165.png',
  'Bayer 04 Leverkusen':   'https://media.api-sports.io/football/teams/168.png',
  'Leverkusen':            'https://media.api-sports.io/football/teams/168.png',
  'Eintracht Frankfurt':   'https://media.api-sports.io/football/teams/169.png',
  'Atalanta BC':           'https://media.api-sports.io/football/teams/499.png',
  'SSC Napoli':            'https://media.api-sports.io/football/teams/492.png',
  'Napoli':                'https://media.api-sports.io/football/teams/492.png',
  'AFC Ajax':              'https://media.api-sports.io/football/teams/194.png',
  'Ajax':                  'https://media.api-sports.io/football/teams/194.png',
  'PSV':                   'https://media.api-sports.io/football/teams/197.png',
  'Sporting Clube de Portugal': 'https://media.api-sports.io/football/teams/228.png',
  'Sporting CP':           'https://media.api-sports.io/football/teams/228.png',
  'Sport Lisboa e Benfica':'https://media.api-sports.io/football/teams/211.png',
  'Benfica':               'https://media.api-sports.io/football/teams/211.png',
  'Olympique de Marseille':'https://media.api-sports.io/football/teams/81.png',
  'Marseille':             'https://media.api-sports.io/football/teams/81.png',
  'AS Monaco FC':          'https://media.api-sports.io/football/teams/91.png',
  'Monaco':                'https://media.api-sports.io/football/teams/91.png',
  'Galatasaray SK':        'https://media.api-sports.io/football/teams/206.png',
  'Galatasaray':           'https://media.api-sports.io/football/teams/206.png',
  // ── Premier League ──
  'Aston Villa FC':        'https://media.api-sports.io/football/teams/66.png',
  'Aston Villa':           'https://media.api-sports.io/football/teams/66.png',
  'Everton FC':            'https://media.api-sports.io/football/teams/45.png',
  'Everton':               'https://media.api-sports.io/football/teams/45.png',
  'Fulham FC':             'https://media.api-sports.io/football/teams/36.png',
  'Fulham':                'https://media.api-sports.io/football/teams/36.png',
  'Newcastle United FC':   'https://media.api-sports.io/football/teams/34.png',
  'Newcastle':             'https://media.api-sports.io/football/teams/34.png',
  'Wolverhampton Wanderers FC': 'https://media.api-sports.io/football/teams/39.png',
  'Wolves':                'https://media.api-sports.io/football/teams/39.png',
  'Nottingham Forest FC':  'https://media.api-sports.io/football/teams/65.png',
  'Nottingham Forest':     'https://media.api-sports.io/football/teams/65.png',
  'Crystal Palace FC':     'https://media.api-sports.io/football/teams/52.png',
  'Crystal Palace':        'https://media.api-sports.io/football/teams/52.png',
  'Brighton & Hove Albion FC': 'https://media.api-sports.io/football/teams/51.png',
  'Brighton':              'https://media.api-sports.io/football/teams/51.png',
  'West Ham United FC':    'https://media.api-sports.io/football/teams/48.png',
  'West Ham':              'https://media.api-sports.io/football/teams/48.png',
  'Brentford FC':          'https://media.api-sports.io/football/teams/55.png',
  'Brentford':             'https://media.api-sports.io/football/teams/55.png',
};

// Cache dinâmico de crests buscados via API
const _crestCache = {};

async function fetchCrestDynamic(teamName) {
  if (_crestCache[teamName]) return _crestCache[teamName];
  try {
    const r = await fetch(`/api/bet/crest?nome=${encodeURIComponent(teamName)}`);
    const d = await r.json();
    if (d.crest) { _crestCache[teamName] = d.crest; return d.crest; }
  } catch(e) {}
  return null;
}

function getTeamLogo(teamName) {
  if (!teamName) return null;
  // 1. Busca exata
  if (_LOGOS[teamName]) return _LOGOS[teamName];
  // 2. Busca parcial — normaliza acentos e case
  const norm = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const lower = norm(teamName);
  for (const [key, url] of Object.entries(_LOGOS)) {
    const k = norm(key);
    if (lower === k || lower.includes(k) || k.includes(lower.split(' ')[0])) return url;
  }
  return null;
}

function teamInitials(name) {
  if (!name) return '?';
  // Remove prefixos, parênteses e caracteres especiais
  const clean = name.trim()
    .replace(/\(.*?\)/g, '')           // remove tudo entre parênteses ex: (BAH)
    .replace(/[^a-zA-ZÀ-ú\s]/g, '')   // remove símbolos
    .replace(/^(FC|SC|CA|CR|CD|EC|SE|RB|AA|AC|AS|CF|SD|FK)\s/i, '')
    .trim();
  if (!clean) return name.substring(0,2).toUpperCase();
  const words = clean.split(/\s+/).filter(w => w.length > 0);
  if (words.length === 1) return words[0].substring(0,3).toUpperCase();
  // Usa primeira letra de cada palavra significativa (ignora artigos)
  const skip = new Set(['de','do','da','dos','das','del','el','la','los','las','di','van','von','e']);
  const meaningful = words.filter(w => !skip.has(w.toLowerCase()));
  if (meaningful.length >= 2) return (meaningful[0][0] + meaningful[meaningful.length-1][0]).toUpperCase();
  return (words[0][0] + (words[words.length-1][0]||'')).toUpperCase();
}

// Retorna label curta e limpa para botão de odd
function teamShortLabel(name) {
  if (!name) return '?';
  // Remove parênteses e conteúdo
  const clean = name.replace(/\s*\(.*?\)\s*/g, '').trim();
  // Prefixos a ignorar
  const noPfx = clean.replace(/^(FC|SC|CA|CR|CD|EC|SE|RB|AA|AC|AS|CF|SD|FK)\s+/i, '').trim();
  const words = noPfx.split(/\s+/);
  // Se nome curto (≤10 chars) retorna direto
  if (noPfx.length <= 10) return noPfx;
  // Artigos/preposições a pular
  const skip = new Set(['de','do','da','dos','das','del','el','la','los','las','di','van','von','e','y','und']);
  const sig = words.filter(w => w.length > 1 && !skip.has(w.toLowerCase()));
  // 1 palavra significativa: retorna ela truncada
  if (sig.length === 1) return sig[0].substring(0,8);
  // 2+ palavras: retorna a primeira palavra significativa
  return sig[0].length > 8 ? sig[0].substring(0,7) + '.' : sig[0];
}

function teamBadgeHtml(name) {
  const logo = getTeamLogo(name);
  const initials = teamInitials(name);
  const uid = 'cr_' + Math.random().toString(36).substr(2,8);
  if (logo) {
    // Retorna img com data-attr; onerror aplicado via delegação depois
    return '<img class="time-escudo" id="' + uid + '" src="' + logo + '" alt="" data-ini="' + initials + '" data-crest="1">';
  }
  // Sem logo: span com iniciais, busca crest em background
  setTimeout(async () => {
    const url = await fetchCrestDynamic(name);
    const el = document.getElementById(uid);
    if (el && url) {
      el.src = url;
      el.className = 'time-escudo';
    }
  }, 300);
  return '<span class="time-initials" id="' + uid + '">' + initials + '</span>';
}

// Delegação global: trata erros de carregamento dos escudos
document.addEventListener('error', function(e) {
  const el = e.target;
  if (el && el.classList && el.classList.contains('time-escudo') && el.dataset.crest) {
    const ini = el.dataset.ini || '?';
    const span = document.createElement('span');
    span.className = 'time-initials';
    span.textContent = ini;
    el.replaceWith(span);
  }
}, true);

// ── Jogos AO VIVO (api-sports) ──────────────────────────────────────────────
let _liveInterval = null;

async function carregarAoVivo() {
  const sec = document.getElementById('sec-live');
  const lista = document.getElementById('jogos-live');
  if (!sec || !lista) return;
  try {
    const r = await fetch(`${BASE}/api/bet/live`);
    const d = await r.json();
    const jogos = d.jogos || [];
    if (!jogos.length) {
      sec.style.display = 'none';
      return;
    }
    sec.style.display = '';
    lista.innerHTML = '';
    jogos.forEach(j => {
      const o = j.odds || {};
      const temEmpate = o.draw != null;
      const homeLogo = j.home_logo || getTeamLogo(j.home_team);
      const awayLogo = j.away_logo || getTeamLogo(j.away_team);
      const card = document.createElement('div');
      card.className = 'jogo-card live';
      card.dataset.id = j.id;
      card.innerHTML = `
        <div class="jogo-meta">
          <span class="jogo-liga">${j.league || 'Ao Vivo'}</span>
          <span class="live-tag"><span class="live-dot"></span> ${j.elapsed || 0}'</span>
        </div>
        <div class="jogo-times">
          <div class="time-bloco">
            ${homeLogo
              ? '<img class="time-escudo" src="'+homeLogo+'" alt="" data-ini="'+teamInitials(j.home_team)+'" data-crest="1">'
              : '<span class="time-initials">'+teamInitials(j.home_team)+'</span>'}
            <span class="time-nome">${j.home_team}</span>
          </div>
          <div class="placar-live">
            <span class="placar-num">${j.home_goals ?? 0}</span>
            <span class="placar-sep">-</span>
            <span class="placar-num">${j.away_goals ?? 0}</span>
          </div>
          <div class="time-bloco away">
            ${awayLogo
              ? '<img class="time-escudo" src="'+awayLogo+'" alt="" data-ini="'+teamInitials(j.away_team)+'" data-crest="1">'
              : '<span class="time-initials">'+teamInitials(j.away_team)+'</span>'}
            <span class="time-nome">${j.away_team}</span>
          </div>
        </div>
        <div class="jogo-odds ${!temEmpate ? 'no-draw' : ''}">
          ${o.home != null ? `<div class="odd-btn" id="odd-${j.id}-casa" onclick="selecionarOdd('${j.id}_live','${j.home_team}','Casa',${o.home},'${j.home_team} × ${j.away_team}')">
            <div class="odd-lbl">${teamShortLabel(j.home_team)}</div>
            <div class="odd-val">${Number(o.home).toFixed(2)}</div></div>` : ''}
          ${temEmpate ? `<div class="odd-btn" id="odd-${j.id}-empate" onclick="selecionarOdd('${j.id}_live','Empate','X',${o.draw},'${j.home_team} × ${j.away_team}')">
            <div class="odd-lbl">Empate</div>
            <div class="odd-val">${Number(o.draw).toFixed(2)}</div></div>` : ''}
          ${o.away != null ? `<div class="odd-btn" id="odd-${j.id}-fora" onclick="selecionarOdd('${j.id}_live','${j.away_team}','Fora',${o.away},'${j.home_team} × ${j.away_team}')">
            <div class="odd-lbl">${teamShortLabel(j.away_team)}</div>
            <div class="odd-val">${Number(o.away).toFixed(2)}</div></div>` : ''}
        </div>`;
      lista.appendChild(card);
    });
  } catch(e) {
    const sec = document.getElementById('sec-live');
    if (sec) sec.style.display = 'none';
  }
}

// ── Jogos / Odds ──
async function carregarJogos(sport) {
  const lista = document.getElementById('jogos-lista');
  lista.innerHTML = '<div class="loading-spinner"><div class="spinner"></div> Carregando...</div>';
  try {
    const r = await fetch(`${BASE}/api/bet/jogos?sport=${sport}`);
    const d = await r.json();
    const jogos = (d.jogos || []).filter(j => sport === 'upcoming' || j.sport === sport);
    if (!jogos.length) {
      lista.innerHTML = '<div class="loading-spinner">⚽ Nenhum jogo encontrado para este esporte agora.</div>';
      return;
    }
    lista.innerHTML = '';
    for (const j of jogos) {
      const live = new Date(j.commence_time) < new Date();
      const o = j.odds || {};
      const temEmpate = o.draw != null;
      const card = document.createElement('div');
      card.className = 'jogo-card' + (live ? ' live' : '');
      const hora = j.commence_time
        ? new Date(j.commence_time).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})
        : '';

      // Buscar previsão em background
      let predHtml = '';
      try {
        const pr = await fetch(`${BASE}/api/bet/predictions?fixture=${j.id}`);
        const pd = await pr.json();
        if (pd.success && pd.prediction) {
          const p = pd.prediction;
          const pct = p.percent || {};
          const winPct = pct.away && p.winner === j.away_team ? pct.away
                       : pct.home && p.winner === j.home_team ? pct.home
                       : pct.draw || '';
          const winLabel = p.winner || 'Empate';
          predHtml = `<div class="pred-bar">
            <span class="pred-home" style="width:${pct.home||'33%'}">${pct.home||'33%'}</span>
            <span class="pred-draw" style="width:${pct.draw||'33%'}">${pct.draw||'34%'}</span>
            <span class="pred-away" style="width:${pct.away||'33%'}">${pct.away||'33%'}</span>
          </div>
          <div class="pred-advice">🤖 ${p.advice || 'Favorito: '+winLabel}</div>`;
        }
      } catch(ep) {}

      card.innerHTML = `
        <div class="jogo-meta">
          <span class="jogo-liga">${j.league || j.sport}</span>
          ${live ? '<span class="live-tag">● AO VIVO</span>' : `<span class="jogo-hora">${hora}</span>`}
        </div>
        <div class="jogo-times">
          <div class="time-bloco">
            ${teamBadgeHtml(j.home_team)}
            <span class="time-nome">${j.home_team}</span>
          </div>
          <span class="placar">${live ? '<span style="color:#ef4444;font-size:12px;font-weight:900">AO VIVO</span>' : 'VS'}</span>
          <div class="time-bloco away">
            ${teamBadgeHtml(j.away_team)}
            <span class="time-nome">${j.away_team}</span>
          </div>
        </div>
        ${predHtml}
        <div class="jogo-odds ${!temEmpate ? 'no-draw' : ''}">
          ${o.home != null ? `<div class="odd-btn" id="odd-${j.id}-casa" onclick="selecionarOdd('${j.id}','${j.home_team}','Casa',${o.home},'${j.home_team} × ${j.away_team}')">
            <div class="odd-lbl">${teamShortLabel(j.home_team)}</div>
            <div class="odd-val">${Number(o.home).toFixed(2)}</div></div>` : ''}
          ${temEmpate ? `<div class="odd-btn" id="odd-${j.id}-empate" onclick="selecionarOdd('${j.id}','Empate','X',${o.draw},'${j.home_team} × ${j.away_team}')">
            <div class="odd-lbl">Empate</div>
            <div class="odd-val">${Number(o.draw).toFixed(2)}</div></div>` : ''}
          ${o.away != null ? `<div class="odd-btn" id="odd-${j.id}-fora" onclick="selecionarOdd('${j.id}','${j.away_team}','Fora',${o.away},'${j.home_team} × ${j.away_team}')">
            <div class="odd-lbl">${teamShortLabel(j.away_team)}</div>
            <div class="odd-val">${Number(o.away).toFixed(2)}</div></div>` : ''}
        </div>`;
      lista.appendChild(card);
    }
  } catch(e) {
    lista.innerHTML = '<div class="loading-spinner">⚠️ Erro ao carregar jogos. Tentando novamente...</div>';
    setTimeout(() => carregarJogos(sport), 5000);
  }
}

function trocarSport(el) {
  document.querySelectorAll('.sport-tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  _sportAtual = el.dataset.sport;
  carregarJogos(_sportAtual);
}

// ── Seleção de odds ──
function selecionarOdd(jogoId, time, tipo, odd, jogo) {
  const existing = _selecoes.findIndex(s => s.jogoId === jogoId);
  if (existing >= 0) {
    if (_selecoes[existing].tipo === tipo) {
      // Desmarcar se clicar no mesmo
      _selecoes.splice(existing, 1);
    } else {
      _selecoes[existing] = {jogoId, time, tipo, odd, jogo};
    }
  } else {
    _selecoes.push({jogoId, time, tipo, odd, jogo});
  }
  // Atualizar visual
  ['casa','empate','fora'].forEach(t => {
    const btn = document.getElementById(`odd-${jogoId}-${t}`);
    if (btn) btn.classList.remove('sel');
  });
  const selAtual = _selecoes.find(s => s.jogoId === jogoId);
  if (selAtual) {
    const tipoMap = {'Casa':'casa','X':'empate','Fora':'fora'};
    const btnSel = document.getElementById(`odd-${jogoId}-${tipoMap[selAtual.tipo] || 'casa'}`);
    if (btnSel) btnSel.classList.add('sel');
  }
  renderSlip();
}

function renderSlip() {
  const count = _selecoes.length;
  const totalOdd = _selecoes.reduce((a, s) => a * s.odd, 1);
  const itemsHtml = _selecoes.map(s => `
    <div class="bs-item">
      <div class="bs-item-info">
        <div class="bs-item-jogo">${s.jogo}</div>
        <div class="bs-item-sel">${s.time} (${s.tipo})</div>
        <div class="bs-item-odd">Odd: ${s.odd.toFixed(2)}</div>
      </div>
      <button class="bs-item-remove" onclick="removerSelecao('${s.jogoId}')">×</button>
    </div>`).join('');

  // ── Mobile: barra inferior ──
  const bar    = document.getElementById('bet-slip-bar');
  const cntEl  = document.getElementById('slip-count');
  const oddEl  = document.getElementById('slip-odd-total');
  const itmEl  = document.getElementById('slip-items');
  const body   = document.getElementById('slip-body');
  if (cntEl) cntEl.textContent = count;
  if (oddEl) oddEl.textContent = count ? `Odd total: ${totalOdd.toFixed(2)}` : 'Sem seleções';
  if (itmEl) itmEl.innerHTML = itemsHtml;
  if (!count && bar) { bar.classList.remove('open'); if(body) body.style.display='none'; }

  // ── Desktop: sidebar lateral ──
  const sCount   = document.getElementById('sidebar-count');
  const sOdd     = document.getElementById('sidebar-odd-total');
  const sItems   = document.getElementById('sidebar-items');
  const sEmpty   = document.getElementById('sidebar-empty');
  const sFooter  = document.getElementById('sidebar-footer');
  if (sCount) sCount.textContent = count;
  if (sOdd)   sOdd.textContent = count ? totalOdd.toFixed(2) : '—';
  if (sItems) sItems.innerHTML = count ? itemsHtml : '<div class="sidebar-empty">Clique nas odds para adicionar apostas</div>';
  if (sFooter) sFooter.style.display = count ? 'block' : 'none';

  calcRetorno();
}

function removerSelecao(jogoId) {
  _selecoes = _selecoes.filter(s => s.jogoId !== jogoId);
  renderSlip();
}

function toggleSlip() {
  const bar = document.getElementById('bet-slip-bar');
  const body = document.getElementById('slip-body');
  if (!_selecoes.length) return;
  bar.classList.toggle('open');
  body.style.display = bar.classList.contains('open') ? 'block' : 'none';
}

function setValor(v) {
  const mEl = document.getElementById('slip-valor');
  const sEl = document.getElementById('sidebar-valor');
  if (mEl) mEl.value = v;
  if (sEl) sEl.value = v;
  calcRetorno();
}

function calcRetorno() {
  const mEl = document.getElementById('slip-valor');
  const sEl = document.getElementById('sidebar-valor');
  const v = parseFloat((mEl && mEl.value) || (sEl && sEl.value) || 0) || 0;
  const totalOdd = _selecoes.reduce((a, s) => a * s.odd, 1);
  const ret = _fmt(v * totalOdd);
  const rEl = document.getElementById('slip-retorno');
  const srEl = document.getElementById('sidebar-retorno');
  if (rEl)  rEl.textContent  = ret;
  if (srEl) srEl.textContent = ret;
}

// ── Apostar ──
async function confirmarAposta() {
  if (!_selecoes.length) { toast('Selecione pelo menos uma aposta', 'err'); return; }
  if (!_usuario) { abrirLogin(); return; }

  const mEl  = document.getElementById('slip-valor');
  const sEl  = document.getElementById('sidebar-valor');
  const valor = parseFloat((mEl && mEl.value) || (sEl && sEl.value) || 0) || 0;
  if (valor < 2) { toast('Valor mínimo: R$ 2,00', 'err'); return; }

  const totalOdd = _selecoes.reduce((a, s) => a * s.odd, 1);
  const primeiraSelecao = _selecoes[0];

  const body = {
    usuario_id: _usuario.id,
    jogo_id:    primeiraSelecao.jogoId,
    jogo_nome:  primeiraSelecao.jogo,
    selecao:    _selecoes.map(s => `${s.time} (${s.tipo})`).join(' + '),
    odd:        totalOdd,
    valor:      valor,
  };

  try {
    const r = await fetch(`${BASE}/api/bet/apostar`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (d.success) {
      toast(d.msg || '✅ Aposta registrada!');
      _usuario.saldo = d.saldo;
      localStorage.setItem('bet_usuario', JSON.stringify(_usuario));
      atualizarSaldo();
      _selecoes = [];
      renderSlip();
      if (document.getElementById('slip-valor')) document.getElementById('slip-valor').value = '';
      if (document.getElementById('sidebar-valor')) document.getElementById('sidebar-valor').value = '';
    } else {
      if (d.error && d.error.includes('Saldo insuficiente')) {
        toast('Saldo insuficiente! Deposite para continuar.', 'err');
        setTimeout(() => window.location.href = '/conta', 1500);
      } else {
        toast(d.error || '❌ Erro ao apostar', 'err');
      }
    }
  } catch(e) {
    toast('Erro de conexão. Tente novamente.', 'err');
  }
}

// ── Login ──
function abrirLogin() {
  document.getElementById('login-modal').classList.add('show');
  document.getElementById('modal-cpf').focus();
}

function fecharModal() {
  document.getElementById('login-modal').classList.remove('show');
}

async function fazerLogin() {
  const nome = document.getElementById('modal-nome').value.trim();
  const cpf  = document.getElementById('modal-cpf').value.replace(/\D/g, '');
  if (cpf.length < 11) { toast('CPF inválido', 'err'); return; }
  try {
    const r = await fetch(`${BASE}/api/bet/login`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({nome, cpf}),
    });
    const d = await r.json();
    if (d.success) {
      _usuario = d.usuario;
      localStorage.setItem('bet_usuario', JSON.stringify(_usuario));
      fecharModal();
      atualizarSaldo();
      toast(`✅ Bem-vindo, ${_usuario.nome}!`);
    } else {
      toast(d.error || 'Erro ao entrar', 'err');
    }
  } catch(e) {
    toast('Erro de conexão', 'err');
  }
}

function maskCPF(el) {
  let v = el.value.replace(/\D/g,'').slice(0,11);
  if (v.length > 9) v = v.replace(/(\d{3})(\d{3})(\d{3})(\d{0,2})/,'$1.$2.$3-$4');
  else if (v.length > 6) v = v.replace(/(\d{3})(\d{3})(\d{0,3})/,'$1.$2.$3');
  else if (v.length > 3) v = v.replace(/(\d{3})(\d{0,3})/,'$1.$2');
  el.value = v;
}

function toast(msg, type) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast' + (type === 'err' ? ' err' : '') + ' show';
  setTimeout(() => el.classList.remove('show'), 2600);
}

// Atualizar jogos a cada 60s
setInterval(() => carregarJogos(_sportAtual), 60000);
</script>
</body>
</html>"""


def _page_conta_html():
    return r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="theme-color" content="#0a0a0f">
<title>PaynexBet — Minha Conta</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{--gold:#FFD700;--blue:#3b82f6;--green:#22c55e;--red:#ef4444;--bg:#0a0a0f;--card:#111118}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:#fff;min-height:100vh}
.topbar{background:#0d0d16;border-bottom:1px solid #ffffff0c;padding:0 16px;height:52px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.topbar-logo{font-size:18px;font-weight:900;background:linear-gradient(135deg,#FFD700,#FFA500);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;text-decoration:none}
.btn-sair{background:#1a0a0a;border:1px solid #ef444433;border-radius:8px;padding:6px 12px;color:#f87171;font-size:12px;font-weight:700;cursor:pointer}
.main{max-width:480px;margin:0 auto;padding:16px 14px 40px}

/* SALDO CARD */
.saldo-card{background:linear-gradient(135deg,#0a1a0a,#0d2010);border:1.5px solid #22c55e33;border-radius:18px;padding:20px;text-align:center;margin-bottom:16px}
.saldo-label{font-size:12px;color:#666;text-transform:uppercase;letter-spacing:2px;margin-bottom:8px}
.saldo-valor{font-size:42px;font-weight:900;color:#4ade80;line-height:1}
.saldo-nome{font-size:13px;color:#555;margin-top:8px}

/* TABS */
.tabs{display:flex;gap:0;background:var(--card);border-radius:12px;padding:4px;margin-bottom:16px}
.tab-btn{flex:1;padding:9px;font-size:13px;font-weight:700;border:none;border-radius:9px;cursor:pointer;background:transparent;color:#666;transition:all .2s}
.tab-btn.active{background:#111827;color:#fff}

/* DEPÓSITO */
.deposito-section{display:none}
.deposito-section.show{display:block}
.secao-titulo{font-size:12px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px}
.valor-chips{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px}
.valor-chip{background:var(--card);border:1px solid #3b82f633;border-radius:8px;padding:8px 16px;font-size:14px;font-weight:700;color:var(--blue);cursor:pointer;transition:all .15s}
.valor-chip:hover,.valor-chip.sel{background:#1e3a8a;border-color:var(--blue);color:#fff}
.input-custom{width:100%;background:#0a0a14;border:1px solid #3b82f633;border-radius:10px;padding:12px;color:#fff;font-size:16px;margin-bottom:10px}
.btn-gerar{width:100%;background:linear-gradient(135deg,#1e3a8a,#3b82f6);border:none;border-radius:10px;padding:14px;color:#fff;font-size:15px;font-weight:900;cursor:pointer;margin-bottom:12px}

/* QR CODE */
.qr-box{background:#0d1120;border:1px solid #3b82f633;border-radius:14px;padding:16px;text-align:center;display:none}
.qr-box.show{display:block}
.qr-img{width:180px;height:180px;margin:0 auto 12px;border-radius:10px;background:#fff;display:flex;align-items:center;justify-content:center;overflow:hidden}
.qr-img img{width:100%;height:100%}
.qr-code-text{background:#0a0a14;border:1px solid #3b82f622;border-radius:8px;padding:10px;font-size:11px;color:#888;word-break:break-all;text-align:left;margin-bottom:10px;font-family:monospace;max-height:60px;overflow:hidden}
.btn-copiar{width:100%;background:#111827;border:1px solid #3b82f633;border-radius:8px;padding:10px;color:var(--blue);font-size:13px;font-weight:700;cursor:pointer}
.aguardando{font-size:12px;color:#22c55e;margin-top:10px;display:flex;align-items:center;justify-content:center;gap:6px}
.dot-pulse{width:6px;height:6px;background:#22c55e;border-radius:50%;animation:blink 1.2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}

/* APOSTAS */
.apostas-section{display:none}
.apostas-section.show{display:block}
.aposta-item{background:var(--card);border:1px solid #ffffff0c;border-radius:12px;padding:12px 14px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:flex-start}
.aposta-left{flex:1}
.aposta-jogo{font-size:11px;color:#666;margin-bottom:2px}
.aposta-sel{font-size:13px;font-weight:700;color:#fff}
.aposta-meta{font-size:11px;color:#888;margin-top:3px}
.aposta-right{text-align:right;flex-shrink:0;padding-left:10px}
.aposta-valor{font-size:13px;font-weight:700;color:#fff}
.aposta-status{font-size:11px;font-weight:700;border-radius:6px;padding:2px 8px;margin-top:4px;display:inline-block}
.st-pendente{background:#f59e0b18;color:#fbbf24;border:1px solid #f59e0b33}
.st-ganha{background:#22c55e18;color:#4ade80;border:1px solid #22c55e33}
.st-perdida{background:#ef444418;color:#f87171;border:1px solid #ef444433}

/* SAQUE */
.saque-section{display:none}
.saque-section.show{display:block}
.form-label{font-size:11px;color:#666;margin-bottom:4px;display:block}
.form-select{width:100%;background:#0a0a14;border:1px solid #ffffff1a;border-radius:8px;padding:10px;color:#fff;font-size:14px;margin-bottom:10px}
.btn-sacar{width:100%;background:linear-gradient(135deg,#1a6b3c,#22c55e);border:none;border-radius:10px;padding:14px;color:#fff;font-size:15px;font-weight:900;cursor:pointer}

/* LOGIN */
.login-card{background:var(--card);border:1px solid #3b82f633;border-radius:18px;padding:24px 20px;margin-top:20px}
.login-title{font-size:18px;font-weight:900;margin-bottom:6px}
.login-sub{font-size:13px;color:#666;margin-bottom:18px;line-height:1.5}
.login-input{width:100%;background:#0a0a14;border:1px solid #3b82f633;border-radius:10px;padding:12px;color:#fff;font-size:15px;margin-bottom:10px}
.login-btn{width:100%;background:linear-gradient(135deg,#1e3a8a,#3b82f6);border:none;border-radius:10px;padding:14px;color:#fff;font-size:15px;font-weight:900;cursor:pointer}

/* TOAST */
.toast{position:fixed;top:20px;left:50%;transform:translateX(-50%);background:#1a2e1a;border:1px solid #22c55e44;border-radius:10px;padding:10px 18px;font-size:13px;font-weight:600;color:#4ade80;z-index:9999;display:none;white-space:nowrap}
.toast.err{background:#2e1a1a;border-color:#ef444444;color:#f87171}
.toast.show{display:block;animation:fadeInOut 2.5s ease}
@keyframes fadeInOut{0%{opacity:0;transform:translateX(-50%) translateY(-10px)}15%,85%{opacity:1;transform:translateX(-50%) translateY(0)}100%{opacity:0}}
</style>
</head>
<body>

<header class="topbar">
  <a href="/" class="topbar-logo">🎰 PaynexBet</a>
  <div style="display:flex;gap:8px;align-items:center">
    <a href="/apostas" style="background:#1e3a8a;border:none;border-radius:8px;padding:6px 12px;color:#fff;font-size:12px;font-weight:700;text-decoration:none">⚽ Apostas</a>
    <button class="btn-sair" onclick="sair()">🚪 Sair</button>
  </div>
</header>

<div class="main" id="main-logado" style="display:none">
  <!-- Saldo -->
  <div class="saldo-card">
    <div class="saldo-label">Seu saldo</div>
    <div class="saldo-valor" id="ct-saldo">R$ 0,00</div>
    <div class="saldo-nome" id="ct-nome">—</div>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab-btn active" onclick="trocarTab('deposito',this)">💳 Depositar</button>
    <button class="tab-btn" onclick="trocarTab('apostas',this)">🎯 Apostas</button>
    <button class="tab-btn" onclick="trocarTab('saque',this)">💸 Sacar</button>
  </div>

  <!-- DEPÓSITO -->
  <div class="deposito-section show" id="tab-deposito">
    <div class="secao-titulo">Quanto deseja depositar?</div>
    <div class="valor-chips">
      <div class="valor-chip" onclick="selecionarValorDep(10,this)">R$10</div>
      <div class="valor-chip" onclick="selecionarValorDep(20,this)">R$20</div>
      <div class="valor-chip" onclick="selecionarValorDep(50,this)">R$50</div>
      <div class="valor-chip" onclick="selecionarValorDep(100,this)">R$100</div>
      <div class="valor-chip" onclick="selecionarValorDep(200,this)">R$200</div>
    </div>
    <input class="input-custom" type="number" id="dep-valor" placeholder="Outro valor (mín. R$5)" min="5">
    <button class="btn-gerar" onclick="gerarDeposito()">🔑 Gerar QR Code PIX</button>

    <div class="qr-box" id="qr-box">
      <div style="font-size:13px;color:#888;margin-bottom:12px">Escaneie o QR Code ou copie o código</div>
      <div class="qr-img" id="qr-img-box"></div>
      <div class="qr-code-text" id="qr-code-text"></div>
      <button class="btn-copiar" onclick="copiarQR()">📋 Copiar código PIX</button>
      <div class="aguardando"><span class="dot-pulse"></span> Aguardando pagamento...</div>
    </div>
  </div>

  <!-- APOSTAS -->
  <div class="apostas-section" id="tab-apostas">
    <div class="secao-titulo">Minhas apostas</div>
    <div id="apostas-lista"><div style="text-align:center;color:#555;padding:20px">Carregando...</div></div>
  </div>

  <!-- SAQUE -->
  <div class="saque-section" id="tab-saque">
    <div class="secao-titulo">Saque via PIX</div>
    <label class="form-label">Valor a sacar (mín. R$20)</label>
    <input class="input-custom" type="number" id="saq-valor" placeholder="R$ valor" min="20">
    <label class="form-label">Tipo de chave PIX</label>
    <select class="form-select" id="saq-tipo">
      <option value="cpf">CPF</option>
      <option value="email">E-mail</option>
      <option value="telefone">Telefone</option>
      <option value="aleatoria">Chave aleatória</option>
    </select>
    <label class="form-label">Chave PIX</label>
    <input class="input-custom" type="text" id="saq-chave" placeholder="Sua chave PIX">
    <button class="btn-sacar" onclick="realizarSaque()">💸 Solicitar Saque</button>
  </div>
</div>

<!-- LOGIN -->
<div class="main" id="main-login" style="display:block">
  <div class="login-card">
    <div class="login-title">👤 Entrar na conta</div>
    <div class="login-sub">Digite seu CPF para acessar. Se for novo, sua conta é criada na hora!</div>
    <input class="login-input" id="lc-nome" type="text" placeholder="Seu nome (opcional)">
    <input class="login-input" id="lc-cpf" type="tel" placeholder="CPF (somente números)" maxlength="14" oninput="maskCPF(this)">
    <button class="login-btn" onclick="fazerLogin()">🚀 Entrar / Cadastrar</button>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const BASE = '';
const _fmt = n => 'R$ ' + Number(n||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
let _usuario = JSON.parse(localStorage.getItem('bet_usuario') || 'null');
let _depValor = 0;
let _qrCode = '';
let _pollInterval = null;

document.addEventListener('DOMContentLoaded', () => {
  if (_usuario) {
    mostrarContaLogada();
    carregarDados();
  }
});

function mostrarContaLogada() {
  document.getElementById('main-logado').style.display = 'block';
  document.getElementById('main-login').style.display = 'none';
  document.getElementById('ct-saldo').textContent = _fmt(_usuario.saldo);
  document.getElementById('ct-nome').textContent = `👤 ${_usuario.nome} · CPF: ${'•'.repeat(7)}${(_usuario.cpf||'').slice(-4)}`;
}

async function carregarDados() {
  if (!_usuario) return;
  try {
    const r = await fetch(`${BASE}/api/bet/saldo/${_usuario.id}`);
    const d = await r.json();
    if (d.success) {
      _usuario.saldo = d.usuario.saldo;
      localStorage.setItem('bet_usuario', JSON.stringify(_usuario));
      document.getElementById('ct-saldo').textContent = _fmt(_usuario.saldo);
      // Renderizar apostas
      const lista = document.getElementById('apostas-lista');
      if (!d.apostas.length) {
        lista.innerHTML = '<div style="text-align:center;color:#555;padding:20px">Nenhuma aposta ainda. <a href="/apostas" style="color:#3b82f6">Aposte agora!</a></div>';
      } else {
        lista.innerHTML = d.apostas.map(a => `
          <div class="aposta-item">
            <div class="aposta-left">
              <div class="aposta-jogo">${a.jogo}</div>
              <div class="aposta-sel">${a.selecao}</div>
              <div class="aposta-meta">Odd: ${a.odd.toFixed(2)} · ${a.data}</div>
            </div>
            <div class="aposta-right">
              <div class="aposta-valor">R$ ${a.valor.toFixed(2)}</div>
              <div class="aposta-status st-${a.status}">${a.status.toUpperCase()}</div>
              ${a.resultado === 'ganha' ? `<div style="font-size:11px;color:#4ade80;margin-top:2px">+R$ ${a.retorno.toFixed(2)}</div>` : ''}
            </div>
          </div>`).join('');
      }
    }
  } catch(e) {}
}

function trocarTab(tab, btn) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.deposito-section,.apostas-section,.saque-section').forEach(s => s.classList.remove('show'));
  document.getElementById('tab-'+tab).classList.add('show');
  if (tab === 'apostas') carregarDados();
}

function selecionarValorDep(v, el) {
  document.querySelectorAll('.valor-chip').forEach(c => c.classList.remove('sel'));
  el.classList.add('sel');
  document.getElementById('dep-valor').value = v;
  _depValor = v;
}

async function gerarDeposito() {
  if (!_usuario) { toast('Faça login primeiro', 'err'); return; }
  const valor = parseFloat(document.getElementById('dep-valor').value) || _depValor;
  if (valor < 5) { toast('Valor mínimo R$5,00', 'err'); return; }

  const btn = document.querySelector('.btn-gerar');
  btn.disabled = true; btn.textContent = '⏳ Gerando...';

  try {
    const r = await fetch(`${BASE}/api/bet/deposito`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        usuario_id: _usuario.id,
        nome: _usuario.nome,
        cpf: _usuario.cpf,
        valor: valor
      }),
    });
    const d = await r.json();
    if (d.success) {
      _qrCode = d.qrCode;
      document.getElementById('qr-code-text').textContent = d.qrCode;
      const imgBox = document.getElementById('qr-img-box');
      if (d.qrImage) {
        imgBox.innerHTML = `<img src="data:image/png;base64,${d.qrImage}" alt="QR Code">`;
      } else {
        imgBox.innerHTML = `<div style="color:#555;font-size:12px;padding:20px">QR Code gerado.<br>Use o código abaixo.</div>`;
      }
      document.getElementById('qr-box').classList.add('show');
      // Polling para verificar pagamento
      if (_pollInterval) clearInterval(_pollInterval);
      _pollInterval = setInterval(() => verificarPagamento(), 5000);
    } else {
      toast(d.error || 'Erro ao gerar QR Code', 'err');
    }
  } catch(e) {
    toast('Erro de conexão', 'err');
  } finally {
    btn.disabled = false; btn.textContent = '🔑 Gerar QR Code PIX';
  }
}

function copiarQR() {
  navigator.clipboard.writeText(_qrCode).then(() => toast('✅ Código PIX copiado!')).catch(() => toast('Erro ao copiar', 'err'));
}

async function verificarPagamento() {
  if (!_usuario) return;
  try {
    const r = await fetch(`${BASE}/api/bet/saldo/${_usuario.id}`);
    const d = await r.json();
    if (d.success && d.usuario.saldo > _usuario.saldo) {
      clearInterval(_pollInterval);
      _usuario.saldo = d.usuario.saldo;
      localStorage.setItem('bet_usuario', JSON.stringify(_usuario));
      document.getElementById('ct-saldo').textContent = _fmt(_usuario.saldo);
      document.getElementById('qr-box').classList.remove('show');
      toast(`✅ Depósito confirmado! Saldo: ${_fmt(_usuario.saldo)}`);
    }
  } catch(e) {}
}

async function realizarSaque() {
  if (!_usuario) { toast('Faça login', 'err'); return; }
  const valor = parseFloat(document.getElementById('saq-valor').value) || 0;
  const tipo  = document.getElementById('saq-tipo').value;
  const chave = document.getElementById('saq-chave').value.trim();
  if (valor < 20) { toast('Valor mínimo R$20', 'err'); return; }
  if (!chave) { toast('Informe a chave PIX', 'err'); return; }

  try {
    const r = await fetch(`${BASE}/api/bet/sacar`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({usuario_id: _usuario.id, valor, chave_pix: chave, tipo_chave: tipo}),
    });
    const d = await r.json();
    if (d.success) {
      toast(d.msg || '✅ Saque solicitado!');
      carregarDados();
    } else {
      toast(d.error || 'Erro no saque', 'err');
    }
  } catch(e) {
    toast('Erro de conexão', 'err');
  }
}

async function fazerLogin() {
  const nome = document.getElementById('lc-nome').value.trim();
  const cpf  = document.getElementById('lc-cpf').value.replace(/\D/g,'');
  if (cpf.length < 11) { toast('CPF inválido (11 dígitos)', 'err'); return; }
  try {
    const r = await fetch(`${BASE}/api/bet/login`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({nome, cpf}),
    });
    const d = await r.json();
    if (d.success) {
      _usuario = d.usuario;
      localStorage.setItem('bet_usuario', JSON.stringify(_usuario));
      mostrarContaLogada();
      carregarDados();
      toast(`✅ Bem-vindo, ${_usuario.nome}!`);
    } else {
      toast(d.error || 'Erro ao entrar', 'err');
    }
  } catch(e) {
    toast('Erro de conexão', 'err');
  }
}

function sair() {
  if (!confirm('Sair da conta?')) return;
  localStorage.removeItem('bet_usuario');
  location.reload();
}

function maskCPF(el) {
  let v = el.value.replace(/\D/g,'').slice(0,11);
  if (v.length > 9) v = v.replace(/(\d{3})(\d{3})(\d{3})(\d{0,2})/,'$1.$2.$3-$4');
  else if (v.length > 6) v = v.replace(/(\d{3})(\d{3})(\d{0,3})/,'$1.$2.$3');
  else if (v.length > 3) v = v.replace(/(\d{3})(\d{0,3})/,'$1.$2');
  el.value = v;
}

function toast(msg, type) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast' + (type === 'err' ? ' err' : '') + ' show';
  setTimeout(() => el.classList.remove('show'), 2600);
}
</script>
</body>
</html>"""


async def route_bet_config(request):
    """POST /api/bet/config — salva ODDS_API_KEY e SUITPAY_CI/CS no PostgreSQL
       Protegido por X-PaynexBet-Secret
    """
    secret = request.headers.get('X-PaynexBet-Secret','') or request.headers.get('x-paynexbet-secret','')
    if secret != WEBHOOK_SECRET:
        return web.json_response({'ok': False, 'error': 'Não autorizado'}, status=403)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({'ok': False, 'error': 'JSON inválido'}, status=400)

    if not DATABASE_URL:
        return web.json_response({'ok': False, 'error': 'DATABASE_URL não configurada'})

    saved = []
    try:
        import psycopg2 as _pg2
        conn = _pg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        mapping = {
            'odds_api_key':  'bet_odds_api_key',
            'suitpay_ci':    'bet_suitpay_ci',
            'suitpay_cs':    'bet_suitpay_cs',
        }
        for field, chave in mapping.items():
            val = (body.get(field) or '').strip()
            if val:
                cur.execute("""
                    INSERT INTO configuracoes (chave, valor) VALUES (%s,%s)
                    ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor
                """, (chave, val))
                saved.append(field)
        conn.commit(); cur.close(); conn.close()
        return web.json_response({'ok': True, 'saved': saved,
            'msg': f'✅ {len(saved)} chave(s) salvas no DB. Ativas imediatamente sem redeploy.'})
    except Exception as e:
        return web.json_response({'ok': False, 'error': str(e)})


# ── Cache de crests football-data.org ──
# Pré-populado com crests oficiais para resposta imediata (sem chamada à API)
_crest_cache_py = {
    # Brasileirão Série A
    'EC Bahia': 'https://crests.football-data.org/1777.png',
    'Bahia': 'https://crests.football-data.org/1777.png',
    'Santos FC': 'https://crests.football-data.org/6685.png',
    'Santos': 'https://crests.football-data.org/6685.png',
    'Botafogo FR': 'https://crests.football-data.org/1770.png',
    'Botafogo': 'https://crests.football-data.org/1770.png',
    'SC Internacional': 'https://crests.football-data.org/6684.png',
    'Internacional': 'https://crests.football-data.org/6684.png',
    'Cruzeiro EC': 'https://crests.football-data.org/1771.png',
    'Cruzeiro': 'https://crests.football-data.org/1771.png',
    'São Paulo FC': 'https://crests.football-data.org/1776.png',
    'Sao Paulo': 'https://crests.football-data.org/1776.png',
    'São Paulo': 'https://crests.football-data.org/1776.png',
    'Mirassol FC': 'https://crests.football-data.org/4364.png',
    'Mirassol': 'https://crests.football-data.org/4364.png',
    'CR Flamengo': 'https://crests.football-data.org/1783.png',
    'Flamengo': 'https://crests.football-data.org/1783.png',
    'Fluminense FC': 'https://crests.football-data.org/1765.png',
    'Fluminense': 'https://crests.football-data.org/1765.png',
    'SE Palmeiras': 'https://crests.football-data.org/1769.png',
    'Palmeiras': 'https://crests.football-data.org/1769.png',
    'CA Mineiro': 'https://crests.football-data.org/1766.png',
    'Atletico Mineiro': 'https://crests.football-data.org/1766.png',
    'Atlético Mineiro': 'https://crests.football-data.org/1766.png',
    'SC Corinthians Paulista': 'https://crests.football-data.org/1779.png',
    'Corinthians': 'https://crests.football-data.org/1779.png',
    'Grêmio FBPA': 'https://crests.football-data.org/1767.png',
    'Gremio': 'https://crests.football-data.org/1767.png',
    'Grêmio': 'https://crests.football-data.org/1767.png',
    'CR Vasco da Gama': 'https://crests.football-data.org/1780.png',
    'Vasco da Gama': 'https://crests.football-data.org/1780.png',
    'Vasco': 'https://crests.football-data.org/1780.png',
    'CA Paranaense': 'https://crests.football-data.org/1768.png',
    'Athletico Paranaense': 'https://crests.football-data.org/1768.png',
    'RB Bragantino': 'https://crests.football-data.org/4286.png',
    'Red Bull Bragantino': 'https://crests.football-data.org/4286.png',
    'Bragantino': 'https://crests.football-data.org/4286.png',
    'EC Vitória': 'https://crests.football-data.org/1782.png',
    'Vitória': 'https://crests.football-data.org/1782.png',
    'Fortaleza EC': 'https://crests.football-data.org/7482.png',
    'Fortaleza': 'https://crests.football-data.org/7482.png',
    'Sport Club do Recife': 'https://crests.football-data.org/1781.png',
    'Sport Recife': 'https://crests.football-data.org/1781.png',
    'Sport': 'https://crests.football-data.org/1781.png',
    'Ceará SC': 'https://crests.football-data.org/4316.png',
    'Ceará': 'https://crests.football-data.org/4316.png',
    # Copa Libertadores
    'CA Boca Juniors': 'https://crests.football-data.org/2061.png',
    'Boca Juniors': 'https://crests.football-data.org/2061.png',
    'CA Peñarol': 'https://crests.football-data.org/5184.png',
    'Penarol': 'https://crests.football-data.org/5184.png',
    'Club Nacional de Football': 'https://crests.football-data.org/7055.png',
    'Nacional': 'https://crests.football-data.org/7055.png',
    'LDU de Quito': 'https://crests.football-data.org/4528.png',
    'LDU Quito': 'https://crests.football-data.org/4528.png',
    'CA Lanús': 'https://crests.football-data.org/2066.png',
    'Lanus': 'https://crests.football-data.org/2066.png',
    'CA Rosario Central': 'https://crests.football-data.org/2070.png',
    'Rosario Central': 'https://crests.football-data.org/2070.png',
    'Estudiantes de La Plata': 'https://crests.football-data.org/2051.png',
    'Barcelona SC': 'https://crests.football-data.org/4520.png',
    'CAR Independiente del Valle': 'https://crests.football-data.org/6989.png',
    'Independiente del Valle': 'https://crests.football-data.org/6989.png',
    'Club Bolívar': 'https://crests.football-data.org/4261.png',
    'Bolivar': 'https://crests.football-data.org/4261.png',
    'Club Guaraní': 'https://crests.football-data.org/7868.png',
    'Club Cerro Porteño': 'https://crests.football-data.org/9373.png',
    # Champions League
    'Real Madrid CF': 'https://crests.football-data.org/86.png',
    'Real Madrid': 'https://crests.football-data.org/86.png',
    'FC Barcelona': 'https://crests.football-data.org/81.png',
    'Barcelona': 'https://crests.football-data.org/81.png',
    'FC Bayern München': 'https://crests.football-data.org/5.png',
    'Bayern Munich': 'https://crests.football-data.org/5.png',
    'Bayern München': 'https://crests.football-data.org/5.png',
    'Paris Saint-Germain FC': 'https://crests.football-data.org/524.png',
    'Paris Saint Germain': 'https://crests.football-data.org/524.png',
    'PSG': 'https://crests.football-data.org/524.png',
    'Club Atlético de Madrid': 'https://crests.football-data.org/78.png',
    'Atletico Madrid': 'https://crests.football-data.org/78.png',
    'Atlético Madrid': 'https://crests.football-data.org/78.png',
    'Chelsea FC': 'https://crests.football-data.org/61.png',
    'Chelsea': 'https://crests.football-data.org/61.png',
    'Arsenal FC': 'https://crests.football-data.org/57.png',
    'Arsenal': 'https://crests.football-data.org/57.png',
    'Liverpool FC': 'https://crests.football-data.org/64.png',
    'Liverpool': 'https://crests.football-data.org/64.png',
    'Manchester City FC': 'https://crests.football-data.org/65.png',
    'Manchester City': 'https://crests.football-data.org/65.png',
    'Manchester United FC': 'https://crests.football-data.org/66.png',
    'Manchester United': 'https://crests.football-data.org/66.png',
    'Tottenham Hotspur FC': 'https://crests.football-data.org/73.png',
    'Tottenham': 'https://crests.football-data.org/73.png',
    'Juventus FC': 'https://crests.football-data.org/109.png',
    'Juventus': 'https://crests.football-data.org/109.png',
    'FC Internazionale Milano': 'https://crests.football-data.org/108.png',
    'Inter Milan': 'https://crests.football-data.org/108.png',
    'Internazionale': 'https://crests.football-data.org/108.png',
    'Borussia Dortmund': 'https://crests.football-data.org/4.png',
    'Bayer 04 Leverkusen': 'https://crests.football-data.org/3.png',
    'Leverkusen': 'https://crests.football-data.org/3.png',
    'SSC Napoli': 'https://crests.football-data.org/113.png',
    'Napoli': 'https://crests.football-data.org/113.png',
    'AFC Ajax': 'https://crests.football-data.org/678.png',
    'Ajax': 'https://crests.football-data.org/678.png',
    'Sporting CP': 'https://crests.football-data.org/498.png',
    'Sport Lisboa e Benfica': 'https://crests.football-data.org/1903.png',
    'Benfica': 'https://crests.football-data.org/1903.png',
    'Galatasaray SK': 'https://crests.football-data.org/610.png',
    'Galatasaray': 'https://crests.football-data.org/610.png',
    'Olympique de Marseille': 'https://crests.football-data.org/516.png',
    'Marseille': 'https://crests.football-data.org/516.png',
    'AS Monaco FC': 'https://crests.football-data.org/548.png',
    'Monaco': 'https://crests.football-data.org/548.png',
    'Atalanta BC': 'https://crests.football-data.org/102.png',
    'Atalanta': 'https://crests.football-data.org/102.png',
    'Eintracht Frankfurt': 'https://crests.football-data.org/19.png',
    # Premier League
    'Aston Villa FC': 'https://crests.football-data.org/58.png',
    'Aston Villa': 'https://crests.football-data.org/58.png',
    'Everton FC': 'https://crests.football-data.org/62.png',
    'Everton': 'https://crests.football-data.org/62.png',
    'Fulham FC': 'https://crests.football-data.org/63.png',
    'Fulham': 'https://crests.football-data.org/63.png',
    'Newcastle United FC': 'https://crests.football-data.org/67.png',
    'Newcastle': 'https://crests.football-data.org/67.png',
    'Nottingham Forest FC': 'https://crests.football-data.org/351.png',
    'Nottingham Forest': 'https://crests.football-data.org/351.png',
    'Crystal Palace FC': 'https://crests.football-data.org/354.png',
    'Crystal Palace': 'https://crests.football-data.org/354.png',
    'Brighton & Hove Albion FC': 'https://crests.football-data.org/397.png',
    'Brighton': 'https://crests.football-data.org/397.png',
    'West Ham United FC': 'https://crests.football-data.org/563.png',
    'West Ham': 'https://crests.football-data.org/563.png',
    'Brentford FC': 'https://crests.football-data.org/402.png',
    'Brentford': 'https://crests.football-data.org/402.png',
    'Wolverhampton Wanderers FC': 'https://crests.football-data.org/76.png',
    'Wolves': 'https://crests.football-data.org/76.png',
}

def _get_crest_static(nome: str):
    """Busca crest no mapa estático com matching normalizado."""
    import unicodedata
    if not nome:
        return None
    # 1. Busca exata
    if nome in _crest_cache_py:
        return _crest_cache_py[nome]
    # 2. Busca normalizada (sem acentos, case-insensitive)
    def norm(s):
        return unicodedata.normalize('NFD', s.lower()).encode('ascii','ignore').decode()
    n = norm(nome)
    for key, url in _crest_cache_py.items():
        k = norm(key)
        if n == k or n in k or k in n:
            _crest_cache_py[nome] = url  # Cachear para próxima vez
            return url
    return None


# ═══════════════════════════════════════════════════════════════════════════
# ███  PÁGINAS LEGAIS: Termos, Privacidade, Jogo Responsável, Suporte  ███
# ═══════════════════════════════════════════════════════════════════════════

_LEGAL_CSS = """
<style>
:root{--bg:#0a0a14;--bg2:#0f0f1e;--bg3:#141428;--card:#12122a;--border:#ffffff10;--gold:#f5a623;--gold2:#ffd369;--blue:#3b82f6;--blue2:#60a5fa;--green:#22c55e;--text:#f1f5f9;--muted:#94a3b8;--radius:14px}
*{margin:0;padding:0;box-sizing:border-box}
html{scroll-behavior:smooth}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}
.topbar{position:sticky;top:0;z-index:100;background:rgba(10,10,20,.92);backdrop-filter:blur(16px);border-bottom:1px solid var(--border);padding:0 24px;height:60px;display:flex;align-items:center;justify-content:space-between}
.topbar-brand{display:flex;align-items:center;gap:10px;font-size:20px;font-weight:900;background:linear-gradient(90deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;text-decoration:none}
.topbar-back{padding:8px 16px;border-radius:8px;font-size:14px;font-weight:600;background:transparent;border:1.5px solid var(--border);color:var(--muted);cursor:pointer;text-decoration:none;display:flex;align-items:center;gap:6px;transition:all .2s}
.topbar-back:hover{border-color:var(--blue);color:var(--blue2)}
.legal-hero{padding:56px 24px 40px;text-align:center;background:linear-gradient(180deg,#0f0f2a 0%,var(--bg) 100%);border-bottom:1px solid var(--border)}
.legal-icon{font-size:52px;margin-bottom:16px;display:block}
.legal-title{font-size:clamp(26px,5vw,42px);font-weight:900;margin-bottom:10px;background:linear-gradient(90deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.legal-sub{font-size:15px;color:var(--muted)}
.legal-date{display:inline-block;margin-top:12px;background:var(--bg3);border:1px solid var(--border);padding:4px 14px;border-radius:99px;font-size:12px;color:var(--muted)}
.legal-body{max-width:820px;margin:0 auto;padding:56px 24px 80px}
.legal-toc{background:var(--bg2);border:1px solid var(--border);border-radius:14px;padding:22px 24px;margin-bottom:48px}
.legal-toc-title{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:14px}
.legal-toc ol{padding-left:20px;display:flex;flex-direction:column;gap:6px}
.legal-toc a{color:var(--blue2);font-size:14px;text-decoration:none}
.legal-toc a:hover{text-decoration:underline}
.legal-section{margin-bottom:48px;scroll-margin-top:80px}
.legal-section h2{font-size:20px;font-weight:800;margin-bottom:16px;padding-bottom:10px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px}
.legal-section h2 .num{width:30px;height:30px;border-radius:8px;background:linear-gradient(135deg,var(--gold),#e8950a);color:#1a0a00;font-size:13px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.legal-section p{font-size:15px;color:#cbd5e1;line-height:1.8;margin-bottom:12px}
.legal-section ul,.legal-section ol{padding-left:20px;display:flex;flex-direction:column;gap:8px;margin-bottom:12px}
.legal-section li{font-size:15px;color:#cbd5e1;line-height:1.7}
.legal-section li strong{color:var(--text)}
.legal-section .highlight{background:var(--bg2);border-left:3px solid var(--gold);border-radius:0 10px 10px 0;padding:14px 18px;margin:16px 0;font-size:14px;color:#cbd5e1;line-height:1.7}
.legal-section .highlight.blue{border-color:var(--blue)}
.legal-section .highlight.green{border-color:var(--green)}
.legal-section .highlight.red{border-color:#ef4444}
.legal-footer{background:var(--bg2);border-top:1px solid var(--border);padding:40px 24px;text-align:center}
.legal-footer-links{display:flex;gap:16px;justify-content:center;flex-wrap:wrap;margin-bottom:16px}
.legal-footer-links a{color:var(--muted);font-size:13px;text-decoration:none}
.legal-footer-links a:hover{color:var(--text)}
.legal-footer-copy{font-size:12px;color:#64748b}
@media(max-width:640px){.legal-body{padding:36px 16px 60px}}
</style>
"""

_LEGAL_NAV = """
<nav class="topbar">
  <a href="/" class="topbar-brand">&#127922; PaynexBet</a>
  <a href="/" class="topbar-back">&#8592; Voltar ao in&iacute;cio</a>
</nav>
"""

_LEGAL_FOOTER = """
<footer class="legal-footer">
  <div class="legal-footer-links">
    <a href="/termos">Termos de uso</a>
    <a href="/privacidade">Pol&iacute;tica de privacidade</a>
    <a href="/responsavel">Jogo respons&aacute;vel</a>
    <a href="/suporte">Suporte</a>
  </div>
  <div class="legal-footer-copy">&copy; 2025 PaynexBet &mdash; Todos os direitos reservados.</div>
</footer>
"""

def _page_termos():
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Termos de Uso &mdash; PaynexBet</title>
{_LEGAL_CSS}
</head>
<body>
{_LEGAL_NAV}
<div class="legal-hero">
  <span class="legal-icon">&#128203;</span>
  <h1 class="legal-title">Termos de Uso</h1>
  <p class="legal-sub">Leia com aten&ccedil;&atilde;o antes de utilizar a plataforma PaynexBet.</p>
  <span class="legal-date">Última atualização: 24 de abril de 2025</span>
</div>
<div class="legal-body">
  <div class="legal-toc">
    <div class="legal-toc-title">&#128209; &Iacute;ndice</div>
    <ol>
      <li><a href="#aceitacao">Aceita&ccedil;&atilde;o dos Termos</a></li>
      <li><a href="#elegibilidade">Elegibilidade e Cadastro</a></li>
      <li><a href="#conta">Sua Conta</a></li>
      <li><a href="#depositos">Dep&oacute;sitos e Saques</a></li>
      <li><a href="#apostas">Apostas Esportivas</a></li>
      <li><a href="#sorteios">Sorteios</a></li>
      <li><a href="#proibicoes">Condutas Proibidas</a></li>
      <li><a href="#responsabilidade">Limita&ccedil;&atilde;o de Responsabilidade</a></li>
      <li><a href="#suspensao">Suspens&atilde;o e Encerramento</a></li>
      <li><a href="#alteracoes">Altera&ccedil;&otilde;es nos Termos</a></li>
      <li><a href="#foro">Foro e Legisla&ccedil;&atilde;o</a></li>
    </ol>
  </div>

  <div class="legal-section" id="aceitacao">
    <h2><span class="num">1</span> Aceita&ccedil;&atilde;o dos Termos</h2>
    <p>Ao acessar, se cadastrar ou utilizar qualquer funcionalidade da plataforma PaynexBet (<strong>paynexbet.com</strong>), voc&ecirc; declara ter lido, compreendido e concordado integralmente com estes Termos de Uso e com nossa Pol&iacute;tica de Privacidade.</p>
    <p>Caso n&atilde;o concorde com qualquer disposi&ccedil;&atilde;o aqui apresentada, solicitamos que se abstenha de utilizar nossos servi&ccedil;os.</p>
    <div class="highlight">&#9888;&#65039; <strong>Importante:</strong> O uso da plataforma &eacute; restrito a maiores de 18 anos. Ao se cadastrar, voc&ecirc; confirma ter idade m&iacute;nima legal para participar de jogos e apostas.</div>
  </div>

  <div class="legal-section" id="elegibilidade">
    <h2><span class="num">2</span> Elegibilidade e Cadastro</h2>
    <p>Para utilizar a PaynexBet, voc&ecirc; deve:</p>
    <ul>
      <li>Ter no m&iacute;nimo <strong>18 anos de idade</strong></li>
      <li>Ser residente no territ&oacute;rio brasileiro</li>
      <li>Possuir CPF v&aacute;lido e regularmente emitido pela Receita Federal</li>
      <li>N&atilde;o estar proibido de participar de atividades de apostas por lei ou ordem judicial</li>
      <li>N&atilde;o possuir conta previamente encerrada por viola&ccedil;&atilde;o destes Termos</li>
    </ul>
    <p>O cadastro &eacute; pessoal e intransfer&iacute;vel. &Eacute; permitida apenas <strong>uma conta por CPF</strong>. A cria&ccedil;&atilde;o de m&uacute;ltiplas contas resultar&aacute; no bloqueio permanente de todas elas e no cancelamento de eventuais pr&ecirc;mios.</p>
  </div>

  <div class="legal-section" id="conta">
    <h2><span class="num">3</span> Sua Conta</h2>
    <p>Voc&ecirc; &eacute; inteiramente respons&aacute;vel por:</p>
    <ul>
      <li>Manter a confidencialidade das suas credenciais de acesso (CPF)</li>
      <li>Todas as atividades realizadas na sua conta, inclusive apostas e participa&ccedil;&otilde;es em sorteios</li>
      <li>Notificar imediatamente a PaynexBet em caso de uso n&atilde;o autorizado</li>
    </ul>
    <p>A PaynexBet n&atilde;o se responsabiliza por perdas decorrentes de uso indevido da conta por terceiros.</p>
    <div class="highlight blue">&#128161; Seu saldo est&aacute; vinculado exclusivamente ao seu CPF. Guarde-o com seguran&ccedil;a.</div>
  </div>

  <div class="legal-section" id="depositos">
    <h2><span class="num">4</span> Dep&oacute;sitos e Saques</h2>
    <p>Todos os dep&oacute;sitos e saques s&atilde;o realizados <strong>exclusivamente via Pix</strong>, de forma autom&aacute;tica e em tempo real.</p>
    <ul>
      <li><strong>Dep&oacute;sito m&iacute;nimo:</strong> R$ 5,00 por transa&ccedil;&atilde;o</li>
      <li><strong>Saque m&iacute;nimo:</strong> R$ 20,00 por solicita&ccedil;&atilde;o</li>
      <li><strong>Prazo de processamento:</strong> imediato ap&oacute;s confirma&ccedil;&atilde;o do Pix</li>
      <li><strong>Titularidade:</strong> o Pix de saque deve ser da mesma titularidade (CPF) cadastrado na plataforma</li>
    </ul>
    <div class="highlight red">&#128683; Dep&oacute;sitos realizados por terceiros ou CPF diferente do titular da conta ser&atilde;o estornados automaticamente.</div>
  </div>

  <div class="legal-section" id="apostas">
    <h2><span class="num">5</span> Apostas Esportivas</h2>
    <ul>
      <li><strong>Valor m&iacute;nimo por aposta:</strong> R$ 2,00</li>
      <li><strong>Odds:</strong> fornecidas em tempo real e podem variar at&eacute; o momento da confirma&ccedil;&atilde;o</li>
      <li>As apostas s&atilde;o <strong>definitivas e irrevog&aacute;veis</strong> ap&oacute;s confirma&ccedil;&atilde;o</li>
      <li>Em caso de cancelamento de evento esportivo, a aposta ser&aacute; reembolsada integralmente ao saldo</li>
      <li>Resultados finais seguem as informa&ccedil;&otilde;es oficiais das ligas e federa&ccedil;&otilde;es esportivas</li>
    </ul>
    <div class="highlight">&#9917; Os resultados das apostas s&atilde;o determinados pelos dados oficiais das competi&ccedil;&otilde;es, fornecidos por APIs esportivas certificadas.</div>
  </div>

  <div class="legal-section" id="sorteios">
    <h2><span class="num">6</span> Sorteios</h2>
    <ul>
      <li>O valor do bilhete e as regras espec&iacute;ficas s&atilde;o definidos antes do in&iacute;cio de cada sorteio</li>
      <li>O sorteio &eacute; realizado de forma autom&aacute;tica e transparente ao atingir o n&uacute;mero m&iacute;nimo de participantes</li>
      <li>O ganhador &eacute; notificado e o pr&ecirc;mio creditado automaticamente no saldo da conta</li>
      <li>&Eacute; vedada a participa&ccedil;&atilde;o de funcion&aacute;rios e colaboradores da PaynexBet</li>
    </ul>
    <div class="highlight green">&#127942; Os pr&ecirc;mios s&atilde;o pagos integralmente via Pix, sem descontos ou taxas adicionais.</div>
  </div>

  <div class="legal-section" id="proibicoes">
    <h2><span class="num">7</span> Condutas Proibidas</h2>
    <ul>
      <li>Utilizar a plataforma com fins fraudulentos ou para lavagem de dinheiro</li>
      <li>Criar m&uacute;ltiplas contas ou usar dados de terceiros para cadastro</li>
      <li>Usar bots, scripts ou qualquer automa&ccedil;&atilde;o para manipular o sistema</li>
      <li>Tentar hackear, comprometer ou prejudicar a infraestrutura da plataforma</li>
      <li>Realizar apostas combinadas de forma a garantir lucro independente do resultado (arbitragem abusiva)</li>
    </ul>
  </div>

  <div class="legal-section" id="responsabilidade">
    <h2><span class="num">8</span> Limita&ccedil;&atilde;o de Responsabilidade</h2>
    <p>A PaynexBet n&atilde;o se responsabiliza por perdas financeiras decorrentes de apostas, falhas em redes banc&aacute;rias ou interrup&ccedil;&otilde;es tempor&aacute;rias do servi&ccedil;o por manuten&ccedil;&atilde;o ou for&ccedil;a maior.</p>
  </div>

  <div class="legal-section" id="suspensao">
    <h2><span class="num">9</span> Suspens&atilde;o e Encerramento</h2>
    <p>A PaynexBet pode suspender ou encerrar sua conta em caso de viola&ccedil;&atilde;o destes Termos, suspeita de fraude, solicita&ccedil;&atilde;o do pr&oacute;prio usu&aacute;rio ou determina&ccedil;&atilde;o judicial.</p>
    <p>Em caso de encerramento por iniciativa do usu&aacute;rio, o saldo dispon&iacute;vel ser&aacute; devolvido na chave Pix cadastrada ap&oacute;s verifica&ccedil;&atilde;o de identidade.</p>
  </div>

  <div class="legal-section" id="alteracoes">
    <h2><span class="num">10</span> Altera&ccedil;&otilde;es nos Termos</h2>
    <p>A PaynexBet reserva o direito de modificar estes Termos a qualquer momento. As altera&ccedil;&otilde;es ser&atilde;o publicadas nesta p&aacute;gina com atualiza&ccedil;&atilde;o da data de vig&ecirc;ncia. O uso continuado da plataforma ap&oacute;s a publica&ccedil;&atilde;o das altera&ccedil;&otilde;es constitui aceita&ccedil;&atilde;o dos novos termos.</p>
  </div>

  <div class="legal-section" id="foro">
    <h2><span class="num">11</span> Foro e Legisla&ccedil;&atilde;o</h2>
    <p>Estes Termos s&atilde;o regidos pelas leis da Rep&uacute;blica Federativa do Brasil. Qualquer lit&iacute;gio ser&aacute; submetido ao foro da comarca de S&atilde;o Paulo/SP. Para d&uacute;vidas, acesse o <a href="/suporte" style="color:var(--blue2)">suporte</a>.</p>
  </div>
</div>
{_LEGAL_FOOTER}
</body>
</html>"""


def _page_privacidade():
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pol&iacute;tica de Privacidade &mdash; PaynexBet</title>
{_LEGAL_CSS}
</head>
<body>
{_LEGAL_NAV}
<div class="legal-hero">
  <span class="legal-icon">&#128274;</span>
  <h1 class="legal-title">Pol&iacute;tica de Privacidade</h1>
  <p class="legal-sub">Como coletamos, usamos e protegemos seus dados pessoais.</p>
  <span class="legal-date">Última atualização: 24 de abril de 2025 &middot; Em conformidade com a LGPD (Lei 13.709/2018)</span>
</div>
<div class="legal-body">
  <div class="legal-toc">
    <div class="legal-toc-title">&#128209; &Iacute;ndice</div>
    <ol>
      <li><a href="#p-dados">Dados que Coletamos</a></li>
      <li><a href="#p-uso">Como Usamos seus Dados</a></li>
      <li><a href="#p-base">Base Legal do Tratamento</a></li>
      <li><a href="#p-compartilhamento">Compartilhamento de Dados</a></li>
      <li><a href="#p-armazenamento">Armazenamento e Seguran&ccedil;a</a></li>
      <li><a href="#p-cookies">Cookies e Tecnologias</a></li>
      <li><a href="#p-direitos">Seus Direitos (LGPD)</a></li>
      <li><a href="#p-retencao">Reten&ccedil;&atilde;o de Dados</a></li>
      <li><a href="#p-contato">Contato com o DPO</a></li>
    </ol>
  </div>

  <div class="legal-section" id="p-dados">
    <h2><span class="num">1</span> Dados que Coletamos</h2>
    <ul>
      <li><strong>Dados de identifica&ccedil;&atilde;o:</strong> nome completo e CPF</li>
      <li><strong>Dados financeiros:</strong> hist&oacute;rico de dep&oacute;sitos e saques, chave Pix utilizada</li>
      <li><strong>Dados de uso:</strong> apostas realizadas, participa&ccedil;&otilde;es em sorteios, saldo e transa&ccedil;&otilde;es</li>
      <li><strong>Dados t&eacute;cnicos:</strong> endere&ccedil;o IP, tipo de dispositivo e navegador</li>
    </ul>
    <div class="highlight blue">&#128161; N&atilde;o coletamos documentos de identidade, selfies, endere&ccedil;o residencial ou dados banc&aacute;rios al&eacute;m da chave Pix para saque.</div>
  </div>

  <div class="legal-section" id="p-uso">
    <h2><span class="num">2</span> Como Usamos seus Dados</h2>
    <ul>
      <li>Identificar e autenticar sua conta de forma segura</li>
      <li>Processar dep&oacute;sitos e saques via Pix</li>
      <li>Registrar apostas e participa&ccedil;&otilde;es em sorteios</li>
      <li>Prevenir fraudes e uso indevido da plataforma</li>
      <li>Cumprir obriga&ccedil;&otilde;es legais e regulat&oacute;rias</li>
    </ul>
    <p><strong>N&atilde;o utilizamos seus dados para:</strong> venda a terceiros ou publicidade de outras empresas.</p>
  </div>

  <div class="legal-section" id="p-base">
    <h2><span class="num">3</span> Base Legal do Tratamento</h2>
    <ul>
      <li><strong>Execu&ccedil;&atilde;o de contrato:</strong> necess&aacute;rio para fornecer os servi&ccedil;os contratados (art. 7&ordm;, V)</li>
      <li><strong>Leg&iacute;timo interesse:</strong> preven&ccedil;&atilde;o de fraudes e seguran&ccedil;a da plataforma (art. 7&ordm;, IX)</li>
      <li><strong>Cumprimento de obriga&ccedil;&atilde;o legal:</strong> quando exigido por autoridades (art. 7&ordm;, II)</li>
    </ul>
  </div>

  <div class="legal-section" id="p-compartilhamento">
    <h2><span class="num">4</span> Compartilhamento de Dados</h2>
    <p>Seus dados <strong>n&atilde;o s&atilde;o vendidos</strong> a terceiros. Podemos compartilh&aacute;-los apenas com processadores de pagamento Pix, provedores de infraestrutura e autoridades competentes quando legalmente exigido.</p>
    <div class="highlight">&#128737;&#65039; Todos os parceiros s&atilde;o contratualmente obrigados a manter os dados confidenciais e seguros.</div>
  </div>

  <div class="legal-section" id="p-armazenamento">
    <h2><span class="num">5</span> Armazenamento e Seguran&ccedil;a</h2>
    <ul>
      <li>Banco de dados PostgreSQL com <strong>criptografia em repouso</strong></li>
      <li>Comunica&ccedil;&otilde;es protegidas com <strong>TLS/SSL (HTTPS)</strong></li>
      <li>Monitoramento cont&iacute;nuo contra tentativas de acesso n&atilde;o autorizado</li>
      <li>Infraestrutura hospedada em servidores com certifica&ccedil;&atilde;o de seguran&ccedil;a</li>
    </ul>
    <div class="highlight green">&#10003; Adotamos as melhores pr&aacute;ticas dispon&iacute;veis para proteger suas informa&ccedil;&otilde;es.</div>
  </div>

  <div class="legal-section" id="p-cookies">
    <h2><span class="num">6</span> Cookies e Tecnologias</h2>
    <p>A PaynexBet utiliza <strong>armazenamento local (localStorage)</strong> no seu dispositivo para manter sua sess&atilde;o ativa e personalizar a experi&ecirc;ncia. Voc&ecirc; pode limpar o armazenamento local a qualquer momento nas configura&ccedil;&otilde;es do seu navegador.</p>
  </div>

  <div class="legal-section" id="p-direitos">
    <h2><span class="num">7</span> Seus Direitos (LGPD)</h2>
    <ul>
      <li><strong>Confirma&ccedil;&atilde;o e acesso:</strong> saber se tratamos seus dados e acess&aacute;-los</li>
      <li><strong>Corre&ccedil;&atilde;o:</strong> corrigir dados incompletos ou inexatos</li>
      <li><strong>Anonimiza&ccedil;&atilde;o ou elimina&ccedil;&atilde;o:</strong> de dados desnecess&aacute;rios</li>
      <li><strong>Portabilidade:</strong> receber seus dados em formato estruturado</li>
      <li><strong>Revoga&ccedil;&atilde;o do consentimento:</strong> a qualquer momento</li>
    </ul>
    <p>Para exercer seus direitos, entre em contato pelo <a href="/suporte" style="color:var(--blue2)">canal de suporte</a>.</p>
  </div>

  <div class="legal-section" id="p-retencao">
    <h2><span class="num">8</span> Reten&ccedil;&atilde;o de Dados</h2>
    <ul>
      <li>Conta ativa: <strong>enquanto ativa</strong></li>
      <li>Obriga&ccedil;&otilde;es legais e fiscais: <strong>at&eacute; 5 anos</strong> ap&oacute;s encerramento</li>
      <li>Preven&ccedil;&atilde;o de fraudes: <strong>at&eacute; 2 anos</strong> ap&oacute;s o &uacute;ltimo acesso</li>
    </ul>
  </div>

  <div class="legal-section" id="p-contato">
    <h2><span class="num">9</span> Contato com o DPO</h2>
    <ul>
      <li>&#128231; <strong>E-mail:</strong> privacidade@paynexbet.com</li>
      <li>&#128172; <strong>Suporte online:</strong> <a href="/suporte" style="color:var(--blue2)">paynexbet.com/suporte</a></li>
    </ul>
    <p>Tamb&eacute;m &eacute; poss&iacute;vel registrar recla&ccedil;&otilde;es perante a Autoridade Nacional de Prote&ccedil;&atilde;o de Dados (ANPD) em: <strong>gov.br/anpd</strong></p>
  </div>
</div>
{_LEGAL_FOOTER}
</body>
</html>"""


def _page_responsavel():
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Jogo Respons&aacute;vel &mdash; PaynexBet</title>
{_LEGAL_CSS}
<style>
.rg-card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;margin-bottom:20px;transition:border-color .2s}}
.rg-card:hover{{border-color:#f5a62330}}
.rg-card-icon{{font-size:32px;margin-bottom:10px}}
.rg-card-title{{font-size:16px;font-weight:700;margin-bottom:6px}}
.rg-card-desc{{font-size:14px;color:#94a3b8;line-height:1.6}}
.rg-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:16px;margin-bottom:40px}}
.rg-quiz{{background:linear-gradient(135deg,#1a1a30,#1e1e3a);border:1px solid var(--blue);border-radius:16px;padding:28px 24px;margin:32px 0}}
.rg-quiz-title{{font-size:18px;font-weight:800;margin-bottom:8px;color:var(--blue2)}}
.rg-quiz-desc{{font-size:14px;color:var(--muted);margin-bottom:20px;line-height:1.6}}
.rg-quiz-item{{display:flex;align-items:flex-start;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)}}
.rg-quiz-item:last-child{{border-bottom:none;padding-bottom:0}}
.rg-check{{width:22px;height:22px;border:2px solid var(--blue);border-radius:6px;flex-shrink:0;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:#fff;transition:all .2s;background:transparent;user-select:none}}
.rg-check.on{{background:var(--blue);border-color:var(--blue)}}
.rg-quiz-label{{font-size:14px;color:#cbd5e1;line-height:1.5;cursor:pointer;padding-top:2px}}
.rg-result{{display:none;padding:14px 18px;border-radius:10px;margin-top:16px;font-size:14px;font-weight:600;line-height:1.7}}
.rg-result.safe{{background:#22c55e18;border:1px solid #22c55e40;color:#4ade80}}
.rg-result.warn{{background:#f59e0b18;border:1px solid #f59e0b40;color:#fbbf24}}
.rg-result.danger{{background:#ef444418;border:1px solid #ef444440;color:#f87171}}
.btn-quiz{{margin-top:16px;padding:11px 28px;border-radius:10px;font-size:14px;font-weight:700;background:var(--blue);border:none;color:#fff;cursor:pointer;transition:all .2s}}
.btn-quiz:hover{{background:#2563eb}}
.crisis-box{{background:linear-gradient(135deg,#1a0a14,#2a0a1a);border:2px solid #ef444480;border-radius:16px;padding:28px 24px;text-align:center;margin:32px 0}}
.crisis-title{{font-size:20px;font-weight:800;color:#f87171;margin-bottom:12px}}
.crisis-desc{{font-size:14px;color:#cbd5e1;margin-bottom:20px;line-height:1.7}}
.btn-crisis{{padding:12px 32px;border-radius:10px;font-size:15px;font-weight:700;background:#ef4444;border:none;color:#fff;cursor:pointer;text-decoration:none;display:inline-block}}
</style>
</head>
<body>
{_LEGAL_NAV}
<div class="legal-hero" style="background:linear-gradient(180deg,#0f1a1a 0%,var(--bg) 100%)">
  <span class="legal-icon">&#128737;&#65039;</span>
  <h1 class="legal-title" style="background:linear-gradient(90deg,#22c55e,#4ade80);-webkit-background-clip:text;-webkit-text-fill-color:transparent">Jogo Respons&aacute;vel</h1>
  <p class="legal-sub">Sua sa&uacute;de e bem-estar s&atilde;o mais importantes que qualquer aposta.</p>
  <span class="legal-date">PaynexBet &mdash; Comprometida com o jogo seguro</span>
</div>
<div class="legal-body">

  <div class="legal-section">
    <h2><span class="num" style="background:linear-gradient(135deg,var(--green),#16a34a);color:#fff">&#9825;</span> Nossa Miss&atilde;o</h2>
    <p>A PaynexBet acredita que as apostas e sorteios devem ser uma forma de entretenimento, nunca uma fonte de press&atilde;o financeira ou emocional. Adotamos pr&aacute;ticas rigorosas de jogo respons&aacute;vel e incentivamos todos os nossos usu&aacute;rios a jogar com consci&ecirc;ncia.</p>
    <div class="highlight green">&#10003; Apostas s&atilde;o entretenimento. Nunca aposte valores que n&atilde;o pode perder.</div>
  </div>

  <div class="rg-grid">
    <div class="rg-card"><div class="rg-card-icon">&#128176;</div><div class="rg-card-title">Defina um or&ccedil;amento</div><div class="rg-card-desc">Estabele&ccedil;a um valor m&aacute;ximo mensal para apostas e nunca ultrapasse esse limite.</div></div>
    <div class="rg-card"><div class="rg-card-icon">&#9201;&#65039;</div><div class="rg-card-title">Controle o tempo</div><div class="rg-card-desc">Fa&ccedil;a pausas regulares. Evite apostar por longos per&iacute;odos sem intervalos.</div></div>
    <div class="rg-card"><div class="rg-card-icon">&#128683;</div><div class="rg-card-title">Nunca persiga perdas</div><div class="rg-card-desc">Tentar recuperar perdas geralmente resulta em mais perdas. Saiba quando parar.</div></div>
    <div class="rg-card"><div class="rg-card-icon">&#129504;</div><div class="rg-card-title">Decida com clareza</div><div class="rg-card-desc">Nunca aposte sob influ&ecirc;ncia de &aacute;lcool, drogas ou quando estiver emocionalmente abalado.</div></div>
    <div class="rg-card"><div class="rg-card-icon">&#128106;</div><div class="rg-card-title">Proteja os menores</div><div class="rg-card-desc">Nunca permita que menores de 18 anos usem sua conta ou participem de apostas.</div></div>
    <div class="rg-card"><div class="rg-card-icon">&#128202;</div><div class="rg-card-title">Acompanhe seus gastos</div><div class="rg-card-desc">Revise regularmente seu hist&oacute;rico de apostas para manter o controle financeiro.</div></div>
  </div>

  <div class="rg-quiz">
    <div class="rg-quiz-title">&#128269; Autoavalia&ccedil;&atilde;o: voc&ecirc; est&aacute; jogando com responsabilidade?</div>
    <div class="rg-quiz-desc">Marque as afirma&ccedil;&otilde;es que se aplicam &agrave; sua situa&ccedil;&atilde;o. Este question&aacute;rio &eacute; sigiloso e serve apenas para sua reflex&atilde;o pessoal.</div>
    <div>
      <div class="rg-quiz-item"><div class="rg-check" onclick="this.classList.toggle('on');this.textContent=this.classList.contains('on')?'&#10003;':''"></div><div class="rg-quiz-label">J&aacute; apostei dinheiro que precisava para despesas essenciais (aluguel, alimenta&ccedil;&atilde;o, contas)</div></div>
      <div class="rg-quiz-item"><div class="rg-check" onclick="this.classList.toggle('on');this.textContent=this.classList.contains('on')?'&#10003;':''"></div><div class="rg-quiz-label">Sinto a necessidade de apostar valores cada vez maiores para sentir emo&ccedil;&atilde;o</div></div>
      <div class="rg-quiz-item"><div class="rg-check" onclick="this.classList.toggle('on');this.textContent=this.classList.contains('on')?'&#10003;':''"></div><div class="rg-quiz-label">J&aacute; menti para familiares ou amigos sobre minhas apostas</div></div>
      <div class="rg-quiz-item"><div class="rg-check" onclick="this.classList.toggle('on');this.textContent=this.classList.contains('on')?'&#10003;':''"></div><div class="rg-quiz-label">Sinto irrita&ccedil;&atilde;o ou ansiedade quando n&atilde;o consigo apostar</div></div>
      <div class="rg-quiz-item"><div class="rg-check" onclick="this.classList.toggle('on');this.textContent=this.classList.contains('on')?'&#10003;':''"></div><div class="rg-quiz-label">J&aacute; tentei parar de apostar e n&atilde;o consegui</div></div>
      <div class="rg-quiz-item"><div class="rg-check" onclick="this.classList.toggle('on');this.textContent=this.classList.contains('on')?'&#10003;':''"></div><div class="rg-quiz-label">As apostas j&aacute; prejudicaram meu trabalho, estudos ou relacionamentos</div></div>
    </div>
    <button class="btn-quiz" onclick="calcQuiz()">Ver resultado</button>
    <div class="rg-result safe" id="rr-safe">&#10003; <strong>Tudo indica que voc&ecirc; est&aacute; jogando com responsabilidade.</strong> Continue assim! Mantenha seus limites e aproveite o entretenimento com equil&iacute;brio.</div>
    <div class="rg-result warn" id="rr-warn">&#9888;&#65039; <strong>Aten&ccedil;&atilde;o: alguns sinais merecem cuidado.</strong> Considere fazer uma pausa nas apostas e conversar com algu&eacute;m de confian&ccedil;a sobre sua rela&ccedil;&atilde;o com o jogo.</div>
    <div class="rg-result danger" id="rr-danger">&#128680; <strong>Sinal de alerta: voc&ecirc; pode estar desenvolvendo depend&ecirc;ncia.</strong> Recomendamos fortemente buscar ajuda profissional. Ligue para o CVV: <strong>188</strong> ou acesse o Jogadores An&ocirc;nimos.</div>
  </div>

  <div class="legal-section">
    <h2><span class="num" style="background:linear-gradient(135deg,var(--blue),#2563eb);color:#fff">&#128296;</span> Ferramentas de Controle</h2>
    <p>Entre em contato com nosso suporte para solicitar:</p>
    <ul>
      <li><strong>Autoexclus&atilde;o tempor&aacute;ria:</strong> bloqueio da conta por 30, 60 ou 90 dias</li>
      <li><strong>Autoexclus&atilde;o permanente:</strong> encerramento definitivo da conta</li>
      <li><strong>Limite de dep&oacute;sito:</strong> definir um teto mensal de dep&oacute;sitos</li>
      <li><strong>Hist&oacute;rico completo:</strong> visualizar todas as suas transa&ccedil;&otilde;es e apostas</li>
    </ul>
    <p>Acesse o <a href="/suporte" style="color:var(--blue2)">nosso suporte</a> para solicitar qualquer uma dessas ferramentas.</p>
  </div>

  <div class="crisis-box">
    <div class="crisis-title">&#128682; Precisa de ajuda agora?</div>
    <div class="crisis-desc">
      Se voc&ecirc; ou algu&eacute;m que conhece est&aacute; com problemas relacionados ao jogo compulsivo:<br><br>
      <strong>CVV (Centro de Valoriza&ccedil;&atilde;o da Vida):</strong> Ligue 188 ou acesse cvv.org.br<br>
      <strong>Jogadores An&ocirc;nimos Brasil:</strong> jogadoresanonimos.org.br<br>
      <strong>CAPS:</strong> acesse na sua cidade pelo Sistema &Uacute;nico de Sa&uacute;de
    </div>
    <a href="tel:188" class="btn-crisis">&#128222; Ligar 188 (gratuito)</a>
  </div>

</div>
{_LEGAL_FOOTER}
<script>
function calcQuiz(){{
  var n = document.querySelectorAll('.rg-check.on').length;
  document.getElementById('rr-safe').style.display='none';
  document.getElementById('rr-warn').style.display='none';
  document.getElementById('rr-danger').style.display='none';
  if(n===0) document.getElementById('rr-safe').style.display='block';
  else if(n<=2) document.getElementById('rr-warn').style.display='block';
  else document.getElementById('rr-danger').style.display='block';
}}
</script>
</body>
</html>"""


def _page_suporte():
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Suporte &mdash; PaynexBet</title>
{_LEGAL_CSS}
<style>
.sup-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:20px;margin-bottom:48px}}
.sup-card{{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:28px 24px;transition:all .25s;text-decoration:none;display:block}}
.sup-card:hover{{border-color:var(--blue);transform:translateY(-4px);box-shadow:0 8px 32px #0006}}
.sup-card-icon{{font-size:36px;margin-bottom:14px}}
.sup-card-title{{font-size:17px;font-weight:800;margin-bottom:6px;color:var(--text)}}
.sup-card-desc{{font-size:14px;color:var(--muted);line-height:1.6}}
.sup-card-action{{display:inline-flex;align-items:center;gap:6px;margin-top:14px;font-size:13px;font-weight:700;color:var(--blue2)}}
.faq-item{{background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:10px}}
.faq-q{{padding:16px 20px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;font-size:15px;font-weight:600;transition:background .15s;user-select:none}}
.faq-q:hover{{background:var(--bg3)}}
.faq-q .arr{{font-size:11px;color:var(--muted);transition:transform .25s;flex-shrink:0;margin-left:12px}}
.faq-a{{display:none;padding:4px 20px 16px;font-size:14px;color:#94a3b8;line-height:1.75;border-top:1px solid var(--border)}}
.faq-item.open .faq-q .arr{{transform:rotate(180deg)}}
.faq-item.open .faq-a{{display:block}}
.sup-form{{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:32px 28px}}
.form-group{{margin-bottom:16px}}
.form-label{{display:block;font-size:13px;font-weight:600;color:var(--muted);margin-bottom:6px}}
.form-input{{width:100%;background:var(--bg3);border:1.5px solid var(--border);border-radius:10px;padding:12px 14px;font-size:15px;color:var(--text);outline:none;transition:border-color .2s}}
.form-input:focus{{border-color:var(--blue)}}
.form-input::placeholder{{color:#475569}}
textarea.form-input{{min-height:120px;resize:vertical}}
.btn-enviar{{width:100%;padding:14px;border-radius:12px;font-size:16px;font-weight:800;background:linear-gradient(135deg,var(--blue),#2563eb);border:none;color:#fff;cursor:pointer;transition:all .25s;margin-top:4px}}
.btn-enviar:hover{{transform:translateY(-1px);box-shadow:0 4px 20px #3b82f640}}
.sent-msg{{display:none;text-align:center;padding:28px;font-size:15px;color:var(--green);line-height:1.8}}
.badge-online{{display:inline-flex;align-items:center;gap:8px;background:#22c55e18;border:1px solid #22c55e40;color:#4ade80;padding:10px 20px;border-radius:99px;font-size:14px;font-weight:700;margin-top:12px}}
.pulse{{width:9px;height:9px;border-radius:50%;background:var(--green);animation:pulse 1.5s infinite;display:inline-block}}
@keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:.5;transform:scale(1.3)}}}}
</style>
</head>
<body>
{_LEGAL_NAV}
<div class="legal-hero" style="background:linear-gradient(180deg,#0f0f2a 0%,var(--bg) 100%)">
  <span class="legal-icon">&#128172;</span>
  <h1 class="legal-title">Central de Suporte</h1>
  <p class="legal-sub">Estamos aqui para ajudar. Encontre respostas r&aacute;pidas ou fale com nossa equipe.</p>
  <div><span class="badge-online"><span class="pulse"></span> Suporte online agora</span></div>
</div>
<div class="legal-body">

  <div class="sup-grid">
    <a href="https://t.me/paynexbet_suporte" target="_blank" class="sup-card">
      <div class="sup-card-icon">&#9992;&#65039;</div>
      <div class="sup-card-title">Telegram</div>
      <div class="sup-card-desc">Canal mais r&aacute;pido. Resposta m&eacute;dia em at&eacute; 10 minutos durante hor&aacute;rio comercial.</div>
      <span class="sup-card-action">Abrir Telegram &#8594;</span>
    </a>
    <a href="https://wa.me/5511999999999?text=Ol%C3%A1%2C+preciso+de+suporte+PaynexBet" target="_blank" class="sup-card">
      <div class="sup-card-icon">&#128241;</div>
      <div class="sup-card-title">WhatsApp</div>
      <div class="sup-card-desc">Envie uma mensagem e nossa equipe responde em at&eacute; 30 minutos.</div>
      <span class="sup-card-action">Abrir WhatsApp &#8594;</span>
    </a>
    <a href="mailto:suporte@paynexbet.com" class="sup-card">
      <div class="sup-card-icon">&#128231;</div>
      <div class="sup-card-title">E-mail</div>
      <div class="sup-card-desc">Para assuntos mais complexos. Resposta em at&eacute; 24 horas &uacute;teis.</div>
      <span class="sup-card-action">suporte@paynexbet.com &#8594;</span>
    </a>
  </div>

  <div class="legal-section">
    <h2><span class="num">?</span> Perguntas Frequentes</h2>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Como fa&ccedil;o um dep&oacute;sito?</span><span class="arr">&#9660;</span></div><div class="faq-a">Acesse "Minha Conta", clique em "Depositar via Pix", informe o valor (m&iacute;nimo R$ 5,00) e escaneie o QR Code com o app do seu banco. O saldo &eacute; creditado automaticamente em segundos.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Como solicitar um saque?</span><span class="arr">&#9660;</span></div><div class="faq-a">Em "Minha Conta", clique em "Sacar via Pix", informe sua chave Pix (mesmo CPF cadastrado) e o valor (m&iacute;nimo R$ 20,00). O processamento &eacute; autom&aacute;tico.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Quanto tempo leva para o saldo aparecer ap&oacute;s o dep&oacute;sito?</span><span class="arr">&#9660;</span></div><div class="faq-a">O saldo &eacute; creditado automaticamente assim que o Pix &eacute; confirmado, geralmente em menos de 10 segundos. Em casos raros de instabilidade, pode levar at&eacute; 5 minutos.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Minha aposta foi feita mas n&atilde;o aparece no hist&oacute;rico.</span><span class="arr">&#9660;</span></div><div class="faq-a">Aguarde alguns instantes e recarregue a p&aacute;gina. Se o saldo foi debitado mas a aposta n&atilde;o aparece, entre em contato com nosso suporte informando valor e hor&aacute;rio.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Como funciona o sorteio?</span><span class="arr">&#9660;</span></div><div class="faq-a">O sorteio &eacute; realizado automaticamente quando um n&uacute;mero m&iacute;nimo de bilhetes &eacute; atingido ou na data programada. O ganhador recebe notifica&ccedil;&atilde;o e o pr&ecirc;mio &eacute; creditado automaticamente.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Posso cancelar uma aposta?</span><span class="arr">&#9660;</span></div><div class="faq-a">N&atilde;o &eacute; poss&iacute;vel cancelar apostas ap&oacute;s a confirma&ccedil;&atilde;o. As apostas s&atilde;o definitivas e irrevog&aacute;veis. Em caso de cancelamento do evento esportivo, o valor &eacute; reembolsado automaticamente.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Os odds mudam depois que confirmo a aposta?</span><span class="arr">&#9660;</span></div><div class="faq-a">N&atilde;o. A odd registrada no momento da confirma&ccedil;&atilde;o &eacute; definitiva e n&atilde;o muda, independente de varia&ccedil;&otilde;es posteriores no mercado.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Preciso enviar documentos para sacar?</span><span class="arr">&#9660;</span></div><div class="faq-a">Para a maioria dos saques, n&atilde;o &eacute; necess&aacute;rio nenhum documento adicional. Em casos de valores elevados, podemos solicitar verifica&ccedil;&atilde;o de identidade.</div></div>
    <div class="faq-item"><div class="faq-q" onclick="tf(this)"><span>Esqueci meu CPF cadastrado. Como acesso a conta?</span><span class="arr">&#9660;</span></div><div class="faq-a">O acesso &eacute; feito exclusivamente pelo CPF. Se n&atilde;o lembrar, entre em contato com nosso suporte informando seu nome completo para que possamos identificar sua conta.</div></div>
  </div>

  <div class="sup-form">
    <h2 style="font-size:20px;font-weight:800;margin-bottom:6px">&#128233; Enviar mensagem</h2>
    <p style="font-size:14px;color:var(--muted);margin-bottom:24px">N&atilde;o encontrou o que precisava? Preencha o formul&aacute;rio e nossa equipe responde em at&eacute; 24h &uacute;teis.</p>
    <div id="form-wrap">
      <div class="form-group"><label class="form-label">Seu nome</label><input class="form-input" type="text" id="sup-nome" placeholder="Nome completo"></div>
      <div class="form-group"><label class="form-label">CPF cadastrado na plataforma</label><input class="form-input" type="text" id="sup-cpf" placeholder="000.000.000-00" maxlength="14"></div>
      <div class="form-group"><label class="form-label">Assunto</label>
        <select class="form-input" id="sup-assunto">
          <option value="">Selecione o assunto...</option>
          <option>Problema com dep&oacute;sito</option>
          <option>Problema com saque</option>
          <option>Aposta n&atilde;o registrada</option>
          <option>Sorteio &mdash; d&uacute;vida ou reclama&ccedil;&atilde;o</option>
          <option>Conta bloqueada ou suspensa</option>
          <option>Privacidade e dados pessoais</option>
          <option>Jogo respons&aacute;vel &mdash; solicitar autoexclus&atilde;o</option>
          <option>Outro assunto</option>
        </select>
      </div>
      <div class="form-group"><label class="form-label">Descreva sua situa&ccedil;&atilde;o</label><textarea class="form-input" id="sup-msg" placeholder="Descreva o problema ou d&uacute;vida com o m&aacute;ximo de detalhes poss&iacute;vel..."></textarea></div>
      <button class="btn-enviar" onclick="enviar()">&#128232; Enviar mensagem</button>
    </div>
    <div class="sent-msg" id="sent-msg">
      &#10003; <strong>Mensagem enviada com sucesso!</strong><br>
      Nossa equipe responder&aacute; em breve pelo Telegram ou e-mail informado.
    </div>
  </div>

</div>
{_LEGAL_FOOTER}
<script>
function tf(el){{
  var item = el.parentElement;
  var open = item.classList.contains('open');
  document.querySelectorAll('.faq-item.open').forEach(function(i){{i.classList.remove('open');}});
  if(!open) item.classList.add('open');
}}
document.getElementById('sup-cpf').addEventListener('input',function(){{
  this.value=this.value.replace(/\\D/g,'').replace(/(\\d{{3}})(\\d)/,'$1.$2').replace(/(\\d{{3}})\\.(\\d{{3}})(\\d)/,'$1.$2.$3').replace(/(\\d{{3}})\\.(\\d{{3}})\\.(\\d{{3}})(\\d)/,'$1.$2.$3-$4').substring(0,14);
}});
function enviar(){{
  var nome=document.getElementById('sup-nome').value.trim();
  var assunto=document.getElementById('sup-assunto').value;
  var msg=document.getElementById('sup-msg').value.trim();
  if(!nome){{alert('Por favor, informe seu nome.');return;}}
  if(!assunto){{alert('Por favor, selecione o assunto.');return;}}
  if(msg.length<10){{alert('Por favor, descreva melhor sua situa\\u00e7\\u00e3o.');return;}}
  document.getElementById('form-wrap').style.display='none';
  document.getElementById('sent-msg').style.display='block';
}}
</script>
</body>
</html>"""


def _load_legal_page(chave, fallback_fn):
    """Carrega página legal do banco (deploy instantâneo) ou usa função fallback."""
    try:
        if DATABASE_URL:
            import psycopg2 as _pg
            _c = _pg.connect(DATABASE_URL, connect_timeout=3)
            _cur = _c.cursor()
            _cur.execute("SELECT valor FROM configuracoes WHERE chave=%s", (chave,))
            _row = _cur.fetchone()
            _c.close()
            if _row and _row[0] and len(_row[0]) > 500:
                return _row[0]
    except Exception:
        pass
    return fallback_fn()

async def route_termos(request):
    """GET /termos"""
    return web.Response(text=_load_legal_page('termos_html', _page_termos), content_type='text/html', charset='utf-8')

async def route_privacidade(request):
    """GET /privacidade"""
    return web.Response(text=_load_legal_page('privacidade_html', _page_privacidade), content_type='text/html', charset='utf-8')

async def route_responsavel(request):
    """GET /responsavel"""
    return web.Response(text=_load_legal_page('responsavel_html', _page_responsavel), content_type='text/html', charset='utf-8')

async def route_suporte(request):
    """GET /suporte"""
    return web.Response(text=_load_legal_page('suporte_html', _page_suporte), content_type='text/html', charset='utf-8')


async def route_bet_crest(request):
    """GET /api/bet/crest?nome=TeamName — retorna URL do escudo (mapa estático, resposta imediata)"""
    nome = (request.rel_url.query.get('nome') or '').strip()
    if not nome:
        return web.json_response({'crest': None})
    # Usar mapa estático com matching normalizado (sem chamada externa)
    crest = _get_crest_static(nome)
    return web.json_response({'crest': crest})


async def route_bet_dbcheck(request):
    """GET /api/bet/dbcheck — diagnóstico de conexão DB"""
    import time, psycopg2 as _pg2
    results = []
    urls_to_try = [
        ('env_DATABASE_URL', DATABASE_URL),
        ('fallback_hardcoded', _BET_DB_URL_FALLBACK),
    ]
    for label, url in urls_to_try:
        if not url:
            results.append({'url': label, 'status': 'vazio'})
            continue
        t0 = time.time()
        try:
            conn = _pg2.connect(url, connect_timeout=8)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM usuarios")
            cnt_u = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM apostas")
            cnt_a = cur.fetchone()[0]
            cur.close(); conn.close()
            ms = int((time.time()-t0)*1000)
            results.append({'url': label, 'status': 'ok', 'ms': ms,
                            'usuarios': cnt_u, 'apostas': cnt_a})
            break
        except Exception as e:
            ms = int((time.time()-t0)*1000)
            results.append({'url': label, 'status': 'erro', 'ms': ms,
                            'erro': f'{type(e).__name__}: {str(e)[:120]}'})
    return web.json_response({'ok': True, 'results': results,
                              'DATABASE_URL_set': bool(os.environ.get('DATABASE_URL'))})


async def route_bet_config_get(request):
    """GET /api/bet/config — retorna status das chaves (sem expor valores)"""
    secret = request.headers.get('X-PaynexBet-Secret','') or request.headers.get('x-paynexbet-secret','')
    if secret != WEBHOOK_SECRET:
        return web.json_response({'ok': False, 'error': 'Não autorizado'}, status=403)
    odds_key = _get_odds_key()
    suit_ci, suit_cs = _get_suit_keys()
    return web.json_response({
        'ok': True,
        'odds_api_key': f'{odds_key[:6]}...{odds_key[-4:]}' if len(odds_key) > 10 else ('configurada' if odds_key else 'NÃO CONFIGURADA'),
        'suitpay_ci':   suit_ci[:4] + '...' if suit_ci else 'NÃO CONFIGURADO',
        'suitpay_cs':   suit_cs[:6] + '...' if suit_cs else 'NÃO CONFIGURADO',
        'fonte_odds':   'db' if (db := _bet_load_keys_from_db()) and db.get('bet_odds_api_key') else ('env' if os.environ.get('ODDS_API_KEY','') else 'nenhuma'),
        'fonte_suit':   'db' if db.get('bet_suitpay_ci','') else ('env' if os.environ.get('SUITPAY_CI','') else 'nenhuma'),
    })


async def route_apostas_page(request):
    """GET /apostas — página de apostas esportivas"""
    return web.Response(text=_page_apostas_html(), content_type='text/html', charset='utf-8')


async def route_conta_page(request):
    """GET /conta — página de conta do apostador"""
    return web.Response(text=_page_conta_html(), content_type='text/html', charset='utf-8')


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

    # ── BET: Apostas Esportivas ──
    app.router.add_get('/api/bet/jogos',        route_bet_jogos)
    app.router.add_get('/api/bet/live',         route_bet_live)
    app.router.add_get('/api/bet/predictions',  route_bet_predictions)
    app.router.add_get('/api/bet/standings',    route_bet_standings)
    app.router.add_get('/api/bet/tabela',       route_tsdb_tabela)
    app.router.add_get('/api/bet/fixtures',     route_tsdb_fixtures)
    app.router.add_post('/api/bet/deposito',    route_bet_deposito)
    app.router.add_post('/webhook/suitpay',     route_webhook_suitpay)
    # BET — auth + saldo + apostar + sacar + páginas
    app.router.add_post('/api/bet/config',      route_bet_config)
    app.router.add_get('/api/bet/config',       route_bet_config_get)
    app.router.add_post('/api/bet/login',       route_bet_login)
    app.router.add_get('/api/bet/saldo/{usuario_id}', route_bet_saldo)
    app.router.add_post('/api/bet/apostar',     route_bet_apostar)
    app.router.add_post('/api/bet/sacar',       route_bet_sacar)
    app.router.add_get('/api/bet/dbcheck',      route_bet_dbcheck)
    app.router.add_get('/api/bet/crest',        route_bet_crest)
    app.router.add_get('/apostas',              route_apostas_page)
    app.router.add_get('/apostas.html',         route_apostas_page)
    app.router.add_get('/tabela',               route_tabela_page)
    app.router.add_get('/tabela.html',          route_tabela_page)
    app.router.add_get('/conta',                route_conta_page)
    app.router.add_get('/conta.html',           route_conta_page)
    app.router.add_get('/', route_home)            # Página principal PaynexBet
    app.router.add_get('/home', route_home)
    # Páginas legais
    app.router.add_get('/termos',      route_termos)
    app.router.add_get('/privacidade', route_privacidade)
    app.router.add_get('/responsavel', route_responsavel)
    app.router.add_get('/suporte',     route_suporte)
    app.router.add_get('/index.html', route_index)
    app.router.add_get('/health', route_health)
    app.router.add_get('/api/status', route_health)
    app.router.add_post('/api/pix', route_pix)
    app.router.add_get('/api/pix/status/{tx_id}', route_pix_status)
    app.router.add_get('/api/debug-pix', route_debug_pix)
    app.router.add_get('/api/check-auth', route_check_auth)
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
    # ── Bot2 webhook (novo — sem polling) ──
    app.router.add_post('/webhook/bot2',                    route_bot2_webhook)
    app.router.add_post('/api/bot2/set-webhook',            route_bot2_set_webhook)
    app.router.add_get('/api/bot2/webhook-info',            route_bot2_webhook_info)
    app.router.add_post('/api/bot2/token',                  route_bot2_salvar_token)
    app.router.add_get('/api/bot2/config',                  route_bot2_config_get)
    app.router.add_get('/api/mp2/depositos',                route_mp2_depositos_admin)
    app.router.add_get('/api/mp2/saques',                   route_mp2_saques_pendentes)
    app.router.add_post('/api/mp2/saques/processar',        route_mp2_processar_saque)
    app.router.add_get('/api/mp2/config',                   route_mp2_config_get)
    app.router.add_post('/api/mp2/config',                  route_mp2_config_save)
    app.router.add_get('/api/mp2/testar',                   route_mp2_testar)
    app.router.add_get('/api/parceiro/{codigo}',            route_parceiro_info_publico)  # público
    app.router.add_get('/api/mp2/parceiros',                route_mp2_parceiros_listar)
    app.router.add_post('/api/mp2/parceiros',               route_mp2_parceiros_criar)
    app.router.add_delete('/api/mp2/parceiros/{codigo}',    route_mp2_parceiros_deletar)
    app.router.add_post('/api/mp2/parceiros/{codigo}/excluir', route_mp2_parceiros_excluir)
    app.router.add_patch('/api/mp2/parceiros/{codigo}',     route_mp2_parceiros_editar)
    app.router.add_get('/api/mp2/comissoes',                route_mp2_comissoes_listar)
    app.router.add_post('/api/mp2/comissoes/pagar',         route_mp2_comissoes_pagar_manual)
    # ── PP (PayPix) — aliases do MP2, mesmas credenciais e lógica ──
    app.router.add_get('/api/pp/status',                    route_mp2_status)
    app.router.add_get('/api/pp/stats',                     route_mp2_stats)
    app.router.add_post('/webhook/pp',                      route_mp2_webhook)
    app.router.add_get('/api/pp/saques',                    route_mp2_saques_pendentes)
    app.router.add_post('/api/pp/saques/processar',         route_mp2_processar_saque)
    app.router.add_get('/api/pp/config',                    route_mp2_config_get)
    app.router.add_post('/api/pp/config',                   route_mp2_config_save)
    app.router.add_get('/api/pp/testar',                    route_mp2_testar)
    app.router.add_get('/api/pp/parceiros',                 route_mp2_parceiros_listar)
    app.router.add_post('/api/pp/parceiros',                route_mp2_parceiros_criar)
    app.router.add_delete('/api/pp/parceiros/{codigo}',     route_mp2_parceiros_deletar)
    app.router.add_post('/api/pp/parceiros/{codigo}/excluir',  route_mp2_parceiros_excluir)
    app.router.add_patch('/api/pp/parceiros/{codigo}',      route_mp2_parceiros_editar)
    app.router.add_get('/api/pp/comissoes',                 route_mp2_comissoes_listar)
    app.router.add_post('/api/pp/comissoes/pagar',          route_mp2_comissoes_pagar_manual)
    app.router.add_get('/api/pp/chaves',                    route_mp2_config_get)
    app.router.add_get('/api/pp/fila',                      route_paypix_fila)
    app.router.add_post('/api/pp/solicitar-codigo',         route_bot2_solicitar_codigo)
    app.router.add_post('/api/pp/confirmar-codigo',         route_bot2_confirmar_codigo)

    # ── PC (PayPix-Cob) — segundo canal PayPix, mesma infra MP2 ──────────
    app.router.add_get('/api/pc/status',                    route_mp2_status)
    app.router.add_get('/api/pc/stats',                     route_mp2_stats)
    app.router.add_get('/api/pc/config',                    route_pc_config_get)
    app.router.add_post('/api/pc/config',                   route_pc_config_save)
    app.router.add_get('/api/pc/testar',                    route_pc_testar)
    app.router.add_get('/api/pc/parceiros',                 route_mp2_parceiros_listar)
    app.router.add_post('/api/pc/parceiros',                route_mp2_parceiros_criar)
    app.router.add_delete('/api/pc/parceiros/{codigo}',     route_mp2_parceiros_deletar)
    app.router.add_post('/api/pc/parceiros/{codigo}/excluir',  route_mp2_parceiros_excluir)
    app.router.add_patch('/api/pc/parceiros/{codigo}',      route_mp2_parceiros_editar)
    app.router.add_get('/api/pc/comissoes',                 route_mp2_comissoes_listar)
    app.router.add_post('/api/pc/comissoes/pagar',          route_mp2_comissoes_pagar_manual)
    app.router.add_get('/api/pc/fila',                      route_paypix_fila)
    app.router.add_get('/api/pc/chaves',                    route_pc_config_get)
    app.router.add_post('/api/pc/solicitar-codigo',         route_bot2_solicitar_codigo)
    app.router.add_post('/api/pc/confirmar-codigo',         route_bot2_confirmar_codigo)

    # ── Bot3 / MP3 — Réplica do Bot2 (@paypix_nexbot2) ──────────────────────────────
    app.router.add_post('/webhook/bot3',                    route_bot3_webhook)
    app.router.add_post('/webhook/mp3',                     route_mp3_webhook)
    app.router.add_post('/api/bot3/set-webhook',            route_bot3_set_webhook)
    app.router.add_get('/api/bot3/webhook-info',            route_bot3_webhook_info)
    app.router.add_post('/api/bot3/token',                  route_bot3_salvar_token)
    app.router.add_get('/api/bot3/config',                  route_bot3_config_get)
    app.router.add_get('/api/mp3/status',                   route_mp3_status)
    app.router.add_get('/api/mp3/stats',                    route_mp3_stats)
    app.router.add_get('/api/mp3/saques',                   route_mp3_saques_pendentes)
    app.router.add_post('/api/mp3/saques/processar',        route_mp3_processar_saque)
    app.router.add_get('/api/mp3/config',                   route_mp3_config_get)
    app.router.add_post('/api/mp3/config',                  route_mp3_config_save)
    app.router.add_get('/api/mp3/testar',                   route_mp3_testar)
    app.router.add_get('/api/mp3/parceiros',                route_mp3_parceiros_listar)
    app.router.add_post('/api/mp3/parceiros',               route_mp3_parceiros_criar)
    app.router.add_delete('/api/mp3/parceiros/{codigo}',    route_mp3_parceiros_deletar)
    app.router.add_post('/api/mp3/parceiros/{codigo}/excluir', route_mp3_parceiros_excluir)
    app.router.add_patch('/api/mp3/parceiros/{codigo}',     route_mp3_parceiros_editar)
    app.router.add_get('/api/mp3/comissoes',                route_mp3_comissoes_listar)
    # ── MP3 — Rotas públicas (paypix2) ──────────────────────────────────────────────
    app.router.add_get('/paypix2',                          route_paypix2_page)
    app.router.add_post('/api/mp3/gerar',                   route_mp3_gerar_pix)
    app.router.add_get('/api/mp3/status/{tx_id}',           route_mp3_status_tx)
    app.router.add_get('/api/mp3/config-publica',           route_mp3_config_publica)
    app.router.add_route('OPTIONS', '/api/mp3/gerar',       lambda r: web.Response(status=200, headers={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type'}))
    # ── Bot PIX - página pública @paypix_nexbot ──
    app.router.add_get('/bot',                              route_bot_pix_page)
    app.router.add_get('/pix',                              route_pix_page)
    app.router.add_post('/api/bot/gerar',                   route_bot_gerar)
    app.router.add_get('/api/bot/status/{payment_id}',      route_bot_status)
    app.router.add_get('/api/bot/config',                   route_bot_config_get)
    app.router.add_post('/api/bot/config',                  route_bot_config_save)
    app.router.add_post('/api/bot/config/save-html',        route_bot_config_save_html)
    # ── Recorrência / Assinaturas mensais ────────────────────────────
    app.router.add_post('/api/recorrente/criar',            route_recorrente_criar)
    app.router.add_get('/api/recorrente/listar',            route_recorrente_listar)
    app.router.add_post('/api/recorrente/cancelar',         route_recorrente_cancelar)
    app.router.add_post('/api/recorrente/pausar',           route_recorrente_pausar)
    app.router.add_post('/api/recorrente/cobrar',           route_recorrente_cobrar)
    app.router.add_get('/api/recorrente/stats',             route_recorrente_stats)
    app.router.add_post('/api/recorrente/job',              route_recorrente_job_manual)
    app.router.add_get('/pagar/{id}',                       route_recorrente_pagar_link)
    # ── Autocadastro via QR Code ──────────────────────────────────
    app.router.add_get('/assinar',                          route_assinar_page)
    app.router.add_post('/api/recorrente/autocadastro',     route_autocadastro)
    app.router.add_get('/api/assinar/config',               route_assinar_config_get)
    app.router.add_post('/api/assinar/config',              route_assinar_config_save)
    app.router.add_get('/api/assinar/qr',                   route_assinar_qr_gerado)
    # ── Preapproval (Débito Automático PIX) ──────────────────────────
    app.router.add_post('/api/assinar/criar-plano',         route_preapproval_criar_plano)
    app.router.add_post('/api/assinar/preapproval',         route_preapproval_assinar)
    app.router.add_get('/api/assinar/status/{preapproval_id}', route_preapproval_status)
    app.router.add_get('/api/assinar/plano',                route_preapproval_plano_info)
    app.router.add_post('/webhook/preapproval',             route_preapproval_webhook)
    app.router.add_get('/assinar/obrigado',                 route_assinar_obrigado)
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
    # PayPix VORTEX — Credenciais, Testar Gateway, Afiliados
    app.router.add_get('/api/paypix/vortex/config',           route_paypix_vortex_config_get)
    app.router.add_post('/api/paypix/vortex/config',          route_paypix_vortex_config_save)
    app.router.add_get('/api/paypix/vortex/testar',           route_paypix_vortex_testar)
    app.router.add_get('/api/paypix/afiliados',               route_paypix_afiliados_listar)
    app.router.add_post('/api/paypix/afiliados',              route_paypix_afiliados_criar)
    app.router.add_patch('/api/paypix/afiliados/{codigo}',    route_paypix_afiliados_editar)
    app.router.add_delete('/api/paypix/afiliados/{codigo}',   route_paypix_afiliados_deletar)
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
            'version': 'v20260420-check-auth-v35',
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
    app.router.add_post('/api/sorteio/config/limpar-manuais', route_sorteio_limpar_manuais)
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
    app.router.add_post('/api/railway/set-vars',      route_railway_set_vars)
    app.router.add_post('/api/railway/add-domain',    route_railway_add_domain)
    app.router.add_get('/api/railway/domain-status',  route_railway_domain_status)
    app.router.add_get('/api/railway/info',           route_railway_get_info)
    app.router.add_post('/api/admin/db-migrate', route_db_migrate)
    app.router.add_post('/api/admin/patch-sorteio-html', route_patch_sorteio_html)
    app.router.add_post('/api/admin/patch-paypix-html', route_patch_paypix_html)
    app.router.add_post('/api/admin/patch-admin-html',  route_patch_admin_html)
    # Campeonatos / Ligas
    app.router.add_get('/api/admin/ligas',              route_admin_ligas_list)
    app.router.add_post('/api/admin/ligas/suspender',   route_admin_ligas_suspender)
    app.router.add_post('/api/admin/ligas/config',      route_admin_ligas_config)
    # Usuários
    app.router.add_get('/api/admin/usuarios',           route_admin_usuarios_list)
    app.router.add_post('/api/admin/usuarios/ajustar',  route_admin_usuarios_ajustar)
    app.router.add_post('/api/admin/usuarios/suspender',route_admin_usuarios_suspender)
    # Margem de lucro por liga
    app.router.add_get('/api/admin/bet/margem',         route_admin_margem_get)
    app.router.add_post('/api/admin/bet/margem',        route_admin_margem_set)
    # Dashboard financeiro de apostas
    app.router.add_get('/api/admin/bet/dashboard',      route_admin_bet_dashboard)
    # Resolver apostas
    app.router.add_post('/api/admin/bet/resolver',      route_admin_bet_resolver)
    app.router.add_post('/api/admin/bet/resolver-auto', route_admin_bet_resolver_auto)
    # Charts / P&L
    app.router.add_get('/api/admin/bet/charts',         route_admin_bet_charts)
    app.router.add_get('/api/admin/bet/pnl',            route_admin_bet_pnl)
    # Exportar CSV
    app.router.add_get('/api/admin/export/apostas.csv',   route_admin_export_apostas_csv)
    app.router.add_get('/api/admin/export/usuarios.csv',  route_admin_export_usuarios_csv)
    app.router.add_get('/api/admin/export/depositos.csv', route_admin_export_depositos_csv)
    # Limites por liga e por usuário
    app.router.add_get('/api/admin/bet/limites',        route_admin_limites_get)
    app.router.add_post('/api/admin/bet/limites',       route_admin_limites_set)
    # Sistema de Bônus
    app.router.add_get('/api/admin/bonus',              route_admin_bonus_list)
    app.router.add_post('/api/admin/bonus/criar',       route_admin_bonus_criar)
    app.router.add_post('/api/admin/bonus/cancelar',    route_admin_bonus_cancelar)
    # Alertas de Risco
    app.router.add_get('/api/admin/alertas',            route_admin_alertas)
    app.router.add_post('/api/admin/alertas/ignorar',   route_admin_alertas_ignorar)
    # Jogo Responsável
    app.router.add_get('/api/admin/jogo-responsavel',   route_admin_jogo_resp_list)
    app.router.add_post('/api/admin/jogo-responsavel',  route_admin_jogo_resp_set)
    # Afiliados
    app.router.add_get('/api/admin/afiliados',          route_admin_afiliados_list)
    # Notificações Telegram admin
    app.router.add_post('/api/admin/notif-config',      route_admin_notif_config)
    app.router.add_get('/api/admin/notif-config',       route_admin_notif_config_get)

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

    # ── Job automático de cobranças recorrentes (a cada 6h) ──
    try:
        from mp2_api import init_recorrentes_db
        init_recorrentes_db()
        asyncio.create_task(_background_job_recorrente(None))
        print('✅ [recorrente] Job automático de cobranças agendado (6h)', flush=True)
    except Exception as e_rec:
        print(f'⚠️ [recorrente] Erro ao iniciar job: {e_rec}', flush=True)

    # ── Worker Apify: FlashScore ao vivo a cada 2 minutos ───────────────────────
    async def _apify_live_worker():
        """Chama Apify FlashScore Scraper Live a cada 2 min e salva no DB."""
        import aiohttp, json as _json, time as _t
        await asyncio.sleep(10)  # aguarda servidor estabilizar
        print('[apify] Worker ao vivo iniciado (intervalo: 2 min)', flush=True)

        while True:
            try:
                # Obter token: env var > banco de dados
                apify_tok = os.environ.get('APIFY_TOKEN', '') or _APIFY_TOKEN
                if not apify_tok:
                    try:
                        import psycopg2 as _pg2
                        _db_url = DATABASE_URL or _BET_DB_URL_FALLBACK
                        _conn = _pg2.connect(_db_url, connect_timeout=5)
                        _cur  = _conn.cursor()
                        _cur.execute("SELECT value FROM apify_config WHERE key='APIFY_TOKEN'")
                        _row = _cur.fetchone()
                        _cur.close(); _conn.close()
                        if _row:
                            apify_tok = _row[0]
                    except Exception as e_tok:
                        print(f'[apify] Erro ler token do DB: {e_tok}', flush=True)

                if not apify_tok:
                    print('[apify] ⚠️ Token não configurado. Configure APIFY_TOKEN no Railway.', flush=True)
                    await asyncio.sleep(_APIFY_INTERVAL)
                    continue

                # ── Disparar execução do actor ──
                async with aiohttp.ClientSession() as sess:
                    run_url = f'https://api.apify.com/v2/acts/{_APIFY_ACTOR_ID}/runs?token={apify_tok}'
                    async with sess.post(run_url,
                                         json={'maxItems': 50},
                                         timeout=aiohttp.ClientTimeout(total=15)) as r:
                        run_data = (await r.json()).get('data', {})
                    run_id = run_data.get('id')
                    if not run_id:
                        raise Exception(f'Run ID não retornado: {run_data}')
                    print(f'[apify] Run iniciado: {run_id}', flush=True)

                    # ── Aguardar conclusão (máx 90s) ──
                    for _ in range(18):
                        await asyncio.sleep(5)
                        status_url = f'https://api.apify.com/v2/acts/{_APIFY_ACTOR_ID}/runs/{run_id}?token={apify_tok}'
                        async with sess.get(status_url, timeout=aiohttp.ClientTimeout(total=10)) as rs:
                            run_status = (await rs.json()).get('data', {}).get('status', '')
                        if run_status in ('SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'):
                            break

                    if run_status != 'SUCCEEDED':
                        raise Exception(f'Run terminou com status: {run_status}')

                    # ── Ler dataset ──
                    dataset_url = (f'https://api.apify.com/v2/acts/{_APIFY_ACTOR_ID}/runs/{run_id}'
                                   f'/dataset/items?token={apify_tok}&limit=200&clean=true')
                    async with sess.get(dataset_url, timeout=aiohttp.ClientTimeout(total=15)) as rd:
                        raw_items = await rd.json()

                # ── Normalizar itens do FlashScore ──
                jogos = []
                for item in (raw_items if isinstance(raw_items, list) else []):
                    sport     = (item.get('sport') or item.get('sportType') or '').lower()
                    home      = item.get('homeTeam') or item.get('home_team') or item.get('homeName', '')
                    away      = item.get('awayTeam') or item.get('away_team') or item.get('awayName', '')
                    h_score   = item.get('homeScore') or item.get('home_score') or item.get('homeGoals')
                    a_score   = item.get('awayScore') or item.get('away_score') or item.get('awayGoals')
                    elapsed   = item.get('elapsed') or item.get('minute') or item.get('clock', '')
                    league    = item.get('league') or item.get('leagueName') or item.get('tournament', 'Ao Vivo')
                    status    = (item.get('status') or item.get('matchStatus') or 'live').lower()
                    h_logo    = item.get('homeLogo') or item.get('home_logo', '')
                    a_logo    = item.get('awayLogo') or item.get('away_logo', '')

                    if not home or not away:
                        continue
                    # Filtrar só jogos em andamento
                    if status in ('finished', 'ft', 'ended', 'postponed', 'cancelled'):
                        continue

                    # Odds simuladas determinísticas
                    seed = sum(ord(c) for c in (home + away))
                    r1   = ((seed * 17 + 3) % 100) / 100
                    r2   = ((seed * 31 + 7) % 100) / 100

                    jogos.append({
                        'id':           item.get('id') or f'{home}_{away}',
                        'home_team':    home,
                        'away_team':    away,
                        'home_logo':    h_logo,
                        'away_logo':    a_logo,
                        'home_goals':   h_score,
                        'away_goals':   a_score,
                        'elapsed':      elapsed,
                        'league':       league,
                        'sport':        sport or 'soccer',
                        'is_live':      True,
                        'status':       'live',
                        'source':       'flashscore',
                        'odds': {
                            'home': round(1.4 + r1 * 2.2, 2),
                            'draw': round(2.8 + (seed % 20) / 10, 2),
                            'away': round(1.4 + r2 * 2.2, 2),
                        },
                        'odds_simulated': True,
                    })

                print(f'[apify] ✅ {len(jogos)} jogos ao vivo normalizados', flush=True)

                # ── Salvar no banco PostgreSQL ──
                try:
                    import psycopg2 as _pg2
                    _db_url = DATABASE_URL or _BET_DB_URL_FALLBACK
                    _conn = _pg2.connect(_db_url, connect_timeout=5)
                    _cur  = _conn.cursor()
                    _cur.execute("""
                        UPDATE live_cache
                        SET jogos=%s, total=%s, updated_at=NOW(), source='flashscore'
                        WHERE id=1
                    """, (_json.dumps(jogos, ensure_ascii=False), len(jogos)))
                    _conn.commit(); _cur.close(); _conn.close()
                    print(f'[apify] DB atualizado: {len(jogos)} jogos', flush=True)
                except Exception as e_db:
                    print(f'[apify] Erro salvar DB: {e_db}', flush=True)

                # ── Atualizar cache em memória ──
                _apify_live_cache['ts']     = _t.time()
                _apify_live_cache['jogos']  = jogos
                _apify_live_cache['run_id'] = run_id
                _apify_live_cache['status'] = 'idle'

            except Exception as e_worker:
                print(f'[apify] ❌ Erro no worker: {e_worker}', flush=True)
                _apify_live_cache['status'] = 'error'

            # Aguardar 2 minutos para próxima execução
            await asyncio.sleep(_APIFY_INTERVAL)

    asyncio.create_task(_apify_live_worker())

    # ── Warm-up do cache ESPN (pré-carrega tabelas para evitar erro no primeiro acesso) ──
    async def _warmup_espn_cache():
        import aiohttp, json as _json
        await asyncio.sleep(5)  # aguarda servidor estabilizar
        print('[warmup] Iniciando pré-carga das tabelas ESPN...', flush=True)
        ligas_warmup = list(_ESPN_LEAGUES.keys())
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as sess:
            for lk in ligas_warmup:
                if lk in _espn_table_cache:
                    continue
                cfg = _ESPN_LEAGUES.get(lk)
                if not cfg:
                    continue
                espn_slug, league_name = cfg
                url = f'{_ESPN_STANDINGS_BASE}/{espn_slug}/standings'
                try:
                    async with sess.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                        raw = await r.read()
                        if not raw:
                            continue
                        d = _json.loads(raw)
                    children = d.get('children', [])
                    multi_group = len(children) > 1
                    grupos = []
                    all_rows = []
                    for grp in children:
                        grp_name = grp.get('name', '')
                        entries = grp.get('standings', {}).get('entries', [])
                        grp_rows = []
                        rank = 1
                        for entry in entries:
                            team = entry.get('team', {})
                            stats_list = entry.get('stats', [])
                            stats = {s['name']: s.get('value', 0) for s in stats_list}
                            logo = team.get('logo', '')
                            if not logo and team.get('logos'):
                                logo = team['logos'][0].get('href', '')
                            gf = int(stats.get('pointsFor', 0) or 0)
                            ga = int(stats.get('pointsAgainst', 0) or 0)
                            row = {
                                'rank': rank, 'name': team.get('displayName', team.get('name', '')),
                                'logo': logo, 'points': int(stats.get('points', 0) or 0),
                                'played': int(stats.get('gamesPlayed', 0) or 0),
                                'won': int(stats.get('wins', 0) or 0), 'draw': int(stats.get('ties', 0) or 0),
                                'lost': int(stats.get('losses', 0) or 0), 'gf': gf, 'ga': ga, 'gd': gf - ga,
                                'form': '', 'description': '',
                            }
                            grp_rows.append(row)
                            all_rows.append(row)
                            rank += 1
                        if grp_rows:
                            grupos.append({'name': grp_name, 'rows': grp_rows})
                    result_data = {
                        'success': True, 'standings': all_rows, 'league': league_name,
                        'grupos': grupos if multi_group else [], 'has_groups': multi_group,
                    }
                    import time as _wt
                    _espn_table_cache[lk] = {'ts': _wt.time(), 'data': result_data}
                    print(f'[warmup] {league_name}: {len(all_rows)} times OK', flush=True)
                    await asyncio.sleep(0.5)  # não sobrecarregar a ESPN
                except Exception as e_wu:
                    print(f'[warmup] {league_name} (id={lk}) erro: {e_wu}', flush=True)
        print('[warmup] Cache ESPN pronto!', flush=True)

    asyncio.create_task(_warmup_espn_cache())

    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
# deploy Sun Apr 12 13:09:00 UTC 2026


# rebuild-1776092829

# rebuild 20260413160152
# rebuild-1776100930
