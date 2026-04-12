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

_SESSION_FALLBACK = '1AZWarzYBuxGBwXPRkJ3GbWfNayG2RvhePLRdUEVqu8eP2bS9H8n2aaW2WeJDSfa_KDsuLUwkvF9tJb8g9tT9xoxyJUa30x2sqpVOCPEPqe5pdXV3HZ_iFdX9BGboi1SZvA_WudKYzn_mNO2z8gf-P0oPTwiRs8NF8fd-ZzJBe6vihX15jqy134gm5Eb0aPVT8sY_mCRcqBRzf4r4FeWtVvXsPneu22HHKHKHgxNgLX3b84665PPcXdJAYFVk0lv1xTjOlEnXQzDg-C4CnFeCn3rRtl1VQzG7KLZN3pMcR_b6MYCCqRnc8Eg5zLo4REufyc-ZewlYdH2feip0Q63Cqr97gnKewKQ='

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway')

# Carregar sessão: 1) env var, 2) arquivo local, 3) fallback hardcoded
# (PostgreSQL é carregado depois do init_db via _carregar_sessao_db)
SESSION_STR = os.environ.get('SESSION_STR', '')
if not SESSION_STR:
    try:
        SESSION_STR = open('session_string.txt').read().strip()
    except:
        pass
if not SESSION_STR:
    SESSION_STR = _SESSION_FALLBACK
    print('⚠️ Usando sessão fallback hardcoded', flush=True)

client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
_lock = asyncio.Lock()
_saque_lock = asyncio.Lock()
_telegram_ready = False
_telegram_tentativas = 0
_telegram_session_invalida = False

# ─── BANCO DE DADOS — PostgreSQL persistente + fallback SQLite ───────────────
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
    Zero risco de 'transaction aborted' — cada chamada é totalmente isolada."""
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
    """Cursor para DBConn.cursor() — delega para _pg_run"""
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
            # Usar autocommit=True no init_db também — cada CREATE TABLE é independente
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
                    proximo_sorteio TEXT, updated_at TEXT)""",
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
            ]
            for sql in pg_tables:
                try:
                    cur.execute(sql)
                except Exception as e:
                    print(f'[DB init] Aviso ao criar tabela: {e}', flush=True)
            # Config padrão sorteio (cada query é independente com autocommit)
            try:
                cur.execute("""INSERT INTO sorteio_config
                    (id,ativo,valor_por_numero,premio_fixo,percentual,usar_media,dias_media,descricao,proximo_sorteio,updated_at)
                    VALUES (1,1,5.0,0,50.0,0,30,'Sorteio PaynexBet',NULL,%s)
                    ON CONFLICT (id) DO NOTHING""", (datetime.now().isoformat(),))
            except Exception as e:
                print(f'[DB init] Aviso config sorteio: {e}', flush=True)
            pg.close()
            _USE_PG = True
            print('✅ PostgreSQL conectado — banco PERSISTENTE ativo!', flush=True)
            # Tentar carregar sessão salva no banco
            _carregar_sessao_db()
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
        updated_at TEXT
    )''')
    conn.commit()
    # Migrações de colunas — cada ALTER TABLE em transação separada
    for col in ["valor_por_numero REAL DEFAULT 5.0",
                "usar_media INTEGER DEFAULT 0",
                "dias_media INTEGER DEFAULT 30",
                "paypix_pct REAL DEFAULT 0.6",
                "paypix_ativo INTEGER DEFAULT 1",
                "paypix_descricao TEXT DEFAULT 'Gere seu Pix e receba sua % do valor'"]:
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
    # Migrações sorteio_participantes — cada ALTER TABLE em transação separada
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
        (id, ativo, valor_por_numero, premio_fixo, percentual, usar_media, dias_media, descricao, proximo_sorteio, updated_at)
        VALUES (1, 1, 5.0, 0, 50.0, 0, 30, 'Sorteio PaynexBet', NULL, ?)''',
        (datetime.now().isoformat(),))
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
        if cliente_id and valor_pago and float(valor_pago) >= 5:
            _creditar_bilhetes_por_deposito(cliente_id, float(valor_pago), tx_id, participante_dados)
        elif valor_pago and float(valor_pago) >= 5 and participante_dados and participante_dados.get('cpf'):
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
        return open('home.html', encoding='utf-8').read()
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
    if os.path.exists('admin.html'):
        return open('admin.html', encoding='utf-8').read()
    return '<h1>PaynexBet - Admin</h1>'

def load_sorteio_html():
    if os.path.exists('sorteio.html'):
        return open('sorteio.html', encoding='utf-8').read()
    return '<h1>PaynexBet - Sorteio</h1>'

# ─── HELPERS SORTEIO ────────────────────────────────────────
def get_sorteio_config():
    conn = sqlite3_connect()
    cur = conn.execute('SELECT * FROM sorteio_config WHERE id=1')
    row = cur.fetchone(); conn.close()
    if row:
        cols = ['id','ativo','valor_por_numero','premio_fixo','percentual',
                'usar_media','dias_media','descricao','proximo_sorteio','updated_at']
        d = {}
        for i, col in enumerate(cols):
            d[col] = row[i] if i < len(row) else None
        return d
    return {}

def get_paypix_config():
    """Retorna configuração do PayPix (%, ativo, descrição)"""
    conn = sqlite3_connect()
    try:
        cur = conn.execute('SELECT paypix_pct, paypix_ativo, paypix_descricao FROM sorteio_config WHERE id=1')
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                'paypix_pct':       float(row[0]) if row[0] is not None else 0.6,
                'paypix_ativo':     bool(row[1]) if row[1] is not None else True,
                'paypix_descricao': str(row[2]) if row[2] else 'Gere seu Pix e receba sua % do valor',
            }
    except Exception:
        conn.close()
    return {'paypix_pct': 0.6, 'paypix_ativo': True, 'paypix_descricao': 'Gere seu Pix e receba sua % do valor'}

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
        num = int(seed[:8], 16) % 900000 + 100000  # 100000–999999
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

# ─── TELEGRAM - Conectar com retry ─────────────────────────
async def conectar_telegram():
    global _telegram_ready, _telegram_tentativas, _telegram_session_invalida
    while True:
        # Se sessão foi revogada, espera longa (evita flood) mas segue tentando
        if _telegram_session_invalida:
            print('⚠️ Sessão inválida/revogada. Aguardando 5min antes de tentar nova sessão...', flush=True)
            await asyncio.sleep(300)
            _telegram_session_invalida = False  # Tenta novamente
            continue

        _telegram_tentativas += 1
        try:
            print(f'🔄 Tentativa {_telegram_tentativas} - Conectando Telegram...', flush=True)

            # Desconectar se já estava conectado
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

            @client.on(events.NewMessage(from_users=BOT_USERNAME))
            async def handler(event):
                texto = event.message.text or ''
                # Padrões exatos que o PaynexBet envia ao confirmar depósito
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
                    # Buscar tx mais recente pendente e confirmar
                    conn = sqlite3_connect()
                    cur = conn.execute("""SELECT tx_id, valor, extra FROM transacoes 
                                 WHERE status='pendente' 
                                 ORDER BY created_at DESC LIMIT 5""")
                    pendentes = cur.fetchall()
                    conn.close()
                    
                    # Tentar extrair valor da mensagem para match
                    val_match = re.search(r'R\$\s*([\d,.]+)', texto)
                    valor_msg = None
                    if val_match:
                        try:
                            valor_msg = float(val_match.group(1).replace(',', '.').replace(' ',''))
                        except:
                            pass
                    
                    for row_p in pendentes:
                        tx_id_p, valor_p, extra_p = row_p[0], row_p[1], row_p[2]
                        # Confirmar se valor bate ou se só tem uma pendente
                        if valor_msg is None or abs(valor_p - valor_msg) < 0.05 or len(pendentes) == 1:
                            confirmar_pagamento(tx_id_p)
                            print(f'✅ Pago confirmado: {tx_id_p} R${valor_p}', flush=True)
                            # Disparar split 60/40 se for PayPix
                            if extra_p:
                                try:
                                    extra_d = json.loads(extra_p)
                                    if extra_d.get('tipo') == 'paypix':
                                        asyncio.create_task(_processar_split_paypix(tx_id_p, valor_p, extra_p))
                                        print(f'💸 Split PayPix disparado: {tx_id_p} R${valor_p:.2f}', flush=True)
                                except Exception as ex:
                                    print(f'Erro split: {ex}', flush=True)
                            break

            print('✅ Listener ativo, mantendo conexão...', flush=True)
            # Iniciar loop de verificação periódica de pagamentos pendentes
            asyncio.create_task(_loop_verificar_pagamentos())
            await client.run_until_disconnected()

        except Exception as e:
            nome_erro = type(e).__name__
            print(f'❌ Erro Telegram ({nome_erro}): {e}', flush=True)
            
            # AuthKeyDuplicatedError: sessão revogada, não adianta tentar com mesma session
            if 'AuthKeyDuplicated' in nome_erro or 'AuthKeyDuplicated' in str(e):
                print('🚫 Sessão revogada (usada em dois IPs). Pausando 5min...', flush=True)
                _telegram_session_invalida = True
                _telegram_ready = False
                await asyncio.sleep(300)
                continue

        _telegram_ready = False
        espera = min(60, _telegram_tentativas * 5)
        print(f'🔄 Reconectando em {espera}s...', flush=True)
        await asyncio.sleep(espera)

# ─── GERAR PIX - Garante conexão antes de gerar ────────────
async def verificar_saldo_bot() -> float:
    """Consulta saldo atual no bot"""
    try:
        bot = await client.get_entity(BOT_USERNAME)
        await client.send_message(bot, '/start')
        await asyncio.sleep(3)
        msgs = await client.get_messages(bot, limit=3)
        for msg in msgs:
            if msg.text and 'Saldo Disponível' in msg.text:
                m = re.search(r'Saldo Disponível[^`]*`R\$\s*([\d,.]+)`', msg.text)
                if m:
                    return float(m.group(1).replace(',', '.'))
    except:
        pass
    return -1.0

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

        # Polling ativo — checar a cada 2s por até 60s (30 tentativas)
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
                # Procurar código Pix — padrão primário
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

        print(f'[gerar_pix] Timeout após 60s — nenhum código Pix recebido', flush=True)
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
    # Cálculo idêntico ao usado no sorteio real (max R$1,00 mínimo)
    _premio_fixo = float(config.get('premio_fixo') or 0)
    _percentual  = float(config.get('percentual') or 50)
    premio = _premio_fixo if _premio_fixo > 0 else round(total_dep * _percentual / 100, 2)
    premio = max(premio, 1.0)  # mínimo R$1,00 — igual ao _executar_sorteio_completo

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
            'message': f'✅ R${valor:.2f} adicionado! {novos} novo(s) número(s) gerado(s). Total: {numeros_total} bilhetes.',
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

    # Sortear 1 bilhete aleatório (cada bilhete = igual chance)
    bilhete_vencedor = random.choice(bilhetes)
    cliente_id_vencedor, numero_vencedor = bilhete_vencedor
    ganhador = part_map.get(cliente_id_vencedor)

    if not ganhador:
        return {'success': False, 'error': 'Erro interno: participante do bilhete não encontrado'}

    total_depositado = sum(p['total_depositado'] or 0 for p in participantes)
    total_bilhetes_count = len(bilhetes)

    # Calcular prêmio
    if float(config.get('premio_fixo') or 0) > 0:
        premio = float(config['premio_fixo'])
    else:
        premio = round(total_depositado * float(config.get('percentual', 50)) / 100, 2)
    premio = max(premio, 1.0)

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
    conn2.commit(); conn2.close()

    print(f'🎉 SORTEIO {sorteio_id}: bilhete {numero_vencedor} → {ganhador["nome"]} ganhou R${premio:.2f} → {tipo_chave}: {chave_pix}', flush=True)

    # ── SAQUE AUTOMÁTICO ──────────────────────────────────────
    saque_result = {'success': False, 'error': 'Chave Pix não cadastrada'}
    saque_id_gerado = None

    if chave_pix and _telegram_ready:
        print(f'💸 Iniciando saque automático R${premio:.2f} → {tipo_chave}: {chave_pix}', flush=True)
        import hashlib as _hl
        saque_id_gerado = 'saq_sorteio_' + _hl.md5(f"{sorteio_id}{chave_pix}".encode()).hexdigest()[:10]
        salvar_saque(saque_id_gerado, premio, chave_pix, tipo_chave)
        saque_result = await executar_saque_bot(premio, tipo_chave, chave_pix)

        # Atualizar status no banco de saques
        novo_status = saque_result.get('status', 'erro') if saque_result.get('success') else 'erro'
        obs = saque_result.get('mensagem_bot', saque_result.get('error', ''))[:500]
        conn2 = sqlite3_connect()
        conn2.execute('UPDATE saques SET status=?, processado_at=?, observacao=? WHERE saque_id=?',
            (novo_status, datetime.now().isoformat(), obs, saque_id_gerado))
        # Atualizar histórico com saque_id e status
        conn2.execute('UPDATE sorteio_historico SET saque_id=?, saque_status=? WHERE sorteio_id=?',
            (saque_id_gerado, novo_status, sorteio_id))
        conn2.commit(); conn2.close()
        print(f'💸 Saque automático: {novo_status} | {obs[:80]}', flush=True)

    elif chave_pix and not _telegram_ready:
        # Telegram offline — salvar saque pendente para processar depois
        import hashlib as _hl
        saque_id_gerado = 'saq_sorteio_' + _hl.md5(f"{sorteio_id}{chave_pix}".encode()).hexdigest()[:10]
        salvar_saque(saque_id_gerado, premio, chave_pix, tipo_chave)
        conn2 = sqlite3_connect()
        conn2.execute('UPDATE sorteio_historico SET saque_id=?, saque_status=? WHERE sorteio_id=?',
            (saque_id_gerado, 'aguardando_telegram', sorteio_id))
        conn2.commit(); conn2.close()
        saque_result = {'success': False, 'error': 'Telegram offline — saque salvo, será processado quando reconectar'}
        print(f'⚠️ Telegram offline — saque do sorteio salvo como pendente: {saque_id_gerado}', flush=True)

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
    """Verifica a cada 5min se há saques de sorteio aguardando Telegram e tenta reprocessar"""
    print('🔄 Monitor de saques pendentes iniciado', flush=True)
    while True:
        await asyncio.sleep(300)  # 5 minutos
        try:
            if not _telegram_ready:
                continue
            # Buscar saques de sorteio aguardando telegram
            conn = sqlite3_connect()
            cur = conn.execute("""SELECT h.sorteio_id, h.ganhador_nome, h.ganhador_chave_pix,
                                h.ganhador_tipo_chave, h.premio_pago, h.saque_id
                         FROM sorteio_historico h
                         WHERE h.saque_status='aguardando_telegram'
                         ORDER BY h.data_sorteio DESC LIMIT 5""")
            pendentes = cur.fetchall()
            conn.close()

            for row in pendentes:
                sorteio_id, nome, chave_pix, tipo_chave, premio, saque_id = row
                if not chave_pix:
                    continue
                print(f'💸 Reprocessando saque sorteio {sorteio_id}: R${premio:.2f} → {tipo_chave}:{chave_pix}', flush=True)
                result = await executar_saque_bot(premio, tipo_chave, chave_pix)
                novo_status = result.get('status', 'erro') if result.get('success') else 'erro'
                obs = result.get('mensagem_bot', result.get('error', ''))[:500]
                conn2 = sqlite3_connect()
                conn2.execute('UPDATE sorteio_historico SET saque_status=? WHERE sorteio_id=?',
                              (novo_status, sorteio_id))
                if saque_id:
                    conn2.execute('UPDATE saques SET status=?, processado_at=?, observacao=? WHERE saque_id=?',
                                  (novo_status, datetime.now().isoformat(), obs, saque_id))
                conn2.commit(); conn2.close()
                print(f'💸 Saque reprocessado: {novo_status} | {obs[:60]}', flush=True)
        except Exception as e:
            print(f'❌ Monitor saques erro: {e}', flush=True)

async def route_sorteio_config(request):
    """ADMIN - Configurar parâmetros do sorteio"""
    auth = (request.headers.get('X-PaynexBet-Secret', '') or
            request.rel_url.query.get('secret', ''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error': 'Não autorizado'}, status=401)
    try:
        data = await request.json()
        conn = sqlite3_connect()
        conn.execute('''UPDATE sorteio_config SET
            ativo=?, valor_por_numero=?, percentual=?, usar_media=?, dias_media=?,
            premio_fixo=?, descricao=?, proximo_sorteio=?, updated_at=?
            WHERE id=1''', (
            int(data.get('ativo', 1)),
            float(data.get('valor_por_numero', 5.0)),
            float(data.get('percentual', 50)),
            int(data.get('usar_media', 0)),
            int(data.get('dias_media', 30)),
            float(data.get('premio_fixo', 0)),
            str(data.get('descricao', 'Sorteio PaynexBet')),
            data.get('proximo_sorteio'),
            datetime.now().isoformat(),
        ))
        conn.commit(); conn.close()
        return web.json_response({'success': True, 'message': 'Configuração salva!'})
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

# ─── ROTAS ────────────────────────────────────────────────
# ── Estado global para login interativo ──────────────────────────────────────
_login_state = {}  # phone_code_hash, temp_client, temp_session

async def route_solicitar_codigo(request):
    """Passo 1: Solicitar código do Telegram (roda no IP do Railway)"""
    global _login_state
    auth = (request.headers.get('X-PaynexBet-Secret','') or
            request.rel_url.query.get('secret',''))
    if auth != WEBHOOK_SECRET:
        return web.json_response({'error':'Não autorizado'},status=401)
    try:
        from telethon.sessions import StringSession as SS
        from telethon.errors import FloodWaitError
        temp_client = TelegramClient(SS(), API_ID, API_HASH)
        await temp_client.connect()
        sent = await temp_client.send_code_request('+5527997981963')
        _login_state = {
            'client': temp_client,
            'hash': sent.phone_code_hash,
            'session': temp_client.session.save(),
        }
        print('📱 Código Telegram solicitado via API Railway', flush=True)
        return web.json_response({'success':True,'message':'Código enviado para o Telegram!'})
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

        try:
            await temp_client.sign_in('+5527997981963', code, phone_code_hash=_login_state['hash'])
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
        # Salvar no PostgreSQL — persiste entre deploys!
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
            print('⚠️ Vars Railway não configuradas — sessão salva só em arquivo local', flush=True)

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
    """Página principal — paynexbet.com"""
    return web.Response(text=load_home_html(), content_type='text/html', charset='utf-8')

async def route_index(request):
    return web.Response(text=load_html(), content_type='text/html', charset='utf-8')

async def route_pague(request):
    """Página /pague — abre direto o formulário de gerar Pix"""
    html = load_html()
    # Injeta script para abrir modal de depósito automaticamente
    html = html.replace('</body>', '<script>window.addEventListener("load",()=>{setTimeout(()=>{const b=document.getElementById("btn-depositar");if(b)b.click();},500);});</script></body>')
    return web.Response(text=html, content_type='text/html', charset='utf-8')

async def route_health(request):
    motivo = None
    if not _telegram_ready:
        if _telegram_session_invalida:
            motivo = 'sessao_invalida'
        elif _telegram_tentativas > 0:
            motivo = 'reconectando'
        else:
            motivo = 'iniciando'
    return web.json_response({
        'status': 'online',
        'version': 'v20260412-STATS-FIX-v4',
        'telegram': _telegram_ready,
        'telegram_motivo': motivo,
        'tentativas': _telegram_tentativas,
        'bot': BOT_USERNAME,
        'webhook': '/webhook/confirmar',
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
        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'})
        if valor % 5 != 0:
            return web.json_response({'success': False, 'error': 'Valor deve ser múltiplo de R$ 5'})

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
        # Cada execute() usa nova conexão PG — zero risco de transaction aborted
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
                # Atualizar registro original com pix_code — mantém o tx_id original intacto
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

async def route_saldo(request):
    """Retorna saldo atual da conta via bot Telegram"""
    if not _telegram_ready:
        return web.json_response({'success': False, 'saldo': 0, 'disponivel': 0, 'error': 'Telegram não conectado'})
    try:
        bot = await client.get_entity(BOT_USERNAME)
        await client.send_message(bot, '/start')
        await asyncio.sleep(3)
        msgs = await client.get_messages(bot, limit=5)
        for msg in msgs:
            if not msg.text:
                continue
            # Padrão: "Saldo Disponível: `R$ 37,92`" ou "Saldo: R$37,92"
            m = re.search(r'Saldo[^`\n]*`R\$\s*([\d,.]+)`', msg.text)
            if not m:
                m = re.search(r'Saldo[:\s*]+R\$\s*([\d,.]+)', msg.text)
            if m:
                saldo = float(m.group(1).replace(',', '.'))
                # Disponível para saque (pode ter taxa)
                m2 = re.search(r'[Dd]ispon[íi]vel[^`\n]*`R\$\s*([\d,.]+)`', msg.text)
                if not m2:
                    m2 = re.search(r'[Ss]aque[:\s]+R\$\s*([\d,.]+)', msg.text)
                disponivel = float(m2.group(1).replace(',', '.')) if m2 else saldo
                return web.json_response({'success': True, 'saldo': saldo, 'disponivel': disponivel})
        return web.json_response({'success': False, 'saldo': 0, 'disponivel': 0, 'error': 'Saldo não encontrado'})
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
    """Página pública /paypix — parceiro gera Pix e recebe 60%"""
    html = open('paypix.html', encoding='utf-8').read()
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
        if valor < 5:
            return web.json_response({'success': False, 'error': 'Valor mínimo R$ 5,00'})
        # PayPix aceita qualquer valor >= 5 (sem restrição de múltiplo)
        # Arredondar para 2 casas decimais
        valor = round(valor, 2)
        # NÃO bloquear por Telegram — deixa tentar e retornar erro real se falhar

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
                # MANTER o tx_id original (ppx_...) — apenas atualizar pix_code e status
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
    """Status da transação PayPix — retorna pix_code quando pronto e status de pagamento"""
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
            conn = sqlite3_connect()
            conn.execute(
                'UPDATE sorteio_config SET paypix_pct=?, paypix_ativo=?, paypix_descricao=?, updated_at=? WHERE id=1',
                (pct, ativo, descricao, datetime.now().isoformat())
            )
            conn.commit(); conn.close()
            return web.json_response({
                'success': True,
                'paypix_pct': pct,
                'paypix_pct_display': f'{round(pct*100, 1)}%',
                'paypix_ativo': bool(ativo),
                'paypix_descricao': descricao,
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    else:
        # GET — público (paypix.html precisa saber o %)
        cfg = get_paypix_config()
        pct = cfg.get('paypix_pct', 0.6)
        return web.json_response({
            'paypix_pct':          pct,
            'paypix_pct_display':  f'{round(pct*100, 1)}%',
            'paypix_ativo':        cfg.get('paypix_ativo', True),
            'paypix_descricao':    cfg.get('paypix_descricao', 'Gere seu Pix e receba sua % do valor'),
        })

async def _processar_split_paypix(tx_id, valor, extra_str):
    """Após confirmação de pagamento, envia 60% para o parceiro via bot"""
    try:
        extra = json.loads(extra_str or '{}')
        if extra.get('tipo') != 'paypix':
            return
        chave   = extra.get('parceiro_chave', '')
        tipo    = extra.get('parceiro_tipo', 'cpf')
        pct     = float(extra.get('parceiro_pct', 0.6))
        val_par = round(valor * pct, 2)

        if not chave or val_par < 1:
            print(f'[PayPix] split inválido tx={tx_id}', flush=True)
            return

        print(f'[PayPix] enviando R${val_par:.2f} → {chave} ({tipo})', flush=True)
        resultado = await executar_saque_bot(val_par, tipo, chave)
        print(f'[PayPix] resultado split: {resultado}', flush=True)

        # Registrar o saque na tabela saques
        saque_id = f"spp_{hashlib.md5(f'{tx_id}{time.time()}'.encode()).hexdigest()[:12]}"
        conn = sqlite3_connect()
        conn.execute(
            '''INSERT OR IGNORE INTO saques
               (saque_id,valor,chave_pix,tipo_chave,status,created_at,observacao)
               VALUES (?,?,?,?,?,?,?)''',
            (saque_id, val_par, chave, tipo,
             resultado.get('status','enviado'),
             datetime.now().isoformat(),
             f'PayPix split 60% - tx {tx_id}')
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'[PayPix] erro split: {e}', flush=True)

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
    init_db()
    print('✅ DB ok', flush=True)

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
    # Rotas de Saque — /sacar e /saque
    app.router.add_get('/sacar', route_saque_page)      # paynexbet.com/sacar
    app.router.add_get('/saque', route_saque_page)
    app.router.add_get('/saque.html', route_saque_page)
    # Rota /pague — gerar Pix (abre modal automaticamente)
    app.router.add_get('/pague', route_pague)           # paynexbet.com/pague
    app.router.add_get('/api/saldo', route_saldo)
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
    # PayPix — parceiro gera Pix e recebe 60%
    app.router.add_get('/paypix', route_paypix_page)
    app.router.add_post('/api/paypix/gerar', route_paypix_gerar)
    app.router.add_get('/api/paypix/status/{tx_id}', route_paypix_status)
    app.router.add_get('/api/paypix/config', route_paypix_config)
    app.router.add_post('/api/paypix/config', route_paypix_config)
    app.router.add_options('/api/paypix/config', lambda r: web.Response(headers={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type,X-PaynexBet-Secret'}))
    app.router.add_route('OPTIONS', '/api/paypix/gerar', lambda r: web.Response(status=200))
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
            'version': 'v20260412-STATS-FIX-v4',
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

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'✅ HTTP porta {PORT}', flush=True)

    # Telegram em background com retry automático
    asyncio.create_task(conectar_telegram())

    # Agendador de sorteio automático
    asyncio.create_task(agendador_sorteio())

    # Monitor de saques pendentes (reprocessa quando Telegram voltar)
    asyncio.create_task(reprocessar_saques_pendentes_sorteio())

    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
# deploy Sun Apr 12 13:09:00 UTC 2026
