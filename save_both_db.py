#!/usr/bin/env python3
"""
Salva server.py e home.html no PostgreSQL nas tabelas CORRETAS:
  - server.py  → paginas_html   (chave: server_py_patch,  coluna: conteudo)
  - home.html  → configuracoes  (chave: home_html_patch,   coluna: valor)
  - admin.html → paginas_html   (chave: admin_html_patch,  coluna: conteudo)
"""
import psycopg2, sys, os

DATABASE_URL = 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'

# (arquivo, tabela, coluna_conteudo, chave)
FILES = [
    ('server.py',  'paginas_html',  'conteudo', 'server_py_patch'),
    ('home.html',  'configuracoes', 'valor',    'home_html_patch'),
    ('admin.html', 'paginas_html',  'conteudo', 'admin_html_patch'),
]

conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
cur  = conn.cursor()

for fname, tabela, coluna, chave in FILES:
    if not os.path.exists(fname):
        print(f'SKIP {fname} (não encontrado)')
        continue
    with open(fname, 'r', encoding='utf-8') as f:
        content = f.read()
    cur.execute(
        f"INSERT INTO {tabela} (chave, {coluna}) VALUES (%s, %s) "
        f"ON CONFLICT (chave) DO UPDATE SET {coluna} = EXCLUDED.{coluna}",
        (chave, content)
    )
    conn.commit()
    cur.execute(f"SELECT length({coluna}) FROM {tabela} WHERE chave=%s", (chave,))
    row = cur.fetchone()
    print(f'OK {fname} → {tabela}.{coluna} [{chave}]: {row[0]} chars')

cur.close()
conn.close()
print('Deploy completo!')
