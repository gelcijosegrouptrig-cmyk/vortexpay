#!/usr/bin/env python3
"""Salva server.py no PostgreSQL tabela paginas_html"""
import psycopg2

DATABASE_URL = 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'

with open('server.py', 'r', encoding='utf-8') as f:
    content = f.read()

conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
cur = conn.cursor()

cur.execute(
    "INSERT INTO paginas_html (chave, conteudo) VALUES ('server_py_patch', %s) ON CONFLICT (chave) DO UPDATE SET conteudo = EXCLUDED.conteudo",
    (content,)
)
conn.commit()
cur.execute("SELECT length(conteudo), atualizado_em FROM paginas_html WHERE chave='server_py_patch'")
row = cur.fetchone()
print(f'OK server.py salvo: {row[0]} chars em {row[1]}')
cur.close()
conn.close()
