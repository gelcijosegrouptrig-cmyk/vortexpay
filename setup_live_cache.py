#!/usr/bin/env python3
"""Setup tabelas live_cache e apify_config no PostgreSQL"""
import psycopg2

DB = 'postgresql://postgres:EfJgSbrAkQbFlQJWdxIpIZftseKsDVKs@metro.proxy.rlwy.net:53914/railway'

conn = psycopg2.connect(DB, connect_timeout=10)
cur = conn.cursor()

# Criar tabela live_cache
cur.execute("""
    CREATE TABLE IF NOT EXISTS live_cache (
        id          SERIAL PRIMARY KEY,
        source      TEXT NOT NULL DEFAULT 'apify',
        jogos       JSONB NOT NULL DEFAULT '[]',
        total       INT NOT NULL DEFAULT 0,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
""")
print("✅ Tabela live_cache criada")

# Criar tabela apify_config
cur.execute("""
    CREATE TABLE IF NOT EXISTS apify_config (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
""")
print("✅ Tabela apify_config criada")

# Salvar token Apify
cur.execute("""
    INSERT INTO apify_config (key, value)
    VALUES (%s, %s)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
""", ('APIFY_TOKEN', 'SEU_TOKEN_APIFY_AQUI'))  # Substituir pelo token real
print("✅ APIFY_TOKEN salvo no banco")

# Inserir linha inicial no live_cache (id=1 sempre é o cache atual)
cur.execute("""
    INSERT INTO live_cache (id, source, jogos, total)
    VALUES (1, 'apify', '[]', 0)
    ON CONFLICT (id) DO NOTHING
""")
print("✅ Linha inicial live_cache criada (id=1)")

conn.commit()
cur.close()
conn.close()
print("\n🎉 Setup completo!")
