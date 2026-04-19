from __future__ import annotations

import os
import sqlite3
from pathlib import Path

try:
    import psycopg2
except Exception as exc:
    raise SystemExit('Falta psycopg2-binary. Ejecuta: python3 -m pip install psycopg2-binary') from exc

from app import DB_PATH, init_db, DBAdapter

SQLITE_PATH = Path(os.environ.get('SQLITE_PATH', str(DB_PATH)))
POSTGRES_URL = os.environ.get('DATABASE_URL')

if not POSTGRES_URL or not (POSTGRES_URL.startswith('postgresql://') or POSTGRES_URL.startswith('postgres://')):
    raise SystemExit('Define DATABASE_URL con una URL PostgreSQL, por ejemplo: postgresql://usuario:clave@localhost/contabilidad')

if not SQLITE_PATH.exists():
    raise SystemExit(f'No se encontró la base SQLite: {SQLITE_PATH}')

src = sqlite3.connect(SQLITE_PATH)
src.row_factory = sqlite3.Row

dst = DBAdapter(POSTGRES_URL)
init_db(dst)

TABLES = ['actividades', 'movimientos', 'alumnos', 'pagos_alumnos', 'usuarios']


def copy_table(name: str):
    rows = src.execute(f'SELECT * FROM {name} ORDER BY id').fetchall()
    if not rows:
        return 0
    cols = rows[0].keys()
    col_sql = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    insert_sql = f'INSERT INTO {name} ({col_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
    cur = dst.conn.cursor()
    for row in rows:
        cur.execute(insert_sql, [row[c] for c in cols])
    return len(rows)

try:
    counts = {table: copy_table(table) for table in TABLES}
    dst.commit()

    # Ajustar secuencias
    cur = dst.conn.cursor()
    for table in TABLES:
        cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE((SELECT MAX(id) FROM {table}), 1), true)")
    dst.commit()
except Exception:
    dst.rollback()
    raise
finally:
    src.close()
    dst.close()

print('Migración completada.')
for k, v in counts.items():
    print(f'- {k}: {v} registros')
