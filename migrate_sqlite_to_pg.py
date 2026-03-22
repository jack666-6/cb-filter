# -*- coding: utf-8 -*-
"""
migrate_sqlite_to_pg.py - 一次性：將本機 SQLite 資料庫移轉到雲端 PostgreSQL

用法（在本機執行，需先設定環境變數或直接填入 DATABASE_URL）：
  set DATABASE_URL=postgresql://user:pass@host:5432/dbname
  python migrate_sqlite_to_pg.py

或直接修改下方 SQLITE_PATH 和 PG_URL 變數。
"""

import os
import sqlite3
import sys
import time

import psycopg2
import psycopg2.extras

# ── 請修改這兩個值 ────────────────────────────────────────────────────────
SQLITE_PATH = r"C:\Users\User\OneDrive\桌面\程式\back\market_data.db"
PG_URL      = os.environ.get("DATABASE_URL", "")   # 也可以直接貼 Supabase 連線字串
# ─────────────────────────────────────────────────────────────────────────

if PG_URL.startswith("postgresql://"):
    PG_URL = PG_URL.replace("postgresql://", "postgres://", 1)


def pg_conn():
    return psycopg2.connect(PG_URL)


def read_sqlite_table(sqlite_conn, table: str, offset: int = 0, limit: int = 10000):
    cur = sqlite_conn.cursor()
    cur.execute(f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}")
    cols = [d[0] for d in cur.description]
    return cols, cur.fetchall()


def count_sqlite(sqlite_conn, table: str) -> int:
    cur = sqlite_conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    except Exception:
        return 0


def migrate_table(sqlite_conn, pg, table: str, batch: int = 2000):
    total = count_sqlite(sqlite_conn, table)
    if total == 0:
        print(f"  {table}: 0 rows (skip)")
        return

    print(f"  {table}: {total:,} rows → migrating...", end=" ", flush=True)
    migrated = 0
    offset = 0

    while True:
        cols, rows = read_sqlite_table(sqlite_conn, table, offset, batch)
        if not rows:
            break

        # 建立 INSERT 語句（ON CONFLICT DO NOTHING，避免重複執行時報錯）
        col_names = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
               f"ON CONFLICT DO NOTHING")

        cur = pg.cursor()
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        pg.commit()

        migrated += len(rows)
        offset   += batch
        print(f"{migrated:,}...", end=" ", flush=True)

        if len(rows) < batch:
            break

    print(f"done ({migrated:,})")


def init_pg_tables(pg):
    """在 PostgreSQL 建立所有資料表（如不存在）"""
    import db_manager_pg as dbpg
    # 暫時繞過 get_conn() 用我們已有的 pg 連線
    cur = pg.cursor()
    for stmt in dbpg.DDL:
        cur.execute(stmt)
    pg.commit()
    print("  PostgreSQL tables created/verified.")


def main():
    if not PG_URL:
        print("❌ 請設定 DATABASE_URL 環境變數或修改腳本中的 PG_URL 變數")
        sys.exit(1)

    if not os.path.exists(SQLITE_PATH):
        print(f"❌ SQLite 檔案不存在: {SQLITE_PATH}")
        sys.exit(1)

    print(f"SQLite: {SQLITE_PATH}")
    print(f"Target: {PG_URL[:40]}...")
    print()

    # 建立連線
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    pg = psycopg2.connect(PG_URL)

    try:
        print("[1/2] 建立 PostgreSQL 資料表...")
        init_pg_tables(pg)

        print("[2/2] 搬移資料...")
        tables = [
            "cb_list",
            "stock_price",
            "institutional",
            "shareholding",
            "margin",
            "cb_price",
        ]
        # sync_log 不搬（讓雲端重新記錄）

        for t in tables:
            try:
                migrate_table(sqlite_conn, pg, t)
            except Exception as e:
                print(f"\n  [ERROR] {t}: {e}")

        print("\n✅ 移轉完成！")

    finally:
        sqlite_conn.close()
        pg.close()


if __name__ == "__main__":
    main()
