# -*- coding: utf-8 -*-
"""
db_manager_pg.py - PostgreSQL 資料庫管理模組（雲端版）
對應本機 SQLite 的 db_manager.py，API 完全相同。
連線字串從環境變數 DATABASE_URL 讀取。
"""

import os
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
# Render 提供的 URL 開頭是 postgres:// ，SQLAlchemy / psycopg2 需要 postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


# ======================================================
# 連線管理
# ======================================================
@contextmanager
def get_conn():
    url = DATABASE_URL.replace("postgresql://", "postgres://", 1)   # psycopg2 用 postgres://
    conn = psycopg2.connect(url)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ======================================================
# 建立資料表
# ======================================================
DDL = [
    """
    CREATE TABLE IF NOT EXISTS cb_list (
        cb_id            VARCHAR(20) NOT NULL,
        stock_id         VARCHAR(10) NOT NULL,
        cb_name          TEXT,
        reference_price  REAL,
        conversion_price REAL,
        due_date         TEXT,
        updated_date     TEXT NOT NULL,
        PRIMARY KEY (cb_id, updated_date)
    )""",
    """
    CREATE TABLE IF NOT EXISTS stock_price (
        stock_id  VARCHAR(10) NOT NULL,
        date      DATE        NOT NULL,
        open      REAL,
        high      REAL,
        low       REAL,
        close     REAL,
        volume    REAL,
        PRIMARY KEY (stock_id, date)
    )""",
    """
    CREATE TABLE IF NOT EXISTS institutional (
        stock_id    VARCHAR(10) NOT NULL,
        date        DATE        NOT NULL,
        foreign_net REAL DEFAULT 0,
        trust_net   REAL DEFAULT 0,
        dealer_net  REAL DEFAULT 0,
        inst_total  REAL DEFAULT 0,
        PRIMARY KEY (stock_id, date)
    )""",
    """
    CREATE TABLE IF NOT EXISTS shareholding (
        stock_id       VARCHAR(10) NOT NULL,
        date           DATE        NOT NULL,
        big_holder_pct REAL,
        retail_pct     REAL,
        shareholders   INTEGER,
        PRIMARY KEY (stock_id, date)
    )""",
    """
    CREATE TABLE IF NOT EXISTS margin (
        stock_id       VARCHAR(10) NOT NULL,
        date           DATE        NOT NULL,
        margin_balance REAL,
        short_balance  REAL,
        PRIMARY KEY (stock_id, date)
    )""",
    """
    CREATE TABLE IF NOT EXISTS cb_price (
        cb_id            VARCHAR(20) NOT NULL,
        stock_id         VARCHAR(10) NOT NULL,
        date             DATE        NOT NULL,
        reference_price  REAL,
        conversion_price REAL,
        coupon_rate      REAL DEFAULT 0,
        PRIMARY KEY (cb_id, date)
    )""",
    """
    CREATE TABLE IF NOT EXISTS sync_log (
        id            SERIAL PRIMARY KEY,
        run_at        TEXT NOT NULL,
        stock_id      TEXT,
        table_name    TEXT NOT NULL,
        rows_upserted INTEGER DEFAULT 0,
        status        TEXT    DEFAULT 'ok',
        message       TEXT
    )""",
    # 索引
    "CREATE INDEX IF NOT EXISTS idx_price_stock_date   ON stock_price   (stock_id, date)",
    "CREATE INDEX IF NOT EXISTS idx_inst_stock_date    ON institutional  (stock_id, date)",
    "CREATE INDEX IF NOT EXISTS idx_share_stock_date   ON shareholding   (stock_id, date)",
    "CREATE INDEX IF NOT EXISTS idx_margin_stock_date  ON margin         (stock_id, date)",
    "CREATE INDEX IF NOT EXISTS idx_cbprice_stock_date ON cb_price       (stock_id, date)",
]


def init_db():
    """建立所有資料表（程式啟動時呼叫一次即可）"""
    with get_conn() as conn:
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
    print("[DB] PostgreSQL tables initialized.")


# ======================================================
# 寫入（Upsert）
# ======================================================
def upsert_prices(rows: list) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
        VALUES (%(stock_id)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume
    """
    with get_conn() as conn:
        psycopg2.extras.execute_batch(conn.cursor(), sql, rows, page_size=500)
    return len(rows)


def upsert_institutional(rows: list) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO institutional (stock_id, date, foreign_net, trust_net, dealer_net, inst_total)
        VALUES (%(stock_id)s, %(date)s, %(foreign_net)s, %(trust_net)s, %(dealer_net)s, %(inst_total)s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            foreign_net=EXCLUDED.foreign_net, trust_net=EXCLUDED.trust_net,
            dealer_net=EXCLUDED.dealer_net,   inst_total=EXCLUDED.inst_total
    """
    with get_conn() as conn:
        psycopg2.extras.execute_batch(conn.cursor(), sql, rows, page_size=500)
    return len(rows)


def upsert_shareholding(rows: list) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO shareholding (stock_id, date, big_holder_pct, retail_pct, shareholders)
        VALUES (%(stock_id)s, %(date)s, %(big_holder_pct)s, %(retail_pct)s, %(shareholders)s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            big_holder_pct=EXCLUDED.big_holder_pct,
            retail_pct=EXCLUDED.retail_pct,
            shareholders=EXCLUDED.shareholders
    """
    with get_conn() as conn:
        psycopg2.extras.execute_batch(conn.cursor(), sql, rows, page_size=500)
    return len(rows)


def upsert_margin(rows: list) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO margin (stock_id, date, margin_balance, short_balance)
        VALUES (%(stock_id)s, %(date)s, %(margin_balance)s, %(short_balance)s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            margin_balance=EXCLUDED.margin_balance,
            short_balance=EXCLUDED.short_balance
    """
    with get_conn() as conn:
        psycopg2.extras.execute_batch(conn.cursor(), sql, rows, page_size=500)
    return len(rows)


def upsert_cb_price(rows: list) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO cb_price (cb_id, stock_id, date, reference_price, conversion_price, coupon_rate)
        VALUES (%(cb_id)s, %(stock_id)s, %(date)s, %(reference_price)s, %(conversion_price)s, %(coupon_rate)s)
        ON CONFLICT (cb_id, date) DO UPDATE SET
            reference_price=EXCLUDED.reference_price,
            conversion_price=EXCLUDED.conversion_price,
            coupon_rate=EXCLUDED.coupon_rate
    """
    with get_conn() as conn:
        psycopg2.extras.execute_batch(conn.cursor(), sql, rows, page_size=500)
    return len(rows)


def upsert_cb_list(rows: list) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO cb_list (cb_id, stock_id, cb_name, reference_price,
                             conversion_price, due_date, updated_date)
        VALUES (%(cb_id)s, %(stock_id)s, %(cb_name)s, %(reference_price)s,
                %(conversion_price)s, %(due_date)s, %(updated_date)s)
        ON CONFLICT (cb_id, updated_date) DO UPDATE SET
            reference_price=EXCLUDED.reference_price,
            conversion_price=EXCLUDED.conversion_price,
            cb_name=EXCLUDED.cb_name
    """
    with get_conn() as conn:
        psycopg2.extras.execute_batch(conn.cursor(), sql, rows, page_size=500)
    return len(rows)


def log_sync(table_name: str, rows_upserted: int, stock_id: str = None,
             status: str = "ok", message: str = None):
    sql = """
        INSERT INTO sync_log (run_at, stock_id, table_name, rows_upserted, status, message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        conn.cursor().execute(sql, (
            datetime.now().isoformat(), stock_id, table_name,
            rows_upserted, status, message
        ))


# ======================================================
# 讀取（供回測 / 更新腳本使用）
# ======================================================
def _fetchall_as_dicts(sql: str, params: tuple) -> list:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def read_prices(stock_id: str, start_date: str, end_date: str) -> list:
    return _fetchall_as_dicts(
        "SELECT date::text, open, high, low, close, volume FROM stock_price "
        "WHERE stock_id=%s AND date BETWEEN %s AND %s ORDER BY date",
        (stock_id, start_date, end_date)
    )


def read_institutional(stock_id: str, start_date: str, end_date: str) -> list:
    return _fetchall_as_dicts(
        "SELECT date::text, foreign_net, trust_net, dealer_net, inst_total FROM institutional "
        "WHERE stock_id=%s AND date BETWEEN %s AND %s ORDER BY date",
        (stock_id, start_date, end_date)
    )


def read_shareholding(stock_id: str, start_date: str, end_date: str) -> list:
    return _fetchall_as_dicts(
        "SELECT date::text, big_holder_pct, retail_pct, shareholders FROM shareholding "
        "WHERE stock_id=%s AND date BETWEEN %s AND %s ORDER BY date",
        (stock_id, start_date, end_date)
    )


def read_margin(stock_id: str, start_date: str, end_date: str) -> list:
    return _fetchall_as_dicts(
        "SELECT date::text, margin_balance, short_balance FROM margin "
        "WHERE stock_id=%s AND date BETWEEN %s AND %s ORDER BY date",
        (stock_id, start_date, end_date)
    )


def get_latest_date(table: str, stock_id: str):
    # table 名稱不能用參數，用白名單防止 SQL injection
    allowed = {"stock_price", "institutional", "shareholding", "margin", "cb_price"}
    if table not in allowed:
        raise ValueError(f"Unknown table: {table}")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(date)::text FROM {table} WHERE stock_id=%s", (stock_id,))
        row = cur.fetchone()
    return row[0] if row else None


def get_latest_cb_price_date(cb_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(date)::text FROM cb_price WHERE cb_id=%s", (cb_id,))
        row = cur.fetchone()
    return row[0] if row else None


def get_all_cb_stocks() -> list:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT stock_id FROM cb_list ORDER BY stock_id")
        return [r[0] for r in cur.fetchall()]


def get_all_cb_ids_for_stock(stock_id: str) -> list:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT cb_id FROM cb_list WHERE stock_id=%s ORDER BY cb_id",
            (stock_id,)
        )
        return [r[0] for r in cur.fetchall()]


def get_db_stats() -> dict:
    tables = ["cb_list", "cb_price", "stock_price", "institutional",
              "shareholding", "margin", "sync_log"]
    stats = {}
    with get_conn() as conn:
        cur = conn.cursor()
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                stats[t] = cur.fetchone()[0]
            except Exception:
                stats[t] = 0
    return stats
