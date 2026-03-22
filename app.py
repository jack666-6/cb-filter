# -*- coding: utf-8 -*-
"""
app.py - CB 籌碼分析平台（雲端 PostgreSQL 版）
部署於 Render.com，資料庫為 Supabase PostgreSQL。
"""
import os
import math
import time
import threading
from flask import Flask, jsonify, request, render_template
import pandas as pd
import psycopg2
import psycopg2.pool

app = Flask(__name__)

# ── 資料庫連線池 ─────────────────────────────────────────────────────────────
_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if _DATABASE_URL.startswith("postgresql://"):
    _DATABASE_URL = _DATABASE_URL.replace("postgresql://", "postgres://", 1)

_pool = None
_pool_lock = threading.Lock()

def get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=5, dsn=_DATABASE_URL
                )
    return _pool

def db(sql: str, params=None) -> pd.DataFrame:
    pool = get_pool()
    conn = pool.getconn()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    finally:
        pool.putconn(conn)


# ── TTL 快取 ─────────────────────────────────────────────────────────────────
_cache: dict = {}
_cache_lock = threading.Lock()
CACHE_TTL = 3600  # 1 小時

def cache_get(key):
    with _cache_lock:
        item = _cache.get(key)
        if item and time.time() - item["ts"] < CACHE_TTL:
            return item["data"]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}

def cache_clear():
    with _cache_lock:
        _cache.clear()


def clean_records(records):
    """將 NaN/Inf 換成 None，避免 JSON 序列化失敗"""
    cleaned = []
    for row in records:
        cleaned.append({
            k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
            for k, v in row.items()
        })
    return cleaned


@app.errorhandler(500)
def handle_500(e):
    return jsonify({"error": str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"error": str(e)}), 500


# ── 首頁 ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


# ── CB 列表（含溢價率、最新籌碼）─────────────────────────────────────────
def _load_all_cbs():
    """從 DB 撈完整 CB 清單，結果快取 1 小時"""
    cached = cache_get("all_cbs")
    if cached is not None:
        return cached

    df = db("""
        SELECT
            cp.cb_id,
            cp.stock_id,
            cl.cb_name,
            ROUND(cp.reference_price::numeric, 2)                  AS cb_price,
            ROUND(cl.conversion_price::numeric, 2)                 AS conversion_price,
            cl.due_date,
            ROUND(sp.close::numeric, 2)                            AS stock_close,
            CASE
                WHEN cl.conversion_price > 0 AND sp.close > 0 THEN
                    ROUND(
                        ((cp.reference_price / (sp.close / cl.conversion_price * 100) - 1) * 100)::numeric
                    , 2)
                ELSE NULL
            END                                                    AS premium,
            ROUND(sh.big_holder_pct::numeric, 2)                   AS big,
            ROUND(sh.retail_pct::numeric, 2)                       AS small,
            sh.shareholders                                        AS holders
        FROM cb_price cp
        JOIN (
            SELECT DISTINCT ON (cb_id) cb_id, date
            FROM cb_price ORDER BY cb_id, date DESC
        ) mcp ON cp.cb_id = mcp.cb_id AND cp.date = mcp.date
        JOIN (
            SELECT DISTINCT ON (cb_id) cb_id, stock_id, cb_name, conversion_price, due_date
            FROM cb_list ORDER BY cb_id, updated_date DESC
        ) cl ON cp.cb_id = cl.cb_id
        JOIN (
            SELECT DISTINCT ON (stock_id) stock_id, date, close
            FROM stock_price ORDER BY stock_id, date DESC
        ) sp ON cp.stock_id = sp.stock_id
        LEFT JOIN (
            SELECT DISTINCT ON (stock_id) stock_id, big_holder_pct, retail_pct, shareholders
            FROM shareholding ORDER BY stock_id, date DESC
        ) sh ON cp.stock_id = sh.stock_id
        ORDER BY cp.cb_id
    """)

    records = clean_records(df.where(pd.notna(df), None).to_dict("records"))
    cache_set("all_cbs", records)
    return records


@app.route("/api/cbs")
def api_cbs():
    pmin = request.args.get("premium_min",  type=float)
    pmax = request.args.get("premium_max",  type=float)
    cmin = request.args.get("cb_price_min", type=float)
    cmax = request.args.get("cb_price_max", type=float)

    rows = _load_all_cbs()

    # 篩選在記憶體做，不重查 DB
    result = rows
    if cmin is not None:
        result = [r for r in result if r.get("cb_price") is not None and r["cb_price"] >= cmin]
    if cmax is not None:
        result = [r for r in result if r.get("cb_price") is not None and r["cb_price"] <= cmax]
    if pmin is not None:
        result = [r for r in result if r.get("premium") is not None and r["premium"] >= pmin]
    if pmax is not None:
        result = [r for r in result if r.get("premium") is not None and r["premium"] <= pmax]

    return jsonify(result)


# ── 個股 K 線 ────────────────────────────────────────────────────────────
@app.route("/api/stock/<sid>/kline")
def stock_kline(sid):
    n = request.args.get("days", 1000, type=int)
    key = f"kline_{sid}_{n}"
    cached = cache_get(key)
    if cached is not None:
        return jsonify(cached)
    df = db(
        "SELECT date::text AS date, open, high, low, close, volume "
        "FROM stock_price WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    records = df.sort_values("date").to_dict("records")
    cache_set(key, records)
    return jsonify(records)


# ── CB 價格走勢（含理論價）──────────────────────────────────────────────
@app.route("/api/cb/<cid>/price")
def cb_price(cid):
    n = request.args.get("days", 1000, type=int)
    key = f"cbprice_{cid}_{n}"
    cached = cache_get(key)
    if cached is not None:
        return jsonify(cached)
    df = db("""
        SELECT
            cp.date::text AS date,
            cp.reference_price,
            ROUND(((sp.close / NULLIF(cl.conversion_price,0)) * 100)::numeric, 2) AS theoretical
        FROM cb_price cp
        LEFT JOIN stock_price sp
               ON cp.stock_id = sp.stock_id AND cp.date = sp.date
        LEFT JOIN (
            SELECT DISTINCT ON (cb_id) cb_id, conversion_price
            FROM cb_list ORDER BY cb_id, updated_date DESC
        ) cl ON cp.cb_id = cl.cb_id
        WHERE cp.cb_id = %s
        ORDER BY cp.date DESC
        LIMIT %s
    """, [cid, n])
    records = clean_records(df.sort_values("date").to_dict("records"))
    cache_set(key, records)
    return jsonify(records)


# ── 大戶 / 散戶 / 股東人數 ──────────────────────────────────────────────
@app.route("/api/stock/<sid>/shareholding")
def shareholding(sid):
    n = request.args.get("weeks", 52, type=int)
    key = f"sh_{sid}_{n}"
    cached = cache_get(key)
    if cached is not None:
        return jsonify(cached)
    df = db(
        "SELECT date::text AS date, big_holder_pct, retail_pct, shareholders "
        "FROM shareholding WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    records = clean_records(df.sort_values("date").to_dict("records"))
    cache_set(key, records)
    return jsonify(records)


# ── 三大法人 ────────────────────────────────────────────────────────────
@app.route("/api/stock/<sid>/inst")
def inst(sid):
    n = request.args.get("days", 1000, type=int)
    key = f"inst_{sid}_{n}"
    cached = cache_get(key)
    if cached is not None:
        return jsonify(cached)
    df = db(
        "SELECT date::text AS date, foreign_net, trust_net, dealer_net "
        "FROM institutional WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    records = df.sort_values("date").to_dict("records")
    cache_set(key, records)
    return jsonify(records)


# ── 融資融券 ────────────────────────────────────────────────────────────
@app.route("/api/stock/<sid>/margin")
def margin(sid):
    n = request.args.get("days", 1000, type=int)
    key = f"margin_{sid}_{n}"
    cached = cache_get(key)
    if cached is not None:
        return jsonify(cached)
    df = db(
        "SELECT date::text AS date, margin_balance, short_balance "
        "FROM margin WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    records = df.sort_values("date").to_dict("records")
    cache_set(key, records)
    return jsonify(records)


# ── 清除快取（每日更新後呼叫）────────────────────────────────────────────
@app.route("/api/cache/clear", methods=["POST"])
def clear_cache():
    cache_clear()
    return jsonify({"status": "cleared"})


# ── 健康檢查 ─────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── 除錯 ─────────────────────────────────────────────────────────────────
@app.route("/debug")
def debug():
    try:
        conn = get_pool().getconn()
        cur = conn.cursor()
        tables = ["cb_list", "cb_price", "stock_price", "institutional", "shareholding", "margin"]
        counts = {}
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
            except Exception as e:
                counts[t] = f"ERROR: {e}"
        get_pool().putconn(conn)
        return jsonify({"status": "ok", "db_url_prefix": _DATABASE_URL[:40], "counts": counts})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
