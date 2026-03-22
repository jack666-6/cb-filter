# -*- coding: utf-8 -*-
"""
app.py - CB 籌碼分析平台（雲端 PostgreSQL 版）
部署於 Render.com，資料庫為 Supabase PostgreSQL。
"""
import os
from flask import Flask, jsonify, request, render_template
import pandas as pd
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# ── 資料庫連線 ──────────────────────────────────────────────────────────────
_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if _DATABASE_URL.startswith("postgresql://"):
    _DATABASE_URL = _DATABASE_URL.replace("postgresql://", "postgres://", 1)


def _get_conn():
    return psycopg2.connect(_DATABASE_URL)


def db(sql: str, params=None) -> pd.DataFrame:
    """執行 SQL 並回傳 DataFrame（params 用 %s 佔位符）"""
    conn = _get_conn()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    finally:
        conn.close()


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
@app.route("/api/cbs")
def api_cbs():
    pmin = request.args.get("premium_min",  type=float)
    pmax = request.args.get("premium_max",  type=float)
    cmin = request.args.get("cb_price_min", type=float)
    cmax = request.args.get("cb_price_max", type=float)

    df = db("""
        SELECT
            cp.cb_id,
            cp.stock_id,
            cl.cb_name,
            ROUND(cp.reference_price::numeric, 2)         AS cb_price,
            ROUND(cp.conversion_price::numeric, 2)        AS conversion_price,
            cl.due_date,
            ROUND(sp.close::numeric, 2)                   AS stock_close,
            ROUND(
                (cp.reference_price / (sp.close / cp.conversion_price * 100) - 1) * 100
            , 2)                                          AS premium,
            ROUND(sh.big_holder_pct::numeric, 2)          AS big,
            ROUND(sh.retail_pct::numeric, 2)              AS small,
            sh.shareholders                               AS holders
        FROM cb_price cp
        JOIN (
            SELECT cb_id, MAX(date) AS d FROM cb_price GROUP BY cb_id
        ) mcp ON cp.cb_id = mcp.cb_id AND cp.date = mcp.d
        JOIN cb_list cl ON cp.cb_id = cl.cb_id
        JOIN (
            SELECT stock_id, MAX(updated_date) AS d FROM cb_list GROUP BY stock_id
        ) mcl ON cl.stock_id = mcl.stock_id AND cl.updated_date = mcl.d
        JOIN (
            SELECT stock_id, MAX(date) AS d FROM stock_price GROUP BY stock_id
        ) msp ON cp.stock_id = msp.stock_id
        JOIN stock_price sp ON sp.stock_id = cp.stock_id AND sp.date = msp.d
        LEFT JOIN (
            SELECT s.stock_id, s.big_holder_pct, s.retail_pct, s.shareholders
            FROM shareholding s
            JOIN (
                SELECT stock_id, MAX(date) AS d FROM shareholding GROUP BY stock_id
            ) ms ON s.stock_id = ms.stock_id AND s.date = ms.d
        ) sh ON cp.stock_id = sh.stock_id
        WHERE cp.conversion_price > 0
        ORDER BY cp.cb_id
    """)

    if df.empty:
        return jsonify([])

    # 篩選
    if cmin is not None:
        df = df[df["cb_price"] >= cmin]
    if cmax is not None:
        df = df[df["cb_price"] <= cmax]
    if pmin is not None:
        df = df[df["premium"].notna() & (df["premium"] >= pmin)]
    if pmax is not None:
        df = df[df["premium"].notna() & (df["premium"] <= pmax)]

    df = df.where(pd.notna(df), None)
    return jsonify(df.to_dict("records"))


# ── 個股 K 線 ────────────────────────────────────────────────────────────
@app.route("/api/stock/<sid>/kline")
def stock_kline(sid):
    n = request.args.get("days", 120, type=int)
    df = db(
        "SELECT date::text AS date, open, high, low, close, volume "
        "FROM stock_price WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    return jsonify(df.sort_values("date").to_dict("records"))


# ── CB 價格走勢（含理論價）──────────────────────────────────────────────
@app.route("/api/cb/<cid>/price")
def cb_price(cid):
    n = request.args.get("days", 120, type=int)
    df = db("""
        SELECT
            cp.date::text AS date,
            cp.reference_price,
            ROUND((sp.close / cp.conversion_price * 100)::numeric, 2) AS theoretical
        FROM cb_price cp
        LEFT JOIN stock_price sp
               ON cp.stock_id = sp.stock_id AND cp.date = sp.date
        WHERE cp.cb_id = %s
        ORDER BY cp.date DESC
        LIMIT %s
    """, [cid, n])
    df = df.where(pd.notna(df), None)
    return jsonify(df.sort_values("date").to_dict("records"))


# ── 大戶 / 散戶 / 股東人數 ──────────────────────────────────────────────
@app.route("/api/stock/<sid>/shareholding")
def shareholding(sid):
    n = request.args.get("weeks", 52, type=int)
    df = db(
        "SELECT date::text AS date, big_holder_pct, retail_pct, shareholders "
        "FROM shareholding WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    df = df.where(pd.notna(df), None)
    return jsonify(df.sort_values("date").to_dict("records"))


# ── 三大法人 ────────────────────────────────────────────────────────────
@app.route("/api/stock/<sid>/inst")
def inst(sid):
    n = request.args.get("days", 60, type=int)
    df = db(
        "SELECT date::text AS date, foreign_net, trust_net, dealer_net "
        "FROM institutional WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    return jsonify(df.sort_values("date").to_dict("records"))


# ── 融資融券 ────────────────────────────────────────────────────────────
@app.route("/api/stock/<sid>/margin")
def margin(sid):
    n = request.args.get("days", 60, type=int)
    df = db(
        "SELECT date::text AS date, margin_balance, short_balance "
        "FROM margin WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        [sid, n]
    )
    return jsonify(df.sort_values("date").to_dict("records"))


# ── 健康檢查（Render keepalive / uptime robot 用）────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── 除錯：資料庫連線與資料表筆數 ──────────────────────────────────────
@app.route("/debug")
def debug():
    try:
        conn = _get_conn()
        cur = conn.cursor()
        tables = ["cb_list", "cb_price", "stock_price", "institutional", "shareholding", "margin"]
        counts = {}
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
            except Exception as e:
                counts[t] = f"ERROR: {e}"
        conn.close()
        return jsonify({"status": "ok", "db_url_prefix": _DATABASE_URL[:40], "counts": counts})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
