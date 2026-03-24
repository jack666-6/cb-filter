# -*- coding: utf-8 -*-
"""
db_updater.py - 每日資料庫同步腳本（雲端 PostgreSQL 版）
功能：
  1. 從 FinMind 拉取全市場 CB 清單 → 取得所有需要追蹤的股票代號
  2. 對每支股票做「增量更新」（只抓 DB 中最後日期到今天的缺失資料）
  3. 第一次執行時自動做「歷史全量下載」

用法：
  python db_updater.py            # 增量更新（每日執行）
  python db_updater.py --init     # 歷史全量下載（首次初始化）
  python db_updater.py --init --years 3
"""

import argparse
import re
import time
from datetime import datetime, timedelta

import requests

import config
import db_manager_pg as db
import data_fetcher as fetcher


def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def days_ago(n: int) -> str:
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


# ======================================================
# 1. CB 清單
# ======================================================
def fetch_cb_list() -> list:
    start = days_ago(90)
    end   = today()
    params = {
        "dataset":    "TaiwanStockConvertibleBondDailyOverview",
        "start_date": start,
        "end_date":   end,
    }
    headers = {"Authorization": config.FINMIND_TOKEN}
    print(f"  Fetching CB list ({start} ~ {end})...")
    try:
        resp = requests.get(config.FINMIND_API, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        raw = resp.json().get("data", [])
    except Exception as e:
        print(f"  [ERROR] CB list: {e}")
        return []

    if not raw:
        return []

    cb_map = {}
    for item in raw:
        cb_id = str(item.get("cb_id", "")).strip()
        if len(cb_id) < 4:
            continue
        d = item.get("date", "")
        if cb_id not in cb_map or d > cb_map[cb_id]["date"]:
            cb_map[cb_id] = item

    result = []
    for cb_id, item in cb_map.items():
        stock_id = cb_id[:4]
        result.append({
            "cb_id":            cb_id,
            "stock_id":         stock_id,
            "cb_name":          item.get("cb_name", ""),
            "reference_price":  item.get("ReferencePrice"),
            "conversion_price": item.get("ConversionPrice"),
            "due_date":         item.get("DueDateOfConversion", ""),
            "updated_date":     item.get("date", today()),
        })

    seen_stocks = {r["stock_id"] for r in result}
    print(f"  Found {len(cb_map)} CBs covering {len(seen_stocks)} stocks.")
    return result


# ======================================================
# 2. CB 每日報價更新
# ======================================================
def fetch_cb_price_list(start: str, end: str) -> list:
    """從 TaiwanStockConvertibleBondDailyOverview 取 CB 每日報價"""
    params = {
        "dataset":    "TaiwanStockConvertibleBondDailyOverview",
        "start_date": start,
        "end_date":   end,
    }
    headers = {"Authorization": config.FINMIND_TOKEN}
    try:
        resp = requests.get(config.FINMIND_API, params=params, headers=headers, timeout=120)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"  [ERROR] CB price: {e}")
        return []


def update_cb_prices(start: str, end: str):
    """更新所有 CB 每日市場報價"""
    raw = fetch_cb_price_list(start, end)
    if not raw:
        print("  No CB price data.")
        return

    rows = []
    for item in raw:
        cb_id = str(item.get("cb_id", "")).strip()
        if len(cb_id) < 4:
            continue
        stock_id = cb_id[:4]
        ref_price = item.get("ReferencePrice")
        conv_price = item.get("ConversionPrice")
        if not ref_price:          # 只要求 CB 市場價格，轉換價從 cb_list 抓
            continue
        rows.append({
            "cb_id":            cb_id,
            "stock_id":         stock_id,
            "date":             item.get("date", ""),
            "reference_price":  float(ref_price),
            "conversion_price": float(conv_price) if conv_price else 0.0,
            "coupon_rate":      float(item.get("CouponRate", 0) or 0),
        })

    n = db.upsert_cb_price(rows)
    db.log_sync("cb_price", n)
    print(f"  CB prices: +{n} rows")


# ======================================================
# 3. 個股四表增量更新
# ======================================================
def _parse_shareholding(stock_id: str, raw: list) -> list:
    def parse_bounds(level: str):
        s = str(level).strip().lower().replace(",", "")
        nums = [int(m) for m in re.findall(r"\d+", s)]
        if re.search(r"(以上|morethan|\+$)", s):
            return (nums[0], float("inf")) if nums else (None, None)
        if re.search(r"(以下|lessthan|under)", s):
            return (0, nums[0]) if nums else (None, None)
        if re.search(r"[-~]|\bto\b", s):
            return (nums[0], nums[1]) if len(nums) >= 2 else (None, None)
        if len(nums) == 2:
            return nums[0], nums[1]
        return (None, None)

    date_data = {}
    seen = {}
    for rec in raw:
        date  = rec.get("date", "")
        level = rec.get("HoldingSharesLevel", "")
        pct   = rec.get("percent")
        if level == "total":
            if date not in date_data:
                date_data[date] = {"big": 0.0, "s30": 0.0, "s1": 0.0, "shareholders": 0}
            date_data[date]["shareholders"] = int(rec.get("people", 0) or 0)
            continue
        if pct is None:
            continue
        low, high = parse_bounds(level)
        if low is None:
            continue
        if date not in seen:
            seen[date] = set()
        bk = f"{low}-{'inf' if high == float('inf') else high}"
        if bk in seen[date]:
            continue
        seen[date].add(bk)
        if date not in date_data:
            date_data[date] = {"big": 0.0, "s30": 0.0, "s1": 0.0, "shareholders": 0}
        pct = float(pct)
        if low >= 400001:
            date_data[date]["big"] += pct
        if high <= 30000:
            date_data[date]["s30"] += pct
        if high <= 1000:
            date_data[date]["s1"] += pct

    rows = []
    for date, vals in sorted(date_data.items()):
        retail = max(vals["s30"] - vals["s1"], 0.0)
        rows.append({
            "stock_id":       stock_id,
            "date":           date,
            "big_holder_pct": round(vals["big"], 4),
            "retail_pct":     round(retail, 4),
            "shareholders":   vals["shareholders"],
        })
    return rows


def update_stock(stock_id: str, start_date: str, end_date: str):
    # ---- 股價 ----
    latest = db.get_latest_date("stock_price", stock_id)
    s = (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
        if latest else start_date
    if s <= end_date:
        raw = fetcher.fetch_stock_price(stock_id, s, end_date)
        if not raw.empty:
            rows = [{"stock_id": stock_id, "date": r["date"].strftime("%Y-%m-%d"),
                     "open": r["open"], "high": r["high"], "low": r["low"],
                     "close": r["close"], "volume": r["volume"]}
                    for _, r in raw.iterrows()]
            n = db.upsert_prices(rows)
            db.log_sync("stock_price", n, stock_id)
            print(f"    price:  +{n}")
        time.sleep(0.3)

    # ---- 三大法人 ----
    latest = db.get_latest_date("institutional", stock_id)
    s = (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
        if latest else start_date
    if s <= end_date:
        raw = fetcher.fetch_institutional(stock_id, s, end_date)
        if not raw.empty:
            rows = [{"stock_id": stock_id, "date": r["date"].strftime("%Y-%m-%d"),
                     "foreign_net": r.get("foreign", 0), "trust_net": r.get("trust", 0),
                     "dealer_net": r.get("dealer", 0), "inst_total": r.get("inst_total", 0)}
                    for _, r in raw.iterrows()]
            n = db.upsert_institutional(rows)
            db.log_sync("institutional", n, stock_id)
            print(f"    inst:   +{n}")
        time.sleep(0.3)

    # ---- 集保（週度）----
    latest = db.get_latest_date("shareholding", stock_id)
    s = (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
        if latest else start_date
    if s <= end_date:
        try:
            raw_sh = fetcher._get("TaiwanStockHoldingSharesPer", stock_id, s, end_date)
        except Exception:
            raw_sh = []
        if raw_sh:
            rows = _parse_shareholding(stock_id, raw_sh)
            n = db.upsert_shareholding(rows)
            db.log_sync("shareholding", n, stock_id)
            print(f"    share:  +{n}")
        time.sleep(0.3)

    # ---- 融資融券 ----
    latest = db.get_latest_date("margin", stock_id)
    s = (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
        if latest else start_date
    if s <= end_date:
        raw = fetcher.fetch_margin(stock_id, s, end_date)
        if not raw.empty:
            rows = [{"stock_id": stock_id, "date": r["date"].strftime("%Y-%m-%d"),
                     "margin_balance": r.get("margin_balance", 0),
                     "short_balance": r.get("short_balance", 0)}
                    for _, r in raw.iterrows()]
            n = db.upsert_margin(rows)
            db.log_sync("margin", n, stock_id)
            print(f"    margin: +{n}")
        time.sleep(0.3)


# ======================================================
# 主流程
# ======================================================
def run_update(init_mode: bool = False, init_years: int = 3):
    print("=" * 60)
    if init_mode:
        hist_start = days_ago(init_years * 365)
        print(f"  DB INIT MODE  |  Downloading {init_years} years of history")
    else:
        hist_start = days_ago(120)
        print(f"  DB DAILY UPDATE  |  {today()}")
    print("=" * 60)

    db.init_db()
    end = today()

    # Step 1: CB 清單
    print("\n[Step 1] CB 清單...")
    cb_rows = fetch_cb_list()
    if cb_rows:
        db.upsert_cb_list(cb_rows)
        db.log_sync("cb_list", len(cb_rows))
        print(f"  cb_list: {len(cb_rows)} records")

    # Step 2: CB 每日報價
    print("\n[Step 2] CB 每日報價...")
    cb_price_start = hist_start if init_mode else days_ago(5)
    update_cb_prices(cb_price_start, end)

    # Step 3: 個股四表（週末跳過）
    weekday = datetime.now().weekday()  # 0=Mon, 5=Sat, 6=Sun
    if not init_mode and weekday >= 5:
        print(f"\n[Step 3] 今天是週末，跳過個股資料更新。")
    else:
        all_stocks = db.get_all_cb_stocks()
        if not all_stocks:
            all_stocks = sorted({r["stock_id"] for r in cb_rows})

        print(f"\n[Step 3] Updating {len(all_stocks)} stocks...")
        for i, sid in enumerate(all_stocks, 1):
            print(f"  [{i:3d}/{len(all_stocks)}] {sid}")
            start = hist_start if init_mode else hist_start
            try:
                update_stock(sid, start, end)
            except Exception as e:
                print(f"    [ERROR] {e}")
                db.log_sync("stock_price", 0, sid, status="error", message=str(e))

    # Step 4: 統計
    stats = db.get_db_stats()
    print("\n" + "=" * 60)
    for table, cnt in stats.items():
        print(f"  {table:<20} : {cnt:>8,} rows")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init",  action="store_true")
    parser.add_argument("--years", type=int, default=3)
    args = parser.parse_args()
    run_update(init_mode=args.init, init_years=args.years)
