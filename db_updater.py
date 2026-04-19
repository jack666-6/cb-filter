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
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config
import db_manager_pg as db
import data_fetcher as fetcher

MAX_WORKERS = 5   # 並行執行緒數（太高容易被 FinMind 限速）


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
def fetch_cb_price_for_date(date: str) -> list:
    """查詢單一日期的全市場 CB 報價（與 n8n 相同做法：start=end=today）"""
    params = {
        "dataset":    "TaiwanStockConvertibleBondDailyOverview",
        "start_date": date,
        "end_date":   date,
    }
    headers = {"Authorization": config.FINMIND_TOKEN}
    try:
        resp = requests.get(config.FINMIND_API, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"  [ERROR] CB price {date}: {e}")
        return []


def fetch_cb_price_bulk(start: str, end: str) -> list:
    """批次查詢 CB 報價（用於 init 初始化模式）"""
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
        print(f"  [ERROR] CB price bulk: {e}")
        return []


def _parse_cb_price_rows(raw: list) -> list:
    """將 API 回傳資料轉成 DB 格式"""
    rows = []
    for item in raw:
        cb_id = str(item.get("cb_id", "")).strip()
        if len(cb_id) < 4:
            continue
        ref_price = item.get("ReferencePrice")
        if not ref_price:
            continue
        conv_price = item.get("ConversionPrice")
        rows.append({
            "cb_id":            cb_id,
            "stock_id":         cb_id[:4],
            "date":             item.get("date", ""),
            "reference_price":  float(ref_price),
            "conversion_price": float(conv_price) if conv_price else 0.0,
            "coupon_rate":      float(item.get("CouponRate", 0) or 0),
        })
    return rows


def update_cb_prices(start: str, end: str, init_mode: bool = False):
    """更新所有 CB 每日市場報價
    - init_mode: 使用批次查詢（歷史資料）
    - 一般模式: 逐日查詢（與 n8n 相同，確保拿到當天最新資料）
    """
    if init_mode:
        # 初始化：一次批次抓歷史資料
        raw = fetch_cb_price_bulk(start, end)
        rows = _parse_cb_price_rows(raw)
        if not rows:
            print("  No CB price data.")
            return
        n = db.upsert_cb_price(rows)
        db.log_sync("cb_price", n)
        print(f"  CB prices: +{n} rows")
    else:
        # 每日更新：逐日查詢，確保拿到最新當天資料
        current = datetime.strptime(start, "%Y-%m-%d")
        end_dt  = datetime.strptime(end, "%Y-%m-%d")
        total   = 0
        while current <= end_dt:
            if current.weekday() < 5:  # 跳過週末
                date_str = current.strftime("%Y-%m-%d")
                raw  = fetch_cb_price_for_date(date_str)
                rows = _parse_cb_price_rows(raw)
                if rows:
                    n = db.upsert_cb_price(rows)
                    total += n
                    print(f"  {date_str}: +{n} rows")
                else:
                    print(f"  {date_str}: 無資料")
                time.sleep(0.5)
            current += timedelta(days=1)
        db.log_sync("cb_price", total)
        print(f"  CB prices total: +{total} rows")


# ======================================================
# 3. 個股四表更新
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


def _fetch_one_stock(sid: str, start: str, end: str) -> dict:
    """單支股票四表全抓（供並行使用）"""
    result = {"sid": sid, "price": [], "inst": [], "margin": [], "share": []}
    try:
        # 股價
        raw = fetcher._get("TaiwanStockPrice", sid, start, end)
        for r in (raw or []):
            vol = float(r.get("Trading_Volume", 0) or 0)
            result["price"].append({
                "stock_id": sid, "date": r.get("date", ""),
                "open":  float(r.get("open", 0) or 0),
                "high":  float(r.get("max",  0) or 0),
                "low":   float(r.get("min",  0) or 0),
                "close": float(r.get("close",0) or 0),
                "volume": vol / 1000 if vol > 100_000 else vol,
            })

        # 三大法人
        raw = fetcher._get("TaiwanStockInstitutionalInvestorsBuySell", sid, start, end)
        daily = {}
        for r in (raw or []):
            date = r.get("date", ""); name = r.get("name", "")
            if not date: continue
            daily.setdefault(date, {"stock_id": sid, "date": date,
                                    "foreign_net": 0.0, "trust_net": 0.0,
                                    "dealer_net": 0.0, "inst_total": 0.0})
            net = ((r.get("buy", 0) or 0) - (r.get("sell", 0) or 0)) / 1000
            if   "Foreign" in name: daily[date]["foreign_net"] += net
            elif "Trust"   in name: daily[date]["trust_net"]   += net
            elif "Dealer"  in name: daily[date]["dealer_net"]  += net
        for r in daily.values():
            r["inst_total"] = r["foreign_net"] + r["trust_net"] + r["dealer_net"]
        result["inst"] = list(daily.values())

        # 融資融券
        raw = fetcher._get("TaiwanStockMarginPurchaseShortSale", sid, start, end)
        result["margin"] = [{"stock_id": sid, "date": r.get("date", ""),
                              "margin_balance": r.get("MarginPurchaseTodayBalance", 0),
                              "short_balance":  r.get("ShortSaleTodayBalance", 0)}
                             for r in (raw or [])]

        # 集保
        raw = fetcher._get("TaiwanStockHoldingSharesPer", sid, start, end)
        if raw:
            result["share"] = _parse_shareholding(sid, raw)

    except Exception as e:
        print(f"    [ERROR] {sid}: {e}")
    return result


def update_stocks_batch(all_stocks: list, start: str, end: str):
    """並行更新所有股票四表（ThreadPoolExecutor，最多 MAX_WORKERS 個同時請求）"""
    total_price = total_inst = total_margin = total_share = 0
    n = len(all_stocks)
    done = 0

    all_price = []; all_inst = []; all_margin = []; all_share = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_one_stock, sid, start, end): sid
                   for sid in all_stocks}
        for future in as_completed(futures):
            res = future.result()
            all_price  += res["price"]
            all_inst   += res["inst"]
            all_margin += res["margin"]
            all_share  += res["share"]
            done += 1
            if done % 50 == 0:
                print(f"  [{done}/{n}] 完成...")

    # 一次性寫入 DB
    if all_price:  total_price  = db.upsert_prices(all_price)
    if all_inst:   total_inst   = db.upsert_institutional(all_inst)
    if all_margin: total_margin = db.upsert_margin(all_margin)
    if all_share:  total_share  = db.upsert_shareholding(all_share)

    db.log_sync("stock_price",   total_price)
    db.log_sync("institutional", total_inst)
    db.log_sync("margin",        total_margin)
    db.log_sync("shareholding",  total_share)
    print(f"  [OK] price:+{total_price}  inst:+{total_inst}  margin:+{total_margin}  share:+{total_share}")


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
    cb_price_start = hist_start if init_mode else days_ago(3)
    update_cb_prices(cb_price_start, end, init_mode=init_mode)

    # Step 3: 個股四表
    weekday = datetime.now().weekday()  # 0=Mon, 5=Sat, 6=Sun
    if not init_mode and weekday >= 5:
        print(f"\n[Step 3] 今天是週末，跳過個股資料更新。")
    else:
        all_stocks = db.get_all_cb_stocks()
        if not all_stocks:
            all_stocks = sorted({r["stock_id"] for r in cb_rows})

        stock_start = hist_start if init_mode else days_ago(3)
        print(f"\n[Step 3] 更新 {len(all_stocks)} 支股票（{stock_start} ~ {end}）...")
        try:
            update_stocks_batch(all_stocks, stock_start, end)
        except Exception as e:
            print(f"  [ERROR] {e}")

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
