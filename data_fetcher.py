"""
data_fetcher.py - FinMind API 資料拉取模組
還原 n8n 工作流程中的所有資料請求節點。
優先從本地 SQLite DB 讀取，DB 不存在或無資料時才呼叫 API。
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import config


def _get(dataset: str, stock_id: str, start_date: str, end_date: str) -> list:
    """共用的 FinMind API GET 呼叫"""
    params = {
        "dataset": dataset,
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    headers = {"Authorization": config.FINMIND_TOKEN}
    try:
        resp = requests.get(config.FINMIND_API, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        print(f"  [API Error] {dataset} {stock_id}: {e}")
        return []


def fetch_stock_price(stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    拉取股價資料 (對應 n8n「請求股價2」節點)
    回傳欄位: date, open, high, low, close, volume(張)
    """
    raw = _get("TaiwanStockPrice", stock_id, start_date, end_date)
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    df["date"] = pd.to_datetime(df["date"])

    rename = {"open": "open", "max": "high", "min": "low",
               "close": "close", "Trading_Volume": "volume"}
    df = df.rename(columns=rename)

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = 0.0

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    # n8n 判斷：若 volume > 100000 則除以 1000（轉換為張）
    df["volume"] = df["volume"].apply(lambda v: v / 1000 if v > 100_000 else v)

    df = df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    return df


def fetch_institutional(stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    拉取三大法人買賣超資料 (對應 n8n「請求法人交易資料2」與「整理數據2」節點)
    回傳欄位: date, foreign, trust, dealer, inst_total（張）
    """
    raw = _get("TaiwanStockInstitutionalInvestorsBuySell", stock_id, start_date, end_date)
    if not raw:
        return pd.DataFrame()

    def get_category(name: str) -> str:
        if "Foreign" in name:
            return "foreign"
        if "Trust" in name:
            return "trust"
        if "Dealer" in name:
            return "dealer"
        return "unknown"

    daily: dict = {}
    for rec in raw:
        date = rec.get("date", "")
        cat = get_category(rec.get("name", ""))
        if cat == "unknown":
            continue
        net_buy = (rec.get("buy", 0) or 0) - (rec.get("sell", 0) or 0)
        net_buy_lots = round(net_buy / 1000, 2)
        if date not in daily:
            daily[date] = {"date": date, "foreign": 0.0, "trust": 0.0, "dealer": 0.0}
        daily[date][cat] += net_buy_lots

    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame(list(daily.values()))
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["inst_total"] = df["foreign"] + df["trust"] + df["dealer"]
    return df


def fetch_shareholding(stock_id: str, start_date: str, end_date: str) -> dict:
    """
    拉取集保股東持股分布 (對應 n8n「請求集保人數2」→「留下千張大戶人數2」節點)
    回傳:
      big_holder   -> {date: pct}   大戶（400張以上）持股%
      retail_holder-> {date: pct}   散戶（30張以下排除1張以下）持股%
      shareholders -> {date: count} 股東人數
    """
    raw = _get("TaiwanStockHoldingSharesPer", stock_id, start_date, end_date)
    if not raw:
        return {"big_holder": {}, "retail_holder": {}, "shareholders": {}}

    import re

    def parse_bounds(level: str):
        s = str(level).strip().lower().replace(",", "").replace("，", "").replace("、", "")
        nums = [int(m) for m in re.findall(r"\d+", s)]
        if re.search(r"(以上|morethan|\+$)", s):
            return (nums[0], float("inf")) if nums else (None, None)
        if re.search(r"(以下|lessthan|under)", s):
            return (0, nums[0]) if nums else (None, None)
        if re.search(r"[-~～]|\bto\b", s):
            return (nums[0], nums[1]) if len(nums) >= 2 else (None, None)
        if len(nums) == 2:
            return (nums[0], nums[1])
        return (None, None)

    big_map: dict = {}
    small30_map: dict = {}
    small1_map: dict = {}
    total_map: dict = {}
    seen: dict = {}

    for rec in raw:
        date = rec.get("date", "")
        level = rec.get("HoldingSharesLevel", "")
        pct = rec.get("percent", None)

        if level == "total":
            total_map[date] = rec.get("people", 0)
            continue

        if pct is None:
            continue

        low, high = parse_bounds(level)
        if low is None:
            continue

        if date not in seen:
            seen[date] = set()
        bucket_key = f"{low}-{'inf' if high == float('inf') else high}"
        if bucket_key in seen[date]:
            continue
        seen[date].add(bucket_key)

        pct = float(pct)
        if low >= 400001:
            big_map[date] = big_map.get(date, 0.0) + pct
        if high <= 30000:
            small30_map[date] = small30_map.get(date, 0.0) + pct
        if high <= 1000:
            small1_map[date] = small1_map.get(date, 0.0) + pct

    retail_map: dict = {}
    for date in small30_map:
        val = small30_map[date] - small1_map.get(date, 0.0)
        retail_map[date] = max(val, 0.0)

    return {
        "big_holder": big_map,
        "retail_holder": retail_map,
        "shareholders": total_map,
    }


def fetch_margin(stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    拉取融資融券資料 (對應 n8n「請求融資融券2」→「整理資料5」節點)
    回傳欄位: date, margin_balance(張), short_balance(張)
    """
    raw = _get("TaiwanStockMarginPurchaseShortSale", stock_id, start_date, end_date)
    if not raw:
        return pd.DataFrame()

    records = []
    for rec in raw:
        records.append({
            "date": rec.get("date", ""),
            "margin_balance": rec.get("MarginPurchaseTodayBalance", 0),
            "short_balance": rec.get("ShortSaleTodayBalance", 0),
        })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ======================================================
# 主要讀取介面（DB 優先，API 備援）
# ======================================================
def _db_available() -> bool:
    """檢查 SQLite DB 是否存在"""
    try:
        import db_manager as db
        import os
        return os.path.exists(db.DB_PATH)
    except ImportError:
        return False


def load_all_data(stock_id: str, start_date: str, end_date: str, warmup_days: int = 365) -> dict:
    """
    一次讀取單支股票的所有所需資料，含預熱期。
    優先從本地 SQLite DB 讀取（毫秒級），DB 無資料才呼叫 API。
    """
    dt_start = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=warmup_days)
    fetch_start = dt_start.strftime("%Y-%m-%d")

    if _db_available():
        import db_manager as db

        print(f"  [{stock_id}] loading from DB...", end=" ", flush=True)

        # ---- 股價 ----
        price_rows = db.read_prices(stock_id, fetch_start, end_date)
        if price_rows:
            price = pd.DataFrame(price_rows)
            price["date"] = pd.to_datetime(price["date"])
        else:
            print("(price→API)", end=" ", flush=True)
            price = fetch_stock_price(stock_id, fetch_start, end_date)
            time.sleep(0.3)

        # ---- 法人 ----
        inst_rows = db.read_institutional(stock_id, fetch_start, end_date)
        if inst_rows:
            institutional = pd.DataFrame(inst_rows)
            institutional["date"] = pd.to_datetime(institutional["date"])
            institutional = institutional.rename(columns={
                "foreign_net": "foreign",
                "trust_net": "trust",
                "dealer_net": "dealer",
            })
        else:
            print("(inst→API)", end=" ", flush=True)
            institutional = fetch_institutional(stock_id, fetch_start, end_date)
            time.sleep(0.3)

        # ---- 集保 ----
        sh_rows = db.read_shareholding(stock_id, fetch_start, end_date)
        if sh_rows:
            shareholding = {
                "big_holder":    {r["date"]: r["big_holder_pct"] for r in sh_rows},
                "retail_holder": {r["date"]: r["retail_pct"]     for r in sh_rows},
                "shareholders":  {r["date"]: r["shareholders"]   for r in sh_rows},
            }
        else:
            print("(share→API)", end=" ", flush=True)
            shareholding = fetch_shareholding(stock_id, fetch_start, end_date)
            time.sleep(0.3)

        # ---- 融資券 ----
        mg_rows = db.read_margin(stock_id, fetch_start, end_date)
        if mg_rows:
            margin = pd.DataFrame(mg_rows)
            margin["date"] = pd.to_datetime(margin["date"])
        else:
            print("(margin→API)", end=" ", flush=True)
            margin = fetch_margin(stock_id, fetch_start, end_date)
            time.sleep(0.3)

        print("done.")

    else:
        # ---- 純 API 模式（DB 不存在時的備援）----
        print(f"  Fetching {stock_id}: price...", end=" ", flush=True)
        price = fetch_stock_price(stock_id, fetch_start, end_date)
        time.sleep(0.5)

        print("institutional...", end=" ", flush=True)
        institutional = fetch_institutional(stock_id, fetch_start, end_date)
        time.sleep(0.5)

        print("shareholding...", end=" ", flush=True)
        shareholding = fetch_shareholding(stock_id, fetch_start, end_date)
        time.sleep(0.5)

        print("margin...", end=" ", flush=True)
        margin = fetch_margin(stock_id, fetch_start, end_date)
        time.sleep(0.5)

        print("done.")

    return {
        "price": price,
        "institutional": institutional,
        "shareholding": shareholding,
        "margin": margin,
    }
