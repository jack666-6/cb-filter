# -*- coding: utf-8 -*-
"""
config.py - 雲端版設定（環境變數優先）
敏感資訊（TOKEN、DATABASE_URL）請設定在 GitHub Secrets 和 Render 環境變數，
不要直接寫在這裡提交到 GitHub。
"""
import os

# FinMind API
FINMIND_TOKEN = os.environ.get(
    "FINMIND_TOKEN",
    "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."   # 本機測試備用，正式請設環境變數
)
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"

# 資料庫（雲端 PostgreSQL）
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ==== 回測 / 更新設定 ====
BACKTEST_START = "2022-01-01"
from datetime import datetime
BACKTEST_END   = datetime.now().strftime("%Y-%m-%d")
WARMUP_DAYS    = 365
