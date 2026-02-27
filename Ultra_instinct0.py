#!/usr/bin/env python3
"""
Ultra_instinct.py  -- MT5-only (no Yahoo) defensive trading bot.
Minimal surgical fixes from your previous file:
  - Added robust threshold validation/clamping to prevent negative CURRENT_THRESHOLD or invalid bounds.
Everything else (strategy, thresholds, adapt/optimizer, db, telegram) is left intact.
Run as you did before:
  set MT5_LOGIN=...
  set MT5_PASSWORD=...
  set MT5_SERVER=...
  set MT5_PATH="C:\Program Files\MetaTrader 5\terminal64.exe"
  python Ultra_instinct7.0.py --backtest
  python Ultra_instinct7.0.py --loop
"""
from __future__ import annotations
import os
import sys
import time
import json
import logging
import sqlite3
import argparse
import random
import warnings
from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, Any, List

# core libs
try:
    import numpy as np
    import pandas as pd
except Exception as e:
    raise RuntimeError("Install numpy and pandas: pip install numpy pandas") from e

# optional libs
try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except Exception:
    MT5_AVAILABLE = False

try:
    from ta.trend import SMAIndicator, ADXIndicator
    from ta.volatility import AverageTrueRange
    from ta.momentum import RSIIndicator
    TA_AVAILABLE = True
except Exception:
    TA_AVAILABLE = False

# ML libs (flexible)
SKLEARN_AVAILABLE = False
try:
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import SGDClassifier
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.exceptions import ConvergenceWarning
    import joblib
    SKLEARN_AVAILABLE = True
    # suppress convergence warnings to avoid spamming logs; training logic is optional and guarded
    warnings.filterwarnings("ignore", category=ConvergenceWarning)
except Exception:
    # fallback flags - the code will still run but ML features will be disabled
    SKLEARN_AVAILABLE = False

# fundamental / web (optional)
FUNDAMENTAL_AVAILABLE = False
try:
    import requests
    FUNDAMENTAL_AVAILABLE = True
except Exception:
    FUNDAMENTAL_AVAILABLE = False

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Ultra_instinct")

# ---------------- Configuration (unchanged logic except requested fixes) ----------------
# Add USOIL symbol and switch broker suffix to 'm' (e.g. XAUUSDm)
SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY", "USOIL"]

# broker mapping - updated to use suffix 'm' (no dot) as you requested
BROKER_SYMBOLS = {
    "EURUSD": "EURUSDm",
    "XAGUSD": "XAGUSDm",
    "XAUUSD": "XAUUSDm",
    "BTCUSD": "BTCUSDm",
    "USDJPY": "USDJPYm",
    "USOIL": "USOILm",
}

TIMEFRAMES = {"M30": "30m", "H1": "60m"}  # M30 + H1 as you requested

# Safety defaults - DEMO removed: go straight to live (you said you understand the risks)
DEMO_SIMULATION = False
AUTO_EXECUTE = True
# if env var set, keep parity (but we already default to live)
if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND_THE RISKS":
    DEMO_SIMULATION = False
    AUTO_EXECUTE = True

# Read and validate numeric thresholds / risk values safely
def _safe_float_env(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        try:
            return float(v.replace(",", "."))
        except Exception:
            logger.warning("Invalid env %s=%r, using default %s", name, v, default)
            return float(default)

BASE_RISK_PER_TRADE_PCT = _safe_float_env("BASE_RISK_PER_TRADE_PCT", 0.003)
MIN_RISK_PER_TRADE_PCT = 0.002
MAX_RISK_PER_TRADE_PCT = 0.01
RISK_PER_TRADE_PCT = float(BASE_RISK_PER_TRADE_PCT)

# Thresholds (read from env if present) — but validated afterwards
MIN_THRESHOLD = _safe_float_env("MIN_THRESHOLD", 0.12)
CURRENT_THRESHOLD = _safe_float_env("CURRENT_THRESHOLD", 0.13)
MAX_THRESHOLD = _safe_float_env("MAX_THRESHOLD", 0.50)

DECISION_SLEEP = int(os.getenv("DECISION_SLEEP", "60"))
ADAPT_EVERY_CYCLES = 6
MODEL_MIN_TRAIN = 40

# Add a single place to validate/clamp thresholds to prevent negatives or invalid ranges
def validate_and_clamp_thresholds():
    """
    Ensure MIN_THRESHOLD, MAX_THRESHOLD, CURRENT_THRESHOLD are sane:
      - Non-negative
      - MIN <= CURRENT <= MAX
      - If env provided nonsense, auto-correct with safe defaults
    """
    global MIN_THRESHOLD, MAX_THRESHOLD, CURRENT_THRESHOLD
    try:
        # force numeric
        MIN_THRESHOLD = float(MIN_THRESHOLD)
    except Exception:
        MIN_THRESHOLD = 0.12
    try:
        MAX_THRESHOLD = float(MAX_THRESHOLD)
    except Exception:
        MAX_THRESHOLD = 0.50
    try:
        CURRENT_THRESHOLD = float(CURRENT_THRESHOLD)
    except Exception:
        CURRENT_THRESHOLD = 0.13

    # disallow negative thresholds
    if MIN_THRESHOLD < 0:
        logger.warning("MIN_THRESHOLD was negative (%s) — forcing to abs()", MIN_THRESHOLD)
        MIN_THRESHOLD = abs(MIN_THRESHOLD)
    if MAX_THRESHOLD < 0:
        logger.warning("MAX_THRESHOLD was negative (%s) — forcing to abs()", MAX_THRESHOLD)
        MAX_THRESHOLD = abs(MAX_THRESHOLD)
    if CURRENT_THRESHOLD < 0:
        logger.warning("CURRENT_THRESHOLD was negative (%s) — forcing to abs()", CURRENT_THRESHOLD)
        CURRENT_THRESHOLD = abs(CURRENT_THRESHOLD)

    # ensure max is at least slightly above min
    if MAX_THRESHOLD <= MIN_THRESHOLD:
        logger.warning("MAX_THRESHOLD <= MIN_THRESHOLD (%s <= %s). Adjusting MAX_THRESHOLD = MIN_THRESHOLD + 0.01", MAX_THRESHOLD, MIN_THRESHOLD)
        MAX_THRESHOLD = MIN_THRESHOLD + 0.01

    # clamp current into [min, max]
    if CURRENT_THRESHOLD < MIN_THRESHOLD or CURRENT_THRESHOLD > MAX_THRESHOLD:
        logger.info("Clamping CURRENT_THRESHOLD %.6f into [%s, %s]", CURRENT_THRESHOLD, MIN_THRESHOLD, MAX_THRESHOLD)
        CURRENT_THRESHOLD = max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD))

    # ensure they are floats
    MIN_THRESHOLD = float(MIN_THRESHOLD)
    MAX_THRESHOLD = float(MAX_THRESHOLD)
    CURRENT_THRESHOLD = float(CURRENT_THRESHOLD)

# run validator at startup
validate_and_clamp_thresholds()

# Safety/environmental limits
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "200"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "STOP_TRADING.flag")
ADAPT_STATE_FILE = "adapt_state.json"
TRADES_DB = "trades.db"
MODEL_FILE = "ultra_instinct_model.joblib"
TRADES_CSV = "trades.csv"
# MT5 credentials env
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
MT5_PATH = os.getenv("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")

# telegram (optional)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# fundamental API (optional) - user may set NEWS_API_KEY env to enable
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
# TradingEconomics API key for economic calendar (ADDED)
TRADING_ECONOMICS_KEY = os.getenv("TRADING_ECONOMICS_KEY", "")

# Pause window before major events (minutes)
PAUSE_BEFORE_EVENT_MINUTES = int(os.getenv("PAUSE_BEFORE_EVENT_MINUTES", "30"))

# ---------------- persistence and state ----------------
def load_adapt_state():
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    if os.path.exists(ADAPT_STATE_FILE):
        try:
            with open(ADAPT_STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
            # only update if present
            if "threshold" in st:
                try:
                    CURRENT_THRESHOLD = float(st.get("threshold", CURRENT_THRESHOLD))
                except Exception:
                    pass
            if "risk" in st:
                try:
                    RISK_PER_TRADE_PCT = float(st.get("risk", RISK_PER_TRADE_PCT))
                except Exception:
                    pass
            # re-validate thresholds after loading persisted state
            validate_and_clamp_thresholds()
            logger.info("Loaded adapt_state threshold=%.3f risk=%.5f", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
        except Exception:
            logger.exception("load_adapt_state failed")

def save_adapt_state():
    try:
        with open(ADAPT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"threshold": CURRENT_THRESHOLD, "risk": RISK_PER_TRADE_PCT}, f)
    except Exception:
        logger.exception("save_adapt_state failed")

load_adapt_state()

# ---------------- DB and logging ----------------
def _get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall()
        return [r[1] for r in rows] if rows else []
    except Exception:
        return []

def init_trade_db():
    """
    Create or migrate the trades table so that older DB schemas won't cause insertion errors.
    This will add missing columns if possible; if the table doesn't exist it will be created with the expected schema.
    """
    conn = sqlite3.connect(TRADES_DB, timeout=5)
    cur = conn.cursor()
    expected_cols = {
        "id": "INTEGER PRIMARY KEY",
        "ts": "TEXT",
        "symbol": "TEXT",
        "side": "TEXT",
        "entry": "REAL",
        "sl": "REAL",
        "tp": "REAL",
        "lots": "REAL",
        "status": "TEXT",
        "pnl": "REAL",
        "rmult": "REAL",
        "regime": "TEXT",
        "score": "REAL",
        "model_score": "REAL",
        "meta": "TEXT",
    }
    try:
        # if table doesn't exist, create it
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
        if not cur.fetchone():
            cols_sql = ",\n      ".join([f"{k} {v}" for k, v in expected_cols.items()])
            create_sql = f"CREATE TABLE trades (\n      {cols_sql}\n    );"
            cur.execute(create_sql)
            conn.commit()
        else:
            # table exists: check for missing columns and add them
            existing = _get_table_columns(conn, "trades")
            for col, ctype in expected_cols.items():
                if col not in existing:
                    # sqlite allows ADD COLUMN (cannot add PK in ALTER)
                    try:
                        if col == "id":
                            # skip adding id to existing table (can't add PK easily)
                            logger.info("Existing trades table found without id column; leaving existing primary key as-is")
                            continue
                        alter_sql = f"ALTER TABLE trades ADD COLUMN {col} {ctype} DEFAULT NULL"
                        cur.execute(alter_sql)
                        conn.commit()
                        logger.info("Added missing column to trades: %s", col)
                    except Exception:
                        logger.exception("Failed to add column %s to trades", col)
    except Exception:
        logger.exception("init_trade_db failed")
    finally:
        conn.close()

    # ensure CSV exists with header matching our columns (do not overwrite existing CSV)
    if not os.path.exists(TRADES_CSV):
        try:
            with open(TRADES_CSV, "w", encoding="utf-8") as f:
                f.write("ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta\n")
        except Exception:
            logger.exception("Failed to create trades csv")

def record_trade(symbol, side, entry, sl, tp, lots, status="sim", pnl=0.0, rmult=0.0, regime="unknown", score=0.0, model_score=0.0, meta=None):
    """Insert a trade into the trades table. This function inspects the existing DB schema and writes only the columns present.
    It also supports legacy column `rm` by populating it with rmult for backwards compatibility.
    """
    ts = datetime.now(timezone.utc).isoformat()
    meta_json = json.dumps(meta or {})
    data = {
        "ts": ts,
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "lots": lots,
        "status": status,
        "pnl": pnl,
        "rmult": rmult,
        # legacy alias
        "rm": rmult,
        "regime": regime,
        "score": score,
        "model_score": model_score,
        "meta": meta_json,
    }
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cols = _get_table_columns(conn, "trades")
        if not cols:
            # if something is very wrong, try to re-init DB
            conn.close()
            init_trade_db()
            conn = sqlite3.connect(TRADES_DB, timeout=5)
            cur = conn.cursor()
            cols = _get_table_columns(conn, "trades")
        # prepare insert using intersection of expected data keys and actual columns
        insert_cols = [c for c in [
            "ts", "symbol", "side", "entry", "sl", "tp", "lots", "status", "pnl", "rmult", "rm", "regime", "score", "model_score", "meta"
        ] if c in cols]
        if not insert_cols:
            logger.error("No writable columns present in trades table; aborting record_trade")
            conn.close()
            return
        placeholders = ",".join(["?" for _ in insert_cols])
        col_list_sql = ",".join(insert_cols)
        values = [data.get(c) for c in insert_cols]
        cur.execute(f"INSERT INTO trades ({col_list_sql}) VALUES ({placeholders})", tuple(values))
        conn.commit(); conn.close()
    except Exception:
        logger.exception("record_trade db failed")
    try:
        # append to CSV (best-effort). Keep CSV format stable with rmult naming.
        with open(TRADES_CSV, "a", encoding="utf-8") as f:
            f.write("{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(ts, symbol, side, entry, sl, tp, lots, status, pnl, rmult, regime, score, model_score))
    except Exception:
        logger.exception("record_trade csv failed")

def get_recent_trades(limit=200):
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT ts,symbol,side,pnl,rmult,regime,score,model_score FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

# ---------------- MT5 mapping and helpers ----------------
_mt5 = None
_mt5_connected = False

def try_start_mt5_terminal():
    if MT5_PATH and os.path.exists(MT5_PATH):
        try:
            import subprocess
            subprocess.Popen([MT5_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2.5)
            return True
        except Exception:
            logger.exception("Failed to spawn MT5 terminal")
    return False

def connect_mt5(login: Optional[int] = None, password: Optional[str] = None, server: Optional[str] = None) -> bool:
    global _mt5, _mt5_connected
    if not MT5_AVAILABLE:
        logger.warning("MetaTrader5 python package not installed")
        return False
    try:
        _mt5 = mt5
    except Exception:
        logger.exception("mt5 import problem")
        return False
    login = login or (int(MT5_LOGIN) if MT5_LOGIN and str(MT5_LOGIN).isdigit() else None)
    password = password or MT5_PASSWORD
    server = server or MT5_SERVER
    if login is None or password is None or server is None:
        logger.warning("MT5 credentials missing; MT5 will not be used")
        return False
    try:
        ok = _mt5.initialize(login=login, password=password, server=server)
        if not ok:
            logger.warning("MT5 initialize failed: %s; trying to start terminal and retry", getattr(_mt5, "last_error", lambda: None)())
            try_start_mt5_terminal()
            time.sleep(2.5)
            try:
                _mt5.shutdown()
            except Exception:
                pass
            ok2 = _mt5.initialize(login=login, password=password, server=server)
            if not ok2:
                logger.error("MT5 initialize retry failed: %s", getattr(_mt5, "last_error", lambda: None)())
                _mt5_connected = False
                return False
        _mt5_connected = True
        logger.info("MT5 initialized (login=%s server=%s)", login, server)
        return True
    except Exception:
        logger.exception("MT5 connect error")
        _mt5_connected = False
        return False

def discover_broker_symbols():
    try:
        if _mt5_connected and _mt5 is not None:
            syms = _mt5.symbols_get()
            return [s.name for s in syms] if syms else []
    except Exception:
        logger.debug("discover_broker_symbols failed")
    return []

def map_symbol_to_broker(requested: str) -> str:
    r = str(requested).strip()
    # explicit mapping first
    if r in BROKER_SYMBOLS:
        return BROKER_SYMBOLS[r]
    if not (_mt5_connected and _mt5 is not None):
        return requested
    try:
        brokers = discover_broker_symbols()
        low_req = r.lower()
        for b in brokers:
            if b.lower() == low_req:
                return b
        # try common variants
        variants = [r, r + ".m", r + "m", r + "-m", r + ".M", r + "M"]
        for v in variants:
            for b in brokers:
                if b.lower() == v.lower():
                    return b
        for b in brokers:
            bn = b.lower()
            if low_req in bn or bn.startswith(low_req) or bn.endswith(low_req):
                return b
        # fallback: return as-is
    except Exception:
        logger.debug("map_symbol_to_broker error", exc_info=True)
    return requested

# ---------------- MT5-only data fetcher ----------------
def fetch_ohlcv_mt5(symbol: str, interval: str = "60m", period_days: int = 60):
    if not MT5_AVAILABLE or not _mt5_connected:
        return None
    try:
        broker_sym = map_symbol_to_broker(symbol)
        si = _mt5.symbol_info(broker_sym)
        if si is None:
            logger.info("Symbol not found on broker: %s (requested %s)", broker_sym, symbol)
            return None
        if not si.visible:
            try:
                _mt5.symbol_select(broker_sym, True)
            except Exception:
                pass
        tf_map = {
            "1m": _mt5.TIMEFRAME_M1,
            "5m": _mt5.TIMEFRAME_M5,
            "15m": _mt5.TIMEFRAME_M15,
            "30m": _mt5.TIMEFRAME_M30,
            "60m": _mt5.TIMEFRAME_H1,
            "1h": _mt5.TIMEFRAME_H1,
            "4h": _mt5.TIMEFRAME_H4,
            "1d": _mt5.TIMEFRAME_D1,
        }
        mt_tf = tf_map.get(interval, _mt5.TIMEFRAME_H1)
        count = 500
        try:
            if interval.endswith("m"):
                minutes = int(interval[:-1])
                bars_per_day = max(1, int(24 * 60 / minutes))
                count = max(120, period_days * bars_per_day)
            elif interval in ("1h", "60m"):
                count = max(120, period_days * 24)
            elif interval in ("4h",):
                count = max(120, int(period_days * 6))
            elif interval in ("1d",):
                count = max(60, period_days)
        except Exception:
            count = 500
        rates = _mt5.copy_rates_from_pos(broker_sym, mt_tf, 0, int(count))
        if rates is None:
            logger.info("MT5 returned no rates for %s", broker_sym)
            return None
        df = pd.DataFrame(rates)
        if "time" in df.columns:
            df.index = pd.to_datetime(df["time"], unit="s")
        if "open" not in df.columns and "open_price" in df.columns:
            df["open"] = df["open_price"]
        if "tick_volume" in df.columns:
            df["volume"] = df["tick_volume"]
        elif "real_volume" in df.columns:
            df["volume"] = df["real_volume"]
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception:
                    pass
            else:
                df[col] = pd.NA
        df = df[["open", "high", "low", "close", "volume"]].dropna(how="all")
        return df
    except Exception:
        logger.exception("fetch_ohlcv_mt5 error")
        return None

def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60):
    # MT5-only fetch: return None if MT5 not available or symbol not found
    df = fetch_ohlcv_mt5(symbol, interval=interval, period_days=period_days)
    if df is None or df.empty:
        logger.info("No MT5 data for %s (%s) - skipping", symbol, interval)
        return None
    return df

def fetch_multi_timeframes(symbol: str, period_days: int = 60):
    out = {}
    for label, intr in TIMEFRAMES.items():
        if label == "H4":
            base = fetch_ohlcv(symbol, interval="60m", period_days=period_days)
            if base is None or getattr(base, "empty", True):
                out[label] = None
                continue
            try:
                df4 = base.resample("4H").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
            except Exception:
                try:
                    df4 = base.resample("4h").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
                except Exception as e:
                    logger.info("Resample H4 failed for %s: %s", symbol, e)
                    out[label] = None
                    continue
            out[label] = df4
        else:
            out[label] = fetch_ohlcv(symbol, interval=intr, period_days=period_days)
    return out

# ---------------- Indicators & scoring (kept intact) ----------------
def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df
    try:
        if TA_AVAILABLE:
            df["sma5"] = SMAIndicator(df["close"], window=5).sma_indicator()
            df["sma20"] = SMAIndicator(df["close"], window=20).sma_indicator()
            df["rsi14"] = RSIIndicator(df["close"], window=14).rsi()
            df["atr14"] = AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()
            df["adx"] = ADXIndicator(df["high"], df["low"], df["close"], window=14).adx()
        else:
            df["sma5"] = df["close"].rolling(5, min_periods=1).mean()
            df["sma20"] = df["close"].rolling(20, min_periods=1).mean()
            delta = df["close"].diff()
            up = delta.clip(lower=0.0).rolling(14, min_periods=1).mean()
            down = -delta.clip(upper=0.0).rolling(14, min_periods=1).mean().replace(0, 1e-9)
            rs = up / down
            df["rsi14"] = 100 - (100 / (1 + rs))
            tr = pd.concat([(df["high"] - df["low"]).abs(), (df["high"] - df["close"].shift()).abs(), (df["low"] - df["close"].shift()).abs()], axis=1).max(axis=1)
            df["atr14"] = tr.rolling(14, min_periods=1).mean()
            df["adx"] = df["close"].diff().abs().rolling(14, min_periods=1).mean()
    except Exception:
        logger.exception("add_technical_indicators error")
    try:
        df = df.bfill().ffill().fillna(0.0)
    except Exception:
        try:
            df = df.fillna(0.0)
        except Exception:
            pass
    return df

def detect_market_regime_from_h1(df_h1: pd.DataFrame):
    try:
        if df_h1 is None or df_h1.empty:
            return "unknown", None, None
        d = add_technical_indicators(df_h1)
        atr = float(d["atr14"].iloc[-1])
        price = float(d["close"].iloc[-1]) if d["close"].iloc[-1] else 1.0
        rel = atr / price if price else 0.0
        adx = float(d["adx"].iloc[-1]) if "adx" in d.columns else 0.0
        if rel < 0.0025 and adx < 20:
            return "quiet", rel, adx
        if rel > 0.0075 and adx > 25:
            return "volatile", rel, adx
        if adx > 25:
            return "trending", rel, adx
        return "normal", rel, adx
    except Exception:
        logger.exception("detect_market_regime failed")
        return "unknown", None, None

def technical_signal_score(df: pd.DataFrame) -> float:
    try:
        if df is None or len(df) < 2:
            return 0.0
        latest = df.iloc[-1]; prev = df.iloc[-2]
        score = 0.0
        if prev["sma5"] <= prev["sma20"] and latest["sma5"] > latest["sma20"]:
            score += 0.6
        if prev["sma5"] >= prev["sma20"] and latest["sma5"] < latest["sma20"]:
            score -= 0.6
        r = float(latest.get("rsi14", 50) or 50)
        if r < 30:
            score += 0.25
        elif r > 70:
            score -= 0.25
        return max(-1.0, min(1.0, score))
    except Exception:
        return 0.0

def aggregate_multi_tf_scores(tf_dfs: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    techs = []
    for label, df in tf_dfs.items():
        try:
            if df is None or getattr(df, "empty", True):
                continue
            dfind = add_technical_indicators(df)
            t = technical_signal_score(dfind)
            weight = {"M30": 1.8, "H1": 1.2}.get(label, 1.0)
            techs.append((t, weight))
        except Exception:
            logger.exception("aggregate_multi_tf_scores failed for %s", label)
    if not techs:
        return {"tech": 0.0, "fund": 0.0, "sent": 0.0}
    s = sum(t * w for t, w in techs); w = sum(w for _, w in techs)
    return {"tech": float(s / w), "fund": 0.0, "sent": 0.0}

# ---------------- Multi-asset blending & fundamental awareness (ADDITIONS) ----------------

# cache portfolio weights (recomputed periodically)
_portfolio_weights_cache = {"ts": 0, "weights": {}}
PORTFOLIO_RECOMPUTE_SECONDS = 300  # recompute every 5 minutes

def compute_portfolio_weights(symbols: List[str], period_days: int = 45):
    """
    Lightweight multi-asset weighting:
      - compute recent returns volatility and pairwise correlations on H1
      - assign weights inverse to volatility, penalized by mean correlation
      - normalise so sum(weights) == 1 and then scaled to [0.6,1.4] factor when used for risk scaling
    Returns dict symbol -> normalized weight (sum=1)
    """
    global _portfolio_weights_cache
    now = time.time()
    if now - _portfolio_weights_cache.get("ts", 0) < PORTFOLIO_RECOMPUTE_SECONDS and _portfolio_weights_cache.get("weights"):
        return _portfolio_weights_cache["weights"]
    dfs = {}
    vols = {}
    rets = {}
    for s in symbols:
        try:
            df = fetch_ohlcv(s, interval="60m", period_days=period_days)
            if df is None or getattr(df, "empty", True):
                continue
            df = df.tail(24 * period_days)  # cap
            dfs[s] = df
            rets_s = df["close"].pct_change().dropna()
            rets[s] = rets_s
            vols[s] = rets_s.std() if not rets_s.empty else 1e-6
        except Exception:
            continue
    symbols_ok = list(rets.keys())
    if not symbols_ok:
        weights = {s: 1.0 / max(1, len(symbols)) for s in symbols}
        _portfolio_weights_cache = {"ts": now, "weights": weights}
        return weights
    # build correlation matrix
    try:
        rets_df = pd.DataFrame(rets)
        corr = rets_df.corr().fillna(0.0)
        avg_corr = corr.mean().to_dict()
    except Exception:
        avg_corr = {s: 0.0 for s in symbols_ok}
    # compute raw score = inverse volatility * (1 - avg_corr)
    raw = {}
    for s in symbols_ok:
        v = float(vols.get(s, 1e-6))
        ac = float(avg_corr.get(s, 0.0))
        raw_score = (1.0 / max(1e-6, v)) * max(0.0, (1.0 - ac))
        raw[s] = raw_score
    # fill missing symbols with small values
    for s in symbols:
        if s not in raw:
            raw[s] = 0.0001
    total = sum(raw.values()) or 1.0
    weights = {s: raw[s] / total for s in symbols}
    _portfolio_weights_cache = {"ts": now, "weights": weights}
    return weights

def get_portfolio_scale_for_symbol(symbol: str, weights: Dict[str, float]):
    """
    Convert normalized weights into a risk scaling factor in [0.6, 1.4].
    Heavier weight => slightly higher risk allowed (scale up), lower weight scale down.
    """
    if not weights or symbol not in weights:
        return 1.0
    w = float(weights.get(symbol, 0.0))
    # map [0, max] -> [0.6,1.4] with center at average weight
    avg = sum(weights.values()) / max(1, len(weights))
    # avoid division by zero
    if avg <= 0:
        return 1.0
    ratio = w / avg
    scale = 1.0 + (ratio - 1.0) * 0.4  # moderate scaling
    return max(0.6, min(1.4, scale))

def fetch_fundamental_score(symbol: str, lookback_days: int = 7) -> float:
    """
    Lightweight fundamental sentiment proxy:
      - If NEWS_API_KEY is set and requests available, query a news API for headlines mentioning the symbol name (best-effort)
      - Score range [-1, 1] where positive means generally positive coverage
    If external API not available, returns 0.0
    Note: this is a best-effort, optional component; users should set NEWS_API_KEY to enable.
    """
    if not FUNDAMENTAL_AVAILABLE or not NEWS_API_KEY:
        return 0.0
    # Map symbol to natural language query
    query = symbol
    # heuristics for metals/crypto
    if symbol.upper() in ("XAUUSD", "GOLD"):
        query = "gold OR xauusd"
    if symbol.upper() in ("XAGUSD", "SILVER"):
        query = "silver OR xagusd"
    if symbol.upper().endswith("USD") and symbol.upper().startswith("BTC"):
        query = "bitcoin OR btc"
    # Use NewsAPI.org as a common provider (user must supply NEWS_API_KEY)
    try:
        # free tier: everything endpoint limited; this is best-effort
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "language": "en",
            "pageSize": 30,
            "sortBy": "relevancy",
            "from": (datetime.utcnow() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d"),
            "apiKey": NEWS_API_KEY,
        }
        resp = requests.get(url, params=params, timeout=6)
        if resp.status_code != 200:
            logger.debug("NewsAPI non-200: %s %s", resp.status_code, resp.text[:200])
            return 0.0
        j = resp.json()
        articles = j.get("articles", [])[:30]
        if not articles:
            return 0.0
        # naive sentiment scoring using presence of positive/negative words (lightweight, no NLP libs)
        pos_words = {"gain", "rise", "surge", "up", "positive", "bull", "beats", "beat", "record", "rally", "higher"}
        neg_words = {"fall", "drop", "down", "loss", "negative", "bear", "miss", "misses", "crash", "decline", "lower"}
        score = 0.0
        for a in articles:
            title = a.get("title") or ""
            desc = a.get("description") or ""
            txt = (title + " " + desc).lower()
            p = sum(1 for w in pos_words if w in txt)
            n = sum(1 for w in neg_words if w in txt)
            score += (p - n)
        # normalize to [-1,1]
        max_possible = max(1, len(articles) * 2)
        normalized = max(-1.0, min(1.0, score / float(max_possible)))
        return float(normalized)
    except Exception:
        logger.exception("fetch_fundamental_score failed")
        return 0.0

# ---------------- Real-time economic calendar (ADDED) ----------------

def _symbol_to_currencies(symbol: str) -> List[str]:
    """
    Map a symbol to associated currencies for calendar lookup.
    EURUSD -> ['EUR','USD']; XAUUSD -> ['XAU','USD'], BTCUSD -> ['BTC','USD']
    """
    s = symbol.upper()
    if len(s) >= 6:
        base = s[:3]
        quote = s[3:6]
        return [base, quote]
    # fallback heuristics
    if s.startswith("XAU") or "XAU" in s:
        return ["XAU", "USD"]
    if s.startswith("XAG") or "XAG" in s:
        return ["XAG", "USD"]
    if s.startswith("BTC"):
        return ["BTC", "USD"]
    return [s]

def fetch_economic_calendar_events(lookback_hours: int = 6, lookahead_hours: int = 6) -> List[Dict[str, Any]]:
    """
    Fetch calendar events from TradingEconomics (best-effort).
    Returns a list of event dicts or empty list on failure.
    Requires TRADING_ECONOMICS_KEY environment variable.
    """
    if not FUNDAMENTAL_AVAILABLE or not TRADING_ECONOMICS_KEY:
        return []
    try:
        now_utc = datetime.utcnow()
        since = (now_utc - pd.Timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%S")
        ahead = (now_utc + pd.Timedelta(hours=lookahead_hours)).strftime("%Y-%m-%dT%H:%M:%S")
        url = f"https://api.tradingeconomics.com/calendar/events?c={TRADING_ECONOMICS_KEY}&d1={since}&d2={ahead}"
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            logger.debug("TradingEconomics non-200: %s", resp.status_code)
            return []
        events = resp.json()
        # normalize keys (best-effort) so callers can read Impact, Country, Actual, Consensus, Date
        out = []
        for e in events:
            try:
                out.append(e)
            except Exception:
                continue
        return out
    except Exception:
        logger.exception("fetch_economic_calendar_events failed")
        return []

def fetch_economic_calendar_score(symbol: str, lookback_hours: int = 6, lookahead_hours: int = 6) -> float:
    """
    Convert recent/upcoming economic releases into a sentiment score [-1,1].
    +1 means overall better-than-expected or positive events for the symbol currencies,
    -1 means worse-than-expected or negative events.
    Only high-impact events are counted, normalized by number of high-impact events found.
    """
    if not FUNDAMENTAL_AVAILABLE or not TRADING_ECONOMICS_KEY:
        return 0.0
    try:
        evs = fetch_economic_calendar_events(lookback_hours=lookback_hours, lookahead_hours=lookahead_hours)
        if not evs:
            return 0.0
        related = []
        currs = _symbol_to_currencies(symbol)
        for e in evs:
            try:
                impact = e.get("Impact") or e.get("importance") or e.get("importanceText") or e.get("impact", "")
                country = (e.get("Country") or e.get("country") or "").upper()
                title = (e.get("Event") or e.get("Title") or e.get("event") or "").lower()
                if not impact:
                    continue
                if str(impact).lower() not in ("high", "h", "high impact"):
                    continue
                match = False
                for c in currs:
                    if c and (c.lower() in title or c.upper() == country or c.upper() in str(e.get("Category", "")).upper()):
                        match = True
                if match:
                    related.append(e)
            except Exception:
                continue
        if not related:
            return 0.0
        score = 0.0
        count = 0
        for e in related:
            actual = e.get("Actual") or e.get("actual") or e.get("Value") or e.get("value")
            forecast = e.get("Consensus") or e.get("Forecast") or e.get("consensus") or e.get("forecast")
            try:
                if actual is None or str(actual).strip() == "":
                    continue
                actual_val = float(str(actual).replace(",", ""))
                forecast_val = float(str(forecast).replace(",", "")) if forecast not in (None, "", "None") else None
                if forecast_val is None:
                    continue
                if actual_val > forecast_val:
                    score += 1.0
                elif actual_val < forecast_val:
                    score -= 1.0
                count += 1
            except Exception:
                count += 1
                continue
        if count == 0:
            return 0.0
        return max(-1.0, min(1.0, score / float(count)))
    except Exception:
        logger.exception("fetch_economic_calendar_score failed")
        return 0.0

def should_pause_for_events(symbol: str, lookahead_minutes: int = 30) -> (bool, Optional[Dict[str, Any]]):
    """
    Return (True, event) if there is a high-impact economic event for the symbol's currencies within lookahead_minutes.
    Useful to pause trading around major releases.
    """
    try:
        if not FUNDAMENTAL_AVAILABLE or not TRADING_ECONOMICS_KEY:
            return False, None
        evs = fetch_economic_calendar_events(lookback_hours=0, lookahead_hours=int(max(1, lookahead_minutes / 60)))
        if not evs:
            return False, None
        now = datetime.utcnow()
        currs = _symbol_to_currencies(symbol)
        for e in evs:
            impact = e.get("Impact") or e.get("importance") or e.get("impact", "")
            if not impact or str(impact).lower() not in ("high", "h", "high impact"):
                continue
            # parse event datetime
            when = None
            for key in ("Date", "date", "Scheduled", "dateTime"):
                if key in e and e.get(key):
                    try:
                        when = pd.to_datetime(e.get(key))
                        break
                    except Exception:
                        continue
            if when is None:
                continue
            diff = (when.to_pydatetime().replace(tzinfo=None) - now).total_seconds() / 60.0
            if diff < 0:
                continue
            if diff <= lookahead_minutes:
                title = (e.get("Event") or e.get("Title") or "").lower()
                country = (e.get("Country") or "").upper()
                for c in currs:
                    if c and (c.lower() in title or c.upper() == country):
                        return True, {"event": title, "minutes_to": diff, "impact": impact, "raw": e}
        return False, None
    except Exception:
        logger.exception("should_pause_for_events failed")
        return False, None

# ---------------- simple ML hooks (ENHANCED) ----------------
model_pipe = None

def build_model():
    """
    Enhanced model builder:
      - Prefer RandomForestClassifier if sklearn available
      - Fallback to SGDClassifier logistic if RandomForest not available
      - Pipeline includes scaling for linear models
    """
    if not SKLEARN_AVAILABLE:
        return None
    try:
        # prefer RandomForest when available (non-linear)
        if 'RandomForestClassifier' in globals():
            clf = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=1)
            return Pipeline([("clf", clf)])
        else:
            # Use a robust linear classifier if RandomForest isn't present
            pipe = Pipeline([("scaler", StandardScaler()), ("clf", SGDClassifier(loss="log", max_iter=5000, tol=1e-5, random_state=42, warm_start=True))])
            return pipe
    except Exception:
        try:
            pipe = Pipeline([("scaler", StandardScaler()), ("clf", SGDClassifier(loss="log", max_iter=5000, tol=1e-5, random_state=42, warm_start=True))])
            return pipe
        except Exception:
            return None

def load_model():
    global model_pipe
    if not SKLEARN_AVAILABLE:
        return None
    if os.path.exists(MODEL_FILE):
        try:
            model_pipe = joblib.load(MODEL_FILE)
            logger.info("Loaded ML model")
            return model_pipe
        except Exception:
            logger.exception("Load model failed")
    # attempt to build fresh model placeholder (do NOT force heavy training here)
    try:
        model_pipe = build_model()
        return model_pipe
    except Exception:
        return None

if SKLEARN_AVAILABLE:
    load_model()

def extract_features_for_model(df_h1: pd.DataFrame, tech_score: float, symbol: str, regime_code: int):
    """
    Build a small feature vector for ML:
      - tech_score, recent ATR/price ratio, recent volatility, momentum (rsi), volume change, regime_code
    Returns numpy array shape (1, n)
    """
    try:
        d = add_technical_indicators(df_h1.copy())
        entry = float(d["close"].iloc[-1])
        atr = float(d["atr14"].iloc[-1] or 0.0)
        vol = float(d["volume"].iloc[-1] or 0.0)
        rsi = float(d.get("rsi14", pd.Series([50])).iloc[-1] if "rsi14" in d.columns else 50)
        vol_mean = float(d["volume"].tail(50).mean() or 1.0)
        vol_change = (vol - vol_mean) / (vol_mean if vol_mean else 1.0)
        atr_rel = atr / (entry if entry else 1.0)
        features = np.array([[tech_score, atr_rel, rsi, vol_change, regime_code]], dtype=float)
        return features
    except Exception:
        # fallback features
        return np.array([[tech_score, 0.0, 50.0, 0.0, regime_code]], dtype=float)

# ---------------- simulation/backtest and optimizer (unchanged in logic) ----------------
def simulate_strategy_on_series(df_h1, threshold, atr_mult=1.25, max_trades=200):
    if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 80:
        return {"n": 0, "net": 0.0, "avg_r": 0.0, "win": 0.0}
    df = add_technical_indicators(df_h1.copy())
    trades = []
    for i in range(30, len(df) - 10):
        window = df.iloc[: i + 1]
        score = technical_signal_score(window)
        if score >= threshold:
            side = "BUY"
        elif score <= -threshold:
            side = "SELL"
        else:
            continue
        entry = float(df["close"].iloc[i])
        atr = float(df["atr14"].iloc[i] or 0.0)
        stop = atr * atr_mult
        if side == "BUY":
            sl = entry - stop
            tp = entry + stop * 2.0
        else:
            sl = entry + stop
            tp = entry - stop * 2.0
        r_mult = 0.0
        for j in range(i + 1, min(i + 31, len(df))):
            high = float(df["high"].iloc[j]); low = float(df["low"].iloc[j])
            if side == "BUY":
                if high >= tp:
                    r_mult = 2.0; break
                if low <= sl:
                    r_mult = -1.0; break
            else:
                if low <= tp:
                    r_mult = 2.0; break
                if high >= sl:
                    r_mult = -1.0; break
        trades.append(r_mult)
        if len(trades) >= max_trades:
            break
    n = len(trades)
    if n == 0:
        return {"n": 0, "net": 0.0, "avg_r": 0.0, "win": 0.0}
    net = sum(trades); avg = net / n; win = sum(1 for t in trades if t > 0) / n
    return {"n": n, "net": net, "avg_r": avg, "win": win}

def light_optimizer(symbols, budget=12):
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    logger.info("Starting light optimizer")
    candidates = []
    for _ in range(budget):
        cand_thresh = max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD + random.uniform(-0.06, 0.06)))
        cand_risk = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, RISK_PER_TRADE_PCT * random.uniform(0.6, 1.4)))
        stats = []
        for s in symbols:
            df = fetch_multi_timeframes(s, period_days=60).get("H1")
            if df is None or getattr(df, "empty", True):
                continue
            st = simulate_strategy_on_series(df, cand_thresh, atr_mult=1.25, max_trades=120)
            if st["n"] > 0:
                stats.append(st)
        if not stats:
            continue
        total_n = sum(st["n"] for st in stats)
        avg_expect = sum(st["avg_r"] * st["n"] for st in stats) / total_n
        candidates.append((avg_expect, cand_thresh, cand_risk))
    if not candidates:
        logger.info("Optimizer found no candidates")
        return None
    candidates.sort(reverse=True, key=lambda x: x[0])
    best_expect, best_thresh, best_risk = candidates[0]
    baseline_stats = []
    for s in symbols:
        df = fetch_multi_timeframes(s, period_days=60).get("H1")
        if df is None or getattr(df, "empty", True):
            continue
        baseline_stats.append(simulate_strategy_on_series(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=120))
    base_n = sum(st["n"] for st in baseline_stats) or 1
    base_expect = sum(st["avg_r"] * st["n"] for st in baseline_stats) / base_n if baseline_stats else 0.0
    if best_expect > base_expect + 0.02:
        step = 0.4
        CURRENT_THRESHOLD = float(max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD * (1 - step) + best_thresh * step)))
        RISK_PER_TRADE_PCT = float(max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, RISK_PER_TRADE_PCT * (1 - step) + best_risk * step)))
        validate_and_clamp_thresholds()
        save_adapt_state()
        logger.info("Optimizer applied new threshold=%.3f risk=%.5f", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
        return {"before": base_expect, "after": best_expect, "threshold": CURRENT_THRESHOLD, "risk": RISK_PER_TRADE_PCT}
    logger.info("Optimizer skipped applying")
    return None

# ---------------- Execution, decision & runner (kept, but decision enhanced) ----------------
cycle_counter = 0

def compute_lots_from_risk(risk_pct, balance, entry_price, stop_price):
    try:
        risk_amount = balance * risk_pct
        pip_risk = abs(entry_price - stop_price)
        if pip_risk <= 0:
            return 0.01
        lots = risk_amount / (pip_risk * 100000)
        return max(0.01, round(lots, 2))
    except Exception:
        return 0.01

def place_order_simulated(symbol, side, lots, entry, sl, tp, score, model_score, regime):
    record_trade(symbol, side, entry, sl, tp, lots, status="sim_open", pnl=0.0, rmult=0.0, regime=regime, score=score, model_score=model_score)
    return {"status":"sim_open"}

def place_order_mt5(symbol, action, lot, price, sl, tp):
    """
    Broker-aware order sender:
      - maps symbol
      - enforces volume_min and volume_step
      - enforces minimum stop level (SL/TP) distances in points (with safe fallbacks)
      - returns clear status explaining why MT5 refused the order
    """
    if not MT5_AVAILABLE or not _mt5_connected:
        return {"status": "mt5_not_connected"}

    try:
        broker = map_symbol_to_broker(symbol)

        # get symbol info (trade properties) and tick
        si = _mt5.symbol_info(broker)
        if si is None:
            return {"status": "symbol_not_found", "symbol": broker}

        # ensure symbol is visible/selected
        try:
            if not si.visible:
                _mt5.symbol_select(broker, True)
        except Exception:
            pass

        tick = _mt5.symbol_info_tick(broker)
        if tick is None:
            return {"status": "no_tick", "symbol": broker}

        # read broker constraints with safe fallbacks
        vol_min   = getattr(si, "volume_min", None) or getattr(si, "volume_min", 0.01) or 0.01
        vol_step  = getattr(si, "volume_step", None) or getattr(si, "volume_step", 0.01) or 0.01
        vol_max   = getattr(si, "volume_max", None) or getattr(si, "volume_max", None)

        # points/precision info (some symbols have 'point' attribute)
        point     = getattr(si, "point", None) or getattr(si, "trade_tick_size", None) or getattr(si, "tick_size", None) or 0.00001
        stop_level = getattr(si, "stop_level", None)  # in *points* typically

        # compute minimum SL/TP distance in price units
        if stop_level is not None and stop_level >= 0:
            min_sl_dist = float(stop_level) * float(point)
        else:
            # fallback: require at least 10 * point distance (safe default)
            min_sl_dist = float(point) * 10.0

        # choose order price (market price if not provided)
        order_price = price if price is not None else (tick.ask if action == "BUY" else tick.bid)

        # ensure lot size respects broker's min and step
        try:
            lots = float(lot)
        except Exception:
            lots = float(vol_min)
        # snap lots up to nearest multiple of vol_step and >= vol_min
        try:
            if vol_step > 0:
                # compute number of steps from vol_min to requested lots
                steps = max(0, int((lots - vol_min) // vol_step))
                lots_adj = vol_min + steps * vol_step
                # if requested greater than lots_adj, try to ceil to next step
                if lots > lots_adj:
                    # ceil to next step
                    steps_ceil = int(((lots - vol_min) + vol_step - 1e-12) // vol_step)
                    lots_adj = vol_min + steps_ceil * vol_step
                lots = round(float(max(vol_min, lots_adj)), 2)
            else:
                lots = float(max(vol_min, lots))
        except Exception:
            lots = float(max(vol_min, 0.01))

        # validate SL/TP distances: compute absolute distances from entry
        entry_price = float(order_price)
        def valid_distance(dist):
            try:
                return (dist is not None) and (abs(dist) >= min_sl_dist)
            except Exception:
                return False

        sl_ok = True; tp_ok = True
        if sl is not None:
            sl_dist = abs(entry_price - float(sl))
            sl_ok = valid_distance(sl_dist)
        if tp is not None:
            tp_dist = abs(entry_price - float(tp))
            tp_ok = valid_distance(tp_dist)

        # if SL/TP invalid, try to adjust them to the minimum allowed distance in the correct direction
        if not sl_ok:
            if action == "BUY":
                sl = entry_price - min_sl_dist
            else:
                sl = entry_price + min_sl_dist
            sl_ok = True
        if not tp_ok:
            if action == "BUY":
                tp = entry_price + (min_sl_dist * 2.0)
            else:
                tp = entry_price - (min_sl_dist * 2.0)
            tp_ok = True

        # final sanity: ensure lots >= vol_min
        if lots < vol_min:
            lots = float(vol_min)

        # respect maximum volume if reported
        if vol_max and lots > vol_max:
            return {"status": "volume_too_large", "requested": lots, "max": vol_max}

        order_type = _mt5.ORDER_TYPE_BUY if action == "BUY" else _mt5.ORDER_TYPE_SELL
        req = {
            "action": _mt5.TRADE_ACTION_DEAL,
            "symbol": broker,
            "volume": float(lots),
            "type": order_type,
            "price": float(order_price),
            "sl": float(sl) if sl is not None else 0.0,
            "tp": float(tp) if tp is not None else 0.0,
            "deviation": 20,
            "magic": 123456,
            "comment": "Ultra_instinct",
            "type_time": _mt5.ORDER_TIME_GTC,
            "type_filling": _mt5.ORDER_FILLING_IOC,
        }

        res = _mt5.order_send(req)
        retcode = getattr(res, "retcode", None)

        # return clearer statuses for common retcodes
        if retcode == 10027:
            return {"status": "autotrading_disabled", "retcode": retcode, "result": str(res)}
        if retcode is not None and retcode != 0:
            return {"status": "rejected", "retcode": retcode, "result": str(res)}

        # success (retcode 0)
        return {"status": "sent", "result": str(res), "used_lots": lots}

    except Exception:
        logger.exception("place_order_mt5 failed")
        return {"status": "error"}

def get_today_trade_count():
    """
    Robust daily trade count with explicit reset logic:
      - Uses DAILY_RESET_TZ env var to choose reset reference:
          * "UTC"   -> midnight UTC (default)
          * "LOCAL" -> midnight local machine time
          * "BROKER"-> broker/server current date (requires MT5 connected)
      - Reads all 'ts' values from trades DB and filters in Python (safer than SQL datetime comparisons).
      - Handles ISO timestamps with/without timezone. If a timestamp is naive (no tz), treat it as UTC.
      - Returns integer count of trades with ts >= chosen day's midnight (in UTC for comparison).
    """
    try:
        # Read all timestamps from DB
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT ts FROM trades")
        rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("get_today_trade_count: DB read failed")
        return 0

    # Determine the day's midnight (as aware datetime in UTC) based on mode
    reset_mode = os.getenv("DAILY_RESET_TZ", "UTC").strip().upper()
    start_utc = None
    try:
        if reset_mode == "BROKER" and MT5_AVAILABLE and _mt5_connected:
            try:
                # mt5.time_current returns epoch seconds (UTC). Use it to get broker current date.
                broker_now_ts = _mt5.time_current()
                if broker_now_ts:
                    broker_now = datetime.utcfromtimestamp(int(broker_now_ts))
                    broker_date = broker_now.date()
                    start_utc = datetime(broker_date.year, broker_date.month, broker_date.day, tzinfo=timezone.utc)
                else:
                    # fallback to UTC
                    today = datetime.utcnow().date()
                    start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
            except Exception:
                logger.debug("get_today_trade_count: broker time fetch failed, falling back to UTC", exc_info=True)
                today = datetime.utcnow().date()
                start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        elif reset_mode == "LOCAL":
            # compute local midnight then convert to UTC
            try:
                local_now = datetime.now().astimezone()
                local_date = local_now.date()
                local_midnight = datetime(local_date.year, local_date.month, local_date.day, tzinfo=local_now.tzinfo)
                start_utc = local_midnight.astimezone(timezone.utc)
            except Exception:
                logger.debug("get_today_trade_count: local timezone conversion failed, falling back to UTC", exc_info=True)
                today = datetime.utcnow().date()
                start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        else:
            # Default: UTC midnight
            today = datetime.utcnow().date()
            start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    except Exception:
        # ultimate fallback
        today = datetime.utcnow().date()
        start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)

    count = 0
    for (ts_raw,) in rows:
        if not ts_raw:
            continue
        parsed = None
        try:
            # Try parsing as timezone-aware (assume UTC if 'Z' or offset present)
            parsed = pd.to_datetime(ts_raw, utc=True, errors="coerce")
        except Exception:
            parsed = None
        if pd.isna(parsed):
            # Try parse naive then assume UTC
            try:
                parsed_naive = pd.to_datetime(ts_raw, errors="coerce")
                if pd.isna(parsed_naive):
                    continue
                # treat naive timestamps as UTC (this matches how record_trade writes timestamps)
                parsed = parsed_naive.replace(tzinfo=timezone.utc)
            except Exception:
                continue
        # Ensure parsed is timezone-aware in UTC
        try:
            if getattr(parsed, "tzinfo", None) is None:
                parsed = parsed.tz_localize(timezone.utc)
        except Exception:
            # if pandas Timestamp, convert to python datetime with tzinfo=UTC
            try:
                parsed = pd.to_datetime(parsed).to_pydatetime()
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        # Compare
        try:
            # normalize to UTC-aware datetime
            if isinstance(parsed, pd.Timestamp):
                parsed_dt = parsed.to_pydatetime()
            else:
                parsed_dt = parsed
            # If parsed_dt has no tzinfo, assume UTC
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            if parsed_dt >= start_utc:
                count += 1
        except Exception:
            continue

    return int(count)

def make_decision_for_symbol(symbol: str, live: bool=False):
    global cycle_counter, model_pipe, CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    try:
        tfs = fetch_multi_timeframes(symbol, period_days=60)
        df_h1 = tfs.get("H1")
        if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 40:
            logger.info("Not enough H1 data for %s - skipping", symbol)
            return None
        scores = aggregate_multi_tf_scores(tfs)
        tech_score = scores["tech"]
        model_score = 0.0
        fundamental_score = 0.0

        # optional model scoring (enhanced)
        if SKLEARN_AVAILABLE and model_pipe is not None:
            try:
                regime, rel, adx = detect_market_regime_from_h1(df_h1)
                entry = float(df_h1["close"].iloc[-1])
                atr = float(add_technical_indicators(df_h1)["atr14"].iloc[-1])
                dist = (atr * 1.25) / (entry if entry != 0 else 1.0)
                regime_code = 0 if regime == "normal" else (1 if regime == "quiet" else 2)
                X = extract_features_for_model(df_h1, tech_score, symbol, regime_code)
                # model might be untrained; guard predict_proba with try
                try:
                    proba = model_pipe.predict_proba(X)[:,1][0]
                    model_score = float((proba - 0.5) * 2.0)
                except Exception:
                    # fallback: try predict -> map {0,1} to [-1,1]
                    try:
                        pred = model_pipe.predict(X)[0]
                        model_score = 0.9 if pred == 1 else -0.9
                    except Exception:
                        model_score = 0.0
            except Exception:
                model_score = 0.0

        # fundamental/sentiment score (ENHANCED: news + econ calendar)
        try:
            news_sent = 0.0
            econ_sent = 0.0
            try:
                news_sent = fetch_fundamental_score(symbol)
            except Exception:
                news_sent = 0.0
            try:
                # use 12h window historical + 12h ahead to capture recent surprises
                econ_sent = fetch_economic_calendar_score(symbol, lookback_hours=12, lookahead_hours=12)
            except Exception:
                econ_sent = 0.0
            # combine them, weight news more but keep econ important
            fundamental_score = float(0.65 * news_sent + 0.35 * econ_sent)
        except Exception:
            fundamental_score = 0.0

        # check trade-pause logic for imminent high-impact events
        try:
            pause, ev = should_pause_for_events(symbol, lookahead_minutes=PAUSE_BEFORE_EVENT_MINUTES)
            if pause:
                # If a high-impact event is within the pause window, skip trading this symbol now.
                logger.info("Pausing trading for %s due to upcoming event (in %.1f minutes): %s", symbol, ev.get("minutes_to", -1), ev.get("event", "unknown"))
                decision = {"symbol": symbol, "agg": 0.0, "tech": tech_score, "model_score": model_score, "fund_score": fundamental_score, "final": None, "paused": True, "pause_event": ev}
                return decision
        except Exception:
            # if pause check fails, continue normally
            pass

        # portfolio-aware weight adjustments
        try:
            weights = compute_portfolio_weights(SYMBOLS, period_days=45)
            port_scale = get_portfolio_scale_for_symbol(symbol, weights)
        except Exception:
            port_scale = 1.0

        # combine scores with new weighting (tech primary, model secondary, fundamentals important)
        # Adjusted weights for stronger fundamentals influence in real world
        total_score = (0.40 * tech_score) + (0.25 * model_score) + (0.35 * fundamental_score)

        # ---- NORMALIZE total_score to [-1, 1] to be safe ----
        if total_score > 1.0:
            total_score = 1.0
        elif total_score < -1.0:
            total_score = -1.0

        # small portfolio directional adjustment (scale roughly between 0.6 and 1.4)
        total_score = total_score * (0.5 + 0.5 * port_scale)  # scale in [~0.8,~1.2] depending on port_scale

        candidate = None
        if total_score >= 0.18:
            candidate = "BUY"
        if total_score <= -0.18:
            candidate = "SELL"
        final_signal = None
        # Use absolute threshold of CURRENT_THRESHOLD (validated) to require stronger signal
        if candidate is not None and abs(total_score) >= CURRENT_THRESHOLD:
            final_signal = candidate
        decision = {"symbol": symbol, "agg": total_score, "tech": tech_score, "model_score": model_score, "fund_score": fundamental_score, "final": final_signal, "port_scale": port_scale, "paused": False}
        if final_signal:
            entry = float(df_h1["close"].iloc[-1])
            atr = float(add_technical_indicators(df_h1)["atr14"].iloc[-1])
            stop_dist = max(1e-6, atr * 1.25)
            if final_signal == "BUY":
                sl = entry - stop_dist; tp = entry + stop_dist * 2.0
            else:
                sl = entry + stop_dist; tp = entry - stop_dist * 2.0
            regime, rel, adx = detect_market_regime_from_h1(df_h1)
            risk_pct = RISK_PER_TRADE_PCT
            # apply portfolio scaling to risk (keeps risk bounds)
            risk_pct = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, risk_pct * port_scale))
            if regime == "volatile":
                risk_pct = max(MIN_RISK_PER_TRADE_PCT, risk_pct * 0.6)
            elif regime == "quiet":
                risk_pct = min(MAX_RISK_PER_TRADE_PCT, risk_pct * 1.15)
            if os.path.exists(KILL_SWITCH_FILE):
                logger.info("Kill switch engaged - skipping order for %s", symbol)
                return decision
            # re-validate thresholds just before checking daily cap / placing trade
            validate_and_clamp_thresholds()
            if live and get_today_trade_count() >= MAX_DAILY_TRADES:
                logger.info("Daily trade cap reached - skipping")
                return decision
            balance = float(os.getenv("FALLBACK_BALANCE", "650.0"))
            lots = compute_lots_from_risk(risk_pct, balance, entry, sl)
            if live and not DEMO_SIMULATION:
                res = place_order_mt5(symbol, final_signal, lots, None, sl, tp)
                record_trade(symbol, final_signal, entry, sl, tp, lots, status=res.get("status", "unknown"), pnl=0.0, rmult=0.0, regime=regime, score=tech_score, model_score=model_score, meta=res)
                # TELEGRAM NOTIFICATION: attempt to notify immediately after recording a live trade
                try:
                    status = res.get("status", "unknown")
                    if status == "sent":
                        emoji = "✅"; status_text = "EXECUTED"
                    elif status == "rejected":
                        emoji = "❌"; status_text = "REJECTED"
                    elif status == "autotrading_disabled":
                        emoji = "⚠️"; status_text = "AUTO TRADING DISABLED"
                    else:
                        emoji = "⚠️"; status_text = str(status).upper()
                    # format numeric display: round to sensible decimals
                    try:
                        entry_s = f"{float(entry):.2f}"; sl_s = f"{float(sl):.2f}"; tp_s = f"{float(tp):.2f}"
                    except Exception:
                        entry_s = str(entry); sl_s = str(sl); tp_s = str(tp)
                    msg = (
                        f"Ultra_instinct signal\n"
                        f"{emoji} {status_text}\n"
                        f"{final_signal} {symbol}\n"
                        f"Lots: {lots}\n"
                        f"Entry: {entry_s}\n"
                        f"SL: {sl_s}\n"
                        f"TP: {tp_s}"
                    )
                    send_telegram_message(msg)
                except Exception:
                    logger.exception("Telegram notify failed after live trade")
            else:
                res = place_order_simulated(symbol, final_signal, lots, entry, sl, tp, tech_score, model_score, regime)
            decision.update({"entry": entry, "sl": sl, "tp": tp, "lots": lots, "placed": res})
        else:
            logger.info("No confident signal for %s (agg=%.3f)", symbol, total_score)
        # Debug snapshot (non-intrusive)
        try:
            logger.info(
                "DEBUG_EXEC -> sym=%s agg=%.5f candidate=%s final_signal=%s CURRENT_THRESHOLD=%.5f port_scale=%.3f paused=%s",
                symbol,
                float(total_score),
                str(candidate),
                str(final_signal),
                float(CURRENT_THRESHOLD),
                float(port_scale),
                decision.get("paused", False) if isinstance(decision, dict) else False
            )
        except Exception:
            logger.exception("DEBUG_EXEC snapshot failed for %s", symbol)
        return decision
    except Exception:
        logger.exception("make_decision_for_symbol failed for %s", symbol)
        return None

def adapt_and_optimize():
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    try:
        recent = get_recent_trades(limit=200)
        vals = [r[3] for r in recent if r[3] is not None]
        n = len(vals)
        winrate = sum(1 for v in vals if v > 0) / n if n > 0 else 0.0
        logger.info("Adapt: recent winrate=%.3f n=%d", winrate, n)

        # ===== Threshold Adaptation (Proportional + Clamp) =====
        ADAPT_MIN_TRADES = 40          # require decent sample size
        TARGET_WINRATE = 0.525        # midpoint between 0.45 and 0.60
        K = 0.04                      # proportional strength
        MAX_ADJ = 0.01                # maximum threshold movement per cycle

        if n >= ADAPT_MIN_TRADES:
            # proportional adjustment
            adj = -K * (winrate - TARGET_WINRATE)

            # clamp movement to prevent instability
            if adj > MAX_ADJ:
                adj = MAX_ADJ
            elif adj < -MAX_ADJ:
                adj = -MAX_ADJ

            # apply and enforce bounds
            CURRENT_THRESHOLD = float(
                max(MIN_THRESHOLD,
                    min(MAX_THRESHOLD, CURRENT_THRESHOLD + adj))
            )
            # ensure thresholds are still valid after change
            validate_and_clamp_thresholds()

            logger.info(
                "Threshold adapted -> winrate=%.3f, adj=%.5f, new_threshold=%.5f",
                winrate, adj, CURRENT_THRESHOLD
            )

        vols = []
        for s in SYMBOLS:
            tfs = fetch_multi_timeframes(s, period_days=45)
            h1 = tfs.get("H1")
            if h1 is None or getattr(h1, "empty", True):
                continue
            _, rel, adx = detect_market_regime_from_h1(h1)
            if rel is not None:
                vols.append(rel)
        if vols:
            avg_vol = sum(vols) / len(vols)
            target = 0.003
            scale = target / avg_vol if avg_vol else 1.0
            scale = max(0.6, min(1.6, scale))
            new_risk = BASE_RISK_PER_TRADE_PCT * scale
            if n >= 20 and sum(vals) < 0:
                new_risk *= 0.7
            RISK_PER_TRADE_PCT = float(max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, new_risk)))
        save_adapt_state()
        # recompute portfolio weights periodically so decision uses fresh weights
        try:
            compute_portfolio_weights(SYMBOLS, period_days=45)
        except Exception:
            pass
        if DEMO_SIMULATION:
            light_optimizer(SYMBOLS, budget=8)
        if SKLEARN_AVAILABLE:
            try:
                # Example placeholder: train a model if enough trades exist.
                # Left intentionally lightweight: user can enable model training with real labeled data.
                pass
            except Exception:
                logger.debug("train model failed")
    except Exception:
        logger.exception("adapt_and_optimize failed")

def run_cycle(live=False):
    global cycle_counter
    cycle_counter += 1
    if cycle_counter % ADAPT_EVERY_CYCLES == 0:
        adapt_and_optimize()
    results = {}
    for s in SYMBOLS:
        try:
            r = make_decision_for_symbol(s, live=live)
            results[s] = r
            time.sleep(0.2)
        except Exception:
            logger.exception("run_cycle symbol %s failed", s)
    return results

def main_loop(live=False):
    logger.info("Starting loop live=%s demo=%s thr=%.3f risk=%.5f", live, DEMO_SIMULATION, CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
    try:
        while True:
            run_cycle(live=live)
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        save_adapt_state()

# ---------------- CLI / startup ----------------
def run_backtest():
    logger.info("Running backtest for symbols: %s", SYMBOLS)
    for s in SYMBOLS:
        df = fetch_multi_timeframes(s, period_days=365).get("H1")
        if df is None:
            logger.info("No H1 for %s (MT5 missing) - skipping", s)
            continue
        res = simulate_strategy_on_series(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=1000)
        logger.info("Backtest %s -> n=%d win=%.3f avg_r=%.3f", s, res["n"], res["win"], res["avg_r"])
    logger.info("Backtest complete")

def confirm_enable_live():
    # This function ensures typing the phrase enables live trading
    if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND_THE RISKS":
        return True
    got = input("To enable LIVE trading type exactly: I UNDERSTAND THE RISKS\nType now: ").strip()
    # mirror the env check too so external invocation works
    if got == "I UNDERSTAND_THE RISKS":
        os.environ["CONFIRM_AUTO"] = "I UNDERSTAND_THE_RISKS"
        return True
    return False

def setup_and_run(args):
    init_trade_db()
    # connect to MT5 if possible
    if MT5_AVAILABLE and MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
        ok = connect_mt5(login=int(MT5_LOGIN) if str(MT5_LOGIN).isdigit() else None, password=MT5_PASSWORD, server=MT5_SERVER)
        if ok:
            logger.info("MT5 connected; preferring MT5 feed/execution")
    else:
        logger.info("MT5 not available or credentials not provided - bot will not fetch data")
    if args.backtest:
        run_backtest()
        return
    # NOTE: DEMO_SIMULATION default changed to False above to go live by default.
    if args.live:
        if not confirm_enable_live():
            logger.info("Live not enabled")
            return
        global DEMO_SIMULATION, AUTO_EXECUTE
        DEMO_SIMULATION = False
        AUTO_EXECUTE = True
    if args.loop:
        main_loop(live=not DEMO_SIMULATION)
    else:
        run_cycle(live=not DEMO_SIMULATION)

# ---------------- --- Telegram helper (ADDED) ----------------
def send_telegram_message(text: str) -> bool:
    """ Safe, minimal Telegram notifier used when live trades occur. Returns True if send succeeded, False otherwise. """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("send_telegram_message: Telegram not configured (missing token or chat id)")
        return False
    if not FUNDAMENTAL_AVAILABLE:
        # requests not available
        logger.debug("send_telegram_message: requests library not available")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        resp = requests.post(url, data=payload, timeout=8)
        if resp.status_code == 200:
            return True
        else:
            logger.warning("send_telegram_message: non-200 %s %s", resp.status_code, resp.text[:200])
            return False
    except Exception:
        logger.exception("send_telegram_message failed")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--symbols", nargs="*", help="override symbols")
    args = parser.parse_args()
    if args.symbols:
        SYMBOLS = args.symbols
    # ensure thresholds are validated at startup in case env changed after module load
    validate_and_clamp_thresholds()
    setup_and_run(args)
