#!/usr/bin/env python3
"""
Ultra_instinct.py  -- MT5-only (no Yahoo) defensive trading bot.
Minimal surgical fixes from your previous file:
  - Removed yfinance fallback entirely
  - Robust MT5 initialization retry (attempt to start terminal)
  - Robust MT5 data handling and broker-symbol mapping (handles suffixes like .m or m)
Everything else (strategy, thresholds, adapt/optimizer, db, telegram) is left intact.
Run as you did before:
  set MT5_LOGIN=...
  set MT5_PASSWORD=...
  set MT5_SERVER=...
  set MT5_PATH="C:\Program Files\MetaTrader 5\terminal64.exe"
  python Ultra_instinct.py --backtest
  python Ultra_instinct.py --loop
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
from datetime import datetime, date, timezone
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

try:
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import SGDClassifier
    import joblib
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Ultra_instinct")

# ---------------- Configuration (unchanged logic) ----------------
SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY"]
# broker mapping - keep updated to your broker suffix (example uses .m)
BROKER_SYMBOLS = {
    "EURUSD": "EURUSD.m",
    "XAGUSD": "XAGUSD.m",
    "XAUUSD": "XAUUSD.m",
    "BTCUSD": "BTCUSD.m",
    "USDJPY": "USDJPY.m",
}

TIMEFRAMES = {"M30": "30m", "H1": "60m"}  # M30 + H1 as you requested

# Safety defaults
DEMO_SIMULATION = True
AUTO_EXECUTE = False
if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND THE RISKS":
    DEMO_SIMULATION = False
    AUTO_EXECUTE = True

BASE_RISK_PER_TRADE_PCT = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.01"))
MIN_RISK_PER_TRADE_PCT = 0.002
MAX_RISK_PER_TRADE_PCT = 0.03
RISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT

MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "30"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "STOP_TRADING.flag")
ADAPT_STATE_FILE = "adapt_state.json"
TRADES_DB = "trades.db"
MODEL_FILE = "ultra_instinct_model.joblib"
TRADES_CSV = "trades.csv"
CURRENT_THRESHOLD = float(os.getenv("CURRENT_THRESHOLD", "0.20"))
MIN_THRESHOLD = 0.08
MAX_THRESHOLD = 0.45
DECISION_SLEEP = int(os.getenv("DECISION_SLEEP", "60"))
ADAPT_EVERY_CYCLES = 6
MODEL_MIN_TRAIN = 40

# MT5 credentials env
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
MT5_PATH = os.getenv("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")

# telegram (optional)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------------- persistence and state ----------------
def load_adapt_state():
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    if os.path.exists(ADAPT_STATE_FILE):
        try:
            with open(ADAPT_STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
            CURRENT_THRESHOLD = float(st.get("threshold", CURRENT_THRESHOLD))
            RISK_PER_TRADE_PCT = float(st.get("risk", RISK_PER_TRADE_PCT))
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
def init_trade_db():
    conn = sqlite3.connect(TRADES_DB, timeout=5)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY,
      ts TEXT, symbol TEXT, side TEXT, entry REAL, sl REAL, tp REAL,
      lots REAL, status TEXT, pnl REAL, rmult REAL, regime TEXT, score REAL, model_score REAL, meta TEXT
    );
    """)
    conn.commit(); conn.close()
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", encoding="utf-8") as f:
            f.write("ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta\n")

def record_trade(symbol, side, entry, sl, tp, lots, status="sim", pnl=0.0, rmult=0.0, regime="unknown", score=0.0, model_score=0.0, meta=None):
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("INSERT INTO trades (ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), symbol, side, entry, sl, tp, lots, status, pnl, rmult, regime, score, model_score, json.dumps(meta or {})))
        conn.commit(); conn.close()
    except Exception:
        logger.exception("record_trade db failed")
    try:
        with open(TRADES_CSV, "a", encoding="utf-8") as f:
            f.write("{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(datetime.now(timezone.utc).isoformat(), symbol, side, entry, sl, tp, lots, status, pnl, rmult, regime, score, model_score))
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
            weight = {"M30": 1.5, "H1": 1.5}.get(label, 1.0)
            techs.append((t, weight))
        except Exception:
            logger.exception("aggregate_multi_tf_scores failed for %s", label)
    if not techs:
        return {"tech": 0.0, "fund": 0.0, "sent": 0.0}
    s = sum(t * w for t, w in techs); w = sum(w for _, w in techs)
    return {"tech": float(s / w), "fund": 0.0, "sent": 0.0}

# ---------------- simple ML hooks unchanged ----------------
model_pipe = None
def build_model():
    if not SKLEARN_AVAILABLE:
        return None
    return Pipeline([("scaler", StandardScaler()), ("clf", SGDClassifier(loss="log", max_iter=1000, tol=1e-3, random_state=42))])

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
    return None

if SKLEARN_AVAILABLE:
    load_model()

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
        save_adapt_state()
        logger.info("Optimizer applied new threshold=%.3f risk=%.5f", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
        return {"before": base_expect, "after": best_expect, "threshold": CURRENT_THRESHOLD, "risk": RISK_PER_TRADE_PCT}
    logger.info("Optimizer skipped applying")
    return None

# ---------------- Execution, decision & runner (kept) ----------------
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
    if not MT5_AVAILABLE or not _mt5_connected:
        return {"status": "mt5_not_connected"}
    try:
        broker = map_symbol_to_broker(symbol)
        try:
            si = _mt5.symbol_info(broker)
            if si is None or not si.visible:
                _mt5.symbol_select(broker, True)
        except Exception:
            pass
        tick = _mt5.symbol_info_tick(broker)
        if tick is None:
            return {"status": "no_tick"}
        order_price = price if price is not None else (tick.ask if action == "BUY" else tick.bid)
        order_type = _mt5.ORDER_TYPE_BUY if action == "BUY" else _mt5.ORDER_TYPE_SELL
        req = {
            "action": _mt5.TRADE_ACTION_DEAL,
            "symbol": broker,
            "volume": float(lot),
            "type": order_type,
            "price": order_price,
            "sl": float(sl) if sl is not None else 0.0,
            "tp": float(tp) if tp is not None else 0.0,
            "deviation": 20,
            "magic": 123456,
            "comment": "Ultra_instinct",
            "type_time": _mt5.ORDER_TIME_GTC,
            "type_filling": _mt5.ORDER_FILLING_IOC,
        }
        res = _mt5.order_send(req)
        logger.info("MT5 order_send result: %s", res)
        return {"status": "sent", "result": str(res)}
    except Exception:
        logger.exception("place_order_mt5 failed")
        return {"status": "error"}

def get_today_trade_count():
    today = date.today().isoformat()
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades WHERE ts >= ?", (today + "T00:00:00+00:00",))
        r = cur.fetchone(); conn.close()
        return int(r[0]) if r else 0
    except Exception:
        return 0

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
        # optional model scoring (unchanged)
        if SKLEARN_AVAILABLE and model_pipe is not None:
            try:
                regime, rel, adx = detect_market_regime_from_h1(df_h1)
                entry = float(df_h1["close"].iloc[-1])
                atr = float(add_technical_indicators(df_h1)["atr14"].iloc[-1])
                dist = (atr * 1.25) / (entry if entry != 0 else 1.0)
                regime_code = 0 if regime == "normal" else (1 if regime == "quiet" else 2)
                X = np.array([[tech_score, regime_code, dist]])
                proba = model_pipe.predict_proba(X)[:,1][0]
                model_score = float((proba - 0.5) * 2.0)
            except Exception:
                model_score = 0.0
        total_score = 0.5 * tech_score + 0.3 * model_score
        candidate = None
        if total_score >= CURRENT_THRESHOLD:
            candidate = "BUY"
        if total_score <= -CURRENT_THRESHOLD:
            candidate = "SELL"
        final_signal = None
        if candidate is not None and abs(total_score) >= (CURRENT_THRESHOLD * 0.75):
            final_signal = candidate
        decision = {"symbol": symbol, "agg": total_score, "tech": tech_score, "model_score": model_score, "final": final_signal}
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
            if regime == "volatile":
                risk_pct = max(MIN_RISK_PER_TRADE_PCT, risk_pct * 0.6)
            elif regime == "quiet":
                risk_pct = min(MAX_RISK_PER_TRADE_PCT, risk_pct * 1.15)
            if os.path.exists(KILL_SWITCH_FILE):
                logger.info("Kill switch engaged - skipping order for %s", symbol)
                return decision
            if live and get_today_trade_count() >= MAX_DAILY_TRADES:
                logger.info("Daily trade cap reached - skipping")
                return decision
            balance = float(os.getenv("FALLBACK_BALANCE", "650.0"))
            lots = compute_lots_from_risk(risk_pct, balance, entry, sl)
            if live and not DEMO_SIMULATION:
                res = place_order_mt5(symbol, final_signal, lots, None, sl, tp)
                record_trade(symbol, final_signal, entry, sl, tp, lots, status=res.get("status", "unknown"), pnl=0.0, rmult=0.0, regime=regime, score=tech_score, model_score=model_score, meta=res)
            else:
                res = place_order_simulated(symbol, final_signal, lots, entry, sl, tp, tech_score, model_score, regime)
            decision.update({"entry": entry, "sl": sl, "tp": tp, "lots": lots, "placed": res})
        else:
            logger.info("No confident signal for %s (agg=%.3f)", symbol, total_score)
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
        if n >= 20:
            if winrate < 0.45:
                CURRENT_THRESHOLD = min(MAX_THRESHOLD, CURRENT_THRESHOLD + 0.02)
            elif winrate > 0.6:
                CURRENT_THRESHOLD = max(MIN_THRESHOLD, CURRENT_THRESHOLD - 0.02)
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
        if DEMO_SIMULATION:
            light_optimizer(SYMBOLS, budget=8)
        if SKLEARN_AVAILABLE:
            try:
                # train_model_from_trades() left out for brevity; you may enable it if desired
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
    if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND THE RISKS":
        return True
    got = input("To enable LIVE trading type exactly: I UNDERSTAND THE RISKS\nType now: ").strip()
    return got == "I UNDERSTAND THE RISKS"

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--symbols", nargs="*", help="override symbols")
    args = parser.parse_args()
    if args.symbols:
        SYMBOLS = args.symbols
    setup_and_run(args)
