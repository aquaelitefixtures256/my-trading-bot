#!/usr/bin/env python3
"""
Notex5.py - defensive, single-file main bot.
Overwrite your existing Notex5.py with this code.
Run inside your venv:
  venv\Scripts\activate
  python Notex5.py
"""
from __future__ import annotations
import os
import sys
import time
import json
import logging
import sqlite3
import threading
import requests
from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Notex5")

# ---------------- Safety / config ----------------
DEMO_SIMULATION = True
AUTO_EXECUTE = False
if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND THE RISKS":
    DEMO_SIMULATION = False
    AUTO_EXECUTE = True

SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY"]
TIMEFRAMES = {"H1": "60m", "H4": "4H", "D": "1d"}

RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "2"))
TRADE_LOG_DB = os.getenv("TRADE_LOG_DB", "trades.db")
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "STOP_TRADING.flag")
DECISION_SLEEP = int(os.getenv("DECISION_SLEEP", "60"))

MT5_PATH = os.getenv("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MODEL_API_URL = os.getenv("MODEL_API_URL")

# ---------------- features import (tech indicators) ----------------
try:
    from features.tech_features import add_technical_indicators, technical_signal_score  # type: ignore
    logger.info("Imported features.tech_features")
except Exception as e:
    logger.warning("Could not import features.tech_features (%s). Using small internal fallbacks.", e)

    def add_technical_indicators(df):
        try:
            import pandas as pd
            df = df.copy()
            if "close" in df.columns:
                df["sma5"] = df["close"].rolling(5, min_periods=1).mean()
                df["sma20"] = df["close"].rolling(20, min_periods=1).mean()
                delta = df["close"].diff()
                up = delta.clip(lower=0.0).rolling(window=14, min_periods=1).mean()
                down = -delta.clip(upper=0.0).rolling(window=14, min_periods=1).mean().replace(0,1e-9)
                rs = up / down
                df["rsi14"] = 100 - (100/(1+rs))
            return df
        except Exception:
            return df

    def technical_signal_score(df):
        try:
            if df is None or len(df) < 2:
                return 0.0
            latest = df.iloc[-1]; prev = df.iloc[-2]
            score = 0.0
            if prev.get("sma5",0) <= prev.get("sma20",0) and latest.get("sma5",0) > latest.get("sma20",0):
                score += 0.6
            r = float(latest.get("rsi14",50) or 50)
            if r < 30: score += 0.2
            if r > 70: score -= 0.2
            return max(-1.0, min(1.0, score))
        except Exception:
            return 0.0

# ---------------- Utility: yfinance candidates ----------------
def symbol_to_yfinance_candidates(sym: str) -> List[str]:
    s = str(sym).upper().replace("/", "").replace("-", "").strip()
    mapping = {
        "XAGUSD": ["SI=F","XAGUSD=X","XAGUSD"],
        "XAUUSD": ["GC=F","XAUUSD=X","XAUUSD"],
        "BTCUSD": ["BTC-USD","BTCUSD=X","BTCUSD"],
        "EURUSD": ["EURUSD=X","EURUSD","EUR-USD"],
        "USDJPY": ["USDJPY=X","USDJPY"],
    }
    candidates = mapping.get(s, []) + [f"{s}=X", s]
    if s.endswith("USD"):
        candidates.append(s.replace("USD","-USD"))
    # dedupe preserving order
    out=[]; seen=set()
    for c in candidates:
        if c and c not in seen:
            out.append(c); seen.add(c)
    return out

# ---------------- Helpers to normalize data ----------------
def _normalize_df_columns(df):
    """Turn column names into simple lowercase strings (handles MultiIndex and tuples)."""
    try:
        if df is None:
            return None
        # flatten MultiIndex or tuple names
        cols = []
        for c in df.columns:
            if isinstance(c, tuple):
                cols.append("_".join(map(str, c)))
            else:
                cols.append(str(c))
        df.columns = cols
        # lower-case mapping
        df.rename(columns={c: str(c).lower() for c in df.columns}, inplace=True)
        return df
    except Exception:
        try:
            df.columns = [str(c).lower() for c in df.columns]
        except Exception:
            pass
        return df

def _map_to_ohlcv(df):
    """Map a DataFrame to columns open/high/low/close/volume and coerce numeric values."""
    import pandas as pd
    import numpy as np
    if df is None:
        return None
    df = _normalize_df_columns(df)
    if df is None:
        return None
    # try to find/match columns
    for req in ("open","high","low","close","volume"):
        if req not in df.columns:
            candidates = [c for c in df.columns if req in str(c)]
            if candidates:
                df[req] = df[candidates[0]]
    # fallback: use 'adj close' if close missing
    if "close" not in df.columns and "adj close" in df.columns:
        df["close"] = df["adj close"]
    # flatten nested values if object dtype
    for col in ("open","high","low","close","volume"):
        if col in df.columns and df[col].dtype == object:
            try:
                df[col] = df[col].apply(lambda x: (x[-1] if (hasattr(x,"__len__") and not isinstance(x,(str,bytes))) else x) if x is not None else None)
            except Exception:
                pass
    # coerce numeric
    for col in ("open","high","low","close","volume"):
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception:
                try:
                    df[col] = df[col].apply(lambda x: float(x) if (x is not None and str(x) != "nan") else None)
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception:
                    df[col] = np.nan
        else:
            df[col] = np.nan
    # ensure datetime index
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        pass
    # drop rows where all OHLCV are NaN
    try:
        df = df.dropna(how="all", subset=["open","high","low","close","volume"])
    except Exception:
        pass
    return df

# ----- compatibility wrapper (fixes NameError calls to _normalize_ohlcv) -----
def _normalize_ohlcv(df):
    """
    Compatibility wrapper: some code calls _normalize_ohlcv but the
    implementation here is _map_to_ohlcv — use that.
    Returns a normalized OHLCV DataFrame or None.
    """
    try:
        if "_map_to_ohlcv" in globals():
            return _map_to_ohlcv(df)
    except Exception:
        return None
    return None
# ---------------------------------------------------------------------------

# ------------------ fetchers (yfinance) ------------------
def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60):
    try:
        import yfinance as yf
    except Exception as e:
        logger.error("yfinance import failed: %s", e)
        return None
    candidates = symbol_to_yfinance_candidates(symbol)
    last_exc = None
    for t in candidates:
        try:
            logger.info("Trying ticker '%s' for symbol '%s' (interval=%s)", t, symbol, interval)
            df = yf.download(t, period=f"{period_days}d", interval=interval, progress=False)
            if df is None or df.empty:
                logger.debug("Ticker %s returned no data", t)
                continue
            # If yfinance returned a tuple or unexpected type skip
            if isinstance(df, tuple):
                last_exc = TypeError("yfinance returned tuple")
                continue
            df = _map_to_ohlcv(df)
            if df is None or df.empty:
                logger.debug("Ticker %s returned data but mapping to OHLCV failed", t)
                continue
            logger.info("Fetched %d rows for %s using %s", len(df), symbol, t)
            return df
        except Exception as e:
            last_exc = e
            logger.debug("yfinance attempt for %s failed: %s", t, getattr(e,"args",e))
            continue
    logger.warning("All candidates failed for %s. Last err: %s", symbol, getattr(last_exc,"args",last_exc))
    return None

def fetch_multi_timeframes(symbol: str, tfs=TIMEFRAMES, period_days=60):
    """
    Defensive fetch for requested timeframes.
    H4 is created by resampling H1 (60m).
    """
    out = {}
    for label, interval in tfs.items():
        if label == "H4":
            base = fetch_ohlcv(symbol, interval="60m", period_days=period_days)
            if base is None or getattr(base,"empty",True):
                out[label] = None
                continue
            # use the compatibility wrapper to normalize
            base_norm = _normalize_ohlcv(base)
            if base_norm is None:
                logger.info("Resample skipped for %s (H4): normalization failed", symbol)
                out[label] = None
                continue
            # try uppercase then lowercase
            try:
                df4 = base_norm.resample("4H").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
            except Exception:
                try:
                    df4 = base_norm.resample("4h").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
                except Exception as e:
                    logger.info("Resampling to 4H failed for %s: %s", symbol, e)
                    out[label] = None
                    continue
            out[label] = df4
        else:
            df = fetch_ohlcv(symbol, interval=interval, period_days=period_days)
            out[label] = _normalize_ohlcv(df) if df is not None else None
    return out

# ---------------- SQLite trade DB ----------------
def init_trade_db():
    conn = sqlite3.connect(TRADE_LOG_DB, timeout=5)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        symbol TEXT,
        side TEXT,
        lots REAL,
        entry REAL,
        sl REAL,
        tp REAL,
        status TEXT,
        order_meta TEXT
    );
    """)
    conn.commit(); conn.close()

def record_trade_db(symbol, side, lots, entry, sl, tp, status, order_meta=""):
    try:
        conn = sqlite3.connect(TRADE_LOG_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("INSERT INTO trades (timestamp,symbol,side,lots,entry,sl,tp,status,order_meta) VALUES (?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), symbol, side, lots, entry, sl, tp, status, json.dumps(order_meta)))
        conn.commit(); conn.close()
    except Exception:
        logger.exception("Failed to write trade to DB")

# ---------------- Telegram alerts ----------------
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured. Skipping message.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        chat_val = TELEGRAM_CHAT_ID
        try:
            chat_val = int(TELEGRAM_CHAT_ID) if str(TELEGRAM_CHAT_ID).isdigit() else TELEGRAM_CHAT_ID
        except Exception:
            pass
        payload = {"chat_id": chat_val, "text": message}
        r = requests.post(url, json=payload, timeout=6)
        if r.status_code != 200:
            logger.debug("Telegram returned %s: %s", r.status_code, r.text)
        return r.status_code == 200
    except Exception:
        logger.exception("Telegram send failed")
        return False

# ----------------- Order placement (demo) -----------------
def place_order(symbol: str, side: str, lots: float, entry_price: float, sl: float, tp: Optional[float] = None):
    logger.info("PLACE ORDER REQUEST %s %s lots=%.2f entry=%s sl=%s tp=%s", symbol, side, lots, entry_price, sl, tp)
    allowed = True
    if not allowed:
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="rejected", order_meta="blocked")
        return {"status":"rejected"}
    if DEMO_SIMULATION:
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="demo", order_meta={"note":"demo"})
        send_telegram(f"Notex5 ALERT (demo)\n{symbol} {side}\nEntry: {entry_price}\nSL: {sl}\nTP: {tp}\nLots: {lots}")
        return {"status":"demo"}
    # live path (not enabled here)
    return {"status":"live_not_enabled"}

# ---------------- Strategy helpers ----------------
def compute_atr_sl(entry_price: float, df, multiplier: float = 1.25) -> float:
    try:
        if df is None or getattr(df,"empty",True):
            return max(0.00001, abs(entry_price)*0.01)
        if "atr14" in df.columns:
            return float(df["atr14"].iloc[-1])
        # fallback ATR
        import pandas as pd
        tr = pd.concat([df["high"] - df["low"], (df["high"] - df["close"].shift()).abs(), (df["low"] - df["close"].shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14, min_periods=1).mean().iloc[-1]
        return max(0.00001, float(atr) * multiplier)
    except Exception:
        return max(0.00001, abs(entry_price)*0.01)

def compute_lots_from_risk(risk_pct: float, balance: float, entry_price: float, stop_price: float) -> float:
    try:
        risk_amount = balance * risk_pct
        pip_risk = abs(entry_price - stop_price)
        if pip_risk <= 0:
            return 0.01
        lots = risk_amount / (pip_risk * 100000)
        return round(max(0.01, lots), 2)
    except Exception:
        return 0.01

# ---------------- Core strategy ----------------
def aggregate_multi_tf_scores(tf_dfs: Dict[str,Any]) -> Dict[str,float]:
    techs=[]
    for label, df in tf_dfs.items():
        try:
            if df is None or getattr(df,"empty",True):
                continue
            dfind = add_technical_indicators(df)
            tscore = technical_signal_score(dfind)
            weight = {"H1":1.0,"H4":1.6,"D":2.0}.get(label,1.0)
            techs.append((tscore, weight))
        except Exception:
            logger.exception("Failed to compute technicals for %s", label)
            continue
    tech_agg = 0.0
    if techs:
        s = sum(t*w for t,w in techs); w = sum(w for _,w in techs); tech_agg = float(s/w)
    return {"tech": tech_agg, "fund": 0.0, "sent": 0.0}

def make_decision_for_symbol(symbol: str):
    try:
        tf_dfs = fetch_multi_timeframes(symbol)
        df_h1 = tf_dfs.get("H1")
        if df_h1 is None or getattr(df_h1,"empty",True) or len(df_h1) < 30:
            logger.info("Not enough H1 data for %s - skipping", symbol)
            return None
        scores = aggregate_multi_tf_scores(tf_dfs)
        model_score = 0.0
        total_score = 0.4*scores["tech"] + 0.15*scores["fund"] + 0.15*scores["sent"] + 0.3*model_score
        candidate = None
        if total_score >= 0.35: candidate = "BUY"
        if total_score <= -0.35: candidate = "SELL"
        final_signal = None
        if candidate is not None and abs(total_score) >= 0.55:
            final_signal = candidate
        decision = {"symbol":symbol,"scores":scores,"model_score":model_score,"agg":total_score,"final_signal":final_signal}
        if final_signal:
            entry = float(df_h1["close"].iloc[-1])
            stop_dist = compute_atr_sl(entry, add_technical_indicators(df_h1), multiplier=1.25)
            if final_signal == "BUY":
                sl = entry - stop_dist; tp = entry + stop_dist*2.0
            else:
                sl = entry + stop_dist; tp = entry - stop_dist*2.0
            balance = 1000.0
            lots = compute_lots_from_risk(RISK_PER_TRADE_PCT, balance, entry, sl)
            order_res = place_order(symbol, final_signal, lots, entry, sl, tp)
            decision.update({"entry":entry,"sl":sl,"tp":tp,"lots":lots,"order_result":order_res})
        else:
            logger.info("No confident signal for %s (agg=%.3f)", symbol, total_score)
        logger.info("Decision for %s final=%s agg=%.3f tech=%.3f", symbol, decision.get("final_signal"), total_score, scores["tech"])
        return decision
    except Exception:
        logger.exception("make_decision_for_symbol failed for %s", symbol)
        return None

# ---------------- Runner ----------------
def run_one_cycle():
    res={}
    for s in SYMBOLS:
        res[s] = make_decision_for_symbol(s)
        time.sleep(0.2)
    return res

def main_loop():
    logger.info("Starting loop (DEMO=%s)", DEMO_SIMULATION)
    try:
        while True:
            run_one_cycle()
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")

# ---------------- Startup ----------------
if __name__ == "__main__":
    init_trade_db()
    logger.info("Prefetching symbol data once before first decision cycle")
    for s in SYMBOLS:
        try:
            _ = fetch_multi_timeframes(s, period_days=60)
        except Exception:
            logger.exception("Prefetch failed for %s", s)
        time.sleep(0.2)
    logger.info("Running single decision cycle for symbols: %s", SYMBOLS)
    run_one_cycle()
    ans = input("Run continuous loop? (yes to run, anything else to exit): ").strip().lower()
    if ans == "yes":
        main_loop()
    else:
        logger.info("Exiting after single cycle.")
