#!/usr/bin/env python3
"""
Notex5_fixed.py
Robust single-file trading bot:
- Prefer MT5 live feed & execution (auto map to broker symbols with 'm' suffix)
- Safe yfinance fallback (intraday restricted to last 60 days)
- Demo-by-default, requires explicit confirmation to go live
- Defensive data handling and logging
Save as Notex5_fixed.py and run inside your venv.
"""
from __future__ import annotations
import os
import sys
import time
import json
import logging
import sqlite3
import subprocess
import argparse
from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, List

# Core libs
try:
    import numpy as np
    import pandas as pd
except Exception as e:
    raise RuntimeError("Install required packages: pip install numpy pandas") from e

# Optional libs - MetaTrader5, yfinance, requests
try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_LIB = True
except Exception:
    MT5_LIB = False

try:
    import yfinance as yf
    YF_LIB = True
except Exception:
    YF_LIB = False

try:
    import requests
except Exception:
    requests = None

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Notex5_fixed")

# ---------------- Config ----------------
SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY"]
# If your broker uses suffix 'm' (Exness) we try to map
COMMON_BROKER_SUFFIXES = ["m", "M", "-m", ".m"]

# TIMEFRAMES mapping for user-friendly intervals
TIMEFRAMES = {"M30": "30m", "H1": "60m", "H4": "4h", "D": "1d"}

# Safety / behavior
DEMO_SIMULATION = True
AUTO_EXECUTE = False
CONFIRM_ENV = os.getenv("CONFIRM_AUTO", "")  # set to "I UNDERSTAND THE RISKS" to bypass prompt
if CONFIRM_ENV == "I UNDERSTAND THE RISKS":
    DEMO_SIMULATION = False
    AUTO_EXECUTE = True

# Execution / risk defaults
BASE_RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.01"))  # 1% default
RISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT
MIN_RISK_PER_TRADE_PCT = 0.002
MAX_RISK_PER_TRADE_PCT = 0.03

MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "30"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "STOP_TRADING.flag")

# Persistence
TRADE_DB = os.getenv("TRADE_LOG_DB", "trades.db")
DECISION_SLEEP = int(os.getenv("DECISION_SLEEP", "60"))

# MT5 config (set these env vars or set below)
MT5_PATH = os.getenv("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")

# Telegram (optional)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------------- Helpers ----------------
def safe_to_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        pass
    return df

def normalize_ohlcv(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Map incoming dataframe to open,high,low,close,volume and coerce numerics."""
    if df is None:
        return None
    # flatten multiindex columns
    cols = []
    for c in df.columns:
        cols.append("_".join(map(str, c)) if isinstance(c, tuple) else str(c))
    df.columns = cols
    # lower-case columns
    df = df.rename(columns={c: c.lower() for c in df.columns})
    # try find columns
    for required in ("open", "high", "low", "close", "volume"):
        if required not in df.columns:
            # find candidate containing the word
            cand = next((c for c in df.columns if required in c), None)
            if cand:
                df[required] = df[cand]
    # fallback: use adj close
    if "close" not in df.columns and "adj close" in df.columns:
        df["close"] = df["adj close"]
    # coerce numeric where possible
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            try:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            except Exception:
                # try elementwise float
                try:
                    df[c] = df[c].apply(lambda x: float(x) if x is not None else np.nan)
                except Exception:
                    df[c] = np.nan
        else:
            df[c] = np.nan
    # ensure datetime index
    df = safe_to_datetime_index(df)
    # drop rows where all OHLCV are NaN
    try:
        df = df.dropna(how="all", subset=["open", "high", "low", "close", "volume"])
    except Exception:
        pass
    return df

# ---------------- MT5 connection & symbol mapping ----------------
_mt5 = None
_mt5_connected = False

def try_start_mt5_terminal():
    """Attempt to start MT5 terminal (Windows) if path provided and not running."""
    if not MT5_PATH:
        return False
    try:
        # Only attempt if executable exists
        if os.path.exists(MT5_PATH):
            logger.info("Starting MT5 terminal from path: %s", MT5_PATH)
            subprocess.Popen([MT5_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2.5)
            return True
    except Exception:
        logger.exception("Starting MT5 terminal failed")
    return False

def connect_mt5() -> bool:
    """Initialize MetaTrader5 and attempt to login if credentials provided."""
    global _mt5, _mt5_connected
    if not MT5_LIB:
        logger.info("MetaTrader5 library not installed; skipping MT5 connect")
        _mt5_connected = False
        return False
    try:
        _mt5 = mt5
    except Exception:
        _mt5_connected = False
        return False
    # try initialize; if fails due to terminal not running, try to start it
    try:
        ok = _mt5.initialize()
        if not ok:
            # try start terminal then initialize again
            logger.info("MT5 initialize returned False - attempting to start terminal")
            try_start_mt5_terminal()
            time.sleep(2)
            ok = _mt5.initialize()
        if not ok:
            logger.warning("MT5 initialize failed: %s", _mt5.last_error() if hasattr(_mt5, "last_error") else "no last_error")
            _mt5_connected = False
            # try logging in if credentials provided (some setups require account login explicitly)
            if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
                try:
                    ok2 = _mt5.initialize(login=int(MT5_LOGIN), password=str(MT5_PASSWORD), server=str(MT5_SERVER))
                    if ok2:
                        _mt5_connected = True
                        logger.info("MT5 initialized via credentials")
                        return True
                except Exception:
                    logger.exception("MT5 initialize with credentials failed")
            return False
        _mt5_connected = True
        logger.info("MT5 initialized (terminal connected).")
        # if credentials provided, ensure logged in (optional)
        if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
            try:
                # log in (some versions require login via mt5.login)
                if hasattr(_mt5, "login"):
                    _mt5.login(int(MT5_LOGIN), password=str(MT5_PASSWORD), server=str(MT5_SERVER))
                    logger.info("MT5 login attempted using credentials")
            except Exception:
                logger.debug("MT5 login attempt failed or not required")
        return True
    except Exception:
        logger.exception("MT5 connect failed")
        _mt5_connected = False
        return False

def discover_mt5_symbols() -> List[str]:
    if not MT5_LIB or not _mt5_connected:
        return []
    try:
        syms = _mt5.symbols_get()
        return [s.name for s in syms] if syms else []
    except Exception:
        logger.debug("discover_mt5_symbols failed")
        return []

def map_symbol_to_broker(sym: str) -> str:
    """Map canonical symbol to broker symbol (try explicit, then suffix heuristics)."""
    if not MT5_LIB or not _mt5_connected:
        # if MT5 not connected, try a common suffix guess: add 'm' for Exness users
        return sym + "m" if not sym.endswith("m") else sym
    canonical = str(sym).upper()
    # explicit exact match
    try:
        syms = discover_mt5_symbols()
        low = canonical.lower()
        for s in syms:
            if s.lower() == low:
                return s
        # try adding common suffixes
        for suf in COMMON_BROKER_SUFFIXES:
            cand = canonical + suf
            for s in syms:
                if s.lower() == cand.lower():
                    return s
        # try contains or startswith heuristics
        for s in syms:
            sn = s.lower()
            if low in sn or sn.startswith(low) or sn.endswith(low):
                return s
    except Exception:
        logger.debug("map_symbol_to_broker heuristics failed")
    # fallback to adding 'm' suffix (common for Exness)
    return canonical + "m" if not canonical.endswith("m") else canonical

# ---------------- Data fetchers ----------------
def _yf_download_safe(ticker: str, interval: str, period_days: int):
    """Call yfinance.download but handle exceptions; return DataFrame or None."""
    try:
        if not YF_LIB:
            return None
        # For intraday intervals (less than '1d'), yfinance only supports up to last 60 days reliably.
        if interval.endswith("m") or interval in ("1h", "60m"):
            period_days = min(period_days, 60)
        df = yf.download(ticker, period=f"{period_days}d", interval=interval, progress=False)
        # yfinance sometimes returns a tuple or unexpected types; guard
        if df is None or isinstance(df, tuple) or getattr(df, "empty", True):
            return None
        return df
    except Exception as e:
        logger.debug("yfinance download failed for %s: %s", ticker, getattr(e, "args", e))
        return None

def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60) -> Optional[pd.DataFrame]:
    """
    Prefer MT5 feed when connected. Fallback to yfinance with conservative period for intraday.
    Returns normalized OHLCV DataFrame or None.
    """
    # Try MT5 first
    if MT5_LIB and _mt5_connected:
        try:
            broker_sym = map_symbol_to_broker(symbol)
            # ensure symbol exists and is selected
            si = _mt5.symbol_info(broker_sym)
            if si is None:
                logger.debug("MT5: symbol %s not found", broker_sym)
            else:
                if not si.visible:
                    try:
                        _mt5.symbol_select(broker_sym, True)
                    except Exception:
                        pass
                # Map interval to timeframe constant
                tf_map = {
                    "1m": _mt5.TIMEFRAME_M1,
                    "5m": _mt5.TIMEFRAME_M5,
                    "15m": _mt5.TIMEFRAME_M15,
                    "30m": _mt5.TIMEFRAME_M30,
                    "60m": _mt5.TIMEFRAME_H1,
                    "1h": _mt5.TIMEFRAME_H1,
                    "4h": _mt5.TIMEFRAME_H4,
                    "4H": _mt5.TIMEFRAME_H4,
                    "1d": _mt5.TIMEFRAME_D1,
                    "1D": _mt5.TIMEFRAME_D1,
                }
                mt_tf = tf_map.get(interval, _mt5.TIMEFRAME_H1)
                # estimate count
                count = 500
                try:
                    if interval.endswith("m"):
                        minutes = int(interval[:-1])
                        bars_per_day = max(1, int(24 * 60 / minutes))
                        count = max(120, int(period_days * bars_per_day))
                    elif interval in ("1h", "60m"):
                        count = max(120, int(period_days * 24))
                    elif interval in ("4h",):
                        count = max(120, int(period_days * 6))
                    elif interval in ("1d",):
                        count = max(60, int(period_days))
                except Exception:
                    count = 500
                rates = _mt5.copy_rates_from_pos(broker_sym, mt_tf, 0, int(count))
                if rates is None or len(rates) == 0:
                    logger.debug("MT5 returned no rates for %s (%s)", broker_sym, interval)
                else:
                    df = pd.DataFrame(rates)
                    if "time" in df.columns:
                        df.index = pd.to_datetime(df["time"], unit="s")
                    # unify columns
                    if "open" not in df.columns and "open_price" in df.columns:
                        df["open"] = df["open_price"]
                    if "tick_volume" in df.columns:
                        df["volume"] = df["tick_volume"]
                    elif "real_volume" in df.columns:
                        df["volume"] = df["real_volume"]
                    # coerce numeric
                    for col in ("open", "high", "low", "close", "volume"):
                        if col in df.columns:
                            try:
                                df[col] = pd.to_numeric(df[col], errors="coerce")
                            except Exception:
                                pass
                        else:
                            df[col] = pd.NA
                    df = df[["open", "high", "low", "close", "volume"]].dropna(how="all")
                    df = normalize_ohlcv(df)
                    logger.info("MT5 feed used for %s (%s) rows=%d broker=%s", symbol, interval, len(df), broker_sym)
                    return df
        except Exception:
            logger.exception("MT5 fetch error for %s", symbol)
    # Fallback: yfinance
    if YF_LIB:
        candidates = []
        s = str(symbol).upper().replace("/", "").replace("-", "")
        # mapping candidates used earlier in conversation
        mapping = {
            "XAGUSD": ["SI=F", "XAGUSD=X", "XAGUSD"],
            "XAUUSD": ["GC=F", "XAUUSD=X", "XAUUSD"],
            "BTCUSD": ["BTC-USD", "BTCUSD=X", "BTCUSD"],
            "EURUSD": ["EURUSD=X", "EURUSD", "EUR-USD"],
            "USDJPY": ["USDJPY=X", "USDJPY"],
        }
        candidates = mapping.get(s, []) + [f"{s}=X", s]
        # dedupe while preserving order
        seen = set(); cands = []
        for c in candidates:
            if c and c not in seen:
                cands.append(c); seen.add(c)
        last_exc = None
        for t in cands:
            df = _yf_download_safe(t, interval, period_days)
            if df is None:
                continue
            # normalize and return
            df = normalize_ohlcv(df)
            if df is None or getattr(df, "empty", True):
                continue
            logger.info("yfinance feed used for %s via %s (%s) rows=%d", symbol, t, interval, len(df))
            return df
    else:
        logger.debug("yfinance library not available for fallback")
    logger.warning("No data available for %s (%s)", symbol, interval)
    return None

def fetch_multi_timeframes(symbol: str, tfs: Dict[str, str] = TIMEFRAMES, period_days: int = 60) -> Dict[str, Optional[pd.DataFrame]]:
    out = {}
    for label, interval in tfs.items():
        # For H4 we can resample from 30m or 60m depending on available data
        if label == "H4":
            base = fetch_ohlcv(symbol, interval="60m", period_days=period_days)
            if base is None:
                out[label] = None
                continue
            try:
                df4 = base.resample("4H").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
                out[label] = normalize_ohlcv(df4)
            except Exception:
                try:
                    df4 = base.resample("4h").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
                    out[label] = normalize_ohlcv(df4)
                except Exception:
                    logger.info("Resample to H4 failed for %s", symbol)
                    out[label] = None
        else:
            out[label] = fetch_ohlcv(symbol, interval=interval, period_days=period_days)
        time.sleep(0.05)
    return out

# ---------------- Simple indicators + scoring ----------------
def add_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    # ensure numeric
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # simple rolling indicators
    df["sma5"] = df["close"].rolling(5, min_periods=1).mean()
    df["sma20"] = df["close"].rolling(20, min_periods=1).mean()
    delta = df["close"].diff()
    up = delta.clip(lower=0.0).rolling(14, min_periods=1).mean()
    down = -delta.clip(upper=0.0).rolling(14, min_periods=1).mean().replace(0, 1e-9)
    rs = up / down
    df["rsi14"] = 100 - (100 / (1 + rs))
    tr = pd.concat([df["high"] - df["low"], (df["high"] - df["close"].shift()).abs(), (df["low"] - df["close"].shift()).abs()], axis=1).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=1).mean()
    df = df.ffill().bfill().fillna(0.0)
    return df

def tech_signal_score(df: pd.DataFrame) -> float:
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

def aggregate_scores(tf_dfs: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, float]:
    techs = []
    for label, df in tf_dfs.items():
        try:
            if df is None or getattr(df, "empty", True):
                continue
            d = add_basic_indicators(df)
            t = tech_signal_score(d)
            # chosen weights: M30 & H1 prioritized equally
            weight = {"M30": 1.5, "H1": 1.5, "H4": 1.6, "D": 2.0}.get(label, 1.0)
            techs.append((t, weight))
        except Exception:
            logger.exception("aggregate_scores failed for %s", label)
    if not techs:
        return {"tech": 0.0, "fund": 0.0, "sent": 0.0}
    s = sum(t * w for t, w in techs)
    w = sum(w for _, w in techs)
    return {"tech": float(s / w), "fund": 0.0, "sent": 0.0}

# ---------------- Trade logging & simulated order placement ----------------
def init_trade_db():
    conn = sqlite3.connect(TRADE_DB, timeout=5)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            lots REAL,
            status TEXT,
            pnl REAL
        );
    """)
    conn.commit()
    conn.close()

def record_trade_db(symbol, side, entry, sl, tp, lots, status="sim", pnl=0.0):
    try:
        conn = sqlite3.connect(TRADE_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("INSERT INTO trades (ts,symbol,side,entry,sl,tp,lots,status,pnl) VALUES (?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), symbol, side, entry, sl, tp, lots, status, pnl))
        conn.commit(); conn.close()
    except Exception:
        logger.exception("record_trade_db failed")

def place_order_sim(symbol, side, lots, entry, sl, tp):
    record_trade_db(symbol, side, entry, sl, tp, lots, status="sim")
    logger.info("SIM ORDER %s %s lots=%.2f entry=%s sl=%s tp=%s", symbol, side, lots, entry, sl, tp)
    return {"status": "sim"}

def place_order_mt5(symbol, side, lots, price, sl, tp):
    if not MT5_LIB or not _mt5_connected:
        return {"status": "mt5_not_connected"}
    try:
        broker = map_symbol_to_broker(symbol)
        si = _mt5.symbol_info(broker)
        if si is None:
            return {"status": "symbol_not_found"}
        # ensure visible
        if not si.visible:
            try:
                _mt5.symbol_select(broker, True)
            except Exception:
                pass
        tick = _mt5.symbol_info_tick(broker)
        if tick is None:
            return {"status": "no_tick"}
        order_type = _mt5.ORDER_TYPE_BUY if side == "BUY" else _mt5.ORDER_TYPE_SELL
        price_to_use = price if price is not None else (tick.ask if side == "BUY" else tick.bid)
        request = {
            "action": _mt5.TRADE_ACTION_DEAL,
            "symbol": broker,
            "volume": float(lots),
            "type": order_type,
            "price": float(price_to_use),
            "sl": float(sl) if sl is not None else 0.0,
            "tp": float(tp) if tp is not None else 0.0,
            "deviation": 20,
            "magic": 123456,
            "comment": "Notex5_fixed",
            "type_time": _mt5.ORDER_TIME_GTC,
            "type_filling": _mt5.ORDER_FILLING_IOC,
        }
        res = _mt5.order_send(request)
        logger.info("MT5 order_send result: %s", res)
        record_trade_db(symbol, side, price_to_use, sl, tp, lots, status=str(res))
        return {"status": "sent", "result": str(res)}
    except Exception:
        logger.exception("place_order_mt5 failed")
        return {"status": "error"}

def compute_lots_from_risk(risk_pct, balance, entry_price, stop_price):
    try:
        if stop_price is None:
            return 0.01
        pip_risk = abs(entry_price - stop_price)
        if pip_risk <= 0:
            return 0.01
        risk_amount = balance * risk_pct
        # assume 100000 units per lot (standard for FX)
        lots = risk_amount / (pip_risk * 100000)
        lots = max(0.01, round(lots, 2))
        return lots
    except Exception:
        return 0.01

def get_today_trades_count():
    today = date.today().isoformat()
    try:
        conn = sqlite3.connect(TRADE_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades WHERE ts >= ?", (today+"T00:00:00+00:00",))
        r = cur.fetchone(); conn.close()
        return int(r[0]) if r else 0
    except Exception:
        return 0

# ---------------- Decision & runner ----------------
THRESHOLD = 0.20  # default final cutoff
def make_decision(symbol: str, live: bool=False):
    try:
        tfs = fetch_multi_timeframes(symbol, period_days=60)
        df_h1 = tfs.get("H1")
        df_m30 = tfs.get("M30")
        # require at least H1 or M30
        if (df_h1 is None or getattr(df_h1, "empty", True)) and (df_m30 is None or getattr(df_m30, "empty", True)):
            logger.info("No enough data for %s - skipping", symbol)
            return None
        scores = aggregate_scores(tfs)
        total = scores["tech"]
        candidate = None
        if total >= THRESHOLD:
            candidate = "BUY"
        elif total <= -THRESHOLD:
            candidate = "SELL"
        final = None
        if candidate is not None and abs(total) >= (THRESHOLD * 0.75):
            final = candidate
        logger.info("Decision %s -> agg=%.3f final=%s", symbol, total, final)
        decision = {"symbol": symbol, "agg": total, "final": final}
        if final:
            df_for_stop = df_h1 if df_h1 is not None and not getattr(df_h1, "empty", True) else df_m30
            dind = add_basic_indicators(df_for_stop)
            entry = float(dind["close"].iloc[-1])
            atr = float(dind["atr14"].iloc[-1] or 0.0)
            stop_dist = atr * 1.25 if atr > 0 else (entry * 0.01)
            if final == "BUY":
                sl = entry - stop_dist; tp = entry + stop_dist * 2.0
            else:
                sl = entry + stop_dist; tp = entry - stop_dist * 2.0
            balance = float(os.getenv("FALLBACK_BALANCE", "1000.0"))
            # simple regime scaling
            lots = compute_lots_from_risk(RISK_PER_TRADE_PCT, balance, entry, sl)
            # safety checks
            if os.path.exists(KILL_SWITCH_FILE):
                logger.info("Kill switch - skipping order")
                return decision
            if live and get_today_trades_count() >= MAX_DAILY_TRADES:
                logger.info("Daily trade cap reached - skipping live order")
                return decision
            if live and not DEMO_SIMULATION:
                res = place_order_mt5(symbol, final, lots, None, sl, tp)
            else:
                res = place_order_sim(symbol, final, lots, entry, sl, tp)
            decision.update({"entry": entry, "sl": sl, "tp": tp, "lots": lots, "order_res": res})
        return decision
    except Exception:
        logger.exception("make_decision failed for %s", symbol)
        return None

def run_one_cycle(live: bool=False):
    out = {}
    for s in SYMBOLS:
        out[s] = make_decision(s, live=live)
        time.sleep(0.2)
    return out

def main_loop(live: bool=False):
    logger.info("Starting main loop (live=%s demo=%s)", live, DEMO_SIMULATION)
    try:
        while True:
            run_one_cycle(live=live)
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")

# ---------------- CLI / startup ----------------
def confirm_live_enable() -> bool:
    if CONFIRM_ENV == "I UNDERSTAND THE RISKS":
        return True
    ans = input("To enable LIVE trading type exactly: I UNDERSTAND THE RISKS\nType now: ").strip()
    return ans == "I UNDERSTAND THE RISKS"

def setup_and_run(args):
    init_trade_db()
    # attempt MT5 connect (safe)
    if MT5_LIB:
        connected = connect_mt5()
        if not connected:
            logger.info("MT5 not connected - you can start terminal and run again with --live after it is running")
    else:
        logger.info("MT5 library not installed; install MetaTrader5 package if you want MT5 feed & execution.")
    if args.backtest:
        # simple backtest: simulate over last year for H1 when yfinance available / MT5 otherwise
        logger.info("Running simple backtest (H1) for symbols: %s", SYMBOLS)
        for s in SYMBOLS:
            df = fetch_ohlcv(s, interval="60m", period_days=365)
            if df is None:
                logger.info("No H1 data for %s - skipping backtest", s)
                continue
            stat = simulate_backtest_summary(df)
            logger.info("Backtest %s -> bars=%d", s, len(df))
        return
    if args.live:
        ok = confirm_live_enable()
        if not ok:
            logger.info("Live not enabled - exiting")
            return
        # require MT5 connected for live
        if not MT5_LIB or not _mt5_connected:
            logger.info("MT5 must be connected for live orders. Exiting.")
            return
        global DEMO_SIMULATION, AUTO_EXECUTE
        DEMO_SIMULATION = False
        AUTO_EXECUTE = True
    if args.loop:
        main_loop(live=not DEMO_SIMULATION)
    else:
        run_one_cycle(live=not DEMO_SIMULATION)

# small backtest helper used in --backtest mode
def simulate_backtest_summary(df):
    # naive walk through like prior simulate_strategy_on_series but short
    df = normalize_ohlcv(df)
    if df is None or df.empty:
        return {}
    df = add_basic_indicators(df)
    wins = 0
    trades = 0
    for i in range(30, len(df) - 10):
        window = df.iloc[: i + 1]
        sc = tech_signal_score(window)
        if sc >= THRESHOLD or sc <= -THRESHOLD:
            trades += 1
            # naive outcome: compare next bar close vs stop
            entry = float(df["close"].iloc[i])
            atr = float(df["atr14"].iloc[i] or 0.0)
            stop = atr * 1.25 if atr > 0 else entry * 0.01
            # scan few bars for result
            r = 0
            for j in range(i + 1, min(i + 30, len(df))):
                if sc >= THRESHOLD:
                    if float(df["high"].iloc[j]) >= (entry + 2 * stop):
                        r = 1; break
                    if float(df["low"].iloc[j]) <= (entry - stop):
                        r = -1; break
                else:
                    if float(df["low"].iloc[j]) <= (entry - 2 * stop):
                        r = 1; break
                    if float(df["high"].iloc[j]) >= (entry + stop):
                        r = -1; break
            if r > 0:
                wins += 1
    return {"trades": trades, "wins": wins, "winrate": (wins / trades if trades else 0.0)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Notex5_fixed - MT5 preferred bot")
    parser.add_argument("--loop", action="store_true", help="Run continuous loop")
    parser.add_argument("--backtest", action="store_true", help="Run quick backtest then exit")
    parser.add_argument("--live", action="store_true", help="Attempt to enable live trading (MT5 required)")
    args = parser.parse_args()
    setup_and_run(args)
