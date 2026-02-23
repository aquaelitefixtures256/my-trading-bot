#!/usr/bin/env python3
# Notex5.py - MT5-preferred full trading bot with auto-broker-symbol mapping
from __future__ import annotations
import os
import sys
import time
import json
import logging
import sqlite3
import threading
import requests
import subprocess
from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Notex5")

# ---------------- Safety / config ----------------
DEMO_SIMULATION = True
AUTO_EXECUTE = False
REQUIRE_MANUAL_LIVE_CONFIRM = False
if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND THE RISKS":
    DEMO_SIMULATION = False
    AUTO_EXECUTE = True
    REQUIRE_MANUAL_LIVE_CONFIRM = False

SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY"]
TIMEFRAMES = {"M30": "30m", "H1": "60m", "H4": "4H"}

RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.0017"))
MAX_TOTAL_OPEN_TRADES = int(os.getenv("MAX_TOTAL_OPEN_TRADES", "15"))
MAX_OPEN_TRADES_PER_SYMBOL = int(os.getenv("MAX_OPEN_TRADES_PER_SYMBOL", "5"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "30"))

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
    logger.warning("Could not import features.tech_features (%s). Using minimal fallbacks.", e)
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

# ---------------- MT5 globals ----------------
_mt5 = None
_mt5_connected = False

# ---------------- MT5 connect / symbol helpers ----------------
def connect_mt5(login: Optional[int]=None, password: Optional[str]=None, server: Optional[str]=None) -> bool:
    """Initialize MetaTrader5 library and connect (if credentials provided)."""
    global _mt5, _mt5_connected, MT5_LOGIN, MT5_PASSWORD, MT5_SERVER
    try:
        import MetaTrader5 as mt5  # type: ignore
        _mt5 = mt5
    except Exception as e:
        logger.error("MetaTrader5 import failed: %s", e)
        _mt5 = None
        _mt5_connected = False
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
            logger.error("MT5 initialize failed: %s", _mt5.last_error())
            _mt5_connected = False
            return False
        _mt5_connected = True
        logger.info("MT5 initialized (login=%s server=%s)", login, server)
        return True
    except Exception as e:
        logger.exception("MT5 connect error: %s", e)
        _mt5_connected = False
        return False

def shutdown_mt5():
    global _mt5, _mt5_connected
    try:
        if _mt5 is not None:
            _mt5.shutdown()
    except Exception:
        pass
    _mt5_connected = False

def discover_broker_symbols() -> List[str]:
    """Return the list of symbol names available in MT5 market watch (if connected)."""
    try:
        if _mt5_connected and _mt5 is not None:
            syms = _mt5.symbols_get()
            return [s.name for s in syms] if syms else []
    except Exception:
        logger.debug("discover_broker_symbols failed")
    return []

def map_symbol_to_broker(requested: str) -> str:
    """
    Heuristic mapping from requested symbol (e.g. 'XAUUSD') to broker symbol (e.g. 'XAUUSDm').
    If MT5 is connected, try to find exact match, suffix variants, case-insensitive containment.
    If not found, return the original requested string (so yfinance fallback can be used).
    """
    requested = str(requested).strip()
    if not _mt5_connected or _mt5 is None:
        return requested
    try:
        brokers = discover_broker_symbols()
        # quick exact matches (case-insensitive)
        low_req = requested.lower()
        for b in brokers:
            if b.lower() == low_req:
                return b
        # try adding common suffixes/prefixes (m, pro, -m, .m)
        variants = [requested, requested + "m", requested + "M", requested + "-m", requested + ".m", requested + "pro"]
        for v in variants:
            for b in brokers:
                if b.lower() == v.lower():
                    return b
        # contains or startswith heuristics
        for b in brokers:
            bn = b.lower()
            if low_req in bn or bn.startswith(low_req) or bn.endswith(low_req):
                return b
        # try reverse: broker contains requested with symbols removed
        for b in brokers:
            bn = b.lower().replace("-", "").replace("_", "")
            if low_req in bn or bn.startswith(low_req):
                return b
    except Exception:
        logger.debug("map_symbol_to_broker error", exc_info=True)
    return requested

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
    out=[]; seen=set()
    for c in candidates:
        if c and c not in seen:
            out.append(c); seen.add(c)
    return out

# ---------------- Data fetcher - Prefer MT5, fallback to yfinance ----------------
def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60):
    """
    Prefer MT5 feed if connected and symbol exists there. Otherwise use yfinance fallback.
    interval examples: "1m","5m","60m","1d"
    """
    # --- Attempt MT5 ---
    try:
        if _mt5_connected and _mt5 is not None:
            import pandas as pd
            broker_sym = map_symbol_to_broker(symbol)
            # ensure symbol visible / selected
            try:
                si = _mt5.symbol_info(broker_sym)
                if si is None:
                    raise Exception("symbol not on broker")
                if not si.visible:
                    _mt5.symbol_select(broker_sym, True)
            except Exception:
                raise
            # map interval to MT5 timeframe constant
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
            # rough count estimate based on timeframe
            count = 500
            try:
                if interval.endswith("m"):
                    minutes = int(interval[:-1])
                    bars_per_day = max(1, int(24*60 / minutes))
                    count = max(120, period_days * bars_per_day)
                elif interval in ("1h","60m"):
                    count = max(120, period_days * 24)
                elif interval in ("4h","4H"):
                    count = max(120, int(period_days * 6))
                elif interval in ("1d","1D"):
                    count = max(60, period_days)
            except Exception:
                count = 500
            rates = _mt5.copy_rates_from_pos(broker_sym, mt_tf, 0, int(count))
            if rates is None:
                raise Exception("mt5 returned no rates")
            df = pd.DataFrame(rates)
            if "time" in df.columns:
                df.index = pd.to_datetime(df["time"], unit="s")
            # unify columns
            if "open" not in df.columns and "open_price" in df.columns:
                df["open"] = df["open_price"]
            # volume: try tick_volume or real_volume
            if "tick_volume" in df.columns:
                df["volume"] = df["tick_volume"]
            elif "real_volume" in df.columns:
                df["volume"] = df["real_volume"]
            # coerce numeric
            for col in ("open","high","low","close","volume"):
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    except Exception:
                        pass
                else:
                    df[col] = pd.NA
            df = df[["open","high","low","close","volume"]].dropna(how="all")
            logger.info("Using MT5 feed (%s) for %s -> %d rows (broker symbol=%s)", interval, symbol, len(df), broker_sym)
            return df
    except Exception as e:
        logger.debug("MT5 fetch failed or not available for %s: %s", symbol, getattr(e, "args", e))

    # --- Fallback: yfinance ---
    try:
        import yfinance as yf
        import pandas as pd
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
            if isinstance(df, tuple):
                last_exc = TypeError("yfinance returned tuple")
                continue
            # lower-case columns and map
            df = df.rename(columns={c:c.lower() for c in df.columns})
            colmap = {}
            for c in df.columns:
                if "open" in c: colmap[c] = "open"
                if "high" in c: colmap[c] = "high"
                if "low" in c: colmap[c] = "low"
                if "close" in c: colmap[c] = "close"
                if "volume" in c: colmap[c] = "volume"
            if colmap:
                df = df.rename(columns=colmap)
            for col in ("open","high","low","close","volume"):
                if col not in df.columns:
                    df[col] = pd.NA
            df.index = pd.to_datetime(df.index)
            df = df[["open","high","low","close","volume"]].dropna(how="all")
            logger.info("Fetched %d rows for %s using %s", len(df), symbol, t)
            return df
        except Exception as e:
            last_exc = e
            logger.debug("yfinance try failed for %s: %s", t, getattr(e,"args",e))
            continue
    logger.warning("All data sources failed for %s. Last err: %s", symbol, getattr(last_exc,"args",last_exc))
    return None

def fetch_multi_timeframes(symbol: str, tfs=TIMEFRAMES, period_days=60):
    out = {}
    for label, interval in tfs.items():
        if label == "H4":
            base = fetch_ohlcv(symbol, interval="60m", period_days=period_days)
            if base is None or getattr(base, "empty", True):
                out[label] = None
                continue
            # attempt resample to 4H (try both '4H' and '4h')
            try:
                df4 = base.resample("4H").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
            except Exception:
                try:
                    df4 = base.resample("4h").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
                except Exception as e:
                    logger.info("Resampling to 4H failed for %s: %s", symbol, e)
                    out[label] = None
                    continue
            out[label] = df4
        else:
            df = fetch_ohlcv(symbol, interval=interval, period_days=period_days)
            out[label] = df
    return out

# ---------------- SQLite DB ----------------
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

def get_today_trade_count():
    today = date.today().isoformat()
    conn = sqlite3.connect(TRADE_LOG_DB, timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM trades WHERE timestamp >= ?", (today + "T00:00:00+00:00",))
    r = cur.fetchone(); conn.close()
    return int(r[0]) if r else 0

# ---------------- Telegram ----------------
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

# ---------------- MT5 order execution ----------------
def place_order_mt5(symbol: str, action: str, lot: float, price: float, sl: Optional[float], tp: Optional[float]):
    global _mt5, _mt5_connected
    if not _mt5_connected or _mt5 is None:
        return {"status":"not_connected"}
    try:
        broker_sym = map_symbol_to_broker(symbol)
        try:
            si = _mt5.symbol_info(broker_sym)
            if si is None or not si.visible:
                _mt5.symbol_select(broker_sym, True)
        except Exception:
            pass
        tick = _mt5.symbol_info_tick(broker_sym)
        if tick is None:
            return {"status":"no_tick"}
        order_price = price if price is not None else (tick.ask if action == "BUY" else tick.bid)
        order_type = _mt5.ORDER_TYPE_BUY if action == "BUY" else _mt5.ORDER_TYPE_SELL
        request = {
            "action": _mt5.TRADE_ACTION_DEAL,
            "symbol": broker_sym,
            "volume": float(lot),
            "type": order_type,
            "price": order_price,
            "sl": float(sl) if sl is not None else 0.0,
            "tp": float(tp) if tp is not None else 0.0,
            "deviation": 20,
            "magic": 123456,
            "comment": "Notex5 auto",
            "type_time": _mt5.ORDER_TIME_GTC,
            "type_filling": _mt5.ORDER_FILLING_IOC,
        }
        res = _mt5.order_send(request)
        logger.info("MT5 order_send result: %s", res)
        return {"status":"sent", "result": str(res)}
    except Exception as e:
        logger.exception("MT5 place order exception: %s", e)
        return {"status":"error","error":str(e)}

def place_order(symbol: str, side: str, lots: float, entry_price: float, sl: float, tp: Optional[float] = None):
    logger.info("PLACE ORDER REQUEST %s %s lots=%.2f entry=%.6f sl=%.6f tp=%s", symbol, side, lots, entry_price, sl, tp)
    # basic safety checks (keep it simple)
    if os.path.exists(KILL_SWITCH_FILE):
        logger.info("Kill switch engaged - rejecting order")
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="rejected", order_meta="kill-switch")
        return {"status":"rejected","reason":"kill-switch"}
    if get_today_trade_count() >= MAX_DAILY_TRADES:
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="rejected", order_meta="daily-cap")
        return {"status":"rejected","reason":"daily-cap"}
    if DEMO_SIMULATION:
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="demo", order_meta={"note":"demo"})
        send_telegram(f"Notex5 ALERT (demo)\nSymbol: {symbol}\nAction: {side}\nEntry: {entry_price}\nSL: {sl}\nTP: {tp}\nLots: {lots}")
        return {"status":"demo"}
    # live path
    if REQUIRE_MANUAL_LIVE_CONFIRM and not AUTO_EXECUTE:
        ans = input(f"Confirm LIVE {side} {symbol} {lots} lots at {entry_price}? (yes to proceed): ").strip().lower()
        if ans != "yes":
            record_trade_db(symbol, side, lots, entry_price, sl, tp, status="cancelled_by_user")
            return {"status":"cancelled_by_user"}
    res = place_order_mt5(symbol, side, lots, entry_price, sl, tp)
    record_trade_db(symbol, side, lots, entry_price, sl, tp, status=res.get("status","unknown"), order_meta=res)
    if res.get("status") in ("sent","sent_mt5"):
        send_telegram(f"Notex5 ALERT\nPlaced {side} {symbol} entry={entry_price} sl={sl} tp={tp} lots={lots}")
    return res

# ---------------- Risk helpers ----------------
def account_balance_estimate() -> float:
    try:
        if _mt5_connected and _mt5 is not None:
            ai = _mt5.account_info()
            if ai is not None:
                return float(ai.balance)
    except Exception:
        pass
    return float(os.getenv("FALLBACK_BALANCE", "1000.0"))

def compute_atr_sl(entry_price: float, df, multiplier: float = 1.25) -> float:
    try:
        if df is None or getattr(df, "empty", True):
            return max(0.00001, abs(entry_price) * 0.01)
        if "atr14" in df.columns:
            return float(df["atr14"].iloc[-1])
        import pandas as pd
        tr = pd.concat([df["high"] - df["low"], (df["high"] - df["close"].shift()).abs(), (df["low"] - df["close"].shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14, min_periods=1).mean().iloc[-1]
        return max(0.00001, float(atr) * multiplier)
    except Exception:
        return max(0.00001, abs(entry_price) * 0.01)

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

# ---------------- Strategy core ----------------
def aggregate_multi_tf_scores(tf_dfs: Dict[str,Any]) -> Dict[str,float]:
    techs=[]
    for label, df in tf_dfs.items():
        try:
            if df is None or getattr(df,"empty",True):
                continue
            dfind = add_technical_indicators(df)
            tscore = technical_signal_score(dfind)
            weight = {"M30":1.0,"H1":1.0,"H4":1.6}.get(label,1.0)
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
        if total_score >= 0.20: candidate = "BUY"
        if total_score <= -0.20: candidate = "SELL"
        final_signal = None
        if candidate is not None and abs(total_score) >= 0.15:
            final_signal = candidate
        decision = {"symbol":symbol,"scores":scores,"model_score":model_score,"agg":total_score,"final_signal":final_signal}
        if final_signal:
            entry = float(df_h1["close"].iloc[-1])
            stop_dist = compute_atr_sl(entry, add_technical_indicators(df_h1), multiplier=1.25)
            if final_signal == "BUY":
                sl = entry - stop_dist; tp = entry + stop_dist*2.0
            else:
                sl = entry + stop_dist; tp = entry - stop_dist*2.0
            balance = account_balance_estimate()
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
    logger.info("Starting loop (DEMO=%s AUTO_EXECUTE=%s)", DEMO_SIMULATION, AUTO_EXECUTE)
    # optional monitor for closed trades thread could be started here if MT5 connected
    try:
        while True:
            run_one_cycle()
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")

# ---------------- Startup ----------------
if __name__ == "__main__":
    init_trade_db()
    # connect to MT5 if credentials provided (attempt but don't fail if not)
    try:
        if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
            connect_mt5(login=int(MT5_LOGIN) if str(MT5_LOGIN).isdigit() else None, password=MT5_PASSWORD, server=MT5_SERVER)
    except Exception:
        logger.exception("MT5 connect attempt failed")
    # prefetch
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
