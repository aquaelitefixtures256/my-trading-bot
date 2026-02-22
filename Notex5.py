#!/usr/bin/env python3
# Notex5.py - robust main trading bot (overwrite your existing file)
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
TIMEFRAMES = {"H1": "60m", "H4": "4H", "D": "1d"}

RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.005"))
MAX_TOTAL_OPEN_TRADES = int(os.getenv("MAX_TOTAL_OPEN_TRADES", "3"))
MAX_OPEN_TRADES_PER_SYMBOL = int(os.getenv("MAX_OPEN_TRADES_PER_SYMBOL", "1"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "5"))

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

# ---------------- features import ----------------
try:
    from features.tech_features import add_technical_indicators, technical_signal_score  # type: ignore
    logger.info("Imported features.tech_features")
except Exception as e:
    logger.warning("Could not import features.tech_features (%s). Using lightweight fallbacks.", e)
    def add_technical_indicators(df):
        try:
            import pandas as pd
            df = df.copy()
            if "close" in df:
                df["sma5"] = df["close"].rolling(5, min_periods=1).mean()
                df["sma20"] = df["close"].rolling(20, min_periods=1).mean()
                df["rsi14"] = (df["close"].diff().fillna(0).clip(lower=0).rolling(14, min_periods=1).mean()
                               / (df["close"].diff().abs().rolling(14, min_periods=1).mean().replace(0,1e-8))) * 100
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

# ---------------- Data fetchers ----------------
def symbol_to_yfinance_candidates(sym: str) -> List[str]:
    s = str(sym).upper().replace("/","").replace("-","").strip()
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
    seen=set(); out=[]
    for c in candidates:
        if c and c not in seen:
            out.append(c); seen.add(c)
    return out

def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60):
    try:
        import yfinance as yf
        import pandas as pd
    except Exception as e:
        logger.error("Missing yfinance/pandas: %s", e)
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
            df = df.rename(columns={c:c.lower() for c in df.columns})
            # try to standardize columns to lower-case open/high/low/close/volume
            colmap = {}
            for c in df.columns:
                if "open" in c.lower(): colmap[c] = "open"
                if "high" in c.lower(): colmap[c] = "high"
                if "low" in c.lower(): colmap[c] = "low"
                if "close" in c.lower(): colmap[c] = "close"
                if "volume" in c.lower(): colmap[c] = "volume"
            if colmap:
                df = df.rename(columns=colmap)
            # keep required columns if present
            if not all(k in df.columns for k in ("open","high","low","close","volume")):
                # still accept if 'Close' present; fill missing with NaN - downstream will handle
                df = df
            # convert index to datetime
            try:
                df.index = pd.to_datetime(df.index)
            except Exception:
                pass
            # keep minimal subset
            # ensure columns exist
            for col in ("open","high","low","close","volume"):
                if col not in df.columns:
                    df[col] = pd.NA
            df = df[["open","high","low","close","volume"]].dropna(how="all")
            logger.info("Fetched %d rows for %s using %s", len(df), symbol, t)
            return df
        except Exception as e:
            last_exc = e
            logger.debug("yfinance try failed for %s: %s", t, getattr(e,"args",e))
            continue
    logger.warning("All candidates failed for %s. Last err: %s", symbol, getattr(last_exc,"args",last_exc))
    return None

def fetch_multi_timeframes(symbol: str, tfs=TIMEFRAMES, period_days=60):
    """
    Defensive fetch for timeframes. Normalizes column names and resamples H1->H4.
    """
    import pandas as pd
    out = {}
    def _normalize_ohlcv_frame(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df is None or getattr(df,"empty",True):
            return None
        df = df.copy()
        try:
            df.columns = [str(c).lower() for c in df.columns]
        except Exception:
            pass
        # map variants
        for req in ("open","high","low","close","volume"):
            if req not in df.columns:
                candidates = [c for c in df.columns if req in str(c)]
                if candidates:
                    df[req] = df[candidates[0]]
        if not all(c in df.columns for c in ("open","high","low","close","volume")):
            return None
        # coerce numeric where possible
        try:
            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            df["high"] = pd.to_numeric(df["high"], errors="coerce")
            df["low"] = pd.to_numeric(df["low"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        except Exception:
            pass
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            pass
        return df

    for label, interval in tfs.items():
        if label == "H4":
            base = fetch_ohlcv(symbol, interval="60m", period_days=period_days)
            if base is None or getattr(base,"empty",True):
                out[label] = None; continue
            base = _normalize_ohlcv_frame(base)
            if base is None:
                logger.warning("Resampling to 4H skipped for %s: required OHLCV columns missing after normalization", symbol)
                out[label] = None; continue
            try:
                df4 = base.resample("4H").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
            except Exception:
                try:
                    df4 = base.resample("4h").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
                except Exception as e:
                    logger.warning("Resampling to 4H failed for %s: %s", symbol, e)
                    out[label] = None; continue
            out[label] = df4
        else:
            df = fetch_ohlcv(symbol, interval=interval, period_days=period_days)
            out[label] = _normalize_ohlcv_frame(df) if df is not None else None
    return out

# ---------------- AI model placeholder ----------------
def ai_model_score(symbol: str, features: Dict[str,Any]) -> float:
    if not MODEL_API_URL:
        return 0.0
    try:
        resp = requests.post(MODEL_API_URL, json={"symbol":symbol,"features":features}, timeout=6)
        if resp.status_code == 200:
            data = resp.json(); return float(data.get("score",0.0))
    except Exception:
        logger.debug("Model call failed")
    return 0.0

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

def get_open_trade_counts(symbol: Optional[str]=None):
    try:
        if _mt5_connected and _mt5 is not None:
            if symbol:
                pos = _mt5.positions_get(symbol=symbol)
            else:
                pos = _mt5.positions_get()
            return len(pos) if pos is not None else 0
    except Exception:
        pass
    conn = sqlite3.connect(TRADE_LOG_DB, timeout=5)
    cur = conn.cursor()
    if symbol:
        cur.execute("SELECT COUNT(*) FROM trades WHERE status='open' AND symbol=?", (symbol,))
    else:
        cur.execute("SELECT COUNT(*) FROM trades WHERE status='open'")
    r = cur.fetchone(); conn.close()
    return int(r[0]) if r else 0

def kill_switch_engaged():
    return os.path.exists(KILL_SWITCH_FILE)

def can_place_trade(symbol):
    if kill_switch_engaged(): return False, "kill-switch"
    if get_today_trade_count() >= MAX_DAILY_TRADES: return False, "daily-cap"
    if get_open_trade_counts() >= MAX_TOTAL_OPEN_TRADES: return False, "total-open-limit"
    if get_open_trade_counts(symbol) >= MAX_OPEN_TRADES_PER_SYMBOL: return False, "symbol-open-limit"
    return True, "ok"

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

def notify_trade(decision: Dict[str,Any]):
    try:
        s = decision.get("symbol"); final = decision.get("final_signal")
        entry = decision.get("entry"); sl = decision.get("sl"); tp = decision.get("tp"); lots = decision.get("lots"); agg = decision.get("agg",0.0)
        msg = (f"Notex5 ALERT\nSymbol: {s}\nAction: {final}\nEntry: {entry}\nSL: {sl}\nTP: {tp}\nLots: {lots}\nScore: {agg:.3f}")
        send_telegram(msg)
    except Exception:
        logger.exception("Failed to send trade alert")

# ---------------- MT5 connection & execution ----------------
_mt5 = None
_mt5_connected = False

def connect_mt5(login: Optional[int]=None, password: Optional[str]=None, server: Optional[str]=None) -> bool:
    global _mt5, _mt5_connected
    try:
        import MetaTrader5 as mt5  # type: ignore
        _mt5 = mt5
    except Exception as e:
        logger.error("MetaTrader5 import failed: %s", e)
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
    except Exception:
        logger.exception("MT5 connect error")
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

def place_order_mt5(symbol: str, action: str, lot: float, price: float, sl: Optional[float], tp: Optional[float]):
    global _mt5, _mt5_connected
    if not _mt5_connected:
        return {"status":"not_connected"}
    try:
        try:
            si = _mt5.symbol_info(symbol)
            if si is None or not si.visible:
                _mt5.symbol_select(symbol, True)
        except Exception:
            pass
        tick = _mt5.symbol_info_tick(symbol)
        if tick is None:
            return {"status":"no_tick"}
        order_price = price if price is not None else (tick.ask if action=="BUY" else tick.bid)
        order_type = _mt5.ORDER_TYPE_BUY if action=="BUY" else _mt5.ORDER_TYPE_SELL
        request = {
            "action": _mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
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
    allowed, reason = can_place_trade(symbol)
    if not allowed:
        logger.info("Trade blocked for %s: %s", symbol, reason)
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="rejected", order_meta=reason)
        return {"status":"rejected", "reason": reason}
    if DEMO_SIMULATION:
        record_trade_db(symbol, side, lots, entry_price, sl, tp, status="demo", order_meta={"note":"demo"})
        notify_trade({"symbol":symbol,"final_signal":side,"entry":entry_price,"sl":sl,"tp":tp,"lots":lots,"agg":0.0})
        return {"status":"demo", "symbol":symbol, "side":side, "lots":lots}
    if REQUIRE_MANUAL_LIVE_CONFIRM and not AUTO_EXECUTE:
        ans = input(f"Confirm LIVE {side} {symbol} {lots} lots at {entry_price}? (yes to proceed): ").strip().lower()
        if ans != "yes":
            record_trade_db(symbol, side, lots, entry_price, sl, tp, status="cancelled_by_user")
            return {"status":"cancelled_by_user"}
    res = place_order_mt5(symbol, side, lots, entry_price, sl, tp)
    record_trade_db(symbol, side, lots, entry_price, sl, tp, status=res.get("status","unknown"), order_meta=res)
    if res.get("status") in ("sent","sent_mt5"):
        notify_trade({"symbol":symbol,"final_signal":side,"entry":entry_price,"sl":sl,"tp":tp,"lots":lots,"agg":0.0})
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
        if df is None or getattr(df,"empty",True):
            return max(0.00001, abs(entry_price)*0.01)
        if "atr14" in df.columns:
            atr = float(df["atr14"].iloc[-1])
        else:
            import pandas as pd
            tr = pd.concat([
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs()
            ], axis=1).max(axis=1)
            atr = tr.rolling(14, min_periods=1).mean().iloc[-1]
        return max(0.00001, float(atr)*multiplier)
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

# ---------------- Strategy ----------------
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

def is_crypto_symbol(sym: str) -> bool:
    s = str(sym).upper()
    return "BTC" in s or "ETH" in s or s.startswith("CRYPTO") or "XBT" in s

def is_tradable_now(symbol: str) -> bool:
    wd = datetime.utcnow().weekday()
    if is_crypto_symbol(symbol):
        return True
    if wd >= 5:
        return False
    return True

def make_decision_for_symbol(symbol: str):
    try:
        tf_dfs = fetch_multi_timeframes(symbol)
        df_h1 = tf_dfs.get("H1")
        if df_h1 is None or getattr(df_h1,"empty",True) or len(df_h1) < 30:
            logger.warning("Not enough H1 data for %s - skipping", symbol)
            return None
        scores = aggregate_multi_tf_scores(tf_dfs)
        model_score = ai_model_score(symbol, scores) if MODEL_API_URL else 0.0
        w_tech=0.4; w_fund=0.15; w_sent=0.15; w_model=0.3
        total_score = (w_tech*scores["tech"] + w_fund*scores["fund"] + w_sent*scores["sent"] + w_model*model_score)
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
            balance = account_balance_estimate()
            lots = compute_lots_from_risk(RISK_PER_TRADE_PCT, balance, entry, sl)
            order_res = place_order(symbol, final_signal, lots, entry, sl, tp)
            decision.update({"entry":entry,"sl":sl,"tp":tp,"lots":lots,"order_result":order_res})
        else:
            logger.info("No confident signal for %s (agg=%.3f)", symbol, total_score)
        logger.info("Decision for %s final=%s agg=%.3f tech=%.3f model=%.3f", symbol, decision.get("final_signal"), total_score, scores["tech"], model_score)
        return decision
    except Exception:
        logger.exception("make_decision_for_symbol failed for %s", symbol)
        return None

# ---------------- Monitor closed trades ----------------
def monitor_closed_trades_poll(interval: int = 30):
    if not _mt5_connected or _mt5 is None:
        return
    while True:
        try:
            utc_from = datetime.utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
            deals = _mt5.history_deals_get(utc_from, datetime.now())
            if deals:
                for d in deals:
                    try:
                        if hasattr(d, "profit") and float(d.profit) != 0:
                            msg = f"Notex5 DEAL CLOSED\nSymbol: {d.symbol}\nProfit: {float(d.profit):.2f}\nTicket: {d.ticket}"
                            send_telegram(msg)
                    except Exception:
                        continue
        except Exception:
            pass
        time.sleep(interval)

# ---------------- Auto-update helper ----------------
def auto_update_and_restart(repo_path="."):
    try:
        if not os.path.isdir(os.path.join(repo_path, ".git")):
            logger.info("No git repo at %s", repo_path); return False
        p = subprocess.run(["git", "-C", repo_path, "pull"], capture_output=True, text=True, timeout=60)
        logger.info("git pull: %s", p.stdout.strip())
        if p.returncode != 0:
            logger.warning("git pull failed: %s", p.stderr.strip()); return False
        python = sys.executable
        os.execv(python, [python] + sys.argv)
    except Exception:
        logger.exception("auto_update error")
        return False

# ---------------- Runner ----------------
def run_one_cycle():
    res = {}
    for s in SYMBOLS:
        res[s] = make_decision_for_symbol(s)
        time.sleep(0.2)
    return res

def main_loop():
    logger.info("Starting continuous loop (DEMO=%s AUTO_EXECUTE=%s)", DEMO_SIMULATION, AUTO_EXECUTE)
    if _mt5_connected:
        t = threading.Thread(target=monitor_closed_trades_poll, args=(30,), daemon=True)
        t.start()
    try:
        while True:
            run_one_cycle()
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")

# ---------------- Startup ----------------
if __name__ == "__main__":
    init_trade_db()
    try:
        if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
            connect_mt5(login=int(MT5_LOGIN) if str(MT5_LOGIN).isdigit() else None, password=MT5_PASSWORD, server=MT5_SERVER)
    except Exception:
        logger.exception("MT5 connect attempt failed")
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
        logger.info("Exiting after single cycle. DEMO_SIMULATION=%s", DEMO_SIMULATION)
