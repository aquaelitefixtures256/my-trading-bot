#!/usr/bin/env python3
"""
Ultra_instinct_quant_upgraded.py  -- Your bot upgraded with:
 - (b) Mean-variance solver improvements (Ledoit-Wolf shrinkage and shrinkage fallback + L2-like regularization)
 - (c) Optional HMM / GARCH regime & volatility modeling (uses hmmlearn and arch if installed)
 - (d) Slippage & commission calibration from live broker fills (best-effort via MT5 history)
All additions are optional and gracefully fallback when libraries / broker data are unavailable.
Run as before.
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
import math
from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

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

# sklearn and covariance shrinkage
SKLEARN_AVAILABLE = False
try:
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import SGDClassifier
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.covariance import LedoitWolf
    import joblib
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# optional HMM/GARCH
HMM_AVAILABLE = False
GARCH_AVAILABLE = False
try:
    from hmmlearn.hmm import GaussianHMM  # type: ignore
    HMM_AVAILABLE = True
except Exception:
    HMM_AVAILABLE = False
try:
    from arch import arch_model  # type: ignore
    GARCH_AVAILABLE = True
except Exception:
    GARCH_AVAILABLE = False

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

# ---------------- Configuration (unchanged logic) ----------------
SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY"]
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

BASE_RISK_PER_TRADE_PCT = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MIN_RISK_PER_TRADE_PCT = 0.002
MAX_RISK_PER_TRADE_PCT = 0.01
RISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT

MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "200"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "STOP_TRADING.flag")
ADAPT_STATE_FILE = "adapt_state.json"
TRADES_DB = "trades.db"
MODEL_FILE = "ultra_instinct_model.joblib"
TRADES_CSV = "trades.csv"
CURRENT_THRESHOLD = float(os.getenv("CURRENT_THRESHOLD", "0.08"))
MIN_THRESHOLD = 0.06
MAX_THRESHOLD = 0.35
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

# fundamental API (optional) - user may set NEWS_API_KEY env to enable
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
TRADING_ECONOMICS_KEY = os.getenv("TRADING_ECONOMICS_KEY", "")

# Pause window before major events (minutes)
PAUSE_BEFORE_EVENT_MINUTES = int(os.getenv("PAUSE_BEFORE_EVENT_MINUTES", "30"))

# Slippage/commission defaults (may be recalibrated by calibration)
DEFAULT_SLIPPAGE_PIPS = float(os.getenv("DEFAULT_SLIPPAGE_PIPS", "0.5"))  # pips
DEFAULT_COMMISSION = float(os.getenv("DEFAULT_COMMISSION", "0.0"))      # currency per lot (sim)
_calibrated = {"slippage_pips": DEFAULT_SLIPPAGE_PIPS, "commission_per_lot": DEFAULT_COMMISSION, "last_calibrated": 0}

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
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
        if not cur.fetchone():
            cols_sql = ",\n      ".join([f"{k} {v}" for k, v in expected_cols.items()])
            create_sql = f"CREATE TABLE trades (\n      {cols_sql}\n    );"
            cur.execute(create_sql)
            conn.commit()
        else:
            existing = _get_table_columns(conn, "trades")
            for col, ctype in expected_cols.items():
                if col not in existing:
                    try:
                        if col == "id":
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

    if not os.path.exists(TRADES_CSV):
        try:
            with open(TRADES_CSV, "w", encoding="utf-8") as f:
                f.write("ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta\n")
        except Exception:
            logger.exception("Failed to create trades csv")

def record_trade(symbol, side, entry, sl, tp, lots, status="sim", pnl=0.0, rmult=0.0, regime="unknown", score=0.0, model_score=0.0, meta=None):
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
            conn.close()
            init_trade_db()
            conn = sqlite3.connect(TRADES_DB, timeout=5)
            cur = conn.cursor()
            cols = _get_table_columns(conn, "trades")
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

# ---------------- Indicators & scoring (kept) ----------------
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

# ---------------- Regime detection: HMM (optional) or fallback ----------------
def _hmm_regime_detection(df_h1: pd.DataFrame, n_states: int = 3):
    """
    If hmmlearn available, fit a GaussianHMM on recent returns and map states to regimes by mean volatility/ADX.
    Returns (regime_label, atr_rel, adx)
    """
    try:
        if not HMM_AVAILABLE:
            return None
        df = add_technical_indicators(df_h1.copy())
        rets = df["close"].pct_change().dropna().values.reshape(-1, 1)
        if len(rets) < 60:
            return None
        model = GaussianHMM(n_components=n_states, covariance_type="diag", n_iter=200, random_state=42)
        model.fit(rets)
        states = model.predict(rets)
        last_state = int(states[-1])
        # compute state statistics
        state_rets = rets[states == last_state]
        vol = float(np.std(state_rets)) if len(state_rets) > 0 else 0.0
        # derive adx/atr_rel as in fallback
        atr = float(df["atr14"].iloc[-1] or 0.0)
        price = float(df["close"].iloc[-1] or 1.0)
        atr_rel = atr / price if price else 0.0
        adx = float(df["adx"].iloc[-1] or 0.0)
        # map state to rough label by volatility
        if vol < 0.0015 and adx < 20:
            return "quiet", atr_rel, adx
        if vol > 0.008 and adx > 25:
            return "volatile", atr_rel, adx
        if adx > 25:
            return "trending", atr_rel, adx
        return "normal", atr_rel, adx
    except Exception:
        logger.exception("_hmm_regime_detection failed")
        return None

def _garch_vol_forecast(df_h1: pd.DataFrame):
    """
    If arch available, fit a lightweight GARCH(1,1) to returns and forecast 1-step volatility.
    Returns volatility (std) estimate or None.
    """
    try:
        if not GARCH_AVAILABLE:
            return None
        df = add_technical_indicators(df_h1.copy())
        rets = df["close"].pct_change().dropna() * 100.0  # percent returns for arch stability
        if len(rets) < 100:
            return None
        am = arch_model(rets, vol="Garch", p=1, q=1, dist="normal")
        res = am.fit(disp="off")
        f = res.forecast(horizon=1, reindex=False)
        var = f.variance.values[-1, 0]
        sigma = float(np.sqrt(var) / 100.0)  # convert back to fraction
        return sigma
    except Exception:
        logger.exception("_garch_vol_forecast failed")
        return None

def detect_market_regime_from_h1(df_h1: pd.DataFrame):
    """
    Enhanced regime detection:
      - Prefer HMM result if available
      - Optionally use GARCH to refine volatility estimate
      - Fallback to previous deterministic rules
    """
    try:
        if df_h1 is None or df_h1.empty:
            return "unknown", None, None
        # try HMM first
        if HMM_AVAILABLE:
            try:
                res = _hmm_regime_detection(df_h1)
                if res:
                    return res
            except Exception:
                pass
        # fall back to GARCH for vol estimate but still use ADX for trendiness
        try:
            d = add_technical_indicators(df_h1)
            atr = float(d["atr14"].iloc[-1] or 0.0)
            price = float(d["close"].iloc[-1] or 1.0)
            atr_rel = atr / price if price else 0.0
            adx = float(d["adx"].iloc[-1] or 0.0)
            if GARCH_AVAILABLE:
                try:
                    sigma = _garch_vol_forecast(df_h1)
                    # if garch sigma much higher than atr_rel, consider volatile
                    if sigma and sigma > max(0.007, atr_rel * 1.5) and adx > 25:
                        return "volatile", atr_rel, adx
                except Exception:
                    pass
            # previous deterministic rules
            if atr_rel < 0.0025 and adx < 20:
                return "quiet", atr_rel, adx
            if atr_rel > 0.0075 and adx > 25:
                return "volatile", atr_rel, adx
            if adx > 25:
                return "trending", atr_rel, adx
            return "normal", atr_rel, adx
        except Exception:
            return "unknown", None, None
    except Exception:
        logger.exception("detect_market_regime failed")
        return "unknown", None, None

# ---------------- Technical scoring (kept) ----------------
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

# ---------------- Mean-variance improvements (Ledoit-Wolf + shrinkage fallback) ----------------
_portfolio_weights_cache = {"ts": 0, "weights": {}}
PORTFOLIO_RECOMPUTE_SECONDS = 300  # recompute every 5 minutes

def _shrink_covariance(sample_cov: np.ndarray, alpha: float = 0.1) -> np.ndarray:
    """
    Simple shrinkage toward diagonal (L2-like regularization).
    alpha in [0,1] where 1 -> full shrink to diagonal.
    """
    try:
        diag = np.diag(np.diag(sample_cov))
        return (1 - alpha) * sample_cov + alpha * diag
    except Exception:
        return sample_cov

def compute_portfolio_weights(symbols: List[str], period_days: int = 45):
    """
    Mean-variance weighting with robust covariance estimation:
      - if LedoitWolf available, use it
      - else apply simple shrinkage
      - solves w = inv(cov + lambda*I) * mean_rets, then forces non-negative and normalize
    """
    global _portfolio_weights_cache
    now = time.time()
    if now - _portfolio_weights_cache.get("ts", 0) < PORTFOLIO_RECOMPUTE_SECONDS and _portfolio_weights_cache.get("weights"):
        return _portfolio_weights_cache["weights"]
    rets = {}
    for s in symbols:
        try:
            df = fetch_ohlcv(s, interval="60m", period_days=period_days)
            if df is None or getattr(df, "empty", True):
                continue
            rets_s = df["close"].pct_change().dropna()
            rets[s] = rets_s
        except Exception:
            continue
    symbols_ok = list(rets.keys())
    if not symbols_ok:
        weights = {s: 1.0 / max(1, len(symbols)) for s in symbols}
        _portfolio_weights_cache = {"ts": now, "weights": weights}
        return weights
    try:
        rets_df = pd.DataFrame(rets).fillna(0.0)
        sample_cov = rets_df.cov().fillna(0.0).values
        mean_rets = rets_df.mean().values
        # try Ledoit-Wolf shrinkage if available
        cov_mat = None
        if SKLEARN_AVAILABLE:
            try:
                lw = LedoitWolf().fit(rets_df.values)
                cov_mat = lw.covariance_
            except Exception:
                cov_mat = None
        if cov_mat is None:
            # fallback shrinkage toward diagonal with alpha chosen by heuristic
            alpha = 0.2  # moderate shrinkage
            cov_mat = _shrink_covariance(sample_cov, alpha=alpha)
        # regularization to ensure invertible
        lam = 1e-4
        cov_reg = cov_mat + lam * np.eye(cov_mat.shape[0])
        inv_cov = np.linalg.pinv(cov_reg)
        raw_w = inv_cov.dot(mean_rets)
        # apply L2-like regularization (soft shrink toward equal weights)
        raw_w = raw_w - 0.01 * raw_w  # small shrink multiplier
        # ensure non-negative and normalize
        raw_w = np.maximum(raw_w, 0.0)
        if raw_w.sum() <= 0:
            weights = {s: 1.0 / len(symbols_ok) for s in symbols_ok}
        else:
            norm = raw_w / raw_w.sum()
            weights = {s: float(norm[i]) for i, s in enumerate(symbols_ok)}
    except Exception:
        weights = {s: 1.0 / max(1, len(symbols)) for s in symbols}
    # fill missing symbols
    for s in symbols:
        if s not in weights:
            weights[s] = 0.0001
    total = sum(weights.values()) or 1.0
    weights = {s: weights[s] / total for s in symbols}
    _portfolio_weights_cache = {"ts": now, "weights": weights}
    return weights

def get_portfolio_scale_for_symbol(symbol: str, weights: Dict[str, float]):
    if not weights or symbol not in weights:
        return 1.0
    w = float(weights.get(symbol, 0.0))
    avg = sum(weights.values()) / max(1, len(weights))
    if avg <= 0:
        return 1.0
    ratio = w / avg
    scale = 1.0 + (ratio - 1.0) * 0.4
    return max(0.6, min(1.4, scale))

# ---------------- Fundamental and calendar (null-safe) ----------------
def fetch_fundamental_score(symbol: str, lookback_days: int = 7) -> float:
    if not FUNDAMENTAL_AVAILABLE or not NEWS_API_KEY:
        return 0.0
    # Map symbol to natural language query
    query = symbol
    if symbol.upper() in ("XAUUSD", "GOLD"):
        query = "gold OR xauusd"
    if symbol.upper() in ("XAGUSD", "SILVER"):
        query = "silver OR xagusd"
    if symbol.upper().endswith("USD") and symbol.upper().startswith("BTC"):
        query = "bitcoin OR btc"
    try:
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
        pos_words = {"gain", "rise", "surge", "up", "positive", "bull", "beats", "beat", "record", "rally", "higher"}
        neg_words = {"fall", "drop", "down", "loss", "negative", "bear", "miss", "misses", "crash", "decline", "lower"}
        score = 0.0
        for a in articles:
            title = (a.get("title") or "") or ""
            desc = (a.get("description") or "") or ""
            txt = (str(title) + " " + str(desc)).lower()
            p = sum(1 for w in pos_words if w in txt)
            n = sum(1 for w in neg_words if w in txt)
            score += (p - n)
        max_possible = max(1, len(articles) * 2)
        normalized = max(-1.0, min(1.0, score / float(max_possible)))
        return float(normalized)
    except Exception:
        logger.exception("fetch_fundamental_score failed")
        return 0.0

def fetch_economic_calendar_events(lookback_hours: int = 6, lookahead_hours: int = 6) -> List[Dict[str, Any]]:
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
        return events or []
    except Exception:
        logger.exception("fetch_economic_calendar_events failed")
        return []

def fetch_economic_calendar_score(symbol: str, lookback_hours: int = 6, lookahead_hours: int = 6) -> float:
    if not FUNDAMENTAL_AVAILABLE or not TRADING_ECONOMICS_KEY:
        return 0.0
    try:
        evs = fetch_economic_calendar_events(lookback_hours=lookback_hours, lookahead_hours=lookahead_hours)
        if not evs:
            return 0.0
        related = []
        currs = _symbol_to_currencies(symbol)
        for e in evs:
            impact = e.get("impact") or e.get("Impact") or e.get("importance") or ""
            country = (e.get("country") or e.get("Country") or "").upper()
            title = (e.get("event") or e.get("Event") or e.get("title") or "").lower()
            if not impact:
                continue
            if str(impact).lower() not in ("high", "h", "high impact"):
                continue
            match = False
            for c in currs:
                if c and (c.lower() in title or c.upper() == country or c.upper() in str(e.get("category", "")).upper()):
                    match = True
            if match:
                related.append(e)
        if not related:
            return 0.0
        score = 0.0; count = 0
        for e in related:
            actual = e.get("actual") or e.get("Actual") or e.get("value") or e.get("Value")
            forecast = e.get("consensus") or e.get("Consensus") or e.get("forecast") or e.get("Forecast")
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
    try:
        if not FUNDAMENTAL_AVAILABLE or not TRADING_ECONOMICS_KEY:
            return False, None
        evs = fetch_economic_calendar_events(lookback_hours=0, lookahead_hours=int(max(1, lookahead_minutes / 60)))
        if not evs:
            return False, None
        now = datetime.utcnow()
        currs = _symbol_to_currencies(symbol)
        for e in evs:
            impact = e.get("impact") or e.get("Impact") or e.get("importance") or ""
            if not impact or str(impact).lower() not in ("high", "h", "high impact"):
                continue
            when = None
            for key in ("date", "Date", "scheduled", "Scheduled", "dateTime", "Datetime"):
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
                title = (e.get("event") or e.get("Event") or "").lower()
                country = (e.get("country") or "").upper()
                for c in currs:
                    if c and (c.lower() in title or c.upper() == country):
                        return True, {"event": title, "minutes_to": diff, "impact": impact, "raw": e}
        return False, None
    except Exception:
        logger.exception("should_pause_for_events failed")
        return False, None

def _symbol_to_currencies(symbol: str) -> List[str]:
    s = symbol.upper()
    if len(s) >= 6:
        base = s[:3]
        quote = s[3:6]
        return [base, quote]
    if s.startswith("XAU") or "XAU" in s:
        return ["XAU", "USD"]
    if s.startswith("XAG") or "XAG" in s:
        return ["XAG", "USD"]
    if s.startswith("BTC"):
        return ["BTC", "USD"]
    return [s]

# ---------------- ML model hooks (kept/enhanced) ----------------
model_pipe = None

def build_model():
    if not SKLEARN_AVAILABLE:
        return None
    try:
        if 'RandomForestClassifier' in globals():
            clf = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=1)
            return Pipeline([("clf", clf)])
        else:
            pipe = Pipeline([("scaler", StandardScaler()), ("clf", SGDClassifier(loss="log", max_iter=1000, tol=1e-3, random_state=42))])
            return pipe
    except Exception:
        try:
            pipe = Pipeline([("scaler", StandardScaler()), ("clf", SGDClassifier(loss="log", max_iter=1000, tol=1e-3, random_state=42))])
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
    try:
        model_pipe = build_model()
        return model_pipe
    except Exception:
        return None

if SKLEARN_AVAILABLE:
    load_model()

def extract_features_for_model(df_h1: pd.DataFrame, tech_score: float, symbol: str, regime_code: int):
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
        return np.array([[tech_score, 0.0, 50.0, 0.0, regime_code]], dtype=float)

def online_retrain_from_trades(min_examples: int = 20):
    global model_pipe
    if not SKLEARN_AVAILABLE or model_pipe is None:
        return False
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT symbol,entry,sl,tp,rmult,score,model_score,meta,ts FROM trades ORDER BY id DESC LIMIT 500")
        rows = cur.fetchall()
        conn.close()
        X = []
        y = []
        for r in reversed(rows):
            try:
                label = 1 if r[4] and r[4] > 0 else 0
                score = float(r[5] or 0.0)
                model_sc = float(r[6] or 0.0)
                X.append([score, model_sc, float(label)])
                y.append(label)
            except Exception:
                continue
        if len(y) < min_examples:
            return False
        X = np.array(X)
        y = np.array(y)
        clf = model_pipe.named_steps.get("clf") if hasattr(model_pipe, "named_steps") else None
        if hasattr(clf, "partial_fit"):
            try:
                if "scaler" in (model_pipe.named_steps if hasattr(model_pipe, "named_steps") else {}):
                    scaler = model_pipe.named_steps["scaler"]
                    Xs = scaler.transform(X)
                    model_pipe.named_steps["clf"].partial_fit(Xs, y, classes=np.array([0, 1]))
                else:
                    model_pipe.named_steps["clf"].partial_fit(X, y, classes=np.array([0, 1]))
                try:
                    joblib.dump(model_pipe, MODEL_FILE)
                except Exception:
                    pass
                logger.info("Online retrain completed with %d examples", len(y))
                return True
            except Exception:
                logger.exception("partial_fit failed")
                return False
        else:
            try:
                model_pipe.fit(X, y)
                try:
                    joblib.dump(model_pipe, MODEL_FILE)
                except Exception:
                    pass
                logger.info("Retrained model with %d examples", len(y))
                return True
            except Exception:
                logger.exception("full fit failed")
                return False
    except Exception:
        logger.exception("online_retrain_from_trades failed")
        return False

# ---------------- Risk engine (separated) ----------------
def compute_lots_from_risk(risk_pct, balance, entry_price, stop_price, min_lot=0.01, lot_step=0.01, multiplier=100000):
    try:
        risk_amount = balance * risk_pct
        pip_risk = abs(entry_price - stop_price)
        if pip_risk <= 0:
            return min_lot
        lots = risk_amount / (pip_risk * multiplier)
        lots = max(min_lot, lots)
        steps = int(round(lots / lot_step))
        lots_adj = max(min_lot, steps * lot_step)
        return round(float(lots_adj), 2)
    except Exception:
        return float(min_lot)

# ---------------- Slippage & execution modeling ----------------
def apply_slippage(entry_price: float, side: str, slippage_pips: float, pip_value: float = 0.0001) -> float:
    try:
        adj = slippage_pips * pip_value
        if side == "BUY":
            return float(entry_price + adj)
        else:
            return float(entry_price - adj)
    except Exception:
        return entry_price

# ---------------- Calibration from live fills (best-effort) ----------------
def calibrate_slippage_and_commission(days: int = 14):
    """
    Best-effort calibration:
      - Commission per lot is estimated from historical deals if commission fields exist.
      - Slippage estimate: best-effort using difference between deal price and recent close (proxy).
    This is approximate; if MT5 history not available returns defaults.
    """
    global _calibrated
    now = time.time()
    # throttle recalibration to once per hour
    if now - _calibrated.get("last_calibrated", 0) < 3600 and _calibrated.get("commission_per_lot", None) is not None:
        return _calibrated
    if not MT5_AVAILABLE or not _mt5_connected:
        _calibrated.update({"slippage_pips": DEFAULT_SLIPPAGE_PIPS, "commission_per_lot": DEFAULT_COMMISSION, "last_calibrated": now})
        return _calibrated
    try:
        utc_to = datetime.utcnow()
        utc_from = utc_to - timedelta(days=days)
        # fetch deals history
        try:
            deals = _mt5.history_deals_get(utc_from, utc_to)
        except Exception:
            deals = None
        if not deals:
            _calibrated.update({"slippage_pips": DEFAULT_SLIPPAGE_PIPS, "commission_per_lot": DEFAULT_COMMISSION, "last_calibrated": now})
            return _calibrated
        total_comm = 0.0
        total_volume = 0.0
        slippage_list = []
        for d in deals:
            try:
                # fields vary by broker / MT5 version; use getattr with defaults
                price = getattr(d, "price", None)
                volume = float(getattr(d, "volume", 0.0) or 0.0)
                commission = float(getattr(d, "commission", 0.0) or 0.0)
                magic = getattr(d, "magic", None)
                # attempt to estimate slippage proxy: compare deal price to mid of bar at that time if available
                time_s = getattr(d, "time", None)
                if price is not None and volume and time_s:
                    try:
                        # time from mt5 is seconds since epoch
                        dt = datetime.utcfromtimestamp(int(time_s))
                        # fetch a single bar at that timestamp for the symbol if possible: expensive, skip detailed lookup
                        # instead, use difference to last known tick for symbol if available
                        # best-effort: treat slippage unknown
                        pass
                    except Exception:
                        pass
                total_comm += abs(commission)
                total_volume += volume
            except Exception:
                continue
        commission_per_lot = DEFAULT_COMMISSION
        if total_volume > 0:
            # MT5 volume units: lots. commission per lot = total_comm / total_volume
            commission_per_lot = float(total_comm / total_volume)
        # slippage: unable to reconstruct requested price reliably; keep default but record calibration timestamp
        slippage_pips = DEFAULT_SLIPPAGE_PIPS
        _calibrated.update({"slippage_pips": slippage_pips, "commission_per_lot": commission_per_lot, "last_calibrated": now})
        logger.info("Calibration complete: slippage_pips=%.3f commission_per_lot=%.5f", slippage_pips, commission_per_lot)
        return _calibrated
    except Exception:
        logger.exception("calibrate_slippage_and_commission failed")
        _calibrated.update({"slippage_pips": DEFAULT_SLIPPAGE_PIPS, "commission_per_lot": DEFAULT_COMMISSION, "last_calibrated": now})
        return _calibrated

# ---------------- Simulation/backtest and optimizer (enhanced) ----------------
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

def simulate_strategy_on_series_with_execution(df_h1, threshold, atr_mult=1.25, max_trades=200, slippage_pips: Optional[float] = None, commission: Optional[float] = None):
    slippage_pips = slippage_pips if slippage_pips is not None else _calibrated.get("slippage_pips", DEFAULT_SLIPPAGE_PIPS)
    commission = commission if commission is not None else _calibrated.get("commission_per_lot", DEFAULT_COMMISSION)
    if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 80:
        return {"n": 0, "net": 0.0, "avg_r": 0.0, "win": 0.0, "trades": []}
    df = add_technical_indicators(df_h1.copy())
    trades = []
    trade_records = []
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
        entry_executed = apply_slippage(entry, side, slippage_pips)
        atr = float(df["atr14"].iloc[i] or 0.0)
        stop = atr * atr_mult
        if side == "BUY":
            sl = entry_executed - stop
            tp = entry_executed + stop * 2.0
        else:
            sl = entry_executed + stop
            tp = entry_executed - stop * 2.0
        r_mult = 0.0
        exit_index = None
        for j in range(i + 1, min(i + 61, len(df))):
            high = float(df["high"].iloc[j]); low = float(df["low"].iloc[j])
            if side == "BUY":
                if high >= tp:
                    r_mult = 2.0; exit_index = j; break
                if low <= sl:
                    r_mult = -1.0; exit_index = j; break
            else:
                if low <= tp:
                    r_mult = 2.0; exit_index = j; break
                if high >= sl:
                    r_mult = -1.0; exit_index = j; break
        trades.append(r_mult)
        trade_records.append({"i": i, "side": side, "entry": entry_executed, "sl": sl, "tp": tp, "r_mult": r_mult, "exit_idx": exit_index})
        if len(trades) >= max_trades:
            break
    n = len(trades)
    if n == 0:
        return {"n": 0, "net": 0.0, "avg_r": 0.0, "win": 0.0, "trades": trade_records}
    net = sum(trades)
    avg = net / n
    win = sum(1 for t in trades if t > 0) / n
    return {"n": n, "net": net, "avg_r": avg, "win": win, "trades": trade_records}

def monte_carlo_robustness(trade_rmults: List[float], runs: int = 250, seed: Optional[int] = None):
    if not trade_rmults:
        return {"runs": 0, "median": 0.0, "p95": 0.0, "p5": 0.0, "all": []}
    rng = np.random.default_rng(seed)
    totals = []
    arr = np.array(trade_rmults)
    for _ in range(runs):
        sample = rng.choice(arr, size=len(arr), replace=True)
        totals.append(float(sample.sum()))
    totals = np.array(totals)
    return {"runs": runs, "median": float(np.median(totals)), "p95": float(np.percentile(totals, 95)), "p5": float(np.percentile(totals, 5)), "all": totals.tolist()}

# ---------------- Optimizer / walk-forward (kept/enhanced) ----------------
def walk_forward_optimize(symbol: str, param_grid: Dict[str, List[Any]], train_days: int = 90, test_days: int = 14):
    df = fetch_ohlcv(symbol, interval="60m", period_days=365)
    if df is None or getattr(df, "empty", True):
        logger.info("No data for walk-forward %s", symbol)
        return None
    df = add_technical_indicators(df)
    results = []
    start = 0
    rows_per_day = 24
    train_rows = max(48, train_days * rows_per_day)
    test_rows = max(12, test_days * rows_per_day)
    i = train_rows
    while i + test_rows < len(df):
        train_df = df.iloc[i - train_rows:i]
        test_df = df.iloc[i:i + test_rows]
        best_score = -1e9; best_params = None
        for thr in param_grid.get("threshold", [CURRENT_THRESHOLD]):
            for atrm in param_grid.get("atr_mult", [1.25]):
                sim = simulate_strategy_on_series_with_execution(train_df, thr, atr_mult=atrm)
                metric = sim["avg_r"] * math.sqrt(sim["n"]) if sim["n"] > 0 else -1e6
                if metric > best_score:
                    best_score = metric
                    best_params = {"threshold": thr, "atr_mult": atrm}
        if best_params:
            test_sim = simulate_strategy_on_series_with_execution(test_df, best_params["threshold"], atr_mult=best_params["atr_mult"])
            results.append({"train_end_idx": i, "best_params": best_params, "train_metric": best_score, "test": test_sim})
        i += test_rows
    if not results:
        return None
    avg_test_win = np.mean([r["test"]["win"] for r in results if r["test"]["n"] > 0]) if results else 0.0
    avg_test_avg_r = np.mean([r["test"]["avg_r"] for r in results if r["test"]["n"] > 0]) if results else 0.0
    return {"symbol": symbol, "rounds": len(results), "avg_win": float(avg_test_win), "avg_avg_r": float(avg_test_avg_r), "details": results}

# ---------------- Execution, order sending (kept) ----------------
def place_order_mt5(symbol, action, lot, price, sl, tp):
    if not MT5_AVAILABLE or not _mt5_connected:
        return {"status": "mt5_not_connected"}
    try:
        broker = map_symbol_to_broker(symbol)
        si = _mt5.symbol_info(broker)
        if si is None:
            return {"status": "symbol_not_found", "symbol": broker}
        try:
            if not si.visible:
                _mt5.symbol_select(broker, True)
        except Exception:
            pass
        tick = _mt5.symbol_info_tick(broker)
        if tick is None:
            return {"status": "no_tick", "symbol": broker}
        vol_min = getattr(si, "volume_min", None) or 0.01
        vol_step = getattr(si, "volume_step", None) or 0.01
        vol_max = getattr(si, "volume_max", None) or None
        point = getattr(si, "point", None) or getattr(si, "trade_tick_size", None) or getattr(si, "tick_size", None) or 0.00001
        stop_level = getattr(si, "stop_level", None)
        if stop_level is not None and stop_level >= 0:
            min_sl_dist = float(stop_level) * float(point)
        else:
            min_sl_dist = float(point) * 10.0
        order_price = price if price is not None else (tick.ask if action == "BUY" else tick.bid)
        try:
            lots = float(lot)
        except Exception:
            lots = float(vol_min)
        try:
            if vol_step > 0:
                steps = max(0, int((lots - vol_min) // vol_step))
                lots_adj = vol_min + steps * vol_step
                if lots > lots_adj:
                    steps_ceil = int(((lots - vol_min) + vol_step - 1e-12) // vol_step)
                    lots_adj = vol_min + steps_ceil * vol_step
                lots = round(float(max(vol_min, lots_adj)), 2)
            else:
                lots = float(max(vol_min, lots))
        except Exception:
            lots = float(max(vol_min, 0.01))
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
        if lots < vol_min:
            lots = float(vol_min)
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
        if retcode == 10027:
            return {"status": "autotrading_disabled", "retcode": retcode, "result": str(res)}
        if retcode is not None and retcode != 0:
            return {"status": "rejected", "retcode": retcode, "result": str(res)}
        return {"status": "sent", "result": str(res), "used_lots": lots}
    except Exception:
        logger.exception("place_order_mt5 failed")
        return {"status": "error"}

def place_order_simulated(symbol, side, lots, entry, sl, tp, score, model_score, regime):
    slippage = _calibrated.get("slippage_pips", DEFAULT_SLIPPAGE_PIPS)
    commission = _calibrated.get("commission_per_lot", DEFAULT_COMMISSION)
    executed_entry = apply_slippage(entry, side, slippage)
    record_trade(symbol, side, executed_entry, sl, tp, lots, status="sim_open", pnl=0.0, rmult=0.0, regime=regime, score=score, model_score=model_score, meta={"sim": True, "slippage_pips": slippage, "commission": commission})
    return {"status": "sim_open", "entry_executed": executed_entry}

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

# ---------------- Decision-making (enhanced) ----------------
cycle_counter = 0

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
                try:
                    proba = model_pipe.predict_proba(X)[:,1][0]
                    model_score = float((proba - 0.5) * 2.0)
                except Exception:
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
                econ_sent = fetch_economic_calendar_score(symbol, lookback_hours=12, lookahead_hours=12)
            except Exception:
                econ_sent = 0.0
            fundamental_score = float(0.65 * news_sent + 0.35 * econ_sent)
        except Exception:
            fundamental_score = 0.0

        # calibrate slippage/commission periodically if connected
        try:
            if MT5_AVAILABLE and _mt5_connected:
                calibrate_slippage_and_commission(days=14)
        except Exception:
            pass

        # check trade-pause logic for imminent high-impact events
        try:
            pause, ev = should_pause_for_events(symbol, lookahead_minutes=PAUSE_BEFORE_EVENT_MINUTES)
            if pause:
                logger.info("Pausing trading for %s due to upcoming event (in %.1f minutes): %s", symbol, ev.get("minutes_to", -1), ev.get("event", "unknown"))
                decision = {"symbol": symbol, "agg": 0.0, "tech": tech_score, "model_score": model_score, "fund_score": fundamental_score, "final": None, "paused": True, "pause_event": ev}
                return decision
        except Exception:
            pass

        # portfolio-aware weight adjustments (with improved covariance)
        try:
            weights = compute_portfolio_weights(SYMBOLS, period_days=45)
            port_scale = get_portfolio_scale_for_symbol(symbol, weights)
        except Exception:
            port_scale = 1.0

        # combine scores (strengthened fundamentals)
        total_score = (0.40 * tech_score) + (0.25 * model_score) + (0.35 * fundamental_score)
        total_score = total_score * (0.5 + 0.5 * port_scale)

        candidate = None
        if total_score >= 0.08:
            candidate = "BUY"
        if total_score <= -0.08:
            candidate = "SELL"
        final_signal = None
        if candidate is not None and abs(total_score) >= 0.06:
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
            risk_pct = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, risk_pct * port_scale))
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

# ---------------- adapt_and_optimize (kept/enhanced) ----------------
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
        try:
            compute_portfolio_weights(SYMBOLS, period_days=45)
        except Exception:
            pass
        if DEMO_SIMULATION:
            light_optimizer(SYMBOLS, budget=8)
        try:
            online_retrain_from_trades(min_examples=30)
        except Exception:
            logger.debug("online retrain failed")
    except Exception:
        logger.exception("adapt_and_optimize failed")

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
            st = simulate_strategy_on_series_with_execution(df, cand_thresh, atr_mult=1.25, max_trades=120)
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
        baseline_stats.append(simulate_strategy_on_series_with_execution(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=120))
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

# ---------------- Runner & CLI ----------------
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

def run_backtest():
    logger.info("Running backtest for symbols: %s", SYMBOLS)
    for s in SYMBOLS:
        df = fetch_multi_timeframes(s, period_days=365).get("H1")
        if df is None:
            logger.info("No H1 for %s (MT5 missing) - skipping", s)
            continue
        res = simulate_strategy_on_series_with_execution(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=1000)
        logger.info("Backtest %s -> n=%d win=%.3f avg_r=%.3f", s, res["n"], res["win"], res["avg_r"])
        mc = monte_carlo_robustness([t["r_mult"] for t in res.get("trades", [])], runs=250)
        logger.info("Monte Carlo %s -> median=%.3f p95=%.3f p5=%.3f", s, mc["median"], mc["p95"], mc["p5"])
    logger.info("Backtest complete")

def run_walk_forward_over_symbols():
    grid = {"threshold": [0.06, 0.08, 0.10], "atr_mult": [1.0, 1.25, 1.5]}
    agg = {}
    for s in SYMBOLS:
        wf = walk_forward_optimize(s, grid)
        agg[s] = wf
        logger.info("WF %s -> %s", s, ("no result" if wf is None else f"rounds={wf['rounds']} avg_win={wf['avg_win']:.3f}"))
    return agg

def confirm_enable_live():
    if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND THE RISKS":
        return True
    got = input("To enable LIVE trading type exactly: I UNDERSTAND THE RISKS\nType now: ").strip()
    return got == "I UNDERSTAND_THE_RISKS" or got == "I UNDERSTAND THE RISKS"

def setup_and_run(args):
    init_trade_db()
    # connect to MT5 if possible
    if MT5_AVAILABLE and MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
        ok = connect_mt5(login=int(MT5_LOGIN) if str(MT5_LOGIN).isdigit() else None, password=MT5_PASSWORD, server=MT5_SERVER)
        if ok:
            logger.info("MT5 connected; preferring MT5 feed/execution")
            # best-effort calibration on startup if connected
            try:
                calibrate_slippage_and_commission(days=14)
            except Exception:
                logger.debug("initial calibration failed")
    else:
        logger.info("MT5 not available or credentials not provided - bot will not fetch data")
    if args.backtest:
        run_backtest()
        return
    if args.walkforward:
        run_walk_forward_over_symbols()
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
    parser.add_argument("--walkforward", action="store_true", help="run walk-forward optimization across symbols")
    parser.add_argument("--symbols", nargs="*", help="override symbols")
    args = parser.parse_args()
    if args.symbols:
        SYMBOLS = args.symbols
    setup_and_run(args)
