#!/usr/bin/env python3
"""
Notex5_advanced.py - Adaptive AI trading bot with MT5 integration, backtest mode,
optimizer, regime detection, volatility-adjusted risk, and optional live execution.

USAGE (examples):
  python Notex5_advanced.py                # one-run in DEMO mode
  python Notex5_advanced.py --loop         # continuous DEMO loop
  python Notex5_advanced.py --backtest     # run historical backtest on symbols
  python Notex5_advanced.py --live         # attempt to run live (requires explicit confirmation)

Important safety: live execution requires user to type EXACT phrase
"I UNDERSTAND THE RISKS" when prompted or set environment variable CONFIRM_AUTO.
Keep DEMO mode until you are 100% ready and have tested backtests extensively.
"""
from __future__ import annotations
import os, sys, time, json, math, random, logging, sqlite3, argparse
from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, List

# core libs
try:
    import numpy as np
    import pandas as pd
except Exception as e:
    raise RuntimeError("This script requires numpy and pandas. Install them: pip install numpy pandas") from e

# optional libraries
try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_LIB_AVAILABLE = True
except Exception:
    MT5_LIB_AVAILABLE = False

try:
    import yfinance as yf
    YF_AVAILABLE = True
except Exception:
    YF_AVAILABLE = False

try:
    from ta.trend import ADXIndicator, SMAIndicator
    from ta.volatility import AverageTrueRange
    from ta.momentum import RSIIndicator
    TA_AVAILABLE = True
except Exception:
    TA_AVAILABLE = False

try:
    from sklearn.linear_model import SGDClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    import joblib
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Notex5_Advanced")

# -------- Configuration (tweak these) ----------
# Default symbol list (broker-flavored names for Exness where common suffix 'm' is used)
BROKER_SYMBOLS = {
    "EURUSD": "EURUSDm",
    "XAGUSD": "XAGUSDm",
    "XAUUSD": "XAUUSDm",
    "BTCUSD": "BTCUSDm",
    "USDJPY": "USDJPYm",
}
# Primary symbols to trade (will be mapped to broker symbols if MT5 connected)
SYMBOLS = list(BROKER_SYMBOLS.keys())

# Timeframes we will fetch and use
TIMEFRAMES = {"M30": "30m", "H1": "60m"}

# Risk & caps
BASE_RISK_PER_TRADE_PCT = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.01"))  # 1%
MIN_RISK_PER_TRADE_PCT = 0.002
MAX_RISK_PER_TRADE_PCT = 0.03
RISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT

MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "30"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "STOP_TRADING.flag")

# Adaptive / optimizer
ADAPT_STATE_FILE = "adapt_state.json"
MODEL_FILE = "notex5_model.joblib"
TRADES_DB = "trades.db"
TRADES_CSV = "trades.csv"
ADAPT_EVERY_CYCLES = 6
CURRENT_THRESHOLD = 0.20
MIN_THRESHOLD = 0.08
MAX_THRESHOLD = 0.45

# Model settings
USE_MODEL = SKLEARN_AVAILABLE
MODEL_MIN_TRAIN = 40
DECISION_SLEEP = int(os.getenv("DECISION_SLEEP", "60"))

# Meta
DEMO_SIMULATION = True
AUTO_EXECUTE = False

# MT5 credentials can be provided via env vars (optional)
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")

# ---------- persistence helpers ----------

def load_adapt_state():
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    if os.path.exists(ADAPT_EVERY_CYCLES and ADAPT_STATE_FILE or ADAPT_STATE_FILE):
        try:
            with open(ADAPT_STATE_FILE, "r") as f:
                st = json.load(f)
            CURRENT_THRESHOLD = float(st.get("threshold", CURRENT_THRESHOLD))
            RISK_PER_TRADE_PCT = float(st.get("risk", RISK_PER_TRADE_PCT))
            logger.info("Loaded adapt_state: threshold=%.3f risk=%.5f", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
        except Exception:
            logger.exception("Failed loading adapt_state")


def save_adapt_state():
    try:
        with open(ADAPT_STATE_FILE, "w") as f:
            json.dump({"threshold": CURRENT_THRESHOLD, "risk": RISK_PER_TRADE_PCT}, f)
    except Exception:
        logger.exception("Failed saving adapt_state")

load_adapt_state()

# ---------- DB / CSV logging ----------

def init_trade_db():
    conn = sqlite3.connect(TRADES_DB, timeout=5)
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
      pnl REAL,
      rmult REAL,
      regime TEXT,
      score REAL,
      model_score REAL,
      meta TEXT
    );
    """)
    conn.commit(); conn.close()
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w") as f:
            f.write("ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta\n")


def record_trade(...):
    # helper placeholder - the full function is in the canvas file
    pass

# ---------- MT5 helpers ----------
_mt5 = None
_mt5_connected = False


def connect_mt5(login: Optional[int] = None, password: Optional[str] = None, server: Optional[str] = None) -> bool:
    global _mt5, _mt5_connected
    if not MT5_LIB_AVAILABLE:
        logger.warning("MetaTrader5 library not available in environment")
        return False
    try:
        _mt5 = mt5
    except Exception:
        logger.exception("mt5 import issue")
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


def map_symbol_to_broker(requested: str) -> str:
    """Map a canonical symbol (e.g. XAUUSD) to broker symbol (XAUUSDm) using common heuristics and known mapping."""
    requested = str(requested).upper().strip()
    # if user supplied explicit mapping, prefer that
    if requested in BROKER_SYMBOLS:
        return BROKER_SYMBOLS[requested]
    # fallback heuristics
    if _mt5_connected and _mt5 is not None:
        try:
            syms = [s.name for s in _mt5.symbols_get()] if _mt5 is not None else []
            low_req = requested.lower()
            for b in syms:
                if b.lower() == low_req or b.lower().startswith(low_req) or b.lower().endswith(low_req):
                    return b
            # try adding 'm' suffix
            for suf in ["m", "M"]:
                cand = requested + suf
                for b in syms:
                    if b.lower() == cand.lower():
                        return b
        except Exception:
            logger.debug("map_symbol_to_broker fail")
    # final fallback: use the requested and hope MT5 accepts it
    return requested

# ---------- Data fetchers (MT5 preferred, yfinance fallback) ----------

def fetch_ohlcv_mt5(symbol: str, interval: str = "60m", period_days: int = 60):
    if not _mt5_connected or _mt5 is None:
        return None
    try:
        import pandas as pd
        broker_sym = map_symbol_to_broker(symbol)
        si = _mt5.symbol_info(broker_sym)
        if si is None:
            logger.debug("symbol not available on broker: %s", broker_sym)
            return None
        if not si.visible:
            _mt5.symbol_select(broker_sym, True)
        tf_map = {
            "1m": _mt5.TIMEFRAME_M1,
            "5m": _mt5.TIMEFRAME_M5,
            "15m": _mt5.TIMEFRAME_M15,
            "30m": _mt5.TIMEFRAME_M30,
            "60m": _mt5.TIMEFRAME_H1,
            "4h": _mt5.TIMEFRAME_H4,
            "1d": _mt5.TIMEFRAME_D1,
        }
        mt_tf = tf_map.get(interval, _mt5.TIMEFRAME_H1)
        # rough count
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
        logger.exception("MT5 fetch failed for %s", symbol)
        return None


def fetch_ohlcv_yf(symbol: str, interval: str = "60m", period_days: int = 60):
    if not YF_AVAILABLE:
        return None
    candidates = [symbol] + [f"{symbol}=X"]
    # include mapping table
    for t in [symbol] + candidates:
        try:
            df = yf.download(t, period=f"{period_days}d", interval=interval, progress=False)
            if df is None or df.empty:
                continue
            df = df.rename(columns={c: c.lower() for c in df.columns})
            colmap = {}
            for c in df.columns:
                lc = c.lower()
                if "open" in lc: colmap[c] = "open"
                if "high" in lc: colmap[c] = "high"
                if "low" in lc: colmap[c] = "low"
                if "close" in lc: colmap[c] = "close"
                if "volume" in lc: colmap[c] = "volume"
            if colmap:
                df = df.rename(columns=colmap)
            for c in ("open", "high", "low", "close", "volume"):
                if c not in df.columns:
                    df[c] = pd.NA
            df.index = pd.to_datetime(df.index)
            df = df[["open", "high", "low", "close", "volume"]].dropna(how="all")
            return df
        except Exception:
            continue
    return None


def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60):
    # prefer MT5
    if MT5_LIB_AVAILABLE and _mt5_connected:
        df = fetch_ohlcv_mt5(symbol, interval=interval, period_days=period_days)
        if df is not None:
            return df
    # fallback to yfinance
    return fetch_ohlcv_yf(symbol, interval=interval, period_days=period_days)


def fetch_multi_timeframes(symbol: str, period_days: int = 60):
    out = {}
    for label, intr in TIMEFRAMES.items():
        out[label] = fetch_ohlcv(symbol, interval=intr, period_days=period_days)
        time.sleep(0.05)
    return out

# ---------- Indicators & regime detection ----------

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
            tr1 = (df["high"] - df["low"]).abs()
            tr2 = (df["high"] - df["close"].shift()).abs()
            tr3 = (df["low"] - df["close"].shift()).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df["atr14"] = tr.rolling(14, min_periods=1).mean()
            df["adx"] = df["close"].diff().abs().rolling(14, min_periods=1).mean()
    except Exception:
        logger.exception("add_technical_indicators error")
    df = df.fillna(method="bfill").fillna(method="ffill").fillna(0.0)
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

# ---------- Scoring & aggregation ----------

def technical_signal_score(df: pd.DataFrame) -> float:
    try:
        if df is None or len(df) < 2:
            return 0.0
        latest = df.iloc[-1]
        prev = df.iloc[-2]
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
            logger.exception("aggregate failed for %s", label)
    if not techs:
        return {"tech": 0.0, "fund": 0.0, "sent": 0.0}
    s = sum(t * w for t, w in techs)
    w = sum(w for _, w in techs)
    return {"tech": float(s / w), "fund": 0.0, "sent": 0.0}

# ---------- Lightweight ML model (optional) ----------
model_pipe = None


def build_model():
    if not SKLEARN_AVAILABLE:
        return None
    pipe = Pipeline([("scaler", StandardScaler()), ("clf", SGDClassifier(loss="log", max_iter=1000, tol=1e-3, random_state=42))])
    return pipe


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
            logger.exception("Failed loading ML model")
    return None

if SKLEARN_AVAILABLE:
    load_model()

# ---------- Backtest & optimizer ----------

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
            high = float(df["high"].iloc[j])
            low = float(df["low"].iloc[j])
            if side == "BUY":
                if high >= tp:
                    r_mult = 2.0
                    break
                if low <= sl:
                    r_mult = -1.0
                    break
            else:
                if low <= tp:
                    r_mult = 2.0
                    break
                if high >= sl:
                    r_mult = -1.0
                    break
        trades.append(r_mult)
        if len(trades) >= max_trades:
            break
    n = len(trades)
    if n == 0:
        return {"n": 0, "net": 0.0, "avg_r": 0.0, "win": 0.0}
    net = sum(trades)
    avg = net / n
    win = sum(1 for t in trades if t > 0) / n
    return {"n": n, "net": net, "avg_r": avg, "win": win}


def light_optimizer(symbols, budget=12):
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    logger.info("Starting light optimizer (budget=%d)", budget)
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
    best = candidates[0]
    best_expect, best_thresh, best_risk = best
    baseline_stats = []
    for s in symbols:
        df = fetch_multi_timeframes(s, period_days=60).get("H1")
        if df is None or getattr(df, "empty", True):
            continue
        baseline_stats.append(simulate_strategy_on_series(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=120))
    base_n = sum(st["n"] for st in baseline_stats) or 1
    base_expect = sum(st["avg_r"] * st["n"] for st in baseline_stats) / base_n if baseline_stats else 0.0
    logger.info("Optimizer best_expect=%.4f base_expect=%.4f", best_expect, base_expect)
    if best_expect > base_expect + 0.02:
        step = 0.4
        CURRENT_THRESHOLD = float(max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD * (1 - step) + best_thresh * step)))
        RISK_PER_TRADE_PCT = float(max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, RISK_PER_TRADE_PCT * (1 - step) + best_risk * step)))
        save_adapt_state()
        logger.info("Optimizer applied new threshold=%.3f risk=%.5f", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
        return {"before": base_expect, "after": best_expect, "threshold": CURRENT_THRESHOLD, "risk": RISK_PER_TRADE_PCT}
    logger.info("Optimizer skipped applying (no meaningful improvement)")
    return None

# ---------- Decision & execution ----------
cycle_counter = 0


def compute_lots_from_risk(risk_pct, balance, entry_price, stop_price):
    try:
        risk_amount = balance * risk_pct
        pip_risk = abs(entry_price - stop_price)
        if pip_risk <= 0:
            return 0.01
        lots = risk_amount / (pip_risk * 100000)
        lots = max(0.01, round(lots, 2))
        return lots
    except Exception:
        return 0.01


def place_order_simulated(symbol, side, lots, entry, sl, tp, score, model_score, regime):
    # demo: record simulated open entry
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("INSERT INTO trades (ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), symbol, side, entry, sl, tp, lots, "sim_open", 0.0, 0.0, regime, score, model_score, json.dumps({})))
        conn.commit(); conn.close()
    except Exception:
        logger.exception("Failed record simulated order")
    return {"status": "sim_open"}


def place_order_mt5(symbol: str, action: str, lot: float, price: Optional[float], sl: Optional[float], tp: Optional[float]):
    if not MT5_LIB_AVAILABLE or not _mt5_connected:
        return {"status": "mt5_not_connected"}
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
            return {"status": "no_tick"}
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
            "comment": "Notex5 advanced",
            "type_time": _mt5.ORDER_TIME_GTC,
            "type_filling": _mt5.ORDER_FILLING_IOC,
        }
        res = _mt5.order_send(request)
        logger.info("MT5 order_send result: %s", res)
        return {"status": "sent", "result": str(res)}
    except Exception:
        logger.exception("MT5 order_send failed")
        return {"status": "error"}


def get_today_trade_count():
    today = date.today().isoformat()
    conn = sqlite3.connect(TRADES_DB, timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM trades WHERE ts >= ?", (today + "T00:00:00+00:00",))
    r = cur.fetchone(); conn.close()
    return int(r[0]) if r else 0


def make_decision_for_symbol(symbol: str, live: bool = False):
    global cycle_counter, model_pipe, CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    try:
        tfs = fetch_multi_timeframes(symbol, period_days=60)
        df_h1 = tfs.get("H1")
        if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 40:
            logger.info("Not enough H1 for %s", symbol)
            return None
        scores = aggregate_multi_tf_scores(tfs)
        tech_score = scores["tech"]
        model_score = 0.0
        if SKLEARN_AVAILABLE and model_pipe is not None:
            try:
                regime, vol, adx = detect_market_regime_from_h1(df_h1)
                entry = float(df_h1["close"].iloc[-1])
                atr = float(add_technical_indicators(df_h1)["atr14"].iloc[-1])
                dist = (atr * 1.25) / (entry if entry != 0 else 1.0)
                regime_code = 0 if regime == "normal" else (1 if regime == "quiet" else 2)
                X = np.array([[tech_score, regime_code, dist]])
                proba = model_pipe.predict_proba(X)[:, 1][0]
                model_score = float((proba - 0.5) * 2.0)
            except Exception:
                logger.exception("model predict error")
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
            regime, vol, adx = detect_market_regime_from_h1(df_h1)
            risk_pct = RISK_PER_TRADE_PCT
            if regime == "volatile":
                risk_pct = max(MIN_RISK_PER_TRADE_PCT, risk_pct * 0.6)
            elif regime == "quiet":
                risk_pct = min(MAX_RISK_PER_TRADE_PCT, risk_pct * 1.15)
            if os.path.exists(KILL_SWITCH_FILE):
                logger.info("Kill switch - skipping order for %s", symbol)
                return decision
            if get_today_trade_count() >= MAX_DAILY_TRADES and live:
                logger.info("Daily trade cap reached - skipping")
                return decision
            balance = float(os.getenv("FALLBACK_BALANCE", "650.0"))
            lots = compute_lots_from_risk(risk_pct, balance, entry, sl)
            if live and not DEMO_SIMULATION:
                res = place_order_mt5(symbol, final_signal, lots, None, sl, tp)
                record_trade_entry = res
            else:
                res = place_order_simulated(symbol, final_signal, lots, entry, sl, tp, tech_score, model_score, regime)
                record_trade_entry = res
            decision.update({"entry": entry, "sl": sl, "tp": tp, "lots": lots, "placed": record_trade_entry})
        else:
            logger.info("No confident signal for %s (agg=%.3f)", symbol, total_score)
        return decision
    except Exception:
        logger.exception("Decision failed for %s", symbol)
        return None

# ---------- Adapt & learning driver ----------

def adapt_and_optimize():
    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT
    try:
        recent = []
        try:
            conn = sqlite3.connect(TRADES_DB, timeout=5)
            cur = conn.cursor()
            cur.execute("SELECT pnl, rmult FROM trades ORDER BY id DESC LIMIT 200")
            recent = cur.fetchall()
            conn.close()
        except Exception:
            logger.debug("No trades available for adapt")
        vals = [r[0] for r in recent if r and r[0] is not None]
        n = len(vals)
        winrate = sum(1 for v in vals if v > 0) / n if n > 0 else 0.0
        logger.info("Adapt: recent winrate=%.3f (n=%d)", winrate, n)
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
        light_optimizer(SYMBOLS, budget=10)
        if SKLEARN_AVAILABLE:
            # retrain model if enough data
            try:
                train_model_from_trades()
            except Exception:
                logger.debug("train model failed")
    except Exception:
        logger.exception("adapt_and_optimize failed")

# ---------- Runner ----------
cycle_counter = 0

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
            logger.exception("run_cycle symbol fail %s", s)
    return results


def main_loop(live=False):
    logger.info("Starting loop (live=%s demo=%s) thr=%.3f risk=%.5f", live, DEMO_SIMULATION, CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)
    try:
        while True:
            run_cycle(live=live)
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        save_adapt_state()

# ---------- CLI helpers (backtest and live confirmation) ----------

def run_backtest():
    logger.info("Running backtest for symbols: %s", SYMBOLS)
    out = {}
    for s in SYMBOLS:
        df = fetch_multi_timeframes(s, period_days=365).get("H1")
        if df is None:
            logger.info("No H1 for %s - skipping", s)
            continue
        res = simulate_strategy_on_series(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=1000)
        logger.info("Backtest %s -> n=%d win=%.3f avg_r=%.3f", s, res["n"], res["win"], res["avg_r"])
        out[s] = res
    return out


def confirm_enable_live():
    # require explicit env var OR typed confirmation
    if os.getenv("CONFIRM_AUTO", "") == "I UNDERSTAND THE RISKS":
        return True
    prompt = "To enable LIVE trading type exactly: I UNDERSTAND THE RISKS (or set CONFIRM_AUTO env var).\nType now: "
    got = input(prompt).strip()
    return got == "I UNDERSTAND THE RISKS"

# ---------- Startup ----------

def setup_and_run(args):
    global DEMO_SIMULATION, AUTO_EXECUTE, SYMBOLS, _mt5_connected
    init_trade_db()
    # map canonical symbols to broker variants if MT5 connected
    if MT5_LIB_AVAILABLE and MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
        if connect_mt5():
            _mt5_connected = True
            logger.info("MT5 connected; mapping symbols to broker variants")
            # replace symbols list with broker keys (we keep canonical names internally)
            # user requested Exness XAUUSDm - ensure mapping present
            for k, v in BROKER_SYMBOLS.items():
                logger.info("%s -> %s", k, v)
    else:
        logger.info("MT5 not connected or credentials not provided; using yfinance fallback where available")
    # CLI modes
    if args.backtest:
        run_backtest()
        return
    if args.live:
        ok = confirm_enable_live()
        if not ok:
            logger.info("Live not enabled - exiting")
            return
        DEMO_SIMULATION = False
        AUTO_EXECUTE = True
    if args.loop:
        main_loop(live=not DEMO_SIMULATION)
    else:
        run_cycle(live=not DEMO_SIMULATION)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Notex5 Advanced - adaptive trading bot")
    parser.add_argument("--loop", action="store_true", help="Run continuous loop")
    parser.add_argument("--backtest", action="store_true", help="Run historical backtest and exit")
    parser.add_argument("--live", action="store_true", help="Attempt to enable live trading (requires explicit confirmation)")
    parser.add_argument("--symbols", nargs="*", help="Override symbols (canonical names) e.g. XAUUSD EURUSD")
    args = parser.parse_args()
    if args.symbols:
        SYMBOLS = args.symbols
    setup_and_run(args)
