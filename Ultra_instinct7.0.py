#!/usr/bin/env python3
"""
Ultra_instinct full bot — recreated with:
- RapidAPI (Ultimate Economic Calendar) as primary calendar
- MarketAux fallback
- NewsData-compatible headlines (NEWSDATA_KEY) as primary news
- Combined fundamentals: news_sentiment + calendar signals
- Economic calendar blocking with high-impact filter
- Per-symbol max-open-trades logic, MT5-first fallback
- Debug snapshot only on first cycle
- Robust order confirmation / recording / Telegram behavior preserved
- reconcile_closed_deals called at cycle start

Replace environment variables and run as before.
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
import traceback
import shutil
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

try:
    import numpy as np
    import pandas as pd
except Exception as e:
    raise RuntimeError("Please install numpy and pandas: pip install numpy pandas") from e

try:
    import requests
    FUNDAMENTAL_AVAILABLE = True
except Exception:
    FUNDAMENTAL_AVAILABLE = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
    _VADER = SentimentIntensityAnalyzer()
except Exception:
    VADER_AVAILABLE = False
    _VADER = None

try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except Exception:
    MT5_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Ultra_instinct")

# ---------------- CONFIG ----------------
SYMBOLS = ["EURUSD", "XAGUSD", "XAUUSD", "BTCUSD", "USDJPY", "USOIL"]
BROKER_SYMBOLS = {}
TIMEFRAMES = {"M30": "30m", "H1": "60m"}

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
MARKETAUX_KEY = os.getenv("MARKETAUX_KEY", "")
NEWSDATA_KEY = os.getenv("NEWSDATA_KEY", "")
ALPHAVANTAGE_KEY = os.getenv("ALPHAVANTAGE_KEY", "")
FINNHUB_KEY = os.getenv("FINNHUB_KEY", "")

BASE_RISK_PER_TRADE_PCT = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MIN_RISK_PER_TRADE_PCT = float(os.getenv("MIN_RISK_PER_TRADE_PCT", "0.002"))
MAX_RISK_PER_TRADE_PCT = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "0.01"))
RISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT

MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "100"))
TRADES_DB = "trades.db"
TRADES_CSV = "trades.csv"
ADAPT_STATE_FILE = "adapt_state.json"
KILL_SWITCH_FILE = "STOP_TRADING.flag"
DECISION_SLEEP = int(os.getenv("DECISION_SLEEP", "60"))
DAILY_CACHE_SECONDS = int(os.getenv("CALENDAR_CACHE_SECONDS", "900"))

TARGET_WINRATE = 0.525
K_ADAPT = 0.04
MAX_ADJUST = 0.01
ADAPT_MIN_TRADES = 40
ADAPT_EVERY = 6

MAX_OPEN_PER_SYMBOL_DEFAULT = 10
MAX_OPEN_PER_SYMBOL: Dict[str, int] = {
    "XAGUSD": 5,
    "XAUUSD": 5,
}

MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
MT5_PATH = os.getenv("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

_mt5 = None
_mt5_connected = False
_cycle_counter = 0
_debug_snapshot_shown = False

def backup_trade_files():
    try:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if os.path.exists(TRADES_CSV):
            shutil.copy(TRADES_CSV, f"backup_{TRADES_CSV}_{stamp}")
        if os.path.exists(TRADES_DB):
            shutil.copy(TRADES_DB, f"backup_{TRADES_DB}_{stamp}")
    except Exception:
        logger.exception("backup_trade_files failed")

def init_trade_db():
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
        if not cur.fetchone():
            cur.execute("""
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY,
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
            )""")
            conn.commit()
        conn.close()
    except Exception:
        logger.exception("init_trade_db failed")
    if not os.path.exists(TRADES_CSV):
        try:
            with open(TRADES_CSV, "w", encoding="utf-8") as f:
                f.write("ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta\n")
        except Exception:
            logger.exception("create trades.csv failed")

def record_trade(symbol, side, entry, sl, tp, lots, status="sim", pnl=0.0, rmult=0.0, regime="unknown", score=0.0, model_score=0.0, meta=None):
    ts = datetime.now(timezone.utc).isoformat()
    meta_json = json.dumps(meta or {})
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("INSERT INTO trades (ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ts, symbol, side, entry, sl, tp, lots, status, pnl, rmult, regime, score, model_score, meta_json))
        conn.commit(); conn.close()
    except Exception:
        logger.exception("record_trade db failed")
    try:
        with open(TRADES_CSV, "a", encoding="utf-8") as f:
            f.write(f"{ts},{symbol},{side},{entry},{sl},{tp},{lots},{status},{pnl},{rmult},{regime},{score},{model_score},{meta_json}\n")
    except Exception:
        logger.exception("record_trade csv failed")

def get_recent_trades(limit=200):
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT id,ts,symbol,side,pnl,rmult,regime,score,model_score FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

def connect_mt5(login: Optional[int] = None, password: Optional[str] = None, server: Optional[str] = None) -> bool:
    global _mt5, _mt5_connected
    if not MT5_AVAILABLE:
        logger.warning("MetaTrader5 package not installed")
        return False
    try:
        _mt5 = mt5
    except Exception:
        logger.exception("mt5 import failed")
        return False
    login = login or (int(MT5_LOGIN) if MT5_LOGIN and str(MT5_LOGIN).isdigit() else None)
    password = password or MT5_PASSWORD
    server = server or MT5_SERVER
    if login is None or password is None or server is None:
        logger.info("MT5 credentials missing; MT5 will not be used")
        return False
    try:
        ok = _mt5.initialize(login=login, password=password, server=server)
        if not ok:
            try:
                _mt5.shutdown()
            except Exception:
                pass
            ok2 = _mt5.initialize(login=login, password=password, server=server)
            if not ok2:
                _mt5_connected = False
                logger.error("MT5 initialize failed")
                return False
        _mt5_connected = True
        logger.info("MT5 initialized")
        return True
    except Exception:
        logger.exception("connect_mt5 error")
        _mt5_connected = False
        return False

def fetch_ohlcv_mt5(symbol: str, interval: str = "60m", period_days: int = 60):
    if not MT5_AVAILABLE or not _mt5_connected:
        return None
    try:
        broker = symbol
        if BROKER_SYMBOLS and symbol in BROKER_SYMBOLS:
            broker = BROKER_SYMBOLS[symbol]
        si = _mt5.symbol_info(broker)
        if si is None:
            try:
                _mt5.symbol_select(broker, True)
            except Exception:
                pass
            si = _mt5.symbol_info(broker)
            if si is None:
                return None
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
        rates = _mt5.copy_rates_from_pos(broker, mt_tf, 0, int(count))
        if rates is None:
            return None
        df = pd.DataFrame(rates)
        if "time" in df.columns:
            df.index = pd.to_datetime(df["time"], unit="s")
        if "open" not in df.columns and "open_price" in df.columns:
            df["open"] = df["open_price"]
        if "tick_volume" in df.columns:
            df["volume"] = df["tick_volume"]
        df = df[["open", "high", "low", "close", "volume"]].dropna(how="all")
        return df
    except Exception:
        logger.exception("fetch_ohlcv_mt5 error")
        return None

def fetch_ohlcv(symbol: str, interval: str = "60m", period_days: int = 60):
    df = fetch_ohlcv_mt5(symbol, interval=interval, period_days=period_days)
    if df is None or df.empty:
        return None
    return df

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df
    try:
        df["sma5"] = df["close"].rolling(5, min_periods=1).mean()
        df["sma20"] = df["close"].rolling(20, min_periods=1).mean()
        delta = df["close"].diff()
        up = delta.clip(lower=0.0).rolling(14, min_periods=1).mean()
        down = -delta.clip(upper=0.0).rolling(14, min_periods=1).mean().replace(0, 1e-9)
        rs = up / down
        df["rsi14"] = 100 - (100 / (1 + rs))
        tr = pd.concat([(df["high"] - df["low"]).abs(), (df["high"] - df["close"].shift()).abs(), (df["low"] - df["close"].shift()).abs()], axis=1).max(axis=1)
        df["atr14"] = tr.rolling(14, min_periods=1).mean()
        df = df.bfill().ffill().fillna(0.0)
    except Exception:
        logger.exception("add_technical_indicators error")
    return df

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
        if df is None or getattr(df, "empty", True):
            continue
        d = add_technical_indicators(df)
        t = technical_signal_score(d)
        weight = {"M30": 1.8, "H1": 1.2}.get(label, 1.0)
        techs.append((t, weight))
    if not techs:
        return {"tech": 0.0, "fund": 0.0, "sent": 0.0}
    s = sum(t * w for t, w in techs); w = sum(w for _, w in techs)
    return {"tech": float(s / w), "fund": 0.0, "sent": 0.0}

_POS_WORDS = {"gain", "rise", "surge", "up", "positive", "bull", "beats", "beat", "record", "rally", "higher", "recover"}
_NEG_WORDS = {"fall", "drop", "down", "loss", "negative", "bear", "miss", "misses", "crash", "decline", "lower", "plunge", "attack", "strike"}
_RISK_KEYWORDS = {"iran", "strike", "war", "missile", "oil", "sanction", "attack", "drone", "escalat", "hormuz"}

_news_cache = {"ts": 0, "data": {}}

def _vader_score(text: str) -> float:
    if VADER_AVAILABLE and _VADER is not None:
        try:
            s = _VADER.polarity_scores(text or "")
            return float(s.get("compound", 0.0))
        except Exception:
            return 0.0
    txt = (text or "").lower()
    p = sum(1 for w in _POS_WORDS if w in txt)
    n = sum(1 for w in _NEG_WORDS if w in txt)
    denom = max(1.0, len(txt.split()))
    return max(-1.0, min(1.0, (p - n) / denom))

# ---------------- Robust NewsData fetch (fixed: no page_size param on /1/news) ----------------
def fetch_newsdata(q: str, pagesize: int = 50, max_pages: int = 2):
    """
    NewsData (newsdata.io) fetcher updated to avoid unsupported 'page_size' param.
    - Uses 'page' parameter only (provider controls page size).
    - Tries up to max_pages pages (if provider supports).
    - Falls back to MarketAux if NewsData returns no useful articles.
    Returns {'count': int, 'articles': [ {title,description,source,publishedAt,raw}, ... ]}.
    """
    if not FUNDAMENTAL_AVAILABLE:
        return {"count": 0, "articles": []}

    key = NEWSDATA_KEY or ""
    q = q or ""
    out_articles = []

    # Defensive: if no key, early return empty
    if not key:
        logger.debug("fetch_newsdata: NEWSDATA_KEY missing")
    else:
        try:
            base = "https://newsdata.io/api/1/news"
            for page in range(1, max_pages + 1):
                # NOTE: do NOT send page_size if the endpoint rejects it
                params = {"q": q, "language": "en", "page": page, "apikey": key}
                r = requests.get(base, params=params, timeout=10)
                # handle auth / rate limits / errors
                if r.status_code in (401, 403):
                    logger.warning("fetch_newsdata: auth error %s %s", r.status_code, r.text[:200])
                    break
                if r.status_code == 429:
                    logger.warning("fetch_newsdata: rate limited (429) - headers: %s", r.headers)
                    break
                # provider-specific validation error 422 -> unsupported param or bad query
                if r.status_code == 422:
                    logger.warning("fetch_newsdata: unsupported parameter or bad request: %s", r.text[:200])
                    # stop trying NewsData and fallback
                    break
                if r.status_code != 200:
                    logger.debug("fetch_newsdata: non-200 %s %s", r.status_code, r.text[:200])
                    break
                j = r.json()
                # NewsData commonly returns "results" (dict) or "results" as dict; be defensive
                items = j.get("results") or j.get("articles") or j.get("data") or j.get("news") or []
                # If results is not a list, try to find a nested list
                if isinstance(items, dict):
                    for k in ("results", "articles", "data", "news"):
                        if k in items and isinstance(items[k], list):
                            items = items[k]
                            break
                if not isinstance(items, list):
                    # nothing useful - stop paging
                    break
                for a in items:
                    try:
                        out_articles.append({
                            "title": a.get("title"),
                            "description": a.get("description") or a.get("summary") or "",
                            "source": a.get("source_id") or (a.get("source") and (a.get("source").get("name") if isinstance(a.get("source"), dict) else a.get("source"))) or "",
                            "publishedAt": a.get("pubDate") or a.get("publishedAt") or a.get("published_at") or "",
                            "raw": a
                        })
                    except Exception:
                        continue
                # if provider seems to return small pages, stop early to avoid useless calls
                if len(items) < 1:
                    break
            if out_articles:
                return {"count": len(out_articles), "articles": out_articles}
        except Exception:
            logger.exception("fetch_newsdata: NewsData call failed")

    # Fallback: MarketAux (if available)
    if MARKETAUX_KEY:
        try:
            url = "https://api.marketaux.com/v1/news/all"
            params = {"api_token": MARKETAUX_KEY, "q": q, "language": "en", "limit": pagesize}
            r = requests.get(url, params=params, timeout=8)
            if r.status_code == 200:
                j = r.json()
                items = j.get("data") or j.get("results") or j.get("articles") or []
                processed = []
                for a in items[:pagesize]:
                    processed.append({
                        "title": a.get("title"),
                        "description": a.get("description"),
                        "source": a.get("source_name") or a.get("source"),
                        "publishedAt": a.get("published_at") or a.get("publishedAt"),
                        "raw": a
                    })
                if processed:
                    return {"count": len(processed), "articles": processed}
        except Exception:
            logger.exception("fetch_newsdata fallback MarketAux failed")

    return {"count": 0, "articles": []}

# ---------------- RapidAPI Ultimate Economic Calendar (primary calendar) ----------------
_RAPIDAPI_CAL_CACHE = {"ts": 0, "data": []}

def _parse_iso_utc(s: str):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def fetch_rapidapi_tradingview_events(from_date: str, to_date: str, countries: str = None, cache_seconds: int = DAILY_CACHE_SECONDS) -> List[Dict[str, Any]]:
    now_ts = time.time()
    if now_ts - _RAPIDAPI_CAL_CACHE.get("ts", 0) < cache_seconds and _RAPIDAPI_CAL_CACHE.get("data"):
        return _RAPIDAPI_CAL_CACHE["data"]
    out = []
    if not RAPIDAPI_KEY:
        return out
    url = "https://ultimate-economic-calendar.p.rapidapi.com/economic-events/tradingview"
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": "ultimate-economic-calendar.p.rapidapi.com"}
    params = {"from": from_date, "to": to_date}
    if countries:
        params["countries"] = countries
    try:
        r = requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code != 200:
            logger.debug("RapidAPI calendar non-200: %s %s", r.status_code, r.text[:200])
            return out
        j = r.json()
        raw_list = j.get("result") or j.get("data") or j or []
        if isinstance(raw_list, dict):
            for k in ("result", "data", "events"):
                if k in raw_list and isinstance(raw_list[k], list):
                    raw_list = raw_list[k]; break
        now_dt = datetime.now(timezone.utc)
        for e in raw_list:
            try:
                dt = _parse_iso_utc(e.get("date") or "")
                if dt is None:
                    continue
                minutes_to = (dt - now_dt).total_seconds() / 60.0
                imp = None
                try:
                    imp = int(e.get("importance")) if e.get("importance") is not None else None
                except Exception:
                    imp = None
                out.append({
                    "date": dt,
                    "country": (e.get("country") or "").upper(),
                    "indicator": e.get("indicator") or e.get("title") or "",
                    "title": e.get("title") or "",
                    "actual": e.get("actual"),
                    "forecast": e.get("forecast"),
                    "previous": e.get("previous"),
                    "importance": imp,
                    "minutes_to": float(minutes_to),
                    "raw": e
                })
            except Exception:
                continue
        _RAPIDAPI_CAL_CACHE["ts"] = time.time()
        _RAPIDAPI_CAL_CACHE["data"] = out
        return out
    except Exception:
        logger.exception("fetch_rapidapi_tradingview_events failed")
        return out

def fetch_marketaux_calendar(days_before=1, days_after=2):
    if not MARKETAUX_KEY or not FUNDAMENTAL_AVAILABLE:
        return []
    try:
        start = (datetime.utcnow().date() - timedelta(days=days_before)).strftime("%Y-%m-%d")
        end = (datetime.utcnow().date() + timedelta(days=days_after)).strftime("%Y-%m-%d")
        url = "https://api.marketaux.com/v1/economic/calendar"
        params = {"api_token": MARKETAUX_KEY, "start_date": start, "end_date": end}
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return []
        j = r.json()
        raw = j.get("data") or j.get("result") or j or []
        out = []
        now_dt = datetime.now(timezone.utc)
        for e in raw:
            try:
                dt = _parse_iso_utc(e.get("date") or "")
                if dt is None:
                    continue
                minutes_to = (dt - now_dt).total_seconds() / 60.0
                imp = None
                try:
                    imp = int(e.get("importance")) if e.get("importance") is not None else None
                except Exception:
                    imp = None
                out.append({
                    "date": dt,
                    "country": (e.get("country") or "").upper(),
                    "indicator": e.get("indicator") or e.get("title") or "",
                    "title": e.get("title") or "",
                    "actual": e.get("actual"),
                    "forecast": e.get("forecast"),
                    "previous": e.get("previous"),
                    "importance": imp,
                    "minutes_to": float(minutes_to),
                    "raw": e
                })
            except Exception:
                continue
        return out
    except Exception:
        logger.exception("fetch_marketaux_calendar failed")
        return []

def _symbol_to_relevant_tokens(symbol: str) -> List[str]:
    s = symbol.upper()
    tokens = []
    if s.startswith("EUR"):
        tokens += ["eur", "euro", "eurozone", "de", "fr", "eu"]
    elif s.startswith("USD"):
        tokens += ["usd", "us", "america", "united states"]
    elif s.startswith("XAU") or "GOLD" in s:
        tokens += ["gold", "xau"]
    elif s.startswith("XAG") or "SILVER" in s:
        tokens += ["silver", "xag"]
    elif s.startswith("BTC"):
        tokens += ["btc", "bitcoin", "crypto", "crypto market", "global"]
    else:
        tokens += [s[:3].lower(), s[3:6].lower()]
    return list(set(tokens))

def should_pause_for_events(symbol: str, lookahead_minutes: int = 30) -> (bool, Optional[Dict[str, Any]]):
    try:
        now = datetime.utcnow().date()
        from_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        to_date = (now + timedelta(days=2)).strftime("%Y-%m-%d")
        events = fetch_rapidapi_tradingview_events(from_date, to_date, countries=None, cache_seconds=DAILY_CACHE_SECONDS)
        if not events:
            events = fetch_marketaux_calendar(days_before=1, days_after=2)
        if not events and FINNHUB_KEY:
            try:
                fh_url = f"https://finnhub.io/api/v1/calendar/economic?from={from_date}&to={to_date}&token={FINNHUB_KEY}"
                r = requests.get(fh_url, timeout=8)
                if r.status_code == 200:
                    j = r.json()
                    raw = j.get("economicCalendar") or j.get("data") or j or []
                    evs = []
                    now_dt = datetime.now(timezone.utc)
                    for e in raw:
                        try:
                            dt = _parse_iso_utc(e.get("date") or e.get("dateTime") or "")
                            if dt is None:
                                continue
                            minutes_to = (dt - now_dt).total_seconds() / 60.0
                            imp = None
                            try:
                                imp = int(e.get("importance")) if e.get("importance") is not None else None
                            except Exception:
                                imp = None
                            evs.append({"date": dt, "country": (e.get("country") or "").upper(), "indicator": e.get("event") or e.get("name") or "", "title": e.get("event") or e.get("name") or "", "importance": imp, "minutes_to": float(minutes_to), "raw": e})
                        except Exception:
                            continue
                    events = evs
            except Exception:
                logger.exception("finnhub fallback failed")
        if not events:
            return False, None
        tokens = _symbol_to_relevant_tokens(symbol)
        now_dt = datetime.now(timezone.utc)
        for e in events:
            try:
                minutes_to = float(e.get("minutes_to", 999999))
                if minutes_to < 0 or minutes_to > lookahead_minutes:
                    continue
                imp = e.get("importance")
                is_high = False
                try:
                    if imp is not None and int(imp) >= 1:
                        is_high = True
                except Exception:
                    is_high = False
                txt = (str(e.get("indicator", "") or "") + " " + str(e.get("title", "") or "")).lower()
                risk_hit = any(k in txt for k in _RISK_KEYWORDS)
                if is_high or risk_hit:
                    for t in tokens:
                        if t and (t.lower() in txt or t.upper() == (e.get("country") or "").upper()):
                            return True, {"event": e.get("indicator") or e.get("title"), "minutes_to": minutes_to, "importance": imp, "raw": e.get("raw")}
            except Exception:
                logger.exception("should_pause_for_events loop error")
                continue
        return False, None
    except Exception:
        logger.exception("should_pause_for_events failed")
        return False, None

def fetch_fundamental_score(symbol: str, lookback_days: int = 2) -> Dict[str, Any]:
    news_sent = 0.0
    calendar_signal = 0.0
    details = {"news_hits": 0, "calendar_event": None}
    try:
        s = symbol.upper()
        qterms = []
        if s.startswith("XAU") or "GOLD" in s:
            qterms.append("gold")
        elif s.startswith("XAG") or "SILVER" in s:
            qterms.append("silver")
        elif s.startswith("BTC"):
            qterms.append("bitcoin")
        elif s in ("USOIL", "OIL", "WTI", "BRENT"):
            qterms.append("oil")
        else:
            qterms.append(symbol)
        qterms += list(_RISK_KEYWORDS)
        q = " OR ".join(list(set(qterms)))
        news = fetch_newsdata(q, pagesize=30)
        arts = news.get("articles", []) if isinstance(news, dict) else []
        details["news_count"] = len(arts)
        if arts:
            scores = []
            hits = 0
            for a in arts:
                txt = (str(a.get("title") or "") + " " + str(a.get("description") or "")).strip()
                sscore = _vader_score(txt)
                scores.append(sscore)
                hits += sum(1 for k in _RISK_KEYWORDS if k in txt.lower())
            avg = float(sum(scores) / max(1, len(scores)))
            if hits >= 2:
                avg = max(-1.0, min(1.0, avg - 0.2 * min(3, hits)))
            news_sent = float(max(-1.0, min(1.0, avg)))
            details["news_sentiment"] = news_sent
            details["news_hits"] = hits
        else:
            news_sent = 0.0
            details["news_sentiment"] = 0.0
    except Exception:
        logger.exception("fetch_fundamental_score news step failed")
        news_sent = 0.0

    try:
        pause, ev = should_pause_for_events(symbol, lookahead_minutes=60)
        if pause:
            calendar_signal = -1.0
            details["calendar_event"] = ev
        else:
            calendar_signal = 0.0
    except Exception:
        calendar_signal = 0.0

    crypto_shock = 0.0
    if symbol.upper().startswith("BTC"):
        try:
            crypto_shock = 0.0
        except Exception:
            crypto_shock = 0.0

    combined = 0.6 * news_sent + 0.3 * 0.0 + 0.1 * crypto_shock
    if calendar_signal < 0:
        combined = min(combined, -0.6)
    combined = max(-1.0, min(1.0, combined))
    return {"combined": float(combined), "news_sentiment": float(news_sent), "calendar_signal": float(calendar_signal), "details": details}

def reconcile_closed_deals(lookback_seconds: int = 3600 * 24):
    if not MT5_AVAILABLE or not _mt5_connected:
        return 0
    updated = 0
    now_utc = datetime.utcnow()
    since = now_utc - timedelta(seconds=lookback_seconds)
    try:
        deals = _mt5.history_deals_get(since, now_utc)
        if not deals:
            return 0
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        for d in deals:
            try:
                dsym = str(getattr(d, "symbol", "") or "").strip()
                dvol = float(getattr(d, "volume", 0.0) or 0.0)
                dprofit = float(getattr(d, "profit", 0.0) or 0.0)
                cur.execute("SELECT id,lots,ts FROM trades WHERE symbol=? AND (pnl IS NULL OR pnl=0 OR pnl='0') ORDER BY ts ASC LIMIT 8", (dsym,))
                rows = cur.fetchall()
                if not rows:
                    continue
                best = None
                best_diff = None
                for row in rows:
                    tid, tlots, tts = row
                    try:
                        tl = float(tlots or 0.0)
                    except Exception:
                        tl = 0.0
                    diff = abs(tl - dvol)
                    if best is None or diff < best_diff:
                        best = (tid, tl)
                        best_diff = diff
                if best is None:
                    continue
                tid, tl = best
                rel_tol = 1e-2
                accept = (tl <= 0) or (abs(tl - dvol) <= max(1e-6, rel_tol * max(abs(tl), abs(dvol), 1.0)))
                if not accept:
                    continue
                new_status = "closed"
                if dprofit > 0:
                    new_status = "closed_win"
                elif dprofit < 0:
                    new_status = "closed_loss"
                deal_meta = {"profit": dprofit, "volume": dvol, "symbol": dsym, "time": getattr(d, "time", None)}
                try:
                    cur.execute("UPDATE trades SET pnl = ?, status = ?, meta = COALESCE(meta,'') || ? WHERE id = ?", (float(dprofit), new_status, f" | deal_meta:{json.dumps(deal_meta)}", int(tid)))
                    conn.commit()
                    updated += 1
                except Exception:
                    logger.exception("reconcile update failed for id %s", tid)
            except Exception:
                logger.exception("processing deal failed")
        conn.close()
    except Exception:
        logger.exception("reconcile_closed_deals failed")
    if updated:
        logger.info("reconcile_closed_deals: updated %d trades", updated)
    return updated

def get_open_positions_count(symbol: str) -> int:
    if MT5_AVAILABLE and _mt5_connected:
        try:
            broker = symbol
            if symbol in BROKER_SYMBOLS:
                broker = BROKER_SYMBOLS[symbol]
            positions = _mt5.positions_get(symbol=broker)
            if not positions:
                return 0
            cnt = 0
            for p in positions:
                try:
                    if getattr(p, "symbol", "").lower() == broker.lower():
                        vol = float(getattr(p, "volume", 0.0) or 0.0)
                        if vol > 0:
                            cnt += 1
                except Exception:
                    continue
            return int(cnt)
        except Exception:
            logger.debug("positions_get failed, falling back to DB")
    try:
        conn = sqlite3.connect(TRADES_DB, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades WHERE symbol=? AND status IN ('sent','sim_open','open','placed')", (symbol,))
        row = cur.fetchone()
        conn.close()
        if row:
            return int(row[0])
    except Exception:
        logger.exception("get_open_positions_count DB fallback failed")
    return 0

def get_max_open_for_symbol(symbol: str) -> int:
    key = symbol.upper()
    if key in MAX_OPEN_PER_SYMBOL:
        return int(MAX_OPEN_PER_SYMBOL[key])
    for k, v in MAX_OPEN_PER_SYMBOL.items():
        if key.startswith(k):
            return int(v)
    return int(MAX_OPEN_PER_SYMBOL_DEFAULT)

def place_order_simulated(symbol, side, lots, entry, sl, tp, score=0.0, model_score=0.0, regime="unknown"):
    record_trade(symbol, side, entry, sl, tp, lots, status="sim_open", pnl=0.0, rmult=0.0, regime=regime, score=score, model_score=model_score)
    return {"status": "sim_open"}

def place_order_mt5(symbol, action, lot, price, sl, tp):
    if not MT5_AVAILABLE or not _mt5_connected:
        return {"status": "mt5_not_connected"}
    try:
        broker = symbol
        if symbol in BROKER_SYMBOLS:
            broker = BROKER_SYMBOLS[symbol]
        si = _mt5.symbol_info(broker)
        if si is None:
            return {"status": "symbol_not_found", "symbol": broker}
        if not si.visible:
            try:
                _mt5.symbol_select(broker, True)
            except Exception:
                pass
        tick = _mt5.symbol_info_tick(broker)
        if tick is None:
            return {"status": "no_tick"}
        vol_min = getattr(si, "volume_min", 0.01) or 0.01
        vol_step = getattr(si, "volume_step", 0.01) or 0.01
        try:
            lots = float(lot)
        except Exception:
            lots = float(vol_min)
        if vol_step > 0:
            steps = max(0, int((lots - vol_min) // vol_step))
            lots_adj = vol_min + steps * vol_step
            if lots > lots_adj:
                lots_adj = vol_min + int(((lots - vol_min) + vol_step - 1e-12) // vol_step) * vol_step
            lots = round(float(max(vol_min, lots_adj)), 2)
        order_price = price if price is not None else (tick.ask if action == "BUY" else tick.bid)
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
        return {"status": "sent", "result": str(res)}
    except Exception:
        logger.exception("place_order_mt5 failed")
        return {"status": "error"}

def make_decision_for_symbol(symbol: str, live: bool = False):
    global _debug_snapshot_shown, _cycle_counter
    try:
        tfs = {}
        for label, intr in TIMEFRAMES.items():
            tfs[label] = fetch_ohlcv(symbol, interval=intr, period_days=60)
        df_h1 = tfs.get("H1")
        if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 40:
            logger.info("Not enough H1 data for %s - skipping", symbol)
            return None
        scores = aggregate_multi_tf_scores(tfs)
        tech_score = scores["tech"]
        model_score = 0.0
        try:
            fund = fetch_fundamental_score(symbol, lookback_days=2)
            fundamental_score = fund.get("combined", 0.0)
            news_sentiment = fund.get("news_sentiment", 0.0)
        except Exception:
            fundamental_score = 0.0
            news_sentiment = 0.0
        weights = {"tech": 0.45, "model": 0.25, "fund": 0.30}
        total_score = (weights["tech"] * tech_score) + (weights["fund"] * fundamental_score) + (weights["model"] * model_score)
        try:
            total_score = float(total_score)
            if total_score != total_score:
                total_score = 0.0
            total_score = max(-1.0, min(1.0, total_score))
        except Exception:
            total_score = max(-1.0, min(1.0, float(total_score if total_score is not None else 0.0)))
        final_signal = None
        if total_score >= 0.18:
            final_signal = "BUY"
        elif total_score <= -0.18:
            final_signal = "SELL"
        decision = {"symbol": symbol, "agg": total_score, "tech": tech_score, "fund": fundamental_score, "news_sentiment": news_sentiment, "final": final_signal}
        try:
            pause, ev = should_pause_for_events(symbol, lookahead_minutes=60)
            if pause:
                logger.info("Pausing trading for %s due to event: %s", symbol, ev)
                decision["paused"] = True
                decision["pause_event"] = ev
                return decision
        except Exception:
            logger.exception("calendar check failed")
        if final_signal:
            entry = float(df_h1["close"].iloc[-1])
            atr = float(add_technical_indicators(df_h1)["atr14"].iloc[-1] or 0.0)
            stop_dist = max(1e-8, atr * 1.25)
            if final_signal == "BUY":
                sl = entry - stop_dist; tp = entry + stop_dist * 2.0
            else:
                sl = entry + stop_dist; tp = entry - stop_dist * 2.0
            try:
                open_count = get_open_positions_count(symbol)
                max_open = get_max_open_for_symbol(symbol)
                if open_count >= max_open:
                    logger.info("Max open positions for %s reached (%d/%d) - skipping", symbol, open_count, max_open)
                    return decision
            except Exception:
                logger.exception("open positions check failed")
            balance = float(os.getenv("FALLBACK_BALANCE", "650.0"))
            risk_pct = RISK_PER_TRADE_PCT
            lots = max(0.01, round((balance * risk_pct) / max(1e-6, abs(entry - sl)) / 100000.0, 2))
            if live and MT5_AVAILABLE and _mt5_connected:
                res = place_order_mt5(symbol, final_signal, lots, None, sl, tp)
                status = None; retcode = None
                try:
                    if isinstance(res, dict):
                        status = str(res.get("status", "")).lower()
                        try:
                            retcode = int(res.get("retcode")) if "retcode" in res and res.get("retcode") is not None else None
                        except Exception:
                            retcode = None
                    else:
                        status = str(getattr(res, "status", "")).lower() if res is not None else None
                        try:
                            retcode = int(getattr(res, "retcode", None))
                        except Exception:
                            retcode = None
                except Exception:
                    status = str(res).lower() if res is not None else ""
                    retcode = None
                confirmed = False
                if retcode == 0 or status == "sent":
                    confirmed = True
                if not confirmed and MT5_AVAILABLE and _mt5_connected:
                    try:
                        time.sleep(0.6)
                        broker = symbol
                        if symbol in BROKER_SYMBOLS:
                            broker = BROKER_SYMBOLS[symbol]
                        try:
                            positions = _mt5.positions_get(symbol=broker)
                            if positions:
                                for p in positions:
                                    try:
                                        if getattr(p, "symbol", "").lower() == broker.lower():
                                            pv = float(getattr(p, "volume", 0.0) or 0.0)
                                            if abs(pv - float(lots)) <= (0.0001 * max(1.0, float(lots))):
                                                confirmed = True
                                                break
                                    except Exception:
                                        continue
                        except Exception:
                            pass
                        if not confirmed:
                            now_utc = datetime.utcnow()
                            since = now_utc - timedelta(seconds=90)
                            try:
                                deals = _mt5.history_deals_get(since, now_utc)
                                if deals:
                                    for d in deals:
                                        try:
                                            dsym = getattr(d, "symbol", "") or ""
                                            dvol = float(getattr(d, "volume", 0.0) or 0.0)
                                            if dsym.lower() == broker.lower() and abs(dvol - float(lots)) <= (0.0001 * max(1.0, float(lots))):
                                                confirmed = True
                                                break
                                        except Exception:
                                            continue
                            except Exception:
                                pass
                    except Exception:
                        logger.exception("Order confirmation probe failed")
                try:
                    if confirmed:
                        rec_status = res.get("status", "sent") if isinstance(res, dict) else "sent"
                        record_trade(symbol, final_signal, entry, sl, tp, lots, status=rec_status, pnl=0.0, rmult=0.0, regime="auto", score=tech_score, model_score=model_score, meta=res)
                        try:
                            entry_s = f"{float(entry):.2f}"
                            sl_s = f"{float(sl):.2f}"
                            tp_s = f"{float(tp):.2f}"
                        except Exception:
                            entry_s, sl_s, tp_s = str(entry), str(sl), str(tp)
                        msg = (f"Ultra_instinct signal\n✅ EXECUTED\n{final_signal} {symbol}\nLots: {lots}\nEntry: {entry_s}\nSL: {sl_s}\nTP: {tp_s}")
                        send_telegram_message(msg)
                    else:
                        try:
                            with open("rejected_orders.log", "a", encoding="utf-8") as rf:
                                rf.write(f"{datetime.now(timezone.utc).isoformat()} | {symbol} | {final_signal} | lots={lots} | status={status} | retcode={retcode} | meta={json.dumps(res)}\n")
                        except Exception:
                            logger.exception("Failed write rejected_orders.log")
                        try:
                            entry_s = f"{float(entry):.2f}"
                            sl_s = f"{float(sl):.2f}"
                            tp_s = f"{float(tp):.2f}"
                        except Exception:
                            entry_s, sl_s, tp_s = str(entry), str(sl), str(tp)
                        msg = (f"Ultra_instinct signal\n❌ REJECTED\n{final_signal} {symbol}\nLots: {lots}\nEntry: {entry_s}\nSL: {sl_s}\nTP: {tp_s}\nReason: {status or retcode}")
                        send_telegram_message(msg)
                except Exception:
                    logger.exception("Post-order handling failed")
            else:
                res = place_order_simulated(symbol, final_signal, lots, entry, sl, tp, score=tech_score, model_score=model_score, regime="auto")
                decision.update({"placed": res, "entry": entry, "sl": sl, "tp": tp, "lots": lots})
        else:
            logger.debug("No confident signal for %s (agg=%.3f)", symbol, total_score)
        try:
            global _debug_snapshot_shown
            if not _debug_snapshot_shown:
                logger.info("DEBUG_EXEC -> sym=%s agg=%.5f final=%s thr_cache=%s", symbol, float(total_score), str(final_signal), DAILY_CACHE_SECONDS)
                _debug_snapshot_shown = True
        except Exception:
            pass
        return decision
    except Exception:
        logger.exception("make_decision_for_symbol failed for %s", symbol)
        return None

def adapt_and_optimize():
    global RISK_PER_TRADE_PCT
    try:
        recent = get_recent_trades(limit=200)
        vals = [r[4] for r in recent if r and r[4] is not None]
        n = len(vals)
        winrate = sum(1 for v in vals if v > 0) / n if n > 0 else 0.0
        logger.info("Adapt: recent winrate=%.3f n=%d", winrate, n)
        if n >= ADAPT_MIN_TRADES:
            adj = -K_ADAPT * (winrate - TARGET_WINRATE)
            if adj > MAX_ADJUST:
                adj = MAX_ADJUST
            elif adj < -MAX_ADJUST:
                adj = -MAX_ADJUST
        vols = []
        for s in SYMBOLS:
            df = fetch_ohlcv(s, interval="60m", period_days=45)
            if df is None:
                continue
            try:
                rel = float(df["close"].pct_change().std() or 0.0)
            except Exception:
                rel = 0.0
            vols.append(rel or 0.0)
        if vols:
            avg_vol = sum(vols) / len(vols)
            target = 0.003
            scale = target / avg_vol if avg_vol else 1.0
            scale = max(0.6, min(1.6, scale))
            new_risk = BASE_RISK_PER_TRADE_PCT * scale
            if n >= 20 and sum(vals) < 0:
                new_risk *= 0.7
            RISK_PER_TRADE_PCT = float(max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, new_risk)))
    except Exception:
        logger.exception("adapt_and_optimize failed")

def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured")
        return False
    if not FUNDAMENTAL_AVAILABLE:
        logger.debug("requests not available for Telegram")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=8)
        if r.status_code == 200:
            return True
        else:
            logger.warning("send_telegram_message non-200 %s %s", r.status_code, r.text[:200])
            return False
    except Exception:
        logger.exception("send_telegram_message failed")
        return False

def run_cycle(live: bool = False):
    global _cycle_counter
    try:
        reconcile_closed_deals(lookback_seconds=3600 * 24)
    except Exception:
        logger.exception("reconcile_closed_deals call failed at cycle start")
    _cycle_counter += 1
    if _cycle_counter % ADAPT_EVERY == 0:
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

def main_loop(live: bool = False):
    logger.info("Starting main loop (live=%s)", live)
    try:
        while True:
            run_cycle(live=live)
            time.sleep(DECISION_SLEEP)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        pass

def setup_and_run(args):
    backup_trade_files()
    init_trade_db()
    if MT5_AVAILABLE and MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
        try:
            connect_mt5()
        except Exception:
            pass
    if args.loop:
        main_loop(live=not os.getenv("DEMO_SIMULATION", "1") == "1")
    else:
        run_cycle(live=not os.getenv("DEMO_SIMULATION", "1") == "1")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    setup_and_run(args)
