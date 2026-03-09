# ===== FULL QUANT NEWS + TELEGRAM UPGRADE (paste entire block as upgrade) =====

# === BEGIN UPGRADE MODULE ===
import os
import re
import time
import math
import json
import threading
import logging
from collections import deque, defaultdict

# Optional: external HTTP client
try:
    import requests
except Exception:
    requests = None

# --- Configuration (from env) ---
# API keys (optional). If missing, fetchers are skipped gracefully.
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
NEWDATA_KEY  = os.getenv("NEWDATA_KEY", "").strip()

# Telegram config
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or 0)
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
TELEGRAM_CHANNELS = [c.strip() for c in os.getenv("TELEGRAM_CHANNELS", "cryptomoneyHQ,TradingNewsIO").split(",") if c.strip()]

# Files & DB
NEWS_JSONL = os.getenv("BEAST_NEWS_JSONL", "beast_news_queue.jsonl")  # file bridge for bot
TRADES_DB = os.getenv("TRADES_DB", "dashboard.db")  # used for winrate check (can be your trades DB)
NEWS_POLL_INTERVAL = int(os.getenv("NEWS_POLL_INTERVAL", "60"))  # seconds

# Fusion tuning (conservative defaults)
NB_TRUST = {
    "internal": 0.75,
    "rapidai": 0.70,
    "newsdata": 0.65,
    "telegram:cryptomoneyHQ": 0.35,
    "telegram:TradingNewsIO": 0.30,
    "telegram:default": 0.25
}
NB_TAU = float(os.getenv("NB_TAU", "600.0"))     # recency constant seconds
NB_WINDOW = int(os.getenv("NB_WINDOW", str(60*30)))  # 30 minutes

# Supported symbols for mapping
WATCHED_SYMBOLS = ["EURUSD", "XAUUSD", "USDJPY", "USOIL", "BTCUSD"]

# Basic lexicons for quick polarity estimation
POS_WORDS = ["surge","gain","increase","rally","upgrade","beat","positive","rise","strong","bull","higher"]
NEG_WORDS = ["crash","drop","fall","selloff","downgrade","negative","decline","lower","bear","ban","fine","default"]

# Internal state
_nb_lock = threading.Lock()
_nb_recent = defaultdict(lambda: deque())   # symbol -> deque of events (oldest left)
BEAST_NEWS_CACHE = deque(maxlen=2000)      # raw events for inspection & file output

# Logging
logger = logging.getLogger("beast_news_upgrade")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# -------------------------
# Utilities
# -------------------------
def _safe_float(x, default=None):
    """Robust float parser, returns default if cannot parse."""
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        try:
            s = str(x).strip()
            s = re.sub(r"[^0-9eE+\-\.]", "", s)
            return float(s) if s not in ("", ".", "+", "-") else default
        except Exception:
            return default

def _now_ts():
    return int(time.time())

def _recency(age_s):
    try:
        return math.exp(-age_s / max(1.0, NB_TAU))
    except Exception:
        return 0.0

def _write_news_jsonl(item: dict):
    """Append news event to a JSONL file (best-effort)."""
    try:
        with open(NEWS_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    except Exception:
        logger.exception("failed to write news jsonl")

# -------------------------
# News event ingestion & fusion
# -------------------------
def add_news_event(source: str, symbols: list, polarity: float, confidence: float = 1.0, ts: int = None, meta: dict = None):
    """
    Add a normalized news event to in-memory store.
    - polarity: -1..1
    - confidence: 0..1
    """
    if ts is None:
        ts = _now_ts()
    try:
        polarity = float(max(-1.0, min(1.0, polarity)))
    except Exception:
        polarity = 0.0
    try:
        confidence = float(max(0.0, min(1.0, confidence)))
    except Exception:
        confidence = 1.0

    ev = {"source": str(source), "polarity": polarity, "confidence": confidence, "ts": int(ts), "meta": meta or {}}
    with _nb_lock:
        for s in symbols:
            s = s.upper()
            if s not in WATCHED_SYMBOLS:
                # ignore other symbols to keep memory low
                continue
            _nb_recent[s].append(ev)
            # drop old
            cutoff = _now_ts() - NB_WINDOW
            while _nb_recent[s] and _nb_recent[s][0]["ts"] < cutoff:
                _nb_recent[s].popleft()

        # keep raw cache for UI & file output
        BEAST_NEWS_CACHE.append({"time": ts, "source": source, "symbols": symbols, "polarity": polarity, "confidence": confidence, "meta": meta or {}})
    # persist JSONL for bot bridge (non-blocking)
    try:
        _write_news_jsonl({"ts": ts, "source": source, "symbols": symbols, "polarity": polarity, "confidence": confidence, "meta": meta or {}})
    except Exception:
        pass

def compute_fundamental_for_symbol(symbol: str, max_fund_contrib: float = 0.30, impact_scale: float = 1.0):
    """
    Fuse recent events for symbol and return polarity in [-1,1] and meta dict.
    Uses NB_TRUST weights and recency decay.
    """
    symbol = symbol.upper()
    with _nb_lock:
        events = list(_nb_recent.get(symbol, []))
    if not events:
        return 0.0, {"reason": "no_events"}
    now = _now_ts()
    num = 0.0
    den = 0.0
    pos_mass = 0.0
    neg_mass = 0.0
    details = []
    for e in events:
        age = now - int(e["ts"])
        r = _recency(age)
        t = NB_TRUST.get(e.get("source"), NB_TRUST.get("telegram:default", 0.25))
        w = t * float(e.get("confidence", 1.0)) * r
        p = float(e.get("polarity", 0.0))
        num += w * p
        den += w
        if p > 0:
            pos_mass += w
        elif p < 0:
            neg_mass += w
        details.append({"src": e.get("source"), "p": p, "c": e.get("confidence", 1.0), "t": t, "r": r, "w": w})
    if den <= 1e-12:
        return 0.0, {"reason": "zero_weights", "details": details}
    S_raw = num / den
    opposing_min = min(pos_mass, neg_mass)
    opposing_max = max(pos_mass, neg_mass, 1e-12)
    contradiction_ratio = opposing_min / opposing_max
    penalty = 1.0 - contradiction_ratio  # softer penalty keeps some signal
    S = S_raw * penalty
    S = max(-1.0, min(1.0, S))
    meta = {"S_raw": S_raw, "penalty": penalty, "pos_mass": pos_mass, "neg_mass": neg_mass, "details": details}
    return S * impact_scale, meta

def compute_fundamental_norm(symbol: str):
    val, meta = compute_fundamental_for_symbol(symbol)
    return (val + 1.0) / 2.0, meta

# -------------------------
# RapidAPI / NewsData fetchers (best-effort)
# -------------------------
# These functions are safe to run even without keys. They try to fetch headlines,
# compute quick polarity with lexical heuristics, and call add_news_event.

def _lexicon_polarity(text: str):
    t = (text or "").lower()
    pos = sum(1 for w in POS_WORDS if w in t)
    neg = sum(1 for w in NEG_WORDS if w in t)
    if pos + neg == 0:
        return 0.0, 0.35
    pol = (pos - neg) / max(1, (pos + neg))
    conf = 0.5 + min(0.5, (pos + neg) / 6.0)
    return float(max(-1.0, min(1.0, pol))), float(max(0.0, min(1.0, conf)))

def _map_headline_to_symbols(text: str):
    t = (text or "").lower()
    syms = []
    if any(k in t for k in ["eur", "euro", "eurusd"]):
        syms.append("EURUSD")
    if any(k in t for k in ["gold", "xau", "xauusd"]):
        syms.append("XAUUSD")
    if any(k in t for k in ["jpy", "yen", "usdjpy"]):
        syms.append("USDJPY")
    if any(k in t for k in ["oil", "wti", "brent", "crude", "usoil"]):
        syms.append("USOIL")
    if any(k in t for k in ["btc", "bitcoin", "btcusd"]):
        syms.append("BTCUSD")
    return list(dict.fromkeys(syms))  # unique preserve order

def poll_rapidapi_loop(interval: int = NEWS_POLL_INTERVAL):
    """Poll RapidAPI endpoint(s) — placeholder: user must configure RAPIDAPI_KEY and endpoints"""
    if not requests:
        logger.info("requests missing; RapidAPI poll disabled")
        return
    if not RAPIDAPI_KEY:
        logger.info("RAPIDAPI_KEY not set; skipping RapidAPI poll")
        return
    headers = {"x-rapidapi-key": RAPIDAPI_KEY}
    # Example endpoint placeholder — replace with your RapidAPI endpoint details if available
    url = os.getenv("RAPIDAPI_ENDPOINT", "")
    if not url:
        logger.info("RAPIDAPI_ENDPOINT env not set; skipping RapidAPI fetch")
        return
    logger.info("RapidAPI poll started -> %s", url)
    while True:
        try:
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code == 200:
                data = r.json()
                # user needs to adapt parsing based on endpoint shape; generic extraction below:
                items = data.get("articles") or data.get("data") or data.get("news") or []
                for it in items:
                    title = it.get("title") or it.get("headline") or it.get("summary") or ""
                    pol, conf = _lexicon_polarity(title)
                    syms = _map_headline_to_symbols(title)
                    if syms:
                        add_news_event("rapidai", syms, pol, min(1.0, conf * 0.9))
            else:
                logger.debug("RapidAPI non-200: %s", r.status_code)
        except Exception:
            logger.exception("RapidAPI poll error")
        time.sleep(interval)

def poll_newsdata_loop(interval: int = NEWS_POLL_INTERVAL):
    """Poll NewsData / similar services using NEWSDATA_KEY (placeholder)."""
    if not requests:
        logger.info("requests missing; NewsData poll disabled")
        return
    if not NEWDATA_KEY:
        logger.info("NEWSDATA_KEY not set; skipping NewsData poll")
        return
    # Replace endpoint with your NewsData endpoint if you have one
    url = os.getenv("NEWSDATA_ENDPOINT", "")
    if not url:
        logger.info("NEWSDATA_ENDPOINT env not set; skipping NewsData fetch")
        return
    logger.info("NewsData poll started -> %s", url)
    while True:
        try:
            params = {"api_token": NEWDATA_KEY}
            r = requests.get(url, params=params, timeout=8)
            if r.status_code == 200:
                data = r.json()
                items = data.get("articles") or data.get("data") or []
                for it in items:
                    title = it.get("title") or it.get("description") or ""
                    pol, conf = _lexicon_polarity(title)
                    syms = _map_headline_to_symbols(title)
                    if syms:
                        add_news_event("newsdata", syms, pol, min(1.0, conf * 0.9))
            else:
                logger.debug("NewsData non-200: %s", r.status_code)
        except Exception:
            logger.exception("NewsData poll error")
        time.sleep(interval)

def start_api_pollers():
    """Start background poller threads for RapidAPI / NewsData if configured."""
    # RapidAPI
    try:
        if RAPIDAPI_KEY and os.getenv("RAPIDAPI_ENDPOINT"):
            t = threading.Thread(target=lambda: poll_rapidapi_loop(), daemon=True, name="rapidapi-poller")
            t.start()
    except Exception:
        logger.exception("start rapidapi poller failed")
    # NewsData
    try:
        if NEWDATA_KEY and os.getenv("NEWSDATA_ENDPOINT"):
            t = threading.Thread(target=lambda: poll_newsdata_loop(), daemon=True, name="newsdata-poller")
            t.start()
    except Exception:
        logger.exception("start newsdata poller failed")

# -------------------------
# Telegram listener (Telethon) — non-blocking background
# -------------------------
def start_telegram_listener(background: bool = True):
    """Start Telethon listener if credentials present."""
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.info("Telegram credentials not set; listener skipped")
        return False
    try:
        from telethon import TelegramClient, events
    except Exception:
        logger.info("telethon library missing; install with 'pip install telethon' to enable Telegram.")
        return False

    session = os.getenv("TELEGRAM_SESSION", "beast_telegram_session")
    channels = TELEGRAM_CHANNELS

    def _runner():
        import asyncio
        async def _main():
            client = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
            await client.start()
            # resolve channels
            entities = []
            for ch in channels:
                try:
                    entities.append(await client.get_entity(ch))
                except Exception:
                    logger.warning("Failed to resolve Telegram channel: %s", ch)
            if not entities:
                logger.info("No Telegram channels resolved; listener stopping")
                return
            @client.on(events.NewMessage(chats=entities))
            async def handler(ev):
                try:
                    text = (ev.message.message or "") if getattr(ev.message, "message", None) is not None else ""
                    txt = text.lower()
                    pol, conf = _lexicon_polarity(text)
                    syms = _map_headline_to_symbols(text)
                    if not syms:
                        return
                    src = "telegram:" + (getattr(ev.chat, "username", str(getattr(ev.chat, "id", "unknown"))))
                    add_news_event(src, syms, pol, conf, ts=_now_ts(), meta={"text": text})
                except Exception:
                    logger.exception("telegram handler exception")
            logger.info("Telegram listener running for channels: %s", channels)
            await client.run_until_disconnected()
        try:
            asyncio.run(_main())
        except Exception:
            logger.exception("Telegram runner exited")
    if background:
        t = threading.Thread(target=_runner, daemon=True, name="beast-telegram-thread")
        t.start()
    else:
        _runner()
    return True

# -------------------------
# Winrate fix & helpers
# -------------------------
def compute_winrate_from_db(trades_db: str = None, limit: int = 200):
    """
    Read pnl from trades DB and compute winrate (wins / n) robustly.
    trades_db: path to sqlite db (should have table trades with pnl column)
    Returns (winrate_float, n)
    """
    db = trades_db or TRADES_DB
    try:
        import sqlite3
        conn = sqlite3.connect(db, timeout=5)
        cur = conn.cursor()
        # Try common trade table shapes: 'trades' with 'profit' or 'pnl' or 'profit' column
        # We will attempt multiple column names robustly
        for col in ("pnl","profit","pl","profit_loss","realized"):
            try:
                cur.execute(f"SELECT {col} FROM trades ORDER BY id DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
                # If rows exist and the first value is not None, we assume column is correct
                if rows:
                    vals = []
                    for r in rows:
                        v = _safe_float(r[0], default=None)
                        if v is None:
                            continue
                        vals.append(v)
                    conn.close()
                    n = len(vals)
                    if n == 0:
                        return 0.0, 0
                    wins = sum(1 for v in vals if v > 0)
                    return float(wins) / float(n), n
            except Exception:
                # try next candidate
                continue
        # fallback: try select * and guess pnl index by header scanning (not always possible)
        try:
            cur.execute("PRAGMA table_info(trades)")
            cols = cur.fetchall()
            # find numeric-like columns candidate
            candidate_idx = None
            for c in cols:
                cname = c[1].lower()
                if cname in ("pnl","profit","pl"):
                    candidate_idx = cname
                    break
        except Exception:
            candidate_idx = None
        conn.close()
    except Exception:
        logger.exception("compute_winrate_from_db failed")
    return 0.0, 0

# -------------------------
# Bootstrap: start background services (non-fatal)
# -------------------------
try:
    start_telegram_listener(background=True)
except Exception:
    logger.exception("start_telegram_listener failed at bootstrap")

try:
    start_api_pollers()
except Exception:
    logger.exception("start_api_pollers failed at bootstrap")

logger.info("Quant news upgrade loaded: watching %s", ", ".join(WATCHED_SYMBOLS))

# === END UPGRADE MODULE ===
# ===== END FULL QUANT NEWS + TELEGRAM UPGRADE =====
