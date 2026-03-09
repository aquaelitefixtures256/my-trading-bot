# beast_telegram.py
"""
Upgraded Telegram listener for VOID Beast
- Listens to multiple public channels (default: cryptomoneyHQ, TradingNewsIO)
- Maps headlines to symbols: EURUSD, XAUUSD, USDJPY, USOIL, BTCUSD (BTC only for crypto)
- Computes fundamental_score_hint in [-1..1] (polarity-aware)
- Deduplicates messages with persistent seen-store (tg_seen.json)
- Provides:
    - start_telegram_listener(background=True, on_news_callback=callable)
    - get_recent_news(limit)
    - fundamental_scores dict (thread-safe) updated by example_on_news_callback
- Does NOT post to dashboard
"""

import os
import re
import time
import json
import logging
import asyncio
import threading
from typing import Callable, Dict, Any, List, Optional
from pathlib import Path
from dataclasses import dataclass, asdict

from telethon import TelegramClient, events
from dotenv import load_dotenv

# load .env if present
load_dotenv()

# ----------------------
# CONFIG (edit here or via env)
# ----------------------
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
# comma-separated channels (user-friendly short names like cryptomoneyHQ, TradingNewsIO)
CHANNELS = [c.strip() for c in os.getenv("TELEGRAM_CHANNELS", "cryptomoneyHQ,TradingNewsIO").split(",") if c.strip()]
SESSION = os.getenv("TELEGRAM_SESSION", "beast_telegram_session")

SEEN_STORE = os.getenv("TG_SEEN_STORE", "tg_seen.json")
SEEN_MAX_KEEP = int(os.getenv("TG_SEEN_MAX_KEEP", "10000"))

# Tuning: how much a single headline moves the fundamental score and how fast news decays
IMPACT = float(os.getenv("TG_IMPACT", "0.30"))   # injected magnitude per headline (scaled, then capped)
DECAY = float(os.getenv("TG_DECAY", "0.85"))     # previous news persistence
MAX_TELEGRAM_FUND_CONTRIBUTION = float(os.getenv("TG_MAX_FUND_CONTRIB", "0.30"))

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("beast_telegram")

# ----------------------
# Symbol mapping & keywords (extend as needed)
# ----------------------
# Only these symbols are targeted: EURUSD, XAUUSD (gold), USDJPY, USOIL (oil/crude), BTCUSD
SYMBOL_KEYWORDS = {
    "EURUSD": ["eurusd", "euro", "eur/usd", "eurodollar", "euro dollar"],
    "XAUUSD": ["xau", "gold", "xauusd", "gold price"],
    "USDJPY": ["usdjpy", "jpy", "yen", "usd/jpy"],
    "USOIL": ["oil", "wti", "brent", "us oil", "crude", "usoil"],
    "BTCUSD": ["btc", "bitcoin", "bitcoinusd", "btc/usd"]
}

# Positive/negative lexical lists (expand over time)
POS_WORDS = ["surge", "gain", "increase", "rally", "upgrade", "beat", "positive", "rise", "strong", "higher"]
NEG_WORDS = ["crash", "drop", "fall", "selloff", "downgrade", "negative", "decline", "lower", "bear", "ban", "fine"]

# URL detection
URL_RE = re.compile(r"https?://\S+")

# ----------------------
# Internal state
# ----------------------
_news_queue_lock = threading.Lock()
_news_queue: List[Dict[str, Any]] = []

_seen_lock = threading.Lock()
_seen_set = set()

_fund_lock = threading.Lock()
fundamental_scores: Dict[str, float] = {}  # symbol -> [-1..1]

# ----------------------
# Data classes / helpers
# ----------------------
@dataclass
class NewsPayload:
    source: str
    channel: str
    message_id: str
    date_ts: int
    text: str
    urls: List[str]
    media: List[str]
    symbols: List[str]
    fundamental_score_hint: float
    raw: Dict[str, Any] = None

    def to_dict(self):
        d = asdict(self)
        if self.raw and not isinstance(self.raw, dict):
            d["raw"] = str(self.raw)
        return d

def _load_seen_store(path: str):
    p = Path(path)
    if not p.exists():
        return set()
    try:
        with p.open("r", encoding="utf8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            if isinstance(data, dict) and "seen" in data:
                return set(data["seen"])
    except Exception:
        logger.exception("Failed to load seen store")
    return set()

def _save_seen_store(path: str, s: set):
    try:
        p = Path(path)
        with p.open("w", encoding="utf8") as f:
            json.dump(sorted(list(s))[-SEEN_MAX_KEEP:], f)
    except Exception:
        logger.exception("Failed to save seen store")

def extract_urls(text: str) -> List[str]:
    return URL_RE.findall(text or "")

def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip()

def map_headline_to_symbols(text: str) -> List[str]:
    txt = (text or "").lower()
    hits = []
    for sym, kws in SYMBOL_KEYWORDS.items():
        for kw in kws:
            if kw in txt:
                hits.append(sym)
                break
    # For BTC we only want BTCUSD (user requested only BTC on crypto)
    # If no symbol matched but generic "crypto" words exist, map to BTCUSD only (per user's preference)
    if not hits:
        if any(k in txt for k in ["crypto", "cryptocurrency", "altcoin", "exchange", "bitcoin"]):
            return ["BTCUSD"]
    return list(sorted(set(hits)))

def compute_fundamental_hint(text: str) -> float:
    if not text:
        return 0.0
    t = text.lower()
    score = 0.0
    for w in POS_WORDS:
        if w in t:
            score += 0.5
    for w in NEG_WORDS:
        if w in t:
            score -= 0.7
    # simple percent heuristic
    for m in re.findall(r"(-?\d+(\.\d+)?)\s?%+", t):
        try:
            val = float(m[0])
            if val > 0:
                score += min(0.4, val / 100.0)
            else:
                score -= min(0.6, abs(val) / 100.0)
        except Exception:
            pass
    # clamp
    if score > 1.0: score = 1.0
    if score < -1.0: score = -1.0
    return score

def _enqueue_news(payload: Dict[str, Any]):
    with _news_queue_lock:
        _news_queue.insert(0, payload)
        if len(_news_queue) > 1000:
            _news_queue.pop()

def get_recent_news(limit: int = 50) -> List[Dict[str, Any]]:
    with _news_queue_lock:
        return _news_queue[:limit]

# ----------------------
# Telethon listener loop
# ----------------------
async def _telethon_run_loop(on_news_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
                             seen_store: str = SEEN_STORE,
                             channels: List[str] = None,
                             session: str = SESSION):
    global _seen_set
    if channels is None:
        channels = CHANNELS
    _seen_set = _load_seen_store(seen_store)
    logger.info("Loaded seen store: %d ids", len(_seen_set))

    client = TelegramClient(session, API_ID, API_HASH)
    await client.start()
    logger.info("Telethon client started (session=%s)", session)

    # resolve entities for channels
    entities = []
    for ch in channels:
        try:
            ent = await client.get_entity(ch)
            entities.append(ent)
            logger.info("Resolved channel: %s -> id=%s", ch, getattr(ent, "id", "?"))
        except Exception:
            logger.exception("Failed to resolve channel: %s", ch)

    if not entities:
        logger.error("No valid channels resolved. Exiting telethon listener.")
        await client.disconnect()
        return

    @client.on(events.NewMessage(chats=entities))
    async def _handler(event):
        try:
            msg = event.message
            # create a unique seen-id per channel to avoid cross-channel id collision
            chname = None
            try:
                chname = getattr(event.chat, "username", None) or getattr(event.chat, "title", None) or str(getattr(event.chat, "id", ""))
            except Exception:
                chname = "unknown"
            msg_id = f"{chname}:{getattr(msg, 'id', 0)}"
            with _seen_lock:
                if msg_id in _seen_set:
                    return
                _seen_set.add(msg_id)
                if len(_seen_set) > SEEN_MAX_KEEP:
                    # trim by converting to list and keeping last slice (not perfect but practical)
                    _seen_set = set(list(_seen_set)[-SEEN_MAX_KEEP:])

            text = normalize_text(getattr(msg, "message", "") or "")
            urls = extract_urls(text)
            media_types = []
            if getattr(msg, "media", None):
                media_types.append(type(msg.media).__name__)

            symbols = map_headline_to_symbols(text)
            hint = compute_fundamental_hint(text)  # -1..1

            news = NewsPayload(
                source=f"telegram:{chname}",
                channel=chname,
                message_id=msg_id,
                date_ts=int(getattr(msg, "date", time.time()).timestamp()) if getattr(msg, "date", None) else int(time.time()),
                text=text,
                urls=urls,
                media=media_types,
                symbols=symbols,
                fundamental_score_hint=hint,
                raw={"sender": getattr(msg, "sender_id", None)}
            )
            payload = news.to_dict()

            _enqueue_news(payload)

            # call callback in separate thread to avoid blocking telethon loop
            if on_news_callback:
                try:
                    threading.Thread(target=lambda p=payload: on_news_callback(p), daemon=True).start()
                except Exception:
                    logger.exception("on_news_callback failed")

            logger.info("Telegram news -> %s ids=%s symbols=%s hint=%.3f", chname, msg_id, symbols or ["-"], hint)

            # persist seen occasionally
            if len(_seen_set) % 20 == 0:
                try:
                    _save_seen_store(seen_store, _seen_set)
                except Exception:
                    pass

        except Exception:
            logger.exception("Exception in Telegram handler")

    try:
        await client.run_until_disconnected()
    finally:
        logger.info("Telethon listener exiting, saving seen store")
        _save_seen_store(seen_store, _seen_set)
        await client.disconnect()

# ----------------------
# public starter
# ----------------------
def start_telegram_listener(background: bool = True,
                            on_news_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
                            seen_store: str = SEEN_STORE,
                            channels: List[str] = None,
                            session: str = SESSION):
    if channels is None:
        channels = CHANNELS
    if API_ID == 0 or not API_HASH:
        raise RuntimeError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in environment / .env")

    def _runner():
        try:
            asyncio.run(_telethon_run_loop(on_news_callback=on_news_callback, seen_store=seen_store, channels=channels, session=session))
        except Exception:
            logger.exception("Telethon run loop ended with exception")

    if background:
        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        logger.info("Telegram listener started in background for channels: %s", ",".join(channels))
    else:
        _runner()

# ----------------------
# example callback to integrate news into fundamental_scores (thread-safe)
# ----------------------
def example_on_news_callback(payload: Dict[str, Any]):
    """
    - maps payload['symbols'] to the in-memory fundamental_scores
    - the injection is: injection = hint * IMPACT, then capped by MAX_TELEGRAM_FUND_CONTRIBUTION
    - final update: new = old * DECAY + injection
    """
    try:
        symbols = payload.get("symbols") or ["BTCUSD"]
        hint = float(payload.get("fundamental_score_hint") or 0.0)  # -1..1

        injection = hint * IMPACT
        max_inj = MAX_TELEGRAM_FUND_CONTRIBUTION
        if abs(injection) > max_inj:
            injection = max_inj if injection > 0 else -max_inj

        with _fund_lock:
            for sym in symbols:
                old = fundamental_scores.get(sym, 0.0)
                new = old * DECAY + injection
                if new > 1.0: new = 1.0
                if new < -1.0: new = -1.0
                fundamental_scores[sym] = new
        logger.info("Applied telegram injection %.4f -> %s (hint=%.3f)", injection, symbols, hint)
    except Exception:
        logger.exception("example_on_news_callback error")

# ----------------------
# small CLI for testing
# ----------------------
if __name__ == "__main__":
    if API_ID == 0 or not API_HASH:
        print("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env or environment before running.")
        raise SystemExit(1)
    print("Starting beast_telegram standalone. Channels:", CHANNELS)
    # print news as they arrive
    def print_cb(p):
        print(json.dumps(p, indent=2, ensure_ascii=False))
    # start in foreground for easier debugging
    start_telegram_listener(background=False, on_news_callback=print_cb)
