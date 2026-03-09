# beast_telegram.py
"""
Beast Telegram listener
- Uses Telethon to listen to a public channel (default: cryptomoneyHQ)
- Produces news_payloads and calls on_news_callback(payload) if provided
- Persists seen message ids to disk to avoid duplicates across restarts
- Does NOT post to any dashboard (local-only)
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

# -------------
# CONFIG (env or edit here)
# -------------
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))         # required, from my.telegram.org
API_HASH = os.getenv("TELEGRAM_API_HASH", "")          # required
CHANNEL = os.getenv("TELEGRAM_CHANNEL", "cryptomoneyHQ")
SESSION = os.getenv("TELEGRAM_SESSION", "beast_telegram_session")

# Where to persist seen message ids (so restart doesn't reprocess)
SEEN_STORE = os.getenv("TG_SEEN_STORE", "tg_seen.json")

# Listener tuning
IMPACT = float(os.getenv("TG_IMPACT", "0.30"))   # how strongly a single headline moves the fundamental score
DECAY = float(os.getenv("TG_DECAY", "0.85"))     # how previous news decays each injection
MAX_TELEGRAM_FUND_CONTRIBUTION = float(os.getenv("TG_MAX_FUND_CONTRIB", "0.30"))
# (This cap ensures telegram can only supply up to ~30% of the 'fundamental' component; final agg uses your weights)

# Basic sentiment/keyword lists (extend as needed)
POS_WORDS = ["surge", "gain", "increase", "bull", "rally", "upgrade", "beat", "positive", "rebound", "drop less", "outperform"]
NEG_WORDS = ["crash", "drop", "fall", "ban", "downgrade", "selloff", "bear", "loss", "default", "fine", "negative", "delist", "bankruptcy"]

# Map keywords -> symbols (lowercase). Extend this to match your symbol set.
SYMBOL_KEYWORDS = {
    "BTC": ["btc", "bitcoin", "sats"],
    "ETH": ["eth", "ethereum"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "ALL_CRYPTO": ["crypto", "crypto market", "altcoin", "altcoins", "bitcoin", "ethereum"]
}

# Rate limiting: avoid processing the same message multiple times; keep last N ids in memory
SEEN_MAX_KEEP = 10000

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("beast_telegram")

# URL extractor
URL_RE = re.compile(r"https?://\S+")

# Thread-safe internal queue for news items (if your bot wants to poll)
_news_queue_lock = threading.Lock()
_news_queue: List[Dict[str, Any]] = []

# In-memory seen set (plus persisted store)
_seen_lock = threading.Lock()
_seen_set = set()  # message ids (ints or strings)


@dataclass
class NewsPayload:
    source: str
    channel: str
    message_id: int
    date_ts: int
    text: str
    urls: List[str]
    media: List[str]
    symbols: List[str]
    fundamental_score_hint: float   # -1..1 polarity
    raw: Dict[str, Any] = None

    def to_dict(self):
        d = asdict(self)
        # raw might be non-serializable in some rare cases — ensure JSONable
        if self.raw and not isinstance(self.raw, dict):
            d["raw"] = str(self.raw)
        return d


# ----------------
# persistence for seen ids (simple file)
# ----------------
def _load_seen_store(path: str):
    p = Path(path)
    if not p.exists():
        return set()
    try:
        with p.open("r", encoding="utf8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            if isinstance(data, dict):
                return set(data.get("seen", []))
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


# ----------------
# text utilities
# ----------------
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
    if not hits:
        # fallback: mark as ALL_CRYPTO if general crypto words appear
        for kw in SYMBOL_KEYWORDS.get("ALL_CRYPTO", []):
            if kw in txt:
                return ["ALL_CRYPTO"]
        return []
    return list(sorted(set(hits)))


def compute_fundamental_hint(text: str) -> float:
    """
    Compute a lightweight polarity score in [-1..1].
    - positive words increase, negative words decrease.
    - also uses simple heuristics for numbers (e.g., 'up 10%') boosting positivity.
    """
    if not text:
        return 0.0
    t = text.lower()
    score = 0.0
    # word lists
    for w in POS_WORDS:
        if w in t:
            score += 0.6  # strong weight for lexical positives
    for w in NEG_WORDS:
        if w in t:
            score -= 0.8  # slightly stronger negative impact
    # percent/number heuristics (simple)
    percent_matches = re.findall(r"(-?\d+(\.\d+)?)\s?%+", t)
    for m in percent_matches:
        try:
            val = float(m[0])
            if val > 0:
                score += min(0.5, val / 100.0)  # small boost for positive % moves
            else:
                score -= min(0.8, abs(val) / 100.0)
        except Exception:
            pass

    # clamp
    if score > 1.0:
        score = 1.0
    if score < -1.0:
        score = -1.0
    return score


# ----------------
# queue and callback plumbing
# ----------------
def _enqueue_news(payload: Dict[str, Any]):
    with _news_queue_lock:
        _news_queue.insert(0, payload)  # newest first
        # keep queue reasonable
        if len(_news_queue) > 1000:
            _news_queue.pop()


def get_recent_news(limit: int = 50) -> List[Dict[str, Any]]:
    with _news_queue_lock:
        return _news_queue[:limit]


# ----------------
# the Telethon listener
# ----------------
async def _telethon_run_loop(on_news_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
                             seen_store: str = SEEN_STORE,
                             channel: str = CHANNEL,
                             session: str = SESSION):
    global _seen_set
    # load seen store once
    _seen_set = _load_seen_store(seen_store)
    logger.info("Seen store loaded: %d ids", len(_seen_set))

    client = TelegramClient(session, API_ID, API_HASH)
    await client.start()
    logger.info("Telethon client started (session=%s). Resolving channel: %s", session, channel)

    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        logger.exception("Failed to resolve channel '%s': %s", channel, e)
        await client.disconnect()
        return

    logger.info("Listening to channel: %s (id=%s)", channel, getattr(entity, "id", "?"))

    @client.on(events.NewMessage(chats=entity))
    async def _handler(event):
        try:
            msg = event.message
            msg_id = int(getattr(msg, "id", 0))
            if not msg_id:
                # fallback based on date+text hash
                msg_id = hash((getattr(msg, "date", None), getattr(msg, "message", None)))

            # dedupe
            with _seen_lock:
                if msg_id in _seen_set:
                    return
                _seen_set.add(msg_id)
                # keep seen set bounded
                if len(_seen_set) > SEEN_MAX_KEEP:
                    # drop oldest (not perfect — set has no order; we keep it simple)
                    while len(_seen_set) > SEEN_MAX_KEEP:
                        _seen_set.pop()

            # build payload
            text = normalize_text(getattr(msg, "message", "") or "")
            urls = extract_urls(text)
            media_types = []
            if getattr(msg, "media", None):
                # basic media detection by type name
                mt = type(msg.media).__name__
                media_types.append(mt)

            symbols = map_headline_to_symbols(text)
            fundamental_hint = compute_fundamental_hint(text)  # -1..1

            news = NewsPayload(
                source=f"telegram:{channel}",
                channel=channel,
                message_id=msg_id,
                date_ts=int(getattr(msg, "date", time.time()).timestamp()) if getattr(msg, "date", None) else int(time.time()),
                text=text,
                urls=urls,
                media=media_types,
                symbols=symbols,
                fundamental_score_hint=fundamental_hint,
                raw={"sender": getattr(msg, "sender_id", None)}
            )

            payload = news.to_dict()

            # push to local queue
            _enqueue_news(payload)

            # also call callback if provided (synchronous call)
            if on_news_callback:
                try:
                    # callback shouldn't be awaited here — call in separate thread to avoid blocking
                    threading.Thread(target=lambda p=payload: on_news_callback(p), daemon=True).start()
                except Exception:
                    logger.exception("on_news_callback failed")

            logger.info("New Telegram news id=%s symbols=%s hint=%.3f text=%s", msg_id, symbols or ["-"], fundamental_hint, (text[:140] + "..." if len(text) > 140 else text))

            # persist seen store periodically (cheap)
            try:
                if len(_seen_set) % 20 == 0:
                    _save_seen_store(seen_store, _seen_set)
            except Exception:
                pass

        except Exception as e:
            logger.exception("Exception in Telegram handler: %s", e)

    # run until disconnected
    try:
        await client.run_until_disconnected()
    finally:
        logger.info("Telethon listener exiting; saving seen store")
        _save_seen_store(seen_store, _seen_set)
        await client.disconnect()


# ----------------
# public starter: run in background thread or blocking
# ----------------
def start_telegram_listener(background: bool = True,
                            on_news_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
                            seen_store: str = SEEN_STORE,
                            channel: str = CHANNEL,
                            session: str = SESSION):
    """
    Start the Telegram listener.

    - background=True: runs Telethon in a daemon thread and returns immediately.
      The on_news_callback (if provided) will be called in a tiny worker thread for each message.
    - background=False: blocks and runs the listener in the current thread (useful for debugging).
    """
    if API_ID == 0 or not API_HASH:
        raise RuntimeError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in environment / .env before starting the listener.")

    def _runner():
        try:
            asyncio.run(_telethon_run_loop(on_news_callback=on_news_callback, seen_store=seen_store, channel=channel, session=session))
        except Exception:
            logger.exception("Telethon run loop ended with exception")

    if background:
        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        logger.info("Telegram listener started in background")
    else:
        _runner()


# ----------------
# Example simple on_news_callback that integrates with a fundamental_scores dict
# (You can paste this pattern into your bot, or import this module and call start_telegram_listener(...))
# ----------------
_fund_lock = threading.Lock()
fundamental_scores: Dict[str, float] = {}  # symbol -> [-1..1]


def example_on_news_callback(payload: Dict[str, Any]):
    """
    This function demonstrates:
      - mapping the payload to symbols
      - turning the fundamental hint into a bounded injection
      - updating the in-memory fundamental_scores map thread-safely
    """
    try:
        symbols = payload.get("symbols") or ["ALL_CRYPTO"]
        hint = float(payload.get("fundamental_score_hint") or 0.0)  # -1..1

        # Limit the influence so telegram cannot exceed MAX_TELEGRAM_FUND_CONTRIBUTION of the fund score
        # Here we interpret hint in [-1..1], impact scales it down
        injection = hint * IMPACT

        # cap injection magnitude (absolute) so single message doesn't dominate
        max_injection = MAX_TELEGRAM_FUND_CONTRIBUTION
        if abs(injection) > max_injection:
            injection = max_injection if injection > 0 else -max_injection

        with _fund_lock:
            for sym in symbols:
                old = fundamental_scores.get(sym, 0.0)
                new = old * DECAY + injection
                # clamp
                if new > 1.0: new = 1.0
                if new < -1.0: new = -1.0
                fundamental_scores[sym] = new
        logger.info("example_on_news_callback applied injection=%.4f -> symbols=%s", injection, symbols)
    except Exception:
        logger.exception("example_on_news_callback error")


# ----------------
# Command-line / standalone runner
# ----------------
if __name__ == "__main__":
    # If run directly, start background listener that prints payloads to stdout
    def print_cb(p):
        print(json.dumps(p, indent=2, ensure_ascii=False))

    print("Starting beast_telegram in standalone mode. Press Ctrl-C to exit.")
    start_telegram_listener(background=False, on_news_callback=print_cb)
