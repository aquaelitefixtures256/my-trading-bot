# upgrade_bot.py
# Usage: put this file in the same folder as your bot (default INPUT_FILE path)
# then run: python upgrade_bot.py
# It will create: voidx_beast_fully_upgraded.py and a backup voidx_beast_NFP.py.bak

import os
import re
import sys
import time
import shutil
import textwrap

# CONFIG - change if your filenames are different
INPUT_FILE = os.environ.get("INPUT_FILE", "voidx_beast_NFP.py")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "voidx_beast_fully_upgraded.py")
BACKUP_SUFFIX = ".bak"

def read_file(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def backup_file(path):
    bak = path + BACKUP_SUFFIX
    shutil.copy2(path, bak)
    return bak

# The full upgrade module to inject (defensive, non-destructive)
UPGRADE_MODULE = textwrap.dedent(r'''
# ===== BEGIN VOID BEAST QUANT NEWS + TELEGRAM UPGRADE MODULE =====
# Automatically injected by upgrade_bot.py
import os, re, time, math, threading, json
from collections import deque, defaultdict

# lightweight logger (doesn't require changing user's logger)
try:
    import logging
    _upgrade_logger = logging.getLogger("void_beast_upgrade")
    if not _upgrade_logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        _upgrade_logger.addHandler(h)
    _upgrade_logger.setLevel(logging.INFO)
except Exception:
    _upgrade_logger = None

# --- Configuration (via env)
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY","").strip()
RAPIDAPI_ENDPOINT = os.getenv("RAPIDAPI_ENDPOINT","").strip()
NEWDATA_KEY = os.getenv("NEWDATA_KEY","").strip()
NEWSDATA_ENDPOINT = os.getenv("NEWSDATA_ENDPOINT","").strip()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID","0") or 0)
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH","").strip()
TELEGRAM_CHANNELS = [c.strip() for c in os.getenv("TELEGRAM_CHANNELS","cryptomoneyHQ,TradingNewsIO").split(",") if c.strip()]

# Fusion weights (conservative defaults)
NB_TRUST = {
    "internal": 0.75,
    "rapidai": 0.70,
    "newsdata": 0.65,
    "telegram:cryptomoneyHQ": 0.35,
    "telegram:TradingNewsIO": 0.30,
    "telegram:default": 0.25
}
NB_TAU = float(os.getenv("NB_TAU","600.0"))
NB_WINDOW = int(os.getenv("NB_WINDOW", str(60*30)))

WATCHED_SYMBOLS = ["EURUSD","XAUUSD","USDJPY","USOIL","BTCUSD"]

_POS_WORDS = ["surge","gain","increase","rally","upgrade","beat","positive","rise","strong","bull","higher"]
_NEG_WORDS = ["crash","drop","fall","selloff","downgrade","negative","decline","lower","bear","ban","fine","default"]

_nb_lock = threading.Lock()
_nb_recent = defaultdict(lambda: deque())
BEAST_NEWS_CACHE = deque(maxlen=2000)

def _now_ts():
    return int(time.time())

def _safe_float(x, default=None):
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        try:
            s = re.sub(r"[^0-9eE+\\-\\.]", "", str(x))
            return float(s) if s not in ("", ".", "+", "-") else default
        except Exception:
            return default

def _recency(age_s):
    try:
        return math.exp(-age_s / max(1.0, NB_TAU))
    except Exception:
        return 0.0

def _lexicon_polarity(text):
    t = (text or "").lower()
    pos = sum(1 for w in _POS_WORDS if w in t)
    neg = sum(1 for w in _NEG_WORDS if w in t)
    if pos + neg == 0:
        return 0.0, 0.35
    pol = (pos - neg) / max(1, (pos + neg))
    conf = 0.5 + min(0.5, (pos + neg) / 6.0)
    return float(max(-1.0, min(1.0, pol))), float(max(0.0, min(1.0, conf)))

def _map_headline_to_symbols(text):
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
    return list(dict.fromkeys(syms))

def add_news_event(source, symbols, polarity, confidence=1.0, ts=None, meta=None):
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
                continue
            _nb_recent[s].append(ev)
            cutoff = _now_ts() - NB_WINDOW
            while _nb_recent[s] and _nb_recent[s][0]["ts"] < cutoff:
                _nb_recent[s].popleft()
        BEAST_NEWS_CACHE.append({"ts": ts, "source": source, "symbols": symbols, "polarity": polarity, "confidence": confidence, "meta": meta or {}})
    # append into a JSONL bridge (best-effort)
    try:
        with open("beast_news_queue.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": ts, "source": source, "symbols": symbols, "polarity": polarity, "confidence": confidence, "meta": meta or {}}) + "\\n")
    except Exception:
        pass

def compute_fundamental_for_symbol(symbol):
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
    penalty = 1.0 - contradiction_ratio
    S = S_raw * penalty
    S = max(-1.0, min(1.0, S))
    meta = {"S_raw": S_raw, "penalty": penalty, "pos_mass": pos_mass, "neg_mass": neg_mass, "details": details}
    return S, meta

# RapidAPI / NewsData placeholders (safe to be present even without keys)
try:
    import requests
except Exception:
    requests = None

def _rapidapi_poller(interval=60):
    if not requests or not RAPIDAPI_KEY or not RAPIDAPI_ENDPOINT:
        return
    _upgrade_logger.info("RapidAPI poller starting")
    while True:
        try:
            resp = requests.get(RAPIDAPI_ENDPOINT, headers={"x-rapidapi-key": RAPIDAPI_KEY}, timeout=8)
            if resp.status_code == 200:
                j = resp.json()
                items = j.get("articles") or j.get("news") or j.get("data") or []
                for it in items:
                    title = it.get("title") or it.get("headline") or it.get("summary") or ""
                    pol, conf = _lexicon_polarity(title)
                    syms = _map_headline_to_symbols(title)
                    if syms:
                        add_news_event("rapidai", syms, pol, min(1.0, conf * 0.9), ts=_now_ts(), meta={"raw": it})
        except Exception:
            try:
                _upgrade_logger.exception("RapidAPI poll error")
            except Exception:
                pass
        time.sleep(interval)

def _newsdata_poller(interval=60):
    if not requests or not NEWDATA_KEY or not NEWSDATA_ENDPOINT:
        return
    _upgrade_logger.info("NewsData poller starting")
    while True:
        try:
            resp = requests.get(NEWSDATA_ENDPOINT, params={"api_token": NEWDATA_KEY}, timeout=8)
            if resp.status_code == 200:
                j = resp.json()
                items = j.get("articles") or j.get("data") or []
                for it in items:
                    title = it.get("title") or it.get("description") or ""
                    pol, conf = _lexicon_polarity(title)
                    syms = _map_headline_to_symbols(title)
                    if syms:
                        add_news_event("newsdata", syms, pol, min(1.0, conf * 0.9), ts=_now_ts(), meta={"raw": it})
        except Exception:
            try:
                _upgrade_logger.exception("NewsData poll error")
            except Exception:
                pass
        time.sleep(interval)

def start_api_pollers():
    try:
        if requests and RAPIDAPI_KEY and RAPIDAPI_ENDPOINT:
            threading.Thread(target=_rapidapi_poller, daemon=True, name="rapidapi-poller").start()
    except Exception:
        pass
    try:
        if requests and NEWDATA_KEY and NEWSDATA_ENDPOINT:
            threading.Thread(target=_newsdata_poller, daemon=True, name="newsdata-poller").start()
    except Exception:
        pass

# Telegram listener using Telethon (best-effort; safe if telethon missing)
def start_telegram_listener(background=True):
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_CHANNELS:
        try:
            if _upgrade_logger:
                _upgrade_logger.info("Telegram not configured; skipping Telegram listener")
        except Exception:
            pass
        return False
    try:
        from telethon import TelegramClient, events
    except Exception:
        try:
            if _upgrade_logger:
                _upgrade_logger.info("telethon not installed; install 'telethon' to enable Telegram")
        except Exception:
            pass
        return False
    session = os.getenv("TELEGRAM_SESSION", "beast_telegram_session")
    channels = TELEGRAM_CHANNELS
    def _runner():
        import asyncio
        async def _main():
            client = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
            await client.start()
            entities = []
            for ch in channels:
                try:
                    entities.append(await client.get_entity(ch))
                except Exception:
                    if _upgrade_logger:
                        _upgrade_logger.warning("Failed to resolve channel %s", ch)
            if not entities:
                if _upgrade_logger:
                    _upgrade_logger.info("No telegram entities resolved; listener exiting")
                return
            @client.on(events.NewMessage(chats=entities))
            async def handler(ev):
                try:
                    text = (ev.message.message or "") if getattr(ev.message, "message", None) is not None else ""
                    pol, conf = _lexicon_polarity(text)
                    syms = _map_headline_to_symbols(text)
                    if not syms:
                        return
                    src = "telegram:" + (getattr(ev.chat, "username", str(getattr(ev.chat, "id", "unknown"))))
                    add_news_event(src, syms, pol, conf, ts=_now_ts(), meta={"text": text})
                except Exception:
                    if _upgrade_logger:
                        _upgrade_logger.exception("telegram handler")
            if _upgrade_logger:
                _upgrade_logger.info("Telegram listener running for channels: %s", channels)
            await client.run_until_disconnected()
        try:
            asyncio.run(_main())
        except Exception:
            if _upgrade_logger:
                _upgrade_logger.exception("Telegram runner")
    if background:
        threading.Thread(target=_runner, daemon=True, name="beast-telegram-thread").start()
    else:
        _runner()
    return True

def compute_winrate_from_db(trades_db=None, limit=200):
    db = trades_db or os.getenv("TRADES_DB", "dashboard.db")
    try:
        import sqlite3
        conn = sqlite3.connect(db, timeout=5)
        cur = conn.cursor()
        candidates = ["pnl","profit","pl","profit_loss","realized"]
        for col in candidates:
            try:
                cur.execute(f"SELECT {col} FROM trades ORDER BY id DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
                if not rows:
                    continue
                vals = []
                for r in rows:
                    v = _safe_float(r[0], default=None)
                    if v is None:
                        continue
                    vals.append(v)
                conn.close()
                if not vals:
                    return 0.0, 0
                wins = sum(1 for v in vals if v > 0)
                return float(wins) / float(len(vals)), len(vals)
            except Exception:
                continue
        conn.close()
    except Exception:
        if _upgrade_logger:
            _upgrade_logger.exception("compute_winrate_from_db failed")
    return 0.0, 0

# bootstrap background services (non-fatal)
try:
    start_telegram_listener(background=True)
except Exception:
    pass
try:
    start_api_pollers()
except Exception:
    pass

if _upgrade_logger:
    _upgrade_logger.info("Quant news upgrade module loaded; watching %s", ", ".join(WATCHED_SYMBOLS))
# ===== END VOID BEAST QUANT NEWS + TELEGRAM UPGRADE MODULE =====
''')

def ensure_bitcoin_keywords_in_source(source_text):
    """
    If the source file contains a dict named _FUND_KEYWORDS (or FUND_KEYWORDS),
    try to insert a bitcoin key if missing. Returns updated source_text.
    """
    # try common variable names
    for varname in ("_FUND_KEYWORDS", "FUND_KEYWORDS", "_fund_keywords", "fund_keywords"):
        pattern = re.compile(r"(" + re.escape(varname) + r"\s*=\s*\{)", re.M)
        m = pattern.search(source_text)
        if m:
            # find the matching closing brace for this dict (naive)
            start = m.end()
            # find the closing brace by scanning forward balancing braces
            idx = start
            depth = 1
            while idx < len(source_text) and depth > 0:
                if source_text[idx] == "{":
                    depth += 1
                elif source_text[idx] == "}":
                    depth -= 1
                idx += 1
            if depth == 0:
                dict_text = source_text[m.start():idx]
                # if 'bitcoin' not present, inject before the final closing brace
                if re.search(r"['\"]bitcoin['\"]\s*:", dict_text) is None:
                    inject = '    "bitcoin": ["bitcoin","btc","btcusd"],\n'
                    # insert before the last '}'
                    new_dict_text = dict_text[:-1] + "\n" + inject + "}"
                    source_text = source_text[:m.start()] + new_dict_text + source_text[idx:]
                    print("Inserted 'bitcoin' into", varname)
                    return source_text
    # nothing found -> return unchanged
    return source_text

def replace_or_append_get_recent_trades(source_text):
    """
    Replace an existing get_recent_trades(...) function if present, otherwise append a robust implementation.
    """
    pattern = re.compile(r"def\s+get_recent_trades\s*\([^)]*\)\s*:", re.M)
    m = pattern.search(source_text)
    new_func = textwrap.dedent(r'''
def get_recent_trades(limit=200):
    """
    Robust get_recent_trades: returns list of tuples; coerces pnl to float when possible.
    """
    try:
        import sqlite3
        db = os.getenv("TRADES_DB", "dashboard.db")
        conn = sqlite3.connect(db, timeout=5)
        cur = conn.cursor()
        try:
            cur.execute("SELECT ts,symbol,side,pnl,rmult,regime,score,model_score FROM trades ORDER BY id DESC LIMIT ?", (limit,))
            rows = cur.fetchall()
        except Exception:
            # fallback: select all columns
            try:
                cur.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
            except Exception:
                conn.close()
                return []
        conn.close()
        out = []
        for r in rows:
            r = list(r)
            if len(r) >= 4:
                r[3] = _safe_float(r[3], default=None)
            out.append(tuple(r))
        return out
    except Exception:
        return []
''')
    if m:
        # replace the function block: find the next "def " after m.start()
        start = m.start()
        next_def = re.search(r"\ndef\s+\w+\s*\(", source_text[start+1:])
        if next_def:
            end = start + 1 + next_def.start()
        else:
            end = len(source_text)
        source_text = source_text[:start] + new_func + source_text[end:]
        print("Replaced existing get_recent_trades()")
    else:
        # append
        source_text = source_text + "\n\n" + new_func
        print("Appended new get_recent_trades()")
    return source_text

def patch_adapt_and_optimize_winrate(source_text):
    """
    Find adapt_and_optimize function and inject robust winrate calculation.
    If function not found, append a helper compute_winrate_from_db at end.
    """
    pattern = re.compile(r"def\s+adapt_and_optimize\s*\([^)]*\)\s*:", re.M)
    m = pattern.search(source_text)
    robust_block = textwrap.dedent(r'''
    # --- robust winrate calculation injected by upgrade_bot.py ---
    try:
        recent = get_recent_trades(limit=200)
        vals = []
        for r in recent:
            try:
                pnl = r[3] if len(r) > 3 else None
                if pnl is None:
                    continue
                f = _safe_float(pnl, default=None)
                if f is None:
                    continue
                vals.append(f)
            except Exception:
                continue
        n = len(vals)
        winrate = (sum(1 for v in vals if v > 0) / n) if n > 0 else 0.0
        try:
            _upgrade_logger.info("Adapt: recent winrate=%.3f n=%d", winrate, n)
        except Exception:
            pass
    except Exception:
        try:
            _upgrade_logger.exception("winrate calc failed")
        except Exception:
            pass
    # --- end injected winrate calc ---
''')
    if m:
        # Insert robust_block after the function signature line
        sig_end = source_text.find("\n", m.end()) + 1
        source_text = source_text[:sig_end] + robust_block + source_text[sig_end:]
        print("Injected robust winrate block into adapt_and_optimize()")
    else:
        # append a small helper at end
        helper = textwrap.dedent(r'''
def compute_winrate_from_db(trades_db=None, limit=200):
    db = trades_db or os.getenv("TRADES_DB", "dashboard.db")
    try:
        import sqlite3
        conn = sqlite3.connect(db, timeout=5)
        cur = conn.cursor()
        candidates = ["pnl","profit","pl","profit_loss","realized"]
        for col in candidates:
            try:
                cur.execute(f"SELECT {col} FROM trades ORDER BY id DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
                if not rows:
                    continue
                vals = []
                for r in rows:
                    v = _safe_float(r[0], default=None)
                    if v is None:
                        continue
                    vals.append(v)
                conn.close()
                if not vals:
                    return 0.0, 0
                wins = sum(1 for v in vals if v > 0)
                return float(wins) / float(len(vals)), len(vals)
            except Exception:
                continue
        conn.close()
    except Exception:
        pass
    return 0.0, 0
''')
        source_text = source_text + "\n\n" + helper
        print("Appended compute_winrate_from_db() helper (adapt_and_optimize not found)")
    return source_text

def inject_upgrade_module(source_text):
    # Avoid duplicate injection
    if "VOID BEAST QUANT NEWS + TELEGRAM UPGRADE MODULE" in source_text:
        print("Upgrade module already present in source; skipping injection.")
        return source_text
    # try to find a good spot: after first logger = logging.getLogger(...) or after imports
    m = re.search(r"logger\s*=\s*logging\.getLogger\([^)]+\)", source_text)
    if m:
        insert_at = source_text.find("\n", m.end()) + 1
    else:
        # find end of imports (first blank line after last import)
        m2 = re.search(r"(?:\n\n)", source_text)
        insert_at = m2.end() if m2 else 0
    new_text = source_text[:insert_at] + "\n\n" + UPGRADE_MODULE + "\n\n" + source_text[insert_at:]
    print("Injected upgrade module.")
    return new_text

def main():
    if not os.path.exists(INPUT_FILE):
        print("ERROR: input file not found:", INPUT_FILE)
        sys.exit(1)
    print("Reading:", INPUT_FILE)
    original = read_file(INPUT_FILE)
    print("Backing up original file...")
    bak = backup_file(INPUT_FILE)
    print("Backup created:", bak)
    source = original

    # Step 1: ensure bitcoin keywords in fund keywords dicts if any
    source = ensure_bitcoin_keywords_in_source(source)

    # Step 2: inject upgrade module
    source = inject_upgrade_module(source)

    # Step 3: replace or append get_recent_trades
    source = replace_or_append_get_recent_trades(source)

    # Step 4: patch adapt_and_optimize to compute winrate robustly
    source = patch_adapt_and_optimize_winrate(source)

    # Step 5: write output file
    write_file(OUTPUT_FILE, source)
    print("WROTE upgraded file:", OUTPUT_FILE)
    print("Please review the new file before running it. Recommended next steps:")
    print("  1) Activate your bot venv (where dependencies are installed)")
    print("  2) Install optional dependencies for Telegram/API polling:")
    print("       pip install telethon requests python-dotenv")
    print("  3) Configure env vars if you want telegram or API pollers to run:")
    print("       TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS")
    print("       RAPIDAPI_KEY, RAPIDAPI_ENDPOINT, NEWDATA_KEY, NEWSDATA_ENDPOINT")
    print("  4) Run the upgraded bot:")
    print("       python", OUTPUT_FILE)
    print("If anything errors or you want me to adjust the upgrade, paste the first ~50 lines of the upgraded file here and I'll patch it further.")

if __name__ == "__main__":
    main()
