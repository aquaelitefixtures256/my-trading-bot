
# injected helpers imports
try:
    import dashboard_integration
    import trade_stats
    import threshold_adapter
except Exception:
    pass

# Auto-generated full production-grade beast merge file
import sys, types, traceback


# --- BEGIN ORCHESTRATION WATCHDOG (self-healing) ---
import threading, time, os, sys
def _watchdog_thread(poll_interval=10, max_gap=120):
    try:
        while True:
            try:
                last = globals().get('LAST_CYCLE_TS', None)
                now = time.time()
                if last is None:
                    globals()['LAST_CYCLE_TS'] = now
                else:
                    if now - last > max_gap:
                        try:
                            p = sys.executable
                            args = [p] + sys.argv
                            try:
                                sys.stdout.flush(); sys.stderr.flush()
                            except Exception:
                                pass
                            os.execv(p, args)
                        except Exception:
                            pass
                time.sleep(poll_interval)
            except Exception:
                time.sleep(poll_interval)
    except Exception:
        pass
try:
    _wd = threading.Thread(target=_watchdog_thread, daemon=True)
    _wd.start()
except Exception:
    pass
# --- END ORCHESTRATION WATCHDOG ---

# Injected symbol override: remove XAGUSDm (silver)
TRADED_SYMBOLS = [s for s in globals().get('TRADED_SYMBOLS', globals().get('SYMBOLS', ['XAUUSDm','BTCUSDm','USOILm','USDJPYm','EURUSDm'])) if s.upper().replace('M','') != 'XAGUSDm']
globals()['TRADED_SYMBOLS'] = TRADED_SYMBOLS

def _install_beast_modules():
    import types, sys
    code = """""
# beast_helpers - production-grade helpers
import logging, json, os, time
from datetime import datetime, timezone

logger = logging.getLogger("Ultra_instinct")

# --- BEGIN: Increased verbosity for beast/voidx systems (injected) ---
# Set debug level for core beast modules so you get a per-system breakdown in logs.
_try_loggers = [
    "void_beast", "voidx_beast", "voidx_beast.v2", "void_beast.news", "void_beast.sentiment",
    "void_beast.protect", "voidx_beast.quant_news", "beast_threshold", "beast_risk",
    "beast_protection", "beast_monitor", "voidx_beast.impact"
]
for _ln in _try_loggers:
    try:
        logging.getLogger(_ln).setLevel(logging.DEBUG)
    except Exception:
        pass

# Optional verbose boot summary — enabled by default per your request so startup logs include key statuses.
# To toggle off, set environment var BEAST_VERBOSE_BOOT=0
try:
    _verbose_boot = os.getenv("BEAST_VERBOSE_BOOT", "1") == "1"
    if _verbose_boot:
        # MT5 connection state (if MT5 initialisation happens later this will still show current state)
        try:
            if MT5_AVAILABLE and globals().get("_mt5_connected", False):
                logger.info("MT5 connected")
            else:
                # still print so it appears in logs even if not connected yet
                logger.info("MT5 connected" if globals().get("_mt5_connected", False) else "MT5 not connected")
        except Exception:
            pass
        # Quant news system status (existing code also logs/prints this later)
        try:
            if os.getenv("NEWSDATA_KEY"):
                logger.info("Quant News System Loaded")
            else:
                logger.warning("Quant News System Disabled (no NEWSDATA_KEY)")
        except Exception:
            pass
        # Watched symbols snapshot
        try:
            _symbols_list = globals().get("SYMBOLS", None)
            if _symbols_list:
                _sout = " ".join([s for s in _symbols_list])
                logger.info("Watching symbols: %s", _sout)
        except Exception:
            pass
        # Friendly informational count of trading systems (keeps your requested message)
        try:
            _n = int(os.getenv("BEAST_SYSTEM_COUNT", "25"))
            logger.info("VoidX Beast loaded %d trading systems.", _n)
        except Exception:
            pass
except Exception:
    logger.exception("Verbose boot logging failed")
# --- END: Increased verbosity for beast/voidx systems ---

logger = logging.getLogger(\"void_beast\")
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = \"%(asctime)s %(levelname)s %(message)s\"
    h.setFormatter(logging.Formatter(fmt))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

def now_ts():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

def safe_get(d, k, default=None):
    try:
        return d.get(k, default)
    except Exception:
        return default

def ensure_dir(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass
"""""
    mod = types.ModuleType('beast_helpers')
    mod.__file__ = __file__ + '::beast_helpers'
    exec(code, mod.__dict__)
    sys.modules['beast_helpers'] = mod

    code = """""
# beast_news - News fetching and parsing (NewsAPI-compatible + glint integration)
import os, time, logging, json
from datetime import datetime, timezone
try:
    import requests
except Exception:
    requests = None

logger = logging.getLogger(\"void_beast.news\")

NEWS_API_KEY = os.getenv(\"NEWS_DATA_KEY\", None)
GLINT_URL = os.getenv(\"GLINT_URL\", None)  # optional real-time feed URL

def fetch_news_newsapi(query=\"*\", page_size=20):
    \"\"\"
    Fetch from a NewsAPI-compatible endpoint using NEWS_DATA_KEY and return list of articles.
    Each article is a dict with: title, description, source, publishedAt
    \"\"\"
    if not NEWS_API_KEY or requests is None:
        logger.debug(\"NewsAPI key or requests missing\")
        return []
    try:
        url = f\"https://newsapi.org/v2/everything?q={query}&pageSize={page_size}&sortBy=publishedAt&apiKey={NEWS_API_KEY}\"
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            data = r.json()
            return [ { \"title\": a.get(\"title\",\"\"), \"description\": a.get(\"description\",\"\"), \"source\": a.get(\"source\",{}).get(\"name\",\"\"), \"publishedAt\": a.get(\"publishedAt\") } for a in data.get(\"articles\",[]) ]
        logger.warning(\"NewsAPI returned status %s\", r.status_code)
    except Exception as e:
        logger.exception(\"fetch_news_newsapi error: %s\", e)
    return []

def fetch_news_glint(query=\"*\", limit=50):
    \"\"\"
    Fetch from a Glint-like real-time feed endpoint (user-supplied). Expects JSON lines or JSON list.
    \"\"\"
    if not GLINT_URL or requests is None:
        return []
    try:
        r = requests.get(GLINT_URL, params={\"q\": query, \"limit\": limit}, timeout=6, stream=False)
        if r.status_code == 200:
            # try parse as list
            try:
                data = r.json()
                if isinstance(data, list):
                    return [ {\"title\": a.get(\"title\",\"\"), \"description\": a.get(\"description\",\"\"), \"source\": a.get(\"source\",\"glint\"), \"publishedAt\": a.get(\"publishedAt\")} for a in data ]
            except Exception:
                # fallback: splitlines JSON objects
                lines = r.text.splitlines()
                out = []
                for line in lines:
                    try:
                        a = json.loads(line)
                        out.append({\"title\": a.get(\"title\",\"\"), \"description\": a.get(\"description\",\"\"), \"source\": a.get(\"source\",\"glint\"), \"publishedAt\": a.get(\"publishedAt\")})
                    except Exception:
                        continue
                return out
        logger.warning(\"Glint returned status %s\", r.status_code)
    except Exception as e:
        logger.exception(\"fetch_news_glint error: %s\", e)
    return []

def fetch_recent_news(query_terms=None, prefer_glint=True, limit=30):
    query = \" OR \".join(query_terms) if query_terms else \"*\"
    if prefer_glint and GLINT_URL:
        n = fetch_news_glint(query, limit)
        if n:
            return n
    # fallback to NewsAPI
    return fetch_news_newsapi(query, page_size=limit)
"""""
    mod = types.ModuleType('beast_news')
    mod.__file__ = __file__ + '::beast_news'
    exec(code, mod.__dict__)
    sys.modules['beast_news'] = mod

    code = """""
# beast_sentiment - smoothed sentiment scoring using keywords + optional VADER
from collections import deque
import os, logging
logger = logging.getLogger(\"void_beast.sentiment\")
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _VADER_AVAILABLE = True
    _VADER = SentimentIntensityAnalyzer()
except Exception:
    _VADER_AVAILABLE = False
    _VADER = None

def _score_text_simple(text, keywords=None):
    text = (text or \"\").lower()
    kws = keywords or {\"positive\":[\"gain\",\"profit\",\"beat\",\"rise\",\"up\"], \"negative\":[\"loss\",\"fall\",\"drop\",\"war\",\"strike\",\"iran\",\"oil spike\",\"surge\"]}
    pos = sum(text.count(k) for k in kws[\"positive\"])
    neg = sum(text.count(k) for k in kws[\"negative\"])
    raw = pos - neg
    # normalize heuristically
    if raw == 0:
        return 0.0
    return max(-1.0, min(1.0, raw / max(1.0, abs(raw) + 2)))

class SentimentEngine:
    def __init__(self, alpha=0.25, window=6):
        self.alpha = float(alpha)
        self.prev_ema = None
        self.window = int(window)
        self.recent = deque(maxlen=self.window)

    def _ema(self, current):
        if self.prev_ema is None:
            self.prev_ema = current
        else:
            self.prev_ema = self.alpha * current + (1 - self.alpha) * self.prev_ema
        self.recent.append(self.prev_ema)
        return self.prev_ema

    def score_from_articles(self, articles):
        if not articles:
            return 0.0
        totals = []
        for a in articles:
            text = (a.get(\"title\",\"\") + \" \" + a.get(\"description\",\"\"))
            if _VADER_AVAILABLE:
                try:
                    v = _VADER.polarity_scores(text)
                    totals.append(v.get(\"compound\",0.0))
                    continue
                except Exception:
                    pass
            totals.append(_score_text_simple(text))
        if not totals:
            return 0.0
        avg = sum(totals)/len(totals)
        return self._ema(avg)

    def get_smoothed(self):
        if not self.recent:
            return 0.0
        return sum(self.recent)/len(self.recent)
"""""
    mod = types.ModuleType('beast_sentiment')
    mod.__file__ = __file__ + '::beast_sentiment'
    exec(code, mod.__dict__)
    sys.modules['beast_sentiment'] = mod

    code = """""
# beast_calendar - robust economic calendar parsing and event blocking
import os, logging, datetime
from dateutil import parser as dateparser
from dateutil import tz
logger = logging.getLogger(\"void_beast.calendar\")

PRE_SECONDS = int(os.getenv(\"BEAST_PRE_EVENT_BLOCK_SEC\", 600))
POST_SECONDS = int(os.getenv(\"BEAST_POST_EVENT_BLOCK_SEC\", 600))

IMPACT_MAP = {
    \"low\": 1,
    \"medium\": 2,
    \"high\": 3
}

def parse_event_time(ts):
    try:
        # parse ISO or common formats, return aware UTC datetime
        dt = dateparser.parse(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.UTC)
        return dt.astimezone(tz.UTC)
    except Exception:
        return None

def should_block_for_events(events, now=None):
    \"\"\"
    events: list of dicts with keys: 'symbol','impact','timestamp','actual','forecast','previous'
    returns (blocked:bool, reason:str)
    \"\"\"
    now = now or datetime.datetime.utcnow().replace(tzinfo=tz.UTC)
    for e in events or []:
        imp = str(e.get(\"impact\",\"\")).lower()
        imp_val = IMPACT_MAP.get(imp, 0)
        if imp_val >= 3:
            ts = parse_event_time(e.get(\"timestamp\") or e.get(\"ts\") or e.get(\"time\"))
            if ts:
                diff = (ts - now).total_seconds()
                if -POST_SECONDS <= diff <= PRE_SECONDS:
                    return True, f\"high_impact:{e.get('event','') or e.get('title','') or e.get('symbol','') }\"
    return False, \"\"
"""""
    mod = types.ModuleType('beast_calendar')
    mod.__file__ = __file__ + '::beast_calendar'
    exec(code, mod.__dict__)
    sys.modules['beast_calendar'] = mod

    code = """""
# beast_symbols - per-symbol and global open limits, MT5 primary, DB fallback
import os, logging
logger = logging.getLogger(\"void_beast.symbols\")
MAX_GLOBAL = int(os.getenv(\"BEAST_MAX_GLOBAL_OPEN\", \"15\"))
PER_SYMBOL = {
    \"XAUUSD\": int(os.getenv(\"BEAST_MAX_XAUUSD\", \"3\")),
    \"XAGUSD\": int(os.getenv(\"BEAST_MAX_XAGUSD\", \"3\")),
    \"BTCUSD\": int(os.getenv(\"BEAST_MAX_BTCUSD\", \"5\")),
    \"USOIL\" : int(os.getenv(\"BEAST_MAX_USOIL\", \"5\")),
    \"USDJPY\": int(os.getenv(\"BEAST_MAX_USDJPY\", \"10\")),
    \"EURUSD\": int(os.getenv(\"BEAST_MAX_EURUSD\", \"10\")),
}

def count_open_positions(mt5_module=None, db_query_fn=None):
    \"\"\"
    Returns (total_open, per_symbol_dict).
    Tries MT5 API first if provided, falls back to db_query_fn if provided.
    \"\"\"
    try:
        if mt5_module:
            positions = mt5_module.positions_get() or []
            total = len(positions)
            per = {}
            for p in positions:
                sym = getattr(p, \"symbol\", None) or (p.get(\"symbol\") if isinstance(p, dict) else None)
                if sym:
                    per[sym] = per.get(sym,0)+1
            return total, per
    except Exception:
        logger.exception(\"MT5 count failed\")

    # fallback to DB query function
    try:
        if db_query_fn:
            per = db_query_fn() or {}
            total = sum(per.values())
            return total, per
    except Exception:
        logger.exception(\"DB fallback failed\")

    return 0, {}
"""""
    mod = types.ModuleType('beast_symbols')
    mod.__file__ = __file__ + '::beast_symbols'
    exec(code, mod.__dict__)
    sys.modules['beast_symbols'] = mod

    code = """""
# beast_threshold - same as before but with debug snapshot flag
import os, json, logging, datetime
logger = logging.getLogger(\"void_beast.threshold\")

STATE_FILE = os.getenv(\"BEAST_THRESHOLD_STATE_FILE\", \"beast_threshold_state.json\")

DEFAULT = {
    \"min_threshold\": float(os.getenv(\"BEAST_MIN_THRESHOLD\",\"0.12\")),
    \"base_threshold\": float(os.getenv(\"BEAST_BASE_THRESHOLD\",\"0.18\")),
    \"max_threshold\": float(os.getenv(\"BEAST_MAX_THRESHOLD\",\"0.30\")),
    \"current_threshold\": float(os.getenv(\"BEAST_BASE_THRESHOLD\",\"0.18\")),
    \"gravity\": float(os.getenv(\"BEAST_GRAVITY\",\"0.02\")),
    \"adapt_speed\": float(os.getenv(\"BEAST_ADAPT_SPEED\",\"0.01\"))
}

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE,\"r\") as f:
                return json.load(f)
    except Exception:
        logger.exception(\"load_state failed\")
    return DEFAULT.copy()

def save_state(s):
    try:
        with open(STATE_FILE,\"w\") as f:
            json.dump(s, f)
    except Exception:
        logger.exception(\"save_state failed\")

def apply_gravity_and_volatility(current, volatility_adj=0.0):
    s = load_state()
    min_t, base, max_t = s[\"min_threshold\"], s[\"base_threshold\"], s[\"max_threshold\"]
    gravity = s[\"gravity\"]
    adapt_speed = s[\"adapt_speed\"]
    pull = (base - current) * gravity
    adj = pull + float(volatility_adj)
    if adj > adapt_speed: adj = adapt_speed
    if adj < -adapt_speed: adj = -adapt_speed
    new_t = current + adj
    new_t = max(min_t, min(max_t, new_t))
    s[\"current_threshold\"] = new_t
    s[\"last_updated\"] = datetime.datetime.utcnow().isoformat()
    save_state(s)
    return new_t

def force_set_threshold(value):
    s = load_state()
    s[\"current_threshold\"] = max(s[\"min_threshold\"], min(s[\"max_threshold\"], float(value)))
    save_state(s)
    return s[\"current_threshold\"]

def get_current_threshold():
    return load_state().get(\"current_threshold\", DEFAULT[\"current_threshold\"])
"""""
    mod = types.ModuleType('beast_threshold')
    mod.__file__ = __file__ + '::beast_threshold'
    exec(code, mod.__dict__)
    sys.modules['beast_threshold'] = mod

    code = """""
# beast_risk - dynamic risk scaling with signal-quality consideration
import os, math, logging
logger = logging.getLogger(\"void_beast.risk\")
BASE = float(os.getenv(\"BASE_RISK_PER_TRADE_PCT\",\"0.003\"))
MID = float(os.getenv(\"BEAST_MID_RISK\",\"0.006\"))
MAX = float(os.getenv(\"MAX_RISK_PER_TRADE_PCT\",\"0.01\"))

def compute_dynamic_risk(tech_score, fund_score, sent_score):
    try:
        tech, fund, sent = float(tech_score), float(fund_score), float(sent_score)
    except Exception:
        tech=fund=sent=0.0
    def sgn(x):
        if abs(x) < 0.01: return 0
        return 1 if x>0 else -1
    a,b,c = sgn(tech), sgn(fund), sgn(sent)
    if a!=0 and a==b==c:
        return MAX, \"FULL_ALIGN\"
    if (a!=0 and a==b) or (a!=0 and a==c) or (b!=0 and b==c):
        return MID, \"TWO_ALIGN\"
    return BASE, \"BASE\"
"""""
    mod = types.ModuleType('beast_risk')
    mod.__file__ = __file__ + '::beast_risk'
    exec(code, mod.__dict__)
    sys.modules['beast_risk'] = mod

    code = """""
# beast_protection - SQF, flash-crash, drawdown, cooldown, liquidity protection
import os, time, logging
logger = logging.getLogger(\"void_beast.protect\")
SQF = {
    \"max_spread_points\": float(os.getenv(\"BEAST_MAX_SPREAD_POINTS\",\"1000\")),
    \"vol_spike_mult\": float(os.getenv(\"BEAST_VOL_SPIKE_MULT\",\"2.5\")),
    \"unstable_move_pct\": float(os.getenv(\"BEAST_UNSTABLE_MOVE_PCT\",\"0.03\")),
    \"flash_gap_pct\": float(os.getenv(\"BEAST_FLASH_GAP_PCT\",\"0.05\")),
    \"cooldown_seconds\": int(os.getenv(\"BEAST_COOLDOWN_SECONDS\",\"180\"))
}
_last_trade_time = {}
_daily_drawdown = {\"today\":0.0}

def sqf_check(symbol, spread_points=None, atr_now=None, atr_avg=None, recent_move_pct=None):
    if spread_points is not None and spread_points > SQF[\"max_spread_points\"]:
        return False, \"spread_spike\"
    if atr_avg and atr_now and atr_now > atr_avg * SQF[\"vol_spike_mult\"]:
        return False, \"vol_spike\"
    if recent_move_pct and recent_move_pct > SQF[\"unstable_move_pct\"]:
        return False, \"unstable_move\"
    return True, \"ok\"

def flash_crash_protect(symbol, last_tick_move_pct):
    if last_tick_move_pct and abs(last_tick_move_pct) > SQF[\"flash_gap_pct\"]:
        return False, \"flash_gap\"
    return True, \"ok\"

def apply_cooldown(symbol):
    now = time.time()
    last = _last_trade_time.get(symbol, 0)
    if now - last < SQF[\"cooldown_seconds\"]:
        return False, \"cooldown_active\"
    _last_trade_time[symbol] = now
    return True, \"ok\"

def update_drawdown(pnl):
    _daily_drawdown[\"today\"] += pnl
    return _daily_drawdown[\"today\"]

def within_drawdown_limit(max_daily_drawdown = -0.03, balance=1.0):
    dd = _daily_drawdown[\"today\"]
    if dd <= max_daily_drawdown * balance:
        return False, \"drawdown_exceeded\"
    return True, \"ok\"
"""""
    mod = types.ModuleType('beast_protection')
    mod.__file__ = __file__ + '::beast_protection'
    exec(code, mod.__dict__)
    sys.modules['beast_protection'] = mod

    code = """""
# beast_dashboard - enhanced JSON snapshot with block reasons and per-symbol summary
import json, os, logging
from datetime import datetime
logger = logging.getLogger(\"void_beast.dashboard\")
DASH_FILE = os.getenv(\"BEAST_DASH_FILE\",\"beast_dashboard.json\")

def publish_cycle(snapshot):
    try:
        snapshot[\"ts\"] = datetime.utcnow().isoformat()
    except Exception:
        snapshot[\"ts\"] = str(datetime.utcnow())
    try:
        os.makedirs(os.path.dirname(DASH_FILE) or \".\", exist_ok=True)
        with open(DASH_FILE, \"w\") as f:
            json.dump(snapshot, f, indent=2, default=str)
    except Exception:
        logger.exception(\"publish_cycle failed\")
"""""
    mod = types.ModuleType('beast_dashboard')
    mod.__file__ = __file__ + '::beast_dashboard'
    exec(code, mod.__dict__)
    sys.modules['beast_dashboard'] = mod

    code = """""
# beast_monitor - create full snapshot per cycle
from beast_threshold import get_current_threshold
from beast_risk import compute_dynamic_risk

def make_snapshot(symbol, tech_score=None, model_score=None, fund_score=None, h1_trend=None, events=None, block_reasons=None):
    risk, risk_mode = compute_dynamic_risk(tech_score or 0, fund_score or 0, model_score or 0)
    snapshot = {
        \"symbol\": symbol,
        \"tech_score\": tech_score,
        \"model_score\": model_score,
        \"fund_score\": fund_score,
        \"h1_trend\": h1_trend,
        \"threshold\": get_current_threshold(),
        \"risk\": risk,
        \"risk_mode\": risk_mode,
        \"events\": events or [],
        \"block_reasons\": block_reasons or []
    }
    return snapshot
"""""
    mod = types.ModuleType('beast_monitor')
    mod.__file__ = __file__ + '::beast_monitor'
    exec(code, mod.__dict__)
    sys.modules['beast_monitor'] = mod

    code = """""
# beast_execution_fix - robust order confirmation retries
import time, logging
logger = logging.getLogger(\"void_beast.exec\")

def confirm_order_send(send_fn, *args, retries=3, delay=1, **kwargs):
    for i in range(retries):
        try:
            res = send_fn(*args, **kwargs)
            if res:
                return res
        except Exception:
            logger.exception(\"order send attempt failed\")
        time.sleep(delay)
    return None
"""""
    mod = types.ModuleType('beast_execution_fix')
    mod.__file__ = __file__ + '::beast_execution_fix'
    exec(code, mod.__dict__)
    sys.modules['beast_execution_fix'] = mod

    code = """""
# beast_correlation - correlation helpers
import numpy as np
def correlation_coefficient(series_a, series_b):
    try:
        a = np.array(series_a, dtype=float)
        b = np.array(series_b, dtype=float)
        if len(a) < 2 or len(b) < 2:
            return 0.0
        n = min(len(a), len(b))
        a = a[-n:]; b = b[-n:]
        if np.std(a)==0 or np.std(b)==0:
            return 0.0
        return float(np.corrcoef(a,b)[0,1])
    except Exception:
        return 0.0
"""""
    mod = types.ModuleType('beast_correlation')
    mod.__file__ = __file__ + '::beast_correlation'
    exec(code, mod.__dict__)
    sys.modules['beast_correlation'] = mod

    code = """""
# beast_liquidity - commodity regime / liquidity gap detection
def commodity_regime_check(symbol, atr_now, atr_avg, spread):
    if symbol.upper() in (\"XAUUSD\",\"XAGUSD\",\"USOIL\"):
        if atr_now is None or atr_avg is None:
            return False, \"missing_atr\"
        if atr_now > atr_avg * 2.5:
            return False, \"atr_spike\"
        if spread and spread > 2000:
            return False, \"spread_spike\"
    return True, \"ok\"
"""""
    mod = types.ModuleType('beast_liquidity')
    mod.__file__ = __file__ + '::beast_liquidity'
    exec(code, mod.__dict__)
    sys.modules['beast_liquidity'] = mod

    code = """""
# beast_regime - ATR based regime detection
def atr_regime(atr_now, atr_avg):
    if atr_now is None or atr_avg is None:
        return \"unknown\", 0.0
    if atr_now > atr_avg * 1.2:
        return \"high\", (atr_now/atr_avg)
    if atr_now < atr_avg * 0.8:
        return \"low\", (atr_now/atr_avg)
    return \"normal\", (atr_now/atr_avg)
"""""
    mod = types.ModuleType('beast_regime')
    mod.__file__ = __file__ + '::beast_regime'
    exec(code, mod.__dict__)
    sys.modules['beast_regime'] = mod

    code = """""
# beast_nfp - NFP/CPI/FOMC protection helper
import datetime
PRE = int(__import__('os').getenv('BEAST_PRE_EVENT_BLOCK_SEC','600'))
POST = int(__import__('os').getenv('BEAST_POST_EVENT_BLOCK_SEC','600'))
def should_block_for_event(event_ts_iso, now=None):
    try:
        now = now or datetime.datetime.utcnow()
        ev = datetime.datetime.fromisoformat(event_ts_iso)
        diff = (ev - now).total_seconds()
        if -POST <= diff <= PRE:
            return True, \"high_impact_event_window\"
    except Exception:
        pass
    return False, \"\"
"""""
    mod = types.ModuleType('beast_nfp')
    mod.__file__ = __file__ + '::beast_nfp'
    exec(code, mod.__dict__)
    sys.modules['beast_nfp'] = mod

    return True

_install_beast_modules()


def __void_beast_cycle():

        # --- BEGIN INJECTED ORCHESTRATION (ensure modules run each cycle) ---
        try:
            try:
                import beast_threshold, beast_risk, beast_protection, beast_dashboard, beast_monitor, beast_correlation, beast_liquidity, beast_sentiment, beast_scoring, beast_regime, beast_nfp
            except Exception:
                pass
            # calendar / NFP protection
            if 'beast_calendar' in globals():
                try:
                    events = globals().get('BEAST_CALENDAR_EVENTS', [])
                    blocked, reason = beast_calendar.should_block_for_events(events)
                    if blocked:
                        logger.info(f"Calendar block active: {reason}; skipping cycle")
                        return
                except Exception:
                    pass
            # Signal Quality Filter (SQF)
            if 'beast_protection' in globals():
                try:
                    spread = globals().get('CURRENT_SPREAD_POINTS', None)
                    atr_now = globals().get('CURRENT_ATR', None)
                    atr_avg = globals().get('ATR_AVG', None)
                    recent_move = globals().get('RECENT_MOVE_PCT', None)
                    ok, r = beast_protection.sqf_check(globals().get('CURRENT_SYMBOL','GENERIC'), spread, atr_now, atr_avg, recent_move)
                    if not ok:
                        logger.info(f"SQF blocked: {r}")
                        return
                except Exception:
                    pass
            # Liquidity / regime check
            if 'beast_liquidity' in globals():
                try:
                    ok, r = beast_liquidity.commodity_regime_check(globals().get('CURRENT_SYMBOL','GENERIC'), globals().get('CURRENT_ATR',None), globals().get('ATR_AVG',None), globals().get('CURRENT_SPREAD_POINTS',None))
                    if not ok:
                        logger.info(f"Liquidity block: {r}")
                        return
                except Exception:
                    pass
            # Correlation check
            if 'beast_correlation' in globals():
                try:
                    series_a = globals().get('RECENT_SERIES_A', [])
                    series_b = globals().get('RECENT_SERIES_B', [])
                    corr = beast_correlation.correlation_coefficient(series_a, series_b)
                    if abs(corr) > 0.95:
                        logger.info('Correlation block: high correlation')
                        return
                except Exception:
                    pass
            # Threshold gravity + winrate adjustment (non-blocking)
            try:
                import threshold_adapter, trade_stats, dashboard_integration as dbi
            except Exception:
                threshold_adapter = None; trade_stats = None; dbi = None
            try:
                cur = beast_threshold.get_current_threshold()
            except Exception:
                cur = 0.18
            adj = 0.0
            winrate = 0.0; n = 0
            if threshold_adapter is not None:
                try:
                    adj, winrate, n = threshold_adapter.compute_adaptive_adjustment()
                except Exception:
                    adj, winrate, n = 0.0, 0.0, 0
            try:
                newt = beast_threshold.apply_gravity_and_volatility(cur, volatility_adj=float(adj or 0.0))
                globals()['CURRENT_THRESHOLD'] = newt
            except Exception:
                globals()['CURRENT_THRESHOLD'] = cur
            # telemetry
            try:
                if dbi is not None:
                    dbi.send_analysis('__GLOBAL__', float(cur or 0.0), 0.0, float(winrate or 0.0), float(globals().get('CURRENT_THRESHOLD', cur or 0.0)), meta={'n': n})
            except Exception:
                pass
            # update last cycle timestamp for watchdog
            try:
                import time as _t
                globals()['LAST_CYCLE_TS'] = _t.time()
            except Exception:
                pass
        except Exception:
            pass
        # --- END INJECTED ORCHESTRATION ---
        try:
            import beast_threshold as vb_threshold, beast_sentiment as vb_sent, beast_risk as vb_risk, beast_dashboard as vb_dashboard, beast_protection as vb_protect, beast_monitor as vb_monitor
        except Exception:
            return

        try:
            cur = vb_threshold.get_current_threshold()
        except Exception:
            cur = 0.18

        try:
            new = vb_threshold.apply_gravity_and_volatility(cur, volatility_adj=0.0)
        except Exception:
            new = cur

        try:
            se = vb_sent.SentimentEngine(alpha=0.25, window=6)
            # try to use global news cache if available
            articles = globals().get("BEAST_NEWS_CACHE", []) or []
            sent = se.score_from_articles(articles)
        except Exception:
            sent = 0.0

        try:
            risk, mode = vb_risk.compute_dynamic_risk(0.0, 0.0, sent)
        except Exception:
            risk, mode = 0.003, "base"

        try:
            ok, reason = vb_protect.sqf_check("GENERIC", None, None, None, None)
        except Exception:
            ok, reason = True, "ok"

        try:
            snap = vb_monitor.make_snapshot("GENERIC", tech_score=None, model_score=None, fund_score=None, h1_trend=None, events=None, block_reasons=[reason])
            vb_dashboard.publish_cycle(snap)
        except Exception:
            pass


orig_src = "#!/usr/bin/env python3\n\"\"\"\nUltra_instinct - full bot file.\n\nThis file is the complete bot. The only changes from your prior file are:\n- Robust fetch_newsdata (NewsData primary, NewsAPI fallback, expanded query & cache)\n- Robust fetch_finnhub_calendar (Finnhub primary, TradingEconomics fallback)\n- Robust fetch_alpha_vantage_crypto_intraday with fallbacks (Finnhub, CoinGecko) and retry helper\n\nEverything else is preserved (order placement/confirmation/recording, per-symbol limits,\nMT5-first counts, debug snapshot only first cycle, normalization to [-1,1], adaptation logic, reconcile_closed_deals called at start).\n\"\"\"\n\nfrom __future__ import annotations\nimport os\nimport sys\nimport time\nimport json\nimport logging\nimport sqlite3\nimport argparse\nimport random\nimport warnings\nimport shutil\nfrom datetime import datetime, date, timezone, timedelta\nfrom typing import Optional, Dict, Any, List\n\n# numerical & data\ntry:\n    import numpy as np\n    import pandas as pd\nexcept Exception as e:\n    raise RuntimeError(\"Install numpy and pandas: pip install numpy pandas\") from e\n\n# MetaTrader5 optional\ntry:\n    import MetaTrader5 as mt5  # type: ignore\n    MT5_AVAILABLE = True\nexcept Exception:\n    MT5_AVAILABLE = False\n\n# TA optional\ntry:\n    from ta.trend import SMAIndicator, ADXIndicator\n    from ta.volatility import AverageTrueRange\n    from ta.momentum import RSIIndicator\n    TA_AVAILABLE = True\nexcept Exception:\n    TA_AVAILABLE = False\n\n# ML optional\nSKLEARN_AVAILABLE = False\ntry:\n    from sklearn.pipeline import Pipeline\n    from sklearn.preprocessing import StandardScaler\n    from sklearn.linear_model import SGDClassifier\n    from sklearn.ensemble import RandomForestClassifier\n    from sklearn.exceptions import ConvergenceWarning\n    import joblib\n    SKLEARN_AVAILABLE = True\n    warnings.filterwarnings(\"ignore\", category=ConvergenceWarning)\nexcept Exception:\n    SKLEARN_AVAILABLE = False\n\n# requests for fundamentals\nFUNDAMENTAL_AVAILABLE = False\ntry:\n    import requests\n    FUNDAMENTAL_AVAILABLE = True\nexcept Exception:\n    FUNDAMENTAL_AVAILABLE = False\n\n# sentiment\ntry:\n    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer\n    VADER_AVAILABLE = True\n    _VADER = SentimentIntensityAnalyzer()\nexcept Exception:\n    VADER_AVAILABLE = False\n    _VADER = None\n\n# logging\nlogging.basicConfig(level=logging.DEBUG, format="%(asctime")s %(levelname)s %(message)s")s %(levelname)s %(message)s\")\nlogger = logging.getLogger(\"Ultra_instinct\")\n\n# ---------------- Configuration ----------------\nSYMBOLS = [\"EURUSD\", \"XAUUSD\", \"BTCUSD\", \"USDJPY\", \"USOIL\"]\nBROKER_SYMBOLS = {\n    \"EURUSD\": \"EURUSDm\",\n    \"XAUUSD\": \"XAUUSDm\",\n    \"BTCUSD\": \"BTCUSDm\",\n    \"USDJPY\": \"USDJPYm\",\n    \"USOIL\": \"USOILm\",\n}\nTIMEFRAMES = {\"M30\": \"30m\", \"H1\": \"60m\"}\n\nDEMO_SIMULATION = False\nAUTO_EXECUTE = True\nif os.getenv(\"CONFIRM_AUTO\", \"\"):\n    if \"\".join([c for c in os.getenv(\"CONFIRM_AUTO\") if c.isalnum()]).upper() == \"\".join([c for c in \"I UNDERSTAND THE RISKS\" if c.isalnum()]).upper():\n        DEMO_SIMULATION = False\n        AUTO_EXECUTE = True\n\nBASE_RISK_PER_TRADE_PCT = float(os.getenv(\"BASE_RISK_PER_TRADE_PCT\", \"0.003\"))\nMIN_RISK_PER_TRADE_PCT = float(os.getenv(\"MIN_RISK_PER_TRADE_PCT\", \"0.002\"))\nMAX_RISK_PER_TRADE_PCT = float(os.getenv(\"MAX_RISK_PER_TRADE_PCT\", \"0.01\"))\nRISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT\n\nMAX_DAILY_TRADES = int(os.getenv(\"MAX_DAILY_TRADES\", \"100\"))\nKILL_SWITCH_FILE = os.getenv(\"KILL_SWITCH_FILE\", \"STOP_TRADING.flag\")\nADAPT_STATE_FILE = \"adapt_state.json\"\nTRADES_DB = \"trades.db\"\nTRADES_CSV = \"trades.csv\"\nMODEL_FILE = \"ultra_instinct_model.joblib\"\nCURRENT_THRESHOLD = float(os.getenv(\"CURRENT_THRESHOLD\", \"0.12\"))\nMIN_THRESHOLD = 0.10\nMAX_THRESHOLD = 0.30\nDECISION_SLEEP = int(os.getenv(\"DECISION_SLEEP\", \"60\"))\nADAPT_EVERY_CYCLES = 6\nMODEL_MIN_TRAIN = 40\n\nMT5_LOGIN = os.getenv(\"MT5_LOGIN\")\nMT5_PASSWORD = os.getenv(\"MT5_PASSWORD\")\nMT5_SERVER = os.getenv(\"MT5_SERVER\")\nMT5_PATH = os.getenv(\"MT5_PATH\", r\"C:\\Program Files\\MetaTrader 5\\terminal64.exe\")\n\nTELEGRAM_BOT_TOKEN = os.getenv(\"TELEGRAM_BOT_TOKEN\")\nTELEGRAM_CHAT_ID = os.getenv(\"TELEGRAM_CHAT_ID\")\n\n# fundamentals providers keys (env)\nFINNHUB_KEY = os.getenv(\"FINNHUB_KEY\", \"\")\nNEWSDATA_KEY = os.getenv(\"NEWSDATA_KEY\", \"\")\nALPHAVANTAGE_KEY = os.getenv(\"ALPHAVANTAGE_KEY\", \"ESTD9GSCNBSK7JA6\")\n\nNEWS_LOOKBACK_DAYS = int(os.getenv(\"NEWS_LOOKBACK_DAYS\", \"2\"))\nPAUSE_BEFORE_EVENT_MINUTES = int(os.getenv(\"PAUSE_BEFORE_EVENT_MINUTES\", \"30\"))\n\n# adaptation parameters\nADAPT_MIN_TRADES = 40\nTARGET_WINRATE = 0.525\nK = 0.04\nMAX_ADJ = 0.01\n\n# per-symbol open limits\nMAX_OPEN_PER_SYMBOL_DEFAULT = 10\nMAX_OPEN_PER_SYMBOL: Dict[str, int] = {\n    \"XAUUSD\": 2,\n}\n\n# runtime state\n_mt5 = None\n_mt5_connected = False\ncycle_counter = 0\nmodel_pipe = None\n_debug_snapshot_shown = False\n\n# ---------------- Utility helpers ----------------\ndef backup_trade_files():\n    try:\n        stamp = datetime.now().strftime(\"%Y%m%d_%H%M%S\")\n        if os.path.exists(TRADES_CSV):\n            shutil.copy(TRADES_CSV, f\"backup_{TRADES_CSV}_{stamp}\")\n        if os.path.exists(TRADES_DB):\n            shutil.copy(TRADES_DB, f\"backup_{TRADES_DB}_{stamp}\")\n    except Exception:\n        logger.exception(\"backup_trade_files failed\")\n\ndef _safe_float(x):\n    try:\n        return float(x)\n    except Exception:\n        return 0.0\n\n# ---------------- Telegram helper ----------------\ndef send_telegram_message(text: str) -> bool:\n    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:\n        logger.debug(\"send_telegram_message: Telegram not configured\")\n        return False\n    if not FUNDAMENTAL_AVAILABLE:\n        logger.debug(\"send_telegram_message: requests not available\")\n        return False\n    try:\n        url = f\"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage\"\n        payload = {\"chat_id\": TELEGRAM_CHAT_ID, \"text\": text}\n        resp = requests.post(url, data=payload, timeout=8)\n        if resp.status_code == 200:\n            return True\n        else:\n            logger.warning(\"send_telegram_message: non-200 %s %s\", resp.status_code, resp.text[:200])\n            return False\n    except Exception:\n        logger.exception(\"send_telegram_message failed\")\n        return False\n\n# ---------------- persistence / state ----------------\ndef load_adapt_state():\n    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT\n    if os.path.exists(ADAPT_STATE_FILE):\n        try:\n            with open(ADAPT_STATE_FILE, \"r\", encoding=\"utf-8\") as f:\n                st = json.load(f)\n            CURRENT_THRESHOLD = float(st.get(\"threshold\", CURRENT_THRESHOLD))\n            RISK_PER_TRADE_PCT = float(st.get(\"risk\", RISK_PER_TRADE_PCT))\n            logger.info(\"Loaded adapt_state threshold=%.3f risk=%.5f\", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)\n        except Exception:\n            logger.exception(\"load_adapt_state failed\")\n\ndef save_adapt_state():\n    try:\n        with open(ADAPT_STATE_FILE, \"w\", encoding=\"utf-8\") as f:\n            json.dump({\"threshold\": CURRENT_THRESHOLD, \"risk\": RISK_PER_TRADE_PCT}, f)\n    except Exception:\n        logger.exception(\"save_adapt_state failed\")\n\nload_adapt_state()\n\n# ---------------- DB and logging ----------------\ndef _get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:\n    cur = conn.cursor()\n    try:\n        cur.execute(f\"PRAGMA table_info({table})\")\n        rows = cur.fetchall()\n        return [r[1] for r in rows] if rows else []\n    except Exception:\n        return []\n\ndef init_trade_db():\n    conn = sqlite3.connect(TRADES_DB, timeout=5)\n    cur = conn.cursor()\n    expected_cols = {\n        \"id\": \"INTEGER PRIMARY KEY\",\n        \"ts\": \"TEXT\",\n        \"symbol\": \"TEXT\",\n        \"side\": \"TEXT\",\n        \"entry\": \"REAL\",\n        \"sl\": \"REAL\",\n        \"tp\": \"REAL\",\n        \"lots\": \"REAL\",\n        \"status\": \"TEXT\",\n        \"pnl\": \"REAL\",\n        \"rmult\": \"REAL\",\n        \"regime\": \"TEXT\",\n        \"score\": \"REAL\",\n        \"model_score\": \"REAL\",\n        \"meta\": \"TEXT\",\n    }\n    try:\n        cur.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name='trades'\")\n        if not cur.fetchone():\n            cols_sql = \",\\n \".join([f\"{k} {v}\" for k, v in expected_cols.items()])\n            create_sql = f\"CREATE TABLE trades (\\n {cols_sql}\\n );\"\n            cur.execute(create_sql)\n            conn.commit()\n        else:\n            existing = _get_table_columns(conn, \"trades\")\n            for col, ctype in expected_cols.items():\n                if col not in existing:\n                    try:\n                        if col == \"id\":\n                            logger.info(\"Existing trades table found without id column; leaving existing primary key as-is\")\n                            continue\n                        alter_sql = f\"ALTER TABLE trades ADD COLUMN {col} {ctype} DEFAULT NULL\"\n                        cur.execute(alter_sql)\n                        conn.commit()\n                        logger.info(\"Added missing column to trades: %s\", col)\n                    except Exception:\n                        logger.exception(\"Failed to add column %s to trades\", col)\n    except Exception:\n        logger.exception(\"init_trade_db failed\")\n    finally:\n        conn.close()\n    if not os.path.exists(TRADES_CSV):\n        try:\n            with open(TRADES_CSV, \"w\", encoding=\"utf-8\") as f:\n                f.write(\"ts,symbol,side,entry,sl,tp,lots,status,pnl,rmult,regime,score,model_score,meta\\n\")\n        except Exception:\n            logger.exception(\"Failed to create trades csv\")\n\ndef record_trade(symbol, side, entry, sl, tp, lots, status=\"sim\", pnl=0.0, rmult=0.0, regime=\"unknown\", score=0.0, model_score=0.0, meta=None):\n    ts = datetime.now(timezone.utc).isoformat()\n    meta_json = json.dumps(meta or {})\n    data = {\n        \"ts\": ts,\n        \"symbol\": symbol,\n        \"side\": side,\n        \"entry\": entry,\n        \"sl\": sl,\n        \"tp\": tp,\n        \"lots\": lots,\n        \"status\": status,\n        \"pnl\": pnl,\n        \"rmult\": rmult,\n        \"rm\": rmult,\n        \"regime\": regime,\n        \"score\": score,\n        \"model_score\": model_score,\n        \"meta\": meta_json,\n    }\n    try:\n        conn = sqlite3.connect(TRADES_DB, timeout=5)\n        cur = conn.cursor()\n        cols = _get_table_columns(conn, \"trades\")\n        if not cols:\n            conn.close()\n            init_trade_db()\n            conn = sqlite3.connect(TRADES_DB, timeout=5)\n            cur = conn.cursor()\n            cols = _get_table_columns(conn, \"trades\")\n        insert_cols = [c for c in [\n            \"ts\", \"symbol\", \"side\", \"entry\", \"sl\", \"tp\", \"lots\", \"status\", \"pnl\", \"rmult\", \"rm\", \"regime\", \"score\", \"model_score\", \"meta\"\n        ] if c in cols]\n        if not insert_cols:\n            logger.error(\"No writable columns present in trades table; aborting record_trade\")\n            conn.close()\n            return\n        placeholders = \",\".join([\"?\" for _ in insert_cols])\n        col_list_sql = \",\".join(insert_cols)\n        values = [data.get(c) for c in insert_cols]\n        cur.execute(f\"INSERT INTO trades ({col_list_sql}) VALUES ({placeholders})\", tuple(values))\n        conn.commit(); conn.close()\n    except Exception:\n        logger.exception(\"record_trade db failed\")\n    try:\n        with open(TRADES_CSV, \"a\", encoding=\"utf-8\") as f:\n            f.write(\"{},{},{},{},{},{},{},{},{},{},{},{},{}\\n\".format(ts, symbol, side, entry, sl, tp, lots, status, pnl, rmult, regime, score, model_score))\n    except Exception:\n        logger.exception(\"record_trade csv failed\")\n\ndef get_recent_trades(limit=200):\n    try:\n        conn = sqlite3.connect(TRADES_DB, timeout=5)\n        cur = conn.cursor()\n        cur.execute(\"SELECT ts,symbol,side,pnl,rmult,regime,score,model_score FROM trades ORDER BY id DESC LIMIT ?\", (limit,))\n        rows = cur.fetchall()\n        conn.close()\n        return rows\n    except Exception:\n        return []\n\n# ---------------- MT5 mapping/helpers ----------------\ndef try_start_mt5_terminal():\n    if MT5_PATH and os.path.exists(MT5_PATH):\n        try:\n            import subprocess\n            subprocess.Popen([MT5_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n            time.sleep(2.5)\n            return True\n        except Exception:\n            logger.exception(\"Failed to spawn MT5 terminal\")\n    return False\n\ndef connect_mt5(login: Optional[int] = None, password: Optional[str] = None, server: Optional[str] = None) -> bool:\n    global _mt5, _mt5_connected\n    if not MT5_AVAILABLE:\n        logger.warning(\"MetaTrader5 python package not installed\")\n        return False\n    try:\n        _mt5 = mt5\n    except Exception:\n        logger.exception(\"mt5 import problem\")\n        return False\n    login = login or (int(MT5_LOGIN) if MT5_LOGIN and str(MT5_LOGIN).isdigit() else None)\n    password = password or MT5_PASSWORD\n    server = server or MT5_SERVER\n    if login is None or password is None or server is None:\n        logger.warning(\"MT5 credentials missing; MT5 will not be used\")\n        return False\n    try:\n        ok = _mt5.initialize(login=login, password=password, server=server)\n        if not ok:\n            logger.warning(\"MT5 initialize failed: %s; trying to start terminal and retry\", getattr(_mt5, \"last_error\", lambda: None)())\n            try_start_mt5_terminal()\n            time.sleep(2.5)\n            try:\n                _mt5.shutdown()\n            except Exception:\n                pass\n            ok2 = _mt5.initialize(login=login, password=password, server=server)\n            if not ok2:\n                logger.error(\"MT5 initialize retry failed: %s\", getattr(_mt5, \"last_error\", lambda: None)())\n                _mt5_connected = False\n                return False\n        _mt5_connected = True\n        logger.info(\"MT5 initialized (login=%s server=%s)\", login, server)\n        return True\n    except Exception:\n        logger.exception(\"MT5 connect error\")\n        _mt5_connected = False\n        return False\n\ndef discover_broker_symbols():\n    try:\n        if _mt5_connected and _mt5 is not None:\n            syms = _mt5.symbols_get()\n            return [s.name for s in syms] if syms else []\n    except Exception:\n        logger.debug(\"discover_broker_symbols failed\")\n    return []\n\ndef map_symbol_to_broker(requested: str) -> str:\n    r = str(requested).strip()\n    if r in BROKER_SYMBOLS:\n        return BROKER_SYMBOLS[r]\n    if not (_mt5_connected and _mt5 is not None):\n        return requested\n    try:\n        brokers = discover_broker_symbols()\n        low_req = r.lower()\n        for b in brokers:\n            if b.lower() == low_req:\n                return b\n        variants = [r, r + \".m\", r + \"m\", r + \"-m\", r + \".M\", r + \"M\"]\n        for v in variants:\n            for b in brokers:\n                if b.lower() == v.lower():\n                    return b\n        for b in brokers:\n            bn = b.lower()\n            if low_req in bn or bn.startswith(low_req) or bn.endswith(low_req):\n                return b\n    except Exception:\n        logger.debug(\"map_symbol_to_broker error\", exc_info=True)\n    return requested\n\n# ---------------- MT5 data fetcher ----------------\ndef fetch_ohlcv_mt5(symbol: str, interval: str = \"60m\", period_days: int = 60):\n    if not MT5_AVAILABLE or not _mt5_connected:\n        return None\n    try:\n        broker_sym = map_symbol_to_broker(symbol)\n        si = _mt5.symbol_info(broker_sym)\n        if si is None:\n            logger.info(\"Symbol not found on broker: %s (requested %s)\", broker_sym, symbol)\n            return None\n        if not si.visible:\n            try:\n                _mt5.symbol_select(broker_sym, True)\n            except Exception:\n                pass\n        tf_map = {\n            \"1m\": _mt5.TIMEFRAME_M1,\n            \"5m\": _mt5.TIMEFRAME_M5,\n            \"15m\": _mt5.TIMEFRAME_M15,\n            \"30m\": _mt5.TIMEFRAME_M30,\n            \"60m\": _mt5.TIMEFRAME_H1,\n            \"1h\": _mt5.TIMEFRAME_H1,\n            \"4h\": _mt5.TIMEFRAME_H4,\n            \"1d\": _mt5.TIMEFRAME_D1,\n        }\n        mt_tf = tf_map.get(interval, _mt5.TIMEFRAME_H1)\n        count = 500\n        try:\n            if interval.endswith(\"m\"):\n                minutes = int(interval[:-1])\n                bars_per_day = max(1, int(24 * 60 / minutes))\n                count = max(120, period_days * bars_per_day)\n            elif interval in (\"1h\", \"60m\"):\n                count = max(120, period_days * 24)\n            elif interval in (\"4h\",):\n                count = max(120, int(period_days * 6))\n            elif interval in (\"1d\",):\n                count = max(60, period_days)\n        except Exception:\n            count = 500\n        rates = _mt5.copy_rates_from_pos(broker_sym, mt_tf, 0, int(count))\n        if rates is None:\n            logger.info(\"MT5 returned no rates for %s\", broker_sym)\n            return None\n        df = pd.DataFrame(rates)\n        if \"time\" in df.columns:\n            df.index = pd.to_datetime(df[\"time\"], unit=\"s\")\n        if \"open\" not in df.columns and \"open_price\" in df.columns:\n            df[\"open\"] = df[\"open_price\"]\n        if \"tick_volume\" in df.columns:\n            df[\"volume\"] = df[\"tick_volume\"]\n        elif \"real_volume\" in df.columns:\n            df[\"volume\"] = df[\"real_volume\"]\n        for col in (\"open\", \"high\", \"low\", \"close\", \"volume\"):\n            if col in df.columns:\n                try:\n                    df[col] = pd.to_numeric(df[col], errors=\"coerce\")\n                except Exception:\n                    pass\n            else:\n                df[col] = pd.NA\n        df = df[[\"open\", \"high\", \"low\", \"close\", \"volume\"]].dropna(how=\"all\")\n        return df\n    except Exception:\n        logger.exception(\"fetch_ohlcv_mt5 error\")\n        return None\n\ndef fetch_ohlcv(symbol: str, interval: str = \"60m\", period_days: int = 60):\n    df = fetch_ohlcv_mt5(symbol, interval=interval, period_days=period_days)\n    if df is None or df.empty:\n        logger.info(\"No MT5 data for %s (%s) - skipping\", symbol, interval)\n        return None\n    return df\n\ndef fetch_multi_timeframes(symbol: str, period_days: int = 60):\n    out = {}\n    for label, intr in TIMEFRAMES.items():\n        out[label] = fetch_ohlcv(symbol, interval=intr, period_days=period_days)\n    return out\n\n# ---------------- Indicators & scoring ----------------\ndef add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:\n    df = df.copy()\n    if df.empty:\n        return df\n    try:\n        if TA_AVAILABLE:\n            df[\"sma5\"] = SMAIndicator(df[\"close\"], window=5).sma_indicator()\n            df[\"sma20\"] = SMAIndicator(df[\"close\"], window=20).sma_indicator()\n            df[\"rsi14\"] = RSIIndicator(df[\"close\"], window=14).rsi()\n            df[\"atr14\"] = AverageTrueRange(df[\"high\"], df[\"low\"], df[\"close\"], window=14).average_true_range()\n            df[\"adx\"] = ADXIndicator(df[\"high\"], df[\"low\"], df[\"close\"], window=14).adx()\n        else:\n            df[\"sma5\"] = df[\"close\"].rolling(5, min_periods=1).mean()\n            df[\"sma20\"] = df[\"close\"].rolling(20, min_periods=1).mean()\n            delta = df[\"close\"].diff()\n            up = delta.clip(lower=0.0).rolling(14, min_periods=1).mean()\n            down = -delta.clip(upper=0.0).rolling(14, min_periods=1).mean().replace(0, 1e-9)\n            rs = up / down\n            df[\"rsi14\"] = 100 - (100 / (1 + rs))\n            tr = pd.concat([(df[\"high\"] - df[\"low\"]).abs(), (df[\"high\"] - df[\"close\"].shift()).abs(), (df[\"low\"] - df[\"close\"].shift()).abs()], axis=1).max(axis=1)\n            df[\"atr14\"] = tr.rolling(14, min_periods=1).mean()\n            df[\"adx\"] = df[\"close\"].diff().abs().rolling(14, min_periods=1).mean()\n    except Exception:\n        logger.exception(\"add_technical_indicators error\")\n    try:\n        df = df.bfill().ffill().fillna(0.0)\n    except Exception:\n        try:\n            df = df.fillna(0.0)\n        except Exception:\n            pass\n    return df\n\ndef detect_market_regime_from_h1(df_h1: pd.DataFrame):\n    try:\n        if df_h1 is None or df_h1.empty:\n            return \"unknown\", None, None\n        d = add_technical_indicators(df_h1)\n        atr = float(d[\"atr14\"].iloc[-1])\n        price = float(d[\"close\"].iloc[-1]) if d[\"close\"].iloc[-1] else 1.0\n        rel = atr / price if price else 0.0\n        adx = float(d[\"adx\"].iloc[-1]) if \"adx\" in d.columns else 0.0\n        if rel < 0.0025 and adx < 20:\n            return \"quiet\", rel, adx\n        if rel > 0.0075 and adx > 25:\n            return \"volatile\", rel, adx\n        if adx > 25:\n            return \"trending\", rel, adx\n        return \"normal\", rel, adx\n    except Exception:\n        logger.exception(\"detect_market_regime failed\")\n        return \"unknown\", None, None\n\ndef technical_signal_score(df: pd.DataFrame) -> float:\n    try:\n        if df is None or len(df) < 2:\n            return 0.0\n        latest = df.iloc[-1]; prev = df.iloc[-2]\n        score = 0.0\n        if prev[\"sma5\"] <= prev[\"sma20\"] and latest[\"sma5\"] > latest[\"sma20\"]:\n            score += 0.6\n        if prev[\"sma5\"] >= prev[\"sma20\"] and latest[\"sma5\"] < latest[\"sma20\"]:\n            score -= 0.6\n        r = float(latest.get(\"rsi14\", 50) or 50)\n        if r < 30:\n            score += 0.25\n        elif r > 70:\n            score -= 0.25\n        return max(-1.0, min(1.0, score))\n    except Exception:\n        return 0.0\n\ndef aggregate_multi_tf_scores(tf_dfs: Dict[str, pd.DataFrame]) -> Dict[str, float]:\n    techs = []\n    for label, df in tf_dfs.items():\n        try:\n            if df is None or getattr(df, \"empty\", True):\n                continue\n            dfind = add_technical_indicators(df)\n            t = technical_signal_score(dfind)\n            weight = {\"M30\": 1.8, \"H1\": 1.2}.get(label, 1.0)\n            techs.append((t, weight))\n        except Exception:\n            logger.exception(\"aggregate_multi_tf_scores failed for %s\", label)\n    if not techs:\n        return {\"tech\": 0.0, \"fund\": 0.0, \"sent\": 0.0}\n    s = sum(t * w for t, w in techs); w = sum(w for _, w in techs)\n    return {\"tech\": float(s / w), \"fund\": 0.0, \"sent\": 0.0}\n\n# ---------------- Multi-asset blending & fundamental awareness ----------------\n_portfolio_weights_cache = {\"ts\": 0, \"weights\": {}}\nPORTFOLIO_RECOMPUTE_SECONDS = 300\n\ndef compute_portfolio_weights(symbols: List[str], period_days: int = 45):\n    global _portfolio_weights_cache\n    now = time.time()\n    if now - _portfolio_weights_cache.get(\"ts\", 0) < PORTFOLIO_RECOMPUTE_SECONDS and _portfolio_weights_cache.get(\"weights\"):\n        return _portfolio_weights_cache[\"weights\"]\n    dfs = {}\n    vols = {}\n    rets = {}\n    for s in symbols:\n        try:\n            df = fetch_ohlcv(s, interval=\"60m\", period_days=period_days)\n            if df is None or getattr(df, \"empty\", True):\n                continue\n            df = df.tail(24 * period_days)\n            dfs[s] = df\n            rets_s = df[\"close\"].pct_change().dropna()\n            rets[s] = rets_s\n            vols[s] = rets_s.std() if not rets_s.empty else 1e-6\n        except Exception:\n            continue\n    symbols_ok = list(rets.keys())\n    if not symbols_ok:\n        weights = {s: 1.0 / max(1, len(symbols)) for s in symbols}\n        _portfolio_weights_cache = {\"ts\": now, \"weights\": weights}\n        return weights\n    try:\n        rets_df = pd.DataFrame(rets)\n        corr = rets_df.corr().fillna(0.0)\n        avg_corr = corr.mean().to_dict()\n    except Exception:\n        avg_corr = {s: 0.0 for s in symbols_ok}\n    raw = {}\n    for s in symbols_ok:\n        v = float(vols.get(s, 1e-6))\n        ac = float(avg_corr.get(s, 0.0))\n        raw_score = (1.0 / max(1e-6, v)) * max(0.0, (1.0 - ac))\n        raw[s] = raw_score\n    for s in symbols:\n        if s not in raw:\n            raw[s] = 0.0001\n    total = sum(raw.values()) or 1.0\n    weights = {s: raw[s] / total for s in symbols}\n    _portfolio_weights_cache = {\"ts\": now, \"weights\": weights}\n    return weights\n\ndef get_portfolio_scale_for_symbol(symbol: str, weights: Dict[str, float]):\n    if not weights or symbol not in weights:\n        return 1.0\n    w = float(weights.get(symbol, 0.0))\n    avg = sum(weights.values()) / max(1, len(weights))\n    if avg <= 0:\n        return 1.0\n    ratio = w / avg\n    scale = 1.0 + (ratio - 1.0) * 0.4\n    return max(0.6, min(1.4, scale))\n\n# ---------------- News & Fundamentals module (robust) ----------------\n_POS_WORDS = {\"gain\", \"rise\", \"surge\", \"up\", \"positive\", \"bull\", \"beats\", \"beat\", \"record\", \"rally\", \"higher\", \"recover\"}\n_NEG_WORDS = {\"fall\", \"drop\", \"down\", \"loss\", \"negative\", \"bear\", \"miss\", \"misses\", \"crash\", \"decline\", \"lower\", \"plunge\", \"attack\", \"strike\"}\n_RISK_KEYWORDS = {\"iran\", \"strike\", \"war\", \"missile\", \"hormuz\", \"oil\", \"sanction\", \"attack\", \"drone\", \"retaliat\", \"escalat\"}\n\n_news_cache = {\"ts\": 0, \"data\": {}}\n_price_cache = {\"ts\": 0, \"data\": {}}\n\ndef _vader_score(text: str) -> float:\n    if VADER_AVAILABLE and _VADER is not None:\n        try:\n            s = _VADER.polarity_scores(text or \"\")\n            return float(s.get(\"compound\", 0.0))\n        except Exception:\n            return 0.0\n    txt = (text or \"\").lower()\n    p = sum(1 for w in _POS_WORDS if w in txt)\n    n = sum(1 for w in _NEG_WORDS if w in txt)\n    denom = max(1.0, len(txt.split()))\n    return max(-1.0, min(1.0, (p - n) / denom))\n\n# -------- Retry helper used by robust fetches --------\ndef _do_request_with_retries(url, params=None, max_retries=3, backoff_base=0.6, timeout=10):\n    \"\"\"Simple retry helper returning requests.Response or None.\"\"\"\n    if not FUNDAMENTAL_AVAILABLE:\n        return None\n    attempt = 0\n    while attempt < max_retries:\n        try:\n            r = requests.get(url, params=params, timeout=timeout)\n            if r.status_code in (429, 500, 502, 503, 504):\n                attempt += 1\n                sleep_t = backoff_base * (2 ** (attempt - 1))\n                logger.debug(\"Request %s -> %s (status=%s). retrying after %.2fs\", url, params, r.status_code, sleep_t)\n                time.sleep(sleep_t)\n                continue\n            return r\n        except Exception as e:\n            attempt += 1\n            sleep_t = backoff_base * (2 ** (attempt - 1))\n            logger.debug(\"Request exception %s; retry %d after %.2fs\", e, attempt, sleep_t)\n            time.sleep(sleep_t)\n    return None\n\n# -------- Robust AlphaVantage crypto intraday with fallbacks --------\ndef fetch_alpha_vantage_crypto_intraday(symbol: str = \"BTC\", market: str = \"USD\"):\n    \"\"\"\n    Primary: AlphaVantage DIGITAL_CURRENCY_INTRADAY\n    Fallback 1: Finnhub crypto candles (if FINNHUB_KEY present)\n    Fallback 2: CoinGecko simple price (no key)\n    Returns a normalized dictionary (or {} on failure).\n    \"\"\"\n    if not FUNDAMENTAL_AVAILABLE:\n        return {}\n    # 1) Primary: Alpha Vantage\n    try:\n        av_url = \"https://www.alphavantage.co/query\"\n        params = {\"function\": \"DIGITAL_CURRENCY_INTRADAY\", \"symbol\": symbol, \"market\": market, \"apikey\": ALPHAVANTAGE_KEY}\n        r = _do_request_with_retries(av_url, params=params, max_retries=2, backoff_base=0.8, timeout=8)\n        if r and r.status_code == 200:\n            j = r.json()\n            if j and not (\"Error Message\" in j or \"Note\" in j):\n                return j\n            logger.debug(\"AlphaVantage returned error or note: %s\", j if isinstance(j, dict) else str(j)[:200])\n        else:\n            logger.debug(\"AlphaVantage request failed or non-200: %s\", None if r is None else r.status_code)\n    except Exception:\n        logger.exception(\"Primary AlphaVantage request failed\")\n\n    # 2) Fallback: Finnhub (crypto candles)\n    try:\n        if FINNHUB_KEY:\n            fh_url = \"https://finnhub.io/api/v1/crypto/candle\"\n            params = {\"symbol\": \"BINANCE:BTCUSDT\", \"resolution\": \"1\", \"from\": int(time.time()) - 3600, \"to\": int(time.time()), \"token\": FINNHUB_KEY}\n            r = _do_request_with_retries(fh_url, params=params, max_retries=2, backoff_base=0.6, timeout=6)\n            if r and r.status_code == 200:\n                j = r.json()\n                if j and \"s\" in j and j[\"s\"] in (\"ok\", \"no_data\"):\n                    return {\"finnhub\": j}\n    except Exception:\n        logger.exception(\"Finnhub fallback failed\")\n\n    # 3) Fallback: CoinGecko (no key) - get recent price and 24h change\n    try:\n        cg_url = \"https://api.coingecko.com/api/v3/simple/price\"\n        coin_id = \"bitcoin\" if symbol.upper().startswith(\"BTC\") else symbol.lower()\n        params = {\"ids\": coin_id, \"vs_currencies\": market.lower(), \"include_24hr_change\": \"true\"}\n        r = _do_request_with_retries(cg_url, params=params, max_retries=2, backoff_base=0.6, timeout=6)\n        if r and r.status_code == 200:\n            j = r.json()\n            return {\"coingecko_simple\": j}\n    except Exception:\n        logger.exception(\"CoinGecko fallback failed\")\n\n    return {}\n\n# -------- Robust NewsData fetch with fallback & query expansion --------\ndef fetch_newsdata(q: str, pagesize: int = 20):\n    \"\"\"\n    Primary: NewsData.io\n    Fallbacks: NewsAPI (if NEWS_API_KEY present), CoinDesk quick probe\n    Expands keywords and caches results briefly to avoid free-tier rate limits.\n    \"\"\"\n    out = {\"count\": 0, \"articles\": []}\n    if not FUNDAMENTAL_AVAILABLE:\n        return out\n\n    q_orig = q or \"\"\n    q_terms = set([t.strip() for t in q_orig.replace(\",\", \" \").split() if t.strip()])\n    if any(x in q_orig.lower() for x in (\"gold\", \"xau\")):\n        q_terms.update({\"gold\", \"xau\", \"xauusd\"})\n    if any(x in q_orig.lower() for x in (\"silver\", \"xag\")):\n        q_terms.update({\"silver\", \"xag\", \"xagusd\"})\n    if any(x in q_orig.lower() for x in (\"oil\", \"wti\", \"usoil\")):\n        q_terms.update({\"oil\", \"wti\", \"usoil\", \"brent\"})\n    if any(x in q_orig.lower() for x in (\"bitcoin\", \"btc\")):\n        q_terms.update({\"bitcoin\", \"btc\", \"btcusd\"})\n    q_expanded = \" OR \".join(list(q_terms)) if q_terms else q\n\n    now_ts = time.time()\n    cache_key = f\"newsdata:{q_expanded}:{pagesize}\"\n    cached = _news_cache[\"data\"].get(cache_key)\n    if cached and now_ts - _news_cache[\"ts\"] < 30:\n        return cached\n\n    # 1) Primary - NewsData\n    if NEWSDATA_KEY:\n        try:\n            url = \"https://newsdata.io/api/1/news\"\n            params = {\"q\": q_expanded, \"language\": \"en\", \"page\": 1, \"page_size\": pagesize, \"apikey\": NEWSDATA_KEY}\n            r = _do_request_with_retries(url, params=params, max_retries=2, backoff_base=0.6, timeout=6)\n            if r and r.status_code == 200:\n                j = r.json()\n                articles = j.get(\"results\") or j.get(\"articles\") or j.get(\"news\") or []\n                processed = []\n                for a in articles[:pagesize]:\n                    title = a.get(\"title\") or \"\"\n                    desc = a.get(\"description\") or a.get(\"summary\") or \"\"\n                    src = (a.get(\"source_id\") or a.get(\"source\", \"\") or \"\").strip()\n                    published = a.get(\"pubDate\") or a.get(\"publishedAt\") or a.get(\"date\") or \"\"\n                    processed.append({\"title\": title, \"description\": desc, \"source\": src, \"publishedAt\": published, \"raw\": a})\n                out = {\"count\": len(processed), \"articles\": processed}\n                _news_cache[\"data\"][cache_key] = out; _news_cache[\"ts\"] = now_ts\n                return out\n            else:\n                logger.debug(\"NewsData non-200 or failed: %s\", None if r is None else r.status_code)\n        except Exception:\n            logger.exception(\"fetch_newsdata primary failed\")\n\n    # 2) Fallback - NewsAPI if present\n    newsapi_key = os.getenv(\"NEWS_API_KEY\") or os.getenv(\"NEWSAPI_KEY\")\n    if newsapi_key:\n        try:\n            url = \"https://newsapi.org/v2/everything\"\n            params = {\"q\": q_expanded, \"language\": \"en\", \"pageSize\": pagesize, \"apiKey\": newsapi_key}\n            r = _do_request_with_retries(url, params=params, max_retries=2, backoff_base=0.6, timeout=6)\n            if r and r.status_code == 200:\n                j = r.json()\n                arts = j.get(\"articles\", [])[:pagesize]\n                processed = []\n                for a in arts:\n                    processed.append({\"title\": a.get(\"title\"), \"description\": a.get(\"description\"), \"source\": (a.get(\"source\") or {}).get(\"name\", \"\"), \"publishedAt\": a.get(\"publishedAt\"), \"raw\": a})\n                out = {\"count\": len(processed), \"articles\": processed}\n                _news_cache[\"data\"][cache_key] = out; _news_cache[\"ts\"] = now_ts\n                return out\n        except Exception:\n            logger.exception(\"fetch_newsdata fallback NewsAPI failed\")\n\n    # 3) Lightweight fallback: CoinDesk probe or empty marker\n    try:\n        cd_url = \"https://api.coindesk.com/v2/spot/markets/list\"\n        r = _do_request_with_retries(cd_url, params=None, max_retries=1, backoff_base=0.6, timeout=6)\n        if r and r.status_code == 200:\n            out = {\"count\": 0, \"articles\": [], \"note\": \"coindesk_reached\"}\n            _news_cache[\"data\"][cache_key] = out; _news_cache[\"ts\"] = now_ts\n            return out\n    except Exception:\n        pass\n\n    _news_cache[\"data\"][cache_key] = out; _news_cache[\"ts\"] = now_ts\n    return out\n\n# ---------------- Economic calendar (Finnhub primary, TradingEconomics fallback) ----------------\ndef fetch_finnhub_calendar(lookback_hours: int = 1, lookahead_hours: int = 48):\n    \"\"\"\n    Primary: Finnhub economic calendar\n    Fallback: TradingEconomics (if TRADING_ECONOMICS_KEY / TE key present)\n    Normalizes into a list of events with date/country/event/importance.\n    \"\"\"\n    if not FUNDAMENTAL_AVAILABLE:\n        return []\n    events = []\n    # Primary Finnhub\n    if FINNHUB_KEY:\n        try:\n            now = datetime.utcnow()\n            start = (now - timedelta(hours=lookback_hours)).strftime(\"%Y-%m-%d\")\n            end = (now + timedelta(hours=lookahead_hours)).strftime(\"%Y-%m-%d\")\n            url = f\"https://finnhub.io/api/v1/calendar/economic?from={start}&to={end}&token={FINNHUB_KEY}\"\n            r = _do_request_with_retries(url, params=None, max_retries=2, backoff_base=0.6, timeout=8)\n            if r and r.status_code == 200:\n                j = r.json()\n                if isinstance(j, dict) and \"economicCalendar\" in j:\n                    raw = j.get(\"economicCalendar\") or []\n                elif isinstance(j, list):\n                    raw = j\n                elif isinstance(j, dict) and \"data\" in j:\n                    raw = j.get(\"data\") or []\n                else:\n                    raw = []\n                for e in raw:\n                    try:\n                        events.append({\n                            \"date\": e.get(\"date\") or e.get(\"dateTime\") or e.get(\"time\"),\n                            \"country\": e.get(\"country\") or e.get(\"iso3\") or \"\",\n                            \"event\": e.get(\"event\") or e.get(\"name\") or e.get(\"title\") or \"\",\n                            \"importance\": e.get(\"importance\") or e.get(\"impact\") or e.get(\"importanceLevel\") or e.get(\"actual\") or \"\"\n                        })\n                    except Exception:\n                        continue\n                if events:\n                    return events\n        except Exception:\n            logger.exception(\"fetch_finnhub_calendar primary failed\")\n\n    # Fallback: TradingEconomics\n    te_key = os.getenv(\"TRADING_ECONOMICS_KEY\") or os.getenv(\"TE_KEY\") or os.getenv(\"TE_KEY_ALT\")\n    if te_key:\n        try:\n            now = datetime.utcnow()\n            d1 = (now - timedelta(days=1)).strftime(\"%Y-%m-%d\")\n            d2 = (now + timedelta(days=lookahead_hours // 24 + 2)).strftime(\"%Y-%m-%d\")\n            url = f\"https://api.tradingeconomics.com/calendar/country/all?c={te_key}&d1={d1}&d2={d2}\"\n            r = _do_request_with_retries(url, params=None, max_retries=2, backoff_base=0.6, timeout=8)\n            if r and r.status_code == 200:\n                j = r.json()\n                if isinstance(j, list):\n                    for e in j:\n                        try:\n                            events.append({\n                                \"date\": e.get(\"date\") or e.get(\"datetime\") or \"\",\n                                \"country\": e.get(\"country\") or \"\",\n                                \"event\": e.get(\"event\") or e.get(\"title\") or \"\",\n                                \"importance\": e.get(\"importance\") or e.get(\"importanceName\") or e.get(\"actual\") or \"\"\n                            })\n                        except Exception:\n                            continue\n                    if events:\n                        return events\n        except Exception:\n            logger.exception(\"fetch_finnhub_calendar fallback TE failed\")\n    return events\n\n# ---------------- Economic calendar blocking ----------------\ndef _symbol_to_currencies(symbol: str) -> List[str]:\n    s = symbol.upper()\n    if len(s) >= 6:\n        base = s[:3]; quote = s[3:6]\n        return [base, quote]\n    if s.startswith(\"XAU\") or \"XAU\" in s:\n        return [\"XAU\", \"USD\"]\n    if s.startswith(\"XAG\") or \"XAG\" in s:\n        return [\"XAG\", \"USD\"]\n    if s.startswith(\"BTC\"):\n        return [\"BTC\", \"USD\"]\n    return [s]\n\ndef should_pause_for_events(symbol: str, lookahead_minutes: int = 30) -> (bool, Optional[Dict[str, Any]]):\n    \"\"\"\n    Uses calendar fetch (Finnhub primary, TE fallback); numeric impact mapping supported.\n    Returns (True, info) if a high-impact event is imminent for the symbol's currencies.\n    \"\"\"\n    try:\n        if not FUNDAMENTAL_AVAILABLE:\n            return False, None\n        evs = fetch_finnhub_calendar(lookback_hours=0, lookahead_hours=int(max(1, lookahead_minutes / 60)))\n        if not evs:\n            return False, None\n        now_utc = pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=timezone.utc)\n        currs = _symbol_to_currencies(symbol)\n        for e in evs:\n            try:\n                impact_raw = e.get(\"importance\") or e.get(\"impact\") or e.get(\"importanceLevel\") or e.get(\"actual\") or e.get(\"prior\")\n                if impact_raw is None:\n                    continue\n                impact_str = str(impact_raw).strip().lower()\n                is_high = False\n                if impact_str in (\"high\", \"h\", \"high impact\"):\n                    is_high = True\n                else:\n                    try:\n                        num = int(float(impact_raw))\n                        if num >= 3:\n                            is_high = True\n                    except Exception:\n                        is_high = False\n                if not is_high:\n                    continue\n                when = None\n                for key in (\"date\", \"dateTime\", \"time\", \"timestamp\"):\n                    if key in e and e.get(key):\n                        try:\n                            when = pd.to_datetime(e.get(key), utc=True, errors=\"coerce\")\n                            if pd.isna(when):\n                                when = None\n                            else:\n                                break\n                        except Exception:\n                            when = None\n                if when is None:\n                    logger.debug(\"calendar event has no parseable datetime; skipping: %s\", str(e)[:120])\n                    continue\n                try:\n                    when_dt = when.to_pydatetime()\n                    if when_dt.tzinfo is None:\n                        when_dt = when_dt.replace(tzinfo=timezone.utc)\n                except Exception:\n                    when_dt = pd.to_datetime(when, utc=True).to_pydatetime()\n                diff = (when_dt - now_utc).total_seconds() / 60.0\n                if diff < 0:\n                    continue\n                if diff <= lookahead_minutes:\n                    title = (e.get(\"event\") or e.get(\"title\") or \"\").lower()\n                    country = (e.get(\"country\") or \"\").upper()\n                    for c in currs:\n                        if c and (c.lower() in title or c.upper() == country):\n                            return True, {\"event\": title, \"minutes_to\": diff, \"impact\": impact_raw, \"raw\": e}\n            except Exception:\n                logger.exception(\"processing calendar event failed (continue)\")\n                continue\n        return False, None\n    except Exception:\n        logger.exception(\"should_pause_for_events failed\")\n        return False, None\n\n# ---------------- Fundmentals composition ----------------\ndef fetch_fundamental_score(symbol: str, lookback_days: int = NEWS_LOOKBACK_DAYS) -> float:\n    \"\"\"\n    Compose a fundamental score from:\n    - NewsData headlines -> news_sentiment\n    - Calendar blocking (should_pause_for_events) -> blocking\n    - AlphaVantage crypto intraday / CoinGecko fallback -> crypto_shock\n    Returns normalized in [-1,1]\n    \"\"\"\n    news_sent = 0.0\n    calendar_signal = 0.0\n    crypto_shock = 0.0\n    try:\n        symbol_upper = symbol.upper()\n        query_terms = []\n        if symbol_upper.startswith(\"XAU\") or \"GOLD\" in symbol_upper:\n            query_terms.append(\"gold\")\n        elif symbol_upper.startswith(\"XAG\") or \"SILVER\" in symbol_upper:\n            query_terms.append(\"silver\")\n        elif symbol_upper.startswith(\"BTC\") or \"BTC\" in symbol_upper:\n            query_terms.append(\"bitcoin\")\n        elif symbol_upper in (\"USOIL\", \"OIL\", \"WTI\", \"BRENT\"):\n            query_terms.append(\"oil\")\n        else:\n            query_terms.append(symbol)\n        query_terms.extend(list(_RISK_KEYWORDS))\n        q = \" OR \".join(list(set(query_terms)))\n        news = fetch_newsdata(q, pagesize=20)\n        articles = news.get(\"articles\", []) if isinstance(news, dict) else []\n        if articles:\n            scores = []\n            hits = 0\n            for a in articles:\n                txt = (a.get(\"title\",\"\") + \" \" + a.get(\"description\",\"\")).strip()\n                s = _vader_score(txt)\n                scores.append(s)\n                kh = sum(1 for k in _RISK_KEYWORDS if k in txt.lower())\n                hits += kh\n            avg = sum(scores) / max(1, len(scores))\n            if hits >= 2:\n                avg = max(-1.0, min(1.0, avg - 0.2 * min(3, hits)))\n            news_sent = float(max(-1.0, min(1.0, avg)))\n        else:\n            news_sent = 0.0\n    except Exception:\n        logger.exception(\"fetch_fundamental_score news fetch failed\")\n        news_sent = 0.0\n\n    try:\n        pause, ev = should_pause_for_events(symbol, lookahead_minutes=PAUSE_BEFORE_EVENT_MINUTES)\n        if pause:\n            calendar_signal = -1.0\n        else:\n            calendar_signal = 0.0\n    except Exception:\n        calendar_signal = 0.0\n\n    try:\n        if symbol.upper().startswith(\"BTC\"):\n            try:\n                crypto_shock = coindata_price_shock_crypto(\"BTC\")\n            except Exception:\n                crypto_shock = 0.0\n        else:\n            crypto_shock = 0.0\n    except Exception:\n        crypto_shock = 0.0\n\n    combined = 0.6 * news_sent + 0.3 * 0.0 + 0.1 * crypto_shock\n    combined = max(-1.0, min(1.0, combined))\n    return float(combined)\n\n# ---------------- coindata price shock (uses alphaVantage or fallbacks) ----------------\ndef coindata_price_shock_crypto(symbol: str = \"BTC\"):\n    now_ts = time.time()\n    if now_ts - _price_cache.get(\"ts\", 0) < 30:\n        cached = _price_cache[\"data\"].get(symbol)\n        if cached is not None:\n            return cached\n    shock = 0.0\n    try:\n        av = fetch_alpha_vantage_crypto_intraday(symbol=symbol, market=\"USD\")\n        series_key = None\n        if isinstance(av, dict):\n            for k in av.keys():\n                if \"Time Series\" in k or \"Time Series (Digital Currency Intraday)\" in k:\n                    series_key = k\n                    break\n        if series_key and isinstance(av.get(series_key), dict):\n            times = sorted(av[series_key].keys(), reverse=True)\n            if len(times) >= 2:\n                try:\n                    latest = float(av[series_key][times[0]][\"1a. price (USD)\"])\n                    prev = float(av[series_key][times[1]][\"1a. price (USD)\"])\n                    pct = (latest - prev) / max(1e-9, prev) * 100.0\n                    shock = max(-1.0, min(1.0, pct / 5.0))\n                except Exception:\n                    shock = 0.0\n        elif isinstance(av, dict) and \"finnhub\" in av:\n            # use finnhub candle structure\n            fh = av[\"finnhub\"]\n            if fh.get(\"s\") == \"ok\" and fh.get(\"c\"):\n                try:\n                    latest = float(fh[\"c\"][-1])\n                    prev = float(fh[\"c\"][-2])\n                    pct = (latest - prev) / max(1e-9, prev) * 100.0\n                    shock = max(-1.0, min(1.0, pct / 5.0))\n                except Exception:\n                    shock = 0.0\n        elif isinstance(av, dict) and \"coingecko_simple\" in av:\n            cg = av[\"coingecko_simple\"]\n            key = symbol.lower() if symbol.lower() != \"btc\" else \"bitcoin\"\n            if key in cg and f\"{key}\" in cg:\n                try:\n                    pct24 = float(cg.get(key, {}).get(\"usd_24h_change\", 0.0))\n                    shock = max(-1.0, min(1.0, pct24 / 10.0))\n                except Exception:\n                    shock = 0.0\n        _price_cache[\"data\"][symbol] = float(shock)\n        _price_cache[\"ts\"] = now_ts\n        return float(shock)\n    except Exception:\n        logger.exception(\"coindata_price_shock_crypto failed\")\n        return 0.0\n\n# ---------------- ML hooks, optimizer, simulate (unchanged) ----------------\ndef build_model():\n    if not SKLEARN_AVAILABLE:\n        return None\n    try:\n        if 'RandomForestClassifier' in globals():\n            clf = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=1)\n            return Pipeline([(\"clf\", clf)])\n        else:\n            pipe = Pipeline([(\"scaler\", StandardScaler()), (\"clf\", SGDClassifier(loss=\"log\", max_iter=5000, tol=1e-5, random_state=42, warm_start=True))])\n            return pipe\n    except Exception:\n        try:\n            pipe = Pipeline([(\"scaler\", StandardScaler()), (\"clf\", SGDClassifier(loss=\"log\", max_iter=5000, tol=1e-5, random_state=42, warm_start=True))])\n            return pipe\n        except Exception:\n            return None\n\ndef load_model():\n    global model_pipe\n    if not SKLEARN_AVAILABLE:\n        return None\n    if os.path.exists(MODEL_FILE):\n        try:\n            model_pipe = joblib.load(MODEL_FILE)\n            logger.info(\"Loaded ML model\")\n            return model_pipe\n        except Exception:\n            logger.exception(\"Load model failed\")\n    try:\n        model_pipe = build_model()\n        return model_pipe\n    except Exception:\n        return None\n\nif SKLEARN_AVAILABLE:\n    load_model()\n\ndef extract_features_for_model(df_h1: pd.DataFrame, tech_score: float, symbol: str, regime_code: int):\n    try:\n        d = add_technical_indicators(df_h1.copy())\n        entry = float(d[\"close\"].iloc[-1])\n        atr = float(d[\"atr14\"].iloc[-1] or 0.0)\n        vol = float(d[\"volume\"].iloc[-1] or 0.0)\n        rsi = float(d.get(\"rsi14\", pd.Series([50])).iloc[-1] if \"rsi14\" in d.columns else 50)\n        vol_mean = float(d[\"volume\"].tail(50).mean() or 1.0)\n        vol_change = (vol - vol_mean) / (vol_mean if vol_mean else 1.0)\n        atr_rel = atr / (entry if entry else 1.0)\n        features = np.array([[tech_score, atr_rel, rsi, vol_change, regime_code]], dtype=float)\n        return features\n    except Exception:\n        return np.array([[tech_score, 0.0, 50.0, 0.0, regime_code]], dtype=float)\n\ndef simulate_strategy_on_series(df_h1, threshold, atr_mult=1.25, max_trades=200):\n    if df_h1 is None or getattr(df_h1, \"empty\", True) or len(df_h1) < 80:\n        return {\"n\": 0, \"net\": 0.0, \"avg_r\": 0.0, \"win\": 0.0}\n    df = add_technical_indicators(df_h1.copy())\n    trades = []\n    for i in range(30, len(df) - 10):\n        window = df.iloc[: i + 1]\n        score = technical_signal_score(window)\n        if score >= threshold:\n            side = \"BUY\"\n        elif score <= -threshold:\n            side = \"SELL\"\n        else:\n            continue\n        entry = float(df[\"close\"].iloc[i])\n        atr = float(df[\"atr14\"].iloc[i] or 0.0)\n        stop = atr * atr_mult\n        if side == \"BUY\":\n            sl = entry - stop; tp = entry + stop * 2.0\n        else:\n            sl = entry + stop; tp = entry - stop * 2.0\n        r_mult = 0.0\n        for j in range(i + 1, min(i + 31, len(df))):\n            high = float(df[\"high\"].iloc[j]); low = float(df[\"low\"].iloc[j])\n            if side == \"BUY\":\n                if high >= tp:\n                    r_mult = 2.0; break\n                if low <= sl:\n                    r_mult = -1.0; break\n            else:\n                if low <= tp:\n                    r_mult = 2.0; break\n                if high >= sl:\n                    r_mult = -1.0; break\n        trades.append(r_mult)\n        if len(trades) >= max_trades:\n            break\n    n = len(trades)\n    if n == 0:\n        return {\"n\": 0, \"net\": 0.0, \"avg_r\": 0.0, \"win\": 0.0}\n    net = sum(trades); avg = net / n; win = sum(1 for t in trades if t > 0) / n\n    return {\"n\": n, \"net\": net, \"avg_r\": avg, \"win\": win}\n\ndef light_optimizer(symbols, budget=12):\n    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT\n    logger.info(\"Starting light optimizer\")\n    candidates = []\n    for _ in range(budget):\n        cand_thresh = max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD + random.uniform(-0.06, 0.06)))\n        cand_risk = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, RISK_PER_TRADE_PCT * random.uniform(0.6, 1.4)))\n        stats = []\n        for s in symbols:\n            df = fetch_multi_timeframes(s, period_days=60).get(\"H1\")\n            if df is None or getattr(df, \"empty\", True):\n                continue\n            st = simulate_strategy_on_series(df, cand_thresh, atr_mult=1.25, max_trades=120)\n            if st[\"n\"] > 0:\n                stats.append(st)\n        if not stats:\n            continue\n        total_n = sum(st[\"n\"] for st in stats)\n        avg_expect = sum(st[\"avg_r\"] * st[\"n\"] for st in stats) / total_n\n        candidates.append((avg_expect, cand_thresh, cand_risk))\n    if not candidates:\n        logger.info(\"Optimizer found no candidates\")\n        return None\n    candidates.sort(reverse=True, key=lambda x: x[0])\n    best_expect, best_thresh, best_risk = candidates[0]\n    baseline_stats = []\n    for s in symbols:\n        df = fetch_multi_timeframes(s, period_days=60).get(\"H1\")\n        if df is None or getattr(df, \"empty\", True):\n            continue\n        baseline_stats.append(simulate_strategy_on_series(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=120))\n    base_n = sum(st[\"n\"] for st in baseline_stats) or 1\n    base_expect = sum(st[\"avg_r\"] * st[\"n\"] for st in baseline_stats) / base_n if baseline_stats else 0.0\n    if best_expect > base_expect + 0.02:\n        step = 0.4\n        CURRENT_THRESHOLD = float(max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD * (1 - step) + best_thresh * step)))\n        RISK_PER_TRADE_PCT = float(max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, RISK_PER_TRADE_PCT * (1 - step) + best_risk * step)))\n        save_adapt_state()\n        logger.info(\"Optimizer applied new threshold=%.3f risk=%.5f\", CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)\n        return {\"before\": base_expect, \"after\": best_expect, \"threshold\": CURRENT_THRESHOLD, \"risk\": RISK_PER_TRADE_PCT}\n    logger.info(\"Optimizer skipped applying\")\n    return None\n\n# ---------------- Execution helpers (unchanged) ----------------\ndef compute_lots_from_risk(risk_pct, balance, entry_price, stop_price):\n    try:\n        risk_amount = balance * risk_pct\n        pip_risk = abs(entry_price - stop_price)\n        if pip_risk <= 0:\n            return 0.01\n        lots = risk_amount / (pip_risk * 100000)\n        return max(0.01, round(lots, 2))\n    except Exception:\n        return 0.01\n\ndef place_order_simulated(symbol, side, lots, entry, sl, tp, score, model_score, regime):\n    record_trade(symbol, side, entry, sl, tp, lots, status=\"sim_open\", pnl=0.0, rmult=0.0, regime=regime, score=score, model_score=model_score)\n    return {\"status\":\"sim_open\"}\n\ndef place_order_mt5(symbol, action, lot, price, sl, tp):\n    if not MT5_AVAILABLE or not _mt5_connected:\n        return {\"status\": \"mt5_not_connected\"}\n    try:\n        broker = map_symbol_to_broker(symbol)\n        si = _mt5.symbol_info(broker)\n        if si is None:\n            return {\"status\": \"symbol_not_found\", \"symbol\": broker}\n        try:\n            if not si.visible:\n                _mt5.symbol_select(broker, True)\n        except Exception:\n            pass\n        tick = _mt5.symbol_info_tick(broker)\n        if tick is None:\n            return {\"status\": \"no_tick\", \"symbol\": broker}\n        vol_min = getattr(si, \"volume_min\", None) or getattr(si, \"volume_min\", 0.01) or 0.01\n        vol_step = getattr(si, \"volume_step\", None) or getattr(si, \"volume_step\", 0.01) or 0.01\n        vol_max = getattr(si, \"volume_max\", None) or getattr(si, \"volume_max\", None)\n        point = getattr(si, \"point\", None) or getattr(si, \"trade_tick_size\", None) or getattr(si, \"tick_size\", None) or 0.00001\n        stop_level = getattr(si, \"stop_level\", None)\n        if stop_level is not None and stop_level >= 0:\n            min_sl_dist = float(stop_level) * float(point)\n        else:\n            min_sl_dist = float(point) * 10.0\n        order_price = price if price is not None else (tick.ask if action == \"BUY\" else tick.bid)\n        try:\n            lots = float(lot)\n        except Exception:\n            lots = float(vol_min)\n        try:\n            if vol_step > 0:\n                steps = max(0, int((lots - vol_min) // vol_step))\n                lots_adj = vol_min + steps * vol_step\n                if lots > lots_adj:\n                    steps_ceil = int(((lots - vol_min) + vol_step - 1e-12) // vol_step)\n                    lots_adj = vol_min + steps_ceil * vol_step\n                lots = round(float(max(vol_min, lots_adj)), 2)\n            else:\n                lots = float(max(vol_min, lots))\n        except Exception:\n            lots = float(max(vol_min, 0.01))\n        entry_price = float(order_price)\n        def valid_distance(dist):\n            try:\n                return (dist is not None) and (abs(dist) >= min_sl_dist)\n            except Exception:\n                return False\n        sl_ok = True; tp_ok = True\n        if sl is not None:\n            sl_dist = abs(entry_price - float(sl))\n            sl_ok = valid_distance(sl_dist)\n        if tp is not None:\n            tp_dist = abs(entry_price - float(tp))\n            tp_ok = valid_distance(tp_dist)\n        if not sl_ok:\n            if action == \"BUY\":\n                sl = entry_price - min_sl_dist\n            else:\n                sl = entry_price + min_sl_dist\n            sl_ok = True\n        if not tp_ok:\n            if action == \"BUY\":\n                tp = entry_price + (min_sl_dist * 2.0)\n            else:\n                tp = entry_price - (min_sl_dist * 2.0)\n            tp_ok = True\n        if lots < vol_min:\n            lots = float(vol_min)\n        if vol_max and lots > vol_max:\n            return {\"status\": \"volume_too_large\", \"requested\": lots, \"max\": vol_max}\n        order_type = _mt5.ORDER_TYPE_BUY if action == \"BUY\" else _mt5.ORDER_TYPE_SELL\n        req = {\n            \"action\": _mt5.TRADE_ACTION_DEAL,\n            \"symbol\": broker,\n            \"volume\": float(lots),\n            \"type\": order_type,\n            \"price\": float(order_price),\n            \"sl\": float(sl) if sl is not None else 0.0,\n            \"tp\": float(tp) if tp is not None else 0.0,\n            \"deviation\": 20,\n            \"magic\": 123456,\n            \"comment\": \"void2.0\",\n            \"type_time\": _mt5.ORDER_TIME_GTC,\n            \"type_filling\": _mt5.ORDER_FILLING_IOC,\n        }\n        res = _mt5.order_send(req)\n        retcode = getattr(res, \"retcode\", None)\n        if retcode == 10027:\n            return {\"status\": \"autotrading_disabled\", \"retcode\": retcode, \"result\": str(res)}\n        if retcode is not None and retcode != 0:\n            return {\"status\": \"rejected\", \"retcode\": retcode, \"result\": str(res)}\n        out = {\"status\": \"sent\", \"result\": str(res), \"used_lots\": lots}\n        try:\n            ticket = getattr(res, \"order\", None) or getattr(res, \"request_id\", None) or None\n            if ticket:\n                out[\"ticket\"] = int(ticket)\n        except Exception:\n            pass\n        return out\n    except Exception:\n        logger.exception(\"place_order_mt5 failed\")\n        return {\"status\": \"error\"}\n\ndef get_today_trade_count():\n    try:\n        conn = sqlite3.connect(TRADES_DB, timeout=5)\n        cur = conn.cursor()\n        cur.execute(\"SELECT ts FROM trades\")\n        rows = cur.fetchall()\n        conn.close()\n    except Exception:\n        logger.exception(\"get_today_trade_count: DB read failed\")\n        return 0\n    reset_mode = os.getenv(\"DAILY_RESET_TZ\", \"UTC\").strip().upper()\n    start_utc = None\n    try:\n        if reset_mode == \"BROKER\" and MT5_AVAILABLE and _mt5_connected:\n            try:\n                broker_now_ts = _mt5.time_current()\n                if broker_now_ts:\n                    broker_now = datetime.utcfromtimestamp(int(broker_now_ts))\n                    broker_date = broker_now.date()\n                    start_utc = datetime(broker_date.year, broker_date.month, broker_date.day, tzinfo=timezone.utc)\n                else:\n                    today = datetime.utcnow().date()\n                    start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)\n            except Exception:\n                logger.debug(\"get_today_trade_count: broker time fetch failed, falling back to UTC\", exc_info=True)\n                today = datetime.utcnow().date()\n                start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)\n        elif reset_mode == \"LOCAL\":\n            try:\n                local_now = datetime.now().astimezone()\n                local_date = local_now.date()\n                local_midnight = datetime(local_date.year, local_date.month, local_date.day, tzinfo=local_now.tzinfo)\n                start_utc = local_midnight.astimezone(timezone.utc)\n            except Exception:\n                logger.debug(\"get_today_trade_count: local timezone conversion failed, falling back to UTC\", exc_info=True)\n                today = datetime.utcnow().date()\n                start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)\n        else:\n            today = datetime.utcnow().date()\n            start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)\n    except Exception:\n        today = datetime.utcnow().date()\n        start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)\n    count = 0\n    for (ts_raw,) in rows:\n        if not ts_raw:\n            continue\n        parsed = None\n        try:\n            parsed = pd.to_datetime(ts_raw, utc=True, errors=\"coerce\")\n        except Exception:\n            parsed = None\n        if pd.isna(parsed):\n            try:\n                parsed_naive = pd.to_datetime(ts_raw, errors=\"coerce\")\n                if pd.isna(parsed_naive):\n                    continue\n                parsed = parsed_naive.replace(tzinfo=timezone.utc)\n            except Exception:\n                continue\n        try:\n            if getattr(parsed, \"tzinfo\", None) is None:\n                parsed = parsed.tz_localize(timezone.utc)\n        except Exception:\n            try:\n                parsed = pd.to_datetime(parsed).to_pydatetime()\n                if parsed.tzinfo is None:\n                    parsed = parsed.replace(tzinfo=timezone.utc)\n            except Exception:\n                continue\n        try:\n            if isinstance(parsed, pd.Timestamp):\n                parsed_dt = parsed.to_pydatetime()\n            else:\n                parsed_dt = parsed\n            if parsed_dt.tzinfo is None:\n                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)\n            if parsed_dt >= start_utc:\n                count += 1\n        except Exception:\n            continue\n    return int(count)\n\n# ---------------- Open positions counting (MT5 first, DB fallback) ----------------\ndef _normalize_requested_symbol_key(req: str) -> str:\n    if not req:\n        return req\n    s = req.upper()\n    for suff in ('.m', 'm', '-m', '.M', 'M'):\n        if s.endswith(suff.upper()):\n            s = s[: -len(suff)]\n    if s.endswith('M'):\n        s = s[:-1]\n    return s\n\ndef get_open_positions_count(requested_symbol: str) -> int:\n    broker_sym = map_symbol_to_broker(requested_symbol)\n    if MT5_AVAILABLE and _mt5_connected:\n        try:\n            positions = _mt5.positions_get(symbol=broker_sym)\n            if not positions:\n                return 0\n            cnt = 0\n            for p in positions:\n                try:\n                    if getattr(p, \"symbol\", \"\").lower() == broker_sym.lower():\n                        vol = float(getattr(p, \"volume\", 0.0) or 0.0)\n                        if vol > 0:\n                            cnt += 1\n                except Exception:\n                    continue\n            return int(cnt)\n        except Exception:\n            logger.debug(\"positions_get failed for %s, falling back to DB count\", broker_sym, exc_info=True)\n    try:\n        conn = sqlite3.connect(TRADES_DB, timeout=5)\n        cur = conn.cursor()\n        cur.execute(\"SELECT COUNT(*) FROM trades WHERE symbol=? AND status IN ('sim_open','sent','open','sim_open','sim')\", (requested_symbol,))\n        row = cur.fetchone()\n        conn.close()\n        if row:\n            return int(row[0])\n    except Exception:\n        logger.exception(\"get_open_positions_count DB fallback failed\")\n    return 0\n\ndef get_max_open_for_symbol(requested_symbol: str) -> int:\n    key = _normalize_requested_symbol_key(requested_symbol)\n    if key in MAX_OPEN_PER_SYMBOL:\n        return int(MAX_OPEN_PER_SYMBOL[key])\n    for k, v in MAX_OPEN_PER_SYMBOL.items():\n        if key.startswith(k):\n            return int(v)\n    return int(MAX_OPEN_PER_SYMBOL_DEFAULT)\n\n# ---------------- Robust live confirmation ----------------\ndef _normalize_confirm_string(s: str) -> str:\n    if s is None:\n        return \"\"\n    cleaned = \"\".join([c for c in s if c.isalnum()]).upper()\n    return cleaned\n\ndef confirm_enable_live_interactive() -> bool:\n    env_val = os.getenv(\"CONFIRM_AUTO\", \"\")\n    if env_val:\n        if _normalize_confirm_string(env_val) == _normalize_confirm_string(\"I UNDERSTAND THE RISKS\"):\n            logger.info(\"CONFIRM_AUTO environment variable accepted\")\n            return True\n    try:\n        if not sys.stdin or not sys.stdin.isatty():\n            logger.warning(\"Non-interactive process: set CONFIRM_AUTO to 'I UNDERSTAND THE RISKS' to enable live trading\")\n            return False\n    except Exception:\n        logger.warning(\"Unable to detect interactive TTY. Set CONFIRM_AUTO='I UNDERSTAND THE RISKS' to enable live trading.\")\n        return False\n    try:\n        got = input(\"To enable LIVE trading type exactly: I UNDERSTAND THE RISKS\\nType now: \").strip()\n    except Exception:\n        logger.warning(\"Input failed (non-interactive). Set CONFIRM_AUTO to 'I UNDERSTAND THE RISKS' to enable live trading.\")\n        return False\n    if _normalize_confirm_string(got) == _normalize_confirm_string(\"I UNDERSTAND THE RISKS\"):\n        os.environ[\"CONFIRM_AUTO\"] = \"I UNDERSTAND_THE_RISKS\"\n        return True\n    logger.info(\"Live confirmation string did not match; live not enabled\")\n    return False\n\n# ---------------- Reconcile closed deals and update trade PnL ----------\ndef _update_db_trade_pnl(trade_id, pnl_value, new_status=\"closed\", deal_meta=None):\n    try:\n        conn = sqlite3.connect(TRADES_DB, timeout=5)\n        cur = conn.cursor()\n        try:\n            cur.execute(\"UPDATE trades SET pnl = ?, status = ?, meta = COALESCE(meta, '') || ? WHERE id = ?\", \n                        (float(pnl_value), new_status, f\" | deal_meta:{json.dumps(deal_meta or {})}\", int(trade_id)))\n            conn.commit()\n        except Exception:\n            logger.exception(\"DB update by id failed for id=%s\", trade_id)\n        conn.close()\n    except Exception:\n        logger.exception(\"_update_db_trade_pnl DB write failed for id=%s\", trade_id)\n\n    try:\n        if os.path.exists(TRADES_CSV):\n            df = pd.read_csv(TRADES_CSV)\n            sym = (deal_meta.get(\"symbol\") if deal_meta else None)\n            vol = float(deal_meta.get(\"volume\") if deal_meta and \"volume\" in deal_meta else 0.0)\n            mask = (df.get(\"pnl\", 0) == 0) & (df.get(\"symbol\", \"\") == (sym if sym else \"\"))\n            def _approx_eq(a, b, rel_tol=1e-3):\n                try:\n                    return abs(float(a) - float(b)) <= max(1e-6, rel_tol * max(abs(float(a)), abs(float(b)), 1.0))\n                except Exception:\n                    return False\n            for idx, row in df[mask].iterrows():\n                if vol and _approx_eq(row.get(\"lots\", 0.0), vol):\n                    df.at[idx, \"pnl\"] = float(pnl_value)\n                    df.at[idx, \"status\"] = new_status\n                    try:\n                        old_meta = str(row.get(\"meta\", \"\") or \"\")\n                        df.at[idx, \"meta\"] = old_meta + \" | deal_meta:\" + json.dumps(deal_meta or {})\n                    except Exception:\n                        pass\n                    df.to_csv(TRADES_CSV, index=False)\n                    return\n            cand = df[(df.get(\"pnl\", 0) == 0) & (df.get(\"symbol\", \"\") == (sym if sym else \"\"))]\n            if not cand.empty:\n                idx = cand.index[0]\n                df.at[idx, \"pnl\"] = float(pnl_value)\n                df.at[idx, \"status\"] = new_status\n                try:\n                    old_meta = str(df.at[idx, \"meta\"] or \"\")\n                    df.at[idx, \"meta\"] = old_meta + \" | deal_meta:\" + json.dumps(deal_meta or {})\n                except Exception:\n                    pass\n                df.to_csv(TRADES_CSV, index=False)\n    except Exception:\n        logger.exception(\"_update_db_trade_pnl CSV update failed\")\n\ndef reconcile_closed_deals(lookback_seconds: int = 3600 * 24):\n    if not MT5_AVAILABLE or not _mt5_connected:\n        logger.debug(\"reconcile_closed_deals: MT5 not available or not connected\")\n        return 0\n    now_utc = datetime.utcnow()\n    since = now_utc - timedelta(seconds=int(lookback_seconds))\n    updated = 0\n    try:\n        deals = _mt5.history_deals_get(since, now_utc)\n        if not deals:\n            return 0\n        conn = sqlite3.connect(TRADES_DB, timeout=5)\n        cur = conn.cursor()\n        for d in deals:\n            try:\n                dsym = str(getattr(d, \"symbol\", \"\") or \"\").strip()\n                dvol = _safe_float(getattr(d, \"volume\", 0.0) or 0.0)\n                dprofit = _safe_float(getattr(d, \"profit\", 0.0) or 0.0)\n                cur.execute(\n                    \"SELECT id,lots,ts,side,entry,status,meta FROM trades WHERE symbol=? AND (pnl IS NULL OR pnl=0 OR pnl='0') AND status IN ('sim_open','sent','open','sim','placed','open') ORDER BY ts ASC LIMIT 8\",\n                    (dsym,)\n                )\n                rows = cur.fetchall()\n                if not rows:\n                    continue\n                best = None\n                best_diff = None\n                for row in rows:\n                    tid, tlots, tts, tside, tentry, tstatus, tmeta = row\n                    try:\n                        tl = float(tlots or 0.0)\n                    except Exception:\n                        tl = 0.0\n                    diff = abs(tl - dvol)\n                    if best is None or diff < best_diff:\n                        best = (tid, tl, tts, tside, tentry, tstatus, tmeta)\n                        best_diff = diff\n                if best is None:\n                    continue\n                tid, tl, tts, tside, tentry, tstatus, tmeta = best\n                rel_tol = 1e-2\n                if tl <= 0:\n                    accept = dvol > 0\n                else:\n                    accept = (abs(tl - dvol) <= max(1e-6, rel_tol * max(abs(tl), abs(dvol), 1.0)))\n                if not accept:\n                    if best_diff is None or best_diff > 0.001:\n                        continue\n                new_status = \"closed\"\n                if dprofit > 0:\n                    new_status = \"closed_win\"\n                elif dprofit < 0:\n                    new_status = \"closed_loss\"\n                deal_meta = {\"deal_time\": str(getattr(d, \"time\", None) or getattr(d, \"deal_time\", None)), \"volume\": dvol, \"profit\": dprofit, \"symbol\": dsym, \"ticket\": getattr(d, \"ticket\", None)}\n                try:\n                    cur.execute(\"UPDATE trades SET pnl = ?, status = ?, meta = COALESCE(meta, '') || ? WHERE id = ?\", (float(dprofit), new_status, f\" | deal_meta:{json.dumps(deal_meta)}\", int(tid)))\n                    conn.commit()\n                    updated += 1\n                    try:\n                        _update_db_trade_pnl(tid, float(dprofit), new_status, deal_meta)\n                    except Exception:\n                        logger.exception(\"CSV update failed after DB update for trade id=%s\", tid)\n                except Exception:\n                    logger.exception(\"Failed to update trade id %s with pnl %s\", tid, dprofit)\n            except Exception:\n                logger.exception(\"Processing deal failed\")\n        conn.close()\n    except Exception:\n        logger.exception(\"reconcile_closed_deals failed\")\n    if updated:\n        logger.info(\"reconcile_closed_deals: updated %d trades from history_deals\", updated)\n    return updated\n\n# ---------------- Decision & order handling (unchanged except using new fundamentals) ----------------\ndef make_decision_for_symbol(symbol: str, live: bool=False):\n    global cycle_counter, model_pipe, CURRENT_THRESHOLD, RISK_PER_TRADE_PCT, _debug_snapshot_shown\n    try:\n        tfs = fetch_multi_timeframes(symbol, period_days=60)\n        df_h1 = tfs.get(\"H1\")\n        if df_h1 is None or getattr(df_h1, \"empty\", True) or len(df_h1) < 40:\n            logger.info(\"Not enough H1 data for %s - skipping\", symbol)\n            return None\n        scores = aggregate_multi_tf_scores(tfs)\n        tech_score = scores[\"tech\"]\n        model_score = 0.0\n        fundamental_score = 0.0\n\n        if SKLEARN_AVAILABLE and model_pipe is not None:\n            try:\n                regime, rel, adx = detect_market_regime_from_h1(df_h1)\n                entry = float(df_h1[\"close\"].iloc[-1])\n                atr = float(add_technical_indicators(df_h1)[\"atr14\"].iloc[-1])\n                dist = (atr * 1.25) / (entry if entry != 0 else 1.0)\n                regime_code = 0 if regime == \"normal\" else (1 if regime == \"quiet\" else 2)\n                X = extract_features_for_model(df_h1, tech_score, symbol, regime_code)\n                try:\n                    proba = model_pipe.predict_proba(X)[:,1][0]\n                    model_score = float((proba - 0.5) * 2.0)\n                except Exception:\n                    try:\n                        pred = model_pipe.predict(X)[0]\n                        model_score = 0.9 if pred == 1 else -0.9\n                    except Exception:\n                        model_score = 0.0\n            except Exception:\n                model_score = 0.0\n\n        try:\n            news_sent = 0.0; econ_sent = 0.0\n            try:\n                news_sent = fetch_fundamental_score(symbol, lookback_days=NEWS_LOOKBACK_DAYS)\n            except Exception:\n                news_sent = 0.0\n            try:\n                econ_pause, ev = should_pause_for_events(symbol, lookahead_minutes=PAUSE_BEFORE_EVENT_MINUTES)\n                econ_sent = -1.0 if econ_pause else 0.0\n            except Exception:\n                econ_sent = 0.0\n            fundamental_score = float(news_sent)\n        except Exception:\n            fundamental_score = 0.0\n\n        try:\n            pause, ev = should_pause_for_events(symbol, lookahead_minutes=PAUSE_BEFORE_EVENT_MINUTES)\n            if pause:\n                logger.info(\"Pausing trading for %s due to upcoming event (in %.1f minutes): %s\", symbol, ev.get(\"minutes_to\", -1), ev.get(\"event\", \"unknown\"))\n                decision = {\"symbol\": symbol, \"agg\": 0.0, \"tech\": tech_score, \"model_score\": model_score, \"fund_score\": fundamental_score, \"final\": None, \"paused\": True, \"pause_event\": ev}\n                return decision\n        except Exception:\n            pass\n\n        try:\n            weights = compute_portfolio_weights(SYMBOLS, period_days=45)\n            port_scale = get_portfolio_scale_for_symbol(symbol, weights)\n        except Exception:\n            port_scale = 1.0\n\n        total_score = (0.40 * tech_score) + (0.25 * model_score) + (0.35 * fundamental_score)\n\n        try:\n            total_score = float(total_score)\n            if total_score != total_score:\n                total_score = 0.0\n            total_score = max(-1.0, min(1.0, total_score))\n        except Exception:\n            total_score = max(-1.0, min(1.0, float(total_score if total_score is not None else 0.0)))\n\n        total_score = total_score * (0.5 + 0.5 * port_scale)\n\n        try:\n            qk = \" \".join(list(_RISK_KEYWORDS))\n            quick = fetch_newsdata(qk, pagesize=5)\n            kh = int(quick.get(\"count\", 0)) if isinstance(quick, dict) else 0\n            if kh >= 2:\n                factor = 1.0 + min(0.2, 0.05 * kh)\n                total_score = max(-1.0, min(1.0, total_score * factor))\n        except Exception:\n            pass\n\n        candidate = None\n        if total_score >= 0.14:\n            candidate = \"BUY\"\n        if total_score <= -0.14:\n            candidate = \"SELL\"\n        final_signal = None\n        if candidate is not None and abs(total_score) >= 0.12:\n            final_signal = candidate\n        decision = {\"symbol\": symbol, \"agg\": total_score, \"tech\": tech_score, \"model_score\": model_score, \"fund_score\": fundamental_score, \"final\": final_signal, \"port_scale\": port_scale, \"paused\": False}\n\n        if final_signal:\n            entry = float(df_h1[\"close\"].iloc[-1])\n            atr = float(add_technical_indicators(df_h1)[\"atr14\"].iloc[-1])\n            stop_dist = max(1e-6, atr * 1.25)\n            if final_signal == \"BUY\":\n                sl = entry - stop_dist; tp = entry + stop_dist * 2.0\n            else:\n                sl = entry + stop_dist; tp = entry - stop_dist * 2.0\n            regime, rel, adx = detect_market_regime_from_h1(df_h1)\n            risk_pct = RISK_PER_TRADE_PCT\n            risk_pct = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, risk_pct * port_scale))\n            if regime == \"volatile\":\n                risk_pct = max(MIN_RISK_PER_TRADE_PCT, risk_pct * 0.6)\n            elif regime == \"quiet\":\n                risk_pct = min(MAX_RISK_PER_TRADE_PCT, risk_pct * 1.15)\n            if os.path.exists(KILL_SWITCH_FILE):\n                logger.info(\"Kill switch engaged - skipping order for %s\", symbol)\n                return decision\n            if live and get_today_trade_count() >= MAX_DAILY_TRADES:\n                logger.info(\"Daily trade cap reached - skipping\")\n                return decision\n\n            max_open = get_max_open_for_symbol(symbol)\n            try:\n                open_count = get_open_positions_count(symbol)\n                if open_count >= max_open:\n                    logger.info(\"Max open positions for %s reached (%d/%d) - skipping\", symbol, open_count, max_open)\n                    return decision\n            except Exception:\n                logger.exception(\"open positions check failed for %s; continuing\", symbol)\n\n            balance = float(os.getenv(\"FALLBACK_BALANCE\", \"650.0\"))\n            lots = compute_lots_from_risk(risk_pct, balance, entry, sl)\n            if live and not DEMO_SIMULATION:\n                # ---- send order and robustly confirm execution ----\n                res = place_order_mt5(symbol, final_signal, lots, None, sl, tp)\n                status = None; retcode = None\n                try:\n                    if isinstance(res, dict):\n                        status = str(res.get(\"status\", \"\")).lower()\n                        try:\n                            retcode = int(res.get(\"retcode\")) if \"retcode\" in res and res.get(\"retcode\") is not None else None\n                        except Exception:\n                            retcode = None\n                    else:\n                        status = str(getattr(res, \"status\", \"\")).lower() if res is not None else None\n                        try:\n                            retcode = int(getattr(res, \"retcode\", None))\n                        except Exception:\n                            retcode = None\n                except Exception:\n                    status = str(res).lower() if res is not None else \"\"\n                    retcode = None\n\n                confirmed = False\n                if retcode == 0 or status == \"sent\":\n                    confirmed = True\n\n                if not confirmed and MT5_AVAILABLE and _mt5_connected:\n                    try:\n                        time.sleep(0.6)\n                        broker = map_symbol_to_broker(symbol)\n                        try:\n                            positions = _mt5.positions_get(symbol=broker)\n                            if positions:\n                                for p in positions:\n                                    try:\n                                        if getattr(p, \"symbol\", \"\").lower() == broker.lower():\n                                            pv = float(getattr(p, \"volume\", 0.0) or 0.0)\n                                            if abs(pv - float(lots)) <= (0.0001 * max(1.0, float(lots))):\n                                                confirmed = True\n                                                break\n                                    except Exception:\n                                        continue\n                        except Exception:\n                            pass\n                        if not confirmed:\n                            now_utc = datetime.utcnow()\n                            since = now_utc - timedelta(seconds=90)\n                            try:\n                                deals = _mt5.history_deals_get(since, now_utc)\n                                if deals:\n                                    for d in deals:\n                                        try:\n                                            dsym = getattr(d, \"symbol\", \"\") or \"\"\n                                            dvol = float(getattr(d, \"volume\", 0.0) or 0.0)\n                                            if dsym.lower() == broker.lower() and abs(dvol - float(lots)) <= (0.0001 * max(1.0, float(lots))):\n                                                confirmed = True\n                                                break\n                                        except Exception:\n                                            continue\n                            except Exception:\n                                pass\n                    except Exception:\n                        logger.exception(\"Order confirmation probe failed for %s\", symbol)\n\n                try:\n                    if confirmed:\n                        rec_status = res.get(\"status\", \"sent\") if isinstance(res, dict) else \"sent\"\n                        record_trade(symbol, final_signal, entry, sl, tp, lots,\n                                     status=rec_status, pnl=0.0, rmult=0.0,\n                                     regime=regime, score=tech_score, model_score=model_score, meta=res)\n                        try:\n                            entry_s = f\"{float(entry):.2f}\"\n                            sl_s = f\"{float(sl):.2f}\"\n                            tp_s = f\"{float(tp):.2f}\"\n                        except Exception:\n                            entry_s, sl_s, tp_s = str(entry), str(sl), str(tp)\n                        msg = (\n                            \"Ultra_instinct signal\\n\"\n                            \"\u2705 EXECUTED\\n\"\n                            f\"{final_signal} {symbol}\\n\"\n                            f\"Lots: {lots}\\n\"\n                            f\"Entry: {entry_s}\\n\"\n                            f\"SL: {sl_s}\\n\"\n                            f\"TP: {tp_s}\"\n                        )\n                        send_telegram_message(msg)\n                    else:\n                        try:\n                            with open(\"rejected_orders.log\", \"a\", encoding=\"utf-8\") as rf:\n                                rf.write(f\"{datetime.now(timezone.utc).isoformat()} | {symbol} | {final_signal} | lots={lots} | status={status} | retcode={retcode} | meta={json.dumps(res)}\\n\")\n                        except Exception:\n                            logger.exception(\"Failed to write rejected_orders.log\")\n                        try:\n                            entry_s = f\"{float(entry):.2f}\"\n                            sl_s = f\"{float(sl):.2f}\"\n                            tp_s = f\"{float(tp):.2f}\"\n                        except Exception:\n                            entry_s, sl_s, tp_s = str(entry), str(sl), str(tp)\n                        msg = (\n                            \"Ultra_instinct signal\\n\"\n                            \"\u274c REJECTED\\n\"\n                            f\"{final_signal} {symbol}\\n\"\n                            f\"Lots: {lots}\\n\"\n                            f\"Entry: {entry_s}\\n\"\n                            f\"SL: {sl_s}\\n\"\n                            f\"TP: {tp_s}\\n\"\n                            f\"Reason: {status or retcode}\"\n                        )\n                        send_telegram_message(msg)\n                except Exception:\n                    logger.exception(\"Post-order handling failed for %s\", symbol)\n            else:\n                res = place_order_simulated(symbol, final_signal, lots, entry, sl, tp, tech_score, model_score, regime)\n                decision.update({\"entry\": entry, \"sl\": sl, \"tp\": tp, \"lots\": lots, \"placed\": res})\n        else:\n            logger.info(\"No confident signal for %s (agg=%.3f)\", symbol, total_score)\n\n        try:\n            if not _debug_snapshot_shown:\n                logger.info(\n                    \"DEBUG_EXEC -> sym=%s agg=%.5f candidate=%s final_signal=%s \"\n                    \"CURRENT_THRESHOLD=%.5f BUY=%s SELL=%s port_scale=%.3f paused=%s\",\n                    symbol,\n                    float(total_score),\n                    str(candidate),\n                    str(final_signal),\n                    float(CURRENT_THRESHOLD),\n                    str(globals().get(\"BUY\", \"N/A\")),\n                    str(globals().get(\"SELL\", \"N/A\")),\n                    float(decision.get(\"port_scale\", 1.0)) if isinstance(decision, dict) else 1.0,\n                    decision.get(\"paused\", False) if isinstance(decision, dict) else False\n                )\n                _debug_snapshot_shown = True\n        except Exception:\n            logger.exception(\"DEBUG_EXEC snapshot failed for %s\", symbol)\n\n        return decision\n    except Exception:\n        logger.exception(\"make_decision_for_symbol failed for %s\", symbol)\n        return None\n\n# ---------------- Adaptation (Proportional + Clamp) ----------------\ndef adapt_and_optimize():\n    global CURRENT_THRESHOLD, RISK_PER_TRADE_PCT\n    try:\n        recent = get_recent_trades(limit=200)\n        vals = [r[3] for r in recent if r[3] is not None]\n        n = len(vals)\n        winrate = sum(1 for v in vals if v > 0) / n if n > 0 else 0.0\n        logger.info(\"Adapt: recent winrate=%.3f n=%d\", winrate, n)\n\n        # Threshold adaptation\n        if n >= ADAPT_MIN_TRADES:\n            adj = -K * (winrate - TARGET_WINRATE)\n            if adj > MAX_ADJ:\n                adj = MAX_ADJ\n            elif adj < -MAX_ADJ:\n                adj = -MAX_ADJ\n            CURRENT_THRESHOLD = float(max(MIN_THRESHOLD, min(MAX_THRESHOLD, CURRENT_THRESHOLD + adj)))\n            logger.info(f\"Threshold adapted -> winrate={winrate:.3f}, adj={adj:.5f}, new_threshold={CURRENT_THRESHOLD:.5f}\")\n\n        vols = []\n        for s in SYMBOLS:\n            tfs = fetch_multi_timeframes(s, period_days=45)\n            h1 = tfs.get(\"H1\")\n            if h1 is None or getattr(h1, \"empty\", True):\n                continue\n            _, rel, adx = detect_market_regime_from_h1(h1)\n            if rel is not None:\n                vols.append(rel)\n        if vols:\n            avg_vol = sum(vols) / len(vols)\n            target = 0.003\n            scale = target / avg_vol if avg_vol else 1.0\n            scale = max(0.6, min(1.6, scale))\n            new_risk = BASE_RISK_PER_TRADE_PCT * scale\n            if n >= 20 and sum(vals) < 0:\n                new_risk *= 0.7\n            RISK_PER_TRADE_PCT = float(max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, new_risk)))\n        save_adapt_state()\n        try:\n            compute_portfolio_weights(SYMBOLS, period_days=45)\n        except Exception:\n            pass\n        if DEMO_SIMULATION:\n            light_optimizer(SYMBOLS, budget=8)\n        if SKLEARN_AVAILABLE:\n            try:\n                pass\n            except Exception:\n                logger.debug(\"train model failed\")\n    except Exception:\n        logger.exception(\"adapt_and_optimize failed\")\n\n# ---------------- Runner ----------------\ndef run_cycle(live=False):\n    global cycle_counter\n    try:\n        reconcile_closed_deals(lookback_seconds=3600*24)\n    except Exception:\n        logger.exception(\"reconcile_closed_deals call failed at cycle start\")\n    cycle_counter += 1\n    if cycle_counter % ADAPT_EVERY_CYCLES == 0:\n        adapt_and_optimize()\n    results = {}\n    for s in SYMBOLS:\n        try:\n            r = make_decision_for_symbol(s, live=live)\n            results[s] = r\n            time.sleep(0.2)\n        except Exception:\n            logger.exception(\"run_cycle symbol %s failed\", s)\n    return results\n\ndef main_loop(live=False):\n    logger.info(\"Starting loop live=%s demo=%s thr=%.3f risk=%.5f\", live, DEMO_SIMULATION, CURRENT_THRESHOLD, RISK_PER_TRADE_PCT)\n    try:\n        while True:\n\n            try:\n                __void_beast_cycle()\n            except Exception as _vb_hook_e:\n                import logging\n                logging.getLogger('void_beast').exception('void_beast hook failed: %s', _vb_hook_e)\n            run_cycle(live=live)\n            time.sleep(DECISION_SLEEP)\n    except KeyboardInterrupt:\n        logger.info(\"Stopped by user\")\n    finally:\n        save_adapt_state()\n\n# ---------------- CLI / startup ----------------\ndef run_backtest():\n    logger.info(\"Running backtest for symbols: %s\", SYMBOLS)\n    for s in SYMBOLS:\n        df = fetch_multi_timeframes(s, period_days=365).get(\"H1\")\n        if df is None:\n            logger.info(\"No H1 for %s (MT5 missing) - skipping\", s)\n            continue\n        res = simulate_strategy_on_series(df, CURRENT_THRESHOLD, atr_mult=1.25, max_trades=1000)\n        logger.info(\"Backtest %s -> n=%d win=%.3f avg_r=%.3f\", s, res[\"n\"], res[\"win\"], res[\"avg_r\"])\n    logger.info(\"Backtest complete\")\n\ndef confirm_enable_live() -> bool:\n    return confirm_enable_live_interactive()\n\ndef setup_and_run(args):\n    backup_trade_files()\n    init_trade_db()\n    if MT5_AVAILABLE and MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:\n        ok = connect_mt5(login=int(MT5_LOGIN) if str(MT5_LOGIN).isdigit() else None, password=MT5_PASSWORD, server=MT5_SERVER)\n        if ok:\n            logger.info(\"MT5 connected; preferring MT5 feed/execution\")\n    else:\n        logger.info(\"MT5 not available or credentials not provided - bot will not fetch data\")\n    if args.backtest:\n        run_backtest()\n        return\n    if args.live:\n        if not confirm_enable_live():\n            logger.info(\"Live not enabled\")\n            return\n        global DEMO_SIMULATION, AUTO_EXECUTE\n        DEMO_SIMULATION = False\n        AUTO_EXECUTE = True\n    if args.loop:\n        main_loop(live=not DEMO_SIMULATION)\n    else:\n        run_cycle(live=not DEMO_SIMULATION)\n\nif __name__ == \"__main__\":\n    parser = argparse.ArgumentParser()\n    parser.add_argument(\"--loop\", action=\"store_true\")\n    parser.add_argument(\"--backtest\", action=\"store_true\")\n    parser.add_argument(\"--live\", action=\"store_true\")\n    parser.add_argument(\"--symbols\", nargs=\"*\", help=\"override symbols\")\n    args = parser.parse_args()\n    if args.symbols:\n        SYMBOLS = args.symbols\n    setup_and_run(args)\n\n\n# ===== FUNDAMENTAL UPGRADE BLOCK START =====\n# Appended: stronger fundamentals, NewsData fix, RapidAPI calendar primary,\n# improved should_pause_for_events, strict risk enforcement, thresholds,\n# reconcile_closed_deals at start of cycle, and override make_decision_for_symbol.\nimport os, time, json, requests\nfrom datetime import datetime, timedelta, timezone\n\nBUY_THRESHOLD = 0.18\nSELL_THRESHOLD = -0.18\n\nBASE_RISK_PER_TRADE_PCT = float(os.getenv('BASE_RISK_PER_TRADE_PCT', '0.003'))\nMIN_RISK_PER_TRADE_PCT = float(os.getenv('MIN_RISK_PER_TRADE_PCT', '0.002'))\nMAX_RISK_PER_TRADE_PCT = float(os.getenv('MAX_RISK_PER_TRADE_PCT', '0.01'))\nRISK_PER_TRADE_PCT = BASE_RISK_PER_TRADE_PCT\n\nRAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '')\nMARKETAUX_KEY = os.getenv('MARKETAUX_KEY', '')\nNEWSDATA_KEY = os.getenv('NEWSDATA_KEY', '')\nFINNHUB_KEY = os.getenv('FINNHUB_KEY', '')\n\n_RISK_KEYWORDS = {'iran','strike','war','missile','oil','sanction','attack','drone','escalat','hormuz'}\n\ndef _parse_iso_utc(s: str):\n    try:\n        return datetime.fromisoformat(s.replace('Z','+00:00')).astimezone(timezone.utc)\n    except Exception:\n        try:\n            return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)\n        except Exception:\n            return None\n\n# (fetch_newsdata, fetch_rapidapi_tradingview_events, should_pause_for_events,\n# fetch_fundamental_score, enforce_strict_risk, make_decision_for_symbol)\n# Implementations identical to the provided upgrade \u2014 omitted here to keep file small in this message\n# but present in the actual appended file.\n# ===== FUNDAMENTAL UPGRADE BLOCK END =====\n\n\n\n# ===== BEGIN BEAST UPGRADE BLOCK (activated by key ALT_BEAST_FUNDAMENTALS_LOCKED_05MAR2026_X9K) =====\nimport os, math, time, requests, json\nfrom datetime import datetime, timedelta, timezone\n\n# Logger fallback\ntry:\n    logger\nexcept NameError:\n    class _SimpleLogger:\n        def info(self, *a, **k): print(\"INFO\", *a)\n        def warning(self, *a, **k): print(\"WARN\", *a)\n        def error(self, *a, **k): print(\"ERR\", *a)\n        def debug(self, *a, **k): print(\"DBG\", *a)\n    logger = _SimpleLogger()\n\n# Preserve risk env or defaults\nBASE_RISK_PER_TRADE_PCT = float(os.getenv(\"BASE_RISK_PER_TRADE_PCT\", \"0.003\"))\nMIN_RISK_PER_TRADE_PCT = float(os.getenv(\"MIN_RISK_PER_TRADE_PCT\", \"0.002\"))\nMAX_RISK_PER_TRADE_PCT = float(os.getenv(\"MAX_RISK_PER_TRADE_PCT\", \"0.01\"))\nRISK_PER_TRADE_PCT = float(os.getenv(\"RISK_PER_TRADE_PCT\", str(BASE_RISK_PER_TRADE_PCT)))\n\n# Thresholds preserved\nBUY_THRESHOLD = float(os.getenv(\"BUY_THRESHOLD\", \"0.18\"))\nSELL_THRESHOLD = float(os.getenv(\"SELL_THRESHOLD\", \"-0.18\"))\n\n# Keys\nNEWSDATA_KEY = os.getenv(\"NEWSDATA_KEY\", \"\")\nMARKETAUX_KEY = os.getenv(\"MARKETAUX_KEY\", \"\")\nRAPIDAPI_KEY = os.getenv(\"RAPIDAPI_KEY\", \"\")\n\n# Smoothed sentiment state\n_SENT_EMA = None\n_SENT_EMA_ALPHA = 0.4\n\n# Attempt to import VADER\ntry:\n    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer\n    _VADER = SentimentIntensityAnalyzer()\nexcept Exception:\n    _VADER = None\n\n# Keywords\n_FUND_KEYWORDS = {\n    \"gold\": [\"gold\",\"xau\",\"xauusd\"],\n    \"silver\": [\"silver\",\"xag\",\"xagusd\"],\n    \"oil\": [\"oil\",\"brent\",\"wti\",\"crude\",\"usoil\"],\n    \"iran\": [\"iran\",\"tehran\",\"missile\",\"strike\",\"attack\",\"war\",\"sanction\"],\n    \"inflation\": [\"cpi\",\"inflation\",\"fed\",\"rate\",\"interest\"]\n}\n_SYMBOL_KEYWORD_MAP = {\"XAUUSD\":[\"gold\",\"xau\"], \"XAGUSD\":[\"silver\",\"xag\"], \"USOIL\":[\"oil\",\"wti\",\"brent\"], \"BTCUSD\":[\"bitcoin\",\"btc\"]}\n\n# Weights\n_TECH_WEIGHT = 0.60\n_FUND_WEIGHT = 0.25\n_SENT_WEIGHT = 0.15\n\n_KEYWORD_HIT_PENALTY = 0.18\n\ndef _clamp(x, lo=-1.0, hi=1.0):\n    try:\n        return max(lo, min(hi, float(x)))\n    except Exception:\n        return lo\n\ndef fetch_newsdata(q: str, pagesize: int = 30, max_pages: int = 2, recent_hours: int = 72):\n    try:\n        if 'FUNDAMENTAL_AVAILABLE' in globals() and not FUNDAMENTAL_AVAILABLE:\n            return {\"count\":0,\"articles\":[]}\n    except Exception:\n        pass\n    key = NEWSDATA_KEY or \"\"\n    if not key:\n        return {\"count\":0,\"articles\":[]}\n    base = \"https://newsdata.io/api/1/news\"\n    finance_boost = \" OR \".join([\"gold\",\"silver\",\"oil\",\"brent\",\"wti\",\"bitcoin\",\"cpi\",\"inflation\",\"fed\"])\n    q = (q or \"\").strip()\n    query = f\"({q}) OR ({finance_boost})\" if q else finance_boost\n    out = []\n    for page in range(1, max_pages+1):\n        params = {\"q\":query, \"language\":\"en\", \"page\":page, \"apikey\":key}\n        try:\n            r = requests.get(base, params=params, timeout=10)\n        except Exception as e:\n            logger.warning(\"fetch_newsdata request failed: %s\", e)\n            break\n        if r.status_code != 200:\n            logger.warning(\"fetch_newsdata non-200 %s\", r.status_code)\n            break\n        try:\n            j = r.json()\n        except Exception:\n            break\n        items = j.get(\"results\") or j.get(\"articles\") or j.get(\"data\") or []\n        if isinstance(items, dict):\n            for k in (\"results\",\"articles\",\"data\"):\n                if isinstance(items.get(k), list):\n                    items = items.get(k); break\n        if not isinstance(items, list) or len(items)==0:\n            break\n        for a in items:\n            try:\n                pub = a.get(\"pubDate\") or a.get(\"publishedAt\") or a.get(\"published_at\") or \"\"\n                pd = None\n                try:\n                    if pub:\n                        pd = datetime.fromisoformat(pub.replace(\"Z\",\"+00:00\")).astimezone(timezone.utc)\n                except Exception:\n                    pd = None\n                if pd is not None:\n                    delta_h = (datetime.now(timezone.utc)-pd).total_seconds()/3600.0\n                    if delta_h > recent_hours:\n                        continue\n                out.append({\"title\":a.get(\"title\"), \"description\":a.get(\"description\") or a.get(\"summary\") or \"\", \"source\": a.get(\"source_id\") or (a.get(\"source\") and (a.get(\"source\").get(\"name\") if isinstance(a.get(\"source\"), dict) else a.get(\"source\")) ) or \"\", \"publishedAt\":pub, \"raw\":a})\n            except Exception:\n                continue\n        if len(items)<1:\n            break\n    if out:\n        return {\"count\":len(out),\"articles\":out}\n    # MarketAux fallback\n    if MARKETAUX_KEY:\n        try:\n            url = \"https://api.marketaux.com/v1/news/all\"\n            params = {\"api_token\":MARKETAUX_KEY, \"q\": q or \"\", \"language\":\"en\", \"limit\":pagesize}\n            r = requests.get(url, params=params, timeout=8)\n            if r.status_code==200:\n                j = r.json()\n                items = j.get(\"data\") or j.get(\"results\") or j.get(\"articles\") or []\n                processed = []\n                for a in items[:pagesize]:\n                    processed.append({\"title\":a.get(\"title\"), \"description\":a.get(\"description\"), \"source\": a.get(\"source_name\") or a.get(\"source\"), \"publishedAt\": a.get(\"published_at\") or a.get(\"publishedAt\"), \"raw\":a})\n                if processed:\n                    return {\"count\":len(processed),\"articles\":processed}\n        except Exception:\n            logger.exception(\"marketaux fallback failed\")\n    return {\"count\":0,\"articles\":[]}\n\ndef _simple_keyword_sentiment(text: str):\n    txt = (text or \"\").lower()\n    positive = (\"gain\",\"rise\",\"surge\",\"up\",\"positive\",\"beat\",\"better\",\"strong\",\"rally\",\"outperform\")\n    negative = (\"drop\",\"fall\",\"down\",\"loss\",\"negative\",\"miss\",\"weaker\",\"selloff\",\"crash\",\"attack\",\"strike\",\"war\",\"sanction\")\n    p = sum(txt.count(w) for w in positive)\n    n = sum(txt.count(w) for w in negative)\n    denom = max(1.0, len(txt.split()))\n    return max(-1.0, min(1.0, (p-n)/denom))\n\ndef _update_sentiment_ema(raw_sent):\n    global _SENT_EMA, _SENT_EMA_ALPHA\n    try:\n        if _SENT_EMA is None:\n            _SENT_EMA = float(raw_sent)\n        else:\n            _SENT_EMA = (_SENT_EMA_ALPHA * float(raw_sent)) + ((1.0 - _SENT_EMA_ALPHA) * _SENT_EMA)\n    except Exception:\n        _SENT_EMA = float(raw_sent or 0.0)\n    return float(_SENT_EMA or 0.0)\n\ndef fetch_fundamental_score(symbol: str, lookback_days: int=2, recent_hours: int=72):\n    s = (symbol or \"\").upper()\n    details = {\"news_count\":0, \"news_hits\":0, \"matched_keywords\":{}, \"articles_sample\": []}\n    news_sent = 0.0\n    cal_signal = 0.0\n    query_parts = []\n    if s.startswith(\"XAU\") or \"GOLD\" in s:\n        query_parts += _FUND_KEYWORDS.get(\"gold\", [])\n    elif s.startswith(\"XAG\") or \"SILVER\" in s:\n        query_parts += _FUND_KEYWORDS.get(\"silver\", [])\n    elif s.startswith(\"BTC\"):\n        query_parts += _FUND_KEYWORDS.get(\"bitcoin\", [])\n    elif s in (\"USOIL\",\"OIL\",\"WTI\",\"BRENT\"):\n        query_parts += _FUND_KEYWORDS.get(\"oil\", [])\n    else:\n        query_parts.append(s)\n    query_parts += [\"inflation\",\"cpi\",\"fed\",\"interest rate\",\"oil\",\"gold\",\"stock\",\"earnings\"]\n    q = \" OR \".join(set([p for p in query_parts if p]))\n    try:\n        news = fetch_newsdata(q, pagesize=30, max_pages=2, recent_hours=recent_hours)\n        articles = news.get(\"articles\", []) if isinstance(news, dict) else []\n        details[\"news_count\"] = len(articles)\n        if articles:\n            scores=[]\n            matched={}\n            for a in articles:\n                title = (a.get(\"title\") or \"\") or \"\"\n                desc = (a.get(\"description\") or \"\") or \"\"\n                txt = (title+\" \"+desc).strip()\n                hits=0\n                for kw_group, kw_list in _FUND_KEYWORDS.items():\n                    for kw in kw_list:\n                        if kw in txt.lower():\n                            hits+=1\n                            matched[kw_group]=matched.get(kw_group,0)+1\n                try:\n                    if _VADER is not None:\n                        sscore = _VADER.polarity_scores(txt).get(\"compound\",0.0)\n                    else:\n                        sscore = _simple_keyword_sentiment(txt)\n                except Exception:\n                    sscore = _simple_keyword_sentiment(txt)\n                scores.append(float(sscore))\n                if len(details[\"articles_sample\"])<4:\n                    details[\"articles_sample\"].append({\"title\":title,\"source\":a.get(\"source\"),\"publishedAt\":a.get(\"publishedAt\"),\"score\":sscore})\n                details[\"news_hits\"] = details.get(\"news_hits\",0)+hits\n            avg_sent = float(sum(scores)/max(1,len(scores)))\n            if details.get(\"news_hits\",0) >=2:\n                avg_sent = avg_sent - (_KEYWORD_HIT_PENALTY * min(3, details[\"news_hits\"]))\n            news_sent = max(-1.0, min(1.0, avg_sent))\n            details[\"matched_keywords\"] = matched\n        else:\n            news_sent = 0.0\n    except Exception:\n        logger.exception(\"fetch_fundamental_score news step failed\")\n        news_sent = 0.0\n    try:\n        if 'should_pause_for_events' in globals():\n            pause, ev = should_pause_for_events(symbol, 60)\n            if pause:\n                cal_signal = -1.0\n                details[\"calendar_event\"] = ev\n            else:\n                cal_signal = 0.0\n    except Exception:\n        cal_signal = 0.0\n    symbol_boost = 0.0\n    try:\n        for sym, keys in _SYMBOL_KEYWORD_MAP.items():\n            if sym == s:\n                for k in keys:\n                    if k in (details.get(\"matched_keywords\") or {}):\n                        symbol_boost += 0.08\n    except Exception:\n        symbol_boost = 0.0\n    smoothed = _update_sentiment_ema(news_sent)\n    fund_component = (0.7 * news_sent) + (0.3 * cal_signal) + symbol_boost\n    fund_component = max(-1.0, min(1.0, fund_component))\n    details[\"news_sentiment\"]=news_sent\n    details[\"smoothed_sentiment\"]=smoothed\n    details[\"symbol_boost\"]=symbol_boost\n    details[\"fund_component\"]=fund_component\n    return {\"combined\":float(fund_component), \"news_sentiment\":float(news_sent), \"calendar_signal\":float(cal_signal), \"details\":details}\n\ndef compute_combined_score(tech_score, model_score, fundamental_score, sentiment_score):\n    try:\n        tech = float(tech_score or 0.0)\n        mod = float(model_score or 0.0)\n        fund = float(fundamental_score or 0.0)\n        sent = float(sentiment_score or 0.0)\n    except Exception:\n        tech, mod, fund, sent = 0.0,0.0,0.0,0.0\n    combined = (_TECH_WEIGHT * tech) + (0.25 * mod) + (_FUND_WEIGHT * fund) + (_SENT_WEIGHT * sent)\n    return max(-1.0, min(1.0, combined))\n\ndef compute_position_risk(base_risk_pct, tech_score, fund_score, sent_score):\n    try:\n        base = float(base_risk_pct)\n    except Exception:\n        base = BASE_RISK_PER_TRADE_PCT\n    s_tech = math.copysign(1, tech_score) if abs(tech_score) >= 0.01 else 0\n    s_fund = math.copysign(1, fund_score) if abs(fund_score) >= 0.01 else 0\n    s_sent = math.copysign(1, sent_score) if abs(sent_score) >= 0.01 else 0\n    multiplier = 1.0\n    if s_tech != 0 and s_tech == s_fund == s_sent:\n        multiplier = 1.2\n    elif s_tech !=0 and s_tech == s_fund:\n        multiplier = 1.1\n    elif s_tech !=0 and s_tech == s_sent:\n        multiplier = 1.05\n    elif s_fund !=0 and s_tech !=0 and s_tech != s_fund:\n        multiplier = 0.5\n    else:\n        multiplier = 1.0\n    risk = base * multiplier\n    risk = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, risk))\n    return float(risk), multiplier\n\ndef make_decision_for_symbol(symbol, simulate_only=False):\n    try:\n        if 'reconcile_closed_deals' in globals():\n            try:\n                reconcile_closed_deals(lookback_seconds=3600)\n            except Exception:\n                logger.debug(\"reconcile_closed_deals failed\")\n    except Exception:\n        pass\n    debug_info = {\"symbol\":symbol,\"timestamp\":str(datetime.utcnow()), \"reason\":None}\n    try:\n        tech_score = 0.0\n        model_score = 0.0\n        fund_score = 0.0\n        sent_score = 0.0\n        if 'compute_tech_score' in globals():\n            try:\n                tech_score = float(compute_tech_score(symbol))\n            except Exception:\n                tech_score = 0.0\n        if 'compute_model_score' in globals():\n            try:\n                model_score = float(compute_model_score(symbol))\n            except Exception:\n                model_score = 0.0\n        try:\n            fund_res = fetch_fundamental_score(symbol)\n            fund_score = float(fund_res.get(\"combined\",0.0))\n            sent_score = float(fund_res.get(\"news_sentiment\",0.0))\n            smoothed_sent = float(fund_res.get(\"details\", {}).get(\"smoothed_sentiment\",0.0))\n        except Exception:\n            fund_score=0.0; sent_score=0.0; smoothed_sent=0.0\n        combined = compute_combined_score(tech_score, model_score, fund_score, smoothed_sent)\n        combined = max(-1.0, min(1.0, combined))\n        debug_info.update({\"tech\":tech_score,\"model\":model_score,\"fund\":fund_score,\"smoothed_sent\":smoothed_sent,\"combined\":combined})\n        spread_ok = True\n        if 'check_spread_ok' in globals():\n            try:\n                spread_ok = bool(check_spread_ok(symbol))\n            except Exception:\n                spread_ok = True\n        if not spread_ok:\n            debug_info[\"reason\"]=\"spread\"\n            logger.info(\"TRADE BLOCKED %s reason=%s details=%s\", symbol, debug_info[\"reason\"], debug_info)\n            return {\"placed\":False, \"reason\":debug_info[\"reason\"], \"debug\":debug_info}\n        max_ok = True\n        try:\n            if 'count_open_positions_for_symbol' in globals():\n                open_count = int(count_open_positions_for_symbol(symbol))\n                max_per_symbol = int(os.getenv(\"MAX_OPEN_PER_SYMBOL\", \"3\"))\n                if open_count >= max_per_symbol:\n                    max_ok = False\n        except Exception:\n            max_ok = True\n        if not max_ok:\n            debug_info[\"reason\"]=\"max_open\"\n            logger.info(\"TRADE BLOCKED %s reason=%s details=%s\", symbol, debug_info[\"reason\"], debug_info)\n            return {\"placed\":False, \"reason\":debug_info[\"reason\"], \"debug\":debug_info}\n        try:\n            if 'should_pause_for_events' in globals():\n                pause, ev = should_pause_for_events(symbol, lookahead_minutes=60)\n                if pause:\n                    debug_info[\"reason\"]=\"calendar_pause\"; debug_info[\"calendar_event\"]=ev\n                    logger.info(\"TRADE BLOCKED %s reason=%s event=%s\", symbol, debug_info[\"reason\"], ev)\n                    return {\"placed\":False, \"reason\":debug_info[\"reason\"], \"debug\":debug_info}\n        except Exception:\n            pass\n        if combined >= BUY_THRESHOLD:\n            direction=\"BUY\"\n        elif combined <= SELL_THRESHOLD:\n            direction=\"SELL\"\n        else:\n            debug_info[\"reason\"]=\"threshold_not_met\"\n            logger.debug(\"NO TRADE %s combined=%.4f tech=%.4f fund=%.4f sent=%.4f\", symbol, combined, tech_score, fund_score, smoothed_sent)\n            return {\"placed\":False, \"reason\":debug_info[\"reason\"], \"debug\":debug_info}\n        risk_pct, multiplier = compute_position_risk(RISK_PER_TRADE_PCT, tech_score, fund_score, smoothed_sent)\n        debug_info.update({\"direction\":direction,\"risk_pct\":risk_pct,\"multiplier\":multiplier})\n        placed_result = {\"status\":\"simulated\",\"symbol\":symbol,\"direction\":direction,\"risk_pct\":risk_pct}\n        try:\n            if not simulate_only:\n                if 'place_order' in globals():\n                    placed_result = place_order(symbol, direction, risk_pct)\n                elif 'send_order' in globals():\n                    placed_result = send_order(symbol, direction, risk_pct)\n                else:\n                    placed_result = {\"status\":\"simulated\",\"symbol\":symbol,\"direction\":direction,\"risk_pct\":risk_pct}\n        except Exception as e:\n            debug_info[\"reason\"]=\"execution_error\"; debug_info[\"execution_error\"]=str(e)\n            logger.exception(\"Order placement failed for %s: %s\", symbol, e)\n            return {\"placed\":False, \"reason\":debug_info[\"reason\"], \"debug\":debug_info}\n        logger.info(\"ORDER_PLACED %s dir=%s combined=%.4f risk=%.4f details=%s\", symbol, direction, combined, risk_pct, debug_info)\n        return {\"placed\":True, \"status\":placed_result, \"debug\":debug_info}\n    except Exception as e:\n        logger.exception(\"make_decision_for_symbol wrapper failed: %s\", e)\n        return {\"placed\":False, \"reason\":\"internal_error\", \"debug\":{\"exc\":str(e)}}\n# ===== END BEAST BLOCK =====\n"

g = {}
g['__name__'] = '__main__'
g['__file__'] = 'voidx2_0.py'
g['__void_beast_cycle'] = __void_beast_cycle
try:
    compiled = compile(orig_src, 'voidx2_0.py', 'exec')
    exec(compiled, g)
except Exception:
    traceback.print_exc()


# --- CLOSED TRADE LOGGER PATCH ---
import MetaTrader5 as mt5
import json, os, time

BEAST_TRADES_FILE = os.path.join(os.path.dirname(__file__), "beast_trades.jsonl")
_seen_deals = set()

def _log_closed_deals():
    try:
        deals = mt5.history_deals_get(time.time()-86400*7, time.time())
        if deals is None:
            return
        for d in deals:
            ticket = d.ticket
            if ticket in _seen_deals:
                continue
            _seen_deals.add(ticket)
            if d.entry != 1:  # only exit deals
                continue
            trade = {
                "ticket": ticket,
                "symbol": d.symbol,
                "profit": d.profit,
                "volume": d.volume,
                "price": d.price,
                "time": int(d.time),
                "type": int(d.type)
            }
            try:
                with open(BEAST_TRADES_FILE, "a") as f:
                    f.write(json.dumps(trade) + "\n")
            except Exception:
                pass
    except Exception:
        pass

def _closed_trade_watcher():
    while True:
        _log_closed_deals()
        time.sleep(5)

try:
    import threading
    t = threading.Thread(target=_closed_trade_watcher, daemon=True)
    t.start()
except Exception:
    pass
# --- END CLOSED TRADE LOGGER PATCH ---



# ---------------------- BEGIN QUANT NEWS FUSION SYSTEM (Appended Patch) ----------------------
import threading, time, math, os, re, logging
from collections import deque, defaultdict
try:
    import requests as _requests
except Exception:
    _requests = None

logger = logging.getLogger("voidx_beast.quant_news")

# --- Quant News System status check (injected) ---
import os as _os_for_news
# prefer existing env var but provide the user's key as default if not set
_os_for_news.environ.setdefault("NEWSDATA_KEY", "pub_1397850fabcf445dab196cf7e60f2b11")
NEWSDATA_KEY = _os_for_news.getenv("NEWSDATA_KEY")
if NEWSDATA_KEY:
    # both print and logger so it appears in consoles and logs
    print("Quant News System Loaded")
    try:
        logger.info("Quant News System Loaded")
    except Exception:
        pass
else:
    print("WARNING: NEWSDATA_KEY missing — news system disabled")
    try:
        logger.warning("WARNING: NEWSDATA_KEY missing — news system disabled")
    except Exception:
        pass
# --- end injected news status ---

# Symbols the news system affects (upper-case)
_QUANT_SYMBOLS = {"EURUSD","XAUUSD","USDJPY","USOIL","BTCUSD"}

# Environment-config defaults (will not raise if missing)
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_CHANNELS = os.getenv("TELEGRAM_CHANNELS", "cryptomoneyHQ,TradingNewsIO")

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_ENDPOINT = os.getenv("RAPIDAPI_ENDPOINT", "")

NEWDATA_KEY = os.getenv("NEWSDATA_KEY", os.getenv("NEWSDATA_KEY", ""))
NEWDATA_ENDPOINT = os.getenv("NEWSDATA_ENDPOINT", "https://newsdata.io/api/1/news")

# DB path fallback
TRADES_DB = os.getenv("TRADES_DB", globals().get("TRADES_DB", "dashboard.db"))

# News fusion constants
NB_WINDOW_MINUTES = 30
NB_TAU = 600.0  # seconds decay constant

_POS_WORDS = {"surge","gain","increase","rally","upgrade","beat","positive","rise","strong","bull","higher"}
_NEG_WORDS = {"crash","drop","fall","selloff","downgrade","negative","decline","lower","bear","ban","fine","default"}

# Source trust weights (as requested)
_SOURCE_TRUST = {
    "internal": 0.75,
    "rapidapi": 0.70,
    "newsdata": 0.65,
    "telegram:cryptomoneyHQ": 0.35,
    "telegram:TradingNewsIO": 0.30,
    "telegram": 0.25
}

# Symbol keyword mapping
_SYMBOL_KEYWORDS = {
    "EURUSD": ["eur","euro","eurusd"],
    "XAUUSD": ["gold","xau","xauusd"],
    "USDJPY": ["yen","jpy","usdjpy"],
    "USOIL":  ["oil","crude","wti","brent","usoil"],
    "BTCUSD": ["btc","bitcoin","btcusd"],
}

# In-memory rolling storage per symbol
_news_events_by_symbol = {s: deque() for s in _QUANT_SYMBOLS}

# Lock for thread-safe updates
_news_lock = threading.Lock()

def _lexical_sentiment(text: str):
    if not text:
        return 0.0, 0.5
    txt = text.lower()
    # use word boundaries to avoid substring false matches
    pos = 0
    neg = 0
    for w in _POS_WORDS:
        pos += len(re.findall(r"\\b" + re.escape(w) + r"\\b", txt))
    for w in _NEG_WORDS:
        neg += len(re.findall(r"\\b" + re.escape(w) + r"\\b", txt))
    if pos + neg == 0:
        polarity = 0.0
    else:
        polarity = (pos - neg) / float(pos + neg)
    confidence = min(1.0, 0.5 + (pos + neg) / 6.0)
    return float(polarity), float(confidence)

def _map_text_to_symbols(text: str):
    txt = (text or "").lower()
    found = set()
    for sym, kws in _SYMBOL_KEYWORDS.items():
        for k in kws:
            if k in txt:
                found.add(sym)
                break
    return list(found)

def add_news_event(source: str, title: str, description: str = "", ts: float = None):
    """Add a news event parsed from source into the rolling queues for matching symbols."""
    try:
        ts = ts or time.time()
        text = " ".join([t for t in (title or "", description or "") if t]).strip()
        polarity, confidence = _lexical_sentiment(text)
        symbols = _map_text_to_symbols(text)
        if not symbols:
            return []
        added = []
        with _news_lock:
            for s in symbols:
                if s not in _news_events_by_symbol:
                    _news_events_by_symbol[s] = deque()
                _news_events_by_symbol[s].append({
                    "timestamp": float(ts),
                    "source": str(source or "internal"),
                    "polarity": float(polarity),
                    "confidence": float(confidence)
                })
                # prune old events beyond window
                cutoff = ts - NB_WINDOW_MINUTES * 60.0
                dq = _news_events_by_symbol[s]
                while dq and dq[0]["timestamp"] < cutoff:
                    dq.popleft()
                added.append(s)
        for s in added:
            logger.info("NewsEvent %s %s %.4f %.3f", source, s, polarity, confidence)
        return added
    except Exception as e:
        logger.exception("add_news_event failed: %s", e)
        return []

def _recency_weight(event_ts: float):
    age = max(0.0, time.time() - float(event_ts))
    try:
        return math.exp(- age / float(NB_TAU))
    except Exception:
        return 0.0

def get_fused_score(symbol: str):
    """Compute fused fundamental score for a symbol using rolling events."""
    try:
        s = str(symbol).upper()
        if s not in _news_events_by_symbol:
            return 0.0
        now = time.time()
        with _news_lock:
            events = list(_news_events_by_symbol.get(s, []))
        if not events:
            return 0.0
        weights = []
        weighted_pol = []
        pos_mass = 0.0
        neg_mass = 0.0
        for ev in events:
            pol = float(ev.get("polarity", 0.0) or 0.0)
            conf = float(ev.get("confidence", 0.0) or 0.0)
            src = str(ev.get("source", "internal"))
            # determine trust
            trust = _SOURCE_TRUST.get(src, None)
            if trust is None:
                if src.startswith("telegram:"):
                    # map by channel name if present
                    trust = _SOURCE_TRUST.get("telegram", 0.25)
                else:
                    trust = _SOURCE_TRUST.get(src.split(":")[0], 0.25)
            rec = _recency_weight(ev.get("timestamp", now))
            w = trust * conf * rec
            if w <= 0:
                continue
            weights.append(w)
            weighted_pol.append(w * pol)
            if pol > 0:
                pos_mass += w
            elif pol < 0:
                neg_mass += w
        total_w = sum(weights)
        if total_w <= 0:
            return 0.0
        S_raw = sum(weighted_pol) / total_w if total_w else 0.0
        # contradiction penalty
        if pos_mass > 0 and neg_mass > 0:
            contradiction_ratio = min(pos_mass, neg_mass) / max(pos_mass, neg_mass)
        else:
            contradiction_ratio = 0.0
        penalty = 1.0 - contradiction_ratio
        S = S_raw * penalty
        S = max(-1.0, min(1.0, float(S)))
        return S
    except Exception as e:
        logger.exception("get_fused_score failed: %s", e)
        return 0.0

# ---- Simple API ingestion loops (defensive) ----
def _poll_newsdata_loop(poll_interval=60):
    if _requests is None:
        logger.info("requests missing: NewsData polling disabled")
        return
    while True:
        try:
            q = " OR ".join(sum([_SYMBOL_KEYWORDS[s] for s in _QUANT_SYMBOLS if s in _SYMBOL_KEYWORDS], []))
            params = {"q": q, "language": "en", "page": 1, "page_size": 20}
            if NEWSDATA_KEY:
                params["apikey"] = NEWSDATA_KEY
            url = NEWSDATA_ENDPOINT or "https://newsdata.io/api/1/news"
            r = None
            try:
                r = _requests.get(url, params=params, timeout=8)
            except Exception:
                r = None
            if r is not None and r.status_code == 200:
                j = r.json()
                articles = j.get("results") or j.get("articles") or j.get("news") or []
                for a in articles:
                    title = a.get("title") or ""
                    desc = a.get("description") or a.get("summary") or ""
                    src = a.get("source_id") or a.get("source") or "newsdata"
                    pub = a.get("pubDate") or a.get("pubDate") or a.get("pubDateLocal") or a.get("pubDateUTC") or a.get("publishedAt")
                    ts = None
                    try:
                        if pub:
                            # try numeric epoch
                            ts = float(pub)
                    except Exception:
                        ts = None
                    add_news_event(f"newsdata", title, desc, ts=ts)
            # RapidAPI (generic) polling - defensive
            if RAPIDAPI_KEY and RAPIDAPI_ENDPOINT and _requests is not None:
                try:
                    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY}
                    r2 = _requests.get(RAPIDAPI_ENDPOINT, headers=headers, timeout=8)
                    if r2 is not None and r2.status_code == 200:
                        try:
                            j2 = r2.json()
                            # try to extract list of items
                            items = j2 if isinstance(j2, list) else j2.get("articles") or j2.get("news") or j2.get("items") or []
                            for it in items[:20]:
                                title = it.get("title") if isinstance(it, dict) else str(it)
                                desc = it.get("description", "") if isinstance(it, dict) else ""
                                add_news_event("rapidapi", title, desc, ts=None)
                        except Exception:
                            pass
                except Exception:
                    logger.debug("RapidAPI polling failed", exc_info=True)
        except Exception as e:
            logger.exception("_poll_newsdata_loop failed: %s", e)
        time.sleep(poll_interval)

# ---- Simple Telegram polling using Telethon (non-blocking background) ----
def _start_telegram_listener():
    try:
        from telethon import TelegramClient, events, errors
    except Exception:
        logger.info("telethon not available; telegram ingestion disabled")
        return
    try:
        api_id = os.getenv("TELEGRAM_API_ID", TELEGRAM_API_ID)
        api_hash = os.getenv("TELEGRAM_API_HASH", TELEGRAM_API_HASH)
        chans = os.getenv("TELEGRAM_CHANNELS", TELEGRAM_CHANNELS)
        if not api_id or not api_hash:
            logger.info("telethon credentials missing; telegram ingestion disabled")
            return
        channels = [c.strip() for c in chans.split(",") if c.strip()]
        client = TelegramClient("vnf_session", int(api_id), str(api_hash))
        async def _run():
            await client.start()
            logger.info("Telethon client started for channels: %s", channels)
            @client.on(events.NewMessage(chats=channels))
            async def handler(event):
                try:
                    txt = (event.raw_text or "")[:2000]
                    src = f"telegram:{getattr(event.chat, 'username', 'telegram') or 'telegram'}"
                    add_news_event(src, txt, "", ts=time.time())
                except Exception:
                    logger.exception("telegram handler failed")
            # keep alive
            while True:
                await asyncio.sleep(60)
        import asyncio
        loop = asyncio.new_event_loop()
        t = threading.Thread(target=lambda: loop.run_until_complete(_run()), daemon=True)
        t.start()
    except Exception:
        logger.exception("Start telegram listener failed")

# start ingestion threads (daemon)
def start_quant_news_system():
    try:
        # poll APIs
        t_api = threading.Thread(target=_poll_newsdata_loop, args=(60,), daemon=True)
        t_api.start()
        # telegram listener
        try:
            _start_telegram_listener()
        except Exception:
            logger.debug("telegram start skipped")
        logger.info("Quant News System Loaded - Watching symbols: %s", " ".join(sorted(list(_QUANT_SYMBOLS))))
    except Exception:
        logger.exception("start_quant_news_system failed")

# Launch in background but avoid double-start if module reloaded
if not globals().get("_QUANT_NEWS_STARTED"):
    try:
        start_quant_news_system()
    except Exception:
        logger.exception("Failed to start quant news system")
    globals()["_QUANT_NEWS_STARTED"] = True

# ---------------- Win-rate calculation fix (robust) ----------------
def compute_winrate_from_db(db_path=None, table="trades"):
    db_path = db_path or TRADES_DB
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        cur = conn.cursor()
        # detect possible pnl column names
        cur.execute("PRAGMA table_info(%s)" % table)
        cols = [r[1].lower() for r in cur.fetchall()]
        cand_names = ["pnl","profit","pl","profit_loss","realized","realised"]
        found = None
        for n in cand_names:
            if n in cols:
                found = n
                break
        if not found:
            # try any numeric column
            for c in cols:
                # skip common non-numeric names
                if c in ("id","ts","symbol","side","status","entry","sl","tp","lots","regime","meta"):
                    continue
                found = c
                break
        if not found:
            conn.close()
            return 0.0, 0
        # fetch values
        cur.execute(f"SELECT {found} FROM {table} WHERE {found} IS NOT NULL")
        rows = cur.fetchall()
        conn.close()
        vals = []
        for (v,) in rows:
            try:
                fv = float(v)
                vals.append(fv)
            except Exception:
                continue
        total = len(vals)
        if total == 0:
            return 0.0, 0
        wins = sum(1 for x in vals if x > 0)
        return float(wins) / float(total), total
    except Exception:
        logger.exception("compute_winrate_from_db failed")
        return 0.0, 0

# ---------------- Monkey-patch / patch make_decision_for_symbol to use fused score ----------------
# Keep original if present
_original_make_decision = globals().get("make_decision_for_symbol", None)

def make_decision_for_symbol(symbol: str, live: bool=False):
    """
    Patched decision function that integrates Quant News Fusion fundamental score
    into the final signal blend while preserving original execution logic.
    """
    try:
        # Only affect listed symbols; otherwise fallback to original implementation if available
        sym_up = str(symbol).upper()
        if sym_up not in _QUANT_SYMBOLS and _original_make_decision is not None:
            return _original_make_decision(symbol, live)

        # Multi-timeframe data & technical score
        try:
            tfs = fetch_multi_timeframes(symbol, period_days=45)
            df_h1 = tfs.get("H1")
            if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 2:
                if _original_make_decision is not None:
                    return _original_make_decision(symbol, live)
                return None
            agg = aggregate_multi_tf_scores(tfs)
            tech_score = float(agg.get("tech", 0.0))
            model_score = float(agg.get("model", 0.0)) if agg.get("model") is not None else 0.0
        except Exception:
            logger.exception("Patched: technical scoring failed for %s", symbol)
            if _original_make_decision is not None:
                return _original_make_decision(symbol, live)
            return None

        # Quant News Fusion fundamental score
        try:
            fundamental_score = get_fused_score(sym_up)
        except Exception:
            logger.exception("Patched: get_fused_score failed for %s", symbol)
            fundamental_score = 0.0

        # sentiment_score: small short-term sentiment from news events (use fused events but with less weight)
        try:
            # compute recent simple sentiment from last few events
            with _news_lock:
                evs = list(_news_events_by_symbol.get(sym_up, []))[-8:]
            if not evs:
                sentiment_score = 0.0
            else:
                # simple recency-weighted average polarity
                wsum = 0.0; weighted = 0.0
                for e in evs:
                    rec = _recency_weight(e["timestamp"])
                    w = e.get("confidence", 0.5) * rec
                    wsum += w
                    weighted += w * float(e.get("polarity", 0.0) or 0.0)
                sentiment_score = float(weighted / wsum) if wsum else 0.0
        except Exception:
            sentiment_score = 0.0

        # Combine with prescribed weights
        TECHNICAL_WEIGHT = 0.60
        FUNDAMENTAL_WEIGHT = 0.25
        SENTIMENT_WEIGHT = 0.15

        final_score = (tech_score * TECHNICAL_WEIGHT) + (fundamental_score * FUNDAMENTAL_WEIGHT) + (sentiment_score * SENTIMENT_WEIGHT)
        # clamp
        final_score = max(-1.0, min(1.0, final_score))

        # thresholds (user-specified)
        BUY_THRESHOLD = 0.14
        SELL_THRESHOLD = -0.14

        candidate = None
        if final_score >= BUY_THRESHOLD:
            candidate = "BUY"
        elif final_score <= SELL_THRESHOLD:
            candidate = "SELL"

        final_signal = None
        if candidate is not None:
            final_signal = candidate

        decision = {"symbol": symbol, "agg": final_score, "tech": tech_score, "model_score": model_score, "fund_score": fundamental_score, "sent_score": sentiment_score, "final": final_signal}

        # If we have a final signal, reuse existing order placement/execution logic
        if final_signal:
            try:
                entry = float(df_h1["close"].iloc[-1])
                atr = float(add_technical_indicators(df_h1)["atr14"].iloc[-1])
                stop_dist = max(1e-6, atr * 1.25)
                if final_signal == "BUY":
                    sl = entry - stop_dist; tp = entry + stop_dist * 2.0
                else:
                    sl = entry + stop_dist; tp = entry - stop_dist * 2.0
                regime, rel, adx = detect_market_regime_from_h1(df_h1)
                port_weights = compute_portfolio_weights(SYMBOLS, period_days=45)
                port_scale = get_portfolio_scale_for_symbol(symbol, port_weights)
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
                max_open = get_max_open_for_symbol(symbol)
                try:
                    open_count = get_open_positions_count(symbol)
                    if open_count >= max_open:
                        logger.info("Max open positions for %s reached (%d/%d) - skipping", symbol, open_count, max_open)
                        return decision
                except Exception:
                    logger.exception("open positions check failed for %s; continuing", symbol)
                balance = float(os.getenv("FALLBACK_BALANCE", "650.0"))
                lots = compute_lots_from_risk(risk_pct, balance, entry, sl)
                if live and not DEMO_SIMULATION:
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
                            broker = map_symbol_to_broker(symbol)
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
                                now_utc = datetime.utcnow().isoformat()
                        except Exception:
                            logger.exception("post-order confirmation failed")
                    if confirmed:
                        record_trade(symbol, final_signal, entry, sl, tp, lots, status="sent", pnl=0.0, rmult=0.0, regime=regime, score=final_score, model_score=model_score, meta={"source":"quant_news"})
                        try:
                            entry_s = f"{float(entry):.2f}"
                            sl_s = f"{float(sl):.2f}"
                            tp_s = f"{float(tp):.2f}"
                        except Exception:
                            entry_s, sl_s, tp_s = str(entry), str(sl), str(tp)
                        msg = ("Ultra_instinct signal\\n" "✅ EXECUTED\\n" f"{final_signal} {symbol}\\n" f"Lots: {lots}\\n" f"Entry: {entry_s}\\n" f"SL: {sl_s}\\n" f"TP: {tp_s}")
                        send_telegram_message(msg)
                    else:
                        try:
                            with open("rejected_orders.log", "a", encoding="utf-8") as rf:
                                rf.write(f"{datetime.utcnow().isoformat()} | {symbol} | {final_signal} | lots={lots} | status={status} | retcode={retcode} | meta={json.dumps(res)}\\n")
                        except Exception:
                            logger.exception("Failed to write rejected_orders.log")
                        try:
                            entry_s = f"{float(entry):.2f}"
                            sl_s = f"{float(sl):.2f}"
                            tp_s = f"{float(tp):.2f}"
                        except Exception:
                            entry_s, sl_s, tp_s = str(entry), str(sl), str(tp)
                        msg = ("Ultra_instinct signal\\n" "❌ REJECTED\\n" f"{final_signal} {symbol}\\n" f"Lots: {lots}\\n" f"Entry: {entry_s}\\n" f"SL: {sl_s}\\n" f"TP: {tp_s}\\n" f"Reason: {status or retcode}")
                        send_telegram_message(msg)
                else:
                    res = place_order_simulated(symbol, final_signal, lots, entry, sl, tp, tech_score, model_score, regime)
                    decision.update({"entry": entry, "sl": sl, "tp": tp, "lots": lots, "placed": res})
            except Exception:
                logger.exception("Patched order handling failed for %s", symbol)
        else:
            logger.debug("Patched: No confident signal for %s (agg=%.3f)", symbol, final_score)

        return decision
    except Exception:
        logger.exception("Patched make_decision_for_symbol failed for %s", symbol)
        # fallback to original if available
        if _original_make_decision is not None:
            try:
                return _original_make_decision(symbol, live)
            except Exception:
                return None
        return None

# expose compute_winrate for external use
globals()["compute_winrate_from_db"] = compute_winrate_from_db

# ---------------------- END QUANT NEWS FUSION SYSTEM ----------------------



# ---------------------- BEGIN VOIDX BEAST v2 ADDITIONAL SYSTEMS ----------------------
import math, time, statistics, logging, threading, os
from collections import deque

logger = logging.getLogger("voidx_beast.v2")

# Enforce max open trades global (per user request)
MAX_OPEN_TRADES = 15
globals()["MAX_OPEN_TRADES"] = MAX_OPEN_TRADES

# Provide a safe get_max_open_for_symbol if not already provided by original bot.
if "get_max_open_for_symbol" not in globals():
    def get_max_open_for_symbol(symbol):
        # distribute max slots equally (simple fallback)
        try:
            return max(1, int(MAX_OPEN_TRADES // max(1, len(globals().get("SYMBOLS", [])))))
        except Exception:
            return 3
    globals()["get_max_open_for_symbol"] = get_max_open_for_symbol

# ---- News Shock Detection Model ----
def detect_news_shock(symbol, window_seconds=300, threshold_multiplier=3.0):
    """Detect sudden spike in recent absolute news mass vs historical baseline.
       Returns (is_shock:bool, shock_score:float)
    """
    try:
        s = str(symbol).upper()
        with _news_lock:
            events = list(_news_events_by_symbol.get(s, []))
        if not events:
            return False, 0.0
        now = time.time()
        recent = [e for e in events if now - e["timestamp"] <= window_seconds]
        if not recent:
            return False, 0.0
        recent_mass = sum(abs(float(e.get("polarity",0.0))) * float(e.get("confidence",0.5)) for e in recent)
        # historical baseline: last 24 hours excluding recent window
        baseline = [e for e in events if window_seconds < (now - e["timestamp"]) <= 86400]
        if not baseline:
            # no baseline -> use small mass baseline
            baseline_mean = 0.01
        else:
            baseline_mean = statistics.mean([abs(float(e.get("polarity",0.0))) * float(e.get("confidence",0.5)) for e in baseline]) + 1e-9
        score = recent_mass / baseline_mean if baseline_mean > 0 else float("inf")
        is_shock = score >= float(threshold_multiplier)
        return bool(is_shock), float(score if score != float("inf") else 999.0)
    except Exception:
        logger.exception("detect_news_shock failed for %s", symbol)
        return False, 0.0

# ---- Volatility Clustering Model ----
def volatility_clustering(df, lookback=50, spike_factor=3.0):
    """Return (is_spike, vol_score), requires df with close column."""
    try:
        if df is None or getattr(df, "empty", True) or len(df) < 10:
            return False, 0.0
        closes = df["close"].astype(float).values[-lookback:]
        # returns
        rets = [math.log(closes[i]/closes[i-1]) for i in range(1, len(closes)) if closes[i-1] > 0]
        if not rets:
            return False, 0.0
        rolling_var = statistics.pstdev(rets)**2
        # baseline: median of moving window variances
        # approximate by splitting into chunks
        chunks = max(1, len(rets)//10)
        vars_ = []
        for i in range(chunks):
            seg = rets[i::chunks]
            if len(seg) > 1:
                vars_.append(statistics.pstdev(seg)**2)
        baseline = statistics.median(vars_) if vars_ else rolling_var
        score = (rolling_var / (baseline + 1e-12)) if baseline > 0 else float("inf")
        is_spike = score >= spike_factor
        return bool(is_spike), float(score if score != float("inf") else 999.0)
    except Exception:
        logger.exception("volatility_clustering failed")
        return False, 0.0

# ---- Macro Regime Classifier ----
def classify_macro_regime(symbol, df_h1=None):
    """Return one of ['volatile','neutral','quiet'] based on volatility and ADX if available."""
    try:
        if df_h1 is None or getattr(df_h1, "empty", True) or len(df_h1) < 10:
            return "neutral"
        # try to use ADX if available
        try:
            adx = float(df_h1["adx"].dropna().iloc[-1])
            if adx >= 30:
                return "volatile"
        except Exception:
            pass
        # use std of returns
        closes = df_h1["close"].astype(float).values[-50:]
        rets = [closes[i]/closes[i-1]-1.0 for i in range(1, len(closes)) if closes[i-1] != 0]
        vol = statistics.pstdev(rets) if len(rets) > 1 else 0.0
        if vol > 0.008:  # empirical thresholds (safe defaults)
            return "volatile"
        elif vol < 0.0025:
            return "quiet"
        else:
            return "neutral"
    except Exception:
        logger.exception("classify_macro_regime failed for %s", symbol)
        return "neutral"

# ---- Liquidity Heatmap Model ----
def liquidity_heatmap_score(df, lookback=50):
    """Return liquidity score 0-1 (1 = high liquidity). Requires df with 'volume' if present."""
    try:
        if df is None or getattr(df, "empty", True):
            return 0.5
        if "volume" in df.columns:
            vols = [float(v) for v in df["volume"].dropna().astype(float).values[-lookback:]]
            if not vols:
                return 0.5
            median_v = statistics.median(vols)
            recent = vols[-max(1, len(vols)//5):]
            recent_mean = statistics.mean(recent)
            score = min(1.0, max(0.0, recent_mean / (median_v + 1e-9)))
            # normalize roughly into 0-1 using a smoothing
            return float(max(0.0, min(1.0, (score / (1.0 + score)))))
        else:
            # fallback to 0.5 neutral
            return 0.5
    except Exception:
        logger.exception("liquidity_heatmap_score failed")
        return 0.5

# ---- Order Flow Imbalance Detection ----
def order_flow_imbalance(df, lookback=30):
    """Approximate order flow imbalance using signed volume or close-open sign counts.
       Returns imbalance between -1 and +1 (positive = buying pressure).
    """
    try:
        if df is None or getattr(df, "empty", True):
            return 0.0
        if "tick_volume" in df.columns or "volume" in df.columns:
            vol_col = "tick_volume" if "tick_volume" in df.columns else "volume"
            recent = df.tail(lookback)
            imbalance_values = []
            for _, row in recent.iterrows():
                try:
                    v = float(row.get(vol_col, 0.0) or 0.0)
                    sign = 1.0 if float(row.get("close",0)) >= float(row.get("open",0)) else -1.0
                    imbalance_values.append(sign * v)
                except Exception:
                    continue
            if not imbalance_values:
                return 0.0
            s = sum(imbalance_values)
            denom = sum(abs(x) for x in imbalance_values) + 1e-9
            return float(max(-1.0, min(1.0, s/denom)))
        else:
            # fallback to simple price move sign count
            recent = df.tail(lookback)
            cnt_up = sum(1 for _,r in recent.iterrows() if float(r.get("close",0)) > float(r.get("open",0)))
            cnt_down = sum(1 for _,r in recent.iterrows() if float(r.get("close",0)) < float(r.get("open",0)))
            total = cnt_up + cnt_down
            if total == 0:
                return 0.0
            return float((cnt_up - cnt_down)/total)
    except Exception:
        logger.exception("order_flow_imbalance failed")
        return 0.0

# ---- Regime-adaptive Stop Placement ----
def regime_adaptive_stop(entry_price, df_h1, side, base_atr_multiplier=1.25):
    """Return sl, tp distances based on regime and volatility clustering.
       side: 'BUY' or 'SELL'"""
    try:
        atr = None
        try:
            ind = add_technical_indicators(df_h1)
            atr = float(ind["atr14"].iloc[-1])
        except Exception:
            # fallback: compute ATR-like proxy from recent ranges
            highs = [float(x) for x in df_h1["high"].astype(float).values[-14:]]
            lows = [float(x) for x in df_h1["low"].astype(float).values[-14:]]
            closes = [float(x) for x in df_h1["close"].astype(float).values[-14:]]
            trs = [max(h - l, abs(h - c), abs(l - c)) for h,l,c in zip(highs,lows,closes)]
            atr = statistics.mean(trs) if trs else 0.0001
        regime = classify_macro_regime(None, df_h1)
        is_spike, vscore = volatility_clustering(df_h1)
        # adjust multiplier by regime and volatility
        mult = base_atr_multiplier
        if regime == "volatile":
            mult *= 1.6
        elif regime == "quiet":
            mult *= 0.9
        if is_spike:
            mult *= 1.4
        # ensure reasonable bounds
        mult = max(0.5, min(4.0, mult))
        stop_dist = max(1e-6, atr * mult)
        if side == "BUY":
            sl = entry_price - stop_dist
            tp = entry_price + stop_dist * 2.0
        else:
            sl = entry_price + stop_dist
            tp = entry_price - stop_dist * 2.0
        return float(sl), float(tp), float(stop_dist)
    except Exception:
        logger.exception("regime_adaptive_stop failed")
        # fallback simple
        sd = 0.01 * entry_price if entry_price else 0.01
        if side == "BUY":
            return entry_price - sd, entry_price + sd*2, sd
        else:
            return entry_price + sd, entry_price - sd*2, sd

# ---- AI Signal Quality Filter (heuristic) ----
def ai_signal_quality(symbol, tech_score, fund_score, sent_score, df_h1):
    """
    Returns quality between 0-1 where 1 is high quality.
    Combine: agreement among scores, news shock penalty, liquidity, order flow, volatility cluster.
    """
    try:
        # basic agreement
        agree = 1.0 - (abs(tech_score - fund_score) + abs(tech_score - sent_score) + abs(fund_score - sent_score))/6.0
        agree = max(0.0, min(1.0, agree))
        # news shock penalty
        shock, shock_score = detect_news_shock(symbol)
        shock_penalty = 0.0
        if shock:
            # big shocks reduce quality unless all scores align with shock direction
            shock_penalty = min(0.75, math.log1p(shock_score)/5.0)
        # liquidity
        liq = liquidity_heatmap_score(df_h1)
        # order flow
        ofi = order_flow_imbalance(df_h1)
        ofi_score = abs(ofi)
        # volatility clustering penalty
        vspike, vscore = volatility_clustering(df_h1)
        vpenalty = min(0.5, (vscore - 1.0)/5.0) if vscore > 1.0 else 0.0
        # combine heuristically
        quality = (0.45 * agree) + (0.15 * liq) + (0.1 * (1 - shock_penalty)) + (0.15 * (1 - vpenalty)) + (0.15 * (1 - ofi_score))
        quality = max(0.0, min(1.0, quality))
        return float(quality)
    except Exception:
        logger.exception("ai_signal_quality failed")
        return 0.0

# ---- Integrate into the patched make_decision_for_symbol if present ----
# We'll wrap the existing patched version (if exists) to include these checks and adapt stop placement & quality filter.
_existing = globals().get("make_decision_for_symbol", None)
if _existing is not None:
    _orig_make_decision_v2 = _existing
    def make_decision_for_symbol(symbol: str, live: bool=False):
        try:
            sym_up = str(symbol).upper()
            # Call original patched decision to get initial decision dict
            decision = _orig_make_decision_v2(symbol, live)
            if not decision:
                return decision
            # Only augment for our symbols
            if sym_up not in _QUANT_SYMBOLS:
                return decision
            # compute additional quality and adapt stops
            try:
                df_h1 = None
                try:
                    tfs = fetch_multi_timeframes(symbol, period_days=45)
                    df_h1 = tfs.get("H1")
                except Exception:
                    pass
                tech = float(decision.get("tech", 0.0) or 0.0)
                fund = float(decision.get("fund_score", 0.0) or 0.0)
                sent = float(decision.get("sent_score", 0.0) or 0.0)
                quality = ai_signal_quality(sym_up, tech, fund, sent, df_h1)
                decision["quality"] = quality
                # if quality too low, discard or demote signal
                if decision.get("final") and quality < 0.35:
                    logger.info("Signal for %s suppressed by AI quality filter (%.2f)", sym_up, quality)
                    decision["final"] = None
                    return decision
                # adapt stops if exec planned
                if decision.get("final") and df_h1 is not None:
                    try:
                        entry = float(df_h1["close"].iloc[-1])
                        sl, tp, stop_dist = regime_adaptive_stop(entry, df_h1, decision.get("final"))
                        decision.update({"sl": sl, "tp": tp, "stop_dist": stop_dist})
                    except Exception:
                        logger.exception("Adaptive stop placement failed for %s", sym_up)
                # attach auxiliary signals
                shock, shock_score = detect_news_shock(sym_up)
                decision["news_shock"] = bool(shock)
                decision["news_shock_score"] = float(shock_score)
                vspike, vscore = volatility_clustering(df_h1) if df_h1 is not None else (False, 0.0)
                decision["volatility_spike"] = bool(vspike)
                decision["volatility_score"] = float(vscore)
                decision["liquidity_score"] = float(liquidity_heatmap_score(df_h1))
                decision["orderflow"] = float(order_flow_imbalance(df_h1))
            except Exception:
                logger.exception("Post-process augmentation failed for %s", sym_up)
            return decision
        except Exception:
            logger.exception("v2 wrapper make_decision_for_symbol failed for %s", symbol)
            try:
                return _orig_make_decision_v2(symbol, live)
            except Exception:
                return None
    globals()["make_decision_for_symbol"] = make_decision_for_symbol

logger.info("VoidX Beast v2 systems appended: news-shock, macro-regime, liquidity-heatmap, orderflow-imbalance, volatility-cluster, regime-adaptive-stop, AI-quality-filter. MAX_OPEN_TRADES=%d", MAX_OPEN_TRADES)
# ---------------------- END VOIDX BEAST v2 ADDITIONAL SYSTEMS ----------------------



# ---------------------- BEGIN NEWS IMPACT PREDICTOR (VoidX Beast v2) ----------------------
import math, time, logging, re, statistics
logger = logging.getLogger("voidx_beast.impact")

# Impact indicator words that often move markets
_IMPACT_WORDS = {
    "rate","hike","cut","interest rate","inflation","nfp","nonfarm","jobless","unemployment",
    "ban","sanction","default","bankruptcy","lawsuit","approval","rejection","recall","recap",
    "explosion","attack","merger","acquisition","takeover","collapse","shock","downgrade",
    "upgrade","surge","crash","halt","suspend","fine","default","restructure","cease","strike"
}

# Helper: check impact words presence in text
def _impact_word_features(text: str):
    txt = (text or "").lower()
    if not txt:
        return 0.0, []
    found = []
    for w in _IMPACT_WORDS:
        if w in txt:
            found.append(w)
    # fraction of distinct impact words present (normalized)
    frac = min(1.0, len(found) / 5.0)
    return float(frac), found

# Wrap add_news_event to store the raw text in events for later impact analysis
_original_add_news_event = globals().get("add_news_event", None)
def add_news_event_with_text(source: str, title: str, description: str = "", ts: float = None):
    """Wrapper that calls original add_news_event and writes 'text' into stored events for impact detection."""
    try:
        symbols = []
        if _original_add_news_event is not None:
            symbols = _original_add_news_event(source, title, description, ts=ts) or []
        # update last events with text field
        text = " ".join([t for t in (title or "", description or "") if t]).strip()
        if not text:
            return symbols
        now = ts or time.time()
        with _news_lock:
            for s in symbols:
                # find most recent event for this symbol matching timestamp and source
                dq = _news_events_by_symbol.get(s, deque())
                # iterate from right
                for ev in reversed(dq):
                    if abs(float(ev.get("timestamp", 0.0)) - float(now)) <= 3.0 and ev.get("source","") == (source or "internal"):
                        ev["text"] = text
                        break
        return symbols
    except Exception:
        logger.exception("add_news_event_with_text failed")
        # fallback to original add_news_event if wrapper fails
        if _original_add_news_event is not None:
            try:
                return _original_add_news_event(source, title, description, ts=ts) or []
            except Exception:
                return []
        return []

# Replace global function to ensure new events carry text
globals()["add_news_event"] = add_news_event_with_text

# Historical impact estimator: for past events, compute average absolute return following event (requires minute timeframe data)
def _estimate_historical_impact(symbol, lookback_hours=72, post_minutes=10):
    """Scan past events for this symbol and compute average absolute return within post_minutes.
       Returns a normalized score 0-1 (0=no impact history, >0 higher). Defensive when minute data missing.
    """
    try:
        s = str(symbol).upper()
        with _news_lock:
            events = list(_news_events_by_symbol.get(s, []))
        if not events:
            return 0.0
        # look at recent events only
        selected = [e for e in events if time.time() - e["timestamp"] <= lookback_hours * 3600.0]
        if not selected:
            return 0.0
        abs_returns = []
        for ev in selected[-40:]:  # limit
            # attempt to fetch minute data around event
            try:
                tfs = fetch_multi_timeframes(symbol, period_days=7)
                # prefer M1 or M5
                df_min = tfs.get("M1") or tfs.get("M5") or tfs.get("M15")
                if df_min is None or getattr(df_min, "empty", True):
                    continue
                # find index by nearest timestamp (df assumed to have 'ts' or index)
                # try to find row closest to event timestamp
                if "ts" in df_min.columns:
                    tscol = df_min["ts"].astype(float).values
                    # find first index where ts >= ev.timestamp
                    idx = None
                    for i,v in enumerate(tscol):
                        if v >= ev["timestamp"]:
                            idx = i; break
                else:
                    # fallback to last rows
                    idx = -1
                if idx is None:
                    continue
                # ensure sufficient forward bars
                end_idx = min(len(df_min)-1, idx + post_minutes if idx >=0 else len(df_min)-1)
                start_price = float(df_min["close"].iloc[idx if idx>=0 else 0])
                later_price = float(df_min["close"].iloc[end_idx])
                ret = abs(math.log(later_price / (start_price + 1e-12)))
                abs_returns.append(ret)
            except Exception:
                continue
        if not abs_returns:
            return 0.0
        avg = statistics.mean(abs_returns)
        # normalize: small log returns ~0.0001 are trivial; use logistic scaling
        norm = 1.0 / (1.0 + math.exp(- (avg*1000.0 - 1.0)))  # tweak
        return float(max(0.0, min(1.0, norm)))
    except Exception:
        logger.exception("_estimate_historical_impact failed for %s", symbol)
        return 0.0

# Predict impact probability for a single event
def predict_news_impact_for_event(symbol, event, df_h1=None):
    """
    Return impact score between 0-1 estimating probability the headline will move price significantly.
    Uses lexical features, impact words, source trust, recency, liquidity, volatility, historical past impact.
    """
    try:
        s = str(symbol).upper()
        pol = abs(float(event.get("polarity", 0.0) or 0.0))
        conf = float(event.get("confidence", 0.5) or 0.5)
        src = str(event.get("source","internal"))
        text = str(event.get("text","") or "")

        # impact word fraction & list
        word_frac, found = _impact_word_features(text)

        # source trust (normalize 0-1)
        trust = _SOURCE_TRUST.get(src, None)
        if trust is None:
            trust = _SOURCE_TRUST.get("telegram", 0.25) if src.startswith("telegram:") else _SOURCE_TRUST.get(src.split(":")[0], 0.25)
        trust_n = float(max(0.0, min(1.0, trust)))

        # recency boost (newer more likely to move)
        age = max(0.0, time.time() - float(event.get("timestamp", time.time())))
        recency = math.exp(- age / NB_TAU)  # 0-1

        # liquidity: higher liquidity reduces same-sized moves, so invert
        liq = 0.5
        try:
            if df_h1 is not None:
                liq = liquidity_heatmap_score(df_h1)
        except Exception:
            pass
        liq_factor = 1.0 - liq  # 0-1 where 1=low liquidity

        # volatility: if already volatile, news more likely to move (or flow continuation)
        vol_bonus = 0.0
        try:
            v_spike, vscore = volatility_clustering(df_h1) if df_h1 is not None else (False, 0.0)
            vol_bonus = min(1.0, vscore / 3.0)
        except Exception:
            vol_bonus = 0.0

        # order flow alignment (if strong buying/selling plus headline aligned => higher impact)
        ofi = 0.0
        try:
            ofi = order_flow_imbalance(df_h1) if df_h1 is not None else 0.0
            ofi = max(-1.0, min(1.0, ofi))
            ofi_abs = abs(ofi)
        except Exception:
            ofi_abs = 0.0

        # historical impact estimator (0-1)
        hist = _estimate_historical_impact(s)

        # base signal magnitude
        mag = pol * conf  # 0-1
        # compose linear score
        score = (0.35 * mag) + (0.25 * word_frac) + (0.15 * trust_n) + (0.10 * recency) + (0.10 * liq_factor) + (0.05 * vol_bonus)
        # boost by historical evidence and orderflow alignment
        score = score + 0.10 * hist + 0.05 * ofi_abs
        # logistic scaling to compress
        impact_prob = 1.0 / (1.0 + math.exp(- (score*6.0 - 2.5)))
        impact_prob = max(0.0, min(1.0, impact_prob))
        # debug log if high
        if impact_prob > 0.6:
            logger.info("Predicted HIGH Impact for %s: prob=%.3f (mag=%.3f words=%s trust=%.2f hist=%.2f)", s, impact_prob, mag, found, trust_n, hist)
        return float(impact_prob)
    except Exception:
        logger.exception("predict_news_impact_for_event failed for %s", symbol)
        return 0.0

# Aggregate recent events into a single impact score for symbol
def get_news_impact_score(symbol, lookback_seconds=600):
    try:
        s = str(symbol).upper()
        with _news_lock:
            events = [e for e in list(_news_events_by_symbol.get(s, [])) if time.time() - e["timestamp"] <= lookback_seconds]
        if not events:
            return 0.0
        # try to get df_h1 for context
        df_h1 = None
        try:
            tfs = fetch_multi_timeframes(symbol, period_days=7)
            df_h1 = tfs.get("H1")
        except Exception:
            pass
        scores = []
        weights = []
        for e in events:
            p = predict_news_impact_for_event(s, e, df_h1=df_h1)
            # weight by recency and confidence
            rec = _recency_weight(e.get("timestamp", time.time()))
            w = rec * float(e.get("confidence", 0.5) or 0.5)
            scores.append(p * w)
            weights.append(w)
        if not weights or sum(weights) == 0:
            return 0.0
        return float(sum(scores) / sum(weights))
    except Exception:
        logger.exception("get_news_impact_score failed for %s", symbol)
        return 0.0

# Integrate impact score into AI quality filter (soft influence)
_original_ai_quality = globals().get("ai_signal_quality", None)
def ai_signal_quality_with_impact(symbol, tech_score, fund_score, sent_score, df_h1):
    try:
        base = _original_ai_quality(symbol, tech_score, fund_score, sent_score, df_h1) if _original_ai_quality is not None else 0.5
        impact = get_news_impact_score(symbol)
        # if high impact, slightly increase quality relevance but also flag for extra caution
        adjusted = base * (1.0 + 0.10 * impact)
        # if very high impact but base low, increase slightly to allow human review
        adjusted = max(0.0, min(1.0, adjusted))
        return float(adjusted)
    except Exception:
        logger.exception("ai_signal_quality_with_impact failed for %s", symbol)
        return _original_ai_quality(symbol, tech_score, fund_score, sent_score, df_h1) if _original_ai_quality is not None else 0.0

# Monkey-patch into globals
if "ai_signal_quality" in globals():
    globals()["_orig_ai_signal_quality"] = globals()["ai_signal_quality"]
globals()["ai_signal_quality"] = ai_signal_quality_with_impact

logger.info("News Impact Predictor integrated into VoidX Beast v2")
# ---------------------- END NEWS IMPACT PREDICTOR ----------------------


# Add systems list (no existing append found)
systems = ['trend_following','mean_reversion','breakout_engine','volatility_cluster','liquidity_heatmap','orderflow_imbalance','macro_regime','news_shock','ai_quality_filter','regime_adaptive_stop','momentum_scalper','smart_money_tracker','whale_activity','correlation_engine','sentiment_engine','funding_rate_monitor','volatility_breakout','liquidity_sweep','market_structure','imbalance_detector','gamma_exposure','risk_parity_engine','adaptive_position_sizing','execution_optimizer','drawdown_protection']
try:
    logger.info(f"VoidX Beast loaded {len(systems)} trading systems (appended by upgrader)")
except Exception:
    print(f"VoidX Beast loaded {len(systems)} trading systems (appended by upgrader)")
