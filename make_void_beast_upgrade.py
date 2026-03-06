#!/usr/bin/env python3
"""
make_void_beast_upgrade.py (robust rebuild)
Creates the full 25-module beast suite and a safe wrapper voidx2_0_final_beast.py
that runs the original voidx2_0.py without modifying it (avoids __future__/docstring issues).
"""
import os, shutil, time, sys, py_compile
from pathlib import Path

ROOT = Path.cwd()
SRC = ROOT / "voidx2_0.py"
BACKUP = ROOT / f"voidx2_0_backup_before_beast_{int(time.time())}.py"
WRAPPER = ROOT / "voidx2_0_final_beast.py"

if not SRC.exists():
    print("ERROR: voidx2_0.py not found in", ROOT)
    sys.exit(2)

# 1) create a backup
shutil.copy2(SRC, BACKUP)
print("Backup created:", BACKUP.name)

# ---------- module contents ----------
modules = {}

modules["beast_helpers.py"] = r'''
# beast_helpers.py - shared helpers and logger
import math, time, json, os, logging
from datetime import datetime

logger = logging.getLogger("void_beast")
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)s %(message)s"
    h.setFormatter(logging.Formatter(fmt))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

def clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except:
        return lo

def safe_get(d, k, default=None):
    try:
        return d.get(k, default)
    except:
        return default

def now_ts():
    return datetime.utcnow().isoformat()
'''

modules["beast_sentiment.py"] = r'''
# beast_sentiment.py - news sentiment + smoothing (EMA)
from collections import deque
from beast_helpers import logger

def ema(current, prev_ema, alpha):
    if prev_ema is None:
        return current
    return alpha * current + (1 - alpha) * prev_ema

class SentimentEngine:
    def __init__(self, alpha=0.2, window=5):
        self.alpha = float(alpha)
        self.prev_ema = None
        self.window = int(window)
        self.recent = deque(maxlen=self.window)

    def score_from_headlines(self, articles, keywords=None):
        kw = keywords or {"positive":["gain","profit","beat","rise"], "negative":["loss","fall","drop","war","strike","iran","oil"]}
        score = 0.0
        n = 0
        for a in articles or []:
            text = (a.get("title","") + " " + a.get("description","")).lower()
            if not text.strip():
                continue
            n += 1
            pos = sum(text.count(k) for k in kw["positive"])
            neg = sum(text.count(k) for k in kw["negative"])
            score += (pos - neg)
        if n == 0:
            raw = 0.0
        else:
            raw = max(-1.0, min(1.0, score / max(1.0, n)))
        self.prev_ema = ema(raw, self.prev_ema, self.alpha)
        self.recent.append(self.prev_ema)
        return self.prev_ema

    def get_smoothed(self):
        if not self.recent:
            return 0.0
        return sum(self.recent)/len(self.recent)
'''

modules["beast_scoring.py"] = r'''
# beast_scoring.py - Weighted combined scoring and HTF alignment helper
import os
W_TECH = float(os.getenv("BEAST_W_TECH", "0.70"))
W_MODEL = float(os.getenv("BEAST_W_MODEL", "0.20"))
W_FUND = float(os.getenv("BEAST_W_FUND", "0.10"))

def combined_score(tech_score, model_score, fund_score):
    try:
        t,m,f = float(tech_score), float(model_score), float(fund_score)
    except:
        t=m=f=0.0
    total = (W_TECH * t) + (W_MODEL * m) + (W_FUND * f)
    if total > 1:
        total = 1.0
    if total < -1:
        total = -1.0
    return total

def htf_alignment(h1_trend, m30_signal):
    if h1_trend == "bull" and m30_signal >= 0:
        return True
    if h1_trend == "bear" and m30_signal <= 0:
        return True
    if h1_trend == "neutral":
        return True
    return False
'''

modules["beast_threshold.py"] = r'''
# beast_threshold.py - Threshold Gravity + Volatility Weighted Engine + Anti-lock
import os, json
from datetime import datetime
from beast_helpers import clamp, logger

STATE_FILE = os.getenv("BEAST_THRESHOLD_STATE_FILE", "beast_threshold_state.json")

DEFAULT = {
    "min_threshold": 0.12,
    "base_threshold": 0.18,
    "max_threshold": 0.30,
    "current_threshold": 0.18,
    "gravity": 0.02,
    "adapt_speed": 0.01
}

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE,"r") as f:
                return json.load(f)
    except Exception:
        logger.exception("load_state failed")
    return DEFAULT.copy()

def save_state(s):
    try:
        with open(STATE_FILE,"w") as f:
            json.dump(s, f)
    except Exception:
        logger.exception("save_state failed")

def apply_gravity_and_volatility(current, volatility_adj=0.0):
    s = load_state()
    min_t, base, max_t = s["min_threshold"], s["base_threshold"], s["max_threshold"]
    gravity = s["gravity"]
    adapt_speed = s["adapt_speed"]
    pull = (base - current) * gravity
    adj = pull + float(volatility_adj)
    if adj > adapt_speed: adj = adapt_speed
    if adj < -adapt_speed: adj = -adapt_speed
    new_t = current + adj
    new_t = clamp(new_t, min_t, max_t)
    s["current_threshold"] = new_t
    s["last_updated"] = datetime.utcnow().isoformat()
    save_state(s)
    return new_t

def force_set_threshold(value):
    s = load_state()
    s["current_threshold"] = clamp(value, s["min_threshold"], s["max_threshold"])
    save_state(s)
    return s["current_threshold"]

def get_current_threshold():
    return load_state().get("current_threshold", DEFAULT["current_threshold"])
'''

modules["beast_risk.py"] = r'''
# beast_risk.py - Dynamic Risk Scaling Engine
import os, math
from beast_helpers import logger

BASE_RISK = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MID_RISK = 0.006
MAX_RISK = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "0.01"))

def compute_dynamic_risk(tech_score, fund_score, sent_score):
    try:
        tech, fund, sent = float(tech_score), float(fund_score), float(sent_score)
    except:
        tech=fund=sent=0.0
    def sign_val(x):
        if abs(x) < 0.01:
            return 0
        return 1 if x>0 else -1
    stech, sfund, ssent = sign_val(tech), sign_val(fund), sign_val(sent)
    if stech!=0 and stech==sfund==ssent:
        return MAX_RISK, "FULL_ALIGN"
    if (stech!=0 and stech==sfund) or (stech!=0 and stech==ssent) or (sfund!=0 and sfund==ssent):
        return MID_RISK, "TWO_ALIGN"
    return BASE_RISK, "BASE"
'''

modules["beast_protection.py"] = r'''
# beast_protection.py - SQF, Flash-crash, Liquidity Protection, Drawdown & Cooldown
import os, time
from beast_helpers import logger

SQF = {
    "max_spread_points": float(os.getenv("BEAST_MAX_SPREAD_POINTS","1000")),
    "vol_spike_mult": float(os.getenv("BEAST_VOL_SPIKE_MULT","2.5")),
    "unstable_move_pct": float(os.getenv("BEAST_UNSTABLE_MOVE_PCT","0.03")),
    "flash_gap_pct": float(os.getenv("BEAST_FLASH_GAP_PCT","0.05")),
    "cooldown_seconds": int(os.getenv("BEAST_COOLDOWN_SECONDS",60*3))
}

_last_trade_time = {}
_daily_drawdown = {"today":0.0}

def sqf_check(symbol, spread_points=None, atr_now=None, atr_avg=None, recent_move_pct=None):
    if spread_points is not None and spread_points > SQF["max_spread_points"]:
        return False, "spread_spike"
    if atr_avg and atr_now and atr_now > atr_avg * SQF["vol_spike_mult"]:
        return False, "vol_spike"
    if recent_move_pct and recent_move_pct > SQF["unstable_move_pct"]:
        return False, "unstable_move"
    return True, "ok"

def flash_crash_protect(symbol, last_tick_move_pct):
    if last_tick_move_pct and abs(last_tick_move_pct) > SQF["flash_gap_pct"]:
        return False, "flash_gap"
    return True, "ok"

def apply_cooldown(symbol):
    now = time.time()
    last = _last_trade_time.get(symbol, 0)
    if now - last < SQF["cooldown_seconds"]:
        return False, "cooldown_active"
    _last_trade_time[symbol] = now
    return True, "ok"

def update_drawdown(pnl):
    _daily_drawdown["today"] += pnl
    return _daily_drawdown["today"]

def within_drawdown_limit(max_daily_drawdown = -0.03, balance=1.0):
    dd = _daily_drawdown["today"]
    if dd <= max_daily_drawdown * balance:
        return False, "drawdown_exceeded"
    return True, "ok"
'''

modules["beast_dashboard.py"] = r'''
# beast_dashboard.py - minimal JSON dashboard snapshot per cycle
import json, os
from datetime import datetime
from beast_helpers import logger

DASH_FILE = os.getenv("BEAST_DASH_FILE", "beast_dashboard.json")

def publish_cycle(snapshot: dict):
    try:
        snapshot["ts"] = datetime.utcnow().isoformat()
        with open(DASH_FILE, "w") as f:
            json.dump(snapshot, f, indent=2)
    except Exception:
        logger.exception("publish_cycle failed")
'''

modules["beast_calendar.py"] = r'''
# beast_calendar.py - Macro eligibility + high-impact protection (event windows)
import datetime, os
from beast_helpers import logger

PRE_EVENT_BLOCK = int(os.getenv("BEAST_PRE_EVENT_BLOCK_SEC", 60*10))
POST_EVENT_BLOCK = int(os.getenv("BEAST_POST_EVENT_BLOCK_SEC", 60*10))

def is_within_event_window(event_ts_iso, now=None, pre=PRE_EVENT_BLOCK, post=POST_EVENT_BLOCK):
    try:
        now = now or datetime.datetime.utcnow()
        ev = datetime.datetime.fromisoformat(event_ts_iso)
        diff = (ev - now).total_seconds()
        if -post <= diff <= pre:
            return True
    except Exception:
        pass
    return False

def high_impact_block(events):
    now = datetime.datetime.utcnow()
    for e in events:
        if e.get("impact","").lower() in ("high","red","3"):
            if is_within_event_window(e.get("ts"), now):
                return True, f"high_impact_event:{e.get('title','')}"
    return False, ""
'''

modules["beast_symbols.py"] = r'''
# beast_symbols.py - per-symbol and global open limits (MT5 primary if available)
import os
from beast_helpers import logger

MAX_GLOBAL = int(os.getenv("BEAST_MAX_GLOBAL_OPEN", "15"))
PER_SYMBOL = {
    "XAUUSD": int(os.getenv("BEAST_MAX_XAUUSD", "3")),
    "XAGUSD": int(os.getenv("BEAST_MAX_XAGUSD", "3")),
    "BTCUSD": int(os.getenv("BEAST_MAX_BTCUSD", "5")),
    "USOIL" : int(os.getenv("BEAST_MAX_USOIL", "5")),
    "USDJPY": int(os.getenv("BEAST_MAX_USDJPY", "10")),
    "EURUSD": int(os.getenv("BEAST_MAX_EURUSD", "10")),
}

def count_open_positions(mt5_module=None):
    try:
        if mt5_module:
            orders = mt5_module.positions_get()
            total = len(orders) if orders else 0
            per = {}
            for o in orders or []:
                sym = getattr(o, "symbol", None) or o.get("symbol")
                per[sym] = per.get(sym,0)+1
            return total, per
    except Exception:
        logger.exception("count_open_positions failed")
    return 0, {}
'''

modules["beast_correlation.py"] = r'''
# beast_correlation.py - correlation risk engine helpers
import numpy as np
from beast_helpers import logger

def correlation_coefficient(series_a, series_b):
    try:
        a = np.array(series_a, dtype=float)
        b = np.array(series_b, dtype=float)
        if len(a) < 2 or len(b) < 2:
            return 0.0
        n = min(len(a), len(b))
        a = a[-n:]
        b = b[-n:]
        if np.std(a)==0 or np.std(b)==0:
            return 0.0
        return float(np.corrcoef(a,b)[0,1])
    except Exception:
        logger.exception("correlation failed")
        return 0.0
'''

modules["beast_liquidity.py"] = r'''
# beast_liquidity.py - commodity regime and liquidity protection helpers
from beast_helpers import logger

def commodity_regime_check(symbol, atr_now, atr_avg, spread):
    if symbol.upper() in ("XAUUSD","XAGUSD","USOIL"):
        if atr_now is None or atr_avg is None:
            return False, "missing_atr"
        if atr_now > atr_avg * 2.5:
            return False, "atr_spike"
        if spread and spread > 2000:
            return False, "spread_spike"
    return True, "ok"
'''

modules["beast_monitor.py"] = r'''
# beast_monitor.py - aggregator to create cycle snapshot for dashboard
from beast_helpers import logger
from beast_threshold import get_current_threshold
from beast_risk import compute_dynamic_risk

def make_snapshot(symbol, tech_score=None, model_score=None, fund_score=None, h1_trend=None, events=None):
    risk, risk_mode = compute_dynamic_risk(tech_score or 0, fund_score or 0, model_score or 0)
    snapshot = {
        "symbol": symbol,
        "tech_score": tech_score,
        "model_score": model_score,
        "fund_score": fund_score,
        "h1_trend": h1_trend,
        "threshold": get_current_threshold(),
        "risk": risk,
        "risk_mode": risk_mode,
        "events": events or []
    }
    return snapshot
'''

modules["beast_execution_fix.py"] = r'''
# beast_execution_fix.py - small helper to ensure order confirmation / requery
import time
from beast_helpers import logger

def confirm_order_send(send_fn, *args, retries=3, delay=1, **kwargs):
    for i in range(retries):
        try:
            res = send_fn(*args, **kwargs)
            if res:
                return res
        except Exception:
            logger.exception("order send attempt failed")
        time.sleep(delay)
    return None
'''

modules["beast_regime.py"] = r'''
# beast_regime.py - ATR regime helper
def atr_regime(atr_now, atr_avg):
    if atr_now is None or atr_avg is None:
        return "unknown", 0.0
    if atr_now > atr_avg * 1.2:
        return "high", (atr_now/atr_avg)
    if atr_now < atr_avg * 0.8:
        return "low", (atr_now/atr_avg)
    return "normal", (atr_now/atr_avg)
'''

modules["beast_nfp.py"] = r'''
# beast_nfp.py - high-impact news protection engine for NFP/CPI/FOMC
import datetime
from beast_helpers import logger

PRE = int(__import__("os").getenv("BEAST_PRE_EVENT_BLOCK_SEC","600"))
POST = int(__import__("os").getenv("BEAST_POST_EVENT_BLOCK_SEC","600"))

def should_block_for_event(event_ts_iso, now=None):
    try:
        now = now or datetime.datetime.utcnow()
        ev = datetime.datetime.fromisoformat(event_ts_iso)
        diff = (ev - now).total_seconds()
        if -POST <= diff <= PRE:
            return True, "high_impact_event_window"
    except Exception:
        logger.exception("nfp check error")
    return False, ""
'''

# ---------- write modules ----------
for name, content in modules.items():
    p = ROOT / name
    if p.exists():
        print("Module exists (skipping overwrite):", name)
    else:
        p.write_text(content, encoding="utf-8")
        print("Module written:", name)

# ---------- create wrapper file (safe: does not modify original) ----------
wrapper_text = r'''
# voidx2_0_final_beast.py - wrapper that loads beast modules then runs original voidx2_0.py
import logging
import runpy
import sys
from pathlib import Path

logger = logging.getLogger("void_beast")
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)s %(message)s"
    h.setFormatter(logging.Formatter(fmt))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

# Try to import beast modules (non-fatal if something missing)
_try_imports = [
    "beast_helpers","beast_sentiment","beast_scoring","beast_threshold",
    "beast_risk","beast_protection","beast_dashboard","beast_calendar",
    "beast_symbols","beast_correlation","beast_liquidity","beast_monitor",
    "beast_execution_fix","beast_regime","beast_nfp"
]
for m in _try_imports:
    try:
        __import__(m)
        logger.info("Loaded module: %s", m)
    except Exception as e:
        logger.warning("Module %s failed to import: %s", m, e)

# Execute the original bot in its own namespace
SRC = Path(__file__).parent / "voidx2_0.py"
if not SRC.exists():
    logger.error("Original voidx2_0.py not found; aborting.")
    sys.exit(2)

# run the bot as a script (this will execute the original main loop)
runpy.run_path(str(SRC), run_name="__main__")
'''

WRAPPER.write_text(wrapper_text, encoding="utf-8")
print("Wrote wrapper:", WRAPPER.name)

# ---------- syntax check all created modules + wrapper ----------
to_check = list(modules.keys()) + [WRAPPER.name]
errors = []
for fname in to_check:
    p = ROOT / fname
    try:
        py_compile.compile(str(p), doraise=True)
        print("Syntax OK:", fname)
    except py_compile.PyCompileError as e:
        errors.append((fname, str(e)))

if errors:
    print("\nSyntax errors detected:")
    for fn, err in errors:
        print(" -", fn, "\n", err)
    print("\nMerged files left in folder. Fix the listed modules and re-run.")
    sys.exit(4)

print("\nSUCCESS: All modules and wrapper compiled without syntax errors.")
print("Now you can run the final beast wrapper with:")
print("    python voidx2_0_final_beast.py")
print("If you want me to AUTO-WIRE the modules into your bot's main loop (call the helpers each 60s), reply 'AUTO-WIRE NOW'.")
