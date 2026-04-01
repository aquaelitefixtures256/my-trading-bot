# KYOTO_INFERNO_V20_PART1_FIXED.py
# --- V20 CLEAN HEADER (repaired) ---
from __future__ import annotations
import os
import sys
import time
import threading
import asyncio
import logging
import json
import traceback

__V15_SOURCE__ = ""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hardened_bot")

# The deterministic live loader below does not exec the embedded V15 source.
# Live trading logic is provided by the later runtime sections of this file.

import traceback

# --- V16 UPGRADE: EMBEDDED ORIGINAL V15 END ---

# --- V16 UPGRADE: infrastructure additions and orchestrator ---
import threading
import time
import logging
import sys
import os
import types
import csv
import math
import random
from datetime import datetime, timezone, timedelta

# Configure logging to stdout (unbuffered recommended with python -u)
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("KYOTO_V16")

def _thread_exception_hook(args):
    try:
        logger.critical(
            "Uncaught exception in thread %s",
            getattr(args.thread, "name", "unknown"),
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )
    except Exception:
        logging.critical(
            "Uncaught exception in thread %s",
            getattr(args.thread, "name", "unknown"),
        )

threading.excepthook = _thread_exception_hook

def _run_resilient_worker(name, stop_event, target, *args, restart_delay=1.0, max_delay=60.0, **kwargs):
    import time as _time
    time = _time
    """
    Run a long-lived worker and restart it if it crashes or returns.
    This keeps background threads alive even when a transient error escapes.
    """
    attempt = 0
    while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
        try:
            logger.info("Worker %s starting target=%s", name, getattr(target, "__name__", repr(target)))
            target(*args, **kwargs)
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break
            attempt = 0
            logger.warning("Worker %s target returned; restarting after %.1fs", name, restart_delay)
            _time.sleep(restart_delay)
        except Exception:
            logger.exception("Worker %s crashed", name)
            attempt += 1
            delay = min(max_delay, restart_delay * (2 ** (attempt - 1)))
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break
            logger.info("Worker %s restarting in %.1fs", name, delay)
            _time.sleep(delay)
    logger.info("Worker %s exiting", name)

# --- V16 UPGRADE: Configuration dict ---
CONFIG = {
    "DRY_RUN": False,
    "MAX_SPREADS": 0.6,  # set False only after testing
    "HEARTBEAT_INTERVAL": 30,
    "TICK_FRESHNESS_THRESHOLD": 3,
    "NEWS_RISK_PCT":  0.003,
    "RISK_PCT_NORMAL":  0.01,
    "MAX_DAILY_DRAWDOWN_PERCENT": 20.0,
    "MAX_CONSECUTIVE_LOSSES": 5,
    "NEWS_SCRAPE_INTERVAL": 60,
    "NEWS_LOOKAHEAD_MINUTES": 120,
    "NEWS_SPIKE_WAIT_SECONDS": 12,
    "NEWS_MIN_TICK_CONFIRM": 3,
    "FAKE_LIQUIDITY_REVERSAL_WINDOW": 5,
    "CORRELATION_THRESHOLD": -0.5,
    "CORRELATION_IGNORE_THRESHOLD": -0.3,
    "WATCH_SYMBOLS": ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL", "DXY", "US10Y"],
    "LIVE_STATUS_LOG_INTERVAL_SECONDS": 10,
    "GATE_SUPPRESSION_LOG_INTERVAL_SECONDS": 60,
    "LIVE_SCAN_SLEEP_SECONDS": 5.0,
    "LIVE_SCAN_ERROR_SLEEP_SECONDS": 1.0,
    "PAUSE_TRADING_FILE": "PAUSE_TRADING",
    "KILL_TRADING_FILE": "KILL_TRADING",
    "DRY_RUN_FLAG": False,
    "EXECUTION_SIGNAL_THRESHOLD": 0.30,
    "BACKTEST_DAYS": 7,
}

# # === PER-SYMBOL THRESHOLDS (use measured 95th percentiles) ===

# --- Per-symbol backtest params: p95 thresholds + ATR fraction thresholds (atr_pct_thresh)
CONFIG.setdefault("BACKTEST_PARAMS", {})
# === FINAL XAU CONSERVATIVE PATCH ===
CONFIG.setdefault("BACKTEST_PARAMS", {})
CONFIG["BACKTEST_PARAMS"].update({
    # conservative, selective XAU settings
    "XAUUSD":  {
        "signal_thresh": 0.92,        # stronger entry filter (fewer noise trades)
        "dxy_gate_thresh": 0.20,      # require stronger DXY confirmation (helps direction)
        "atr_pct_thresh": 0.0015,     # unchanged
        "sl_atr_mult": 4.0,           # tighten SL (less room for mid-size losers)
        "tp_atr_mult": 6.0,           # keep TP > SL to preserve R:R (give winners room)
        "max_hold": 30,               # unchanged
        "max_loss_abs": 18.0          # slightly tighter absolute cap (protect against remaining tails)
    },
    "XAUUSDm": {
        "signal_thresh": 0.92,
        "dxy_gate_thresh": 0.20,
        "atr_pct_thresh": 0.0015,
        "sl_atr_mult": 4.0,
        "tp_atr_mult": 6.0,
        "max_hold": 30,
        "max_loss_abs": 18.0
    }
})
# === end patch ===


# ===== APPLY SWEEP WINNER FOR XAU (inserted by script) =====
CONFIG["BACKTEST_PARAMS"].update({
    "BTCUSD": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0012, "max_hold": 60},
    "BTCUSDm": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0012, "max_hold": 60},

    "EURUSD": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0008, "max_hold": 60},
    "EURUSDm": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0008, "max_hold": 60},

    "USDJPY": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0009, "max_hold": 60},
    "USDJPYm": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0009, "max_hold": 60},

        "XAUUSD":  {"signal_thresh": 0.92, "dxy_gate_thresh": 0.20, "atr_pct_thresh": 0.0015, "sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "max_hold": 30, "max_loss_abs": 18.0},
    "XAUUSDm": {"signal_thresh": 0.92, "dxy_gate_thresh": 0.20, "atr_pct_thresh": 0.0015, "sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "max_hold": 30, "max_loss_abs": 18.0},




    "USOIL": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0010, "max_hold": 60},
    "USOILm": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0010, "max_hold": 60},

    "DXY": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0008, "max_hold": 60},
    "DXYm": {"signal_thresh": 0.30, "atr_pct_thresh": 0.0008, "max_hold": 60},
})


# APPLY: tighter XAU absolute loss cap -> 20
CONFIG.setdefault("BACKTEST_PARAMS", {})
CONFIG["BACKTEST_PARAMS"].update({
    "XAUUSD":  {"signal_thresh": 0.92, "dxy_gate_thresh": 0.20, "atr_pct_thresh": 0.0015, "sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "max_hold": 30, "max_loss_abs": 18.0},
    "XAUUSDm": {"signal_thresh": 0.92, "dxy_gate_thresh": 0.20, "atr_pct_thresh": 0.0015, "sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "max_hold": 30, "max_loss_abs": 18.0},

})



# === end thresholds ===
 

# === end block ===


# --- V16 UPGRADE: loader for embedded V15 ---
def load_v15_module():
    """Build a deterministic V15-compatible module without executing the broken
    embedded source string. This avoids the syntax error and keeps live trading
    on the deterministic MT5-based adapter path."""
    m = types.ModuleType("v15_impl")
    m.__dict__.update({
        "__name__": "v15_impl",
        "__file__": "<v15 compatibility shim>",
    })

    # Never exec the embedded source here; the embedded string is known to be
    # malformed in this build and would raise a SyntaxError at runtime.
    try:
        m = _install_v15_compute_signal_adapter(m)
    except Exception:
        logger.exception("Failed to install V15 compute_signal adapter")

    if not hasattr(m, "signal_to_side"):
        def _signal_to_side(symbol, price, ctx=None):
            try:
                sig = m.compute_signal(symbol, price, ctx) if callable(getattr(m, "compute_signal", None)) else 0.0
                if sig > 0:
                    return "BUY"
                if sig < 0:
                    return "SELL"
                return None
            except Exception:
                return None
        m.signal_to_side = _signal_to_side

    # Mark the loader as deterministic and live-only.
    m.V15_COMPAT_SHIM = True
    m.V15_EMBEDDED_SOURCE_SKIPPED = True
    return m



# === CHATGPT ADDED: deterministic compute_signal adapter (safe fallback) ===
def _install_v15_compute_signal_adapter(v15):
    """
    Ensure v15 has compute_signal(symbol, price, ctx) callable.
    If missing, attach a deterministic MT5-based fallback.
    """
    if v15 is None:
        return None

    if hasattr(v15, "compute_signal") and callable(getattr(v15, "compute_signal")):
        return v15  # already good

    # fallback implementation using MT5 bars: normalized (price - sma)/atr clipped to [-1,1]
    def compute_signal_fallback(symbol, price, ctx=None):
        """
        Deterministic fallback. If ctx is provided with 'bars' (history from backtest),
        use it. Otherwise fetch recent bars from MT5.
        Accepts bars either as list-of-dicts (with keys 'open','high','low','close') or
        as MT5-style tuples where index 4 is close, 2 is high, 3 is low.
        Returns float in [-1,1].
        """
        try:
            # 1) Try to get bars from ctx (preferred for backtests)
            bars_list = None
            if isinstance(ctx, dict) and ctx.get("bars"):
                bars_ctx = ctx.get("bars")
                if isinstance(bars_ctx, list) and len(bars_ctx) > 0:
                    # detect dict-style bars
                    first = bars_ctx[0]
                    if isinstance(first, dict):
                        closes = [float(b.get("close") or b.get("c") or 0.0) for b in bars_ctx]
                        highs  = [float(b.get("high")  or b.get("h") or 0.0) for b in bars_ctx]
                        lows   = [float(b.get("low")   or b.get("l") or 0.0) for b in bars_ctx]
                        bars_list = (closes, highs, lows)
                    else:
                        # assume tuple/list like MT5 (time, open, high, low, close, ...)
                        try:
                            closes = [float(r[4]) for r in bars_ctx]
                            highs  = [float(r[2]) for r in bars_ctx]
                            lows   = [float(r[3]) for r in bars_ctx]
                            bars_list = (closes, highs, lows)
                        except Exception:
                            bars_list = None

            # 2) If no ctx bars, fetch recent MT5 bars
            if bars_list is None:
                try:
                    import MetaTrader5 as mt5
                except Exception:
                    return 0.0
                sym = symbol
                try:
                    if not mt5.symbol_select(sym, True):
                        if mt5.symbol_select(sym + "m", True):
                            sym = sym + "m"
                except Exception:
                    pass
                tf = getattr(mt5, "TIMEFRAME_M30", mt5.TIMEFRAME_M30)
                raw = mt5.copy_rates_from_pos(sym, tf, 0, 60)
                if raw is None or len(raw) < 10:
                    return 0.0
                closes = [float(r[4]) for r in raw]
                highs  = [float(r[2]) for r in raw]
                lows   = [float(r[3]) for r in raw]
            else:
                closes, highs, lows = bars_list

            # Compute SMA (last up to 20) and ATR (14)
            n = len(closes)
            period = min(20, n)
            sma = sum(closes[-period:]) / period if period > 0 else closes[-1] if n>0 else float(price)

            tr_list = []
            for i in range(1, n):
                high = highs[i]; low = lows[i]; prev_close = closes[i-1]
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                tr_list.append(tr)
            atr_period = min(14, len(tr_list))
            atr = sum(tr_list[-atr_period:]) / atr_period if atr_period > 0 else 0.0

            eps = 1e-9
            raw_signal = (float(price) - sma) / (atr + eps)
            import math
            s = math.tanh(raw_signal / 2.0)
            if s > 1.0: s = 1.0
            if s < -1.0: s = -1.0
            return float(s)
        except Exception:
            return 0.0

    # attach to v15
    try:
        setattr(v15, "compute_signal", compute_signal_fallback)
    except Exception:
        # if cannot set attribute, wrap v15 in a tiny proxy class
        class _Proxy:
            def __init__(self, mod):
                self._mod = mod
            def __getattr__(self, name):
                if name == "compute_signal":
                    return compute_signal_fallback
                return getattr(self._mod, name)
        return _Proxy(v15)
    return v15
# === END CHATGPT ADDED BLOCK ===
# --- V16 UPGRADE: MT5 init and symbol auto-mapping ---
def mt5_init():
    """Lazy import MetaTrader5 and initialize. Returns (mt5_module, ok_bool)."""
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        logger.warning("MetaTrader5 not available: %s", e)
        return (None, False)
    try:
        ok = mt5.initialize()
        acc = mt5.account_info()  # may be None
        term = mt5.terminal_info()
        logger.info("mt5.initialize() returned %s, account_info present: %s", ok, bool(acc))
        return (mt5, bool(ok))
    except Exception as e:
        logger.exception("Error initializing MT5: %s", e)
        return (mt5, False)

def auto_map_symbols(mt5_module, symbols):
    """Auto-map canonical symbols to broker-specific variants (Exness style)."""
    _symbol_map = {}
    variants = ["{s}", "{s}m", "{s}.m", "{s}pro", "{s}.pro"]
    for s in symbols:
        mapped = None
        if mt5_module:
            for v in variants:
                candidate = v.format(s=s)
                try:
                    info = mt5_module.symbol_info(candidate)
                    if info is not None:
                        mapped = candidate
                        break
                except Exception:
                    continue
        if not mapped:
            # Try simple Exness-like 'm' suffix
            cand = s + "m"
            mapped = cand  # fallback to candidate even if symbol not present
        _symbol_map[s] = mapped
        logger.info("Mapped %s -> %s", s, mapped)
    return _symbol_map

# --- V16 UPGRADE: helper functions ---
# --- ATR helper used by backtest and live scanner ---
def compute_atr_from_ctx(recent):
    """
    recent: list of dicts with keys open/high/low/close (oldest->newest)
    returns (atr, sma)
    """
    highs = [r.get("high") for r in recent if r.get("high") is not None]
    lows  = [r.get("low") for r in recent if r.get("low") is not None]
    closes= [r.get("close") for r in recent if r.get("close") is not None]
    if len(closes) < 2:
        return 0.0, (closes[-1] if closes else 0.0)
    tr = []
    for j in range(1, len(closes)):
        try:
            tr_val = max(highs[j] - lows[j], abs(highs[j] - closes[j-1]), abs(lows[j] - closes[j-1]))
        except Exception:
            # fallback if highs/lows shorter
            tr_val = abs(closes[j] - closes[j-1])
        tr.append(tr_val)
    atr_period = min(14, len(tr))
    atr = sum(tr[-atr_period:]) / atr_period if atr_period > 0 else 0.0
    sma = sum(closes[-min(20, len(closes)):]) / min(20, len(closes)) if closes else 0.0
    return atr, sma


def _compute_atr_from_recent(bars, period=14):
    """
    Robust ATR from a recent 'bars' list.

    Accepts:
      - list of dicts with keys "high"/"low"/"close" (or 'h','l','c')
      - list/ndarray of sequences where indices map to (time, open, high, low, close, ...)
      - numpy structured array rows (indexed like tuple)

    Returns ATR (float). If not enough data returns a conservative average over available TRs or 0.0.
    """
    if not bars:
        return 0.0

    # helper to extract numeric fields from a bar (dict or sequence)
    def _get(bar, key_seq, idx):
        # try mapping names first
        if isinstance(bar, dict):
            for k in key_seq:
                if k in bar and bar[k] is not None:
                    try:
                        return float(bar[k])
                    except Exception:
                        pass
            # fallback to sequence access if dict values are sequence-like
            try:
                return float(bar[idx])
            except Exception:
                return None
        else:
            # sequence / numpy record
            try:
                return float(bar[idx])
            except Exception:
                # try named attributes/fields
                for k in key_seq:
                    try:
                        v = getattr(bar, k)
                        if v is not None:
                            return float(v)
                    except Exception:
                        pass
            return None

    # Build true ranges list
    trs = []
    prev_close = None
    # iterate bars in chronological order if possible (assume input already oldest->newest)
    for i, b in enumerate(bars):
        high = _get(b, ("high", "h"), 2)   # prefer dict keys, else idx 2
        low = _get(b, ("low", "l"), 3)     # prefer dict keys, else idx 3
        close = _get(b, ("close", "c"), 4) # prefer dict keys, else idx 4

        if high is None or low is None:
            # skip bars that don't give us high/low
            prev_close = close if prev_close is None else prev_close
            continue

        if prev_close is None:
            # set prev_close from this bar's close (can't compute TR yet)
            prev_close = close
            continue

        # compute TR
        tr_candidates = [high - low]
        if prev_close is not None:
            tr_candidates.append(abs(high - prev_close))
            tr_candidates.append(abs(low - prev_close))
        tr = max(tr_candidates)
        try:
            trs.append(float(tr))
        except Exception:
            pass

        prev_close = close if close is not None else prev_close

    if not trs:
        return 0.0

    if len(trs) < period:
        # conservative average over available TRs
        return sum(trs) / float(max(1, len(trs)))

    # return simple moving average of last `period` TRs
    return sum(trs[-period:]) / float(period)


__compute_atr_from_recent = _compute_atr_from_recent


__compute_atr_from_recent = _compute_atr_from_recent


def file_flag(name):
    return os.path.exists(name)

# --- V16 UPGRADE: fake-liquidity detector ---
def detect_fake_liquidity(samples, spike_pct=0.002, reversal_pct=0.001, window=CONFIG.get("FAKE_LIQUIDITY_REVERSAL_WINDOW",5)):
    """Detect a spike followed by reversal within window seconds.
    samples: list of (ts, price) tuples sorted by ts asc"""
    if len(samples) < 3:
        return False
    prices = [p for t,p in samples]
    max_p = max(prices)
    min_p = min(prices)
    start = prices[0]
    # spike up then reverse
    if (max_p - start)/start >= spike_pct and (max_p - prices[-1])/max_p >= reversal_pct:
        return True
    # spike down then reverse
    if (start - min_p)/start >= spike_pct and (prices[-1] - min_p)/min_p >= reversal_pct:
        return True
    return False

# --- V16 UPGRADE: correlation computation ---
def compute_correlation(list_a, list_b):
    try:
        import numpy as np
    except Exception:
        # simple Pearson fallback
        if len(list_a) != len(list_b) or len(list_a) < 2:
            return 0.0
        a_mean = sum(list_a)/len(list_a)
        b_mean = sum(list_b)/len(list_b)
        num = sum((x-a_mean)*(y-b_mean) for x,y in zip(list_a,list_b))
        den = math.sqrt(sum((x-a_mean)**2 for x in list_a)*sum((y-b_mean)**2 for y in list_b))
        if den == 0: return 0.0
        return num/den
    a = np.array(list_a)
    b = np.array(list_b)
    if a.size < 2: return 0.0
    return float(np.corrcoef(a,b)[0,1])

# --- V16 UPGRADE: order wrapper ---

def _deprecated_order_wrapper(mt5_module, order_request):
    """Centralized order execution wrapper. order_request is a dict following MT5 order_send or a custom dict."""
    # Basic checks
    if file_flag(CONFIG.get("KILL_TRADING_FILE")):
        logger.critical("KILL_TRADING flag present - refusing to send orders.")
        return {"retcode": -1, "comment": "KILLED"}
    if file_flag(CONFIG.get("PAUSE_TRADING_FILE")):
        logger.warning("PAUSE_TRADING flag present - pausing orders.")
        return {"retcode": -1, "comment": "PAUSED"}
    if CONFIG.get("DRY_RUN") or CONFIG.get("DRY_RUN_FLAG") or mt5_module is None:
        logger.info("DRY_RUN active or MT5 not available - dry-run order: %s", order_request)
        return {"retcode": 0, "comment": "DRY_RUN_DRY_RUN", "order": order_request}

    try:
        if order_request is None:
            return {"retcode": -1, "comment": "ORDER_REQUEST_NONE"}

        if not isinstance(order_request, dict):
            try:
                order_request = dict(order_request)
            except Exception:
                return {"retcode": -1, "comment": "BAD_ORDER_REQUEST"}

        # Validate connection
        info = mt5_module.account_info()
        if info is None:
            logger.error("MT5 account info not available - cannot send order")
            return {"retcode": -1, "comment": "NO_ACCOUNT"}

        sym = order_request.get("symbol") or order_request.get("instrument")
        if not sym:
            return {"retcode": -1, "comment": "MISSING_SYMBOL"}

        tick = None
        if hasattr(mt5_module, "symbol_info_tick"):
            try:
                tick = mt5_module.symbol_info_tick(sym)
            except Exception:
                tick = None

        # check spread if symbol and price provided
        if tick is not None and hasattr(tick, "ask") and hasattr(tick, "bid"):
            spread = abs(float(tick.ask) - float(tick.bid))
            base_max_spread = CONFIG.get("MAX_SPREADS", 0.6)
            if isinstance(base_max_spread, dict):
                max_spread = float(
                    base_max_spread.get(sym,
                    base_max_spread.get(sym.upper(), base_max_spread.get("DEFAULT", 0.6)))
                )
            else:
                max_spread = float(base_max_spread)

            sym_u = str(sym).upper()
            if sym_u.startswith("BTC"):
                max_spread = max(max_spread, 25.0)
            elif sym_u.startswith("XAU"):
                max_spread = max(max_spread, 1.5)
            elif sym_u.startswith("USOIL") or "OIL" in sym_u:
                max_spread = max(max_spread, 1.0)
            elif sym_u.startswith("EURUSD"):
                max_spread = max(max_spread, 0.0020)
            elif sym_u.startswith("USDJPY"):
                max_spread = max(max_spread, 0.0500)
            elif sym_u.startswith("DXY") or sym_u.startswith("US10Y"):
                max_spread = max(max_spread, 0.2500)

            if spread > max_spread:
                logger.warning("Spread too high for %s: %s > %s", sym, spread, max_spread)
                return {"retcode": -1, "comment": "SPREAD_TOO_HIGH"}

        side = str(order_request.get("type", "")).lower()
        side_is_buy = side in ("buy", "long")
        side_is_sell = side in ("sell", "short")

        # Build a broker-safe MT5 request if possible.
        req = dict(order_request)
        req["symbol"] = sym
        req["volume"] = float(req.get("volume", CONFIG.get("DEFAULT_ORDER_VOLUME", 0.01)))
        req.setdefault("deviation", int(CONFIG.get("ORDER_DEVIATION", 20)))
        req.setdefault("magic", int(CONFIG.get("MAGIC_NUMBER", 26032026)))
        req.setdefault("comment", str(CONFIG.get("ORDER_COMMENT", "kyoto_live")))

        if side_is_buy or side_is_sell:
            order_type = getattr(mt5_module, "ORDER_TYPE_BUY", None) if side_is_buy else getattr(mt5_module, "ORDER_TYPE_SELL", None)
            if order_type is not None:
                req["type"] = order_type

        action = getattr(mt5_module, "TRADE_ACTION_DEAL", None)
        if action is not None:
            req["action"] = action

        # Use live broker price if missing or invalid.
        price = req.get("price")
        try:
            price_f = float(price) if price is not None else 0.0
        except Exception:
            price_f = 0.0
        if price_f <= 0.0 and tick is not None:
            if side_is_buy and hasattr(tick, "ask"):
                price_f = float(tick.ask)
            elif side_is_sell and hasattr(tick, "bid"):
                price_f = float(tick.bid)
            else:
                price_f = float(getattr(tick, "last", 0.0) or 0.0)
        req["price"] = price_f

        # Optional MT5 execution preferences.
        type_time = getattr(mt5_module, "ORDER_TIME_GTC", None)
        if type_time is not None:
            req.setdefault("type_time", type_time)
        for fill_name in ("ORDER_FILLING_IOC", "ORDER_FILLING_RETURN", "ORDER_FILLING_FOK"):
            fill_val = getattr(mt5_module, fill_name, None)
            if fill_val is not None:
                req.setdefault("type_filling", fill_val)
                break

        # send order
        res = mt5_module.order_send(req)
        logger.info("order_send result: %s", res)

        if res is None:
            logger.error("order_send returned None for %s; request=%s", sym, req)
            return {"retcode": -1, "comment": "ORDER_SEND_RETURNED_NONE", "request": req}

        if hasattr(res, "_asdict"):
            return res._asdict()
        if isinstance(res, dict):
            return res
        try:
            return dict(res)
        except Exception:
            return {
                "retcode": getattr(res, "retcode", -1),
                "comment": getattr(res, "comment", str(res)),
                "result": str(res),
            }
    except Exception as e:
        logger.exception("Exception in order_wrapper: %s", e)
        return {"retcode": -1, "comment": str(e)}


# --- V16 UPGRADE: live scanner loop ---

# -----------------------------------------------------------------------------
# LIVE SCANNER (UPGRADED) - explanation (simple English):
# This function runs in a dedicated background thread and continuously scans
# the configured WATCH_SYMBOLS. For each symbol it:
# 1) Maps the canonical symbol to the broker/MT5 symbol using symbol_map.
# 2) Tries to read a live tick from MT5. If there's no tick it skips the symbol.
# 3) Extracts the best available price (last -> ask -> bid) and checks tick age.
# 4) Calls the V15 module (if present) to compute a signal:
#      - Prefer v15_module.compute_signal(sym, price, {})
#      - Fallback to v15_module.signal_to_side(sym, price)
#    If the call fails or returns None, a small random fallback signal is used.
# 5) Classifies the market regime as 'trending' when |signal| > 0.5, otherwise 'ranging'.
# 6) Logs a special execution message when regime == 'trending' so the execution layer
#    (order placement code) can monitor logs or hook into this event.
# 7) Prints a concise line with symbol, price, signal, and regime.
#
# Rationale and safety:
# - The scanner only logs an "EXECUTION ENGINE TRIGGERED" message when the signal is
#   strong (|signal|>0.5). The execution layer should subscribe to this and apply
#   additional checks (risk governor, SQF, spread, drawdown) before sending real orders.
# - Using MT5 ticks ensures trades are based on live broker data; for symbols without
#   MT5 ticks we skip them (safer than guessing).
# - The function is defensive: any exception in computing signals or reading ticks
#   will be caught and will not crash the thread.
# -----------------------------------------------------------------------------

def _deprecated_execute_signal(sym, signal, price, mt5_module, symbol_map):
    """
    Conservative execution helper:
    - Only runs when AUTO_EXECUTE is True and not in DRY_RUN.
    - Applies basic checks and then calls order_wrapper().
    - Default execution threshold is CONFIG["EXECUTION_SIGNAL_THRESHOLD"] (default 0.40).
    """
    try:
        threshold = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.40))
        if str(sym).upper().startswith(("DXY", "US10Y")):
            logger.info("Execution skipped for %s: macro filter symbol only", sym)
            return None
        if not globals().get("AUTO_EXECUTE", True):
            logger.info("Execution skipped for %s: AUTO_EXECUTE disabled", sym)
            return None
        if CONFIG.get("DRY_RUN") or CONFIG.get("DRY_RUN_FLAG"):
            logger.info("Execution skipped for %s: DRY_RUN active", sym)
            return None
        if signal is None:
            logger.info("Execution skipped for %s: signal is None", sym)
            return None
        if abs(signal) < threshold:
            logger.info("Execution skipped for %s: signal below execution threshold (%.3f) signal=%.4f", sym, threshold, signal)
            return None

        side = "buy" if signal > 0 else "sell"
        mapped = symbol_map.get(sym, sym) if symbol_map else sym
        volume = float(CONFIG.get("DEFAULT_ORDER_VOLUME", 0.01))
        order = {"symbol": mapped, "volume": volume, "type": side}
        logger.info("Attempting execution for %s: side=%s vol=%s price=%.6f signal=%.4f threshold=%.3f", sym, side, volume, price, signal, threshold)
        res = order_wrapper(mt5_module, order)

        # Surface any broker-side / spread-side rejections in a friendly, visible way
        try:
            comment = None
            if isinstance(res, dict):
                comment = res.get("comment") or res.get("retcode")
            else:
                comment = getattr(res, "comment", None) or getattr(res, "retcode", None)
            if comment:
                comment_str = str(comment)
                if "SPREAD_TOO_HIGH" in comment_str:
                    logger.info("Skipped trade for %s because spread was too high", sym)
                elif "KILLED" in comment_str:
                    logger.info("Skipped trade for %s because trading is killed by flag", sym)
                elif "PAUSED" in comment_str:
                    logger.info("Skipped trade for %s because trading is paused by flag", sym)
                elif "NO_ACCOUNT" in comment_str:
                    logger.info("Skipped trade for %s because MT5 account info is unavailable", sym)
                elif "ORDER_SEND_RETURNED_NONE" in comment_str:
                    logger.info("Skipped trade for %s because MT5 returned no order result", sym)
        except Exception:
            pass

        logger.info("Execution result for %s: %s", sym, res)
        return res
    except Exception:
        logger.exception("execute_signal failed for %s", sym)
        return None


def live_scanner_loop(stop_event, mt5_module, symbol_map, v15_module):
    import time as _time
    time = _time  # local guarantee so both time and _time resolve
    logger.info("Started background thread: live-scanner")
    last_print = 0
    while not stop_event.is_set():
        try:
            for sym in CONFIG.get("WATCH_SYMBOLS", []):
                mapped = symbol_map.get(sym, sym)
                tick = None
                if mt5_module:
                    try:
                        tick = mt5_module.symbol_info_tick(mapped)
                    except Exception:
                        tick = None
                # Skip if no tick
                if tick is None:
                    logger.warning(f"No MT5 tick for {sym}, skipping")
                    continue
                # Extract price
                price = float(
                    getattr(
                        tick,
                        "last",
                        getattr(
                            tick,
                            "ask",
                            getattr(tick, "bid", 0.0)
                        )
                    )
                )
                tick_time = getattr(tick, "time", time.time())
                # Skip stale ticks
                if time.time() - tick_time > CONFIG.get("TICK_FRESHNESS_THRESHOLD", 3):
                    continue
                
                # --- build recent_for_ctx for ATR check (try MT5 minute bars) ---
                recent_for_ctx = []
                try:
                    params = CONFIG.get("BACKTEST_PARAMS", {}).get(sym, {})
                except Exception:
                    params = {}
                try:
                    if mt5_module:
                        mapped = symbol_map.get(sym, sym) if symbol_map else sym
                        try:
                            rates = mt5_module.copy_rates_from_pos(mapped, getattr(mt5_module, "TIMEFRAME_M30", 0), 0, 120) or []
                        except Exception:
                            rates = []
                        for r in (rates[-60:] if rates else []):
                            try:
                                recent_for_ctx.append({"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])})
                            except Exception:
                                continue
                except Exception:
                    recent_for_ctx = []
                try:
                    atr_pct_thresh = params.get("atr_pct_thresh", 0.0)
                    atr, sma = compute_atr_from_ctx(recent_for_ctx)
                    atr_frac = (atr / sma) if (sma and sma != 0) else 0.0
                except Exception:
                    atr_frac = 0.0
                    atr_pct_thresh = 0.0
                if atr_pct_thresh and atr_frac < atr_pct_thresh:
                    regime = "low_vol"
                    if _time.time() - last_print > 0.5:
                        logger.info(f"{sym} marked low_vol (atr_frac={atr_frac:.6f} < thresh={atr_pct_thresh})")
                        last_print = _time.time()
                    continue
                signal = None
                regime = "unknown"
                try:
                    if v15_module and hasattr(v15_module, "compute_signal"):
                        globals().setdefault("_KYOTO_LAST_RECENT_FOR_CTX_BY_SYMBOL", {})[sym] = recent_for_ctx
                        signal = v15_module.compute_signal(sym, price, {"bars": recent_for_ctx})
                    elif v15_module and hasattr(v15_module, "signal_to_side"):
                        signal = v15_module.signal_to_side(sym, price)
                except Exception:
                    signal = None
                # Fallback if signal fails
                if signal is None:
                    logger.info("Skipping %s: no deterministic signal from v15", sym)
                    continue
                # Detect regime
                params = CONFIG.get("BACKTEST_PARAMS", {}).get(sym, {})
                live_signal_thresh = params.get("signal_thresh", 0.50)
                # --- V16 UPGRADE: correlation-aware XAU macro gate ---
                if (sym if 'sym' in locals() else canon).upper().startswith("XAU"):
                    try:
                        desired_side = "BUY" if signal is not None and signal > 0 else ("SELL" if signal is not None and signal < 0 else None)
                        macro_ok = xau_macro_confirm(
                            mt5_module,
                            desired_side,
                            symbol_map.get("XAUUSD", "XAUUSDm") if symbol_map else "XAUUSDm",
                            symbol_map.get("DXY", "DXY") if symbol_map else "DXY",
                            symbol_map=symbol_map or {},
                        )
                    except Exception:
                        macro_ok = True
                    if not macro_ok:
                        regime = "dxy_blocked"
                        try:
                            logger.info("XAU entry suppressed by macro confirmation gate | sym=%s | signal=%.4f", (sym if 'sym' in locals() else canon), float(signal) if signal is not None else None)
                        except Exception:
                            logger.info("XAU entry suppressed by macro confirmation gate | sym=%s", (sym if 'sym' in locals() else canon))
                        continue
                # --- end V16 UPGRADE: correlation-aware XAU macro gate ---

                
                if not hasattr(live_scanner_loop, "_logged_thresholds"):
                    live_scanner_loop._logged_thresholds = set()
                if sym not in getattr(live_scanner_loop, "_logged_thresholds"):
                    logger.info("Live scanner using threshold %s for %s", live_signal_thresh, sym)
                    live_scanner_loop._logged_thresholds.add(sym)
                
                if signal is not None and signal >= live_signal_thresh:
                    regime = "trending"
                    # ... existing trending logic ...
                elif signal is not None and signal <= -live_signal_thresh:
                    regime = "trending"
                    # ... existing trending logic ...
                else:
                    regime = "ranging"
                # 🔔 EXECUTION SIGNAL LOG
                if regime == "trending":
                    logger.info(
                        f"EXECUTION ENGINE TRIGGERED | {sym} | signal={signal:.4f} | price={price}"
                    )
                
                # Attempt conservative execution (uses CONFIG flags and DRY_RUN safety)
                try:
                    execute_signal(sym, signal, price, mt5_module, symbol_map)
                except Exception:
                    logger.exception("execute_signal call failed")
# Print scanner output
                line = f"{sym} | price: {price:.6f} | signal: {signal:.4f} | regime: {regime}"
                if time.time() - last_print > 0.5:
                    print(line, flush=True)
                    last_print = time.time()
            time.sleep(1.0)
        except Exception as e:
            logger.exception("Exception in live_scanner_loop: %s", e)
            time.sleep(1.0)

def trailing_manager_thread(stop_event, mt5_module, symbol_map):
    logger.info("Started background thread: trailing-manager")
    while not stop_event.is_set():
        try:
            # This is a placeholder: in live, check open positions and modify SL progressively
            # For now, just sleep
            time.sleep(5.0)
        except Exception as e:
            logger.exception("Exception in trailing_manager_thread: %s", e)
            time.sleep(1.0)

# --- V16 UPGRADE: news scheduler & handler (simplified) ---
def fetch_forexfactory_calendar():
    try:
        import requests
        from bs4 import BeautifulSoup as BS
    except Exception as e:
        logger.warning("requests/bs4 not available: %s", e)
        return []
    try:
        url = 'https://www.forexfactory.com/calendar.php'
        r = requests.get(url, timeout=10)
        soup = BS(r.text, 'html.parser')
        rows = soup.select('.calendar__row') or soup.select('tr.calendar__row')
        events = []
        for row in rows[:50]:
            try:
                # very defensive parsing
                t = row.get('data-event-datetime') or row.get('data-epoch')
                title = row.get('data-event') or row.text.strip()
                events.append({'title': title, 'time': t})
            except Exception:
                continue
        return events
    except Exception as e:
        logger.exception("Error fetching forex factory: %s", e)
        return []

def news_scheduler_thread(stop_event, mt5_module, symbol_map, v15_module):
    logger.info("Started background thread: news-scheduler")
    dedupe = set()
    interval = CONFIG.get('NEWS_SCRAPE_INTERVAL',60)
    while not stop_event.is_set():
        try:
            events = fetch_forexfactory_calendar()
            now = datetime.utcnow()
            for ev in events:
                key = ev.get('title') + '|' + str(ev.get('time'))
                if key in dedupe: continue
                # schedule if USD related - naive
                if 'USD' in ev.get('title','').upper():
                    dedupe.add(key)
                    # spawn handler
                    threading.Thread(target=news_handler, args=(ev, mt5_module, symbol_map, v15_module), daemon=True).start()
            time.sleep(interval)
        except Exception as e:
            logger.exception("Exception in news_scheduler_thread: %s", e)
            time.sleep(5)

def sample_ticks(mt5_module, symbol, duration_seconds=10):
    samples = []
    t0 = time.time()
    while time.time() - t0 < duration_seconds:
        if mt5_module:
            try:
                tick = mt5_module.symbol_info_tick(symbol)
                price = tick.bid if (tick and tick.bid > 0) else (tick.ask if (tick and tick.ask > 0) else 0.0)
            except Exception:
                price = 0.0
        else:
            price = 0.0
        samples.append((time.time(), price))
        time.sleep(3)
    return samples

def news_handler(event, mt5_module, symbol_map, v15_module):
    logger.info("Handling news event: %s", event.get('title'))
    # simplified workflow: sample ticks and decide nothing unless clear
    try:
        # sample around XAUUSD
        x_sym = symbol_map.get('XAUUSD','XAUUSDm')
        samples = sample_ticks(mt5_module, x_sym, duration_seconds=CONFIG.get('NEWS_SPIKE_WAIT_SECONDS',12))
        if detect_fake_liquidity(samples):
            logger.warning("Fake liquidity detected around event %s - skipping", event.get('title'))
            return

        # macro confirmation uses the same helper as live scanning
        try:
            bias_ok = xau_macro_confirm(
                mt5_module,
                "BUY",
                symbol_map.get("XAUUSD", "XAUUSDm") if symbol_map else "XAUUSDm",
                symbol_map.get("DXY", "DXY") if symbol_map else "DXY",
                symbol_map=symbol_map or {},
            )
        except Exception:
            bias_ok = True
        if not bias_ok:
            logger.info("News macro confirmation failed — skipping news trade")
            return

        # very conservative: do not place automated news trades in DRY_RUN
        if CONFIG.get('DRY_RUN'):
            logger.info("DRY_RUN active - news handler will not open trades.")
            return
        # otherwise build a conservative order (placeholder)
        order = {'symbol': x_sym, 'volume': 0.01, 'type': 'buy'}
        order_wrapper(mt5_module, order)
    except Exception as e:
        logger.exception("Exception in news_handler: %s", e)

# --- V16 UPGRADE: health monitor thread used by start_bot ---
def health_monitor_thread(stop_event, mt5_module, symbol_map):
    logger.info("Started background thread: health-monitor")
    backoff = 1
    while not stop_event.is_set():
        try:
            # heartbeat
            logger.info("HEARTBEAT: %s", datetime.utcnow().isoformat())
            # check MT5
            if mt5_module:
                try:
                    acc = mt5_module.account_info()
                    if acc is None:
                        logger.warning("MT5 account_info missing - attempting reinit")
                        mt5_module.initialize()
                except Exception as e:
                    logger.warning("MT5 health check failed: %s", e)
            time.sleep(CONFIG.get('HEARTBEAT_INTERVAL',30))
        except Exception as e:
            logger.exception("Exception in health_monitor_thread: %s", e)
            time.sleep(backoff)
            backoff = min(backoff*2, 60)

# --- V16 UPGRADE: backtest harness (simple) ---
def run_backtest(v15_module, symbol='XAUUSD', days=30):

    logger.info("Starting backtest for %s for %s days", symbol, days)

    import csv
    import random

        # --- MT5 safe fetch + symbol resolution (paste in run_backtest where you previously called copy_rates_from_pos) ---
    import MetaTrader5 as mt5

    # Ensure MT5 is initialized
    try:
        if not mt5.initialize():
            logger.error("MT5 initialize failed in run_backtest: %s", mt5.last_error())
            return {"net":0,"trades":0,"win_rate":0.0,"max_dd":0.0, "error":"mt5_init_failed"}
    except Exception as e:
        logger.exception("MT5 initialize exception: %s", e)
        return {"net":0,"trades":0,"win_rate":0.0,"max_dd":0.0, "error":"mt5_init_exception"}

    # Resolve symbol (try as-is, then try Exness 'm' suffix)
    resolved_sym = None
    try:
        if mt5.symbol_select(symbol, True):
            resolved_sym = symbol
        elif mt5.symbol_select(symbol + "m", True):
            resolved_sym = symbol + "m"
        else:
            logger.error("Failed to select symbol %s or %sm in MT5 Market Watch. Add symbol to Market Watch.", symbol, symbol)
            return {"net":0,"trades":0,"win_rate":0.0,"max_dd":0.0, "error":"symbol_not_found"}
    except Exception:
        logger.exception("Symbol selection error for %s", symbol)
        return {"net":0,"trades":0,"win_rate":0.0,"max_dd":0.0, "error":"symbol_select_exception"}

    # Request bars (reliable copy_rates_from_pos)
    try:
        total_bars = int(days * 24 * 60)  # minutes
        rates = mt5.copy_rates_from_pos(resolved_sym, mt5.TIMEFRAME_M30, 0, total_bars)
    except Exception:
        logger.exception("MT5 copy_rates_from_pos raised for %s", resolved_sym)
        return {"net":0,"trades":0,"win_rate":0.0,"max_dd":0.0, "error":"mt5_copy_error"}

    if rates is None or len(rates) == 0:
        logger.error("MT5 returned no historical data for %s (resolved=%s). Ensure MT5 terminal logged into Exness and symbol present in Market Watch.", symbol, resolved_sym)
        return {"net":0,"trades":0,"win_rate":0.0,"max_dd":0.0, "error":"no_data"}

    # Build bars list (oldest->newest)
    bars = []
    for r in rates:
        # r may be tuple-like or dict-like depending on MT5 binding
        try:
            t = int(r[0])
            open_p = float(r[1]); high = float(r[2]); low = float(r[3]); close = float(r[4])
            vol = int(r[5]) if len(r) > 5 else int(getattr(r, "tick_volume", 0))
        except Exception:
            # try mapping-style access
            t = int(r.get("time", int(time.time())))
            open_p = float(r.get("open", 0.0))
            high = float(r.get("high", 0.0))
            low = float(r.get("low", 0.0))
            close = float(r.get("close", 0.0))
            vol = int(r.get("tick_volume", 0))
        bars.append({
            "time": datetime.utcfromtimestamp(t),
            "open": open_p, "high": high, "low": low, "close": close, "tick_volume": vol
        })

    # Use resolved_sym for any broker-specific checks later
    symbol = resolved_sym


    trades = []

    position = None
    entry_price = None
    entry_index = None

    wins = 0
    losses = 0

    for i, bar in enumerate(bars):

        price = bar['close']

        signal = None

                # --- Begin run_backtest: pass recent backtest bars to the signal function --- create recent slice of historical bars (up to last 60 bars) for model context 

        start_idx = max(0, i - 60)
        recent_slice = bars[start_idx:i+1]  # includes current bar

        # normalize to dicts if necessary 

        recent_for_ctx = []
        for b in recent_slice:
            if isinstance(b, dict) and "close" in b:
                recent_for_ctx.append(b)
            else:
                # assume MT5-style tuple/list: (time, open, high, low, close, ...)
                try:
                    recent_for_ctx.append({
                        "time": int(b[0]),
                        "open": float(b[1]),
                        "high": float(b[2]),
                        "low": float(b[3]),
                        "close": float(b[4])
                    })
                except Exception:
                    # safe fallback
                    try:
                        recent_for_ctx.append({"close": float(b.get("close", 0.0))})
                    except Exception:
                        recent_for_ctx.append({"close": 0.0})

        # call model with bars context 

        try:
            if v15_module and hasattr(v15_module, "compute_signal"):
                signal = v15_module.compute_signal(symbol, price, {"bars": recent_for_ctx})
            elif v15_module and hasattr(v15_module, "signal_to_side"):
                signal = v15_module.signal_to_side(symbol, price)
            else:
                signal = None
        except Exception:
            signal = None
        # --- End run_backtest replacement ---

        
        # --- ATR volatility gate (in run_backtest, right before checking entry) ---
        try:
            params = CONFIG.get("BACKTEST_PARAMS", {}).get(symbol, {})
        except Exception:
            params = {}
        atr_pct_thresh = params.get("atr_pct_thresh", 0.0)
        atr, sma = compute_atr_from_ctx(recent_for_ctx)
        # avoid divide by zero
        atr_frac = (atr / sma) if (sma and sma != 0) else 0.0
        if atr_pct_thresh and atr_frac < atr_pct_thresh:
            suppressed_for_volatility = True
        else:
            suppressed_for_volatility = False
        # --- end ATR gate ---
        # --- DXY <-> XAU correlation gate (backtest) ---
        sparams = CONFIG.get("BACKTEST_PARAMS", {}).get(symbol, {})
        dxy_thresh = sparams.get("dxy_gate_thresh", None)

        dxy_signal = None
        dxy_gate_block = False
        if dxy_thresh is not None and symbol.upper().startswith("XAU"):
            try:
                # try to compute DXY signal for the same time index using v15 and DXY bars
                # if not present, attempt to load a small recent slice from mt5 as fallback
                if 'dxy_bars' not in globals():
                    try:
                        import MetaTrader5 as mt5
                        d_res = CONFIG.get("SYMBOL_MAP", {}).get("DXY", "DXY")
                        dxy_raw = mt5.copy_rates_from_pos(d_res if d_res else "DXY", mt5.TIMEFRAME_M30, 0, len(bars))
                        dxy_bars = list(dxy_raw) if dxy_raw is not None else []
                    except Exception:
                        dxy_bars = []
                # get aligned DXY recent slice at this index (use same i)
                if 'dxy_bars' in globals() and dxy_bars and len(dxy_bars) > i:
                    start_idx = max(0, i - 60)
                    dxy_recent = []
                    for r in dxy_bars[start_idx:i+1]:
                        dxy_recent.append({"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])})
                    try:
                        dxy_price = float(dxy_bars[i][4])
                        if v15_module and hasattr(v15_module, "compute_signal"):
                            dxy_signal = v15_module.compute_signal("DXY", dxy_price, {"bars": dxy_recent})
                        elif v15_module and hasattr(v15_module, "signal_to_side"):
                            dxy_signal = v15_module.signal_to_side("DXY", dxy_price)
                    except Exception:
                        dxy_signal = None
                else:
                    dxy_signal = None
            except Exception:
                dxy_signal = None

        # decide whether to suppress XAU entry due to DXY
        if dxy_thresh is not None and dxy_signal is not None:
            # buy requires DXY bearish (negative enough), sell requires DXY bullish
            if signal is not None and signal > 0 and not (dxy_signal <= -abs(dxy_thresh)):
                dxy_gate_block = True
            if signal is not None and signal < 0 and not (dxy_signal >= abs(dxy_thresh)):
                dxy_gate_block = True
        # when dxy_gate_block is True we will skip entry (treat as suppressed)
        # --- end DXY gate ---


        if signal is None:
            continue

        # ENTRY
        try:
            params = CONFIG.get("BACKTEST_PARAMS", {}).get(symbol, {})
        except Exception:
            params = {}
        signal_thresh = params.get("signal_thresh", 0.50) # default old behavior
        atr_thresh = params.get("atr_thresh", 0.0)
        max_hold = params.get("max_hold", 60)

        if i == 0: # only log at first bar for this symbol to avoid spam
            logger.info("Using params for %s -> signal_thresh=%s atr_thresh=%s max_hold=%s", symbol, signal_thresh, atr_thresh, max_hold)

        # ENTRY using per-symbol threshold
        if position is None and not suppressed_for_volatility and signal is not None and (not ('dxy_gate_block' in locals() and dxy_gate_block)):
                if signal >= signal_thresh:
                    position = 'buy'
                    entry_price = price
                    entry_index = i
# compute SL/TP for XAU with absolute cap (insert where you compute sl_price/tp_price)
                    params = CONFIG.get("BACKTEST_PARAMS", {}).get(symbol, {})
                    sl_mult = params.get("sl_atr_mult", None)
                    tp_mult = params.get("tp_atr_mult", None)
                    max_loss_abs = params.get("max_loss_abs", None)
                    
                    # compute ATR from recent_for_ctx (reuse your helper or inline)
                    atr_val = _compute_atr_from_recent(recent_for_ctx)  # use existing helper; if not present reimplement as before
                    
                    sl_price = None
                    tp_price = None
                    if sl_mult is not None or tp_mult is not None:
                        # --- ENSURE trade_type is defined (defensive) ---
                        # prefer existing local trade_type, then 'side', else derive from signal (fallback)
                        try:
                            _trade_type = locals().get("trade_type", None)
                            if _trade_type is None:
                                _trade_type = locals().get("side", None)
                            if _trade_type is None:
                                # signal may be None or numeric; treat None as sell (conservative)
                                _trade_type = "buy" if (signal is not None and float(signal) > 0) else "sell"
                        except Exception:
                            _trade_type = "buy"
                        trade_type = _trade_type

                        # --- SAFELY determine sl_mult, tp_mult (from params if present) ---
                        _sl_mult = None
                        _tp_mult = None
                        try:
                            if "params" in locals() and isinstance(params, dict):
                                _sl_mult = params.get("sl_mult", params.get("sl", None))
                                _tp_mult = params.get("tp_mult", params.get("tp", None))
                        except Exception:
                            pass
                        _sl_mult = (_sl_mult if (_sl_mult is not None) else locals().get("sl_mult", 4.0))
                        _tp_mult = (_tp_mult if (_tp_mult is not None) else locals().get("tp_mult", 6.0))
                        try:
                            _sl_mult = float(_sl_mult)
                        except Exception:
                            _sl_mult = 4.0
                        try:
                            _tp_mult = float(_tp_mult)
                        except Exception:
                            _tp_mult = 6.0

                        # --- Compute SL / TP using ATR when available, otherwise fallback to small % ---
                        _sl_price = None
                        _tp_price = None
                        _atr_for_entry = None
                        try:
                            _atr_for_entry = float(atr_val) if ("atr_val" in locals() and atr_val is not None) else 0.0
                        except Exception:
                            _atr_for_entry = 0.0

                        if _atr_for_entry > 0.0:
                            if trade_type == "buy":
                                _sl_price = price - (_atr_for_entry * _sl_mult)
                                _tp_price = price + (_atr_for_entry * _tp_mult)
                            else:
                                _sl_price = price + (_atr_for_entry * _sl_mult)
                                _tp_price = price - (_atr_for_entry * _tp_mult)
                        else:
                            # fallback: percent-based SL/TP (use params.sl_pct if present)
                            pct = 0.01
                            try:
                                if "params" in locals() and isinstance(params, dict):
                                    pct = float(params.get("sl_pct", pct))
                            except Exception:
                                pct = 0.01
                            if trade_type == "buy":
                                _sl_price = price * (1.0 - pct)
                                _tp_price = price * (1.0 + pct * _tp_mult)
                            else:
                                _sl_price = price * (1.0 + pct)
                                _tp_price = price * (1.0 - pct * _tp_mult)

                        # expose final names used later
                        sl_price = _sl_price
                        tp_price = _tp_price
                        atr_at_entry = _atr_for_entry
                        # --- end defensive SL/TP + trade_type block ---
# --- end SL/TP with absolute cap
                elif signal <= -signal_thresh:
                    position = 'sell'
                    entry_price = price
                    entry_index = i
                    trades.append({'type': 'sell', 'entry': price, 'time': bar['time'], 'atr_at_entry': compute_atr(recent) if 'compute_atr' in globals() else None})

        # --- check SL hit (insert before regular exit logic) ---
        if position and trades:
            last_trade = trades[-1]
            slp = last_trade.get('sl_price', None)
            if slp is not None:
                if position == 'buy' and price <= slp:
                    # SL hit — close now
                    exit_price = price
                    t = trades[-1]; t.update({'exit': exit_price, 'exit_time': bar['time'], 'exit_reason': 'SL'})
                    pnl = exit_price - t['entry']
                    t['pnl'] = pnl
                    if pnl > 0: wins += 1
                    else: losses += 1
                    position = None; entry_price = None; entry_index = None
                    # continue to next bar
                    continue
                elif position == 'sell' and price >= slp:
                    exit_price = price
                    t = trades[-1]; t.update({'exit': exit_price, 'exit_time': bar['time'], 'exit_reason': 'SL'})
                    pnl = t['entry'] - exit_price
                    t['pnl'] = pnl
                    if pnl > 0: wins += 1
                    else: losses += 1
                    position = None; entry_price = None; entry_index = None
                    continue
        # --- end SL check ---
        # EXIT: keep previous logic but use max_hold
        elif position:
            # --- defensive handling: avoid IndexError when trades list is empty ---
            if not trades:
                # No trades yet — nothing to refer to. Skip safely.
                t = None
            else:
                t = trades[-1]

            # Example usage (if the original code used t['entry'] etc):
            if t is None:
                # nothing to update/close/inspect — skip or set defaults
                pass
            else:
                # calculate how long we've held (in bars)
                held_bars = i - (entry_index if entry_index is not None else i)
                if held_bars >= max_hold or (signal is not None and abs(signal) < 0.2):
                    exit_price = price
                    t = trades[-1]
                    t.update({'exit': exit_price, 'exit_time': bar['time']})
                    pnl = (exit_price - t['entry']) if t['type'] == 'buy' else (t['entry'] - exit_price)
                    t['pnl'] = pnl
                    if pnl > 0:
                        wins += 1
                    else:
                        losses += 1
                    position = None
                    entry_price = None
                    entry_index = None

    net = sum(t.get('pnl', 0) for t in trades)

    num = len(trades)

    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0

    max_dd = 0

    with open("KYOTO_V16_BACKTEST_REPORT.csv", "w", newline="") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=["time","type","entry","exit","pnl","exit_time","atr_at_entry"]
        )

        writer.writeheader()

        for t in trades:
            writer.writerow(t)

    logger.info(
        "Backtest complete: net=%s trades=%s win_rate=%s",
        net,
        num,
        win_rate
    )

    return {
        "net": net,
        "trades": num,
        "win_rate": win_rate,
        "max_dd": max_dd
    }

# --- V16 UPGRADE: Ensure start_bot runs when executed as script (fixed main loop issue) ---

# --- V16 UPGRADE: auto symbol mapping for brokers (Exness style) ---
def auto_map_symbols(mt5_module, symbols):
    """Map canonical symbols to broker-specific variants (tries Exness style suffixes)."""
    _symbol_map = {}
    variants = [lambda s: s, lambda s: s + "m", lambda s: s + ".m", lambda s: s + "pro", lambda s: s + ".pro"]
    for s in symbols:
        mapped = None
        for f in variants:
            candidate = f(s)
            try:
                if mt5_module:
                    try:
                        info = mt5_module.symbol_info(candidate)
                        if info is not None:
                            mapped = candidate
                            break
                    except Exception:
                        # some MT5 installs expose symbol_info as function returning None/False
                        try:
                            info2 = mt5_module.symbol_select(candidate, True)
                            if info2:
                                mapped = candidate
                                break
                        except Exception:
                            pass
                else:
                    # No MT5 available: prefer Exness style 'm' suffix for common symbols
                    if candidate.endswith("m") or candidate.endswith(".m") or candidate.endswith("pro") or candidate.endswith(".pro"):
                        mapped = candidate
                        break
            except Exception:
                continue
        if mapped is None:
            logger.warning("Symbol auto-mapping failed for %s, using canonical", s)
            mapped = s
        else:
            logger.info("Mapped %s -> %s", s, mapped)
        _symbol_map[s] = mapped
    return _symbol_map



def run_telethon_news_listener():
    """Real background Telethon listener. Connects in a daemon thread and logs incoming messages.
    If credentials are missing, it warns and exits cleanly without crashing the bot.
    """
    try:
        api_id = CONFIG.get("TELEGRAM_API_ID") or os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID")
        api_hash = CONFIG.get("TELEGRAM_API_HASH") or os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH")
        session_name = CONFIG.get("TELEGRAM_SESSION", "kyoto_telethon")
    except Exception:
        api_id = os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH")
        session_name = "kyoto_telethon"

    if not api_id or not api_hash:
        logger.warning("Telethon credentials missing; listener disabled.")
        return

    try:
        api_id = int(api_id)
    except Exception:
        logger.warning("Telethon api_id is not a valid integer; listener disabled.")
        return

    try:
        from telethon import TelegramClient, events
    except Exception as e:
        logger.warning("Telethon not available: %s", e)
        return

    async def _main():
        client = TelegramClient(session_name, api_id, api_hash)
        await client.start()
        logger.info("Telethon listener connected.")

        @client.on(events.NewMessage)
        async def _on_message(event):
            try:
                text = event.raw_text or ""
                msg = {
                    "chat_id": getattr(event, "chat_id", None),
                    "date": getattr(getattr(event, "message", None), "date", None),
                    "text": text,
                }
                globals().setdefault("_telethon_messages", []).append(msg)
                handler = globals().get("handle_telegram_message") or globals().get("process_telegram_message") or globals().get("on_telegram_message")
                if callable(handler):
                    try:
                        handler(msg)
                    except TypeError:
                        handler(text)
                logger.info("[telethon-news-listener] %s", text[:300].replace("\n", " "))
            except Exception:
                logger.exception("Telethon message handler error")

        await client.run_until_disconnected()

    def _runner():
        try:
            import asyncio as _asyncio
            _asyncio.run(_main())
        except Exception:
            logger.exception("Telethon listener crashed")
    threading.Thread(target=_runner, name="telethon-news-listener", daemon=True).start()


def signal_scanner_loop():
    # Production scanner entrypoint that routes through the live MT5 scanner.
    # No hardcoded signal fallbacks are used here.
    logger.info("signal_scanner_loop routing through live_scanner_loop")
    mt5_module = None
    try:
        mt5_module, mt5_ok = mt5_init()
    except Exception:
        logger.exception("MT5 initialization failed in signal_scanner_loop")
        return
    if not mt5_module or not mt5_ok:
        logger.warning("MT5 unavailable; live scanner will not start.")
        return

    try:
        watch = CONFIG.get("WATCH_SYMBOLS", ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL"])
        symbol_map = auto_map_symbols(mt5_module, watch)
    except Exception:
        logger.exception("Symbol mapping failed in signal_scanner_loop")
        symbol_map = {}

    try:
        V15 = load_v15_module()
        V15 = _install_v15_compute_signal_adapter(V15)
    except Exception:
        logger.exception("Failed to load V15 module in signal_scanner_loop")
        V15 = None

    stop_event = threading.Event()
    live_scanner_loop(stop_event, mt5_module, symbol_map, V15)

# --- V16 UPGRADE: orchestrator start_bot ---
def start_bot():
    """Main orchestrator: initializes MT5, loads V15 module, starts background threads."""
    symbols = CONFIG.get("WATCH_SYMBOLS", [])
    try:
        symbols_text = " ".join(map(str, symbols))
    except Exception:
        symbols_text = "BTCUSD EURUSD USDJPY XAUUSD USOIL"

    logger.info("Session Filter")
    logger.info("Weekend Protection")
    logger.info("News Impact Predictor Ready")
    logger.info("Market Microstructure Engine Ready")
    logger.info("Routing all live symbol work through live_scanner_loop")
    logger.info("Watching symbols: %s", symbols_text)

    try:
        for sym in symbols:
            logger.info("Configured symbol: %s", sym)
    except Exception:
        pass

    # load V15 safely
    try:
        V15 = load_v15_module()
        # ensure v15 provides compute_signal (adapter will be installed if missing)
        V15 = _install_v15_compute_signal_adapter(V15)
    except Exception as e:
        logger.exception("Failed to load embedded V15 module in start_bot: %s", e)
        V15 = None

    # init MT5
    try:
        mt5_module, mt5_ok = mt5_init()
    except Exception as e:
        logger.exception("mt5_init failed in start_bot: %s", e)
        mt5_module, mt5_ok = (None, False)

    # create symbol map
    try:
        symbol_map = auto_map_symbols(mt5_module, CONFIG.get("WATCH_SYMBOLS", []))
    except Exception as e:
        logger.exception("auto_map_symbols failed in start_bot: %s", e)
        symbol_map = {s: s for s in CONFIG.get("WATCH_SYMBOLS", [])}

    # stop event for threads
    stop_event = threading.Event()

    # start threads
    threads = []

    try:
        t_scanner = threading.Thread(target=_run_resilient_worker, args=("live-scanner", stop_event, live_scanner_loop, stop_event, mt5_module, symbol_map, V15), daemon=True, name="live-scanner")
        threads.append(t_scanner)
    except Exception:
        pass
    try:
        t_news = threading.Thread(target=_run_resilient_worker, args=("news-scheduler", stop_event, news_scheduler_thread, stop_event, mt5_module, symbol_map, V15), daemon=True, name="news-scheduler")
        threads.append(t_news)
    except Exception:
        pass
    try:
        t_health = threading.Thread(target=_run_resilient_worker, args=("health-monitor", stop_event, health_monitor_thread, stop_event, mt5_module, symbol_map), daemon=True, name="health-monitor")
        threads.append(t_health)
    except Exception:
        pass
    try:
        t_trail = threading.Thread(target=_run_resilient_worker, args=("trailing-manager", stop_event, trailing_manager_thread, stop_event, mt5_module, symbol_map), daemon=True, name="trailing-manager")
        threads.append(t_trail)
    except Exception:
        pass    # Real Telethon news listener (no placeholder fallback)
    try:
        t_tele = threading.Thread(target=_run_resilient_worker, args=("telethon-news-listener", stop_event, run_telethon_news_listener), daemon=True, name="telethon-news-listener")
        threads.append(t_tele)
    except Exception:
        pass

    # start all threads
    for t in threads:
        try:
            t.start()
            logger.info("Started background thread: %s", t.name)
        except Exception as e:
            logger.exception("Failed to start thread %s: %s", getattr(t, "name", str(t)), e)

    # main loop: monitor kill/pause files and keep process alive
    try:
        backoff = 1
        while True:
            try:
                # heartbeat emitted by health_monitor_thread; just check flags here
                if file_flag(CONFIG.get("KILL_TRADING_FILE")):
                    logger.critical("KILL_TRADING file detected - shutting down now.")
                    stop_event.set()
                    break
                if file_flag(CONFIG.get("PAUSE_TRADING_FILE")):
                    logger.warning("PAUSE_TRADING file present - trading paused.")
                time.sleep(1.0)
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received, shutting down.")
                stop_event.set()
                break
            except Exception as e:
                logger.exception("Exception in start_bot main loop: %s", e)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
    finally:
        logger.info("start_bot exiting, waiting for threads to terminate.")
        stop_event.set()
        time.sleep(0.5)

def get_price_for_symbol(mt5_module, symbol_map, canonical_symbol):
    """
    Return (price: float, source: str).
    Tries MT5 first using the mapped broker symbol; if no valid tick/price is available,
    falls back to yfinance (if available) using a small mapping table.
    """
    price = 0.0
    src = "none"
    mapped = symbol_map.get(canonical_symbol, canonical_symbol) if symbol_map else canonical_symbol
    # Try MT5 tick first
    try:
        if mt5_module:
            try:
                tick = mt5_module.symbol_info_tick(mapped)
            except Exception:
                tick = None
            if tick is not None:
                # prefer last, then ask, then bid
                last = getattr(tick, "last", None)
                ask = getattr(tick, "ask", None)
                bid = getattr(tick, "bid", None)
                cand = None
                for v in (last, (ask+bid)/2 if (ask and bid) else None, ask, bid):
                    try:
                        if v is not None and float(v) > 0:
                            cand = float(v)
                            break
                    except Exception:
                        continue
                if cand is not None:
                    return cand, "mt5"
    except Exception:
        pass

    # yfinance fallback (if available)
    try:
        import yfinance as yf
        ticker_map = {
            "US10Y": "^TNX",
            "BTCUSD": "BTC-USD",
            "XAUUSD": "XAUUSD=X",
            "EURUSD": "EURUSD=X",
            "USDJPY": "JPY=X",
            "USOIL": "CL=F",
            "DXY": "DX-Y.NYB"
        }
        tf = ticker_map.get(canonical_symbol)
        if tf is None:
            if canonical_symbol.endswith("USD"):
                tf = canonical_symbol.replace("USD", "-USD")
            else:
                tf = canonical_symbol
        try:
            t = yf.Ticker(tf)
            info = {}
            try:
                info = t.info or {}
            except Exception:
                info = {}
            price = info.get("regularMarketPrice", None)
            if price is None:
                try:
                    hist = t.history(period="1d", interval="1m")
                    if hist is not None and len(hist) > 0:
                        price = hist["Close"].dropna().iloc[-1]
                except Exception:
                    price = None
            if price is not None:
                try:
                    return float(price), "yfinance"
                except Exception:
                    pass
        except Exception:
            pass
    except Exception:
        # yfinance not installed
        pass

    # secondary MT5 attempt: try canonical symbol without suffix
    try:
        if mt5_module and mapped != canonical_symbol:
            try:
                tick2 = mt5_module.symbol_info_tick(canonical_symbol)
            except Exception:
                tick2 = None
            if tick2 is not None:
                last = getattr(tick2, "last", None)
                ask = getattr(tick2, "ask", None)
                bid = getattr(tick2, "bid", None)
                for v in (last, (ask+bid)/2 if (ask and bid) else None, ask, bid):
                    try:
                        if v is not None and float(v) > 0:
                            return float(v), "mt5-alt"
                    except Exception:
                        continue
    except Exception:
        pass

    return 0.0, "none"



def xau_macro_confirm(mt5_module, desired_side, xau_sym, dxy_sym, symbol_map=None):
    """
    Correlation-aware macro confirmation for XAU.
    - Computes correlation(XAU, DXY) and only applies DXY gating when corr <= CORRELATION_IGNORE_THRESHOLD
    - Samples short-window pre/post changes for DXY and US10Y
    - Requires BOTH DXY and US10Y to disagree before blocking when DXY gating is active
    - Falls back conservatively to allowing the trade if data is unavailable
    """
    try:
        if desired_side is None:
            return True
        side = str(desired_side).upper()
        if side not in ("BUY", "SELL"):
            return True
    except Exception:
        return True

    try:
        sym_map = symbol_map or {}
        xau_canon = "XAUUSD"
        dxy_canon = "DXY"
        us10y_canon = "US10Y"

        xau_mapped = sym_map.get(xau_canon, xau_sym or xau_canon)
        dxy_mapped = sym_map.get(dxy_canon, dxy_sym or dxy_canon)
        us10y_mapped = sym_map.get(us10y_canon, us10y_canon)

        def _recent_closes(mapped_symbol, count=50):
            if not mt5_module or not mapped_symbol:
                return []
            try:
                rates = mt5_module.copy_rates_from_pos(mapped_symbol, getattr(mt5_module, "TIMEFRAME_M30", mt5_module.TIMEFRAME_M30), 0, count)
            except Exception:
                rates = None
            if rates is None:
                return []
            closes = []
            try:
                for r in rates:
                    try:
                        closes.append(float(r[4]))
                    except Exception:
                        try:
                            closes.append(float(r.get("close", 0.0)))
                        except Exception:
                            continue
            except Exception:
                return []
            return closes

        corr = None
        try:
            x_closes = _recent_closes(xau_mapped, 60)
            d_closes = _recent_closes(dxy_mapped, 60)
            if x_closes and d_closes:
                corr = compute_correlation(x_closes[-50:], d_closes[-50:])
        except Exception:
            corr = None

        use_dxy = (corr is not None and corr <= CONFIG.get("CORRELATION_IGNORE_THRESHOLD", -0.3))

        def _pct_change(pre, post):
            try:
                if pre is None or post is None:
                    return None
                pre = float(pre)
                post = float(post)
                if pre == 0:
                    return None
                return (post - pre) / abs(pre)
            except Exception:
                return None

        dxy_change = None
        us_change = None

        try:
            dxy_pre, _ = get_price_for_symbol(mt5_module, sym_map, dxy_canon)
            time.sleep(0.5)
            dxy_post, _ = get_price_for_symbol(mt5_module, sym_map, dxy_canon)
            dxy_change = _pct_change(dxy_pre, dxy_post)
        except Exception:
            dxy_change = None

        try:
            us_pre, _ = get_price_for_symbol(mt5_module, sym_map, us10y_canon)
            time.sleep(0.5)
            us_post, _ = get_price_for_symbol(mt5_module, sym_map, us10y_canon)
            us_change = _pct_change(us_pre, us_post)
        except Exception:
            us_change = None

        if side == "BUY":
            dxy_confirms = (dxy_change is None or dxy_change < 0)
            us_confirms = (us_change is None or us_change < 0)
        else:
            dxy_confirms = (dxy_change is None or dxy_change > 0)
            us_confirms = (us_change is None or us_change > 0)

        if use_dxy:
            # block only when both confirm the opposite of the desired side
            if dxy_change is not None and us_change is not None:
                if (not dxy_confirms) and (not us_confirms):
                    return False
            return True

        # if DXY is not trusted due to correlation, rely on US10Y alone
        if us_change is not None and not us_confirms:
            return False
        return True

    except Exception:
        # conservative failure mode: do not block if helper itself fails
        return True


# Override the live scanner loop to use get_price_for_symbol safely.
def live_scanner_loop(stop_event, mt5_module, symbol_map, v15_module):
    import time as _time
    time = _time  # local guarantee so both time and _time resolve
    logger.info("Started background thread: live-scanner (patched)")

    if not hasattr(live_scanner_loop, "_logged_thresholds"):
        live_scanner_loop._logged_thresholds = set()
    if not hasattr(live_scanner_loop, "_last_symbol_log"):
        live_scanner_loop._last_symbol_log = {}
    if not hasattr(live_scanner_loop, "_last_no_tick_log"):
        live_scanner_loop._last_no_tick_log = {}
    if not hasattr(live_scanner_loop, "_last_gate_log"):
        live_scanner_loop._last_gate_log = {}

    try:
        watch = list(CONFIG.get("WATCH_SYMBOLS", []))
    except Exception:
        watch = []
    if not watch:
        try:
            watch = list(symbol_map.keys()) if symbol_map else []
        except Exception:
            watch = []

    status_interval = float(CONFIG.get("LIVE_STATUS_LOG_INTERVAL_SECONDS", 60))
    gate_interval = float(CONFIG.get("GATE_SUPPRESSION_LOG_INTERVAL_SECONDS", 60))
    loop_sleep = float(CONFIG.get("LIVE_SCAN_SLEEP_SECONDS", 1.0))
    error_sleep = float(CONFIG.get("LIVE_SCAN_ERROR_SLEEP_SECONDS", 1.0))

    while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
        try:
            for sym in watch:
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break

                canon = sym
                now = time.time()

                try:
                    price, source = get_price_for_symbol(mt5_module, symbol_map or {}, canon)
                except Exception:
                    price, source = 0.0, "err"

                # compute signal using v15 if available
                signal = None
                regime = "unknown"
                try:
                    if v15_module and hasattr(v15_module, "compute_signal"):
                        try:
                            signal = v15_module.compute_signal(canon, price, {}) if callable(v15_module.compute_signal) else None
                        except Exception:
                            signal = None
                    elif v15_module and hasattr(v15_module, "signal_to_side"):
                        try:
                            signal = v15_module.signal_to_side(canon, price)
                        except Exception:
                            signal = None
                except Exception:
                    signal = None

                if signal is None:
                    logger.info("Skipping %s: no deterministic signal from v15", sym)
                    continue

                try:
                    signal = float(signal)
                except Exception:
                    signal = 0.0

                params = CONFIG.get("BACKTEST_PARAMS", {}).get(sym, {})
                live_signal_thresh = float(params.get("signal_thresh", 0.50))

                # --- V16 UPGRADE: correlation-aware XAU macro gate ---
                if str(canon).upper().startswith("XAU"):
                    desired_side = "BUY" if signal > 0 else ("SELL" if signal < 0 else None)
                    try:
                        macro_ok = xau_macro_confirm(
                            mt5_module,
                            desired_side,
                            symbol_map.get("XAUUSD", "XAUUSDm") if symbol_map else "XAUUSDm",
                            symbol_map.get("DXY", "DXY") if symbol_map else "DXY",
                            symbol_map=symbol_map or {},
                        )
                    except Exception:
                        macro_ok = True
                    if not macro_ok:
                        regime = "macro_blocked"
                        last_gate = live_scanner_loop._last_gate_log.get(sym, 0.0)
                        if now - last_gate >= gate_interval:
                            logger.info(
                                "XAU entry suppressed by macro confirmation gate | sym=%s | signal=%.4f | thresh=%s",
                                sym,
                                signal,
                                live_signal_thresh,
                            )
                            live_scanner_loop._last_gate_log[sym] = now
                        continue
                # --- end V16 UPGRADE: correlation-aware XAU macro gate ---

                if signal >= live_signal_thresh:
                    regime = "trending"
                elif signal <= -live_signal_thresh:
                    regime = "trending"
                else:
                    regime = "ranging"

                # Attempt execution when AUTO_EXECUTE allows it.
                exec_res = None
                try:
                    exec_res = execute_signal(sym, signal, price, mt5_module, symbol_map or {})
                except Exception:
                    logger.exception("execute_signal call failed for %s", sym)
                if exec_res is not None:
                    logger.info("Execution result for %s: %s", sym, exec_res)

                # Status line: throttled per symbol so the logs show the whole watchlist.
                last_status = live_scanner_loop._last_symbol_log.get(sym, 0.0)
                if now - last_status >= status_interval:
                    logger.info(
                        "%s | price: %.6f | signal: %.4f | regime: %s | src: %s",
                        canon,
                        price,
                        signal,
                        regime,
                        source,
                    )
                    live_scanner_loop._last_symbol_log[sym] = now

        except Exception:
            logger.exception("Exception in patched live_scanner_loop")
            time.sleep(error_sleep)
            continue

        time.sleep(loop_sleep)

# Ensure telethon/news listener runs in background if present

# -----------------------------------------------------------------------------
# TELETHON BACKGROUND START (simple English):
# _ensure_telethon_thread() will try to start your telethon/news listener in a
# separate daemon thread without blocking the main trading loop.
# - It first checks globals() for an existing "_telethon_thread" to avoid double-start.
# - It then looks up the function "run_telethon_news_listener" dynamically via globals().
#   If that function isn't yet defined or isn't callable, the helper logs and returns safely.
# - If callable, it starts the function in a daemon thread and stores the Thread object
#   in globals()["_telethon_thread"] so subsequent calls won't re-start it.
# This prevents the NameError you previously saw because the function is looked up at
# runtime (not referenced as a bare name during module import).
# -----------------------------------------------------------------------------
def _ensure_telethon_thread():
    try:
        if globals().get("_telethon_thread"):
            return

        fn = globals().get("run_telethon_news_listener")

        if not callable(fn):
            logger.info("Telethon listener not started yet (function not loaded).")
            return

        t = threading.Thread(
            target=fn,
            daemon=True,
            name="telethon_listener_thread"
        )

        t.start()
        globals()["_telethon_thread"] = t

        logger.info("Telethon/news listener thread started (daemon).")

    except Exception:
        logger.exception("Failed to schedule telethon/news listener thread.")





# --- BEGIN KYOTO V18 RISK / SLTP TRANSPLANT ---
import os as _k_os, math as _k_math, statistics as _k_statistics, threading as _k_threading, contextlib as _k_contextlib, traceback as _k_traceback

if "GLOBAL_MAX_OPEN_TRADES" not in globals():
    GLOBAL_MAX_OPEN_TRADES = 8
if "MAX_OPEN_TRADES" not in globals():
    MAX_OPEN_TRADES = GLOBAL_MAX_OPEN_TRADES
if "SYMBOL_TRADE_LIMITS" not in globals():
    SYMBOL_TRADE_LIMITS = {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}
globals()["GLOBAL_MAX_OPEN_TRADES"] = int(GLOBAL_MAX_OPEN_TRADES)
globals()["MAX_OPEN_TRADES"] = int(MAX_OPEN_TRADES)
globals()["SYMBOL_TRADE_LIMITS"] = dict(SYMBOL_TRADE_LIMITS)

_kyoto_risk_ctx = _k_threading.local()

def _kyoto_ctx_set(**kwargs):
    _kyoto_risk_ctx.data = dict(kwargs)

def _kyoto_ctx_get():
    return getattr(_kyoto_risk_ctx, "data", None)

def _kyoto_ctx_clear():
    if hasattr(_kyoto_risk_ctx, "data"):
        delattr(_kyoto_risk_ctx, "data")

def _deprecated_allowed_to_open(symbol: str):
    try:
        s = str(symbol).upper()
        per = 0
        try:
            if "get_open_positions_count" in globals():
                per = int(get_open_positions_count(s) or 0)
        except Exception:
            per = 0
        total = 0
        try:
            if "count_open_positions" in globals():
                total, _per = count_open_positions()
                total = int(total or 0)
        except Exception:
            total = 0
        if total >= GLOBAL_MAX_OPEN_TRADES:
            return False, f"global_max_open_reached:{total}"
        limit = int(SYMBOL_TRADE_LIMITS.get(s, int(_k_os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
        if per >= limit:
            return False, f"symbol_limit_reached:{s}:{per}/{limit}"
        return True, "ok"
    except Exception:
        return False, "error"

def compute_position_risk(base_risk_pct, tech_score, fund_score, sent_score):
    try:
        base = float(base_risk_pct)
    except Exception:
        base = float(globals().get("BASE_RISK_PER_TRADE_PCT", 0.003))
    def _sgn(v):
        try:
            v = float(v)
        except Exception:
            return 0
        return 1 if v >= 0.01 else (-1 if v <= -0.01 else 0)
    s_tech, s_fund, s_sent = _sgn(tech_score), _sgn(fund_score), _sgn(sent_score)
    if s_tech != 0 and s_tech == s_fund == s_sent:
        mult = 1.2
    elif s_tech != 0 and s_tech == s_fund:
        mult = 1.1
    elif s_tech != 0 and s_tech == s_sent:
        mult = 1.05
    elif s_fund != 0 and s_tech != 0 and s_tech != s_fund:
        mult = 0.5
    else:
        mult = 1.0
    lo = float(globals().get("MIN_RISK_PER_TRADE_PCT", 0.002))
    hi = float(globals().get("MAX_RISK_PER_TRADE_PCT", 0.01))
    risk = max(lo, min(hi, base * mult))
    return float(risk), float(mult)

def regime_adaptive_stop(entry_price, df_h1, side, base_atr_multiplier=4.0):
    try:
        atr = None
        try:
            ind = add_technical_indicators(df_h1.copy())
            atr = float(ind["atr14"].iloc[-1])
        except Exception:
            highs = [float(x) for x in df_h1["high"].astype(float).values[-14:]]
            lows = [float(x) for x in df_h1["low"].astype(float).values[-14:]]
            closes = [float(x) for x in df_h1["close"].astype(float).values[-14:]]
            trs = [max(h - l, abs(h - c), abs(l - c)) for h, l, c in zip(highs, lows, closes)]
            atr = _k_statistics.mean(trs) if trs else 0.0001
        regime = classify_macro_regime(None, df_h1)
        is_spike, vscore = volatility_clustering(df_h1)
        mult = float(base_atr_multiplier)
        if regime == "volatile":
            mult *= 1.6
        elif regime == "quiet":
            mult *= 0.9
        if is_spike:
            mult *= 1.4
        mult = max(0.5, min(4.0, mult))
        stop_dist = max(1e-6, float(atr) * mult)
        if str(side).upper() == "BUY":
            return float(entry_price - stop_dist), float(entry_price + stop_dist * 6.0), float(stop_dist)
        return float(entry_price + stop_dist), float(entry_price - stop_dist * 6.0), float(stop_dist)
    except Exception:
        sd = 0.01 * float(entry_price) if entry_price else 0.01
        if str(side).upper() == "BUY":
            return float(entry_price - sd), float(entry_price + sd * 6.0), float(sd)
        return float(entry_price + sd), float(entry_price - sd * 6.0), float(sd)

def ai_signal_quality(symbol, tech_score, fund_score, sent_score, df_h1):
    try:
        agree = 1.0 - (abs(float(tech_score) - float(fund_score)) + abs(float(tech_score) - float(sent_score)) + abs(float(fund_score) - float(sent_score))) / 6.0
        agree = max(0.0, min(1.0, agree))
        shock, shock_score = detect_news_shock(symbol)
        shock_penalty = min(0.75, _k_math.log1p(shock_score) / 5.0) if shock else 0.0
        liq = liquidity_heatmap_score(df_h1)
        ofi = abs(order_flow_imbalance(df_h1))
        vspike, vscore = volatility_clustering(df_h1)
        vpenalty = min(0.5, (vscore - 1.0) / 5.0) if vscore > 1.0 else 0.0
        quality = (0.45 * agree) + (0.15 * liq) + (0.10 * (1 - shock_penalty)) + (0.15 * (1 - vpenalty)) + (0.15 * (1 - ofi))
        return float(max(0.0, min(1.0, quality)))
    except Exception:
        return 0.0

def _kyoto_h1_df(mt5_module, symbol_map, symbol, bars=160):
    try:
        if mt5_module is None:
            return None
        mapped = symbol_map.get(symbol, symbol) if symbol_map else symbol
        tf = getattr(mt5_module, "TIMEFRAME_H1", 60)
        rates = mt5_module.copy_rates_from_pos(mapped, tf, 0, int(bars))
        if rates is None or len(rates) == 0:
            return None
        df = pd.DataFrame(rates)
        if "time" in df.columns:
            df.index = pd.to_datetime(df["time"], unit="s")
        if "tick_volume" in df.columns:
            df["volume"] = df["tick_volume"]
        elif "real_volume" in df.columns:
            df["volume"] = df["real_volume"]
        return df[[c for c in ("open","high","low","close","volume") if c in df.columns]].dropna(how="all")
    except Exception:
        return None

if "compute_lots_from_risk" in globals() and "_KYOTO_ORIG_compute_lots_from_risk" not in globals():
    _KYOTO_ORIG_compute_lots_from_risk = compute_lots_from_risk
    def compute_lots_from_risk(risk_pct, balance, entry_price, stop_price):
        ctx = _kyoto_ctx_get() or {}
        try:
            dyn_risk, mult = compute_position_risk(risk_pct, ctx.get("tech", 0.0), ctx.get("fund", 0.0), ctx.get("sent", 0.0))
            if ctx.get("regime") == "volatile":
                dyn_risk *= 0.6
            elif ctx.get("regime") == "quiet":
                dyn_risk *= 1.15
            risk_pct = max(float(globals().get("MIN_RISK_PER_TRADE_PCT", 0.002)), min(float(globals().get("MAX_RISK_PER_TRADE_PCT", 0.01)), dyn_risk))
        except Exception:
            pass
        return _KYOTO_ORIG_compute_lots_from_risk(risk_pct, balance, entry_price, stop_price)

if "place_order_mt5" in globals() and "_KYOTO_ORIG_place_order_mt5" not in globals():
    _KYOTO_ORIG_place_order_mt5 = place_order_mt5
    def place_order_mt5(symbol, action, lot, price, sl, tp):
        ctx = _kyoto_ctx_get() or {}
        try:
            if ctx.get("allowed") is False:
                return {"status": "skipped", "comment": ctx.get("reason", "risk_gate"), "symbol": symbol}
            if ctx.get("quality") is not None and float(ctx.get("quality", 0.0)) < 0.35:
                return {"status": "skipped", "comment": "quality_below_threshold", "symbol": symbol}
            if ctx.get("regime") in ("ranging", "sideways", "choppy"):
                return {"status": "skipped", "comment": f"regime_{ctx.get('regime')}", "symbol": symbol}
            if ctx.get("df_h1") is not None:
                side = "BUY" if str(action).lower() in ("buy", "long", "0", "1") else "SELL"
                try:
                    calc_sl, calc_tp, _sd = regime_adaptive_stop(float(price or ctx.get("entry") or 0.0), ctx["df_h1"], side)
                    if not sl:
                        sl = calc_sl
                    if not tp:
                        tp = calc_tp
                except Exception:
                    pass
            if (sl is None or float(sl) == 0.0 or tp is None or float(tp) == 0.0) and price is not None:
                px = float(price)
                sd = max(1e-6, abs(px) * 0.005)
                if str(action).lower() in ("buy", "long", "0", "1"):
                    sl = px - sd
                    tp = px + sd * 1.5
                else:
                    sl = px + sd
                    tp = px - sd * 1.5
        except Exception:
            pass
        return _KYOTO_ORIG_place_order_mt5(symbol, action, lot, price, sl, tp)

if "order_wrapper" in globals() and "_KYOTO_ORIG_order_wrapper" not in globals():
    _KYOTO_ORIG_order_wrapper = order_wrapper
    def order_wrapper(mt5_module, order_request):
        req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
        ctx = _kyoto_ctx_get() or {}
        try:
            if ctx.get("allowed") is False:
                return {"retcode": -1, "comment": ctx.get("reason", "risk_gate"), "request": req}
            if ctx.get("quality") is not None and float(ctx.get("quality", 0.0)) < 0.35:
                return {"retcode": -1, "comment": "quality_below_threshold", "request": req}
            if ctx.get("regime") in ("ranging", "sideways", "choppy"):
                return {"retcode": -1, "comment": f"regime_{ctx.get('regime')}", "request": req}
            if ctx.get("df_h1") is not None:
                side = "BUY" if str(req.get("type", req.get("side", ""))).lower() in ("buy", "long", "0", "1") else "SELL"
                try:
                    calc_sl, calc_tp, _sd = regime_adaptive_stop(float(req.get("price") or ctx.get("entry") or 0.0), ctx["df_h1"], side)
                    if not req.get("sl"):
                        req["sl"] = calc_sl
                    if not req.get("tp"):
                        req["tp"] = calc_tp
                except Exception:
                    pass
            if (req.get("sl") in (None, 0, 0.0, "")) or (req.get("tp") in (None, 0, 0.0, "")):
                px = float(req.get("price") or ctx.get("entry") or 0.0)
                if px > 0:
                    sd = max(1e-6, abs(px) * 0.005)
                    if str(req.get("type", req.get("side", ""))).lower() in ("buy", "long", "0", "1"):
                        req["sl"] = px - sd
                        req["tp"] = px + sd * 1.5
                    else:
                        req["sl"] = px + sd
                        req["tp"] = px - sd * 1.5
        except Exception:
            pass
        return _KYOTO_ORIG_order_wrapper(mt5_module, req)

if "execute_signal" in globals() and "_KYOTO_ORIG_execute_signal" not in globals():
    _KYOTO_ORIG_execute_signal = execute_signal
    def execute_signal(sym, signal, price, mt5_module, symbol_map):
        try:
            threshold = max(float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.40)), 0.60)
            if str(sym).upper().startswith(("DXY", "US10Y")):
                return _KYOTO_ORIG_execute_signal(sym, signal, price, mt5_module, symbol_map)
            if signal is None or abs(float(signal)) < threshold:
                return None
            df_h1 = _kyoto_h1_df(mt5_module, symbol_map or {}, sym, 160)
            regime = detect_market_regime_from_h1(df_h1)[0] if df_h1 is not None else "unknown"
            allowed, reason = allowed_to_open(sym)
            ctx = {
                "symbol": sym,
                "signal": float(signal),
                "quality": min(1.0, abs(float(signal))),
                "regime": regime,
                "allowed": allowed,
                "reason": reason,
                "df_h1": df_h1,
                "entry": float(price or 0.0),
                "tech": float(signal),
                "fund": float(get_fused_score(sym)) if "get_fused_score" in globals() else 0.0,
                "sent": float(get_news_impact_score(sym)) if "get_news_impact_score" in globals() else 0.0,
            }
            _kyoto_ctx_set(**ctx)
            try:
                return _KYOTO_ORIG_execute_signal(sym, signal, price, mt5_module, symbol_map)
            finally:
                _kyoto_ctx_clear()
        except Exception:
            _kyoto_ctx_clear()
            return _KYOTO_ORIG_execute_signal(sym, signal, price, mt5_module, symbol_map)

if "make_decision_for_symbol" in globals() and "_KYOTO_ORIG_make_decision_for_symbol" not in globals():
    _KYOTO_ORIG_make_decision_for_symbol = make_decision_for_symbol
    def make_decision_for_symbol(symbol: str, live: bool=False):
        try:
            tfs = fetch_multi_timeframes(symbol, period_days=60)
            df_h1 = tfs.get("H1") if isinstance(tfs, dict) else None
            scores = aggregate_multi_tf_scores(tfs) if isinstance(tfs, dict) else {"tech": 0.0, "model": 0.0}
            tech = float(scores.get("tech", 0.0) or 0.0)
            fund = float(get_fused_score(symbol)) if "get_fused_score" in globals() else float(fetch_fundamental_score(symbol)) if "fetch_fundamental_score" in globals() else 0.0
            sent = float(get_news_impact_score(symbol)) if "get_news_impact_score" in globals() else 0.0
            regime = detect_market_regime_from_h1(df_h1)[0] if df_h1 is not None else "unknown"
            quality = ai_signal_quality(symbol, tech, fund, sent, df_h1) if df_h1 is not None else 0.0
            allowed, reason = allowed_to_open(symbol)
            _kyoto_ctx_set(symbol=symbol, df_h1=df_h1, regime=regime, quality=quality, allowed=allowed, reason=reason, tech=tech, fund=fund, sent=sent, entry=float(df_h1["close"].iloc[-1]) if df_h1 is not None and not getattr(df_h1, "empty", True) else None)
            try:
                return _KYOTO_ORIG_make_decision_for_symbol(symbol, live)
            finally:
                _kyoto_ctx_clear()
        except Exception:
            _kyoto_ctx_clear()
            return _KYOTO_ORIG_make_decision_for_symbol(symbol, live)

# tighten the scanner threshold at runtime for strong signals only
try:
    if "CONFIG" in globals():
        CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = max(float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.30)), 0.30)
except Exception:
    pass
# --- END KYOTO V18 RISK / SLTP TRANSPLANT ---


# --- FINAL KYOTO ENFORCEMENT PATCH (single source of truth) ---
# Keep the intended thresholds and limits from CONFIG / SYMBOL_TRADE_LIMITS,
# and override any earlier MTF/H1-heavy or threshold-bumping duplicates.
try:
    CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.30))
except Exception:
    pass

try:
    GLOBAL_MAX_OPEN_TRADES = int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8))
except Exception:
    GLOBAL_MAX_OPEN_TRADES = 8

try:
    SYMBOL_TRADE_LIMITS = dict(globals().get("SYMBOL_TRADE_LIMITS", {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}))
except Exception:
    SYMBOL_TRADE_LIMITS = {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}


def _deprecated_allowed_to_open(symbol: str):
    """Final live enforcement for open-trade limits."""
    try:
        s = str(symbol).upper()
        if s.startswith(("DXY", "US10Y")):
            return False, "macro_filter_symbol_only"

        total = 0
        per = 0
        try:
            if callable(globals().get("count_open_positions")):
                total, per_map = count_open_positions()
                total = int(total or 0)
                if isinstance(per_map, dict):
                    per = int(per_map.get(s, 0) or 0)
        except Exception:
            pass
        try:
            if callable(globals().get("get_open_positions_count")):
                per = max(per, int(get_open_positions_count(s) or 0))
        except Exception:
            pass

        if total >= int(GLOBAL_MAX_OPEN_TRADES):
            return False, f"global_max_open_reached:{total}"

        limit = int(SYMBOL_TRADE_LIMITS.get(s, int(os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
        if per >= limit:
            return False, f"symbol_limit_reached:{s}:{per}/{limit}"

        return True, "ok"
    except Exception:
        logger.exception("allowed_to_open failed for %s", symbol)
        return False, "error"


def _deprecated_order_wrapper(mt5_module, order_request):
    """Final order wrapper: enforce SL/TP, preserve broker safety, and never crash on None."""
    try:
        ctx = _kyoto_ctx_get() or {}
        req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})

        # Keep macro-filter symbols out of execution.
        sym = str(req.get("symbol") or req.get("instrument") or ctx.get("symbol") or "").upper()
        if sym.startswith(("DXY", "US10Y")):
            return {"retcode": -1, "comment": "MACRO_FILTER_SYMBOL_ONLY", "request": req}

        # Enforce open limits again at order stage.
        if ctx.get("allowed") is False:
            return {"retcode": -1, "comment": ctx.get("reason", "risk_gate"), "request": req}
        if sym:
            ok, reason = allowed_to_open(sym)
            if not ok:
                return {"retcode": -1, "comment": reason, "request": req}

        # Ensure SL/TP are always present.
        try:
            sl = req.get("sl")
            tp = req.get("tp")
            if (sl in (None, 0, 0.0, "")) or (tp in (None, 0, 0.0, "")):
                px = float(req.get("price") or ctx.get("entry") or 0.0)
                if px > 0:
                    sd = max(1e-6, abs(px) * 0.005)
                    side = str(req.get("type", req.get("side", ""))).lower()
                    if side in ("buy", "long", "0", "1"):
                        req["sl"] = px - sd
                        req["tp"] = px + sd * 1.5
                    else:
                        req["sl"] = px + sd
                        req["tp"] = px - sd * 1.5
        except Exception:
            pass

        # Delegate to the original safe MT5 wrapper.
        return _KYOTO_ORIG_order_wrapper(mt5_module, req)
    except Exception as e:
        logger.exception("Final order_wrapper failed: %s", e)
        return {"retcode": -1, "comment": str(e)}


def _deprecated_execute_signal(sym, signal, price, mt5_module, symbol_map):
    """Final live execution gate: strong signal only, symbol threshold + execution threshold, no MTF dependency."""
    try:
        sym_u = str(sym).upper()
        if sym_u.startswith(("DXY", "US10Y")):
            logger.info("Execution skipped for %s: macro filter symbol only", sym)
            return None
        if not globals().get("AUTO_EXECUTE", True):
            logger.info("Execution skipped for %s: AUTO_EXECUTE disabled", sym)
            return None
        if CONFIG.get("DRY_RUN") or CONFIG.get("DRY_RUN_FLAG"):
            logger.info("Execution skipped for %s: DRY_RUN active", sym)
            return None
        if signal is None:
            logger.info("Execution skipped for %s: signal is None", sym)
            return None

        try:
            signal = float(signal)
        except Exception:
            logger.info("Execution skipped for %s: invalid signal", sym)
            return None

        params = CONFIG.get("BACKTEST_PARAMS", {}).get(sym_u, CONFIG.get("BACKTEST_PARAMS", {}).get(sym, {}))
        symbol_thresh = float(params.get("signal_thresh", CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.50)))
        exec_thresh = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.50))
        threshold = max(symbol_thresh, exec_thresh)

        if abs(signal) < threshold:
            logger.info(
                "Execution skipped for %s: signal below execution threshold (%.3f) signal=%.4f",
                sym, threshold, signal,
            )
            return None

        allowed, reason = allowed_to_open(sym_u)
        if not allowed:
            logger.info("Execution skipped for %s: %s", sym, reason)
            return None

        side = "buy" if signal > 0 else "sell"
        mapped = symbol_map.get(sym, sym) if symbol_map else sym
        volume = float(CONFIG.get("DEFAULT_ORDER_VOLUME", 0.01))
        req = {"symbol": mapped, "volume": volume, "type": side, "price": float(price or 0.0)}

        # Preserve a context for the wrapper without requiring H1/MTF data.
        try:
            _kyoto_ctx_set(
                symbol=sym_u,
                signal=signal,
                quality=min(1.0, abs(signal)),
                regime="trending",
                allowed=allowed,
                reason=reason,
                entry=float(price or 0.0),
                tech=signal,
                fund=float(get_fused_score(sym_u)) if "get_fused_score" in globals() else 0.0,
                sent=float(get_news_impact_score(sym_u)) if "get_news_impact_score" in globals() else 0.0,
                bars=globals().get("_KYOTO_LAST_RECENT_FOR_CTX_BY_SYMBOL", {}).get(sym_u),
            )
        except Exception:
            pass

        try:
            logger.info(
                "Attempting execution for %s: side=%s vol=%s price=%.6f signal=%.4f threshold=%.3f",
                sym, side, volume, float(price or 0.0), signal, threshold,
            )
            res = order_wrapper(mt5_module, req)
        finally:
            try:
                _kyoto_ctx_clear()
            except Exception:
                pass

        # Surface broker-side / spread-side rejections clearly.
        try:
            comment = None
            if isinstance(res, dict):
                comment = res.get("comment") or res.get("retcode")
            else:
                comment = getattr(res, "comment", None) or getattr(res, "retcode", None)
            if comment:
                c = str(comment)
                if "SPREAD_TOO_HIGH" in c:
                    logger.info("Skipped trade for %s because spread was too high", sym)
                elif "KILLED" in c:
                    logger.info("Skipped trade for %s because trading is killed by flag", sym)
                elif "PAUSED" in c:
                    logger.info("Skipped trade for %s because trading is paused by flag", sym)
                elif "NO_ACCOUNT" in c:
                    logger.info("Skipped trade for %s because MT5 account info is unavailable", sym)
                elif "MACRO_FILTER_SYMBOL_ONLY" in c:
                    logger.info("Execution skipped for %s: macro filter symbol only", sym)
        except Exception:
            pass

        logger.info("Execution result for %s: %s", sym, res)
        return res
    except Exception:
        logger.exception("execute_signal failed for %s", sym)
        try:
            _kyoto_ctx_clear()
        except Exception:
            pass
        return None

# Keep the final execution gate in the config at the intended value.
try:
    CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = 0.30
except Exception:
    pass

# --- END FINAL KYOTO ENFORCEMENT PATCH ---

# --- FINAL LIMIT ENFORCEMENT OVERRIDE (single source of truth) ---
try:
    GLOBAL_MAX_OPEN_TRADES = int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8))
except Exception:
    GLOBAL_MAX_OPEN_TRADES = 8

try:
    SYMBOL_TRADE_LIMITS = dict(globals().get(
        "SYMBOL_TRADE_LIMITS",
        {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}
    ))
except Exception:
    SYMBOL_TRADE_LIMITS = {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}

def _kyoto_broker_symbol_for_count(symbol: str) -> str:
    try:
        s = str(symbol).upper()
    except Exception:
        s = str(symbol)
    try:
        fn = globals().get("map_symbol_to_broker")
        if callable(fn):
            mapped = fn(s)
            if mapped:
                return str(mapped)
    except Exception:
        pass
    return s

def _kyoto_count_open_symbol(symbol: str) -> int:
    """
    Best-effort live count for a single symbol using MT5 first, then existing helpers.
    This is intentionally strict: if anything is unclear, it returns the current best count,
    and order execution will be blocked by allowed_to_open().
    """
    s = str(symbol).upper()
    broker = _kyoto_broker_symbol_for_count(s)

    # 1) MT5 direct count
    try:
        mt5_mod = globals().get("_mt5")
        if globals().get("MT5_AVAILABLE") and globals().get("_mt5_connected") and mt5_mod is not None:
            try:
                positions = mt5_mod.positions_get(symbol=broker) or []
                if positions:
                    return len(positions)
            except Exception:
                pass
            try:
                positions = mt5_mod.positions_get() or []
                if positions:
                    total = 0
                    for p in positions:
                        psym = str(getattr(p, "symbol", "") or "").upper()
                        if psym in {s, broker, s.replace("M", ""), broker.replace("M", "")} or psym.startswith(s) or psym.startswith(broker):
                            total += 1
                    return total
            except Exception:
                pass
    except Exception:
        pass

    # 2) Existing helpers
    try:
        fn = globals().get("get_open_positions_count")
        if callable(fn):
            return int(fn(s) or 0)
    except Exception:
        pass

    try:
        fn = globals().get("count_open_positions")
        if callable(fn):
            total, per_map = fn()
            if isinstance(per_map, dict):
                return int(per_map.get(s, per_map.get(broker, 0)) or 0)
    except Exception:
        pass

    return 0

def _kyoto_count_total_open() -> int:
    try:
        mt5_mod = globals().get("_mt5")
        if globals().get("MT5_AVAILABLE") and globals().get("_mt5_connected") and mt5_mod is not None:
            try:
                positions = mt5_mod.positions_get() or []
                return len(positions)
            except Exception:
                pass
    except Exception:
        pass

    try:
        fn = globals().get("count_open_positions")
        if callable(fn):
            total, _per_map = fn()
            return int(total or 0)
    except Exception:
        pass

    return 0

def _deprecated_allowed_to_open(symbol: str):
    """
    Final hard gate for open-trade limits.
    This is the single source of truth used by execution.
    """
    try:
        s = str(symbol).upper()

        # Macro-filter symbols are not tradeable.
        if s.startswith(("DXY", "US10Y")):
            return False, "macro_filter_symbol_only"

        total = _kyoto_count_total_open()
        if total >= int(GLOBAL_MAX_OPEN_TRADES):
            return False, f"global_max_open_reached:{total}"

        per = _kyoto_count_open_symbol(s)
        limit = int(SYMBOL_TRADE_LIMITS.get(s, int(os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
        if per >= limit:
            return False, f"symbol_limit_reached:{s}:{per}/{limit}"

        return True, "ok"
    except Exception:
        logger.exception("allowed_to_open failed for %s", symbol)
        return False, "error"

# Keep a safe backup of the current live wrappers before overriding them.
if "_KYOTO_ORIG_order_wrapper_LIMITS" not in globals():
    _KYOTO_ORIG_order_wrapper_LIMITS = globals().get("order_wrapper")
if "_KYOTO_ORIG_execute_signal_LIMITS" not in globals():
    _KYOTO_ORIG_execute_signal_LIMITS = globals().get("execute_signal")

def _deprecated_order_wrapper(mt5_module, order_request):
    """
    Final execution wrapper with hard max-open enforcement.
    Never opens beyond per-symbol or global limits.
    """
    try:
        ctx = _kyoto_ctx_get() or {}
        req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})

        sym = str(req.get("symbol") or req.get("instrument") or ctx.get("symbol") or "").upper()
        if not sym:
            return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}

        allowed, reason = allowed_to_open(sym)
        if not allowed:
            logger.info("Order skipped for %s: %s", sym, reason)
            return {"retcode": -1, "comment": reason, "request": req}

        # preserve previous safety checks from the earlier wrapper, if any
        prev = globals().get("_KYOTO_ORIG_order_wrapper_LIMITS")
        if callable(prev):
            return prev(mt5_module, req)

        # If no previous wrapper exists, fail safe rather than risk unmanaged order placement.
        return {"retcode": -1, "comment": "ORDER_WRAPPER_MISSING_BASE_IMPL", "request": req}
    except Exception as e:
        logger.exception("Final LIMITS order_wrapper failed: %s", e)
        return {"retcode": -1, "comment": str(e)}

def _deprecated_execute_signal(sym, signal, price, mt5_module, symbol_map):
    """
    Strong-signal execution gate with max-open enforcement as a hard stop.
    """
    try:
        sym_u = str(sym).upper()
        if sym_u.startswith(("DXY", "US10Y")):
            logger.info("Execution skipped for %s: macro filter symbol only", sym)
            return None

        # Hard stop before anything else.
        allowed, reason = allowed_to_open(sym_u)
        if not allowed:
            logger.info("Execution skipped for %s: %s", sym, reason)
            return None

        prev = globals().get("_KYOTO_ORIG_execute_signal_LIMITS")
        if callable(prev):
            res = prev(sym, signal, price, mt5_module, symbol_map)
            return res
        return None
    except Exception:
        logger.exception("Final LIMITS execute_signal failed for %s", sym)
        return None

# Keep the intended execution threshold as-is.
try:
    CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.30))
except Exception:
    pass
# --- END FINAL LIMIT ENFORCEMENT OVERRIDE ---


# --- BEGIN FINAL TRAILING / RISK ENFORCEMENT OVERRIDE ---
# Enforce the user-approved settings at the end of the file so they win over earlier defaults.
ATR_STOP_MULTIPLIER = 4.0
ATR_TAKE_PROFIT_MULTIPLIER = 6.0
try:
    BASE_RISK_PER_TRADE_PCT = 0.005
except Exception:
    pass
try:
    MIN_RISK_PER_TRADE_PCT = 0.005
except Exception:
    pass
try:
    MAX_RISK_PER_TRADE_PCT = 0.005
except Exception:
    pass
try:
    RISK_PER_TRADE_PCT = 0.005
except Exception:
    pass
try:
    GLOBAL_MAX_OPEN_TRADES = 8
    MAX_OPEN_TRADES = 8
    SYMBOL_TRADE_LIMITS = {"BTCUSD": 3, "USOIL": 3, "XAUUSD": 2, "EURUSD": 10, "USDJPY": 10}
except Exception:
    pass
try:
    CONFIG.setdefault("BACKTEST_PARAMS", {})
    for _sym, _params in {
        "BTCUSD": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "BTCUSDm": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "USOIL": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "USOILm": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "XAUUSD": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "XAUUSDm": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "EURUSD": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "EURUSDm": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "USDJPY": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
        "USDJPYm": {"sl_atr_mult": 4.0, "tp_atr_mult": 6.0, "risk_pct": 0.005},
    }.items():
        CONFIG["BACKTEST_PARAMS"].setdefault(_sym, {})
        CONFIG["BACKTEST_PARAMS"][_sym].update(_params)
except Exception:
    pass

def _kyoto_position_side(pos):
    try:
        t = getattr(pos, "type", None)
        if t is not None:
            try:
                t = int(t)
            except Exception:
                t = str(t).lower()
            if t in (0, "0", "buy", "long"):
                return "BUY"
            if t in (1, "1", "sell", "short"):
                return "SELL"
        side = getattr(pos, "side", None) or getattr(pos, "direction", None)
        if side:
            s = str(side).upper()
            if s.startswith("B"):
                return "BUY"
            if s.startswith("S"):
                return "SELL"
    except Exception:
        pass
    return None

def _kyoto_position_entry(pos):
    for key in ("price_open", "entry_price", "open_price", "price"):
        try:
            val = getattr(pos, key, None)
            if val is not None:
                return float(val)
        except Exception:
            continue
    return None

def _kyoto_position_sl(pos):
    for key in ("sl", "stop_loss", "sl_price"):
        try:
            val = getattr(pos, key, None)
            if val is not None:
                return float(val)
        except Exception:
            continue
    return None

def _kyoto_position_tp(pos):
    for key in ("tp", "take_profit", "tp_price"):
        try:
            val = getattr(pos, key, None)
            if val is not None:
                return float(val)
        except Exception:
            continue
    return None

def _kyoto_modify_position_sl_tp(mt5_module, pos, new_sl, new_tp=None):
    try:
        ticket = getattr(pos, "ticket", None) or getattr(pos, "order", None) or getattr(pos, "position_id", None)
        symbol = getattr(pos, "symbol", None)
        if ticket is None or not symbol:
            return False
        req = {
            "action": getattr(mt5_module, "TRADE_ACTION_SLTP", None),
            "position": int(ticket),
            "symbol": symbol,
            "sl": float(new_sl),
            "tp": float(new_tp) if new_tp is not None else float(_kyoto_position_tp(pos) or 0.0),
        }
        res = mt5_module.order_send(req)
        retcode = getattr(res, "retcode", None)
        return bool(retcode in (0, 10009, 10008) or str(retcode) == "0")
    except Exception:
        try:
            logger.exception("Failed to modify SL/TP for position %s", getattr(pos, "ticket", "?"))
        except Exception:
            pass
        return False

def trailing_manager_thread(stop_event, mt5_module, symbol_map):
    """
    Move stops to breakeven at +1R, then trail in 0.5R steps after +1.5R.
    """
    logger.info("Started background thread: trailing-manager")
    while not stop_event.is_set():
        try:
            if mt5_module is None:
                time.sleep(5.0)
                continue
            try:
                positions = mt5_module.positions_get() or []
            except Exception:
                positions = []
            for pos in positions:
                try:
                    side = _kyoto_position_side(pos)
                    if side not in ("BUY", "SELL"):
                        continue
                    entry = _kyoto_position_entry(pos)
                    sl = _kyoto_position_sl(pos)
                    tp = _kyoto_position_tp(pos)
                    if entry is None or sl is None:
                        continue
                    risk_dist = abs(float(entry) - float(sl))
                    if risk_dist <= 0:
                        continue
                    bid = getattr(getattr(mt5_module, "symbol_info_tick", lambda *_: None)(getattr(pos, "symbol", "")), "bid", None)
                    ask = getattr(getattr(mt5_module, "symbol_info_tick", lambda *_: None)(getattr(pos, "symbol", "")), "ask", None)
                    current = float(bid if side == "BUY" else ask if ask is not None else bid if bid is not None else 0.0)
                    if current <= 0:
                        continue
                    if side == "BUY":
                        profit_r = (current - entry) / risk_dist
                        target_sl = sl
                        if profit_r >= 1.0:
                            target_sl = max(target_sl, entry)
                        if profit_r >= 1.5:
                            locked_r = min(max(0.5, (profit_r - 1.5) // 0.5 * 0.5 + 0.5), profit_r - 0.01)
                            target_sl = max(target_sl, entry + risk_dist * locked_r)
                        if target_sl > sl + (risk_dist * 0.05):
                            _kyoto_modify_position_sl_tp(mt5_module, pos, target_sl, tp)
                    else:
                        profit_r = (entry - current) / risk_dist
                        target_sl = sl
                        if profit_r >= 1.0:
                            target_sl = min(target_sl, entry)
                        if profit_r >= 1.5:
                            locked_r = min(max(0.5, (profit_r - 1.5) // 0.5 * 0.5 + 0.5), profit_r - 0.01)
                            target_sl = min(target_sl, entry - risk_dist * locked_r)
                        if target_sl < sl - (risk_dist * 0.05):
                            _kyoto_modify_position_sl_tp(mt5_module, pos, target_sl, tp)
                except Exception:
                    continue
            time.sleep(5.0)
        except Exception:
            try:
                logger.exception("Exception in trailing_manager_thread")
            except Exception:
                pass
            time.sleep(5.0)
# --- END FINAL TRAILING / RISK ENFORCEMENT OVERRIDE ---


# --- FINAL STRICT ENFORCEMENT PATCH (loaded last so it wins) ---
try:
    ATR_STOP_MULTIPLIER = 4.0
    ATR_TAKE_PROFIT_MULTIPLIER = 6.0
except Exception:
    pass

try:
    BASE_RISK_PER_TRADE_PCT = 0.005
    MIN_RISK_PER_TRADE_PCT = 0.005
    MAX_RISK_PER_TRADE_PCT = 0.005
    RISK_PER_TRADE_PCT = 0.005
except Exception:
    pass

try:
    GLOBAL_MAX_OPEN_TRADES = 8
    MAX_OPEN_TRADES = 8
    SYMBOL_TRADE_LIMITS = {"BTCUSD": 3, "USOIL": 3, "XAUUSD": 2, "EURUSD": 10, "USDJPY": 10}
except Exception:
    pass

try:
    CONFIG.setdefault("BACKTEST_PARAMS", {})
    for _sym in ("BTCUSD", "BTCUSDm", "USOIL", "USOILm", "XAUUSD", "XAUUSDm", "EURUSD", "EURUSDm", "USDJPY", "USDJPYm"):
        CONFIG["BACKTEST_PARAMS"].setdefault(_sym, {})
        CONFIG["BACKTEST_PARAMS"][_sym].update({
            "sl_atr_mult": 4.0,
            "tp_atr_mult": 6.0,
            "risk_pct": 0.005,
        })
except Exception:
    pass


def _kyoto_canonical_symbol(symbol):
    try:
        s = str(symbol).upper()
    except Exception:
        s = str(symbol)
    if s.endswith("M") and len(s) > 1:
        return s[:-1]
    return s


def _kyoto_broker_symbol(symbol):
    s = _kyoto_canonical_symbol(symbol)
    try:
        fn = globals().get("map_symbol_to_broker")
        if callable(fn):
            mapped = fn(s)
            if mapped:
                return str(mapped)
    except Exception:
        pass
    return s


def _kyoto_positions_snapshot(mt5_module=None):
    total = 0
    per_map = {}
    try:
        mt5_mod = mt5_module or globals().get("_mt5") or globals().get("mt5")
        if mt5_mod is not None and globals().get("MT5_AVAILABLE", True) and globals().get("_mt5_connected", True):
            try:
                positions = mt5_mod.positions_get() or []
                total = len(positions)
                for p in positions:
                    sym = str(getattr(p, "symbol", "") or "").upper()
                    if sym:
                        per_map[sym] = per_map.get(sym, 0) + 1
                return total, per_map
            except Exception:
                pass
    except Exception:
        pass

    try:
        fn = globals().get("count_open_positions")
        if callable(fn):
            result = fn()
            if isinstance(result, tuple) and len(result) >= 2:
                total = int(result[0] or 0)
                per_map = dict(result[1] or {}) if isinstance(result[1], dict) else {}
                return total, per_map
            if isinstance(result, dict):
                per_map = {str(k).upper(): int(v or 0) for k, v in result.items()}
                total = sum(per_map.values())
                return total, per_map
    except Exception:
        pass

    return total, per_map


def _deprecated_allowed_to_open(symbol: str):
    """Final hard gate: global cap + per-symbol cap, with MT5-first counting."""
    try:
        s = _kyoto_canonical_symbol(symbol)
        if s.startswith(("DXY", "US10Y")):
            return False, "macro_filter_symbol_only"

        total, per_map = _kyoto_positions_snapshot()
        broker = _kyoto_broker_symbol(s)
        per = 0
        try:
            per = max(per, int(per_map.get(s, 0) or 0))
            per = max(per, int(per_map.get(broker, 0) or 0))
        except Exception:
            pass

        try:
            fn = globals().get("get_open_positions_count")
            if callable(fn):
                per = max(per, int(fn(s) or 0))
                per = max(per, int(fn(broker) or 0))
        except Exception:
            pass

        if total >= int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8)):
            return False, f"global_max_open_reached:{total}"

        limit = int(globals().get("SYMBOL_TRADE_LIMITS", {}).get(s, 10))
        if per >= limit:
            return False, f"symbol_limit_reached:{s}:{per}/{limit}"

        return True, "ok"
    except Exception:
        logger.exception("allowed_to_open failed for %s", symbol)
        return False, "error"


def _kyoto_extract_ohlc_rows(bars):
    if bars is None:
        return None, None, None, None
    try:
        if hasattr(bars, "columns"):
            cols = {str(c).lower(): c for c in list(bars.columns)}
            high = bars[cols.get("high")].astype(float).tolist()
            low = bars[cols.get("low")].astype(float).tolist()
            close = bars[cols.get("close")].astype(float).tolist()
            open_ = bars[cols.get("open")].astype(float).tolist() if cols.get("open") else close
            return open_, high, low, close
    except Exception:
        pass
    try:
        if isinstance(bars, list) and bars:
            first = bars[0]
            if isinstance(first, dict):
                open_ = [float(b.get("open", b.get("o", b.get("close", 0.0)))) for b in bars]
                high = [float(b.get("high", b.get("h", 0.0))) for b in bars]
                low = [float(b.get("low", b.get("l", 0.0))) for b in bars]
                close = [float(b.get("close", b.get("c", 0.0))) for b in bars]
                return open_, high, low, close
            if isinstance(first, (list, tuple)):
                open_ = [float(r[1]) for r in bars if len(r) > 1]
                high = [float(r[2]) for r in bars if len(r) > 3]
                low = [float(r[3]) for r in bars if len(r) > 3]
                close = [float(r[4]) for r in bars if len(r) > 4]
                return open_, high, low, close
    except Exception:
        pass
    return None, None, None, None


def _kyoto_atr_from_bars(bars, period=14):
    try:
        import math
        open_, high, low, close = _kyoto_extract_ohlc_rows(bars)
        if not high or not low or not close or len(close) < max(3, period):
            return None
        trs = []
        prev_close = close[0]
        for i in range(len(close)):
            h = float(high[i])
            l = float(low[i])
            c_prev = float(prev_close)
            trs.append(max(h - l, abs(h - c_prev), abs(l - c_prev)))
            prev_close = float(close[i])
        window = trs[-int(period):]
        return float(sum(window) / len(window)) if window else None
    except Exception:
        return None


def _kyoto_get_h1_bars(mt5_module, symbol, symbol_map=None, bars=160):
    try:
        cached = globals().get("_kyoto_h1_df")
        if callable(cached):
            try:
                return cached(mt5_module, symbol_map or {}, symbol, bars)
            except Exception:
                pass
    except Exception:
        pass

    try:
        mt5_mod = mt5_module or globals().get("_mt5") or globals().get("mt5")
        if mt5_mod is None:
            import MetaTrader5 as mt5_mod  # type: ignore
        mapped = symbol_map.get(symbol, symbol) if isinstance(symbol_map, dict) else symbol
        tf = getattr(mt5_mod, "TIMEFRAME_H1", None)
        if tf is None:
            return None
        raw = mt5_mod.copy_rates_from_pos(mapped, tf, 0, int(bars))
        if raw is None or len(raw) == 0:
            return None
        try:
            import pandas as _pd
            df = _pd.DataFrame(raw)
            if not getattr(df, "empty", True):
                return df
        except Exception:
            return raw
    except Exception:
        return None
    return None


def _kyoto_build_sl_tp(entry_price, side, bars, price_fallback=None):
    try:
        atr = _kyoto_atr_from_bars(bars, period=14)
        px = float(entry_price if entry_price not in (None, 0, 0.0, "") else price_fallback or 0.0)
        if px <= 0:
            return None, None
        if atr is None or atr <= 0:
            atr = max(1e-6, abs(px) * 0.005)
        stop_dist = max(1e-6, float(atr) * float(ATR_STOP_MULTIPLIER))
        tp_dist = max(1e-6, float(atr) * float(ATR_TAKE_PROFIT_MULTIPLIER))
        side_u = str(side).upper()
        if side_u in ("BUY", "LONG"):
            return float(px - stop_dist), float(px + tp_dist)
        return float(px + stop_dist), float(px - tp_dist)
    except Exception:
        return None, None


# Keep the original wrapper around, but force the stricter SL/TP and limit logic last.
if "order_wrapper" in globals() and "_KYOTO_ORIG_order_wrapper_STRICT" not in globals():
    _KYOTO_ORIG_order_wrapper_STRICT = order_wrapper
    def order_wrapper(mt5_module, order_request):
        try:
            ctx = _kyoto_ctx_get() or {}
            req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
            sym = str(req.get("symbol") or req.get("instrument") or ctx.get("symbol") or "").upper()
            if sym.startswith(("DXY", "US10Y")):
                return {"retcode": -1, "comment": "MACRO_FILTER_SYMBOL_ONLY", "request": req}

            ok, reason = allowed_to_open(sym or ctx.get("symbol", ""))
            if not ok:
                return {"retcode": -1, "comment": reason, "request": req}

            bars = ctx.get("df_h1") or ctx.get("bars")
            if bars is None:
                bars = _kyoto_get_h1_bars(mt5_module, sym or ctx.get("symbol", ""), ctx.get("symbol_map") or {}, 160)

            side = str(req.get("type", req.get("side", ""))).upper()
            entry = req.get("price") or ctx.get("entry")
            sl, tp = _kyoto_build_sl_tp(entry, side, bars, price_fallback=entry)
            if sl is not None and tp is not None:
                req["sl"] = sl
                req["tp"] = tp
            elif (req.get("sl") in (None, 0, 0.0, "")) or (req.get("tp") in (None, 0, 0.0, "")):
                px = float(req.get("price") or ctx.get("entry") or 0.0)
                if px > 0:
                    sd = max(1e-6, abs(px) * 0.005)
                    if side.lower() in ("buy", "long", "0", "1"):
                        req["sl"] = px - sd
                        req["tp"] = px + sd * 1.5
                    else:
                        req["sl"] = px + sd
                        req["tp"] = px - sd * 1.5
            return _KYOTO_ORIG_order_wrapper_STRICT(mt5_module, req)
        except Exception as e:
            try:
                logger.exception("Strict order_wrapper failed: %s", e)
            except Exception:
                pass
            return {"retcode": -1, "comment": str(e), "request": order_request}


if "execute_signal" in globals() and "_KYOTO_ORIG_execute_signal_STRICT" not in globals():
    _KYOTO_ORIG_execute_signal_STRICT = execute_signal
    def execute_signal(sym, signal, price, mt5_module, symbol_map):
        try:
            sym_u = str(sym).upper()
            if sym_u.startswith(("DXY", "US10Y")):
                return _KYOTO_ORIG_execute_signal_STRICT(sym, signal, price, mt5_module, symbol_map)
            if not globals().get("AUTO_EXECUTE", True):
                return None
            if CONFIG.get("DRY_RUN") or CONFIG.get("DRY_RUN_FLAG"):
                return None
            if signal is None:
                logger.info("Execution skipped for %s: signal is None", sym)
                return None
            try:
                signal = float(signal)
            except Exception:
                return None

            params = CONFIG.get("BACKTEST_PARAMS", {}).get(sym_u, CONFIG.get("BACKTEST_PARAMS", {}).get(sym, {}))
            symbol_thresh = float(params.get("signal_thresh", CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.50)))
            exec_thresh = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.50))
            threshold = max(symbol_thresh, exec_thresh)
            if abs(signal) < threshold:
                logger.info("Execution skipped for %s: signal below execution threshold (%.3f) signal=%.4f", sym, threshold, signal)
                return None

            allowed, reason = allowed_to_open(sym_u)
            if not allowed:
                logger.info("Execution skipped for %s: %s", sym, reason)
                return None

            mapped = symbol_map.get(sym, sym) if symbol_map else sym
            side = "buy" if signal > 0 else "sell"
            volume = float(CONFIG.get("DEFAULT_ORDER_VOLUME", 0.01))
            df_h1 = _kyoto_get_h1_bars(mt5_module, sym_u, symbol_map or {}, 160)
            _kyoto_ctx_set(
                symbol=sym_u,
                df_h1=df_h1,
                bars=df_h1,
                signal=signal,
                quality=min(1.0, abs(signal)),
                regime="trending",
                allowed=allowed,
                reason=reason,
                entry=float(price or 0.0),
                tech=signal,
                fund=float(get_fused_score(sym_u)) if "get_fused_score" in globals() else 0.0,
                sent=float(get_news_impact_score(sym_u)) if "get_news_impact_score" in globals() else 0.0,
                symbol_map=dict(symbol_map or {}),
            )
            try:
                req = {"symbol": mapped, "volume": volume, "type": side, "price": float(price or 0.0)}
                res = order_wrapper(mt5_module, req)
            finally:
                try:
                    _kyoto_ctx_clear()
                except Exception:
                    pass
            return res
        except Exception:
            try:
                logger.exception("Strict execute_signal failed for %s", sym)
            except Exception:
                pass
            try:
                _kyoto_ctx_clear()
            except Exception:
                pass
            return None

# Reassert the final live settings.
try:
    CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.30))
except Exception:
    pass

# --- END FINAL STRICT ENFORCEMENT PATCH ---


# --- FINAL HARD LIMIT PATCH (atomic reservations, last source of truth) ---
try:
    import threading as _kyoto_threading
except Exception:
    _kyoto_threading = None

try:
    _KYOTO_LIMIT_LOCK
except NameError:
    _KYOTO_LIMIT_LOCK = _kyoto_threading.RLock() if _kyoto_threading is not None else None

try:
    _KYOTO_LIMIT_PENDING
except NameError:
    _KYOTO_LIMIT_PENDING = {}

try:
    _KYOTO_LIMIT_CTX
except NameError:
    _KYOTO_LIMIT_CTX = {"token": None, "symbol": None}

try:
    _KYOTO_LIMIT_TTL_SECONDS = int(globals().get("_KYOTO_LIMIT_TTL_SECONDS", 90))
except Exception:
    _KYOTO_LIMIT_TTL_SECONDS = 90


def _kyoto_limit_symbol(symbol):
    try:
        s = str(symbol).upper()
    except Exception:
        s = str(symbol)
    try:
        if s.endswith("M") and len(s) > 1:
            s = s[:-1]
    except Exception:
        pass
    try:
        fn = globals().get("map_symbol_to_broker")
        if callable(fn):
            mapped = fn(s)
            if mapped:
                return str(mapped).upper()
    except Exception:
        pass
    return s


def _kyoto_limit_cleanup(now=None):
    try:
        if now is None:
            import time as _t
            now = _t.time()
        ttl = int(globals().get("_KYOTO_LIMIT_TTL_SECONDS", 90))
        if not isinstance(globals().get("_KYOTO_LIMIT_PENDING"), dict):
            globals()["_KYOTO_LIMIT_PENDING"] = {}
        pending = globals()["_KYOTO_LIMIT_PENDING"]
        dead = [tok for tok, meta in list(pending.items()) if now - float(meta.get("ts", now)) > ttl]
        for tok in dead:
            pending.pop(tok, None)
    except Exception:
        pass


def _kyoto_limit_live_counts(symbol=None, ignore_token=None):
    """
    Returns (total_open, per_symbol_open) including pending reservations.
    """
    sym = _kyoto_limit_symbol(symbol) if symbol is not None else None
    total = 0
    per = 0
    try:
        mt5_mod = globals().get("_mt5")
        if globals().get("MT5_AVAILABLE") and globals().get("_mt5_connected") and mt5_mod is not None:
            try:
                positions = mt5_mod.positions_get() or []
                total = len(positions)
                if sym is not None:
                    for p in positions:
                        psym = str(getattr(p, "symbol", "") or "").upper()
                        if psym == sym or psym.startswith(sym) or psym.startswith(sym.replace("M", "")):
                            per += 1
                    if per == 0:
                        try:
                            positions = mt5_mod.positions_get(symbol=sym) or []
                            per = len(positions)
                        except Exception:
                            pass
                else:
                    per = 0
            except Exception:
                pass
    except Exception:
        pass

    try:
        fn = globals().get("count_open_positions")
        if callable(fn):
            result = fn()
            if isinstance(result, tuple) and len(result) >= 2:
                total2 = int(result[0] or 0)
                per_map = result[1] if isinstance(result[1], dict) else {}
                total = max(total, total2)
                if sym is not None:
                    per = max(per, int(per_map.get(sym, per_map.get(sym.replace("M", ""), 0)) or 0))
            elif isinstance(result, dict):
                per_map = {str(k).upper(): int(v or 0) for k, v in result.items()}
                total2 = sum(per_map.values())
                total = max(total, total2)
                if sym is not None:
                    per = max(per, int(per_map.get(sym, per_map.get(sym.replace("M", ""), 0)) or 0))
    except Exception:
        pass

    try:
        fn = globals().get("get_open_positions_count")
        if callable(fn) and sym is not None:
            per = max(per, int(fn(sym) or 0))
    except Exception:
        pass

    try:
        _kyoto_limit_cleanup()
        pending = globals().get("_KYOTO_LIMIT_PENDING", {})
        if isinstance(pending, dict) and pending:
            for tok, meta in pending.items():
                if ignore_token is not None and tok == ignore_token:
                    continue
                total += 1
                if sym is not None and str(meta.get("symbol", "")).upper() == sym:
                    per += 1
    except Exception:
        pass

    return int(total or 0), int(per or 0)


def _kyoto_limit_reserve(symbol):
    """
    Reserve a single slot before order placement to stop concurrent overshoots.
    Returns (allowed, reason, token).
    """
    sym = _kyoto_limit_symbol(symbol)
    try:
        lock = globals().get("_KYOTO_LIMIT_LOCK")
        if lock is None:
            return False, "limit_lock_missing", None
        with lock:
            _kyoto_limit_cleanup()
            total, per = _kyoto_limit_live_counts(sym, None)
            gmax = int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8))
            limits = dict(globals().get("SYMBOL_TRADE_LIMITS", {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}))
            limit = int(limits.get(sym, int(os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
            if total >= gmax:
                return False, f"global_max_open_reached:{total}", None
            if per >= limit:
                return False, f"symbol_limit_reached:{sym}:{per}/{limit}", None
            import time as _t, uuid as _uuid
            token = _uuid.uuid4().hex
            globals()["_KYOTO_LIMIT_PENDING"][token] = {"symbol": sym, "ts": _t.time()}
            return True, "ok", token
    except Exception as e:
        try:
            logger.exception("reserve failed for %s: %s", symbol, e)
        except Exception:
            pass
        return False, "error", None


def _kyoto_limit_release(token):
    try:
        if not token:
            return
        lock = globals().get("_KYOTO_LIMIT_LOCK")
        if lock is None:
            return
        with lock:
            pending = globals().get("_KYOTO_LIMIT_PENDING", {})
            if isinstance(pending, dict):
                pending.pop(token, None)
    except Exception:
        pass


def _kyoto_order_success(res):
    try:
        if res is None:
            return False
        if isinstance(res, dict):
            rc = res.get("retcode", None)
            if rc is not None:
                try:
                    if int(rc) == 0:
                        return True
                except Exception:
                    pass
            st = str(res.get("status", "")).lower()
            if st in {"sent", "filled", "ok", "success", "executed"}:
                return True
            if res.get("order_id") is not None or res.get("order") is not None:
                return True
            if res.get("comment") and str(res.get("comment")).startswith("global_max_open_reached"):
                return False
        st = str(getattr(res, "status", "")).lower()
        if st in {"sent", "filled", "ok", "success", "executed"}:
            return True
        rc = getattr(res, "retcode", None)
        if rc is not None:
            try:
                if int(rc) == 0:
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def _deprecated_allowed_to_open(symbol: str):
    """
    Final hard gate used by execution, with reservations included.
    """
    try:
        s = _kyoto_limit_symbol(symbol)
        if s.startswith(("DXY", "US10Y")):
            return False, "macro_filter_symbol_only"
        ignore_token = None
        try:
            ignore_token = globals().get("_KYOTO_LIMIT_CTX", {}).get("token")
        except Exception:
            ignore_token = None
        total, per = _kyoto_limit_live_counts(s, ignore_token)
        gmax = int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8))
        limits = dict(globals().get("SYMBOL_TRADE_LIMITS", {"USOIL": 3, "BTCUSD": 3, "USDJPY": 10, "EURUSD": 10, "XAUUSD": 2}))
        limit = int(limits.get(s, int(os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
        if total >= gmax:
            return False, f"global_max_open_reached:{total}"
        if per >= limit:
            return False, f"symbol_limit_reached:{s}:{per}/{limit}"
        return True, "ok"
    except Exception:
        try:
            logger.exception("allowed_to_open final hard gate failed for %s", symbol)
        except Exception:
            pass
        return False, "error"


if "_KYOTO_PREV_order_wrapper_FINAL_LIMITS" not in globals():
    _KYOTO_PREV_order_wrapper_FINAL_LIMITS = globals().get("order_wrapper")

if "_KYOTO_PREV_execute_signal_FINAL_LIMITS" not in globals():
    _KYOTO_PREV_execute_signal_FINAL_LIMITS = globals().get("execute_signal")


def _deprecated_order_wrapper(mt5_module, order_request):
    """
    Final hard gate with atomic reservation.
    """
    token = None
    sym = ""
    try:
        ctx = _kyoto_ctx_get() or {}
        req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
        sym = str(req.get("symbol") or req.get("instrument") or ctx.get("symbol") or "").upper()
        if not sym:
            return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}

        ok, reason, token = _kyoto_limit_reserve(sym)
        if not ok:
            logger.info("Order skipped for %s: %s", sym, reason)
            return {"retcode": -1, "comment": reason, "request": req}

        try:
            _KYOTO_LIMIT_CTX["token"] = token
            _KYOTO_LIMIT_CTX["symbol"] = sym
        except Exception:
            pass

        prev = globals().get("_KYOTO_PREV_order_wrapper_FINAL_LIMITS")
        if callable(prev):
            res = prev(mt5_module, req)
        else:
            res = {"retcode": -1, "comment": "ORDER_WRAPPER_MISSING_BASE_IMPL", "request": req}

        if not _kyoto_order_success(res):
            _kyoto_limit_release(token)
        return res

    except Exception as e:
        try:
            logger.exception("Final hard-limit order_wrapper failed for %s: %s", sym, e)
        except Exception:
            pass
        _kyoto_limit_release(token)
        return {"retcode": -1, "comment": str(e), "request": order_request}
    finally:
        try:
            _KYOTO_LIMIT_CTX["token"] = None
            _KYOTO_LIMIT_CTX["symbol"] = None
        except Exception:
            pass


def _deprecated_execute_signal(sym, signal, price, mt5_module, symbol_map):
    """
    Keep execute_signal strict, but let order_wrapper do the atomic enforcement.
    """
    try:
        sym_u = str(sym).upper()
        if sym_u.startswith(("DXY", "US10Y")):
            prev = globals().get("_KYOTO_PREV_execute_signal_FINAL_LIMITS")
            if callable(prev):
                return prev(sym, signal, price, mt5_module, symbol_map)
            return None

        ok, reason = allowed_to_open(sym_u)
        if not ok:
            logger.info("Execution skipped for %s: %s", sym, reason)
            return None

        prev = globals().get("_KYOTO_PREV_execute_signal_FINAL_LIMITS")
        if callable(prev):
            return prev(sym, signal, price, mt5_module, symbol_map)
        return None
    except Exception:
        try:
            logger.exception("Final hard-limit execute_signal failed for %s", sym)
        except Exception:
            pass
        return None


try:
    if "UVXRiskManager" in globals() and hasattr(UVXRiskManager, "can_open"):
        _KYOTO_PREV_UVXRiskManager_can_open = UVXRiskManager.can_open
        def _kyoto_uvx_can_open(self, symbol, size):
            ok, _reason = allowed_to_open(symbol)
            return bool(ok)
        UVXRiskManager.can_open = _kyoto_uvx_can_open
except Exception:
    pass

# --- END FINAL HARD LIMIT PATCH ---





# --- KYOTO MEMORY LAYER (per-symbol / per-timeframe adaptive memory) ---
try:
    from collections import defaultdict, deque
except Exception:
    pass

import json
import os
import threading
import time

_KYOTO_MEMORY_FILE = os.path.join(os.path.dirname(__file__), "kyoto_trade_memory.json")
_KYOTO_MEMORY_LOCK = threading.RLock()
_KYOTO_MEMORY_TLS = threading.local()
_KYOTO_MEMORY_PENDING = defaultdict(lambda: deque(maxlen=120))
_KYOTO_MEMORY_STATE = {"version": 1, "symbols": {}}

def _kyoto_mem_symbol(symbol):
    try:
        return str(symbol or "UNKNOWN").upper()
    except Exception:
        return "UNKNOWN"

def _kyoto_mem_tf(timeframe):
    tf = str(timeframe or "H1").upper()
    return tf if tf in ("M30", "H1", "M15", "H4", "D1") else "H1"

def _kyoto_mem_key(symbol, timeframe):
    return _kyoto_mem_symbol(symbol), _kyoto_mem_tf(timeframe)

def _kyoto_mem_default_bucket():
    return {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "net_pnl": 0.0,
        "avg_r": 0.0,
        # Closed trades only. Non-closed updates are kept in last_* context fields.
        "recent": [],
        "closed_ids": [],
        "signal_stats": {},
        "setup_stats": {},
        "pattern_stats": {},
        "signal_type_stats": {},
        "last_update": None,
        "last_regime": "unknown",
        "last_volatility": None,
        "last_atr": None,
        "last_threshold": None,
        "last_quality": None,
        "last_signal_score": None,
        "last_signal_type": None,
        "last_setup_id": None,
        "last_pattern_id": None,
        "last_signal_key": None,
    }

def _kyoto_mem_load():
    global _KYOTO_MEMORY_STATE
    try:
        if os.path.exists(_KYOTO_MEMORY_FILE):
            with open(_KYOTO_MEMORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "symbols" in data:
                _KYOTO_MEMORY_STATE = data
                _KYOTO_MEMORY_STATE.setdefault("symbols", {})
                _KYOTO_MEMORY_STATE.setdefault("version", 1)
                return
    except Exception:
        try:
            logger.exception("KYOTO memory load failed")
        except Exception:
            pass
    _KYOTO_MEMORY_STATE = {"version": 1, "symbols": {}}

def _kyoto_mem_save():
    try:
        tmp = _KYOTO_MEMORY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_KYOTO_MEMORY_STATE, f, indent=2, default=str)
        os.replace(tmp, _KYOTO_MEMORY_FILE)
    except Exception:
        try:
            logger.exception("KYOTO memory save failed")
        except Exception:
            pass

def _kyoto_mem_bucket(symbol, timeframe, create=True):
    sym = _kyoto_mem_symbol(symbol)
    tf = _kyoto_mem_tf(timeframe)
    with _KYOTO_MEMORY_LOCK:
        symbols = _KYOTO_MEMORY_STATE.setdefault("symbols", {})
        if sym not in symbols:
            if not create:
                return None
            symbols[sym] = {}
        sym_map = symbols[sym]
        if tf not in sym_map:
            if not create:
                return None
            sym_map[tf] = _kyoto_mem_default_bucket()
        bucket = sym_map[tf]
        bucket.setdefault("recent", [])
        return bucket

def _kyoto_mem_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)

def _kyoto_mem_clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

def _kyoto_mem_set_context(**kwargs):
    for k, v in kwargs.items():
        setattr(_KYOTO_MEMORY_TLS, k, v)

def _kyoto_mem_get_context(name, default=None):
    return getattr(_KYOTO_MEMORY_TLS, name, default)

def _kyoto_mem_clear_context():
    for name in ("symbol", "timeframe", "regime", "volatility", "atr", "quality", "threshold", "signal_score", "signal_type", "setup_id", "pattern_id", "signal_key"):
        try:
            if hasattr(_KYOTO_MEMORY_TLS, name):
                delattr(_KYOTO_MEMORY_TLS, name)
        except Exception:
            pass


def _kyoto_mem_first_present(payload, *keys):
    try:
        for key in keys:
            if key in payload:
                value = payload.get(key)
                if value is not None and value != "":
                    return value
    except Exception:
        pass
    return None


def _kyoto_mem_infer_symbol(args, kwargs):
    try:
        context_symbol = _kyoto_mem_get_context("symbol", None)
        if context_symbol:
            return _kyoto_mem_symbol(context_symbol)
    except Exception:
        pass
    try:
        if isinstance(kwargs, dict):
            for key in ("symbol", "sym", "ticker", "instrument", "market", "asset"):
                value = kwargs.get(key)
                if value:
                    return _kyoto_mem_symbol(value)
    except Exception:
        pass
    try:
        for item in args or ():
            if isinstance(item, str) and item:
                return _kyoto_mem_symbol(item)
            if isinstance(item, dict):
                for key in ("symbol", "sym", "ticker", "instrument", "market", "asset"):
                    value = item.get(key)
                    if value:
                        return _kyoto_mem_symbol(value)
    except Exception:
        pass
    return _kyoto_mem_symbol(None)


def _kyoto_mem_infer_timeframe(args, kwargs):
    try:
        context_tf = _kyoto_mem_get_context("timeframe", None)
        if context_tf:
            return _kyoto_mem_tf(context_tf)
    except Exception:
        pass
    try:
        if isinstance(kwargs, dict):
            for key in ("timeframe", "tf", "time_frame", "bar_tf", "period"):
                value = kwargs.get(key)
                if value:
                    return _kyoto_mem_tf(value)
    except Exception:
        pass
    try:
        for item in args or ():
            if isinstance(item, str) and item.upper() in ("M30", "H1", "M15", "H4", "D1"):
                return _kyoto_mem_tf(item)
            if isinstance(item, dict):
                for key in ("timeframe", "tf", "time_frame", "bar_tf", "period"):
                    value = item.get(key)
                    if value:
                        return _kyoto_mem_tf(value)
    except Exception:
        pass
    return _kyoto_mem_tf(None)


def _kyoto_mem_extract_payload(args, kwargs, result=None):
    payload = {}
    try:
        if isinstance(kwargs, dict):
            payload.update({k: v for k, v in kwargs.items() if v is not None})
    except Exception:
        pass
    try:
        for item in args or ():
            if isinstance(item, dict):
                payload.update({k: v for k, v in item.items() if v is not None})
    except Exception:
        pass
    try:
        if isinstance(result, dict):
            payload.update({k: v for k, v in result.items() if v is not None})
        elif isinstance(result, (list, tuple)) and result and isinstance(result[0], dict):
            payload.update({k: v for k, v in result[0].items() if v is not None})
    except Exception:
        pass
    try:
        ctx = {
            "symbol": _kyoto_mem_get_context("symbol", None),
            "timeframe": _kyoto_mem_get_context("timeframe", None),
            "regime": _kyoto_mem_get_context("regime", None),
            "volatility": _kyoto_mem_get_context("volatility", None),
            "atr": _kyoto_mem_get_context("atr", None),
            "quality": _kyoto_mem_get_context("quality", None),
            "threshold": _kyoto_mem_get_context("threshold", None),
            "signal_score": _kyoto_mem_get_context("signal_score", None),
            "signal_type": _kyoto_mem_get_context("signal_type", None),
            "setup_id": _kyoto_mem_get_context("setup_id", None),
            "pattern_id": _kyoto_mem_get_context("pattern_id", None),
            "signal_key": _kyoto_mem_get_context("signal_key", None),
        }
        payload.update({k: v for k, v in ctx.items() if v is not None and k not in payload})
    except Exception:
        pass
    return payload


def _kyoto_mem_is_closed(payload):
    try:
        if not isinstance(payload, dict):
            return False
        status = str(payload.get("status", "") or "").strip().lower()
        if status in {"closed", "close", "win", "loss", "closed_win", "closed_loss", "tp", "sl", "take_profit", "stop_loss"}:
            return True
        pnl = payload.get("pnl")
        rmult = payload.get("rmult")
        if pnl is not None or rmult is not None:
            return True
    except Exception:
        pass
    return False

def _kyoto_mem_signal_key_from_payload(payload, context=None):
    context = context or {}
    merged = {}
    try:
        if isinstance(context, dict):
            merged.update({k: v for k, v in context.items() if v is not None})
        if isinstance(payload, dict):
            merged.update({k: v for k, v in payload.items() if v is not None})
    except Exception:
        pass
    signal_type = _kyoto_mem_first_present(merged, "signal_type", "signal", "signal_name", "signal_family", "signal_class")
    setup_id = _kyoto_mem_first_present(merged, "setup_id", "setup", "strategy", "strategy_id")
    pattern_id = _kyoto_mem_first_present(merged, "pattern_id", "pattern", "pattern_name")
    parts = []
    if signal_type is not None:
        parts.append(f"sig={signal_type}")
    if setup_id is not None:
        parts.append(f"setup={setup_id}")
    if pattern_id is not None:
        parts.append(f"pattern={pattern_id}")
    if parts:
        return "|".join(str(p) for p in parts)
    return None


def _kyoto_mem_signal_context(symbol=None, timeframe=None, payload=None):
    payload = payload or {}
    signal_type = _kyoto_mem_first_present(payload, "signal_type", "signal", "signal_name", "signal_family", "signal_class")
    setup_id = _kyoto_mem_first_present(payload, "setup_id", "setup", "strategy", "strategy_id")
    pattern_id = _kyoto_mem_first_present(payload, "pattern_id", "pattern", "pattern_name")
    signal_key = _kyoto_mem_signal_key_from_payload(payload)
    return {
        "signal_type": signal_type,
        "setup_id": setup_id,
        "pattern_id": pattern_id,
        "signal_key": signal_key,
    }

def _kyoto_mem_queue_pending(symbol, timeframe, snapshot):
    key = _kyoto_mem_key(symbol, timeframe)
    with _KYOTO_MEMORY_LOCK:
        _KYOTO_MEMORY_PENDING[key].append(dict(snapshot or {}))

def _kyoto_mem_pop_pending(symbol, timeframe):
    key = _kyoto_mem_key(symbol, timeframe)
    with _KYOTO_MEMORY_LOCK:
        dq = _KYOTO_MEMORY_PENDING.get(key)
        if dq:
            try:
                return dq.popleft()
            except Exception:
                return None
    return None


def _kyoto_mem_update_nested_stats(container, key, rec, pnl, rmult):
    try:
        stats_bucket = container.setdefault(str(key), {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "net_pnl": 0.0,
            "recent": [],
            "last_update": None,
            "last_regime": "unknown",
            "last_quality": None,
            "last_signal_score": None,
            "last_signal_type": None,
            "last_setup_id": None,
            "last_pattern_id": None,
            "last_signal_key": None,
        })
        stats_bucket["trades"] = int(stats_bucket.get("trades", 0) or 0) + 1
        if _kyoto_mem_float(pnl, 0.0) > 0:
            stats_bucket["wins"] = int(stats_bucket.get("wins", 0) or 0) + 1
        else:
            stats_bucket["losses"] = int(stats_bucket.get("losses", 0) or 0) + 1
        if pnl is not None:
            stats_bucket["net_pnl"] = _kyoto_mem_float(stats_bucket.get("net_pnl", 0.0), 0.0) + _kyoto_mem_float(pnl, 0.0)
        elif rmult is not None:
            stats_bucket["net_pnl"] = _kyoto_mem_float(stats_bucket.get("net_pnl", 0.0), 0.0) + _kyoto_mem_float(rmult, 0.0)
        stats_recent = stats_bucket.setdefault("recent", [])
        stats_recent.append(dict(rec))
        if len(stats_recent) > 30:
            del stats_recent[:-30]
        stats_bucket["last_regime"] = rec.get("regime", stats_bucket.get("last_regime", "unknown"))
        stats_bucket["last_quality"] = rec.get("quality", stats_bucket.get("last_quality"))
        stats_bucket["last_signal_score"] = rec.get("signal_score", stats_bucket.get("last_signal_score"))
        stats_bucket["last_signal_type"] = rec.get("signal_type", stats_bucket.get("last_signal_type"))
        stats_bucket["last_setup_id"] = rec.get("setup_id", stats_bucket.get("last_setup_id"))
        stats_bucket["last_pattern_id"] = rec.get("pattern_id", stats_bucket.get("last_pattern_id"))
        stats_bucket["last_signal_key"] = rec.get("signal_key", stats_bucket.get("last_signal_key"))
        stats_bucket["avg_r"] = (stats_bucket.get("net_pnl", 0.0) / stats_bucket["trades"]) if stats_bucket["trades"] > 0 else 0.0
        stats_bucket["last_update"] = time.time()
    except Exception:
        pass

def kyoto_memory_profile(symbol, timeframe="H1"):
    sym = _kyoto_mem_symbol(symbol)
    tf = _kyoto_mem_tf(timeframe)
    signal_key = _kyoto_mem_get_context("signal_key", None)
    signal_type = _kyoto_mem_get_context("signal_type", None)
    setup_id = _kyoto_mem_get_context("setup_id", None)
    pattern_id = _kyoto_mem_get_context("pattern_id", None)
    with _KYOTO_MEMORY_LOCK:
        bucket = _kyoto_mem_bucket(sym, tf, create=True)
        recent = list(bucket.get("recent", []))[-30:]
        trades = int(bucket.get("trades", 0) or 0)
        wins = int(bucket.get("wins", 0) or 0)
        losses = int(bucket.get("losses", 0) or 0)
        win_rate = (wins / trades) if trades > 0 else 0.5
        r_values = []
        vols = []
        for item in recent:
            try:
                if item.get("rmult") is not None:
                    r_values.append(float(item.get("rmult")))
            except Exception:
                pass
            try:
                if item.get("volatility") is not None:
                    vols.append(float(item.get("volatility")))
            except Exception:
                pass
        avg_r = sum(r_values) / len(r_values) if r_values else 0.0
        base_vol = None
        if vols:
            vols_sorted = sorted(vols)
            base_vol = vols_sorted[len(vols_sorted) // 2]
        last_vol = bucket.get("last_volatility")
        vol_ratio = 1.0
        try:
            if base_vol and float(base_vol) > 0 and last_vol is not None:
                vol_ratio = float(last_vol) / float(base_vol)
        except Exception:
            vol_ratio = 1.0
        vol_ratio = _kyoto_mem_clamp(vol_ratio, 0.70, 1.30)

        stop_mult = _kyoto_mem_clamp(4.0 * vol_ratio, 3.0, 5.5)
        tp_mult = _kyoto_mem_clamp(6.0 * vol_ratio, 4.5, 8.0)

        if trades >= 12:
            if win_rate >= 0.60 and avg_r > 0:
                threshold_factor = 0.96
            elif win_rate <= 0.40 or avg_r < 0:
                threshold_factor = 1.08
            else:
                threshold_factor = 1.0
        else:
            threshold_factor = 1.0

        if trades >= 12:
            if win_rate >= 0.60 and avg_r > 0:
                stop_mult *= 0.96
                tp_mult *= 1.04
            elif win_rate <= 0.40 or avg_r < 0:
                stop_mult *= 1.06
                tp_mult *= 0.96
        stop_mult = _kyoto_mem_clamp(stop_mult, 3.0, 5.5)
        tp_mult = _kyoto_mem_clamp(tp_mult, 4.5, 8.0)

        signal_profile = {}
        setup_profile = {}
        pattern_profile = {}
        signal_type_profile = {}
        stats_map = bucket.get("signal_stats", {}) or {}
        setup_map = bucket.get("setup_stats", {}) or {}
        pattern_map = bucket.get("pattern_stats", {}) or {}
        signal_type_map = bucket.get("signal_type_stats", {}) or {}
        if signal_key is not None:
            signal_profile = dict(stats_map.get(str(signal_key), {}) or {})
        if setup_id is not None and not signal_profile:
            setup_profile = dict(setup_map.get(str(setup_id), {}) or {})
        if pattern_id is not None and not signal_profile and not setup_profile:
            pattern_profile = dict(pattern_map.get(str(pattern_id), {}) or {})
        if signal_type is not None and not signal_profile and not setup_profile and not pattern_profile:
            signal_type_profile = dict(signal_type_map.get(str(signal_type), {}) or {})
        effective_profile = signal_profile or setup_profile or pattern_profile or signal_type_profile

        signal_trades = int(effective_profile.get("trades", 0) or 0)
        signal_wins = int(effective_profile.get("wins", 0) or 0)
        signal_losses = int(effective_profile.get("losses", 0) or 0)
        signal_win_rate = (signal_wins / signal_trades) if signal_trades > 0 else None
        signal_avg_r = effective_profile.get("avg_r")
        try:
            if signal_avg_r is not None:
                signal_avg_r = float(signal_avg_r)
        except Exception:
            signal_avg_r = None

        profile = {
            "symbol": sym,
            "timeframe": tf,
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_r": avg_r,
            "vol_ratio": vol_ratio,
            "threshold_factor": threshold_factor,
            "stop_mult": stop_mult,
            "tp_mult": tp_mult,
            "last_regime": bucket.get("last_regime", "unknown"),
            "last_quality": bucket.get("last_quality", None),
            "last_signal_score": bucket.get("last_signal_score", None),
            "last_signal_type": bucket.get("last_signal_type", None),
            "last_setup_id": bucket.get("last_setup_id", None),
            "last_pattern_id": bucket.get("last_pattern_id", None),
            "last_signal_key": bucket.get("last_signal_key", None),
            "signal_key": signal_key,
            "signal_type": signal_type,
            "setup_id": setup_id,
            "pattern_id": pattern_id,
            "signal_trades": signal_trades,
            "signal_wins": signal_wins,
            "signal_losses": signal_losses,
            "signal_win_rate": signal_win_rate,
            "signal_avg_r": signal_avg_r,
            "setup_profile_active": bool(setup_profile),
            "pattern_profile_active": bool(pattern_profile),
            "signal_type_profile_active": bool(signal_type_profile),
        }
        return profile
def _kyoto_mem_record_from_args_kwargs(args, kwargs, result=None):
    symbol = _kyoto_mem_infer_symbol(args, kwargs)
    timeframe = _kyoto_mem_infer_timeframe(args, kwargs)
    payload = _kyoto_mem_extract_payload(args, kwargs, result=result)

    pnl = payload.get("pnl")
    rmult = payload.get("rmult")
    regime = payload.get("regime")
    threshold = payload.get("threshold", payload.get("score"))
    quality = payload.get("quality")
    signal_score = payload.get("signal_score", payload.get("signal"))
    volatility = payload.get("volatility")
    atr = payload.get("atr")
    side = payload.get("side")
    entry = payload.get("entry")
    exit_price = payload.get("exit_price", payload.get("exit"))
    status = payload.get("status")
    meta = payload.get("meta")
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = None
    if isinstance(meta, dict):
        payload.update(meta)
        timeframe = meta.get("timeframe", meta.get("tf", timeframe))
        regime = meta.get("regime", regime)
        volatility = meta.get("volatility", meta.get("atr", volatility))
        threshold = meta.get("threshold", meta.get("score", threshold))
        quality = meta.get("quality", quality)
        signal_score = meta.get("signal_score", meta.get("signal", signal_score))
        side = meta.get("side", side)
        entry = meta.get("entry", entry)
        exit_price = meta.get("exit_price", meta.get("exit", exit_price))
        status = meta.get("status", status)
        pnl = meta.get("pnl", pnl)
        rmult = meta.get("rmult", rmult)

    if pnl is None and isinstance(result, (int, float)):
        pnl = result

    context = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "volatility": volatility,
        "atr": atr,
        "quality": quality,
        "threshold": threshold,
        "signal_score": signal_score,
        "signal_type": payload.get("signal_type"),
        "setup_id": payload.get("setup_id"),
        "pattern_id": payload.get("pattern_id"),
    }
    signal_ctx = _kyoto_mem_signal_context(symbol=symbol, timeframe=timeframe, payload=payload)
    payload_signal_key = signal_ctx.get("signal_key")
    pending = _kyoto_mem_pop_pending(symbol, timeframe)
    if isinstance(pending, dict):
        regime = regime or pending.get("regime")
        volatility = volatility if volatility is not None else pending.get("volatility")
        threshold = threshold if threshold is not None else pending.get("threshold")
        quality = quality if quality is not None else pending.get("quality")
        signal_score = signal_score if signal_score is not None else pending.get("signal_score")
        side = side or pending.get("side")
        entry = entry if entry is not None else pending.get("entry")
        if payload_signal_key is None:
            payload_signal_key = pending.get("signal_key") or _kyoto_mem_signal_key_from_payload(pending)

    if payload_signal_key is None:
        payload_signal_key = _kyoto_mem_signal_key_from_payload(payload, context)

    if pnl is None and rmult is None and str(status).lower() not in {"closed", "close", "win", "loss", "closed_win", "closed_loss"}:
        kyoto_memory_update(
            symbol, timeframe,
            volatility=volatility, regime=regime, threshold=threshold, atr=atr,
            quality=quality, signal_score=signal_score, side=side, status=status,
            entry=entry, exit_price=exit_price, meta=meta,
            signal_type=context.get("signal_type"), setup_id=context.get("setup_id"),
            pattern_id=context.get("pattern_id"), signal_key=payload_signal_key
        )
        return

    kyoto_memory_update(
        symbol, timeframe,
        pnl=pnl, rmult=rmult, volatility=volatility, regime=regime, threshold=threshold, atr=atr,
        quality=quality, signal_score=signal_score, side=side, status=status,
        entry=entry, exit_price=exit_price, meta=meta,
        signal_type=context.get("signal_type"), setup_id=context.get("setup_id"),
        pattern_id=context.get("pattern_id"), signal_key=payload_signal_key
    )
def _kyoto_mem_update_bucket(symbol, timeframe, *, pnl=None, rmult=None, volatility=None, regime=None, threshold=None, atr=None, quality=None, signal_score=None, side=None, status=None, entry=None, exit_price=None, meta=None, trade_id=None, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    sym = _kyoto_mem_symbol(symbol)
    tf = _kyoto_mem_tf(timeframe)
    with _KYOTO_MEMORY_LOCK:
        bucket = _kyoto_mem_bucket(sym, tf, create=True)

        # Keep current market context fresh for adaptive reads,
        # but do not pollute the closed-trade learning window.
        if regime is not None:
            bucket["last_regime"] = str(regime)
        if volatility is not None:
            bucket["last_volatility"] = _kyoto_mem_float(volatility, bucket.get("last_volatility", 0.0) or 0.0)
        if atr is not None:
            bucket["last_atr"] = _kyoto_mem_float(atr, bucket.get("last_atr", 0.0) or 0.0)
        if threshold is not None:
            bucket["last_threshold"] = _kyoto_mem_float(threshold, bucket.get("last_threshold", 0.0) or 0.0)
        if quality is not None:
            bucket["last_quality"] = _kyoto_mem_float(quality, bucket.get("last_quality", 0.0) or 0.0)
        if signal_score is not None:
            bucket["last_signal_score"] = _kyoto_mem_float(signal_score, bucket.get("last_signal_score", 0.0) or 0.0)
        if signal_type is not None:
            bucket["last_signal_type"] = str(signal_type)
        if setup_id is not None:
            bucket["last_setup_id"] = str(setup_id)
        if pattern_id is not None:
            bucket["last_pattern_id"] = str(pattern_id)
        if signal_key is not None:
            bucket["last_signal_key"] = str(signal_key)

        is_closed = _kyoto_mem_is_closed({"status": status, "pnl": pnl, "rmult": rmult})

        # Only closed trades become learning samples.
        if not is_closed:
            bucket["last_update"] = time.time()
            _kyoto_mem_save()
            return

        closed_ids = bucket.setdefault("closed_ids", [])
        if trade_id is not None:
            trade_id_s = str(trade_id)
            if trade_id_s in set(map(str, closed_ids)):
                bucket["last_update"] = time.time()
                _kyoto_mem_save()
                return
            closed_ids.append(trade_id_s)
            if len(closed_ids) > 300:
                del closed_ids[:-300]

        bucket["trades"] = int(bucket.get("trades", 0) or 0) + 1
        if _kyoto_mem_float(pnl, 0.0) > 0:
            bucket["wins"] = int(bucket.get("wins", 0) or 0) + 1
        else:
            bucket["losses"] = int(bucket.get("losses", 0) or 0) + 1

        if pnl is not None:
            bucket["net_pnl"] = _kyoto_mem_float(bucket.get("net_pnl", 0.0), 0.0) + _kyoto_mem_float(pnl, 0.0)
        elif rmult is not None:
            bucket["net_pnl"] = _kyoto_mem_float(bucket.get("net_pnl", 0.0), 0.0) + _kyoto_mem_float(rmult, 0.0)

        recent = bucket.setdefault("recent", [])
        rec = {
            "trade_id": trade_id,
            "ts": time.time(),
            "pnl": pnl,
            "rmult": rmult,
            "volatility": volatility,
            "regime": regime,
            "threshold": threshold,
            "atr": atr,
            "quality": quality,
            "signal_score": signal_score,
            "signal_type": signal_type,
            "setup_id": setup_id,
            "pattern_id": pattern_id,
            "signal_key": signal_key,
            "side": side,
            "status": status,
            "entry": entry,
            "exit_price": exit_price,
        }
        recent.append(rec)
        if len(recent) > 30:
            del recent[:-30]

        signal_stats = bucket.setdefault("signal_stats", {})
        setup_stats = bucket.setdefault("setup_stats", {})
        pattern_stats = bucket.setdefault("pattern_stats", {})
        signal_type_stats = bucket.setdefault("signal_type_stats", {})

        if signal_key is not None:
            skey = str(signal_key)
            sig = signal_stats.setdefault(skey, {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "net_pnl": 0.0,
                "recent": [],
                "last_update": None,
                "last_regime": "unknown",
                "last_quality": None,
                "last_signal_score": None,
                "last_signal_type": None,
                "last_setup_id": None,
                "last_pattern_id": None,
                "last_signal_key": None,
            })
            sig["trades"] = int(sig.get("trades", 0) or 0) + 1
            if _kyoto_mem_float(pnl, 0.0) > 0:
                sig["wins"] = int(sig.get("wins", 0) or 0) + 1
            else:
                sig["losses"] = int(sig.get("losses", 0) or 0) + 1
            if pnl is not None:
                sig["net_pnl"] = _kyoto_mem_float(sig.get("net_pnl", 0.0), 0.0) + _kyoto_mem_float(pnl, 0.0)
            elif rmult is not None:
                sig["net_pnl"] = _kyoto_mem_float(sig.get("net_pnl", 0.0), 0.0) + _kyoto_mem_float(rmult, 0.0)
            sig_recent = sig.setdefault("recent", [])
            sig_recent.append(rec)
            if len(sig_recent) > 30:
                del sig_recent[:-30]
            sig["last_regime"] = str(regime) if regime is not None else sig.get("last_regime", "unknown")
            sig["last_quality"] = quality if quality is not None else sig.get("last_quality")
            sig["last_signal_score"] = signal_score if signal_score is not None else sig.get("last_signal_score")
            sig["last_signal_type"] = signal_type if signal_type is not None else sig.get("last_signal_type")
            sig["last_setup_id"] = setup_id if setup_id is not None else sig.get("last_setup_id")
            sig["last_pattern_id"] = pattern_id if pattern_id is not None else sig.get("last_pattern_id")
            sig["last_signal_key"] = signal_key if signal_key is not None else sig.get("last_signal_key")
            sig["last_update"] = time.time()
            sig["avg_r"] = (sig.get("net_pnl", 0.0) / sig["trades"]) if sig["trades"] > 0 else 0.0

        if setup_id is not None:
            _kyoto_mem_update_nested_stats(setup_stats, setup_id, rec, pnl, rmult)
        if pattern_id is not None:
            _kyoto_mem_update_nested_stats(pattern_stats, pattern_id, rec, pnl, rmult)
        if signal_type is not None:
            _kyoto_mem_update_nested_stats(signal_type_stats, signal_type, rec, pnl, rmult)

        bucket["avg_r"] = (bucket.get("net_pnl", 0.0) / bucket["trades"]) if bucket["trades"] > 0 else 0.0
        bucket["last_update"] = time.time()
        _kyoto_mem_save()


def kyoto_memory_update(symbol, timeframe="H1", *, pnl=None, rmult=None, volatility=None, regime=None, threshold=None, atr=None, quality=None, signal_score=None, side=None, status=None, entry=None, exit_price=None, meta=None, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    trade_id = None
    if isinstance(meta, dict):
        trade_id = meta.get("trade_id", meta.get("ticket", meta.get("id")))
        signal_type = meta.get("signal_type", meta.get("signal", signal_type))
        setup_id = meta.get("setup_id", meta.get("setup", setup_id))
        pattern_id = meta.get("pattern_id", meta.get("pattern", pattern_id))
        signal_key = meta.get("signal_key", signal_key)
    if signal_key is None:
        signal_key = _kyoto_mem_signal_key_from_payload({
            "signal_type": signal_type,
            "setup_id": setup_id,
            "pattern_id": pattern_id,
            "signal": signal_type,
            "setup": setup_id,
            "pattern": pattern_id,
        })
    _kyoto_mem_update_bucket(
        symbol, timeframe,
        pnl=pnl, rmult=rmult, volatility=volatility, regime=regime, threshold=threshold, atr=atr,
        quality=quality, signal_score=signal_score, side=side, status=status, entry=entry, exit_price=exit_price,
        meta=meta, trade_id=trade_id, signal_type=signal_type, setup_id=setup_id, pattern_id=pattern_id, signal_key=signal_key
    )

def _kyoto_mem_adjust_quality(symbol, timeframe, base_score):
    try:
        prof = kyoto_memory_profile(symbol, timeframe)
        score = _kyoto_mem_float(base_score, 0.0)

        signal_trades = int(prof.get("signal_trades", 0) or 0)
        signal_win_rate = prof.get("signal_win_rate", None)
        signal_avg_r = prof.get("signal_avg_r", None)

        if signal_trades >= 8 and signal_win_rate is not None:
            if signal_win_rate >= 0.60 and (signal_avg_r is None or signal_avg_r > 0):
                score += 0.07
            elif signal_win_rate <= 0.40 or (signal_avg_r is not None and signal_avg_r < 0):
                score -= 0.10
        elif prof["trades"] >= 12:
            if prof["win_rate"] >= 0.60 and prof["avg_r"] > 0:
                score += 0.05
            elif prof["win_rate"] <= 0.40 or prof["avg_r"] < 0:
                score -= 0.08
        return _kyoto_mem_clamp(score, 0.0, 1.0)
    except Exception:
        return _kyoto_mem_clamp(base_score, 0.0, 1.0)


def _kyoto_mem_adjust_stop_tp(symbol, timeframe, base_stop_mult=4.0, base_tp_mult=6.0):
    try:
        prof = kyoto_memory_profile(symbol, timeframe)
        stop_mult = float(base_stop_mult) * float(prof.get("stop_mult", 4.0) / 4.0)
        tp_mult = float(base_tp_mult) * float(prof.get("tp_mult", 6.0) / 6.0)

        signal_trades = int(prof.get("signal_trades", 0) or 0)
        signal_win_rate = prof.get("signal_win_rate", None)
        signal_avg_r = prof.get("signal_avg_r", None)
        if signal_trades >= 8 and signal_win_rate is not None:
            if signal_win_rate >= 0.60 and (signal_avg_r is None or signal_avg_r > 0):
                stop_mult *= 0.97
                tp_mult *= 1.03
            elif signal_win_rate <= 0.40 or (signal_avg_r is not None and signal_avg_r < 0):
                stop_mult *= 1.05
                tp_mult *= 0.95

        stop_mult = _kyoto_mem_clamp(stop_mult, 3.0, 5.5)
        tp_mult = _kyoto_mem_clamp(tp_mult, 4.5, 8.0)
        return stop_mult, tp_mult, prof
    except Exception:
        return float(base_stop_mult), float(base_tp_mult), {}

# Replace the live symbol inference/update helpers with the hardened versions above.
globals()['_kyoto_mem_infer_symbol'] = _kyoto_mem_infer_symbol
globals()['_kyoto_mem_infer_timeframe'] = _kyoto_mem_infer_timeframe
globals()['_kyoto_mem_record_from_args_kwargs'] = _kyoto_mem_record_from_args_kwargs
globals()['_kyoto_mem_update_bucket'] = _kyoto_mem_update_bucket
globals()['kyoto_memory_update'] = kyoto_memory_update
globals()['_kyoto_mem_adjust_quality'] = _kyoto_mem_adjust_quality
globals()['_kyoto_mem_adjust_stop_tp'] = _kyoto_mem_adjust_stop_tp



_kyoto_mem_load()

try:
    _KYOTO_PREV_record_trade_MEMORY = globals().get("record_trade")
    if callable(_KYOTO_PREV_record_trade_MEMORY):
        def record_trade(*args, **kwargs):
            result = None
            try:
                result = _KYOTO_PREV_record_trade_MEMORY(*args, **kwargs)
            finally:
                try:
                    _kyoto_mem_record_from_args_kwargs(args, kwargs, result=result)
                except Exception:
                    try:
                        logger.exception("KYOTO memory record_trade wrapper failed")
                    except Exception:
                        pass
            return result
        globals()["record_trade"] = record_trade
        try:
            logger.info("KYOTO memory layer wrapped record_trade")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("Failed to install KYOTO memory record_trade wrapper")
    except Exception:
        pass

try:
    _KYOTO_PREV_make_decision_for_symbol_MEMORY = globals().get("make_decision_for_symbol")
    if callable(_KYOTO_PREV_make_decision_for_symbol_MEMORY):
        def make_decision_for_symbol(symbol: str, live: bool=False):
            sym = _kyoto_mem_symbol(symbol)
            tf = "H1"
            try:
                _kyoto_mem_set_context(symbol=sym, timeframe=tf)
                decision = _KYOTO_PREV_make_decision_for_symbol_MEMORY(symbol, live)
                if isinstance(decision, dict):
                    decision_signal_context = {
                        "signal_type": decision.get("signal_type", decision.get("signal", None)),
                        "setup_id": decision.get("setup_id", decision.get("setup", None)),
                        "pattern_id": decision.get("pattern_id", decision.get("pattern", None)),
                        "signal_key": decision.get("signal_key", None),
                    }
                    snapshot = {
                        "regime": decision.get("regime"),
                        "volatility": decision.get("volatility_score", decision.get("volatility", None)),
                        "threshold": decision.get("threshold", None),
                        "quality": decision.get("quality", decision.get("ai_quality", None)),
                        "signal_score": decision.get("final", decision.get("signal", None)),
                        "signal_type": decision_signal_context.get("signal_type"),
                        "setup_id": decision_signal_context.get("setup_id"),
                        "pattern_id": decision_signal_context.get("pattern_id"),
                        "signal_key": decision_signal_context.get("signal_key"),
                        "side": decision.get("side"),
                        "entry": decision.get("entry"),
                    }
                    _kyoto_mem_queue_pending(sym, tf, snapshot)
                    _kyoto_mem_set_context(**decision_signal_context)
                    if decision.get("final") is not None:
                        try:
                            decision["memory_profile"] = kyoto_memory_profile(sym, tf)
                            decision["quality"] = _kyoto_mem_adjust_quality(sym, tf, decision.get("quality", 0.0))
                        except Exception:
                            pass
                return decision
            finally:
                _kyoto_mem_clear_context()
        globals()["make_decision_for_symbol"] = make_decision_for_symbol
        try:
            logger.info("KYOTO memory layer wrapped make_decision_for_symbol")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("Failed to install KYOTO memory decision wrapper")
    except Exception:
        pass

try:
    _KYOTO_PREV_ai_signal_quality_MEMORY = globals().get("ai_signal_quality")
    if callable(_KYOTO_PREV_ai_signal_quality_MEMORY):
        def ai_signal_quality(*args, **kwargs):
            base = 0.0
            try:
                base = float(_KYOTO_PREV_ai_signal_quality_MEMORY(*args, **kwargs) or 0.0)
            except Exception:
                base = 0.0
            symbol = kwargs.get("symbol") if isinstance(kwargs, dict) else None
            if symbol is None:
                for a in args:
                    if isinstance(a, str):
                        symbol = a
                        break
            tf = kwargs.get("timeframe", kwargs.get("tf", None)) if isinstance(kwargs, dict) else None
            if tf is None:
                tf = _kyoto_mem_get_context("timeframe", "H1")
            return _kyoto_mem_adjust_quality(symbol, tf, base)
        globals()["ai_signal_quality"] = ai_signal_quality
        try:
            logger.info("KYOTO memory layer wrapped ai_signal_quality")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("Failed to install KYOTO ai_signal_quality memory wrapper")
    except Exception:
        pass

try:
    _KYOTO_PREV_regime_adaptive_stop_MEMORY = globals().get("regime_adaptive_stop")
    if callable(_KYOTO_PREV_regime_adaptive_stop_MEMORY):
        def regime_adaptive_stop(*args, **kwargs):
            res = None
            try:
                res = _KYOTO_PREV_regime_adaptive_stop_MEMORY(*args, **kwargs)
            except Exception:
                res = None
            try:
                entry = None
                side = None
                symbol = kwargs.get("symbol") if isinstance(kwargs, dict) else None
                tf = kwargs.get("timeframe", kwargs.get("tf", None)) if isinstance(kwargs, dict) else None
                if tf is None:
                    tf = _kyoto_mem_get_context("timeframe", "H1")
                if symbol is None:
                    symbol = _kyoto_mem_get_context("symbol", None)
                if len(args) >= 3:
                    try:
                        entry = float(args[0])
                    except Exception:
                        entry = None
                    side = args[2]
                if not isinstance(res, (tuple, list)) or len(res) < 3 or entry is None:
                    return res
                stop_mult, tp_mult, _prof = _kyoto_mem_adjust_stop_tp(symbol, tf, 4.0, 6.0)
                sl, tp, stop_dist = res[0], res[1], res[2]
                try:
                    base_dist = abs(float(entry) - float(sl))
                except Exception:
                    base_dist = 0.0
                if base_dist > 0:
                    side_s = str(side).lower()
                    if side_s in ("buy", "long", "1", "bull", "up"):
                        sl = float(entry) - base_dist * (stop_mult / 4.0)
                        tp = float(entry) + base_dist * (tp_mult / 6.0)
                    elif side_s in ("sell", "short", "-1", "bear", "down"):
                        sl = float(entry) + base_dist * (stop_mult / 4.0)
                        tp = float(entry) - base_dist * (tp_mult / 6.0)
                    return (sl, tp, abs(float(entry) - float(sl)))
                return res
            except Exception:
                return res
        globals()["regime_adaptive_stop"] = regime_adaptive_stop
        try:
            logger.info("KYOTO memory layer wrapped regime_adaptive_stop")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("Failed to install KYOTO regime_adaptive_stop memory wrapper")
    except Exception:
        pass

globals()["_KYOTO_MEMORY_STATE"] = _KYOTO_MEMORY_STATE
globals()["_KYOTO_MEMORY_FILE"] = _KYOTO_MEMORY_FILE
globals()["kyoto_memory_profile"] = kyoto_memory_profile
globals()["kyoto_memory_update"] = kyoto_memory_update
globals()["_kyoto_mem_adjust_quality"] = _kyoto_mem_adjust_quality
globals()["_kyoto_mem_adjust_stop_tp"] = _kyoto_mem_adjust_stop_tp

try:
    _KYOTO_STATS_MANAGER_CLASS = globals().get('StatsManager')
    if _KYOTO_STATS_MANAGER_CLASS is not None and hasattr(_KYOTO_STATS_MANAGER_CLASS, 'record_trade'):
        logger.info('KYOTO memory hardening: StatsManager kept legacy-only; adaptation memory is isolated from global stats.')
except Exception:
    try:
        logger.exception('KYOTO memory hardening could not inspect StatsManager')
    except Exception:
        pass

# --- END KYOTO MEMORY LAYER HARDENING (v2) ---
# --- END KYOTO MEMORY LAYER ---


# --- PART 2 INTELLIGENCE LAYER: setup-aware adaptive calibration + signal quality control ---
# This layer sits on top of the strengthened memory bucket and makes the bot
# behave like a context-aware system:
#   - signal quality is adjusted from the exact signal/setup/pattern history
#   - stop-loss / take-profit calibration uses the same local history
#   - decision -> execution context is preserved so the trade wrapper can
#     see the same setup metadata used by the signal engine

try:
    _KYOTO_PART2_LOCK
except NameError:
    _KYOTO_PART2_LOCK = threading.RLock() if "threading" in globals() else None

try:
    _KYOTO_PART2_LAST_DECISION
except NameError:
    _KYOTO_PART2_LAST_DECISION = {}

try:
    _KYOTO_PART2_LAST_DECISION_TTL_SECONDS
except NameError:
    _KYOTO_PART2_LAST_DECISION_TTL_SECONDS = int(globals().get("_KYOTO_PART2_LAST_DECISION_TTL_SECONDS", 120))


def _kyoto_part2_symbol_key(symbol):
    try:
        return _kyoto_mem_symbol(symbol)
    except Exception:
        try:
            return str(symbol).upper()
        except Exception:
            return str(symbol)


def _kyoto_part2_cleanup_last_decisions(now=None):
    try:
        if now is None:
            now = time.time()
        ttl = int(globals().get("_KYOTO_PART2_LAST_DECISION_TTL_SECONDS", 120))
        dead = []
        for key, meta in list(globals().get("_KYOTO_PART2_LAST_DECISION", {}).items()):
            try:
                ts = float(meta.get("ts", 0.0) or 0.0)
                if now - ts > ttl:
                    dead.append(key)
            except Exception:
                dead.append(key)
        for key in dead:
            globals()["_KYOTO_PART2_LAST_DECISION"].pop(key, None)
    except Exception:
        pass


def _kyoto_part2_set_last_decision(symbol, payload):
    try:
        sym = _kyoto_part2_symbol_key(symbol)
        entry = dict(payload or {})
        entry["ts"] = time.time()
        globals().setdefault("_KYOTO_PART2_LAST_DECISION", {})[sym] = entry
        _kyoto_part2_cleanup_last_decisions()
    except Exception:
        pass


def _kyoto_part2_take_last_decision(symbol):
    try:
        sym = _kyoto_part2_symbol_key(symbol)
        _kyoto_part2_cleanup_last_decisions()
        return dict(globals().get("_KYOTO_PART2_LAST_DECISION", {}).get(sym, {}) or {})
    except Exception:
        return {}


def _kyoto_mem_profile_for_context(symbol, timeframe="H1", *, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    """
    Fetch kyoto_memory_profile under a temporary context so the profile can
    resolve the exact signal/setup/pattern bucket for the current trade idea.
    """
    prev = {}
    try:
        prev = {
            "signal_type": _kyoto_mem_get_context("signal_type", None),
            "setup_id": _kyoto_mem_get_context("setup_id", None),
            "pattern_id": _kyoto_mem_get_context("pattern_id", None),
            "signal_key": _kyoto_mem_get_context("signal_key", None),
        }
    except Exception:
        prev = {}
    try:
        _kyoto_mem_set_context(
            signal_type=signal_type,
            setup_id=setup_id,
            pattern_id=pattern_id,
            signal_key=signal_key,
        )
        return kyoto_memory_profile(symbol, timeframe)
    except Exception:
        return kyoto_memory_profile(symbol, timeframe)
    finally:
        try:
            _kyoto_mem_set_context(**prev)
        except Exception:
            pass


def _kyoto_mem_part2_grade_profile(profile):
    """
    Translate a memory profile into bounded adaptive adjustments.
    Returns:
      quality_delta, quality_floor, threshold_factor, stop_mult, tp_mult, confidence
    """
    try:
        prof = dict(profile or {})
    except Exception:
        prof = {}

    trades = int(prof.get("trades", 0) or 0)
    win_rate = prof.get("win_rate", None)
    avg_r = prof.get("avg_r", None)

    signal_trades = int(prof.get("signal_trades", 0) or 0)
    signal_win_rate = prof.get("signal_win_rate", None)
    signal_avg_r = prof.get("signal_avg_r", None)

    # Base values keep the system conservative until enough history exists.
    quality_delta = 0.0
    quality_floor = 0.35
    threshold_factor = float(prof.get("threshold_factor", 1.0) or 1.0)
    stop_mult = float(prof.get("stop_mult", 4.0) or 4.0)
    tp_mult = float(prof.get("tp_mult", 6.0) or 6.0)
    confidence = 0.0

    # Prefer signal-specific stats when available, then setup/pattern stats,
    # then broader symbol/timeframe stats.
    if signal_trades >= 8 and signal_win_rate is not None:
        confidence = min(1.0, signal_trades / 20.0)
        if signal_win_rate >= 0.65 and (signal_avg_r is None or signal_avg_r > 0):
            quality_delta += 0.12
            quality_floor = 0.30
            threshold_factor *= 0.92
            stop_mult *= 0.96
            tp_mult *= 1.06
        elif signal_win_rate <= 0.40 or (signal_avg_r is not None and signal_avg_r < 0):
            quality_delta -= 0.16
            quality_floor = 0.42
            threshold_factor *= 1.10
            stop_mult *= 1.08
            tp_mult *= 0.94
        else:
            quality_delta += 0.02 if signal_win_rate >= 0.50 else -0.03
            quality_floor = 0.34

    elif trades >= 12 and win_rate is not None:
        confidence = min(1.0, trades / 30.0)
        if win_rate >= 0.62 and (avg_r is None or avg_r > 0):
            quality_delta += 0.07
            quality_floor = 0.31
            threshold_factor *= 0.96
            stop_mult *= 0.97
            tp_mult *= 1.04
        elif win_rate <= 0.40 or (avg_r is not None and avg_r < 0):
            quality_delta -= 0.11
            quality_floor = 0.40
            threshold_factor *= 1.08
            stop_mult *= 1.05
            tp_mult *= 0.96
        else:
            quality_delta += 0.01
            quality_floor = 0.35

    # A slightly higher threshold for tiny samples, but never extreme.
    if trades < 8 and signal_trades < 8:
        quality_floor = 0.37
        threshold_factor *= 1.02

    # Keep bounded.
    quality_delta = max(-0.25, min(0.25, quality_delta))
    quality_floor = max(0.20, min(0.55, quality_floor))
    threshold_factor = max(0.85, min(1.18, threshold_factor))
    stop_mult = max(3.0, min(5.5, stop_mult))
    tp_mult = max(4.5, min(8.0, tp_mult))
    confidence = max(0.0, min(1.0, confidence))

    return {
        "quality_delta": float(quality_delta),
        "quality_floor": float(quality_floor),
        "threshold_factor": float(threshold_factor),
        "stop_mult": float(stop_mult),
        "tp_mult": float(tp_mult),
        "confidence": float(confidence),
        "trades": int(trades),
        "win_rate": win_rate,
        "avg_r": avg_r,
        "signal_trades": int(signal_trades),
        "signal_win_rate": signal_win_rate,
        "signal_avg_r": signal_avg_r,
    }


def _kyoto_mem_part2_memory_adjustments(symbol, timeframe="H1", *, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    prof = _kyoto_mem_profile_for_context(
        symbol,
        timeframe,
        signal_type=signal_type,
        setup_id=setup_id,
        pattern_id=pattern_id,
        signal_key=signal_key,
    )
    return _kyoto_mem_part2_grade_profile(prof), prof


def _kyoto_mem_part2_adaptive_quality(symbol, timeframe="H1", base_score=0.0, *, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    try:
        (adj, _prof) = _kyoto_mem_part2_memory_adjustments(
            symbol,
            timeframe,
            signal_type=signal_type,
            setup_id=setup_id,
            pattern_id=pattern_id,
            signal_key=signal_key,
        )
        score = _kyoto_mem_float(base_score, 0.0)
        score += adj["quality_delta"]
        if adj["confidence"] >= 0.6:
            # Stronger confidence lets the memory slightly influence the score.
            score += 0.03 if adj["quality_delta"] > 0 else -0.02
        return _kyoto_mem_clamp(score, 0.0, 1.0)
    except Exception:
        return _kyoto_mem_clamp(base_score, 0.0, 1.0)


def _kyoto_mem_part2_adaptive_stop_tp(symbol, timeframe="H1", base_stop_mult=4.0, base_tp_mult=6.0, *, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    try:
        (adj, prof) = _kyoto_mem_part2_memory_adjustments(
            symbol,
            timeframe,
            signal_type=signal_type,
            setup_id=setup_id,
            pattern_id=pattern_id,
            signal_key=signal_key,
        )
        stop_mult = _kyoto_mem_float(base_stop_mult, 4.0) * (adj["stop_mult"] / 4.0)
        tp_mult = _kyoto_mem_float(base_tp_mult, 6.0) * (adj["tp_mult"] / 6.0)

        # Add a light volatility overlay using the current bucket's recent vol.
        try:
            vol_ratio = float(prof.get("vol_ratio", 1.0) or 1.0)
            vol_ratio = max(0.70, min(1.30, vol_ratio))
            stop_mult *= vol_ratio
            tp_mult *= vol_ratio
        except Exception:
            pass

        stop_mult = _kyoto_mem_clamp(stop_mult, 3.0, 5.5)
        tp_mult = _kyoto_mem_clamp(tp_mult, 4.5, 8.0)
        return stop_mult, tp_mult, prof, adj
    except Exception:
        return float(base_stop_mult), float(base_tp_mult), {}, {
            "quality_delta": 0.0,
            "quality_floor": 0.35,
            "threshold_factor": 1.0,
            "stop_mult": float(base_stop_mult),
            "tp_mult": float(base_tp_mult),
            "confidence": 0.0,
        }


# Expose the part 2 helpers so later wrappers and the live bot can use them.
globals()["_KYOTO_PART2_LAST_DECISION"] = globals().get("_KYOTO_PART2_LAST_DECISION", {})
globals()["_kyoto_part2_set_last_decision"] = _kyoto_part2_set_last_decision
globals()["_kyoto_part2_take_last_decision"] = _kyoto_part2_take_last_decision
globals()["_kyoto_mem_profile_for_context"] = _kyoto_mem_profile_for_context
globals()["_kyoto_mem_part2_grade_profile"] = _kyoto_mem_part2_grade_profile
globals()["_kyoto_mem_part2_memory_adjustments"] = _kyoto_mem_part2_memory_adjustments
globals()["_kyoto_mem_part2_adaptive_quality"] = _kyoto_mem_part2_adaptive_quality
globals()["_kyoto_mem_part2_adaptive_stop_tp"] = _kyoto_mem_part2_adaptive_stop_tp

# Re-wrap the adaptive quality function with the part 2 logic.
try:
    _KYOTO_PART2_PREV_ai_signal_quality = globals().get("ai_signal_quality")
    if callable(_KYOTO_PART2_PREV_ai_signal_quality):
        def ai_signal_quality(*args, **kwargs):
            base = 0.0
            try:
                base = float(_KYOTO_PART2_PREV_ai_signal_quality(*args, **kwargs) or 0.0)
            except Exception:
                base = 0.0

            symbol = kwargs.get("symbol") if isinstance(kwargs, dict) else None
            if symbol is None:
                for a in args:
                    if isinstance(a, str):
                        symbol = a
                        break

            tf = None
            if isinstance(kwargs, dict):
                tf = kwargs.get("timeframe", kwargs.get("tf", None))
            if tf is None:
                try:
                    tf = _kyoto_mem_get_context("timeframe", "H1")
                except Exception:
                    tf = "H1"
            signal_type = kwargs.get("signal_type") if isinstance(kwargs, dict) else None
            setup_id = kwargs.get("setup_id") if isinstance(kwargs, dict) else None
            pattern_id = kwargs.get("pattern_id") if isinstance(kwargs, dict) else None
            signal_key = kwargs.get("signal_key") if isinstance(kwargs, dict) else None
            if signal_type is None:
                signal_type = _kyoto_mem_get_context("signal_type", None)
            if setup_id is None:
                setup_id = _kyoto_mem_get_context("setup_id", None)
            if pattern_id is None:
                pattern_id = _kyoto_mem_get_context("pattern_id", None)
            if signal_key is None:
                signal_key = _kyoto_mem_get_context("signal_key", None)

            return _kyoto_mem_part2_adaptive_quality(
                symbol,
                tf,
                base_score=base,
                signal_type=signal_type,
                setup_id=setup_id,
                pattern_id=pattern_id,
                signal_key=signal_key,
            )
        globals()["ai_signal_quality"] = ai_signal_quality
        try:
            logger.info("PART 2: setup-aware ai_signal_quality installed")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 2: failed to wrap ai_signal_quality")
    except Exception:
        pass

# Re-wrap stop/TP calibration with the part 2 logic.
try:
    _KYOTO_PART2_PREV_regime_adaptive_stop = globals().get("regime_adaptive_stop")
    if callable(_KYOTO_PART2_PREV_regime_adaptive_stop):
        def regime_adaptive_stop(*args, **kwargs):
            res = None
            try:
                res = _KYOTO_PART2_PREV_regime_adaptive_stop(*args, **kwargs)
            except Exception:
                res = None

            try:
                entry = None
                side = None
                symbol = kwargs.get("symbol") if isinstance(kwargs, dict) else None
                tf = kwargs.get("timeframe", kwargs.get("tf", None)) if isinstance(kwargs, dict) else None
                if tf is None:
                    tf = _kyoto_mem_get_context("timeframe", "H1")
                if symbol is None:
                    symbol = _kyoto_mem_get_context("symbol", None)
                if len(args) >= 3:
                    try:
                        entry = float(args[0])
                    except Exception:
                        entry = None
                    side = args[2]

                if not isinstance(res, (tuple, list)) or len(res) < 3 or entry is None:
                    return res

                signal_type = kwargs.get("signal_type") if isinstance(kwargs, dict) else None
                setup_id = kwargs.get("setup_id") if isinstance(kwargs, dict) else None
                pattern_id = kwargs.get("pattern_id") if isinstance(kwargs, dict) else None
                signal_key = kwargs.get("signal_key") if isinstance(kwargs, dict) else None
                if signal_type is None:
                    signal_type = _kyoto_mem_get_context("signal_type", None)
                if setup_id is None:
                    setup_id = _kyoto_mem_get_context("setup_id", None)
                if pattern_id is None:
                    pattern_id = _kyoto_mem_get_context("pattern_id", None)
                if signal_key is None:
                    signal_key = _kyoto_mem_get_context("signal_key", None)

                stop_mult, tp_mult, prof, adj = _kyoto_mem_part2_adaptive_stop_tp(
                    symbol,
                    tf,
                    4.0,
                    6.0,
                    signal_type=signal_type,
                    setup_id=setup_id,
                    pattern_id=pattern_id,
                    signal_key=signal_key,
                )

                sl, tp, stop_dist = res[0], res[1], res[2]
                try:
                    base_dist = abs(float(entry) - float(sl))
                except Exception:
                    base_dist = 0.0

                if base_dist > 0:
                    side_s = str(side).lower()
                    if side_s in ("buy", "long", "1", "bull", "up"):
                        sl = float(entry) - base_dist * (stop_mult / 4.0)
                        tp = float(entry) + base_dist * (tp_mult / 6.0)
                    elif side_s in ("sell", "short", "-1", "bear", "down"):
                        sl = float(entry) + base_dist * (stop_mult / 4.0)
                        tp = float(entry) - base_dist * (tp_mult / 6.0)
                    return (sl, tp, abs(float(entry) - float(sl)))
                return res
            except Exception:
                return res
        globals()["regime_adaptive_stop"] = regime_adaptive_stop
        try:
            logger.info("PART 2: setup-aware regime_adaptive_stop installed")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 2: failed to wrap regime_adaptive_stop")
    except Exception:
        pass

# Preserve setup metadata from decision to execution in the same symbol stream.
try:
    _KYOTO_PART2_PREV_make_decision_for_symbol = globals().get("make_decision_for_symbol")
    if callable(_KYOTO_PART2_PREV_make_decision_for_symbol):
        def make_decision_for_symbol(symbol: str, live: bool=False):
            sym = _kyoto_part2_symbol_key(symbol)
            tf = "H1"
            try:
                _kyoto_mem_set_context(symbol=sym, timeframe=tf)
                decision = _KYOTO_PART2_PREV_make_decision_for_symbol(symbol, live)
                if isinstance(decision, dict):
                    signal_context = {
                        "signal_type": decision.get("signal_type", decision.get("signal", None)),
                        "setup_id": decision.get("setup_id", decision.get("setup", None)),
                        "pattern_id": decision.get("pattern_id", decision.get("pattern", None)),
                        "signal_key": decision.get("signal_key", None),
                    }
                    memory_profile = _kyoto_mem_profile_for_context(
                        sym,
                        tf,
                        signal_type=signal_context.get("signal_type"),
                        setup_id=signal_context.get("setup_id"),
                        pattern_id=signal_context.get("pattern_id"),
                        signal_key=signal_context.get("signal_key"),
                    )
                    grade, _prof = _kyoto_mem_part2_grade_profile(memory_profile), memory_profile
                    decision["memory_profile"] = memory_profile
                    decision["memory_grade"] = grade
                    decision["adaptive_quality"] = _kyoto_mem_part2_adaptive_quality(
                        sym,
                        tf,
                        base_score=decision.get("quality", decision.get("ai_quality", 0.0) or 0.0),
                        signal_type=signal_context.get("signal_type"),
                        setup_id=signal_context.get("setup_id"),
                        pattern_id=signal_context.get("pattern_id"),
                        signal_key=signal_context.get("signal_key"),
                    )
                    decision["quality_floor"] = grade.get("quality_floor", 0.35)
                    decision["adaptive_threshold_factor"] = grade.get("threshold_factor", 1.0)
                    decision["adaptive_stop_mult"] = grade.get("stop_mult", 4.0)
                    decision["adaptive_tp_mult"] = grade.get("tp_mult", 6.0)

                    # Keep a short-lived copy of the exact decision context for the execution layer.
                    _kyoto_part2_set_last_decision(sym, {
                        "symbol": sym,
                        "timeframe": tf,
                        **signal_context,
                        "quality_floor": decision.get("quality_floor"),
                        "threshold_factor": decision.get("adaptive_threshold_factor"),
                        "adaptive_quality": decision.get("adaptive_quality"),
                        "adaptive_stop_mult": decision.get("adaptive_stop_mult"),
                        "adaptive_tp_mult": decision.get("adaptive_tp_mult"),
                        "regime": decision.get("regime"),
                        "entry": decision.get("entry"),
                        "quality": decision.get("adaptive_quality"),
                    })
                    try:
                        _kyoto_mem_set_context(**signal_context)
                    except Exception:
                        pass
                return decision
            finally:
                try:
                    _kyoto_mem_clear_context()
                except Exception:
                    pass
        globals()["make_decision_for_symbol"] = make_decision_for_symbol
        try:
            logger.info("PART 2: decision-to-execution signal context installed")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 2: failed to wrap make_decision_for_symbol")
    except Exception:
        pass

# Make execution use the latest decision context and the setup-aware thresholds.
try:
    _KYOTO_PART2_PREV_execute_signal = globals().get("execute_signal")
    if callable(_KYOTO_PART2_PREV_execute_signal):
        def execute_signal(sym, signal, price, mt5_module, symbol_map):
            try:
                sym_u = _kyoto_part2_symbol_key(sym)
                live_ctx = dict(_kyoto_ctx_get() or {})
                last_decision = _kyoto_part2_take_last_decision(sym_u)
                if last_decision:
                    for k in ("signal_type", "setup_id", "pattern_id", "signal_key", "quality_floor", "threshold_factor", "adaptive_quality", "adaptive_stop_mult", "adaptive_tp_mult", "regime", "entry"):
                        if last_decision.get(k) is not None and live_ctx.get(k) is None:
                            live_ctx[k] = last_decision.get(k)

                if "df_h1" not in live_ctx or live_ctx.get("df_h1") is None:
                    try:
                        live_ctx["df_h1"] = _kyoto_h1_df(mt5_module, symbol_map or {}, sym_u, 160)
                    except Exception:
                        live_ctx["df_h1"] = None

                if live_ctx.get("df_h1") is not None and live_ctx.get("regime") in (None, "unknown"):
                    try:
                        live_ctx["regime"] = detect_market_regime_from_h1(live_ctx["df_h1"])[0]
                    except Exception:
                        live_ctx["regime"] = "unknown"

                # Prefer the adaptive quality from the decision layer when available.
                if live_ctx.get("adaptive_quality") is None:
                    base_q = abs(float(signal)) if signal is not None else 0.0
                    live_ctx["adaptive_quality"] = _kyoto_mem_part2_adaptive_quality(
                        sym_u,
                        "H1",
                        base_q,
                        signal_type=live_ctx.get("signal_type"),
                        setup_id=live_ctx.get("setup_id"),
                        pattern_id=live_ctx.get("pattern_id"),
                        signal_key=live_ctx.get("signal_key"),
                    )
                live_ctx["quality"] = live_ctx.get("adaptive_quality", abs(float(signal)) if signal is not None else 0.0)
                if live_ctx.get("quality_floor") is None:
                    live_ctx["quality_floor"] = 0.35
                if live_ctx.get("threshold_factor") is None:
                    live_ctx["threshold_factor"] = 1.0
                if live_ctx.get("entry") is None:
                    live_ctx["entry"] = float(price or 0.0)
                live_ctx["signal"] = float(signal) if signal is not None else 0.0
                live_ctx["symbol"] = sym_u
                _kyoto_ctx_set(**live_ctx)
                try:
                    return _KYOTO_PART2_PREV_execute_signal(sym, signal, price, mt5_module, symbol_map)
                finally:
                    try:
                        _kyoto_ctx_clear()
                    except Exception:
                        pass
            except Exception:
                try:
                    _kyoto_ctx_clear()
                except Exception:
                    pass
                return _KYOTO_PART2_PREV_execute_signal(sym, signal, price, mt5_module, symbol_map)
        globals()["execute_signal"] = execute_signal
        try:
            logger.info("PART 2: execution context now includes setup-aware signal memory")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 2: failed to wrap execute_signal")
    except Exception:
        pass

# Final order-stage guards: let the quality floor and adaptive multipliers follow the memory layer.
try:
    _KYOTO_PART2_PREV_place_order_mt5 = globals().get("place_order_mt5")
    if callable(_KYOTO_PART2_PREV_place_order_mt5):
        def place_order_mt5(symbol, action, lot, price, sl, tp):
            ctx = _kyoto_ctx_get() or {}
            try:
                # Lower floor only when the exact setup is proven; otherwise keep it strict.
                q_floor = ctx.get("quality_floor", None)
                if q_floor is None:
                    q_floor = 0.35
                q_val = ctx.get("quality")
                if q_val is not None and float(q_val) < float(q_floor):
                    return {"status": "skipped", "comment": "quality_below_threshold", "symbol": symbol, "quality_floor": float(q_floor)}
                if ctx.get("regime") in ("ranging", "sideways", "choppy"):
                    return {"status": "skipped", "comment": f"regime_{ctx.get('regime')}", "symbol": symbol}

                df_h1 = ctx.get("df_h1")
                if df_h1 is not None:
                    side = "BUY" if str(action).lower() in ("buy", "long", "0", "1") else "SELL"
                    try:
                        calc_sl, calc_tp, _sd = regime_adaptive_stop(float(price or ctx.get("entry") or 0.0), df_h1, side)
                        if not sl:
                            sl = calc_sl
                        if not tp:
                            tp = calc_tp
                    except Exception:
                        pass

                if (sl is None or float(sl) == 0.0 or tp is None or float(tp) == 0.0) and price is not None:
                    px = float(price)
                    sd = max(1e-6, abs(px) * 0.005)
                    if str(action).lower() in ("buy", "long", "0", "1"):
                        sl = px - sd
                        tp = px + sd * 1.5
                    else:
                        sl = px + sd
                        tp = px - sd * 1.5
            except Exception:
                pass
            return _KYOTO_PART2_PREV_place_order_mt5(symbol, action, lot, price, sl, tp)
        globals()["place_order_mt5"] = place_order_mt5
        try:
            logger.info("PART 2: place_order_mt5 now uses adaptive quality floor and setup-aware stop placement")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 2: failed to wrap place_order_mt5")
    except Exception:
        pass

try:
    _KYOTO_PART2_PREV_order_wrapper = globals().get("order_wrapper")
    if callable(_KYOTO_PART2_PREV_order_wrapper):
        def order_wrapper(mt5_module, order_request):
            req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
            ctx = _kyoto_ctx_get() or {}
            try:
                q_floor = ctx.get("quality_floor", None)
                if q_floor is None:
                    q_floor = 0.35
                q_val = ctx.get("quality")
                if q_val is not None and float(q_val) < float(q_floor):
                    return {"retcode": -1, "comment": "quality_below_threshold", "request": req, "quality_floor": float(q_floor)}
                if ctx.get("regime") in ("ranging", "sideways", "choppy"):
                    return {"retcode": -1, "comment": f"regime_{ctx.get('regime')}", "request": req}

                if ctx.get("df_h1") is not None:
                    side = "BUY" if str(req.get("type", req.get("side", ""))).lower() in ("buy", "long", "0", "1") else "SELL"
                    try:
                        calc_sl, calc_tp, _sd = regime_adaptive_stop(float(req.get("price") or ctx.get("entry") or 0.0), ctx["df_h1"], side)
                        if not req.get("sl"):
                            req["sl"] = calc_sl
                        if not req.get("tp"):
                            req["tp"] = calc_tp
                    except Exception:
                        pass

                if (req.get("sl") in (None, 0, 0.0, "")) or (req.get("tp") in (None, 0, 0.0, "")):
                    px = float(req.get("price") or ctx.get("entry") or 0.0)
                    if px > 0:
                        sd = max(1e-6, abs(px) * 0.005)
                        if str(req.get("type", req.get("side", ""))).lower() in ("buy", "long", "0", "1"):
                            req["sl"] = px - sd
                            req["tp"] = px + sd * 1.5
                        else:
                            req["sl"] = px + sd
                            req["tp"] = px - sd * 1.5
            except Exception:
                pass
            return _KYOTO_PART2_PREV_order_wrapper(mt5_module, req)
        globals()["order_wrapper"] = order_wrapper
        try:
            logger.info("PART 2: order_wrapper now respects setup-aware quality floor and adaptive exits")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 2: failed to wrap order_wrapper")
    except Exception:
        pass

try:
    if "CONFIG" in globals():
        # keep the existing strong execution guard, but make the adaptive layer visible
        CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.30))
except Exception:
    pass

# Make the part 2 helpers visible for later layers / debugging.
globals()["_KYOTO_PART2_LAST_DECISION"] = globals().get("_KYOTO_PART2_LAST_DECISION", {})
# --- END PART 2 INTELLIGENCE LAYER ---


# --- PART 3 LIVE RUNTIME LAYER (fresh-start, live-only, warm-up aware) ---
try:
    _KYOTO_PART3_RUNTIME = dict(globals().get("_KYOTO_PART3_RUNTIME", {}) or {})
    _KYOTO_PART3_RUNTIME.update({
        "mode": "live_only",
        "bootstrapped": False,
        "fresh_start": True,
        "bootstrap_version": 3,
        "legacy_stats_disabled": True,
    })
    globals()["_KYOTO_PART3_RUNTIME"] = _KYOTO_PART3_RUNTIME

    _KYOTO_PART3_BOOTSTRAP_FILE = os.path.join(os.path.dirname(__file__), "kyoto_part3_bootstrap.json")
    _KYOTO_PART3_MIN_TOTAL_TRADES = int(os.getenv("KYOTO_PART3_MIN_TOTAL_TRADES", "20"))
    _KYOTO_PART3_MIN_SIGNAL_TRADES = int(os.getenv("KYOTO_PART3_MIN_SIGNAL_TRADES", "10"))
    _KYOTO_PART3_MIN_SETUP_TRADES = int(os.getenv("KYOTO_PART3_MIN_SETUP_TRADES", "10"))
    _KYOTO_PART3_MIN_PATTERN_TRADES = int(os.getenv("KYOTO_PART3_MIN_PATTERN_TRADES", "10"))
    _KYOTO_PART3_LOG_EVERY_SYMBOL = True
    _KYOTO_PART3_DISABLE_LEGACY_STATS = True
except Exception:
    pass


def _kyoto_part3_bootstrap_state():
    try:
        if os.path.exists(_KYOTO_PART3_BOOTSTRAP_FILE):
            with open(_KYOTO_PART3_BOOTSTRAP_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        try:
            logger.exception("PART 3: failed to read bootstrap state")
        except Exception:
            pass
    return {}


def _kyoto_part3_save_bootstrap_state(state):
    try:
        tmp = _KYOTO_PART3_BOOTSTRAP_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, _KYOTO_PART3_BOOTSTRAP_FILE)
    except Exception:
        try:
            logger.exception("PART 3: failed to save bootstrap state")
        except Exception:
            pass


def _kyoto_part3_reset_memory_state(reason="fresh_start"):
    """
    Reset the adaptive KYOTO memory once so the live system does not inherit
    poisoned history from the earlier broken risk / threshold regime.
    """
    try:
        global _KYOTO_MEMORY_STATE
        legacy_path = _KYOTO_MEMORY_FILE
        if os.path.exists(legacy_path):
            try:
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                backup_path = legacy_path + f".pre_part3_{ts}.bak"
                if not os.path.exists(backup_path):
                    os.replace(legacy_path, backup_path)
                    logger.info("PART 3: backed up legacy memory to %s", backup_path)
            except Exception:
                try:
                    logger.exception("PART 3: failed to back up legacy memory")
                except Exception:
                    pass

        _KYOTO_MEMORY_STATE = {"version": 3, "symbols": {}}
        _kyoto_mem_save()
        _kyoto_part3_save_bootstrap_state({
            "bootstrapped": True,
            "reason": reason,
            "bootstrap_version": 3,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })
        _KYOTO_PART3_RUNTIME["bootstrapped"] = True
        _KYOTO_PART3_RUNTIME["fresh_start"] = True
        logger.info("PART 3: memory reset completed (%s)", reason)
    except Exception:
        try:
            logger.exception("PART 3: memory reset failed")
        except Exception:
            pass


def _kyoto_part3_initialize_runtime():
    """
    Force the bot into a live-only runtime mode with warm-up-aware learning.
    """
    try:
        global DEMO_SIMULATION, AUTO_EXECUTE
        DEMO_SIMULATION = False
        AUTO_EXECUTE = True

        try:
            if "CONFIG" in globals() and isinstance(CONFIG, dict):
                CONFIG["DRY_RUN_FLAG"] = False
                CONFIG["EXECUTION_SIGNAL_THRESHOLD"] = float(CONFIG.get("EXECUTION_SIGNAL_THRESHOLD", 0.30))
        except Exception:
            pass

        state = _kyoto_part3_bootstrap_state()
        if not state.get("bootstrapped"):
            _kyoto_part3_reset_memory_state(reason=state.get("reason", "live_only_bootstrap"))
        else:
            _KYOTO_PART3_RUNTIME["bootstrapped"] = True
            _KYOTO_PART3_RUNTIME["fresh_start"] = False

        logger.info(
            "PART 3: live-only runtime enabled (fresh_start=%s, min_total=%d, min_signal=%d)",
            _KYOTO_PART3_RUNTIME.get("fresh_start", False),
            _KYOTO_PART3_MIN_TOTAL_TRADES,
            _KYOTO_PART3_MIN_SIGNAL_TRADES,
        )
    except Exception:
        try:
            logger.exception("PART 3: runtime initialization failed")
        except Exception:
            pass


def _kyoto_part3_memory_status(symbol, timeframe="H1", *, signal_type=None, setup_id=None, pattern_id=None, signal_key=None):
    try:
        prof = _kyoto_mem_profile_for_context(
            symbol,
            timeframe,
            signal_type=signal_type,
            setup_id=setup_id,
            pattern_id=pattern_id,
            signal_key=signal_key,
        )
    except Exception:
        prof = kyoto_memory_profile(symbol, timeframe)
    try:
        total_trades = int(prof.get("trades", 0) or 0)
    except Exception:
        total_trades = 0
    try:
        signal_trades = int(prof.get("signal_trades", 0) or 0)
    except Exception:
        signal_trades = 0

    try:
        setup_active = bool(prof.get("setup_profile_active", False))
        pattern_active = bool(prof.get("pattern_profile_active", False))
        signal_type_active = bool(prof.get("signal_type_profile_active", False))
    except Exception:
        setup_active = pattern_active = signal_type_active = False

    warmup = (total_trades < _KYOTO_PART3_MIN_TOTAL_TRADES) or (signal_trades < _KYOTO_PART3_MIN_SIGNAL_TRADES)
    ready = not warmup
    quality_floor = 0.35 if warmup else float(prof.get("threshold_factor", 1.0) * 0.35)
    quality_floor = max(0.35, min(0.75, quality_floor))

    return {
        "profile": prof,
        "warmup": warmup,
        "ready": ready,
        "total_trades": total_trades,
        "signal_trades": signal_trades,
        "setup_active": setup_active,
        "pattern_active": pattern_active,
        "signal_type_active": signal_type_active,
        "quality_floor": quality_floor,
    }


def _kyoto_part3_merge_live_context(sym, signal=None, price=None):
    """
    Build the live execution context using the newest setup-aware decision context.
    """
    sym_u = _kyoto_part3_symbol_key(sym)
    ctx = dict(_kyoto_ctx_get() or {})
    last_decision = _kyoto_part2_take_last_decision(sym_u)
    if last_decision:
        for k in (
            "symbol", "timeframe", "signal_type", "setup_id", "pattern_id", "signal_key",
            "quality_floor", "threshold_factor", "adaptive_quality",
            "adaptive_stop_mult", "adaptive_tp_mult", "regime", "entry",
            "memory_profile", "memory_grade", "part3_live_mode", "part3_memory_ready"
        ):
            if last_decision.get(k) is not None and ctx.get(k) is None:
                ctx[k] = last_decision.get(k)

    signal_type = ctx.get("signal_type", None)
    setup_id = ctx.get("setup_id", None)
    pattern_id = ctx.get("pattern_id", None)
    signal_key = ctx.get("signal_key", None)

    status = _kyoto_part3_memory_status(
        sym_u,
        ctx.get("timeframe", "H1") or "H1",
        signal_type=signal_type,
        setup_id=setup_id,
        pattern_id=pattern_id,
        signal_key=signal_key,
    )
    ctx["part3_memory_profile"] = status["profile"]
    ctx["part3_memory_ready"] = status["ready"]
    ctx["part3_live_mode"] = "adaptive" if status["ready"] else "warmup"
    ctx["part3_warmup"] = status["warmup"]

    if status["warmup"]:
        # Warm-up should not block trading; it simply keeps the bot on base logic.
        ctx["quality_floor"] = 0.35
        ctx["threshold_factor"] = 1.0
        ctx["adaptive_stop_mult"] = 4.0
        ctx["adaptive_tp_mult"] = 6.0
        if signal is not None:
            try:
                ctx["adaptive_quality"] = abs(float(signal))
            except Exception:
                ctx["adaptive_quality"] = 0.0
        else:
            ctx["adaptive_quality"] = float(ctx.get("adaptive_quality", 0.0) or 0.0)
    else:
        # Keep the memory-derived values if they already exist, otherwise hydrate them.
        ctx.setdefault("quality_floor", float(status["quality_floor"]))
        ctx.setdefault("threshold_factor", float(status["profile"].get("threshold_factor", 1.0)))
        ctx.setdefault("adaptive_stop_mult", float(status["profile"].get("stop_mult", 4.0)))
        ctx.setdefault("adaptive_tp_mult", float(status["profile"].get("tp_mult", 6.0)))
        if signal is not None and ctx.get("adaptive_quality") is None:
            try:
                ctx["adaptive_quality"] = abs(float(signal))
            except Exception:
                ctx["adaptive_quality"] = 0.0

    if price is not None and ctx.get("entry") is None:
        try:
            ctx["entry"] = float(price)
        except Exception:
            pass

    return ctx, status


def _kyoto_part3_symbol_key(sym):
    try:
        return _kyoto_mem_symbol(sym)
    except Exception:
        try:
            return str(sym).upper().strip() if sym is not None else ""
        except Exception:
            return ""


# Disable the legacy global stats learning path so the new bucketed memory remains the only learning source.
try:
    _KYOTO_PART3_STATS_CLASS = globals().get("StatsManager")
    if _KYOTO_PART3_STATS_CLASS is not None:
        _KYOTO_PART3_PREV_StatsManager_record_trade = getattr(_KYOTO_PART3_STATS_CLASS, "record_trade", None)

        def record_trade(self, trade_id: str, profit: float):
            if _KYOTO_PART3_DISABLE_LEGACY_STATS:
                try:
                    self.closed_ids.add(trade_id)
                except Exception:
                    pass
                try:
                    logger.info(
                        "PART 3: legacy StatsManager disabled for live learning; trade_id=%s profit=%s",
                        trade_id,
                        profit,
                    )
                except Exception:
                    pass
                trades = int(getattr(self, "wins", 0) + getattr(self, "losses", 0))
                return {
                    "wins": int(getattr(self, "wins", 0)),
                    "losses": int(getattr(self, "losses", 0)),
                    "trades": trades,
                    "win_rate": (getattr(self, "wins", 0) / trades) if trades > 0 else 0.0,
                }
            if callable(_KYOTO_PART3_PREV_StatsManager_record_trade):
                return _KYOTO_PART3_PREV_StatsManager_record_trade(self, trade_id, profit)
            return None

        _KYOTO_PART3_STATS_CLASS.record_trade = record_trade
    else:
        logger.info("PART 3: legacy StatsManager not present; bucketed KYOTO memory is the only learning source")
except Exception:
    try:
        logger.exception("PART 3: failed to disable legacy StatsManager learning")
    except Exception:
        pass


# Wrap the top-level runtime entry points so live mode is always enforced.
try:
    _KYOTO_PART3_PREV_start_all_components = globals().get("start_all_components")
    if callable(_KYOTO_PART3_PREV_start_all_components):
        def start_all_components(*args, **kwargs):
            _kyoto_part3_initialize_runtime()
            return _KYOTO_PART3_PREV_start_all_components(*args, **kwargs)
        globals()["start_all_components"] = start_all_components
        try:
            logger.info("PART 3: start_all_components now bootstraps the live-only runtime")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap start_all_components")
    except Exception:
        pass

try:
    _KYOTO_PART3_PREV_main_loop = globals().get("main_loop")
    if callable(_KYOTO_PART3_PREV_main_loop):
        def main_loop(live=False):
            _kyoto_part3_initialize_runtime()
            return _KYOTO_PART3_PREV_main_loop(live=True)
        globals()["main_loop"] = main_loop
        try:
            logger.info("PART 3: main_loop forced to live=True")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap main_loop")
    except Exception:
        pass

try:
    _KYOTO_PART3_PREV_run_cycle = globals().get("run_cycle")
    if callable(_KYOTO_PART3_PREV_run_cycle):
        def run_cycle(live=False):
            _kyoto_part3_initialize_runtime()
            return _KYOTO_PART3_PREV_run_cycle(live=True)
        globals()["run_cycle"] = run_cycle
        try:
            logger.info("PART 3: run_cycle forced to live=True")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap run_cycle")
    except Exception:
        pass

try:
    _KYOTO_PART3_PREV_run_backtest = globals().get("run_backtest")
    if callable(_KYOTO_PART3_PREV_run_backtest):
        def run_backtest(*args, **kwargs):
            if _KYOTO_PART3_RUNTIME.get("mode") == "live_only":
                logger.info("PART 3: live-only mode active; run_backtest() disabled.")
                return {"status": "disabled", "reason": "live_only"}
            return _KYOTO_PART3_PREV_run_backtest(*args, **kwargs)
        globals()["run_backtest"] = run_backtest
        try:
            logger.info("PART 3: run_backtest disabled in live-only mode")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap run_backtest")
    except Exception:
        pass


# Make the decision layer warm-up aware without blocking real trading.
try:
    _KYOTO_PART3_PREV_make_decision_for_symbol = globals().get("make_decision_for_symbol")
    if callable(_KYOTO_PART3_PREV_make_decision_for_symbol):
        def make_decision_for_symbol(symbol: str, live: bool=False):
            _kyoto_part3_initialize_runtime()
            live = True
            decision = _KYOTO_PART3_PREV_make_decision_for_symbol(symbol, live=live)
            try:
                sym_u = _kyoto_part3_symbol_key(symbol)
                tf = "H1"
                sig_ctx = {
                    "signal_type": None,
                    "setup_id": None,
                    "pattern_id": None,
                    "signal_key": None,
                }
                if isinstance(decision, dict):
                    sig_ctx["signal_type"] = decision.get("signal_type", decision.get("signal", None))
                    sig_ctx["setup_id"] = decision.get("setup_id", decision.get("setup", None))
                    sig_ctx["pattern_id"] = decision.get("pattern_id", decision.get("pattern", None))
                    sig_ctx["signal_key"] = decision.get("signal_key", None)

                    status = _kyoto_part3_memory_status(
                        sym_u,
                        tf,
                        signal_type=sig_ctx["signal_type"],
                        setup_id=sig_ctx["setup_id"],
                        pattern_id=sig_ctx["pattern_id"],
                        signal_key=sig_ctx["signal_key"],
                    )
                    decision["part3_live_mode"] = "adaptive" if status["ready"] else "warmup"
                    decision["part3_memory_ready"] = status["ready"]
                    decision["part3_memory_profile"] = status["profile"]
                    decision["part3_warmup"] = status["warmup"]
                    decision["part3_quality_floor"] = status["quality_floor"]
                    if _KYOTO_PART3_LOG_EVERY_SYMBOL:
                        logger.info(
                            "PART 3: %s %s -> mode=%s trades=%d setup_trades=%d quality_floor=%.2f",
                            sym_u,
                            tf,
                            decision.get("part3_live_mode"),
                            status["total_trades"],
                            status["signal_trades"],
                            status["quality_floor"],
                        )
            except Exception:
                try:
                    logger.exception("PART 3: decision post-processing failed for %s", symbol)
                except Exception:
                    pass
            return decision
        globals()["make_decision_for_symbol"] = make_decision_for_symbol
        try:
            logger.info("PART 3: make_decision_for_symbol now emits warm-up / live-ready metadata")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap make_decision_for_symbol")
    except Exception:
        pass


# The execution layer gets the final live-context policy before any order is sent.
try:
    _KYOTO_PART3_PREV_execute_signal = globals().get("execute_signal")
    if callable(_KYOTO_PART3_PREV_execute_signal):
        def execute_signal(sym, signal, price, mt5_module, symbol_map):
            _kyoto_part3_initialize_runtime()
            sym_u = _kyoto_part3_symbol_key(sym)
            if mt5_module is None or price is None:
                logger.info("PART 3: live execution skipped for %s because live market data is unavailable", sym_u)
                return None

            try:
                live_ctx, status = _kyoto_part3_merge_live_context(sym_u, signal=signal, price=price)
                if status["warmup"]:
                    logger.info(
                        "PART 3: %s warm-up active -> base logic only (trades=%d signal_trades=%d)",
                        sym_u,
                        status["total_trades"],
                        status["signal_trades"],
                    )
                else:
                    logger.info(
                        "PART 3: %s live-ready -> adaptive logic enabled (trades=%d signal_trades=%d)",
                        sym_u,
                        status["total_trades"],
                        status["signal_trades"],
                    )
                _kyoto_ctx_set(**live_ctx)
                try:
                    return _KYOTO_PART3_PREV_execute_signal(sym, signal, price, mt5_module, symbol_map)
                finally:
                    try:
                        _kyoto_ctx_clear()
                    except Exception:
                        pass
            except Exception:
                try:
                    _kyoto_ctx_clear()
                except Exception:
                    pass
                return _KYOTO_PART3_PREV_execute_signal(sym, signal, price, mt5_module, symbol_map)
        globals()["execute_signal"] = execute_signal
        try:
            logger.info("PART 3: execution layer now honors live-only runtime and warm-up context")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap execute_signal")
    except Exception:
        pass


# Final order-stage guards: refuse simulation paths and preserve live-only behavior.
try:
    _KYOTO_PART3_PREV_place_order_mt5 = globals().get("place_order_mt5")
    if callable(_KYOTO_PART3_PREV_place_order_mt5):
        def place_order_mt5(symbol, action, lot, price, sl, tp):
            if _KYOTO_PART3_RUNTIME.get("mode") == "live_only" and (globals().get("DEMO_SIMULATION") or globals().get("CONFIG", {}).get("DRY_RUN_FLAG")):
                return {"status": "skipped", "comment": "live_only_no_simulation", "symbol": symbol}
            ctx = _kyoto_ctx_get() or {}
            if ctx.get("part3_live_mode") == "warmup":
                logger.info("PART 3: warm-up trade allowed for %s but no adaptive override applied", symbol)
            return _KYOTO_PART3_PREV_place_order_mt5(symbol, action, lot, price, sl, tp)
        globals()["place_order_mt5"] = place_order_mt5
        try:
            logger.info("PART 3: place_order_mt5 remains live-only with warm-up transparency")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap place_order_mt5")
    except Exception:
        pass

try:
    _KYOTO_PART3_PREV_order_wrapper = globals().get("order_wrapper")
    if callable(_KYOTO_PART3_PREV_order_wrapper):
        def order_wrapper(mt5_module, order_request):
            if _KYOTO_PART3_RUNTIME.get("mode") == "live_only" and (globals().get("DEMO_SIMULATION") or globals().get("CONFIG", {}).get("DRY_RUN_FLAG")):
                return {"retcode": -1, "comment": "live_only_no_simulation", "request": dict(order_request) if isinstance(order_request, dict) else order_request}
            ctx = _kyoto_ctx_get() or {}
            if ctx.get("part3_live_mode") == "warmup":
                logger.info("PART 3: order_wrapper warm-up context preserved for %s", ctx.get("symbol", "unknown"))
            return _KYOTO_PART3_PREV_order_wrapper(mt5_module, order_request)
        globals()["order_wrapper"] = order_wrapper
        try:
            logger.info("PART 3: order_wrapper remains on live-only execution path")
        except Exception:
            pass
except Exception:
    try:
        logger.exception("PART 3: failed to wrap order_wrapper")
    except Exception:
        pass


# Bootstrap immediately so the first live component starts from clean memory.
try:
    _kyoto_part3_initialize_runtime()
except Exception:
    try:
        logger.exception("PART 3: immediate runtime bootstrap failed")
    except Exception:
        pass

# --- END PART 3 LIVE RUNTIME LAYER ---


# === FINAL SINGLE-DOOR TRADE LIMIT LOCK (added last) ===
try:
    import threading as _kyoto_threading
    import time as _kyoto_time
    import uuid as _kyoto_uuid
except Exception:
    pass

try:
    SYMBOL_TRADE_LIMITS = {
        "BTCUSD": 3,
        "USOIL": 3,
        "EURUSD": 10,
        "USDJPY": 10,
        "XAUUSD": 2,
    }
except Exception:
    pass

try:
    GLOBAL_MAX_OPEN_TRADES = 8
except Exception:
    pass

if "_KYOTO_FINAL_LIMIT_LOCK" not in globals():
    _KYOTO_FINAL_LIMIT_LOCK = _kyoto_threading.RLock()
if "_KYOTO_FINAL_LIMIT_PENDING" not in globals():
    _KYOTO_FINAL_LIMIT_PENDING = {}
if "_KYOTO_FINAL_LIMIT_CTX" not in globals():
    _KYOTO_FINAL_LIMIT_CTX = {"token": None, "symbol": None}


def _kyoto_final_symbol(symbol):
    try:
        return str(symbol or "").upper().strip()
    except Exception:
        return ""


def _kyoto_final_cleanup(now=None):
    try:
        if now is None:
            now = _kyoto_time.time()
        ttl = int(globals().get("_KYOTO_FINAL_LIMIT_TTL_SECONDS", 120))
        pending = globals().get("_KYOTO_FINAL_LIMIT_PENDING", {})
        if not isinstance(pending, dict):
            globals()["_KYOTO_FINAL_LIMIT_PENDING"] = {}
            return
        dead = []
        for tok, meta in list(pending.items()):
            try:
                if now - float(meta.get("ts", now)) > ttl:
                    dead.append(tok)
            except Exception:
                dead.append(tok)
        for tok in dead:
            pending.pop(tok, None)
    except Exception:
        pass


def _kyoto_final_live_counts(symbol=None, ignore_token=None):
    sym = _kyoto_final_symbol(symbol) if symbol is not None else None
    total = 0
    per = 0
    try:
        mt5_mod = globals().get("_mt5")
        if globals().get("MT5_AVAILABLE") and globals().get("_mt5_connected") and mt5_mod is not None:
            try:
                positions = mt5_mod.positions_get() or []
                total = len(positions)
                if sym is not None:
                    for p in positions:
                        psym = str(getattr(p, "symbol", "") or "").upper()
                        if psym == sym or psym.startswith(sym) or psym.startswith(sym.replace("M", "")):
                            per += 1
            except Exception:
                pass
    except Exception:
        pass

    # best-effort fallback to existing counters if MT5 is unavailable
    try:
        fn = globals().get("count_open_positions")
        if callable(fn):
            result = fn()
            if isinstance(result, tuple) and len(result) >= 2:
                total2 = int(result[0] or 0)
                per_map = result[1] if isinstance(result[1], dict) else {}
                total = max(total, total2)
                if sym is not None:
                    per = max(per, int(per_map.get(sym, per_map.get(sym.replace("M", ""), 0)) or 0))
            elif isinstance(result, dict):
                per_map = {str(k).upper(): int(v or 0) for k, v in result.items()}
                total2 = sum(per_map.values())
                total = max(total, total2)
                if sym is not None:
                    per = max(per, int(per_map.get(sym, per_map.get(sym.replace("M", ""), 0)) or 0))
    except Exception:
        pass

    try:
        fn = globals().get("get_open_positions_count")
        if callable(fn) and sym is not None:
            per = max(per, int(fn(sym) or 0))
    except Exception:
        pass

    try:
        _kyoto_final_cleanup()
        pending = globals().get("_KYOTO_FINAL_LIMIT_PENDING", {})
        if isinstance(pending, dict):
            for tok, meta in list(pending.items()):
                if ignore_token is not None and tok == ignore_token:
                    continue
                total += 1
                if sym is not None and str(meta.get("symbol", "")).upper() == sym:
                    per += 1
    except Exception:
        pass

    return int(total or 0), int(per or 0)


def _kyoto_final_reserve(symbol):
    sym = _kyoto_final_symbol(symbol)
    try:
        with _KYOTO_FINAL_LIMIT_LOCK:
            _kyoto_final_cleanup()
            total, per = _kyoto_final_live_counts(sym, None)
            gmax = int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8))
            limits = dict(globals().get("SYMBOL_TRADE_LIMITS", {"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2}))
            limit = int(limits.get(sym, int(os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
            if total >= gmax:
                return False, f"global_max_open_reached:{total}", None
            if per >= limit:
                return False, f"symbol_limit_reached:{sym}:{per}/{limit}", None
            token = _kyoto_uuid.uuid4().hex
            _KYOTO_FINAL_LIMIT_PENDING[token] = {"symbol": sym, "ts": _kyoto_time.time()}
            return True, "ok", token
    except Exception as e:
        try:
            logger.exception("final reserve failed for %s: %s", symbol, e)
        except Exception:
            pass
        return False, "error", None


def _kyoto_final_release(token):
    try:
        if not token:
            return
        with _KYOTO_FINAL_LIMIT_LOCK:
            _KYOTO_FINAL_LIMIT_PENDING.pop(token, None)
    except Exception:
        pass


def _kyoto_final_order_success(res):
    try:
        if res is None:
            return False
        if isinstance(res, dict):
            rc = res.get("retcode", None)
            if rc is not None:
                try:
                    if int(rc) == 0:
                        return True
                except Exception:
                    pass
            st = str(res.get("status", "")).lower()
            if st in {"sent", "filled", "ok", "success", "executed"}:
                return True
            if res.get("order_id") is not None or res.get("order") is not None:
                return True
            if res.get("comment") and str(res.get("comment")).startswith("global_max_open_reached"):
                return False
        st = str(getattr(res, "status", "")).lower()
        if st in {"sent", "filled", "ok", "success", "executed"}:
            return True
        rc = getattr(res, "retcode", None)
        if rc is not None:
            try:
                if int(rc) == 0:
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def allowed_to_open(symbol):
    try:
        sym = _kyoto_final_symbol(symbol)
        if sym.startswith(("DXY", "US10Y")):
            return False, "macro_filter_symbol_only"
        ignore_token = None
        try:
            ignore_token = globals().get("_KYOTO_FINAL_LIMIT_CTX", {}).get("token")
        except Exception:
            pass
        total, per = _kyoto_final_live_counts(sym, ignore_token)
        gmax = int(globals().get("GLOBAL_MAX_OPEN_TRADES", 8))
        limits = dict(globals().get("SYMBOL_TRADE_LIMITS", {"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2}))
        limit = int(limits.get(sym, int(os.getenv("BEAST_MAX_PER_SYMBOL_DEFAULT", "10"))))
        if total >= gmax:
            return False, f"global_max_open_reached:{total}"
        if per >= limit:
            return False, f"symbol_limit_reached:{sym}:{per}/{limit}"
        return True, "ok"
    except Exception:
        try:
            logger.exception("final allowed_to_open failed for %s", symbol)
        except Exception:
            pass
        return False, "error"


def _kyoto_final_send_request(mt5_module, req):
    if mt5_module is None:
        return {"retcode": -1, "comment": "NO_MT5_MODULE"}
    if req is None:
        return {"retcode": -1, "comment": "ORDER_REQUEST_NONE"}
    if not isinstance(req, dict):
        try:
            req = dict(req)
        except Exception:
            return {"retcode": -1, "comment": "BAD_ORDER_REQUEST"}

    try:
        info = mt5_module.account_info()
        if info is None:
            return {"retcode": -1, "comment": "NO_ACCOUNT"}
    except Exception:
        return {"retcode": -1, "comment": "NO_ACCOUNT"}

    try:
        raw_sym = _kyoto_final_symbol(req.get("symbol") or req.get("instrument"))
        if not raw_sym:
            return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}

        sym = raw_sym
        try:
            broker_mapper = globals().get("map_symbol_to_broker")
            if callable(broker_mapper):
                mapped = broker_mapper(raw_sym)
                mapped = str(mapped).strip() if mapped is not None else ""
                if mapped:
                    sym = mapped
        except Exception:
            pass

        broker_symbol_fn = globals().get("_kyoto_broker_symbol")
        if callable(broker_symbol_fn):
            try:
                mapped2 = broker_symbol_fn(sym)
                if mapped2:
                    sym = str(mapped2).strip()
            except Exception:
                pass

        req["symbol"] = sym

        if "action" not in req:
            req["action"] = getattr(mt5_module, "TRADE_ACTION_DEAL", req.get("action"))
        if "deviation" not in req:
            req["deviation"] = 20
        if "magic" not in req:
            req["magic"] = 123456
        if "comment" not in req:
            req["comment"] = "kyoto_final"

        original_side = str(req.get("type", req.get("side", ""))).lower().strip()
        buy_const = getattr(mt5_module, "ORDER_TYPE_BUY", None)
        sell_const = getattr(mt5_module, "ORDER_TYPE_SELL", None)
        type_value = req.get("type")
        is_buy = original_side in {"buy", "long"} or type_value == buy_const
        is_sell = original_side in {"sell", "short"} or type_value == sell_const

        if is_buy:
            req["type"] = buy_const if buy_const is not None else req.get("type")
        elif is_sell:
            req["type"] = sell_const if sell_const is not None else req.get("type")
        elif isinstance(type_value, str):
            return {"retcode": -1, "comment": f"BAD_ORDER_SIDE:{req.get('type')}", "request": req}

        try:
            try:
                if hasattr(mt5_module, "symbol_select"):
                    mt5_module.symbol_select(sym, True)
            except Exception:
                pass

            si = None
            try:
                si = mt5_module.symbol_info(sym)
            except Exception:
                si = None

            if si is None:
                fallback_variants = [raw_sym, raw_sym + "m", raw_sym + ".m"]
                for alt in fallback_variants:
                    alt = _kyoto_final_symbol(alt)
                    if not alt or alt == sym:
                        continue
                    try:
                        if hasattr(mt5_module, "symbol_select"):
                            mt5_module.symbol_select(alt, True)
                    except Exception:
                        pass
                    try:
                        si = mt5_module.symbol_info(alt)
                    except Exception:
                        si = None
                    if si is not None:
                        sym = alt
                        req["symbol"] = sym
                        break
            if si is None:
                return {"retcode": -1, "comment": "SYMBOL_INFO_MISSING", "request": req}

            tick = None
            try:
                tick = mt5_module.symbol_info_tick(sym)
            except Exception:
                tick = None

            def _round_to_step(value, step, minimum):
                try:
                    if step and step > 0:
                        steps = round((float(value) - float(minimum)) / float(step))
                        return float(minimum) + (steps * float(step))
                except Exception:
                    pass
                return float(value)

            vol_min = float(getattr(si, "volume_min", 0.01) or 0.01)
            vol_step = float(getattr(si, "volume_step", 0.01) or 0.01)
            vol_max = getattr(si, "volume_max", None)
            try:
                req["volume"] = float(req.get("volume") or vol_min)
            except Exception:
                req["volume"] = vol_min
            req["volume"] = max(vol_min, _round_to_step(req["volume"], vol_step, vol_min))
            if vol_max not in (None, 0, 0.0) and req["volume"] > float(vol_max):
                req["volume"] = float(vol_max)

            point = float(getattr(si, "point", None) or getattr(si, "trade_tick_size", None) or getattr(si, "tick_size", None) or 0.00001)
            stop_level = getattr(si, "stop_level", None)
            if stop_level is not None and stop_level >= 0:
                min_sl_dist = float(stop_level) * point
            else:
                min_sl_dist = point * 10.0
            min_sl_dist = max(min_sl_dist, point)

            if req.get("price") in (None, 0, 0.0):
                if tick is not None:
                    if is_buy and hasattr(tick, "ask") and tick.ask:
                        req["price"] = float(tick.ask)
                    elif is_sell and hasattr(tick, "bid") and tick.bid:
                        req["price"] = float(tick.bid)
                    else:
                        req["price"] = float(getattr(tick, "last", 0.0) or getattr(tick, "bid", 0.0) or getattr(tick, "ask", 0.0) or 0.0)

            try:
                price = float(req.get("price") or 0.0)
            except Exception:
                price = 0.0
            if price <= 0:
                return {"retcode": -1, "comment": "NO_VALID_PRICE", "request": req}

            try:
                _kyoto_rt_fill_sl_tp(mt5_module, sym, "buy" if is_buy else "sell", price, req)
            except Exception:
                if is_buy:
                    req["sl"] = price - min_sl_dist
                    req["tp"] = price + (min_sl_dist * 2.0)
                else:
                    req["sl"] = price + min_sl_dist
                    req["tp"] = price - (min_sl_dist * 2.0)

            try:
                if req.get("type_filling") in (None, 0, 0.0, ""):
                    filling = getattr(si, "filling_mode", None)
                    if filling in (None, 0, 0.0, ""):
                        filling = getattr(mt5_module, "ORDER_FILLING_RETURN", None)
                    if filling in (None, 0, 0.0, ""):
                        filling = getattr(mt5_module, "ORDER_FILLING_IOC", None)
                    if filling not in (None, 0, 0.0, ""):
                        req["type_filling"] = filling
            except Exception:
                pass

            res = mt5_module.order_send(req)
            return res
        except Exception:
            return {"retcode": -1, "comment": "SYMBOL_INFO_MISSING", "request": req}
    except Exception as e:
        try:
            logger.exception("final send request failed: %s", e)
        except Exception:
            pass
        return {"retcode": -1, "comment": str(e), "request": req}


def order_wrapper(mt5_module, order_request):
    token = None
    sym = ""
    try:
        req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
        sym = _kyoto_final_symbol(req.get("symbol") or req.get("instrument") or req.get("symbol_name"))
        if not sym:
            return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}
        ok, reason, token = _kyoto_final_reserve(sym)
        if not ok:
            logger.info("Execution skipped for %s: %s", sym, reason)
            return {"retcode": -1, "comment": reason, "request": req}
        try:
            _KYOTO_FINAL_LIMIT_CTX["token"] = token
            _KYOTO_FINAL_LIMIT_CTX["symbol"] = sym
        except Exception:
            pass
        res = _kyoto_final_send_request(mt5_module, req)
        if not _kyoto_final_order_success(res):
            _kyoto_final_release(token)
        return res if isinstance(res, dict) else (res._asdict() if hasattr(res, "_asdict") else {"raw": str(res)})
    except Exception as e:
        try:
            logger.exception("Final order_wrapper failed for %s: %s", sym, e)
        except Exception:
            pass
        _kyoto_final_release(token)
        return {"retcode": -1, "comment": str(e), "request": order_request}
    finally:
        try:
            _KYOTO_FINAL_LIMIT_CTX["token"] = None
            _KYOTO_FINAL_LIMIT_CTX["symbol"] = None
        except Exception:
            pass


def execute_signal(sym, signal, price, mt5_module, symbol_map):
    try:
        sym_u = _kyoto_final_symbol(sym)
        if sym_u.startswith(("DXY", "US10Y")):
            return None
        if signal is None:
            return None
        try:
            signal = float(signal)
        except Exception:
            return None
        params = globals().get("CONFIG", {}).get("BACKTEST_PARAMS", {}).get(sym_u, {}) if isinstance(globals().get("CONFIG", {}), dict) else {}
        threshold = float(params.get("signal_thresh", globals().get("CONFIG", {}).get("EXECUTION_SIGNAL_THRESHOLD", 0.30) if isinstance(globals().get("CONFIG", {}), dict) else 0.30))
        if abs(signal) < threshold:
            logger.info("Execution skipped for %s: signal below execution threshold (%.3f) signal=%.4f", sym, threshold, signal)
            return None
        ok, reason = allowed_to_open(sym_u)
        if not ok:
            logger.info("Execution skipped for %s: %s", sym, reason)
            return None
        mapped = symbol_map.get(sym, sym) if symbol_map else sym
        side = "buy" if signal > 0 else "sell"
        volume = float(globals().get("CONFIG", {}).get("DEFAULT_ORDER_VOLUME", 0.01) if isinstance(globals().get("CONFIG", {}), dict) else 0.01)
        req = {"symbol": mapped, "volume": volume, "type": side, "price": float(price or 0.0)}
        try:
            _kyoto_ctx_set = globals().get("_kyoto_ctx_set")
            if callable(_kyoto_ctx_set):
                _kyoto_ctx_set(symbol=sym_u, signal=signal, quality=min(1.0, abs(signal)), regime="trending", allowed=ok, reason=reason, entry=float(price or 0.0), tech=signal)
        except Exception:
            pass
        return order_wrapper(mt5_module, req)
    except Exception:
        try:
            logger.exception("final execute_signal failed for %s", sym)
        except Exception:
            pass
        return None

try:
    if "UVXExecutionEngine" in globals() and hasattr(UVXExecutionEngine, "market_order"):
        def _kyoto_final_uvx_market_order(self, symbol, side, size, sl=None, tp=None):
            sym = _kyoto_broker_symbol(symbol) if callable(globals().get("_kyoto_broker_symbol")) else _kyoto_final_symbol(symbol)
            if not sym:
                return {"order_id": None, "status": "blocked", "comment": "NO_SYMBOL"}
            if getattr(self, "mode", "dry_run") != "mt5":
                return {"order_id": None, "status": "blocked", "comment": "LIVE_ONLY"}
            if str(sym).upper().startswith(("DXY", "US10Y")):
                return {"order_id": None, "status": "blocked", "comment": "MACRO_FILTER_SYMBOL_ONLY"}
            ok, reason, token = _kyoto_final_reserve(sym)
            if not ok:
                logger.info("Execution skipped for %s: %s", sym, reason)
                return {"order_id": None, "status": "blocked", "comment": reason}
            try:
                _KYOTO_FINAL_LIMIT_CTX["token"] = token
                _KYOTO_FINAL_LIMIT_CTX["symbol"] = sym

                if hasattr(self._mt5, "symbol_select"):
                    try:
                        self._mt5.symbol_select(sym, True)
                    except Exception:
                        pass

                tick = None
                try:
                    tick = self._mt5.symbol_info_tick(sym)
                except Exception:
                    tick = None

                req = {
                    "action": getattr(self._mt5, "TRADE_ACTION_DEAL", None),
                    "symbol": sym,
                    "volume": float(size),
                    "type": getattr(self._mt5, "ORDER_TYPE_BUY", None) if str(side).lower() == "buy" else getattr(self._mt5, "ORDER_TYPE_SELL", None),
                    "price": float(getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)),
                    "deviation": 10,
                    "magic": 123456,
                    "comment": "kyoto_final",
                }
                if req["price"] <= 0.0:
                    return {"order_id": None, "status": "blocked", "comment": "NO_VALID_PRICE", "request": req}
                if sl is not None:
                    req["sl"] = float(sl)
                if tp is not None:
                    req["tp"] = float(tp)
                if req.get("sl") in (None, 0, 0.0, "") or req.get("tp") in (None, 0, 0.0, ""):
                    try:
                        si = self._mt5.symbol_info(sym)
                        point = float(getattr(si, "point", 0.00001) or 0.00001)
                    except Exception:
                        point = 0.00001
                    dist = max(point * 10.0, point)
                    if str(side).lower() == "buy":
                        req.setdefault("sl", req["price"] - dist)
                        req.setdefault("tp", req["price"] + dist * 2.0)
                    else:
                        req.setdefault("sl", req["price"] + dist)
                        req.setdefault("tp", req["price"] - dist * 2.0)
                res = _kyoto_final_send_request(self._mt5, req)
                if not _kyoto_final_order_success(res):
                    _kyoto_final_release(token)
                if isinstance(res, dict):
                    return {"order_id": res.get("order") or res.get("order_id"), "status": res.get("retcode") or res.get("status"), "raw": res}
                return res
            except Exception as e:
                _kyoto_final_release(token)
                logger.exception("final UVX market_order failed for %s", sym)
                return {"order_id": None, "status": "error", "comment": str(e)}
            finally:
                try:
                    _KYOTO_FINAL_LIMIT_CTX["token"] = None
                    _KYOTO_FINAL_LIMIT_CTX["symbol"] = None
                except Exception:
                    pass
        UVXExecutionEngine.market_order = _kyoto_final_uvx_market_order
except Exception:
    try:
        logger.exception("failed to patch UVXExecutionEngine.market_order for final gate")
    except Exception:
        pass

try:
    if "UVXRiskManager" in globals() and hasattr(UVXRiskManager, "can_open"):
        def _kyoto_final_uvx_can_open(self, symbol, size):
            ok, _reason = allowed_to_open(symbol)
            return bool(ok)
        UVXRiskManager.can_open = _kyoto_final_uvx_can_open
except Exception:
    pass

try:
    _KYOTO_FINAL_PREV_place_order_mt5 = globals().get("place_order_mt5")
    if callable(_KYOTO_FINAL_PREV_place_order_mt5):
        def place_order_mt5(*args, **kwargs):
            try:
                symbol = kwargs.get("symbol")
                if symbol is None and len(args) > 0:
                    symbol = args[0]
                sym = _kyoto_final_symbol(symbol)
                if sym:
                    ok, reason = allowed_to_open(sym)
                    if not ok:
                        logger.info("Execution skipped for %s: %s", sym, reason)
                        return {"status": "blocked", "comment": reason}
                return _KYOTO_FINAL_PREV_place_order_mt5(*args, **kwargs)
            except Exception:
                logger.exception("final place_order_mt5 gate failed")
                return {"status": "error"}
        globals()["place_order_mt5"] = place_order_mt5
except Exception:
    pass

# Re-assert the requested limits at the very end.
try:
    SYMBOL_TRADE_LIMITS.update({"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2})
    GLOBAL_MAX_OPEN_TRADES = 8
except Exception:
    pass
# === END FINAL SINGLE-DOOR TRADE LIMIT LOCK ===


# === BEGIN FINAL SINGLE-DOOR CLEANUP OVERRIDE ===
try:
    _KYOTO_SINGLE_DOOR_ORIG_ORDER_WRAPPER = globals().get("order_wrapper")
    _KYOTO_SINGLE_DOOR_ORIG_PLACE_ORDER = globals().get("place_order_mt5")

    def _kyoto_single_door_submit(mt5_module, order_request, *, source="live"):
        """One and only live execution door: reserve -> send -> release on failure."""
        try:
            req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
        except Exception:
            return {"retcode": -1, "comment": f"BAD_ORDER_REQUEST:{source}"}

        sym = _kyoto_final_symbol(req.get("symbol") or req.get("instrument") or req.get("symbol_name"))
        if not sym:
            return {"retcode": -1, "comment": f"NO_SYMBOL:{source}", "request": req}
        if sym.startswith(("DXY", "US10Y")):
            return {"retcode": -1, "comment": "MACRO_FILTER_SYMBOL_ONLY", "request": req}

        ok, reason, token = _kyoto_final_reserve(sym)
        if not ok:
            logger.info("Execution skipped for %s: %s", sym, reason)
            return {"retcode": -1, "comment": reason, "request": req}

        try:
            try:
                _KYOTO_FINAL_LIMIT_CTX["token"] = token
                _KYOTO_FINAL_LIMIT_CTX["symbol"] = sym
            except Exception:
                pass

            req["symbol"] = sym
            res = _kyoto_final_send_request(mt5_module, req)
            if not _kyoto_final_order_success(res):
                _kyoto_final_release(token)
            return res if isinstance(res, dict) else (res._asdict() if hasattr(res, "_asdict") else {"raw": str(res)})
        except Exception as e:
            _kyoto_final_release(token)
            try:
                logger.exception("single-door execution failed for %s", sym)
            except Exception:
                pass
            return {"retcode": -1, "comment": str(e), "request": req}
        finally:
            try:
                _KYOTO_FINAL_LIMIT_CTX["token"] = None
                _KYOTO_FINAL_LIMIT_CTX["symbol"] = None
            except Exception:
                pass

    def order_wrapper(mt5_module, order_request):
        return _kyoto_single_door_submit(mt5_module, order_request, source="order_wrapper")

    def place_order_mt5(*args, **kwargs):
        try:
            mt5_module = kwargs.pop("mt5_module", None) or globals().get("mt5") or globals().get("MT5")
            req = {}

            if len(args) == 1 and isinstance(args[0], dict):
                req = dict(args[0])
            elif len(args) >= 6:
                # Legacy signature: (symbol, action, lot, price, sl, tp)
                req = {
                    "symbol": args[0],
                    "type": args[1],
                    "volume": args[2],
                    "price": args[3],
                    "sl": args[4],
                    "tp": args[5],
                }
            elif len(args) >= 1:
                req["symbol"] = args[0]

            if kwargs:
                req.update(kwargs)

            if mt5_module is None:
                return {"retcode": -1, "comment": "NO_MT5_MODULE", "request": req}

            return _kyoto_single_door_submit(mt5_module, req, source="place_order_mt5")
        except Exception:
            try:
                logger.exception("single-door place_order_mt5 failed")
            except Exception:
                pass
            return {"status": "error"}

    globals()["order_wrapper"] = order_wrapper
    globals()["place_order_mt5"] = place_order_mt5

    if "UVXExecutionEngine" in globals() and hasattr(UVXExecutionEngine, "market_order"):
        def _kyoto_single_door_uvx_market_order(self, symbol, side, size, sl=None, tp=None):
            sym = _kyoto_broker_symbol(symbol) if callable(globals().get("_kyoto_broker_symbol")) else _kyoto_final_symbol(symbol)
            if not sym:
                return {"order_id": None, "status": "blocked", "comment": "NO_SYMBOL"}
            if getattr(self, "mode", "dry_run") != "mt5":
                return {"order_id": None, "status": "blocked", "comment": "LIVE_ONLY"}
            if hasattr(self._mt5, "symbol_select"):
                try:
                    self._mt5.symbol_select(sym, True)
                except Exception:
                    pass
            tick = None
            try:
                tick = self._mt5.symbol_info_tick(sym)
            except Exception:
                tick = None
            req = {
                "action": getattr(self._mt5, "TRADE_ACTION_DEAL", None),
                "symbol": sym,
                "volume": float(size),
                "type": getattr(self._mt5, "ORDER_TYPE_BUY", None) if str(side).lower() == "buy" else getattr(self._mt5, "ORDER_TYPE_SELL", None),
                "price": float(getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)),
                "deviation": 10,
                "magic": 123456,
                "comment": "kyoto_final",
            }
            if req["price"] <= 0.0:
                return {"order_id": None, "status": "blocked", "comment": "NO_VALID_PRICE", "request": req}
            if sl is not None:
                req["sl"] = float(sl)
            if tp is not None:
                req["tp"] = float(tp)
            if req.get("sl") in (None, 0, 0.0, "") or req.get("tp") in (None, 0, 0.0, ""):
                try:
                    si = self._mt5.symbol_info(sym)
                    point = float(getattr(si, "point", 0.00001) or 0.00001)
                except Exception:
                    point = 0.00001
                dist = max(point * 10.0, point)
                if str(side).lower() == "buy":
                    req.setdefault("sl", req["price"] - dist)
                    req.setdefault("tp", req["price"] + dist * 2.0)
                else:
                    req.setdefault("sl", req["price"] + dist)
                    req.setdefault("tp", req["price"] - dist * 2.0)
            return _kyoto_single_door_submit(self._mt5, req, source="UVXExecutionEngine.market_order")
        UVXExecutionEngine.market_order = _kyoto_single_door_uvx_market_order

    try:
        SYMBOL_TRADE_LIMITS.update({"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2})
        GLOBAL_MAX_OPEN_TRADES = 8
    except Exception:
        pass

    try:
        logger.info("FINAL CLEANUP: one single live execution door is active")
    except Exception:
        pass
except Exception:
    try:
        logger.exception("FINAL CLEANUP OVERRIDE failed")
    except Exception:
        pass
# === END FINAL SINGLE-DOOR CLEANUP OVERRIDE ===


# === BEGIN FINAL EXECUTION CORE REBUILD ===
try:
    SYMBOL_TRADE_LIMITS.update({"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2})
    GLOBAL_MAX_OPEN_TRADES = 8
except Exception:
    pass

try:
    # Repoint any remaining backup generations to the same single active door.
    if callable(globals().get("order_wrapper")):
        globals()["_KYOTO_PREV_order_wrapper_FINAL_LIMITS"] = globals()["order_wrapper"]
        globals()["_KYOTO_SINGLE_DOOR_ORIG_ORDER_WRAPPER"] = globals()["order_wrapper"]
    if callable(globals().get("place_order_mt5")):
        globals()["_KYOTO_FINAL_PREV_place_order_mt5"] = globals()["place_order_mt5"]
        globals()["_KYOTO_SINGLE_DOOR_ORIG_PLACE_ORDER"] = globals()["place_order_mt5"]
    if callable(globals().get("execute_signal")):
        globals()["_KYOTO_PREV_execute_signal_FINAL_LIMITS"] = globals()["execute_signal"]

    if "UVXRiskManager" in globals() and hasattr(UVXRiskManager, "can_open"):
        def _kyoto_rebuilt_uvx_can_open(self, symbol, size):
            ok, _reason = allowed_to_open(symbol)
            return bool(ok)
        UVXRiskManager.can_open = _kyoto_rebuilt_uvx_can_open

    if "UVXExecutionEngine" in globals() and hasattr(UVXExecutionEngine, "market_order"):
        # Keep the already-installed single-door market_order wrapper active.
        pass
except Exception:
    try:
        logger.exception("FINAL EXECUTION CORE REBUILD failed")
    except Exception:
        pass
# === END FINAL EXECUTION CORE REBUILD ===


# === BEGIN FINAL LEGACY WRAPPER NEUTRALIZATION ===
try:
    # Collapse every remaining legacy wrapper/alias onto the final single door.
    _KYOTO_FINAL_SINGLE_DOOR = globals().get("order_wrapper")
    _KYOTO_FINAL_SINGLE_DOOR_EXEC = globals().get("execute_signal")
    _KYOTO_FINAL_LIMIT_GATE = globals().get("allowed_to_open")

    if callable(_KYOTO_FINAL_SINGLE_DOOR):
        for _n in (
            "_KYOTO_PREV_order_wrapper_FINAL_LIMITS",
            "_KYOTO_ORIG_order_wrapper",
            "_KYOTO_ORIG_order_wrapper_STRICT",
            "_KYOTO_PART2_PREV_order_wrapper",
            "_KYOTO_PART3_PREV_order_wrapper",
            "_KYOTO_SINGLE_DOOR_ORIG_ORDER_WRAPPER",
            "_KYOTO_FINAL_PREV_place_order_mt5",
        ):
            globals()[_n] = _KYOTO_FINAL_SINGLE_DOOR

        # Keep the public entry points on the same door.
        globals()["order_wrapper"] = _KYOTO_FINAL_SINGLE_DOOR
        globals()["place_order_mt5"] = globals().get("place_order_mt5") or _KYOTO_FINAL_SINGLE_DOOR

    if callable(_KYOTO_FINAL_SINGLE_DOOR_EXEC):
        for _n in (
            "_KYOTO_PREV_execute_signal_FINAL_LIMITS",
            "_KYOTO_ORIG_execute_signal",
            "_KYOTO_ORIG_execute_signal_STRICT",
            "_KYOTO_PART2_PREV_execute_signal",
            "_KYOTO_PART3_PREV_execute_signal",
        ):
            globals()[_n] = _KYOTO_FINAL_SINGLE_DOOR_EXEC
        globals()["execute_signal"] = _KYOTO_FINAL_SINGLE_DOOR_EXEC

    if callable(_KYOTO_FINAL_LIMIT_GATE):
        for _n in (
            "_KYOTO_FINAL_LIMIT_GATE",
        ):
            globals()[_n] = _KYOTO_FINAL_LIMIT_GATE

    # Reassert requested limits.
    SYMBOL_TRADE_LIMITS.update({"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2})
    GLOBAL_MAX_OPEN_TRADES = 8
except Exception:
    try:
        logger.exception("FINAL LEGACY WRAPPER NEUTRALIZATION failed")
    except Exception:
        pass
# === END FINAL LEGACY WRAPPER NEUTRALIZATION ===


# === BEGIN FINAL ONE-DOOR ENFORCEMENT (LAST OVERRIDE) ===
try:
    # Resolve the final live door from the most recent single-door wrapper.
    _KYOTO_FINAL_DOOR = globals().get("order_wrapper")
    _KYOTO_FINAL_EXEC = globals().get("execute_signal")
    _KYOTO_FINAL_LIMITS = globals().get("allowed_to_open")
    _KYOTO_FINAL_MT5_MKT = None

    # Preserve the live order methods on the same door.
    if callable(_KYOTO_FINAL_DOOR):
        globals()["order_wrapper"] = _KYOTO_FINAL_DOOR
        globals()["place_order_mt5"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_PREV_order_wrapper_FINAL_LIMITS"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_ORIG_order_wrapper"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_ORIG_order_wrapper_STRICT"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_PART2_PREV_order_wrapper"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_PART3_PREV_order_wrapper"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_SINGLE_DOOR_ORIG_ORDER_WRAPPER"] = _KYOTO_FINAL_DOOR
        globals()["_KYOTO_FINAL_PREV_place_order_mt5"] = _KYOTO_FINAL_DOOR

    if callable(_KYOTO_FINAL_EXEC):
        globals()["execute_signal"] = _KYOTO_FINAL_EXEC
        globals()["_KYOTO_PREV_execute_signal_FINAL_LIMITS"] = _KYOTO_FINAL_EXEC
        globals()["_KYOTO_ORIG_execute_signal"] = _KYOTO_FINAL_EXEC
        globals()["_KYOTO_ORIG_execute_signal_STRICT"] = _KYOTO_FINAL_EXEC
        globals()["_KYOTO_PART2_PREV_execute_signal"] = _KYOTO_FINAL_EXEC
        globals()["_KYOTO_PART3_PREV_execute_signal"] = _KYOTO_FINAL_EXEC

    if callable(_KYOTO_FINAL_LIMITS):
        globals()["allowed_to_open"] = _KYOTO_FINAL_LIMITS
        globals()["_KYOTO_FINAL_LIMIT_GATE"] = _KYOTO_FINAL_LIMITS

    # Force the execution engine to use the same single door.
    if "UVXExecutionEngine" in globals():
        def _kyoto_single_door_market_order(self, symbol: str, side: str, size: float, sl=None, tp=None):
            sym = _kyoto_broker_symbol(symbol) if callable(globals().get("_kyoto_broker_symbol")) else _kyoto_canonical_symbol(symbol)
            if getattr(self, "mode", "mt5") != "mt5":
                return {"order_id": None, "status": "blocked", "comment": "LIVE_ONLY"}
            if hasattr(self._mt5, "symbol_select"):
                try:
                    self._mt5.symbol_select(sym, True)
                except Exception:
                    pass
            tick = None
            try:
                tick = self._mt5.symbol_info_tick(sym)
            except Exception:
                tick = None
            req = {
                "action": getattr(self._mt5, "TRADE_ACTION_DEAL", None),
                "symbol": sym,
                "volume": float(size),
                "type": getattr(self._mt5, "ORDER_TYPE_BUY", None) if str(side).lower() == "buy" else getattr(self._mt5, "ORDER_TYPE_SELL", None),
                "price": float(getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)),
                "deviation": 10,
                "magic": 123456,
                "comment": "kyoto_single_door_final",
            }
            if req["price"] <= 0.0:
                return {"order_id": None, "status": "blocked", "comment": "NO_VALID_PRICE", "request": req}
            if sl is not None:
                req["sl"] = float(sl)
            if tp is not None:
                req["tp"] = float(tp)
            if req.get("sl") in (None, 0, 0.0, "") or req.get("tp") in (None, 0, 0.0, ""):
                try:
                    si = self._mt5.symbol_info(sym)
                    point = float(getattr(si, "point", 0.00001) or 0.00001)
                except Exception:
                    point = 0.00001
                dist = max(point * 10.0, point)
                if str(side).lower() == "buy":
                    req.setdefault("sl", req["price"] - dist)
                    req.setdefault("tp", req["price"] + dist * 2.0)
                else:
                    req.setdefault("sl", req["price"] + dist)
                    req.setdefault("tp", req["price"] - dist * 2.0)
            return _kyoto_single_door_submit(self._mt5, req, source="UVXExecutionEngine.market_order")
        UVXExecutionEngine.market_order = _kyoto_single_door_market_order

    # Reassert the requested live limits at the very end.
    SYMBOL_TRADE_LIMITS.update({"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2})
    GLOBAL_MAX_OPEN_TRADES = 8
except Exception:
    try:
        logger.exception("FINAL ONE-DOOR ENFORCEMENT failed")
    except Exception:
        pass
# === END FINAL ONE-DOOR ENFORCEMENT ===


# === FINAL MT5 SYMBOL RESOLUTION / EXECUTION PATCH ===
try:
    def _kyoto_rt_discover_broker_symbols():
        mt5_mod = globals().get("_mt5") or globals().get("mt5") or globals().get("MT5")
        try:
            if mt5_mod is not None:
                syms = mt5_mod.symbols_get() or []
                return [s.name for s in syms]
        except Exception:
            pass
        return []

    def _kyoto_rt_map_symbol_to_broker(requested: str) -> str:
        r = str(requested or "").strip()
        if not r:
            return ""
        broker_symbols = []
        try:
            broker_symbols = _kyoto_rt_discover_broker_symbols()
        except Exception:
            broker_symbols = []
        # direct canonical aliases first
        direct = {
            "BTCUSD": "BTCUSDm",
            "XAUUSD": "XAUUSDm",
            "USDJPY": "USDJPYm",
            "EURUSD": "EURUSDm",
            "USOIL": "USOILm",
        }
        key = r.upper().replace(".m", "m")
        key = key[:-1] if key.endswith("m") and key[:-2].endswith(("BTCUSD", "XAUUSD", "USDJPY", "EURUSD", "USOIL")) else key
        if key in direct:
            return direct[key]
        # exact market watch match
        for b in broker_symbols:
            if str(b).lower() == r.lower():
                return str(b)
        # suffix / prefix / contains matching
        low_req = r.lower()
        for b in broker_symbols:
            bl = str(b).lower()
            if bl == low_req or bl.startswith(low_req) or bl.endswith(low_req) or low_req in bl:
                return str(b)
        # conservative fallback
        for suffix in ("m", ".m", "-m"):
            candidate = r if r.lower().endswith(suffix.lower()) else r + suffix
            for b in broker_symbols:
                if str(b).lower() == candidate.lower():
                    return str(b)
        return r

    def _kyoto_rt_broker_symbol(symbol):
        try:
            # keep the broker's exact case if mapping succeeds
            mapped = _kyoto_rt_map_symbol_to_broker(_kyoto_canonical_symbol(symbol))
            return str(mapped).strip() if mapped else _kyoto_canonical_symbol(symbol)
        except Exception:
            return _kyoto_canonical_symbol(symbol)

    globals()["discover_broker_symbols"] = _kyoto_rt_discover_broker_symbols
    globals()["map_symbol_to_broker"] = _kyoto_rt_map_symbol_to_broker
    globals()["_kyoto_broker_symbol"] = _kyoto_rt_broker_symbol

    def _kyoto_rt_fill_sl_tp(mt5_mod, sym, side, price, req):
        try:
            side_l = str(side).lower().strip()
            price_f = float(price)
        except Exception:
            return req

        def _coerce(v):
            try:
                if v in (None, 0, 0.0, ""):
                    return None
                return float(v)
            except Exception:
                return None

        sl_in = _coerce(req.get("sl"))
        tp_in = _coerce(req.get("tp"))
        sl_distance = _coerce(req.get("sl_distance"))
        tp_distance = _coerce(req.get("tp_distance"))

        try:
            if side_l in ("buy", "long") and sl_in is not None and tp_in is not None and sl_in < price_f < tp_in:
                req["sl"] = float(sl_in)
                req["tp"] = float(tp_in)
                return req
            if side_l in ("sell", "short") and sl_in is not None and tp_in is not None and tp_in < price_f < sl_in:
                req["sl"] = float(sl_in)
                req["tp"] = float(tp_in)
                return req
        except Exception:
            pass

        try:
            si = None
            try:
                si = mt5_mod.symbol_info(sym)
            except Exception:
                si = None
            point = float(getattr(si, "point", 0.00001) or 0.00001)
            stops_level = float(getattr(si, "trade_stops_level", 0.0) or getattr(si, "stop_level", 0.0) or 0.0)
            spread = 0.0
            try:
                tick = mt5_mod.symbol_info_tick(sym)
                if tick is not None:
                    bid = float(getattr(tick, "bid", 0.0) or 0.0)
                    ask = float(getattr(tick, "ask", 0.0) or 0.0)
                    if bid > 0.0 and ask > 0.0:
                        spread = max(0.0, ask - bid)
            except Exception:
                spread = 0.0

            min_dist = max(point, spread * 3.0, stops_level * point)

            if sl_distance is None and sl_in is not None:
                sl_distance = abs(sl_in - price_f) if abs(sl_in - price_f) <= max(price_f * 0.3, point * 100.0) else abs(sl_in)
            if tp_distance is None and tp_in is not None:
                tp_distance = abs(tp_in - price_f) if abs(tp_in - price_f) <= max(price_f * 0.3, point * 100.0) else abs(tp_in)

            if sl_distance is None or sl_distance <= 0:
                sl_distance = min_dist
            if tp_distance is None or tp_distance <= 0:
                tp_distance = max(min_dist * 2.0, sl_distance * 1.5)

            if side_l in ("buy", "long"):
                req["sl"] = float(price_f - max(sl_distance, min_dist))
                req["tp"] = float(price_f + max(tp_distance, min_dist))
            else:
                req["sl"] = float(price_f + max(sl_distance, min_dist))
                req["tp"] = float(price_f - max(tp_distance, min_dist))
        except Exception:
            try:
                if side_l in ("buy", "long"):
                    req["sl"] = float(price_f - max(0.0001, price_f * 0.0005))
                    req["tp"] = float(price_f + max(0.0002, price_f * 0.0010))
                else:
                    req["sl"] = float(price_f + max(0.0001, price_f * 0.0005))
                    req["tp"] = float(price_f - max(0.0002, price_f * 0.0010))
            except Exception:
                pass
        return req

    def _kyoto_rt_send_request(mt5_module, req):
        if mt5_module is None:
            return {"retcode": -1, "comment": "NO_MT5_MODULE"}
        if req is None:
            return {"retcode": -1, "comment": "ORDER_REQUEST_NONE"}
        if not isinstance(req, dict):
            try:
                req = dict(req)
            except Exception:
                return {"retcode": -1, "comment": "BAD_ORDER_REQUEST"}

        try:
            info = mt5_module.account_info()
            if info is None:
                return {"retcode": -1, "comment": "NO_ACCOUNT"}
        except Exception:
            return {"retcode": -1, "comment": "NO_ACCOUNT"}

        try:
            raw_sym = _kyoto_canonical_symbol(req.get("symbol") or req.get("instrument"))
            if not raw_sym:
                return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}

            sym = _kyoto_rt_broker_symbol(raw_sym)
            req["symbol"] = sym

            if "action" not in req:
                req["action"] = getattr(mt5_module, "TRADE_ACTION_DEAL", req.get("action"))
            if "deviation" not in req:
                req["deviation"] = 20
            if "magic" not in req:
                req["magic"] = 123456
            if "comment" not in req:
                req["comment"] = "kyoto_final"

            original_side = str(req.get("type", req.get("side", ""))).lower().strip()
            buy_const = getattr(mt5_module, "ORDER_TYPE_BUY", None)
            sell_const = getattr(mt5_module, "ORDER_TYPE_SELL", None)
            type_value = req.get("type")
            is_buy = original_side in {"buy", "long"} or type_value == buy_const
            is_sell = original_side in {"sell", "short"} or type_value == sell_const

            if is_buy:
                req["type"] = buy_const if buy_const is not None else req.get("type")
            elif is_sell:
                req["type"] = sell_const if sell_const is not None else req.get("type")
            elif isinstance(type_value, str):
                return {"retcode": -1, "comment": f"BAD_ORDER_SIDE:{req.get('type')}", "request": req}

            try:
                if hasattr(mt5_module, "symbol_select"):
                    mt5_module.symbol_select(sym, True)
            except Exception:
                pass

            si = None
            try:
                si = mt5_module.symbol_info(sym)
            except Exception:
                si = None

            if si is None:
                # try common broker suffix fallbacks without forcing uppercase
                for alt in (raw_sym, raw_sym + "m", raw_sym + ".m", raw_sym + "-m"):
                    if not alt or alt == sym:
                        continue
                    try:
                        if hasattr(mt5_module, "symbol_select"):
                            mt5_module.symbol_select(alt, True)
                    except Exception:
                        pass
                    try:
                        si = mt5_module.symbol_info(alt)
                    except Exception:
                        si = None
                    if si is not None:
                        sym = alt
                        req["symbol"] = sym
                        break
            if si is None:
                return {"retcode": -1, "comment": "SYMBOL_INFO_MISSING", "request": req}

            tick = None
            try:
                tick = mt5_module.symbol_info_tick(sym)
            except Exception:
                tick = None

            vol_min = float(getattr(si, "volume_min", 0.01) or 0.01)
            vol_step = float(getattr(si, "volume_step", 0.01) or 0.01)
            vol_max = getattr(si, "volume_max", None)

            try:
                req["volume"] = float(req.get("volume") or vol_min)
            except Exception:
                req["volume"] = vol_min
            if vol_step > 0:
                steps = round((req["volume"] - vol_min) / vol_step)
                req["volume"] = max(vol_min, vol_min + (steps * vol_step))
            if vol_max not in (None, 0, 0.0) and req["volume"] > float(vol_max):
                req["volume"] = float(vol_max)

            price = req.get("price")
            try:
                price_f = float(price) if price is not None else 0.0
            except Exception:
                price_f = 0.0
            if price_f <= 0.0 and tick is not None:
                if is_buy and hasattr(tick, "ask"):
                    price_f = float(tick.ask)
                elif is_sell and hasattr(tick, "bid"):
                    price_f = float(tick.bid)
                else:
                    price_f = float(getattr(tick, "last", 0.0) or 0.0)
            req["price"] = price_f
            if req["price"] <= 0.0:
                return {"retcode": -1, "comment": "NO_VALID_PRICE", "request": req}

            _kyoto_rt_fill_sl_tp(mt5_module, sym, "buy" if is_buy else "sell", req["price"], req)

            try:
                if req.get("type_filling") in (None, 0, 0.0, ""):
                    filling = getattr(si, "filling_mode", None)
                    if filling in (None, 0, 0.0, ""):
                        filling = getattr(mt5_module, "ORDER_FILLING_RETURN", None)
                    if filling in (None, 0, 0.0, ""):
                        filling = getattr(mt5_module, "ORDER_FILLING_IOC", None)
                    if filling not in (None, 0, 0.0, ""):
                        req["type_filling"] = filling
            except Exception:
                pass

            res = mt5_module.order_send(req)
            return res
        except Exception:
            return {"retcode": -1, "comment": "SYMBOL_INFO_MISSING", "request": req}
    globals()["_kyoto_final_send_request"] = _kyoto_rt_send_request

    def _kyoto_rt_market_order(self, symbol, side, size, sl=None, tp=None, comment="kyoto_final"):
        sym = _kyoto_rt_broker_symbol(symbol)
        if not sym:
            return {"order_id": None, "status": "blocked", "comment": "NO_SYMBOL"}
        if getattr(self, "mode", "dry_run") != "mt5":
            return {"order_id": None, "status": "blocked", "comment": "LIVE_ONLY"}
        if str(sym).upper().startswith(("DXY", "US10Y")):
            return {"order_id": None, "status": "blocked", "comment": "MACRO_FILTER_SYMBOL_ONLY"}
        mt5_mod = getattr(self, "_mt5", None)
        if mt5_mod is None:
            return {"order_id": None, "status": "blocked", "comment": "NO_MT5_MODULE"}
        try:
            if hasattr(mt5_mod, "symbol_select"):
                mt5_mod.symbol_select(sym, True)
        except Exception:
            pass
        tick = None
        try:
            tick = mt5_mod.symbol_info_tick(sym)
        except Exception:
            tick = None
        is_buy = str(side).lower() in ("buy", "long")
        price = 0.0
        try:
            if tick is not None:
                price = float(getattr(tick, "ask", 0.0) if is_buy else getattr(tick, "bid", 0.0))
        except Exception:
            price = 0.0
        req = {
            "action": getattr(mt5_mod, "TRADE_ACTION_DEAL", None),
            "symbol": sym,
            "volume": float(size),
            "type": getattr(mt5_mod, "ORDER_TYPE_BUY", None) if is_buy else getattr(mt5_mod, "ORDER_TYPE_SELL", None),
            "price": price,
            "deviation": 10,
            "magic": 123456,
            "comment": comment,
        }
        if price <= 0.0:
            return {"order_id": None, "status": "blocked", "comment": "NO_VALID_PRICE", "request": req}
        if sl is not None:
            req["sl"] = float(sl)
        if tp is not None:
            req["tp"] = float(tp)
        _kyoto_rt_fill_sl_tp(mt5_mod, sym, side, price, req)
        return _kyoto_single_door_submit(mt5_mod, req, source="market_order")

    if "UVXExecutionEngine" in globals() and hasattr(UVXExecutionEngine, "market_order"):
        UVXExecutionEngine.market_order = _kyoto_rt_market_order

except Exception:
    try:
        logger.exception("FINAL MT5 SYMBOL RESOLUTION / EXECUTION PATCH failed")
    except Exception:
        pass
# === END FINAL MT5 SYMBOL RESOLUTION / EXECUTION PATCH ===


# === BEGIN FINAL MT5 FILLING + ATR OVERRIDE (v3) ===
try:
    def _kyoto_final_is_unsupported_filling(res):
        try:
            if res is None:
                return False
            comment = ""
            retcode = None
            if isinstance(res, dict):
                retcode = res.get("retcode", None)
                comment = str(res.get("comment", "") or res.get("result", "") or "").lower()
            else:
                retcode = getattr(res, "retcode", None)
                comment = str(getattr(res, "comment", "") or "").lower()
            if retcode is not None:
                try:
                    if int(retcode) == 10030:
                        return True
                except Exception:
                    pass
            return "unsupported filling mode" in comment or "unsupported filling" in comment
        except Exception:
            return False

    def _kyoto_final_side_from_request(mt5_module, req):
        try:
            t = req.get("type", None)
        except Exception:
            t = None
        try:
            buy = getattr(mt5_module, "ORDER_TYPE_BUY", None)
            sell = getattr(mt5_module, "ORDER_TYPE_SELL", None)
            if t == buy:
                return "buy"
            if t == sell:
                return "sell"
        except Exception:
            pass
        try:
            t_s = str(t).lower()
            if t_s in ("buy", "long", "1", "bull", "up"):
                return "buy"
            if t_s in ("sell", "short", "-1", "bear", "down"):
                return "sell"
        except Exception:
            pass
        return "buy"

    def _kyoto_final_supported_filling_modes(mt5_module, si, req):
        modes = []
        def _add(val):
            if val in (None, "", 0.0):
                return
            if val not in modes:
                modes.append(val)

        try:
            explicit = req.get("type_filling", None)
            _add(explicit)
        except Exception:
            pass

        try:
            fm = getattr(si, "filling_mode", None)
            fm_int = int(fm)
            fm_map = {0: "ORDER_FILLING_FOK", 1: "ORDER_FILLING_IOC", 2: "ORDER_FILLING_RETURN"}
            if fm_int in fm_map:
                const_name = fm_map[fm_int]
                if hasattr(mt5_module, const_name):
                    _add(getattr(mt5_module, const_name))
        except Exception:
            pass

        for const_name in ("ORDER_FILLING_RETURN", "ORDER_FILLING_IOC", "ORDER_FILLING_FOK"):
            try:
                _add(getattr(mt5_module, const_name, None))
            except Exception:
                pass

        return [m for m in modes if m not in (None, "")]

    def _kyoto_final_apply_atr_sl_tp(mt5_module, sym, req, si, tick):
        try:
            side = _kyoto_final_side_from_request(mt5_module, req)
            is_buy = side == "buy"

            try:
                price = float(req.get("price") or 0.0)
            except Exception:
                price = 0.0
            if price <= 0 and tick is not None:
                try:
                    price = float(tick.ask if is_buy else tick.bid)
                except Exception:
                    price = float(getattr(tick, "last", 0.0) or 0.0)
            if price <= 0:
                return req

            point = float(getattr(si, "point", None) or getattr(si, "trade_tick_size", None) or getattr(si, "tick_size", None) or 0.00001)
            stop_level = getattr(si, "stop_level", None)
            if stop_level is not None and stop_level >= 0:
                min_sl_dist = float(stop_level) * point
            else:
                min_sl_dist = point * 10.0
            min_sl_dist = max(min_sl_dist, point)

            sl = req.get("sl", None)
            tp = req.get("tp", None)
            sl_ok = sl not in (None, 0, 0.0, "")
            tp_ok = tp not in (None, 0, 0.0, "")
            too_close = False
            try:
                if sl_ok and abs(price - float(sl)) < min_sl_dist:
                    too_close = True
                if tp_ok and abs(price - float(tp)) < min_sl_dist:
                    too_close = True
            except Exception:
                too_close = True

            if (not sl_ok) or (not tp_ok) or too_close:
                df_h1 = None
                try:
                    df_h1 = _kyoto_h1_df(mt5_module, None, sym, bars=160)
                except Exception:
                    df_h1 = None
                if df_h1 is not None and callable(globals().get("regime_adaptive_stop")):
                    try:
                        sl2, tp2, _stop = regime_adaptive_stop(price, df_h1, "BUY" if is_buy else "SELL", base_atr_multiplier=4.0)
                        if sl2 and tp2:
                            req["sl"] = float(sl2)
                            req["tp"] = float(tp2)
                            return req
                    except Exception:
                        pass

                if is_buy:
                    req["sl"] = price - max(min_sl_dist, point * 10.0)
                    req["tp"] = price + max(min_sl_dist * 2.0, point * 20.0)
                else:
                    req["sl"] = price + max(min_sl_dist, point * 10.0)
                    req["tp"] = price - max(min_sl_dist * 2.0, point * 20.0)
            else:
                req["sl"] = float(sl)
                req["tp"] = float(tp)

            try:
                if abs(price - float(req["sl"])) < min_sl_dist:
                    req["sl"] = price - min_sl_dist if is_buy else price + min_sl_dist
                if abs(price - float(req["tp"])) < min_sl_dist:
                    req["tp"] = price + (min_sl_dist * 2.0) if is_buy else price - (min_sl_dist * 2.0)
            except Exception:
                pass
            req["price"] = price
            return req
        except Exception:
            return req

    def _kyoto_final_send_request(mt5_module, req):
        if mt5_module is None:
            return {"retcode": -1, "comment": "NO_MT5_MODULE"}
        if req is None:
            return {"retcode": -1, "comment": "ORDER_REQUEST_NONE"}
        if not isinstance(req, dict):
            try:
                req = dict(req)
            except Exception:
                return {"retcode": -1, "comment": "BAD_ORDER_REQUEST"}

        try:
            info = mt5_module.account_info()
            if info is None:
                return {"retcode": -1, "comment": "NO_ACCOUNT"}
        except Exception:
            return {"retcode": -1, "comment": "NO_ACCOUNT"}

        try:
            raw_sym = _kyoto_final_symbol(req.get("symbol") or req.get("instrument"))
            if not raw_sym:
                return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}

            sym = raw_sym
            try:
                broker_mapper = globals().get("map_symbol_to_broker")
                if callable(broker_mapper):
                    mapped = broker_mapper(raw_sym)
                    mapped = str(mapped).strip() if mapped is not None else ""
                    if mapped:
                        sym = mapped
            except Exception:
                pass

            req["symbol"] = sym
            try:
                mt5_module.symbol_select(sym, True)
            except Exception:
                pass

            si = None
            try:
                si = mt5_module.symbol_info(sym)
                if si is None:
                    try:
                        mt5_module.symbol_select(sym, True)
                    except Exception:
                        pass
                    si = mt5_module.symbol_info(sym)
            except Exception:
                si = None
            if si is None:
                return {"retcode": -1, "comment": "SYMBOL_INFO_MISSING", "request": req}

            tick = None
            try:
                tick = mt5_module.symbol_info_tick(sym)
            except Exception:
                tick = None
            if tick is None:
                return {"retcode": -1, "comment": "SYMBOL_TICK_MISSING", "request": req}

            try:
                req = _kyoto_final_apply_atr_sl_tp(mt5_module, sym, req, si, tick)
            except Exception:
                pass

            try:
                req["volume"] = float(req.get("volume") or getattr(si, "volume_min", 0.01) or 0.01)
            except Exception:
                req["volume"] = float(getattr(si, "volume_min", 0.01) or 0.01)

            try:
                req.setdefault("action", getattr(mt5_module, "TRADE_ACTION_DEAL", 1))
                req.setdefault("price", float(getattr(tick, "ask", 0.0) or getattr(tick, "bid", 0.0) or 0.0))
                req.setdefault("deviation", 10)
                req.setdefault("magic", 123456)
                req.setdefault("comment", "kyoto_final")
            except Exception:
                pass

            filling_modes = _kyoto_final_supported_filling_modes(mt5_module, si, req)
            if not filling_modes:
                filling_modes = [getattr(mt5_module, "ORDER_FILLING_RETURN", None), getattr(mt5_module, "ORDER_FILLING_IOC", None), getattr(mt5_module, "ORDER_FILLING_FOK", None)]
                filling_modes = [x for x in filling_modes if x not in (None, "")]

            last_res = None
            tried = []
            for fill in filling_modes:
                trial = dict(req)
                trial["type_filling"] = fill
                tried.append(fill)
                try:
                    res = mt5_module.order_send(trial)
                except Exception as e:
                    last_res = {"retcode": -1, "comment": str(e), "request": trial}
                    continue
                last_res = res
                if not _kyoto_final_is_unsupported_filling(res):
                    return res

            if last_res is not None:
                return last_res
            return {"retcode": -1, "comment": "ORDER_SEND_FAILED", "request": req, "tried_fillings": tried}
        except Exception as e:
            try:
                logger.exception("final send request failed: %s", e)
            except Exception:
                pass
            return {"retcode": -1, "comment": str(e), "request": req}

    globals()["_kyoto_final_send_request"] = _kyoto_final_send_request
    globals()["_kyoto_final_apply_atr_sl_tp"] = _kyoto_final_apply_atr_sl_tp
    globals()["_kyoto_final_supported_filling_modes"] = _kyoto_final_supported_filling_modes
    globals()["_kyoto_final_is_unsupported_filling"] = _kyoto_final_is_unsupported_filling
    globals()["_kyoto_final_side_from_request"] = _kyoto_final_side_from_request

    if "UVXExecutionEngine" in globals() and hasattr(UVXExecutionEngine, "market_order"):
        def _kyoto_final_v3_uvx_market_order(self, symbol, side, size, sl=None, tp=None):
            sym = _kyoto_final_symbol(symbol)
            if not sym:
                return {"order_id": None, "status": "blocked", "comment": "NO_SYMBOL"}
            if getattr(self, "mode", "dry_run") != "mt5":
                return {"order_id": None, "status": "blocked", "comment": "LIVE_ONLY"}
            if sym.startswith(("DXY", "US10Y")):
                return {"order_id": None, "status": "blocked", "comment": "MACRO_FILTER_SYMBOL_ONLY"}
            ok, reason, token = _kyoto_final_reserve(sym)
            if not ok:
                logger.info("Execution skipped for %s: %s", sym, reason)
                return {"order_id": None, "status": "blocked", "comment": reason}
            try:
                _KYOTO_FINAL_LIMIT_CTX["token"] = token
                _KYOTO_FINAL_LIMIT_CTX["symbol"] = sym
                req = {
                    "action": getattr(self._mt5, "TRADE_ACTION_DEAL", None),
                    "symbol": sym,
                    "volume": float(size),
                    "type": getattr(self._mt5, "ORDER_TYPE_BUY", None) if str(side).lower() == "buy" else getattr(self._mt5, "ORDER_TYPE_SELL", None),
                    "price": float(getattr(self._mt5.symbol_info_tick(sym), "ask", 0.0) if str(side).lower() == "buy" else getattr(self._mt5.symbol_info_tick(sym), "bid", 0.0)),
                    "deviation": 10,
                    "magic": 123456,
                    "comment": "kyoto_final",
                }
                if sl is not None:
                    req["sl"] = float(sl)
                if tp is not None:
                    req["tp"] = float(tp)
                res = _kyoto_final_send_request(self._mt5, req)
                if not _kyoto_final_order_success(res):
                    _kyoto_final_release(token)
                if isinstance(res, dict):
                    return {"order_id": res.get("order") or res.get("order_id"), "status": res.get("retcode") or res.get("status"), "raw": res}
                return res
            except Exception as e:
                _kyoto_final_release(token)
                logger.exception("final UVX market_order failed for %s", sym)
                return {"order_id": None, "status": "error", "comment": str(e)}
            finally:
                try:
                    _KYOTO_FINAL_LIMIT_CTX["token"] = None
                    _KYOTO_FINAL_LIMIT_CTX["symbol"] = None
                except Exception:
                    pass
        UVXExecutionEngine.market_order = _kyoto_final_v3_uvx_market_order

    _KYOTO_FINAL_PREV_place_order_mt5 = globals().get("place_order_mt5")
    if callable(_KYOTO_FINAL_PREV_place_order_mt5):
        def place_order_mt5(*args, **kwargs):
            try:
                symbol = kwargs.get("symbol")
                if symbol is None and len(args) > 0:
                    symbol = args[0]
                sym = _kyoto_final_symbol(symbol)
                if sym:
                    ok, reason = allowed_to_open(sym)
                    if not ok:
                        logger.info("Execution skipped for %s: %s", sym, reason)
                        return {"status": "blocked", "comment": reason}
                return _KYOTO_FINAL_PREV_place_order_mt5(*args, **kwargs)
            except Exception:
                logger.exception("final place_order_mt5 gate failed")
                return {"status": "error"}
        globals()["place_order_mt5"] = place_order_mt5

    try:
        SYMBOL_TRADE_LIMITS.update({"BTCUSD": 3, "USOIL": 3, "EURUSD": 10, "USDJPY": 10, "XAUUSD": 2})
        GLOBAL_MAX_OPEN_TRADES = 8
    except Exception:
        pass
    try:
        ATR_STOP_MULTIPLIER = 4.0
        ATR_TAKE_PROFIT_MULTIPLIER = 6.0
    except Exception:
        pass
except Exception:
    try:
        logger.exception("FINAL MT5 FILLING + ATR OVERRIDE failed")
    except Exception:
        pass
# === END FINAL MT5 FILLING + ATR OVERRIDE (v3) ===

# === BEGIN FINAL SYMBOL PRESERVATION FIX (append-only) ===
try:
    def _kyoto_final_preserve_symbol_case(symbol):
        try:
            return str(symbol or "").strip()
        except Exception:
            return ""

    def _kyoto_final_mt5_symbol_candidates(symbol):
        raw = _kyoto_final_preserve_symbol_case(symbol)
        if not raw:
            return []
        base = raw.strip()
        cands = []
        def add(x):
            x = _kyoto_final_preserve_symbol_case(x)
            if x and x not in cands:
                cands.append(x)

        add(base)
        add(base.upper())
        add(base.lower())
        if base.endswith("m"):
            add(base[:-1])
        if base.endswith(".m"):
            add(base[:-2])
        canon = base.upper()
        add(canon + "m")
        return cands

    def _kyoto_final_resolve_mt5_symbol(mt5_module, symbol):
        raw = _kyoto_final_preserve_symbol_case(symbol)
        if not raw:
            return ""
        candidates = _kyoto_final_mt5_symbol_candidates(raw)
        try:
            if mt5_module is not None and hasattr(mt5_module, "symbol_info"):
                for cand in candidates:
                    try:
                        si = mt5_module.symbol_info(cand)
                        if si is not None:
                            try:
                                if hasattr(mt5_module, "symbol_select"):
                                    mt5_module.symbol_select(cand, True)
                            except Exception:
                                pass
                            return cand
                    except Exception:
                        continue
        except Exception:
            pass
        for cand in candidates:
            if cand.endswith(("m", ".m")):
                return cand
        return raw

    def _kyoto_broker_symbol(symbol):
        mt5_mod = globals().get("_mt5") or globals().get("mt5")
        resolved = _kyoto_final_resolve_mt5_symbol(mt5_mod, symbol)
        return resolved or _kyoto_final_preserve_symbol_case(symbol)

    def _kyoto_final_send_request(mt5_module, req):
        if mt5_module is None:
            return {"retcode": -1, "comment": "NO_MT5_MODULE"}
        if req is None:
            return {"retcode": -1, "comment": "ORDER_REQUEST_NONE"}
        if not isinstance(req, dict):
            try:
                req = dict(req)
            except Exception:
                return {"retcode": -1, "comment": "BAD_ORDER_REQUEST"}

        try:
            info = mt5_module.account_info()
            if info is None:
                return {"retcode": -1, "comment": "NO_ACCOUNT"}
        except Exception:
            return {"retcode": -1, "comment": "NO_ACCOUNT"}

        try:
            raw_input = req.get("symbol") or req.get("instrument") or req.get("symbol_name")
            if not raw_input:
                return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}

            sym = _kyoto_final_resolve_mt5_symbol(mt5_module, raw_input)
            if not sym:
                return {"retcode": -1, "comment": "NO_SYMBOL", "request": req}
            req["symbol"] = sym

            if "action" not in req:
                req["action"] = getattr(mt5_module, "TRADE_ACTION_DEAL", req.get("action"))
            if "deviation" not in req:
                req["deviation"] = 20
            if "magic" not in req:
                req["magic"] = 123456
            if "comment" not in req:
                req["comment"] = "kyoto_final"

            original_side = str(req.get("type", req.get("side", ""))).lower().strip()
            buy_const = getattr(mt5_module, "ORDER_TYPE_BUY", None)
            sell_const = getattr(mt5_module, "ORDER_TYPE_SELL", None)
            type_value = req.get("type")
            is_buy = original_side in {"buy", "long"} or type_value == buy_const
            is_sell = original_side in {"sell", "short"} or type_value == sell_const
            if is_buy:
                req["type"] = buy_const if buy_const is not None else req.get("type")
            elif is_sell:
                req["type"] = sell_const if sell_const is not None else req.get("type")
            elif isinstance(type_value, str):
                return {"retcode": -1, "comment": f"BAD_ORDER_SIDE:{req.get('type')}", "request": req}

            try:
                if hasattr(mt5_module, "symbol_select"):
                    mt5_module.symbol_select(sym, True)
            except Exception:
                pass

            si = None
            try:
                si = mt5_module.symbol_info(sym)
            except Exception:
                si = None
            if si is None:
                for alt in _kyoto_final_mt5_symbol_candidates(raw_input):
                    if alt == sym:
                        continue
                    try:
                        if hasattr(mt5_module, "symbol_select"):
                            mt5_module.symbol_select(alt, True)
                    except Exception:
                        pass
                    try:
                        si = mt5_module.symbol_info(alt)
                    except Exception:
                        si = None
                    if si is not None:
                        sym = alt
                        req["symbol"] = sym
                        break
            if si is None:
                return {"retcode": -1, "comment": "SYMBOL_INFO_MISSING", "request": req}

            tick = None
            try:
                tick = mt5_module.symbol_info_tick(sym)
            except Exception:
                tick = None
            if tick is None:
                return {"retcode": -1, "comment": "SYMBOL_TICK_MISSING", "request": req}

            try:
                req = _kyoto_final_apply_atr_sl_tp(mt5_module, sym, req, si, tick)
            except Exception:
                pass

            try:
                req["volume"] = float(req.get("volume") or getattr(si, "volume_min", 0.01) or 0.01)
            except Exception:
                req["volume"] = float(getattr(si, "volume_min", 0.01) or 0.01)

            try:
                req.setdefault("action", getattr(mt5_module, "TRADE_ACTION_DEAL", 1))
                req.setdefault("price", float(getattr(tick, "ask", 0.0) or getattr(tick, "bid", 0.0) or 0.0))
                req.setdefault("deviation", 10)
                req.setdefault("magic", 123456)
                req.setdefault("comment", "kyoto_final")
            except Exception:
                pass

            filling_modes = _kyoto_final_supported_filling_modes(mt5_module, si, req)
            if not filling_modes:
                filling_modes = [getattr(mt5_module, "ORDER_FILLING_RETURN", None), getattr(mt5_module, "ORDER_FILLING_IOC", None), getattr(mt5_module, "ORDER_FILLING_FOK", None)]
                filling_modes = [x for x in filling_modes if x not in (None, "")]

            last_res = None
            tried = []
            for fill in filling_modes:
                trial = dict(req)
                trial["type_filling"] = fill
                tried.append(fill)
                try:
                    res = mt5_module.order_send(trial)
                except Exception as e:
                    last_res = {"retcode": -1, "comment": str(e), "request": trial}
                    continue
                last_res = res
                if not _kyoto_final_is_unsupported_filling(res):
                    return res

            if last_res is not None:
                return last_res
            return {"retcode": -1, "comment": "ORDER_SEND_FAILED", "request": req, "tried_fillings": tried}
        except Exception as e:
            try:
                logger.exception("final send request failed: %s", e)
            except Exception:
                pass
            return {"retcode": -1, "comment": str(e), "request": req}

    globals()["_kyoto_final_preserve_symbol_case"] = _kyoto_final_preserve_symbol_case
    globals()["_kyoto_final_mt5_symbol_candidates"] = _kyoto_final_mt5_symbol_candidates
    globals()["_kyoto_final_resolve_mt5_symbol"] = _kyoto_final_resolve_mt5_symbol
    globals()["_kyoto_broker_symbol"] = _kyoto_broker_symbol
    globals()["_kyoto_final_send_request"] = _kyoto_final_send_request

    if "UVXExecutionEngine" in globals() and hasattr(UVXExecutionEngine, "market_order"):
        def _kyoto_final_v4_uvx_market_order(self, symbol, side, size, sl=None, tp=None):
            sym = _kyoto_final_resolve_mt5_symbol(getattr(self, "_mt5", None), symbol)
            if not sym:
                sym = _kyoto_final_preserve_symbol_case(symbol)
            if not sym:
                return {"order_id": None, "status": "blocked", "comment": "NO_SYMBOL"}
            if getattr(self, "mode", "dry_run") != "mt5":
                return {"order_id": None, "status": "blocked", "comment": "LIVE_ONLY"}
            if hasattr(self._mt5, "symbol_select"):
                try:
                    self._mt5.symbol_select(sym, True)
                except Exception:
                    pass
            tick = None
            try:
                tick = self._mt5.symbol_info_tick(sym)
            except Exception:
                tick = None
            req = {
                "action": getattr(self._mt5, "TRADE_ACTION_DEAL", None),
                "symbol": sym,
                "volume": float(size),
                "type": getattr(self._mt5, "ORDER_TYPE_BUY", None) if str(side).lower() == "buy" else getattr(self._mt5, "ORDER_TYPE_SELL", None),
                "price": float(getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)),
                "deviation": 10,
                "magic": 123456,
                "comment": "kyoto_final",
            }
            if req["price"] <= 0.0:
                return {"order_id": None, "status": "blocked", "comment": "NO_VALID_PRICE", "request": req}
            if sl is not None:
                req["sl"] = float(sl)
            if tp is not None:
                req["tp"] = float(tp)
            if req.get("sl") in (None, 0, 0.0, "") or req.get("tp") in (None, 0, 0.0, ""):
                try:
                    si = self._mt5.symbol_info(sym)
                    point = float(getattr(si, "point", 0.00001) or 0.00001)
                except Exception:
                    point = 0.00001
                dist = max(point * 10.0, point)
                if str(side).lower() == "buy":
                    req.setdefault("sl", req["price"] - dist)
                    req.setdefault("tp", req["price"] + dist * 2.0)
                else:
                    req.setdefault("sl", req["price"] + dist)
                    req.setdefault("tp", req["price"] - dist * 2.0)
            return _kyoto_single_door_submit(self._mt5, req, source="UVXExecutionEngine.market_order")
        UVXExecutionEngine.market_order = _kyoto_final_v4_uvx_market_order
except Exception:
    try:
        logger.exception("FINAL SYMBOL PRESERVATION FIX failed")
    except Exception:
        pass
# === END FINAL SYMBOL PRESERVATION FIX ===


# === BEGIN FINAL BROKER SYMBOL HARD FIX ===
try:
    _KYOTO_BROKER_ALIAS = {
        "EURUSD": "EURUSDm",
        "XAUUSD": "XAUUSDm",
        "BTCUSD": "BTCUSDm",
        "USDJPY": "USDJPYm",
        "USOIL": "USOILm",
    }

    def _kyoto_force_broker_symbol(symbol):
        try:
            raw = str(symbol or "").strip()
        except Exception:
            raw = ""
        if not raw:
            return ""

        cleaned = raw.replace(" ", "")
        upper = cleaned.upper().replace(".M", "M").replace("-M", "M")
        base = upper[:-1] if upper.endswith("M") else upper

        if base in _KYOTO_BROKER_ALIAS:
            return _KYOTO_BROKER_ALIAS[base]

        # Preserve an exact broker alias if one is already passed in.
        for v in _KYOTO_BROKER_ALIAS.values():
            if upper == v.upper():
                return v

        # Try market-watch discovery only as a fallback.
        try:
            mt5_mod = globals().get("_mt5") or globals().get("mt5") or globals().get("MT5")
            if mt5_mod is not None and hasattr(mt5_mod, "symbols_get"):
                symbols = mt5_mod.symbols_get() or []
                low = raw.lower()
                for s in symbols:
                    name = str(getattr(s, "name", s) or "")
                    if name.lower() == low:
                        return name
                # allow aliasless canonical matching
                for s in symbols:
                    name = str(getattr(s, "name", s) or "")
                    name_u = name.upper()
                    if name_u == base or name_u == upper:
                        return name
                    if name_u.startswith(base) or name_u.endswith(base) or base in name_u:
                        return name
        except Exception:
            pass

        return _KYOTO_BROKER_ALIAS.get(base, raw)

    def _kyoto_force_broker_request(req):
        try:
            if not isinstance(req, dict):
                req = dict(req or {})
        except Exception:
            return req
        sym = req.get("symbol") or req.get("instrument") or req.get("symbol_name")
        req["symbol"] = _kyoto_force_broker_symbol(sym)
        return req

    # Override the broker mapping helpers used by every live send path.
    def map_symbol_to_broker(requested: str) -> str:
        return _kyoto_force_broker_symbol(requested)

    def _kyoto_broker_symbol(symbol):
        return _kyoto_force_broker_symbol(symbol)

    def _kyoto_rt_broker_symbol(symbol):
        return _kyoto_force_broker_symbol(symbol)

    globals()["map_symbol_to_broker"] = map_symbol_to_broker
    globals()["_kyoto_broker_symbol"] = _kyoto_broker_symbol
    globals()["_kyoto_rt_broker_symbol"] = _kyoto_rt_broker_symbol
    globals()["discover_broker_symbols"] = lambda: list(_KYOTO_BROKER_ALIAS.values())

    # Wrap the execution doors so any raw symbol is normalized before MT5 lookup.
    _KYOTO_PREV_place_order_mt5 = globals().get("place_order_mt5")
    if callable(_KYOTO_PREV_place_order_mt5):
        def place_order_mt5(symbol, action, lot, price, sl, tp):
            return _KYOTO_PREV_place_order_mt5(_kyoto_force_broker_symbol(symbol), action, lot, price, sl, tp)
        globals()["place_order_mt5"] = place_order_mt5

    _KYOTO_PREV_order_wrapper = globals().get("order_wrapper")
    if callable(_KYOTO_PREV_order_wrapper):
        def order_wrapper(mt5_module, order_request):
            req = _kyoto_force_broker_request(order_request)
            return _KYOTO_PREV_order_wrapper(mt5_module, req)
        globals()["order_wrapper"] = order_wrapper

    _KYOTO_PREV_final_send_request = globals().get("_kyoto_final_send_request")
    if callable(_KYOTO_PREV_final_send_request):
        def _kyoto_final_send_request(mt5_module, req):
            return _KYOTO_PREV_final_send_request(mt5_module, _kyoto_force_broker_request(req))
        globals()["_kyoto_final_send_request"] = _kyoto_final_send_request

    _KYOTO_PREV_rt_send_request = globals().get("_kyoto_rt_send_request")
    if callable(_KYOTO_PREV_rt_send_request):
        def _kyoto_rt_send_request(mt5_module, req):
            return _KYOTO_PREV_rt_send_request(mt5_module, _kyoto_force_broker_request(req))
        globals()["_kyoto_rt_send_request"] = _kyoto_rt_send_request

    _KYOTO_PREV_single_door_submit = globals().get("_kyoto_single_door_submit")
    if callable(_KYOTO_PREV_single_door_submit):
        def _kyoto_single_door_submit(mt5_module, order_request, *, source="live"):
            return _KYOTO_PREV_single_door_submit(mt5_module, _kyoto_force_broker_request(order_request), source=source)
        globals()["_kyoto_single_door_submit"] = _kyoto_single_door_submit

    _KYOTO_PREV_uvx_market_order = globals().get("UVXExecutionEngine", None)
    if _KYOTO_PREV_uvx_market_order is not None and hasattr(_KYOTO_PREV_uvx_market_order, "market_order"):
        _KYOTO_PREV_uvx_market_order_fn = getattr(_KYOTO_PREV_uvx_market_order, "market_order")
        if callable(_KYOTO_PREV_uvx_market_order_fn):
            def _kyoto_uvx_market_order(self, symbol, side, size, sl=None, tp=None):
                sym = _kyoto_force_broker_symbol(symbol)
                return _KYOTO_PREV_uvx_market_order_fn(self, sym, side, size, sl=sl, tp=tp)
            UVXExecutionEngine.market_order = _kyoto_uvx_market_order

    logger.info("FINAL BROKER SYMBOL HARD FIX armed: %s", _KYOTO_BROKER_ALIAS)
except Exception:
    try:
        logger.exception("FINAL BROKER SYMBOL HARD FIX failed")
    except Exception:
        pass
# === END FINAL BROKER SYMBOL HARD FIX ===

# === FINAL BROKER PATH UNIFICATION v2 ===
try:
    _KYOTO_FORCE_BROKER = globals().get("_kyoto_force_broker_symbol") or globals().get("map_symbol_to_broker")

    def _kyoto_unified_symbol(symbol):
        try:
            if callable(_KYOTO_FORCE_BROKER):
                mapped = _KYOTO_FORCE_BROKER(symbol)
                if mapped:
                    return str(mapped)
        except Exception:
            pass
        try:
            return str(symbol or "")
        except Exception:
            return ""

    def _kyoto_unify_symbol_map(symbol_map):
        try:
            if isinstance(symbol_map, dict):
                for canon in ("BTCUSD", "XAUUSD", "USDJPY", "EURUSD", "USOIL"):
                    symbol_map[canon] = _kyoto_unified_symbol(canon)
                    symbol_map[canon + "m"] = _kyoto_unified_symbol(canon)
        except Exception:
            pass
        return symbol_map

    _KYOTO_PREV_EXECUTE_SIGNAL_UNIFY = globals().get("execute_signal")
    if callable(_KYOTO_PREV_EXECUTE_SIGNAL_UNIFY):
        def execute_signal(sym, signal, price, mt5_module, symbol_map):
            sym_broker = _kyoto_unified_symbol(sym)
            symbol_map = _kyoto_unify_symbol_map(symbol_map)
            return _KYOTO_PREV_EXECUTE_SIGNAL_UNIFY(sym_broker, signal, price, mt5_module, symbol_map)
        globals()["execute_signal"] = execute_signal

    _KYOTO_PREV_ORDER_WRAPPER_UNIFY = globals().get("order_wrapper")
    if callable(_KYOTO_PREV_ORDER_WRAPPER_UNIFY):
        def order_wrapper(mt5_module, order_request):
            try:
                req = dict(order_request) if isinstance(order_request, dict) else dict(order_request or {})
            except Exception:
                req = {}
            req["symbol"] = _kyoto_unified_symbol(req.get("symbol") or req.get("instrument") or req.get("symbol_name"))
            return _KYOTO_PREV_ORDER_WRAPPER_UNIFY(mt5_module, req)
        globals()["order_wrapper"] = order_wrapper

    _KYOTO_PREV_PLACE_ORDER_UNIFY = globals().get("place_order_mt5")
    if callable(_KYOTO_PREV_PLACE_ORDER_UNIFY):
        def place_order_mt5(symbol, action, lot, price, sl, tp):
            return _KYOTO_PREV_PLACE_ORDER_UNIFY(_kyoto_unified_symbol(symbol), action, lot, price, sl, tp)
        globals()["place_order_mt5"] = place_order_mt5

    logger.info("FINAL BROKER PATH UNIFICATION v2 armed")
except Exception:
    try:
        logger.exception("FINAL BROKER PATH UNIFICATION v2 failed")
    except Exception:
        pass


# === BEGIN KYOTO MT5 RISK LAYER (append-only) ===
try:
    import math
    import time as _time
    from datetime import datetime as _dt
except Exception:
    pass

try:
    _KYOTO_RISK_LAYER_INSTALLED
except Exception:
    _KYOTO_RISK_LAYER_INSTALLED = False

if not _KYOTO_RISK_LAYER_INSTALLED:
    _KYOTO_RISK_LAYER_INSTALLED = True

    def _kyoto_risk_now() -> float:
        try:
            return float(_time.time())
        except Exception:
            return 0.0

    def _kyoto_risk_symbol_text(symbol) -> str:
        try:
            return str(symbol or "").strip()
        except Exception:
            return ""

    def _kyoto_risk_base_root(symbol) -> str:
        s = _kyoto_risk_symbol_text(symbol).upper().replace(" ", "")
        if not s:
            return ""
        for suf in (".PRO", ".M", "-M"):
            if s.endswith(suf):
                s = s[: -len(suf)]
        if s.endswith("M") and len(s) > 3:
            # preserve canonical roots like EURUSDm -> EURUSD
            s = s[:-1]
        return s

    def _kyoto_risk_timeframe_to_mt5(mt5_module, timeframe_name: str):
        tf = str(timeframe_name or "H1").upper()
        mapping = {
            "M1": "TIMEFRAME_M1",
            "M2": "TIMEFRAME_M2",
            "M3": "TIMEFRAME_M3",
            "M4": "TIMEFRAME_M4",
            "M5": "TIMEFRAME_M5",
            "M6": "TIMEFRAME_M6",
            "M10": "TIMEFRAME_M10",
            "M12": "TIMEFRAME_M12",
            "M15": "TIMEFRAME_M15",
            "M20": "TIMEFRAME_M20",
            "M30": "TIMEFRAME_M30",
            "H1": "TIMEFRAME_H1",
            "H2": "TIMEFRAME_H2",
            "H3": "TIMEFRAME_H3",
            "H4": "TIMEFRAME_H4",
            "H6": "TIMEFRAME_H6",
            "H8": "TIMEFRAME_H8",
            "H12": "TIMEFRAME_H12",
            "D1": "TIMEFRAME_D1",
            "W1": "TIMEFRAME_W1",
            "MN1": "TIMEFRAME_MN1",
        }
        const_name = mapping.get(tf, "TIMEFRAME_H1")
        if mt5_module is not None:
            return getattr(mt5_module, const_name, getattr(mt5_module, "TIMEFRAME_H1", None))
        try:
            import MetaTrader5 as _mt5_local
            return getattr(_mt5_local, const_name, getattr(_mt5_local, "TIMEFRAME_H1", None))
        except Exception:
            return None

    def _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol):
        raw = _kyoto_risk_symbol_text(symbol)
        if not raw:
            return ""
        raw = raw.replace(" ", "")
        base = _kyoto_risk_base_root(raw)
        candidates = []
        def add(x):
            x = _kyoto_risk_symbol_text(x).replace(" ", "")
            if x and x not in candidates:
                candidates.append(x)

        add(raw)
        add(base)
        add(base.upper())
        add(base.lower())
        add(base + "m")
        add(base + ".pro")
        add(base + ".PRO")
        add(base + ".m")
        add(base + "-m")
        if base.endswith("m"):
            add(base[:-1])
        try:
            if mt5_module is not None:
                for cand in list(candidates):
                    try:
                        if hasattr(mt5_module, "symbol_info"):
                            si = mt5_module.symbol_info(cand)
                            if si is not None:
                                try:
                                    if hasattr(mt5_module, "symbol_select"):
                                        mt5_module.symbol_select(cand, True)
                                except Exception:
                                    pass
                                return cand
                    except Exception:
                        continue
                broker_symbols = []
                try:
                    if hasattr(mt5_module, "symbols_get"):
                        broker_symbols = mt5_module.symbols_get() or []
                except Exception:
                    broker_symbols = []
                names = []
                for item in broker_symbols:
                    try:
                        names.append(str(getattr(item, "name", item)).strip())
                    except Exception:
                        continue
                if not names:
                    try:
                        if hasattr(mt5_module, "symbols_total") and hasattr(mt5_module, "symbol_name"):
                            total = int(mt5_module.symbols_total() or 0)
                            for i in range(total):
                                try:
                                    n = mt5_module.symbol_name(i)
                                    if n:
                                        names.append(str(n))
                                except Exception:
                                    continue
                    except Exception:
                        pass
                base_u = base.upper()
                exact = [n for n in names if n.upper() == raw.upper()]
                if exact:
                    return exact[0]
                for n in names:
                    nu = n.upper()
                    if nu.startswith(base_u):
                        return n
                for n in names:
                    nu = n.upper()
                    if base_u in nu:
                        return n
                for cand in candidates:
                    for n in names:
                        if n.upper() == cand.upper():
                            return n
        except Exception:
            pass
        return candidates[0] if candidates else raw

    def _kyoto_risk_get_mt5():
        try:
            mod = globals().get("mt5")
            if mod is not None:
                return mod
        except Exception:
            pass
        try:
            import MetaTrader5 as _mt5_local
            return _mt5_local
        except Exception:
            return None

    def _kyoto_risk_mt5_healthy(mt5_module):
        if mt5_module is None:
            return False, "NO_MT5_MODULE"
        try:
            ai = mt5_module.account_info()
            if ai is None:
                return False, "NO_ACCOUNT"
        except Exception:
            return False, "NO_ACCOUNT"
        try:
            ti = mt5_module.terminal_info()
            if ti is None:
                return False, "NO_TERMINAL"
        except Exception:
            return False, "NO_TERMINAL"
        return True, "OK"

    def _kyoto_risk_tick_is_fresh(tick, max_age_seconds=3.0):
        try:
            if tick is None:
                return False, "NO_TICK"
            now = _kyoto_risk_now()
            ts = None
            try:
                if getattr(tick, "time_msc", None):
                    ts = float(getattr(tick, "time_msc")) / 1000.0
            except Exception:
                ts = None
            if ts is None:
                try:
                    if getattr(tick, "time", None):
                        ts = float(getattr(tick, "time"))
                except Exception:
                    ts = None
            if ts is None or ts <= 0:
                return False, "NO_TICK_TIME"
            age = now - ts
            if age > float(max_age_seconds):
                return False, f"TICK_STALE:{age:.2f}s"
            return True, "OK"
        except Exception:
            return False, "TICK_CHECK_ERROR"

    def _kyoto_risk_symbol_info(mt5_module, symbol):
        try:
            if mt5_module is None:
                return None
            return mt5_module.symbol_info(symbol)
        except Exception:
            return None

    def minimum_symbol_buffer(symbol, mt5_module=None):
        mt5_module = mt5_module or _kyoto_risk_get_mt5()
        sym = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
        root = _kyoto_risk_base_root(sym or symbol)
        si = _kyoto_risk_symbol_info(mt5_module, sym) if sym else None
        point = 0.0
        try:
            point = float(getattr(si, "point", 0.0) or getattr(si, "trade_tick_size", 0.0) or getattr(si, "tick_size", 0.0) or 0.0)
        except Exception:
            point = 0.0
        stops_level = 0.0
        try:
            stops_level = float(getattr(si, "trade_stops_level", 0.0) or getattr(si, "stop_level", 0.0) or 0.0)
        except Exception:
            stops_level = 0.0
        base = point * max(10.0, stops_level if stops_level > 0 else 10.0)
        root_u = root.upper()
        if root_u.startswith("BTC"):
            return max(base, point * 2000.0 if point else 50.0)
        if root_u.startswith("XAU") or root_u.startswith("XAG"):
            return max(base, point * 200.0 if point else 0.75)
        if root_u.startswith("USOIL") or "OIL" in root_u:
            return max(base, point * 150.0 if point else 0.25)
        if root_u.endswith("JPY") or root_u.startswith("JPY"):
            return max(base, point * 30.0 if point else 0.03)
        return max(base, point * 20.0 if point else 0.0003)

    def _kyoto_risk_max_spread(symbol, tick, mt5_module=None):
        mt5_module = mt5_module or _kyoto_risk_get_mt5()
        sym = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
        root = _kyoto_risk_base_root(sym or symbol)
        si = _kyoto_risk_symbol_info(mt5_module, sym) if sym else None
        try:
            spread = float(getattr(tick, "ask", 0.0) - getattr(tick, "bid", 0.0))
        except Exception:
            spread = 0.0
        point = 0.0
        try:
            point = float(getattr(si, "point", 0.0) or getattr(si, "trade_tick_size", 0.0) or getattr(si, "tick_size", 0.0) or 0.0)
        except Exception:
            point = 0.0
        root_u = root.upper()
        if root_u.startswith("BTC"):
            return max(point * 1000.0 if point else 0.0, getattr(tick, "ask", 0.0) * 0.003, 50.0 * point if point else 0.0)
        if root_u.startswith("XAU") or root_u.startswith("XAG"):
            return max(point * 500.0 if point else 0.0, 0.75)
        if root_u.startswith("USOIL") or "OIL" in root_u:
            return max(point * 250.0 if point else 0.0, 0.20)
        if root_u.endswith("JPY") or root_u.startswith("JPY"):
            return max(point * 30.0 if point else 0.0, 0.04)
        return max(point * 20.0 if point else 0.0, 0.00025)

    def _kyoto_risk_validate_volume(mt5_module, symbol, requested=0.01):
        sym = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
        si = _kyoto_risk_symbol_info(mt5_module, sym) if sym else None
        if si is None:
            return None
        try:
            vol_min = float(getattr(si, "volume_min", 0.0) or 0.0)
            vol_step = float(getattr(si, "volume_step", 0.0) or 0.0)
            vol_max = float(getattr(si, "volume_max", 0.0) or 0.0)
        except Exception:
            return None
        if vol_min <= 0:
            vol_min = 0.01
        if vol_step <= 0:
            vol_step = 0.01
        try:
            vol = float(requested)
        except Exception:
            vol = 0.01
        if vol < vol_min:
            vol = vol_min
        if vol_max > 0 and vol > vol_max:
            vol = vol_max
        try:
            steps = round((vol - vol_min) / vol_step)
            vol = vol_min + steps * vol_step
            if vol < vol_min:
                vol = vol_min
            if vol_max > 0 and vol > vol_max:
                vol = vol_max
            # keep a realistic precision for MT5 lot sizes
            vol = float(round(vol, 8))
        except Exception:
            pass
        if vol_max > 0 and (vol < vol_min or vol > vol_max):
            return None
        return float(vol)

    def _kyoto_risk_compute_live_atr(mt5_module, symbol, timeframe_name=None, bars=14):
        mt5_module = mt5_module or _kyoto_risk_get_mt5()
        sym = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
        if not sym or mt5_module is None:
            return None
        tf = _kyoto_risk_timeframe_to_mt5(mt5_module, timeframe_name or globals().get("EXECUTION_TIMEFRAME") or globals().get("STRATEGY_TIMEFRAME") or globals().get("TIMEFRAME") or "H1")
        if tf is None:
            return None
        try:
            rates = mt5_module.copy_rates_from_pos(sym, tf, 0, max(20, int(bars) + 5))
        except Exception:
            rates = None
        if rates is None or len(rates) < int(bars) + 1:
            return None
        try:
            highs = [float(r["high"] if isinstance(r, dict) else r[2]) for r in rates]
            lows = [float(r["low"] if isinstance(r, dict) else r[3]) for r in rates]
            closes = [float(r["close"] if isinstance(r, dict) else r[4]) for r in rates]
            trs = []
            for i in range(1, len(rates)):
                high = highs[i]
                low = lows[i]
                prev_close = closes[i - 1]
                trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            if len(trs) < int(bars):
                return None
            atr = sum(trs[-int(bars):]) / float(int(bars))
            return float(atr)
        except Exception:
            return None

    def estimate_loss_cash(symbol, volume, stop_distance, side=None, entry_price=None):
        mt5_module = _kyoto_risk_get_mt5()
        sym = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
        if not sym or mt5_module is None:
            return None
        side_text = _kyoto_risk_symbol_text(side).lower().strip() if side is not None else ""
        if side_text not in ("buy", "sell"):
            side_text = "buy"
        try:
            vol = float(volume)
            stop_distance = float(stop_distance)
        except Exception:
            return None
        if vol <= 0 or stop_distance <= 0:
            return None
        try:
            si = mt5_module.symbol_info(sym)
        except Exception:
            si = None
        if si is None:
            return None
        try:
            if entry_price is None:
                tick = mt5_module.symbol_info_tick(sym)
                if tick is None:
                    return None
                entry_price = float(getattr(tick, "ask", 0.0) if side_text == "buy" else getattr(tick, "bid", 0.0))
            entry_price = float(entry_price)
            if entry_price <= 0:
                return None
        except Exception:
            return None

        stop_price = entry_price - stop_distance if side_text == "buy" else entry_price + stop_distance
        order_type = getattr(mt5_module, "ORDER_TYPE_BUY", None) if side_text == "buy" else getattr(mt5_module, "ORDER_TYPE_SELL", None)
        if order_type is not None and hasattr(mt5_module, "order_calc_profit"):
            try:
                profit = mt5_module.order_calc_profit(order_type, sym, vol, entry_price, stop_price)
                if profit is not None:
                    return abs(float(profit))
            except Exception:
                pass

        try:
            tick_value = float(getattr(si, "trade_tick_value", 0.0) or getattr(si, "tick_value", 0.0) or 0.0)
            tick_size = float(getattr(si, "trade_tick_size", 0.0) or getattr(si, "tick_size", 0.0) or 0.0)
            contract_size = float(getattr(si, "trade_contract_size", 0.0) or getattr(si, "contract_size", 0.0) or 0.0)
        except Exception:
            return None

        # Preferred fallback: broker tick-value model.
        if tick_value > 0 and tick_size > 0:
            return abs((stop_distance / tick_size) * tick_value * vol)

        # Final fallback: contract-size approximation if metadata is present.
        if contract_size > 0 and entry_price > 0:
            return abs(stop_distance * contract_size * vol)

        return None

    def calc_trade_plan(symbol, tick, atr_value, equity):
        mt5_module = _kyoto_risk_get_mt5()
        ok, reason = _kyoto_risk_mt5_healthy(mt5_module)
        if not ok:
            logger.info("RISK BLOCK %s: %s", symbol, reason)
            return None

        resolved_symbol = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
        if not resolved_symbol:
            logger.info("RISK BLOCK %s: symbol_not_resolved", symbol)
            return None

        try:
            if hasattr(mt5_module, "symbol_select"):
                mt5_module.symbol_select(resolved_symbol, True)
        except Exception:
            pass

        tick_ok, tick_reason = _kyoto_risk_tick_is_fresh(tick, 3.0)
        if not tick_ok:
            logger.info("RISK BLOCK %s: %s", resolved_symbol, tick_reason)
            return None

        try:
            ask = float(getattr(tick, "ask", 0.0))
            bid = float(getattr(tick, "bid", 0.0))
            spread = ask - bid
        except Exception:
            logger.info("RISK BLOCK %s: bad_tick_prices", resolved_symbol)
            return None

        if ask <= 0 or bid <= 0 or spread <= 0:
            logger.info("RISK BLOCK %s: invalid_prices ask=%s bid=%s spread=%s", resolved_symbol, ask, bid, spread)
            return None

        si = _kyoto_risk_symbol_info(mt5_module, resolved_symbol)
        if si is None:
            logger.info("RISK BLOCK %s: missing_symbol_info", resolved_symbol)
            return None

        max_spread = _kyoto_risk_max_spread(resolved_symbol, tick, mt5_module)
        if spread > max_spread:
            logger.info("RISK BLOCK %s: spread_too_wide spread=%s max=%s", resolved_symbol, spread, max_spread)
            return None

        try:
            atr_value = float(atr_value)
        except Exception:
            atr_value = 0.0
        if atr_value <= 0:
            logger.info("RISK BLOCK %s: missing_atr", resolved_symbol)
            return None

        try:
            equity = float(equity)
        except Exception:
            equity = 0.0
        if equity <= 0:
            logger.info("RISK BLOCK %s: invalid_equity", resolved_symbol)
            return None

        risk_pct = float(globals().get("RISK_PCT_PER_TRADE", globals().get("BASE_RISK_PER_TRADE_PCT", 0.005)) or 0.005)
        risk_cash = equity * risk_pct

        spread_buffer = spread * 3.0
        atr_buffer = atr_value * 1.5
        min_buffer = minimum_symbol_buffer(resolved_symbol, mt5_module)
        sl_distance = max(atr_buffer, spread_buffer, min_buffer)
        tp_distance = sl_distance * 1.5

        lot = _kyoto_risk_validate_volume(mt5_module, resolved_symbol, 0.01)
        if lot is None:
            logger.info("RISK BLOCK %s: invalid_volume", resolved_symbol)
            return None

        estimated_buy = estimate_loss_cash(resolved_symbol, lot, sl_distance, side="buy", entry_price=ask)
        estimated_sell = estimate_loss_cash(resolved_symbol, lot, sl_distance, side="sell", entry_price=bid)
        if estimated_buy is None and estimated_sell is None:
            logger.info("RISK BLOCK %s: loss_estimation_failed", resolved_symbol)
            return None

        estimated_loss = max([x for x in (estimated_buy, estimated_sell) if x is not None])
        if estimated_loss > risk_cash:
            logger.info(
                "RISK BLOCK %s: loss_exceeds_budget est_loss=%.6f risk_cash=%.6f lot=%.4f atr=%.6f spread=%.6f sl_distance=%.6f tp_distance=%.6f",
                resolved_symbol, estimated_loss, risk_cash, lot, atr_value, spread, sl_distance, tp_distance,
            )
            return None

        plan = {
            "lot": float(lot),
            "sl_distance": float(sl_distance),
            "tp_distance": float(tp_distance),
            "risk_cash": float(risk_cash),
            "estimated_loss": float(estimated_loss),
            "resolved_symbol": resolved_symbol,
            "spread": float(spread),
            "atr_value": float(atr_value),
            "entry_ask": float(ask),
            "entry_bid": float(bid),
            "max_spread": float(max_spread),
        }
        logger.info(
            "RISK PLAN %s resolved=%s ask=%.8f bid=%.8f spread=%.8f atr=%.8f sl=%.8f tp=%.8f lot=%.4f loss=%.6f budget=%.6f PASS",
            _kyoto_risk_symbol_text(symbol), resolved_symbol, ask, bid, spread, atr_value, sl_distance, tp_distance, lot, estimated_loss, risk_cash
        )
        return plan

    def _kyoto_risk_extract_side(req):
        try:
            side = _kyoto_risk_symbol_text(req.get("type") or req.get("side") or req.get("action")).lower().strip()
            if side in ("buy", "long"):
                return "buy"
            if side in ("sell", "short"):
                return "sell"
            t = req.get("type")
            if isinstance(t, (int, float)):
                return "buy" if int(t) in (0, 1) else "sell"
        except Exception:
            pass
        return "buy"

    def _kyoto_risk_get_equity(mt5_module):
        try:
            info = mt5_module.account_info()
            if info is None:
                return None
            eq = getattr(info, "equity", None)
            if eq is None:
                eq = getattr(info, "balance", None)
            return float(eq) if eq is not None else None
        except Exception:
            return None

    def _kyoto_risk_apply_to_request(mt5_module, req, *, source="live"):
        try:
            if mt5_module is None:
                return False, {"retcode": -1, "comment": "NO_MT5_MODULE", "request": req}, "NO_MT5_MODULE", None
            if req is None:
                return False, {"retcode": -1, "comment": "ORDER_REQUEST_NONE"}, "ORDER_REQUEST_NONE", None
            if not isinstance(req, dict):
                try:
                    req = dict(req)
                except Exception:
                    return False, {"retcode": -1, "comment": "BAD_ORDER_REQUEST"}, "BAD_ORDER_REQUEST", None

            raw_symbol = req.get("symbol") or req.get("instrument") or req.get("symbol_name")
            resolved_symbol = _kyoto_risk_resolve_mt5_symbol(mt5_module, raw_symbol)
            if not resolved_symbol:
                return False, {"retcode": -1, "comment": "NO_SYMBOL", "request": req}, "NO_SYMBOL", None

            try:
                if hasattr(mt5_module, "symbol_select"):
                    mt5_module.symbol_select(resolved_symbol, True)
            except Exception:
                pass

            tick = None
            try:
                tick = mt5_module.symbol_info_tick(resolved_symbol)
            except Exception:
                tick = None

            tick_ok, tick_reason = _kyoto_risk_tick_is_fresh(tick, 3.0)
            if not tick_ok:
                return False, {"retcode": -1, "comment": tick_reason, "request": req}, tick_reason, None

            try:
                ask = float(getattr(tick, "ask", 0.0))
                bid = float(getattr(tick, "bid", 0.0))
                spread = ask - bid
            except Exception:
                return False, {"retcode": -1, "comment": "BAD_TICK", "request": req}, "BAD_TICK", None

            max_spread = _kyoto_risk_max_spread(resolved_symbol, tick, mt5_module)
            if spread > max_spread:
                reason = f"SPREAD_TOO_HIGH:{spread:.8f}>{max_spread:.8f}"
                logger.info("RISK BLOCK %s: %s", resolved_symbol, reason)
                return False, {"retcode": -1, "comment": reason, "request": req}, reason, None

            ok, mt5_reason = _kyoto_risk_mt5_healthy(mt5_module)
            if not ok:
                return False, {"retcode": -1, "comment": mt5_reason, "request": req}, mt5_reason, None

            equity = _kyoto_risk_get_equity(mt5_module)
            if equity is None:
                return False, {"retcode": -1, "comment": "NO_EQUITY", "request": req}, "NO_EQUITY", None

            atr_value = None
            for key in ("atr_value", "atr", "atr14", "atr_for_entry", "atr_at_entry"):
                try:
                    if key in req and req.get(key) not in (None, "", 0, 0.0):
                        atr_value = float(req.get(key))
                        break
                except Exception:
                    continue
            if atr_value is None:
                atr_value = _kyoto_risk_compute_live_atr(mt5_module, resolved_symbol, req.get("timeframe") or req.get("tf") or globals().get("EXECUTION_TIMEFRAME") or globals().get("STRATEGY_TIMEFRAME") or globals().get("TIMEFRAME") or "H1")
            if atr_value is None:
                return False, {"retcode": -1, "comment": "NO_ATR", "request": req}, "NO_ATR", None

            plan = calc_trade_plan(resolved_symbol, tick, atr_value, equity)
            if plan is None:
                return False, {"retcode": -1, "comment": "RISK_PLAN_REJECTED", "request": req}, "RISK_PLAN_REJECTED", None

            side = _kyoto_risk_extract_side(req)
            req["symbol"] = plan["resolved_symbol"]
            req["volume"] = float(plan["lot"])
            req["price"] = float(ask if side == "buy" else bid)
            if side == "buy":
                req["sl"] = float(req["price"] - plan["sl_distance"])
                req["tp"] = float(req["price"] + plan["tp_distance"])
                req["type"] = getattr(mt5_module, "ORDER_TYPE_BUY", req.get("type"))
            else:
                req["sl"] = float(req["price"] + plan["sl_distance"])
                req["tp"] = float(req["price"] - plan["tp_distance"])
                req["type"] = getattr(mt5_module, "ORDER_TYPE_SELL", req.get("type"))
            req.setdefault("deviation", 20)
            req.setdefault("magic", int(globals().get("MAGIC_NUMBER", 123456)))
            req.setdefault("comment", str(req.get("comment") or "kyoto_risk"))
            req["_kyoto_risk_plan"] = dict(plan)
            req["_kyoto_risk_source"] = source
            return True, req, "OK", plan
        except Exception as e:
            logger.exception("risk apply failed from %s", source)
            return False, {"retcode": -1, "comment": f"RISK_ERROR:{e}", "request": req}, "RISK_ERROR", None

    # Make the strategy-execution wrappers all pass through the same risk gate.
    try:
        _KYOTO_PREV_rt_send_request = globals().get("_kyoto_rt_send_request")
        if callable(_KYOTO_PREV_rt_send_request):
            def _kyoto_rt_send_request(mt5_module, req):
                ok, new_req, reason, plan = _kyoto_risk_apply_to_request(mt5_module, req, source="_kyoto_rt_send_request")
                if not ok:
                    return new_req
                return _KYOTO_PREV_rt_send_request(mt5_module, new_req)
            globals()["_kyoto_rt_send_request"] = _kyoto_rt_send_request
    except Exception:
        pass

    try:
        _KYOTO_PREV_send_request = globals().get("_kyoto_final_send_request")
        if callable(_KYOTO_PREV_send_request):
            def _kyoto_final_send_request(mt5_module, req):
                ok, new_req, reason, plan = _kyoto_risk_apply_to_request(mt5_module, req, source="_kyoto_final_send_request")
                if not ok:
                    return new_req
                return _KYOTO_PREV_send_request(mt5_module, new_req)
            globals()["_kyoto_final_send_request"] = _kyoto_final_send_request
    except Exception:
        pass

    try:
        _KYOTO_PREV_single_door_submit = globals().get("_kyoto_single_door_submit")
        if callable(_KYOTO_PREV_single_door_submit):
            def _kyoto_single_door_submit(mt5_module, order_request, *, source="live"):
                ok, new_req, reason, plan = _kyoto_risk_apply_to_request(mt5_module, order_request, source=source)
                if not ok:
                    return new_req
                return _KYOTO_PREV_single_door_submit(mt5_module, new_req, source=source)
            globals()["_kyoto_single_door_submit"] = _kyoto_single_door_submit
    except Exception:
        pass

    try:
        _KYOTO_PREV_place_order_mt5 = globals().get("place_order_mt5")
        if callable(_KYOTO_PREV_place_order_mt5):
            def place_order_mt5(*args, **kwargs):
                req = {}
                if len(args) == 1 and isinstance(args[0], dict):
                    req = dict(args[0])
                elif len(args) >= 6:
                    req = {
                        "symbol": args[0],
                        "type": args[1],
                        "volume": args[2],
                        "price": args[3],
                        "sl": args[4],
                        "tp": args[5],
                    }
                elif len(args) >= 1:
                    req["symbol"] = args[0]
                if kwargs:
                    req.update(kwargs)
                mt5_module = kwargs.get("mt5_module") or globals().get("_mt5") or _kyoto_risk_get_mt5()
                ok, new_req, reason, plan = _kyoto_risk_apply_to_request(mt5_module, req, source="place_order_mt5")
                if not ok:
                    return new_req
                if mt5_module is None:
                    return {"retcode": -1, "comment": "NO_MT5_MODULE", "request": new_req}
                final_send = globals().get("_kyoto_final_send_request")
                if callable(final_send):
                    return final_send(mt5_module, new_req)
                return {"retcode": -1, "comment": "NO_FINAL_SEND_REQUEST", "request": new_req}
            globals()["place_order_mt5"] = place_order_mt5
    except Exception:
        pass

    try:
        _KYOTO_PREV_ORDER_WRAPPER = globals().get("order_wrapper")
        if callable(_KYOTO_PREV_ORDER_WRAPPER):
            def order_wrapper(mt5_module, order_request):
                ok, new_req, reason, plan = _kyoto_risk_apply_to_request(mt5_module, order_request, source="order_wrapper")
                if not ok:
                    return new_req
                return _KYOTO_PREV_ORDER_WRAPPER(mt5_module, new_req)
            globals()["order_wrapper"] = order_wrapper
    except Exception:
        pass

    try:
        _KYOTO_PREV_UVX_MARKET_ORDER = globals().get("UVXExecutionEngine", None)
        if _KYOTO_PREV_UVX_MARKET_ORDER is not None and hasattr(_KYOTO_PREV_UVX_MARKET_ORDER, "market_order"):
            _KYOTO_ORIG_UVX_MARKET_ORDER = getattr(_KYOTO_PREV_UVX_MARKET_ORDER, "market_order")
            def _kyoto_risk_uvx_market_order(self, symbol, side, size, sl=None, tp=None):
                try:
                    if getattr(self, "mode", "dry_run") != "mt5":
                        return _KYOTO_ORIG_UVX_MARKET_ORDER(self, symbol, side, size, sl=sl, tp=tp)
                    mt5_module = getattr(self, "_mt5", None) or _kyoto_risk_get_mt5()
                    resolved = _kyoto_risk_resolve_mt5_symbol(mt5_module, symbol)
                    if not resolved:
                        return {"order_id": None, "status": "blocked", "comment": "NO_SYMBOL"}
                    tick = None
                    try:
                        tick = mt5_module.symbol_info_tick(resolved)
                    except Exception:
                        tick = None
                    tick_ok, tick_reason = _kyoto_risk_tick_is_fresh(tick, 3.0)
                    if not tick_ok:
                        return {"order_id": None, "status": "blocked", "comment": tick_reason}
                    equity = _kyoto_risk_get_equity(mt5_module)
                    if equity is None:
                        return {"order_id": None, "status": "blocked", "comment": "NO_EQUITY"}
                    atr_value = _kyoto_risk_compute_live_atr(mt5_module, resolved, globals().get("EXECUTION_TIMEFRAME") or globals().get("STRATEGY_TIMEFRAME") or globals().get("TIMEFRAME") or "H1")
                    if atr_value is None:
                        return {"order_id": None, "status": "blocked", "comment": "NO_ATR"}
                    plan = calc_trade_plan(resolved, tick, atr_value, equity)
                    if plan is None:
                        return {"order_id": None, "status": "blocked", "comment": "RISK_PLAN_REJECTED"}
                    req = {
                        "action": getattr(mt5_module, "TRADE_ACTION_DEAL", None),
                        "symbol": plan["resolved_symbol"],
                        "volume": plan["lot"],
                        "type": getattr(mt5_module, "ORDER_TYPE_BUY", None) if str(side).lower() == "buy" else getattr(mt5_module, "ORDER_TYPE_SELL", None),
                        "price": float(getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)),
                        "deviation": 10,
                        "magic": 123456,
                        "comment": "kyoto_risk",
                        "sl": float((getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)) - plan["sl_distance"]) if str(side).lower() == "buy" else float((getattr(tick, "bid", 0.0)) + plan["sl_distance"]),
                        "tp": float((getattr(tick, "ask", 0.0) if str(side).lower() == "buy" else getattr(tick, "bid", 0.0)) + plan["tp_distance"]) if str(side).lower() == "buy" else float((getattr(tick, "bid", 0.0)) - plan["tp_distance"]),
                    }
                    return _kyoto_final_send_request(mt5_module, req)
                except Exception:
                    logger.exception("risk wrapped UVXExecutionEngine.market_order failed")
                    return _KYOTO_ORIG_UVX_MARKET_ORDER(self, symbol, side, size, sl=sl, tp=tp)
            UVXExecutionEngine.market_order = _kyoto_risk_uvx_market_order
    except Exception:
        pass

    logger.info(
        "KYOTO MT5 risk layer armed: calc_trade_plan / estimate_loss_cash / symbol resolution / spread / freshness / live request gating"
    )

# === END KYOTO MT5 RISK LAYER ===
# === END KYOTO MT5 RISK LAYER ===
