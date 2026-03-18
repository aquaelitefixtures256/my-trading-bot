# signal_publisher_wrapper.py
"""
Lightweight signal publisher wrapper used by the runner and the mother bot.

- Exposes `signals_queue` (queue.Queue) that the runner will consume.
- Exposes `publish_signal(sig)` to push validated signals.
- Provides a small `start_autopublisher(...)` helper that demonstrates how
  the mother bot (v15) could publish signals periodically. That helper
  is intentionally conservative and acts as a placeholder — replace its
  internals with your real H1/M30 confirmation logic later.
"""

import queue
import time
import uuid
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("signal_pub")

# Public queue that runner will import & consume from
signals_queue = queue.Queue()

def publish_signal(sig: dict):
    """
    Put a validated signal into the shared queue.

    Required minimal fields (must be present):
      - symbol (e.g. "EURUSD")
      - side ("buy" or "sell")
      - price (float)
      - sl_price (float)
      - tp_price (float)

    Optional:
      - lot (float)   -> if provided runner should prefer this lot
      - risk_pct (float)
      - resolved_symbol (str)
      - id (str)      -> optional unique id
      - timestamp (float)
    """
    # shallow validation
    if not isinstance(sig, dict):
        raise ValueError("signal must be dict")
    required = ("symbol", "side", "price", "sl_price", "tp_price")
    missing = [k for k in required if k not in sig]
    if missing:
        raise ValueError(f"signal missing required fields: {missing}")

    # ensure id + timestamp
    if "id" not in sig:
        sig["id"] = str(uuid.uuid4())
    sig["timestamp"] = sig.get("timestamp", time.time())

    # normalize types
    try:
        sig["price"] = float(sig["price"])
        sig["sl_price"] = float(sig["sl_price"])
        sig["tp_price"] = float(sig["tp_price"])
    except Exception:
        raise ValueError("price/sl/tp must be numeric")

    # put into queue
    signals_queue.put(sig)
    logger.info("Published signal: id=%s sym=%s side=%s", sig["id"], sig["symbol"], sig["side"])
    return sig["id"]

# -------------------------
# small ATR helper (safe, re-usable)
def compute_atr_from_recent(bars, period=14):
    """
    Safe ATR for arrays/lists: bars either list of dicts with 'high','low','close'
    or numpy recarray rows [time,open,high,low,close,...].
    Returns float ATR (average true range).
    """
    if not bars:
        return 0.0
    norm = []
    for b in bars:
        if isinstance(b, dict):
            try:
                h = float(b.get("high", b.get("h", 0.0)))
                l = float(b.get("low", b.get("l", 0.0)))
                c = float(b.get("close", b.get("c", 0.0)))
            except Exception:
                continue
        else:
            # assume tuple-like (time, o, h, l, c, ...)
            try:
                h = float(b[2]); l = float(b[3]); c = float(b[4])
            except Exception:
                # skip malformed
                continue
        norm.append({"high": h, "low": l, "close": c})
    if len(norm) < 2:
        return 0.0
    trs = []
    for i in range(1, len(norm)):
        h = norm[i]["high"]; l = norm[i]["low"]; pc = norm[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return 0.0
    if len(trs) < period:
        return sum(trs) / len(trs)
    return sum(trs[-period:]) / period

# -------------------------
# Simple autopublisher demonstration (placeholder)
def start_autopublisher(v15_module, mt5_module, symbols, stop_event, poll_interval=5, sig_thresh=0.9):
    """
    Simple example publisher loop. This is a *placeholder* to show wiring.
    It:
      - reads price/bars from mt5,
      - calls v15_module.compute_signal(sym, price, {"bars":...}) if available,
      - if |sig| >= sig_thresh, it publishes a minimal signal dict.
    IMPORTANT: Replace the internals with your mother bot's H1+M30 confirmation,
    and/or call the mother's confirmation function instead of this placeholder.

    Call like:
       stop_event = threading.Event()
       t = threading.Thread(target=start_autopublisher, args=(v15, mt5, symbols, stop_event))
       t.start()
       ...
       stop_event.set()
    """
    logger.info("Autopublisher started (placeholder). Symbols=%s", symbols)
    while not stop_event.is_set():
        for sym in symbols:
            try:
                # best-effort get price and bars
                try:
                    tick = mt5_module.symbol_info_tick(sym + "m") or mt5_module.symbol_info_tick(sym)
                    price = float(tick.ask or tick.bid)
                except Exception:
                    price = 0.0

                bars = None
                try:
                    bars = mt5_module.copy_rates_from_pos(sym + "m", getattr(mt5_module, "TIMEFRAME_M1"), 0, 120)
                except Exception:
                    try:
                        bars = mt5_module.copy_rates_from_pos(sym, getattr(mt5_module, "TIMEFRAME_M1"), 0, 120)
                    except Exception:
                        bars = None

                sig_val = None
                if v15_module is not None and hasattr(v15_module, "compute_signal"):
                    # pass a minimal bars list -- replace with real ctx
                    ctx_bars = [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
                                 "low": float(r[3]), "close": float(r[4])} for r in (bars or [])]
                    sig_val = v15_module.compute_signal(sym, price, {"bars": ctx_bars})
                # very conservative publishing: only publish if strong
                if sig_val is not None and abs(sig_val) >= sig_thresh and price:
                    atr = compute_atr_from_recent(bars, period=14)
                    # build minimal signal: prefer lot left blank (mother decides)
                    sl_price = price - (atr * 1.5) if sig_val > 0 else price + (atr * 1.5)
                    tp_price = price + (atr * 3.0) if sig_val > 0 else price - (atr * 3.0)
                    sig = {
                        "symbol": sym,
                        "resolved_symbol": sym + "m",
                        "side": "buy" if sig_val > 0 else "sell",
                        "price": price,
                        "sl_price": sl_price,
                        "tp_price": tp_price,
                        "risk_pct": None,   # leave None so runner will ask mother-bot for exact lot if possible
                        "origin": "autopublisher-placeholder",
                    }
                    publish_signal(sig)
            except Exception:
                logger.exception("autopublisher loop error for %s", sym)
        # be polite, small sleep
        stop_event.wait(poll_interval)

# -------------------------
# Convenience quick-test helper (not starting threads)
def quick_test_publish_and_read():
    """Small self-test: publish a sample signal and read it back immediately."""
    s = {
        "symbol": "EURUSD",
        "resolved_symbol": "EURUSDm",
        "side": "buy",
        "price": 1.1500,
        "sl_price": 1.1490,
        "tp_price": 1.1520,
        "lot": 0.01
    }
    publish_signal(s)
    # try to get it
    try:
        got = signals_queue.get(timeout=1.0)
        logger.info("quick_test got signal: %s", got)
        return got
    except Exception as e:
        logger.exception("quick_test failed: %s", e)
        return None
