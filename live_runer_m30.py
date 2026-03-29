#!/usr/bin/env python3
"""
live_runner_m30.py

M30-entry / H1-confirmation live runner that imports your mother bot (the "brain")
and attempts to use it (in-process) to compute signals. This runner:

 - Starts any background task the mother bot exposes (non-destructive).
 - Uses M30 bars for entry and H1 bars for confirmation (like the mother bot).
 - Reads per-symbol params & max-open-trades from the mother bot (if exposed).
 - Uses ATR (from M30 bars) to compute SL/TP and a risk-based lot sizing helper.
 - Writes each attempted / placed trade to live_trades_m30.csv and logs to live_runner_m30.log
 - Kill-switch: create an empty file named "kill_live.txt" or "KILL_NOW" in same folder.

Usage:
    python live_runner_m30.py

NOTES:
 - DRY_RUN remains the safe default; if account does not look like demo/trial, DRY_RUN is forced.
 - This runner *loads* your bot module (importlib) and calls its compute_signal (or v15 adapter).
 - The runner will respect per-symbol max open trades if the mother bot exposes that mapping.
"""

from datetime import datetime, timedelta
import importlib.util
import logging
import math
import os
import sys
import time
import threading
import csv

# ---------- CONFIG ----------
BOT_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"  # change if your bot has a different filename
SYMBOLS = ["BTCUSD", "EURUSD", "USDJPY", "USOIL", "DXY"]  # XAU removed as requested
TIMEFRAME_ENTRY = "M30"
TIMEFRAME_CONFIRM = "H1"
RUN_HOURS = 7
LOOP_DELAY = 60  # seconds between iterations
ACCOUNT_BALANCE_OVERRIDE = 30.0  # user-provided starting balance (demo override)
RISK_PER_TRADE_PCT = 0.5  # percent of account to risk per trade
SL_ATR_MULT = 1.8
TP_ATR_MULT = 3.0
ATR_PERIOD = 14
MIN_BARS_FOR_ATR = 20
DRY_RUN = False  # user requested False, but runner will force DRY_RUN if not demo
EXECUTE_ORDERS = True  # only used when account looks demo/trial
OUT_CSV = "live_trades_m30.csv"
LOG_FILE = "live_runner_m30.log"
KILL_FILE = "kill_live.txt"
MAX_TRADES_PER_DAY = 50
MAX_DAILY_LOSS_USD = 0.2 * ACCOUNT_BALANCE_OVERRIDE
# --------------------------------

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)


def load_bot(path):
    """Dynamically import the mother bot as a module and return it."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Bot file not found: {path}")
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot


def start_mother_background(bot):
    """
    If the mother bot has background tasks (scraper/calendar) expose a start helper,
    call it in a daemon thread. We try multiple common names.
    Non-destructive: if none found, do nothing.
    """
    # candidate names the bot might use
    candidates = ["start_background_tasks", "start_background", "run_background", "start_daemon"]
    for name in candidates:
        fn = getattr(bot, name, None)
        if callable(fn):
            try:
                t = threading.Thread(target=fn, daemon=True, name=f"bot-bg-{name}")
                t.start()
                logging.info("Started mother-bot background via %s()", name)
                return True
            except Exception as e:
                logging.exception("Failed to start background task %s(): %s", name, e)
                return False
    # some bots expose a "BACKGROUND_THREAD" or "bg_thread" object we can start
    return False


def compute_atr_from_recent(bars, period=ATR_PERIOD):
    """
    Compute ATR from bars.
    Accepts either:
      - list of dicts with keys high/low/close; OR
      - numpy recarray or list/tuple rows (time, open, high, low, close, ...)
    Returns ATR (float) or 0.0 if insufficient data.
    """
    if bars is None:
        return 0.0
    # convert to list (safe even if bars is a numpy recarray)
    try:
        bars_list = list(bars)
    except Exception:
        bars_list = bars if isinstance(bars, (list, tuple)) else []
    norm = []
    for b in bars_list:
        if isinstance(b, dict):
            try:
                h = float(b.get("high", b.get("h", 0.0)))
                l = float(b.get("low", b.get("l", 0.0)))
                c = float(b.get("close", b.get("c", 0.0)))
            except Exception:
                continue
        else:
            # assume tuple-like: (time, open, high, low, close, ...)
            try:
                h = float(b[2])
                l = float(b[3])
                c = float(b[4])
            except Exception:
                # try mapping style fallback
                try:
                    h = float(b["high"])
                    l = float(b["low"])
                    c = float(b["close"])
                except Exception:
                    continue
        norm.append({"high": h, "low": l, "close": c})
    if len(norm) < 2:
        return 0.0
    trs = []
    for i in range(1, len(norm)):
        high = norm[i]["high"]
        low = norm[i]["low"]
        prev_close = norm[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    if len(trs) < period:
        return sum(trs) / len(trs)
    return sum(trs[-period:]) / period


def calc_volume_for_risk(mt5_module, symbol, account_balance_usd, risk_pct, sl_price_distance, min_volume=0.01):
    """
    Compute lot sized based on MT5 symbol info and dollar risk.
    Returns (lot, note).
    """
    try:
        si = mt5_module.symbol_info(symbol)
        if si is None:
            return (min_volume, "no_symbol_info")
        tick_val = getattr(si, "trade_tick_value", None)
        point = getattr(si, "point", None) or 1e-8
        contract_size = getattr(si, "trade_contract_size", None) or 1.0
        volume_step = getattr(si, "volume_step", 0.01) or 0.01
        volume_min = getattr(si, "volume_min", min_volume) or min_volume
        volume_max = getattr(si, "volume_max", 100.0) or 100.0

        usd_risk = account_balance_usd * (risk_pct / 100.0)
        if tick_val and point:
            ticks = abs(sl_price_distance) / point if point != 0 else abs(sl_price_distance)
            usd_loss_per_lot = ticks * float(tick_val)
        elif contract_size:
            usd_loss_per_lot = abs(sl_price_distance) * float(contract_size)
        else:
            usd_loss_per_lot = abs(sl_price_distance)

        if usd_loss_per_lot <= 0:
            return (volume_min, "zero_loss_per_lot")

        raw_lots = usd_risk / usd_loss_per_lot
        steps = math.floor(raw_lots / volume_step)
        lot = (steps * volume_step) if steps >= 1 else volume_min
        lot = max(volume_min, min(volume_max, lot))
        return (round(lot, 8), "calc_volume_for_risk")
    except Exception as e:
        return (min_volume, f"error_calc:{e}")


def send_order_mt5(mt5, symbol, side, lot, price, sl, tp):
    """
    Place a market order using mt5.order_send. Returns (success_bool, result_dict)
    """
    try:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            return (False, {"error": "symbol_info_none"})
        ticket_type = mt5.ORDER_TYPE_BUY if side == "buy" else mt5.ORDER_TYPE_SELL
        deviation = 20
        price_field = float(mt5.symbol_info_tick(symbol).ask) if side == "buy" else float(mt5.symbol_info_tick(symbol).bid)
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(lot),
            "type": ticket_type,
            "price": price_field,
            "sl": float(sl),
            "tp": float(tp),
            "deviation": deviation,
            "magic": 123456,
            "comment": "live_runner_m30",
            "type_filling": mt5.ORDER_FILLING_FOK if hasattr(mt5, "ORDER_FILLING_FOK") else mt5.ORDER_FILLING_IOC,
            "type_time": mt5.ORDER_TIME_GTC,
        }
        res = mt5.order_send(request)
        # res may be a namedtuple or object; coerce to dict if possible
        if hasattr(res, "_asdict"):
            return (True, res._asdict())
        try:
            return (True, dict(res))
        except Exception:
            return (True, {"result": str(res)})
    except Exception as e:
        return (False, {"error": str(e)})


def write_row_csv(row):
    header = ["time", "symbol", "signal", "side", "price", "sl_price", "tp_price", "lot", "dollar_risk", "sl_points", "tp_points", "status", "note"]
    newfile = not os.path.exists(OUT_CSV)
    try:
        with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if newfile:
                w.writerow(header)
            w.writerow(row)
    except Exception:
        logging.exception("Failed to write CSV row")


def get_resolved_symbol(mt5, sym):
    """Try common naming variants for symbol and ensure symbol_select."""
    candidates = [sym + "m", sym, sym + "USDm", sym + ".m"]
    for c in candidates:
        try:
            si = mt5.symbol_info(c)
            if si:
                try:
                    mt5.symbol_select(c, True)
                except Exception:
                    pass
                return c
        except Exception:
            continue
    return None


def is_demo_account(mt5):
    try:
        acc = mt5.account_info()
        if acc is None:
            return False
        srv = (acc.server or "").lower()
        comp = (acc.company or "").lower()
        if "trial" in srv or "demo" in srv or "test" in srv or "trial" in comp or "demo" in comp:
            return True
        return False
    except Exception:
        return False


def kill_switch_check():
    if os.path.exists(KILL_FILE) or os.path.exists("KILL_NOW"):
        logging.info("Kill file detected — shutting down.")
        sys.exit(0)


def safe_bars_to_list(bars):
    """Convert bars (numpy recarray or list) to a Python list safely."""
    if bars is None:
        return []
    try:
        return list(bars)
    except Exception:
        # some exotic structure, return as-is if already list-like
        return bars if isinstance(bars, (list, tuple)) else []


def find_compute_signal_and_invoke(v15, bot, sym, resolved_sym, price, m30_bars, h1_bars):
    """
    Robust compute_signal invocation:
      - Try v15.compute_signal(sym, price, ctx)
      - Try v15.compute_signal(resolved_sym, price, ctx)
      - Try bot.compute_signal(sym, price, ctx)
      - Try bot.compute_signal(resolved_sym, price, ctx)
    Returns (signal or None, which_was_used_str, tried_list)
    """
    tried = []
    ctx = {
        "bars_m30": [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])} for r in safe_bars_to_list(m30_bars)],
        "bars_h1": [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])} for r in safe_bars_to_list(h1_bars)],
    }
    # also provide legacy 'bars' convenience as M30 bars
    ctx["bars"] = ctx["bars_m30"]
    # helper to try a callable
    def try_call(obj, fnname, symbol_name):
        fn = getattr(obj, fnname, None)
        if not callable(fn):
            return None, f"{obj.__name__ if hasattr(obj,'__name__') else str(obj)}:{fnname}_not_callable"
        try:
            sig = fn(symbol_name, price, ctx)
            return sig, f"{obj.__name__ if hasattr(obj,'__name__') else str(obj)}:{fnname}@{symbol_name}"
        except Exception as e:
            return None, f"error:{fnname}@{symbol_name}:{e}"

    # order of attempts
    attempts = []
    if v15 is not None:
        attempts.append( (v15, "compute_signal", sym) )
        attempts.append( (v15, "compute_signal", resolved_sym) )
    attempts.append( (bot, "compute_signal", sym) )
    attempts.append( (bot, "compute_signal", resolved_sym) )

    for obj, fnname, symbol_name in attempts:
        if obj is None:
            tried.append(f"{getattr(obj,'__name__',str(obj))}:none")
            continue
        sig, note = try_call(obj, fnname, symbol_name)
        tried.append(note)
        if sig is not None:
            return sig, note, tried
    return None, None, tried


def main():
    # load bot
    try:
        bot = load_bot(BOT_FILE)
        logging.info("Loaded bot: %s", BOT_FILE)
    except Exception as e:
        logging.exception("Failed to load bot module: %s", e)
        return

    # start mother background if available
    try:
        started = start_mother_background(bot)
        if started:
            logging.info("Mother bot background started (if any)")
    except Exception:
        logging.exception("Failed starting mother background (ignored)")

    # attempt to load v15 adapter if provided
    v15 = None
    try:
        loader = getattr(bot, "load_v15_module", None) or getattr(bot, "load_v15", None)
        if callable(loader):
            try:
                v15 = loader()
                logging.info("v15 loaded: %s", getattr(v15, "__name__", v15))
            except Exception:
                logging.exception("v15 loader failed; continuing without v15")
                v15 = None
    except Exception:
        v15 = None

    # initialize MT5
    try:
        mt5 = getattr(bot, "mt5", None)
        if mt5 is None:
            import MetaTrader5 as mt5
        ok_init = False
        try:
            ok_init = bool(mt5.initialize())
        except Exception:
            ok_init = False
        if not ok_init:
            logging.error("mt5.initialize() failed or returned False")
            return
        acc = mt5.account_info()
        if acc is None:
            logging.warning("mt5.account_info() returned None; using override balance")
            account_balance = ACCOUNT_BALANCE_OVERRIDE
        else:
            account_balance = float(acc.balance)
            if ACCOUNT_BALANCE_OVERRIDE and ACCOUNT_BALANCE_OVERRIDE > 0:
                logging.info("Overriding account balance with provided ACCOUNT_BALANCE_OVERRIDE=%s", ACCOUNT_BALANCE_OVERRIDE)
                account_balance = ACCOUNT_BALANCE_OVERRIDE
        is_demo = is_demo_account(mt5)
        if not is_demo:
            logging.warning("Account does not look like demo/trial -> forcing DRY_RUN for safety.")
    except Exception as e:
        logging.exception("MT5 init failed: %s", e)
        return

    do_execute = EXECUTE_ORDERS and is_demo and (not DRY_RUN)
    if not is_demo:
        do_execute = False

    # get any per-symbol max open trades mapping from the mother bot
    max_open_map = {}
    try:
        if hasattr(bot, "MAX_OPEN_TRADES"):
            max_open_map = getattr(bot, "MAX_OPEN_TRADES") or {}
        elif hasattr(bot, "max_open_trades"):
            max_open_map = getattr(bot, "max_open_trades") or {}
        else:
            # sometimes stored in params mapping
            params_map = getattr(bot, "params", {}) or {}
            for k, v in params_map.items():
                # keys often like 'EURUSDm'
                try:
                    max_open_map[k] = int(v.get("max_open_trades") or v.get("max_open") or 0)
                except Exception:
                    continue
    except Exception:
        max_open_map = {}

    logging.info("Starting main loop. RUN_HOURS=%s DRY_RUN=%s EXECUTE=%s RiskPct=%s", RUN_HOURS, DRY_RUN, do_execute, RISK_PER_TRADE_PCT)
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(hours=RUN_HOURS)
    trades_today = 0
    cumulative_pnl = 0.0

    # gather timeframe constants safely
    tf_entry_const = getattr(mt5, f"TIMEFRAME_{TIMEFRAME_ENTRY}", None)
    tf_confirm_const = getattr(mt5, f"TIMEFRAME_{TIMEFRAME_CONFIRM}", None)
    if tf_entry_const is None or tf_confirm_const is None:
        # fallback to common names
        tf_entry_const = getattr(mt5, "TIMEFRAME_M30", None) or tf_entry_const
        tf_confirm_const = getattr(mt5, "TIMEFRAME_H1", None) or tf_confirm_const

    while datetime.utcnow() < end_time:
        kill_switch_check()
        try:
            for sym in SYMBOLS:
                try:
                    resolved = get_resolved_symbol(mt5, sym)
                    if not resolved:
                        logging.warning("Symbol %s not available -> skipping", sym)
                        continue

                    # check current open positions for this symbol (respect mother-bot max limits)
                    try:
                        positions = mt5.positions_get(symbol=resolved) or []
                        open_count = len(positions)
                    except Exception:
                        open_count = 0

                    allowed_open = max_open_map.get(resolved, max_open_map.get(sym + "m", None))
                    if allowed_open is None:
                        # fallback to 10 if not specified
                        allowed_open = None

                    if allowed_open is not None and open_count >= int(allowed_open):
                        logging.info("Symbol %s has open_count=%s which >= allowed %s -> skipping", resolved, open_count, allowed_open)
                        continue

                    # fetch entry (M30) and confirm (H1) bars
                    m30_bars = []
                    h1_bars = []
                    try:
                        if tf_entry_const is not None:
                            m30_bars = mt5.copy_rates_from_pos(resolved, tf_entry_const, 0, 200)
                        else:
                            m30_bars = mt5.copy_rates_from_pos(resolved, mt5.TIMEFRAME_M30, 0, 200)
                    except Exception:
                        m30_bars = []
                    try:
                        if tf_confirm_const is not None:
                            h1_bars = mt5.copy_rates_from_pos(resolved, tf_confirm_const, 0, 200)
                        else:
                            h1_bars = mt5.copy_rates_from_pos(resolved, mt5.TIMEFRAME_H1, 0, 200)
                    except Exception:
                        h1_bars = []

                    # make them lists safe for processing
                    m30_bars_list = safe_bars_to_list(m30_bars)
                    h1_bars_list = safe_bars_to_list(h1_bars)

                    # price tick
                    tick = mt5.symbol_info_tick(resolved)
                    if tick is None:
                        logging.warning("No tick for %s", resolved)
                        continue
                    price = float(tick.ask or tick.bid)

                    # compute ATR from M30 bars (entry TF)
                    atr = compute_atr_from_recent(m30_bars_list, period=ATR_PERIOD)
                    if atr <= 0:
                        logging.debug("ATR<=0 for %s; skipping", resolved)
                        continue

                    # compute signal using robust block (v15 or bot, sym or resolved)
                    sig, used, tried = find_compute_signal_and_invoke(v15, bot, sym, resolved, price, m30_bars_list, h1_bars_list)
                    if sig is None:
                        logging.warning("No compute signal resolved for sym=%s resolved=%s tried=%s", sym, resolved, tried)
                        continue
                    logging.debug("Signal for %s -> %s (via %s)", resolved, sig, used)

                    # resolve signal threshold from bot params or DEFAULT_SIGNAL_THRESH
                    signal_thresh = getattr(bot, "DEFAULT_SIGNAL_THRESH", 0.92)
                    try:
                        per_params = getattr(bot, "params", {}) or {}
                        p = per_params.get(resolved) or per_params.get(sym + "m") or {}
                        if isinstance(p, dict):
                            signal_thresh = float(p.get("signal_thresh", signal_thresh))
                    except Exception:
                        pass

                    if abs(sig) < signal_thresh:
                        logging.debug("%s signal %.4f below thresh %.4f", resolved, float(sig), float(signal_thresh))
                        continue

                    # side & SL/TP from ATR (M30)
                    side = "buy" if float(sig) > 0 else "sell"
                    sl_points = SL_ATR_MULT * atr
                    tp_points = TP_ATR_MULT * atr
                    if side == "buy":
                        sl_price = price - sl_points
                        tp_price = price + tp_points
                    else:
                        sl_price = price + sl_points
                        tp_price = price - tp_points

                    # lot sizing
                    dollar_risk = account_balance * (RISK_PER_TRADE_PCT / 100.0)
                    si = mt5.symbol_info(resolved)
                    # prefer calc_volume_for_risk, fallback to other
                    lot, note = calc_volume_for_risk(mt5, resolved, account_balance, RISK_PER_TRADE_PCT, sl_points, min_volume=getattr(si, "volume_min", 0.01))
                    if lot < float(getattr(si, "volume_min", 0.01)):
                        lot = float(getattr(si, "volume_min", 0.01))
                        note = (note or "") + "|used_volume_min"

                    # prepare CSV row
                    row_tpl = [
                        datetime.utcnow().isoformat(),
                        resolved,
                        f"{float(sig):.6f}",
                        side,
                        f"{price:.6f}",
                        f"{sl_price:.6f}",
                        f"{tp_price:.6f}",
                        f"{lot:.6f}",
                        f"{dollar_risk:.6f}",
                        f"{sl_points:.6f}",
                        f"{tp_points:.6f}",
                        "simulated" if not do_execute else "placed",
                        note
                    ]

                    # safety: max trades per day
                    if trades_today >= MAX_TRADES_PER_DAY:
                        logging.warning("Reached MAX_TRADES_PER_DAY (%s). Skipping further trades.", MAX_TRADES_PER_DAY)
                        continue

                    # execute or simulate
                    if do_execute:
                        ok, res = send_order_mt5(mt5, resolved, side, lot, price, sl_price, tp_price)
                        if ok:
                            logging.info("Order sent %s %s lot=%s (res=%s)", resolved, side, lot, res)
                            row_tpl[11] = "placed"
                            row_tpl[12] = str(res)
                        else:
                            logging.error("Order failed for %s: %s", resolved, res)
                            row_tpl[11] = "error"
                            row_tpl[12] = str(res)
                    else:
                        logging.info("Simulated %s %s lot=%s price=%s", resolved, side, lot, price)

                    # record
                    write_row_csv(row_tpl)
                    trades_today += 1

                    # small throttle
                    time.sleep(1)

                except Exception as e_sym:
                    logging.exception("Symbol loop error for %s: %s", sym, e_sym)
                    continue

            # wait for loop delay (approx M1)
            time.sleep(LOOP_DELAY)
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt -> exiting")
            break
        except Exception as e_outer:
            logging.exception("Main loop exception: %s", e_outer)
            time.sleep(5)

    try:
        mt5.shutdown()
    except Exception:
        pass
    logging.info("Runner finished. Wrote rows to %s", OUT_CSV)


if __name__ == "__main__":
    main()
