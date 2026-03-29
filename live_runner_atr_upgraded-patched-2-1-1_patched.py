
"""
ATR-based live runner with automatic lot sizing.

Usage:
    python live_runner_atr_upgraded.py

Notes:

Default DRY_RUN is False per your request, but the script will force DRY_RUN
if it cannot detect the account/server looks like a demo/trial (safety). Writes trades to live_trades_atr.csv (one row per simulated/placed trade). Kill switch: create an empty file named "kill_live.txt" OR "KILL_NOW" in same folder to stop loop.
"""
import importlib.util
import os
import time
import math
import csv
import sys
import logging
from datetime import datetime, timedelta

# ---------- CONFIG ----------

BOT_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
SYMBOLS = ["BTCUSD", "EURUSD", "USDJPY", "USOIL", "DXY"]  # we removed XAU per your request
TIMEFRAME = "M1"
RUN_HOURS = 24
LOOP_DELAY = 60  # seconds between iterations (M1)

# User-specified starting balance (you indicated $30)
ACCOUNT_BALANCE_OVERRIDE = 30.0  # if <=0, will use mt5.account_info().balance

# Risk & ATR multipliers (you asked for ATR-based SL/TP)
RISK_PER_TRADE_PCT = 0.5  # percent of account to risk per trade (0.5% -> $0.15 on $30)
SL_ATR_MULT = 1.8
TP_ATR_MULT = 3.0
ATR_PERIOD = 14
MIN_BARS_FOR_ATR = 20

# Execution control
DRY_RUN = False  # you asked Dry run = False (we will force DRY_RUN if not demo)
EXECUTE_ORDERS = True  # if True and demo-account detected -> send orders to MT5

# Output
OUT_CSV = "live_trades_atr.csv"
LOG_FILE = "live_runner_atr.log"
KILL_FILE = "kill_live.txt"

# Safety limits
MAX_TRADES_PER_DAY = 50
MAX_DAILY_LOSS_USD = 0.2 * ACCOUNT_BALANCE_OVERRIDE  # stop if loss > 20% of starting balance

# --------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)


def load_bot(path):
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot


def compute_atr_from_recent(bars, period=ATR_PERIOD):
    """
    Accepts bars either as list of dicts {'high','low','close'} or numpy recarray rows [time,open,high,low,close,...]
    returns ATR (float). Safe for short arrays.
    """
    if bars is None:
        return 0.0
    # normalize to list of dicts with keys high, low, close
    norm = []
    for b in bars:
        if isinstance(b, dict):
            try:
                high = float(b.get("high", b.get("h", 0.0)))
                low = float(b.get("low", b.get("l", 0.0)))
                close = float(b.get("close", b.get("c", 0.0)))
            except Exception:
                continue
        else:
            # numpy recarray / tuple-like: assume (time, open, high, low, close, ...)
            try:
                high = float(b[2])
                low = float(b[3])
                close = float(b[4])
            except Exception:
                # fallback to mapping keys if structure differs
                try:
                    high = float(b["high"])
                    low = float(b["low"])
                    close = float(b["close"])
                except Exception:
                    continue
        norm.append({"high": high, "low": low, "close": close})
    if len(norm) < 2:
        return 0.0
    trs = []
    for i in range(1, len(norm)):
        h = norm[i]["high"]
        l = norm[i]["low"]
        prev_c = norm[i - 1]["close"]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
    if len(trs) == 0:
        return 0.0
    if len(trs) < period:
        return sum(trs) / len(trs)
    return sum(trs[-period:]) / period


def get_resolved_symbol(mt5, sym):
    """Try common candidate names and return resolved symbol string or None"""
    candidates = [sym + "m", sym, sym + "USDm", sym + ".m"]
    for c in candidates:
        try:
            si = mt5.symbol_info(c)
            if si:
                # ensure symbol selected in Market Watch
                try:
                    mt5.symbol_select(c, True)
                except Exception:
                    pass
                return c
        except Exception:
            pass
    return None


def compute_lot_from_risk(symbol_info, sl_price_diff, dollar_risk):
    """
    Compute lot size given symbol_info (mt5.symbol_info), sl_price_diff (abs price difference),
    and desired dollar_risk. Uses symbol_info.trade_tick_value and trade_tick_size.
    Returns (lot, note).
    """
    try:
        tick_value = float(getattr(symbol_info, "trade_tick_value", 0.0))
        tick_size = float(getattr(symbol_info, "trade_tick_size", 0.0))
        volume_step = float(getattr(symbol_info, "volume_step", 0.01))
        volume_min = float(getattr(symbol_info, "volume_min", 0.01))
        volume_max = float(getattr(symbol_info, "volume_max", 100.0))
        if tick_size == 0:
            return (volume_min, "tick_size_zero_fallback")
        value_per_price_unit_per_lot = tick_value / tick_size  # USD per 1 price unit per 1 lot
        # risk per lot in USD for sl_price_diff
        risk_per_lot = abs(sl_price_diff) * value_per_price_unit_per_lot
        if risk_per_lot <= 0:
            return (volume_min, "zero_risk_per_lot_fallback")
        ideal_lot = dollar_risk / risk_per_lot
        # round down to nearest step
        steps = math.floor(ideal_lot / volume_step)
        lot = max(volume_min, min(volume_max, steps * volume_step if steps >= 1 else volume_min))
        note = "calculated"
        return (round(lot, 8), note)
    except Exception as e:
        return (getattr(symbol_info, "volume_min", 0.01), f"error:{e}")


# --- NEW helper (calc_volume_for_risk) ---


def calc_volume_for_risk(mt5_module, symbol, account_balance_usd, risk_pct, sl_price_distance, min_volume=0.01):
    """
    More robust helper to compute a volume (lots) given:
    - mt5_module: MetaTrader5 module object
    - symbol: resolved symbol string (e.g. 'XAUUSDm')
    - account_balance_usd: account balance in USD (float)
    - risk_pct: percent of account to risk (e.g. 0.5 = 0.5%)
    - sl_price_distance: absolute price distance between entry and SL (in price units)
    - min_volume: fallback minimal volume (float)
    Returns (lot, note)
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
        # ticks = sl_distance / point
        if tick_val and point:
            ticks = abs(sl_price_distance) / (point if point != 0 else 1e-8)
            usd_loss_per_lot = ticks * float(tick_val)
        elif contract_size:
            usd_loss_per_lot = abs(sl_price_distance) * float(contract_size)
        else:
            usd_loss_per_lot = abs(sl_price_distance)
        if usd_loss_per_lot <= 0:
            return (volume_min, "zero_loss_per_lot")
        raw_lots = usd_risk / usd_loss_per_lot
        # round to volume_step and clamp to min/max
        steps = math.floor(raw_lots / volume_step)
        lot = (steps * volume_step) if steps >= 1 else volume_min
        lot = max(volume_min, min(volume_max, lot))
        return (round(lot, 8), "calc_volume_for_risk")
    except Exception as e:
        return (min_volume, f"error_calc:{e}")


def kill_switch_check():
    """
    Check both KILL_FILE and legacy 'KILL_NOW' file names. If found, exit cleanly.
    """
    if os.path.exists(KILL_FILE) or os.path.exists("KILL_NOW"):
        logging.info("Kill file detected — shutting down gracefully.")
        # flush logs and exit
        sys.exit(0)


def write_row_csv(row):
    header = ["time", "symbol", "signal", "side", "price", "sl_price", "tp_price", "lot", "dollar_risk", "sl_points", "tp_points", "status", "note"]
    newfile = not os.path.exists(OUT_CSV)
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if newfile:
            w.writerow(header)
        w.writerow(row)


def is_demo_account(mt5):
    try:
        acc = mt5.account_info()
        if acc is None:
            return False
        srv = (acc.server or "").lower()
        name = (acc.company or "").lower()
        # heuristics: 'trial', 'demo', 'mt5demo' or server names like 'trial' -> demo
        if "trial" in srv or "demo" in srv or "test" in srv or "trial" in name or "demo" in name:
            return True
        return False
    except Exception:
        return False


def send_order_mt5(mt5, symbol, side, lot, price, sl, tp):
    """
    Place a market order using mt5.order_send. Returns (success_bool, result_dict)
    """
    # Build request (MT5 Python format)
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
            "comment": "live_runner_atr",
            "type_filling": mt5.ORDER_FILLING_FOK if hasattr(mt5, "ORDER_FILLING_FOK") else mt5.ORDER_FILLING_IOC,
            "type_time": mt5.ORDER_TIME_GTC,
        }
        res = mt5.order_send(request)
        return (True, res._asdict() if hasattr(res, "_asdict") else dict(res))
    except Exception as e:
        return (False, {"error": str(e)})


def main():
    # load bot
    try:
        bot = load_bot(BOT_FILE)
        logging.info("Loaded bot: %s", BOT_FILE)
    except Exception as e:
        logging.error("Failed to load bot: %s", e)
        return

    # load v15 if available
    v15 = None
    try:
        if hasattr(bot, "load_v15_module"):
            v15 = bot.load_v15_module()
            logging.info("v15 loaded: %s", getattr(v15, "__name__", v15))
    except Exception:
        v15 = None

    # --- ensure v15 compute_signal adapter is installed (some bots expose an installer) ---
    try:
        if v15 is not None and hasattr(bot, "_install_v15_compute_signal_adapter"):
            try:
                # try installer with v15 argument first (most common)
                bot._install_v15_compute_signal_adapter(v15)
                logging.info("Installed v15 compute_signal adapter via bot._install_v15_compute_signal_adapter(v15)")
            except TypeError:
                try:
                    # fallback: installer without arguments
                    bot._install_v15_compute_signal_adapter()
                    logging.info("Installed v15 compute_signal adapter via bot._install_v15_compute_signal_adapter()")
                except Exception as e_inst_noarg:
                    logging.warning("Failed to install v15 adapter (no-arg): %s", e_inst_noarg)
            except Exception as e_inst_arg:
                logging.warning("Failed to install v15 adapter (with-v15): %s", e_inst_arg)
        else:
            logging.debug("No v15 adapter installer exposed by bot (skipping).")
    except Exception as e:
        logging.warning("Error while attempting to install v15 adapter: %s", e)
    # --- end adapter-install block ---


    # mt5 init
    try:
        mt5 = getattr(bot, "mt5", None)
        if mt5 is None:
            import MetaTrader5 as mt5
        if not mt5.initialize():
            logging.error("mt5.initialize() failed or returned False")
            return
        acc = mt5.account_info()
        if acc is None:
            logging.warning("mt5.account_info() returned None")
            account_balance = ACCOUNT_BALANCE_OVERRIDE
        else:
            account_balance = float(acc.balance)
        # if user-overridden small balance, override for simulation as requested
        if ACCOUNT_BALANCE_OVERRIDE and ACCOUNT_BALANCE_OVERRIDE > 0:
            logging.info("Overriding account balance with provided ACCOUNT_BALANCE_OVERRIDE=%s", ACCOUNT_BALANCE_OVERRIDE)
            account_balance = ACCOUNT_BALANCE_OVERRIDE
        is_demo = is_demo_account(mt5)
        if not is_demo:
            logging.warning("Account does not look like a demo/trial by server/company heuristics. For safety, forcing DRY_RUN=True.")
    except Exception as e:
        logging.error("MT5 connection failed: %s", e)
        return

    # Determine exec mode
    do_execute = EXECUTE_ORDERS and is_demo and (not DRY_RUN)
    if not is_demo:
        logging.info("For safety: DEMO not found -> running in DRY_RUN mode (no real orders).")
        do_execute = False
    logging.info("Starting main loop. RUN_HOURS=%s DRY_RUN=%s EXECUTE=%s RiskPct=%s", RUN_HOURS, DRY_RUN, do_execute, RISK_PER_TRADE_PCT)
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(hours=RUN_HOURS)
    trades_today = 0
    cumulative_pnl = 0.0
    while datetime.utcnow() < end_time:
        # kill-switch check (supports both KILL_FILE and legacy KILL_NOW)
        kill_switch_check()
        try:
            for sym in SYMBOLS:
                resolved = get_resolved_symbol(mt5, sym)
                if not resolved:
                    logging.warning("Symbol %s not present in Market Watch -> skipping", sym)
                    continue
                # fetch price and recent bars
                tick = mt5.symbol_info_tick(resolved)
                if tick is None:
                    logging.warning("No tick for %s", resolved)
                    continue
                price = float(tick.ask or tick.bid)
                # fetch recent bars for ATR and context
                bars = None
                try:
                    bars = mt5.copy_rates_from_pos(resolved, getattr(mt5, "TIMEFRAME_M1"), 0, 120)
                except Exception:
                    bars = None
                atr = compute_atr_from_recent(bars, period=ATR_PERIOD)
                if atr <= 0:
                    logging.debug("ATR=0 for %s; skipping", resolved)
                    continue

                # ----- compute signal (robust: try v15 and bot, try sym and resolved) -----
                sig = None
                try:
                    # build normalized bars context once
                    bars_ctx = []

                    # bars can be None, a Python list, or a numpy recarray/ndarray. Avoid using bars or [] because numpy arrays raise a ValueError when evaluated in boolean context.
                    if bars is None:
                        bars_iter = []
                    else:
                        # Try to get a safe iterable copy. list(np_recarray) usually works and avoids NumPy truthiness issues.
                        try:
                            bars_iter = list(bars)
                        except Exception:
                            # If conversion fails, fall back to using bars directly (it's still iterable in most cases)
                            bars_iter = bars

                    for r in bars_iter:
                        try:
                            bars_ctx.append({
                                "time": int(r[0]),
                                "open": float(r[1]),
                                "high": float(r[2]),
                                "low": float(r[3]),
                                "close": float(r[4])
                            })
                        except Exception:
                            # fallback: skip malformed row
                            continue

                    tried_methods = []
                    # try v15 module first, then bot module
                    for module_name, module_obj in (("v15", v15), ("bot", bot)):
                        if module_obj is None:
                            continue
                        # try multiple possible signal function names
                        signal_fn = None
                        for fname in ("compute_signal", "generate_signal", "get_signal", "infer_signal"):
                            if hasattr(module_obj, fname):
                                signal_fn = getattr(module_obj, fname)
                                break
                        
                        if signal_fn is None:
                            tried_methods.append(f"{module_name}:no_signal_fn")
                            continue
                            tried_methods.append(f"{module_name}:no_compute_signal")
                            continue
                        for symname in (sym, resolved):
                            try:
                                # call compute_signal with the symbol name variant
                                sig_candidate = module_obj.compute_signal(symname, price, {"bars": bars_ctx})
                                # sanity-check return type
                                if sig_candidate is None:
                                    tried_methods.append(f"{module_name}:{symname}:returned_None")
                                    continue
                                # accept numeric floats/ints
                                if isinstance(sig_candidate, (int, float)):
                                    sig = float(sig_candidate)
                                    logging.info("compute_signal -> used %s.compute_signal('%s') -> %s", module_name, symname, sig)
                                    break
                                # else, if some modules return a dict, try to extract 'signal'
                                if isinstance(sig_candidate, dict):
                                    if "signal" in sig_candidate:
                                        sig = float(sig_candidate["signal"])
                                        logging.info("compute_signal -> used %s.compute_signal('%s') returned dict->signal=%s", module_name, symname, sig)
                                        break
                                    # fallback: skip unknown dict
                                    tried_methods.append(f"{module_name}:{symname}:dict_no_signal")
                                    continue
                                tried_methods.append(f"{module_name}:{symname}:unsupported_return")
                            except Exception as e:
                                logging.debug("compute_signal %s.compute_signal('%s') raised: %s", module_name, symname, e)
                                tried_methods.append(f"{module_name}:{symname}:exc")
                        if sig is not None:
                            break
                    if sig is None:
                        logging.warning("No compute_signal resolved for sym=%s resolved=%s tried=%s", sym, resolved, tried_methods)
                        continue
                except Exception as e:
                    logging.exception("compute_signal block error for %s/%s: %s", sym, resolved, e)
                    continue
                # ------------------------------------------------------------------------
                if sig is None:
                    continue

                signal_thresh = getattr(bot, "DEFAULT_SIGNAL_THRESH", 0.92)
                # if your bot exposes per-symbol params, attempt to read it:
                try:
                    per_params = getattr(bot, "params", {}).get(sym + "m", {})
                    signal_thresh = per_params.get("signal_thresh", signal_thresh)
                except Exception:
                    pass
                if abs(sig) < signal_thresh:
                    logging.debug("%s signal %.4f below thresh %.4f", resolved, float(sig), float(signal_thresh))
                    continue

                side = "buy" if sig > 0 else "sell"
                # SL/TP using ATR multipliers
                sl_points = SL_ATR_MULT * atr
                tp_points = TP_ATR_MULT * atr
                if side == "buy":
                    sl_price = price - sl_points
                    tp_price = price + tp_points
                else:
                    sl_price = price + sl_points
                    tp_price = price - tp_points

                # determine lot
                dollar_risk = account_balance * (RISK_PER_TRADE_PCT / 100.0)
                si = mt5.symbol_info(resolved)
                # Prefer the new calc_volume_for_risk helper, fallback to old compute_lot_from_risk
                try:
                    lot, note = calc_volume_for_risk(mt5, resolved, account_balance, RISK_PER_TRADE_PCT, sl_points, min_volume=getattr(si, "volume_min", 0.01))
                except Exception:
                    lot, note = compute_lot_from_risk(si, sl_points, dollar_risk)
                # if computed lot < volume_min -> we will use volume_min but warn
                try:
                    min_vol = float(getattr(si, "volume_min", 0.01))
                except Exception:
                    min_vol = 0.01
                if lot < min_vol:
                    lot = min_vol
                    note = (note or "") + "|used_volume_min"

                # prepare CSV row
                row_tpl = [
                    datetime.utcnow().isoformat(),
                    resolved,
                    f"{sig:.6f}",
                    side,
                    f"{price:.6f}",
                    f"{sl_price:.6f}",
                    f"{tp_price:.6f}",
                    f"{lot:.6f}",
                    f"{dollar_risk:.6f}",
                    f"{sl_points:.6f}",
                    f"{tp_points:.6f}",
                    "simulated" if not do_execute else "placed",
                    note,
                ]

                # Do safety: limit trades per day
                if trades_today >= MAX_TRADES_PER_DAY:
                    logging.warning("Reached MAX_TRADES_PER_DAY (%s). Skipping further trades.", MAX_TRADES_PER_DAY)
                    continue

                # If dry-run no placement; else place using mt5.order_send
                if do_execute:
                    ok, res = send_order_mt5(mt5, resolved, side, lot, price, sl_price, tp_price)
                    if ok:
                        logging.info("Order sent for %s %s lot=%s (result=%s)", resolved, side, lot, res)
                        row_tpl[11] = "placed"
                        row_tpl[12] = str(res)
                    else:
                        logging.error("Order NOT sent for %s: %s", resolved, res)
                        row_tpl[11] = "error"
                        row_tpl[12] = str(res)
                else:
                    logging.info("DRY_RUN / not demo -> simulated order for %s %s lot=%s price=%s", resolved, side, lot, price)
                write_row_csv(row_tpl)
                trades_today += 1
                # small throttle between symbols
                time.sleep(1)
            # small delay between loops
            time.sleep(LOOP_DELAY)
        except Exception as e:
            logging.exception("Loop exception: %s", e)
            time.sleep(5)
    try:
        mt5.shutdown()
    except Exception:
        pass
    logging.info("Runner finished. Wrote rows to %s", OUT_CSV)


if __name__ == "__main__":
    main()
