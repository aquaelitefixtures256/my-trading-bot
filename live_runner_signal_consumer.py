#!/usr/bin/env python3
"""
Signal consumer runner (Step 2) — upgraded to use M30 entry + ATR and H1 confirmation.

Reads signals from signal_publisher_wrapper.signals_queue and executes/simulates them.
Writes rows to live_trades_from_signals.csv.

Usage:
    python live_runner_signal_consumer.py
"""
import importlib.util
import time
import os
import csv
import logging
import sys
from datetime import datetime, timedelta
from queue import Empty

# CONFIG (tweak if needed)
BOT_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
OUT_CSV = "live_trades_from_signals.csv"
LOG_FILE = "live_consumer.log"
KILL_FILE = "kill_live.txt"
RUN_HOURS = 7
LOOP_DELAY = 1.0  # seconds to wait between queue gets if empty
DEFAULT_RISK_PCT = 0.5  # used only if bot does not provide calc helper
DEFAULT_MAX_OPEN = {  # fallback per-symbol open trade limits
    "USDJPY": 10,
    "EURUSD": 10,
    "USOIL": 5,
    "BTCUSD": 5,
    "DXY": 7
}
DRY_RUN = False  # safety default; the script will only place if demo-account & you explicitly flip to False
# Confirmation behavior: if True require simple H1 confirmation (can be changed by bot via attribute)
REQUIRE_H1_CONFIRM = True

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)


def load_bot(path):
    if not os.path.exists(path):
        raise FileNotFoundError("Bot file not found: " + path)
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot


def write_row_csv(row):
    header = ["time", "signal_id", "symbol", "resolved_symbol", "side", "price", "sl_price", "tp_price", "lot", "dollar_risk", "status", "note"]
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
        comp = (acc.company or "").lower()
        if "demo" in srv or "trial" in srv or "demo" in comp or "trial" in comp:
            return True
    except Exception:
        pass
    return False


def resolve_symbol(mt5, bot, sym):
    # prefer resolved_symbol from bot params mapping if provided
    try:
        if hasattr(bot, "resolve_symbol"):
            r = bot.resolve_symbol(sym)
            if r:
                return r
    except Exception:
        pass
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
            pass
    return None


def count_open_positions_for_symbol(mt5, resolved_symbol):
    try:
        pos = mt5.positions_get(symbol=resolved_symbol)
        if pos is None:
            return 0
        return len(pos)
    except Exception:
        try:
            allpos = mt5.positions_get()
            if not allpos:
                return 0
            return sum(1 for p in allpos if getattr(p, "symbol", "") == resolved_symbol)
        except Exception:
            return 0


def send_order_mt5(mt5, symbol, side, lot, price, sl, tp):
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
            "comment": "signal_consumer",
            "type_filling": mt5.ORDER_FILLING_FOK if hasattr(mt5, "ORDER_FILLING_FOK") else mt5.ORDER_FILLING_IOC,
            "type_time": mt5.ORDER_TIME_GTC,
        }
        res = mt5.order_send(request)
        return (True, res._asdict() if hasattr(res, "_asdict") else dict(res))
    except Exception as e:
        return (False, {"error": str(e)})


def compute_volume(mt5, bot, resolved_symbol, account_balance, risk_pct, sl_price_distance):
    """Try to use bot.calc_volume_for_risk or bot.calc_volume or runner fallbacks."""
    try:
        # prefer calc_volume_for_risk if bot exposes it
        if hasattr(bot, "calc_volume_for_risk"):
            return bot.calc_volume_for_risk(mt5, resolved_symbol, account_balance, risk_pct, sl_price_distance)
        if hasattr(bot, "compute_lot_from_risk"):
            return bot.compute_lot_from_risk(mt5.symbol_info(resolved_symbol), sl_price_distance, account_balance * (risk_pct / 100.0))
    except Exception:
        pass
    # fallback local calculation: simple tick-based if available
    try:
        si = mt5.symbol_info(resolved_symbol)
        if si is None:
            return (0.01, "fallback_min")
        tick_val = getattr(si, "trade_tick_value", None)
        point = getattr(si, "point", None) or 1e-8
        if tick_val:
            ticks = abs(sl_price_distance) / point if point else abs(sl_price_distance)
            usd_loss_per_lot = ticks * float(tick_val)
        else:
            contract_size = getattr(si, "trade_contract_size", 1.0) or 1.0
            usd_loss_per_lot = abs(sl_price_distance) * float(contract_size)
        if usd_loss_per_lot <= 0:
            return (getattr(si, "volume_min", 0.01), "fallback_zero_loss")
        usd_risk = account_balance * (risk_pct / 100.0)
        raw = usd_risk / usd_loss_per_lot
        step = getattr(si, "volume_step", 0.01) or 0.01
        steps = int(raw // step)
        lot = (steps * step) if steps >= 1 else getattr(si, "volume_min", 0.01)
        lot = max(getattr(si, "volume_min", 0.01), min(getattr(si, "volume_max", 100.0), lot))
        return (round(lot, 8), "fallback_calc")
    except Exception as e:
        return (0.01, "fallback_exception:" + str(e))


# --- helpers for timeframe-safe ATR + confirmation ---
def fetch_bars_safe(mt5, symbol, timeframe_attr, count):
    """
    Safely fetch bars for the requested timeframe attribute name or fallback.
    timeframe_attr expected to be a string name like 'TIMEFRAME_M30' or 'TIMEFRAME_H1'
    Returns list (possibly numpy recarray). Never raises if mt5 returns None.
    """
    try:
        tf = getattr(mt5, timeframe_attr, None)
        if tf is None:
            # try common fallbacks
            if timeframe_attr == "TIMEFRAME_M30":
                tf = getattr(mt5, "TIMEFRAME_M1", None)
            elif timeframe_attr == "TIMEFRAME_H1":
                tf = getattr(mt5, "TIMEFRAME_M1", None)
            else:
                tf = getattr(mt5, "TIMEFRAME_M1", None)
        bars = None
        if tf is not None:
            bars = mt5.copy_rates_from_pos(symbol, tf, 0, count)
        return bars if bars is not None else []
    except Exception:
        return []


def compute_atr_from_bars(bars, period=14):
    """
    Accept bars as numpy recarray or list of tuples. Return ATR float (safe).
    """
    if bars is None:
        return 0.0
    # require at least 2 bars
    try:
        n = len(bars)
    except Exception:
        return 0.0
    if n < 2:
        return 0.0
    trs = []
    for i in range(1, n):
        try:
            hi = float(bars[i][2]); lo = float(bars[i][3]); prevc = float(bars[i-1][4])
            trs.append(max(hi - lo, abs(hi - prevc), abs(lo - prevc)))
        except Exception:
            # try dict-style
            try:
                hi = float(bars[i].get("high"))
                lo = float(bars[i].get("low"))
                prevc = float(bars[i-1].get("close"))
                trs.append(max(hi - lo, abs(hi - prevc), abs(lo - prevc)))
            except Exception:
                continue
    if not trs:
        return 0.0
    if len(trs) < period:
        return sum(trs) / len(trs)
    return sum(trs[-period:]) / period


def h1_simple_confirmation(bars_h1, side):
    """
    Very small H1 confirmation rule:
      - compute average of last 3 H1 closes and compare last H1 close to that average
      - if side == 'buy' require last_close >= avg * (1 + tolerance)
      - if side == 'sell' require last_close <= avg * (1 - tolerance)
    This is intentionally simple and conservative.
    """
    try:
        if not bars_h1 or len(bars_h1) < 4:
            return True  # not enough data -> treat as confirmed (do not block)
        # use numpy recarray layout: [time,open,high,low,close,...]
        last_close = float(bars_h1[-1][4])
        recent = [float(r[4]) for r in bars_h1[-4:-1]]  # 3 prior closes
        avg_recent = sum(recent) / max(1, len(recent))
        # tolerance small (0.0006 -> 6 pips for 1.0000) but for instruments with large prices it still works
        tolerance = 0.0006 if avg_recent > 50 else 0.00006
        if side == "buy":
            return last_close >= (avg_recent * (1.0 - tolerance))
        else:
            return last_close <= (avg_recent * (1.0 + tolerance))
    except Exception:
        return True


def main():
    # load bot
    try:
        bot = load_bot(BOT_FILE)
        logging.info("Loaded bot: %s", BOT_FILE)
    except Exception as e:
        logging.error("Failed to load bot: %s", e)
        return

    # import signal publisher wrapper (must be in same project)
    try:
        import signal_publisher_wrapper as spw
        q = getattr(spw, "signals_queue", None)
        if q is None:
            logging.error("signal_publisher_wrapper has no signals_queue")
            return
    except Exception as e:
        logging.error("Failed to import signal_publisher_wrapper: %s", e)
        return

    # load mt5 (bot may expose instance)
    try:
        mt5 = getattr(bot, "mt5", None)
        if mt5 is None:
            import MetaTrader5 as mt5  # type: ignore
        if not mt5.initialize():
            logging.error("mt5.initialize() failed or returned False")
            return
        acc = mt5.account_info()
        account_balance = float(acc.balance) if acc else 0.0
        # override with small-test balance if bot provided (safer)
        if hasattr(bot, "ACCOUNT_BALANCE_OVERRIDE") and getattr(bot, "ACCOUNT_BALANCE_OVERRIDE"):
            account_balance = float(getattr(bot, "ACCOUNT_BALANCE_OVERRIDE"))
        # global safety: force DRY_RUN unless demo or caller overrides
        demo_ok = is_demo_account(mt5)
    except Exception as e:
        logging.error("MT5 init error: %s", e)
        return

    # read open-limits from bot if available
    try:
        per_symbol_limits = getattr(bot, "OPEN_LIMITS", None) or getattr(bot, "max_open_per_symbol", None) or getattr(bot, "symbol_open_limits", None) or {}
        if not per_symbol_limits:
            per_symbol_limits = DEFAULT_MAX_OPEN.copy()
    except Exception:
        per_symbol_limits = DEFAULT_MAX_OPEN.copy()

    start_time = datetime.utcnow()
    end_time = start_time + timedelta(hours=RUN_HOURS)
    logging.info("Starting consumer loop. Will run until %s (UTC). DRY_RUN=%s demo_ok=%s", end_time.isoformat(), DRY_RUN, demo_ok)

    while datetime.utcnow() < end_time:
        if os.path.exists(KILL_FILE):
            logging.info("Kill file detected, exiting.")
            break
        try:
            sig = q.get(timeout=LOOP_DELAY)
        except Empty:
            continue
        except Exception as e:
            logging.exception("Queue get error: %s", e)
            time.sleep(1)
            continue

        try:
            sig_id = sig.get("id", None) if isinstance(sig, dict) else None
            base_symbol = sig.get("symbol") if isinstance(sig, dict) else None
            resolved = sig.get("resolved_symbol") if isinstance(sig, dict) else None
            side = sig.get("side") if isinstance(sig, dict) else None
            if not base_symbol or not side:
                logging.warning("Malformed signal, skipping: %s", repr(sig))
                write_row_csv([datetime.utcnow().isoformat(), sig_id, base_symbol, resolved, side, 0, 0, 0, 0, 0, "skipped", "malformed"])
                continue

            # resolve symbol if needed
            if not resolved:
                resolved = resolve_symbol(mt5, bot, base_symbol)
            if not resolved:
                logging.warning("Could not resolve symbol for %s; skipping", base_symbol)
                write_row_csv([datetime.utcnow().isoformat(), sig_id, base_symbol, resolved, side, 0, 0, 0, 0, 0, "skipped", "no_symbol"])
                continue

            # enforce max open trades per symbol
            max_open = per_symbol_limits.get(base_symbol, per_symbol_limits.get(resolved, DEFAULT_MAX_OPEN.get(base_symbol, 5)))
            if max_open is None:
                max_open = 5
            open_count = count_open_positions_for_symbol(mt5, resolved)
            if open_count >= int(max_open):
                logging.info("Max open reached for %s (%s/%s). Skipping signal.", base_symbol, open_count, max_open)
                write_row_csv([datetime.utcnow().isoformat(), sig_id, base_symbol, resolved, side, sig.get("price"), sig.get("sl_price"), sig.get("tp_price"), sig.get("lot"), 0, "skipped", f"max_open({open_count}/{max_open})"])
                continue

            # ENTRY price: prefer signal price; if missing use last M30 close (preferred), fallback to tick
            entry_price = sig.get("price")
            if entry_price is None:
                bars_m30 = fetch_bars_safe(mt5, resolved, "TIMEFRAME_M30", 10)
                if bars_m30 and len(bars_m30) >= 1:
                    try:
                        entry_price = float(bars_m30[-1][4])
                        logging.debug("Using last M30 close as entry for %s -> %s", resolved, entry_price)
                    except Exception:
                        entry_price = None
                if entry_price is None:
                    tick = mt5.symbol_info_tick(resolved)
                    entry_price = float(tick.ask) if side == "buy" else float(tick.bid)

            # Confirmation: check H1 trend if required and if signal didn't already include 'force' or 'confirmed'
            force = sig.get("force", False) or sig.get("confirmed", False)
            require_confirm = getattr(bot, "REQUIRE_H1_CONFIRM", REQUIRE_H1_CONFIRM)
            if require_confirm and not force:
                bars_h1 = fetch_bars_safe(mt5, resolved, "TIMEFRAME_H1", 8)
                ok_h1 = h1_simple_confirmation(bars_h1, side)
                if not ok_h1:
                    logging.info("H1 not confirming signal for %s (%s). skipping.", resolved, side)
                    write_row_csv([datetime.utcnow().isoformat(), sig_id, base_symbol, resolved, side, entry_price, None, None, sig.get("lot"), 0, "skipped", "h1_not_confirm"])
                    continue

            # ensure we have SL/TP; if missing compute using ATR on M30 (preferred) then fallback to M1
            sl_price = sig.get("sl_price")
            tp_price = sig.get("tp_price")
            note_atr = ""
            if sl_price is None or tp_price is None:
                atr = None
                try:
                    bars_for_atr = fetch_bars_safe(mt5, resolved, "TIMEFRAME_M30", 60)
                    if bars_for_atr and len(bars_for_atr) >= 2:
                        atr = compute_atr_from_bars(bars_for_atr, period=14)
                    # if M30 failed, try M1
                    if (atr is None or atr <= 0):
                        bars_m1 = fetch_bars_safe(mt5, resolved, "TIMEFRAME_M1", 60)
                        if bars_m1 and len(bars_m1) >= 2:
                            atr = compute_atr_from_bars(bars_m1, period=14)
                except Exception:
                    atr = None

                if atr is None or atr <= 0:
                    sl_price = sig.get("sl_price", entry_price - 0.5 if side == "buy" else entry_price + 0.5)
                    tp_price = sig.get("tp_price", entry_price + 1.0 if side == "buy" else entry_price - 1.0)
                    note_atr = "atr_missing_fallback"
                else:
                    sl_mult = getattr(bot, "SL_ATR_MULT", 1.8) if hasattr(bot, "SL_ATR_MULT") else 1.8
                    tp_mult = getattr(bot, "TP_ATR_MULT", 3.0) if hasattr(bot, "TP_ATR_MULT") else 3.0
                    sl_points = sl_mult * atr
                    tp_points = tp_mult * atr
                    if side == "buy":
                        sl_price = entry_price - sl_points
                        tp_price = entry_price + tp_points
                    else:
                        sl_price = entry_price + sl_points
                        tp_price = entry_price - tp_points
                    note_atr = f"atr_used_M30({atr:.5g})"

            else:
                note_atr = "sltp_from_signal"

            # determine lot
            lot = sig.get("lot")
            lot_note = ""
            if lot is None:
                try:
                    lot, lot_note = compute_volume(mt5, bot, resolved, account_balance, DEFAULT_RISK_PCT, abs(entry_price - sl_price))
                except Exception as e:
                    lot = 0.01
                    lot_note = "compute_volume_error:" + str(e)

            # ensure lot >= min
            try:
                si = mt5.symbol_info(resolved)
                minv = float(getattr(si, "volume_min", 0.01))
                if float(lot) < minv:
                    lot = minv
                    lot_note = (lot_note or "") + "|min_vol_forced"
            except Exception:
                pass

            # decide whether to place or simulate
            do_place = (not DRY_RUN) and demo_ok and hasattr(mt5, "order_send") and getattr(bot, "ALLOW_SIGNALS_PLACE", True)
            status = "simulated"
            note = f"{note_atr}|{lot_note}"
            if do_place:
                ok, res = send_order_mt5(mt5, resolved, side, lot, entry_price, sl_price, tp_price)
                if ok:
                    status = "placed"
                    note = note + "|order_ok"
                else:
                    status = "error"
                    note = note + "|order_err:" + str(res)
                    logging.error("Order failed for %s: %s", resolved, res)
            else:
                logging.info("Simulated order for %s %s lot=%s price=%s (DRY_RUN=%s demo_ok=%s)", resolved, side, lot, entry_price, DRY_RUN, demo_ok)

            # write CSV row
            write_row_csv([
                datetime.utcnow().isoformat(),
                sig_id,
                base_symbol,
                resolved,
                side,
                "{:.8f}".format(float(entry_price)),
                "{:.8f}".format(float(sl_price)),
                "{:.8f}".format(float(tp_price)),
                "{:.8f}".format(float(lot)),
                "{:.6f}".format(float(account_balance * (DEFAULT_RISK_PCT / 100.0))),
                status,
                note
            ])
        except Exception as e:
            logging.exception("Error handling signal: %s", e)
            write_row_csv([datetime.utcnow().isoformat(), None, None, None, None, 0, 0, 0, 0, 0, "error", str(e)])
            continue

    try:
        mt5.shutdown()
    except Exception:
        pass
    logging.info("Consumer finished.")


if __name__ == "__main__":
    main()
