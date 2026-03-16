#!/usr/bin/env python3
"""
Safe runner for 30-day backtests.
Saves per-symbol logs to \tmp\backtest_results and appends normalized rows to KYOTO_V16_BACKTEST_REPORT.csv.

Usage:
    python run_30d_backtests.py
    python run_30d_backtests.py BTCUSD EURUSD
"""

import os
import sys
import csv
import traceback
import importlib.util
from datetime import datetime

# ---------- Configuration ----------
BOT_FILENAME = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"  # adjust if your bot file has a different name
REPORT_PATH = "KYOTO_V16_BACKTEST_REPORT.csv"
BACKTEST_DIR = os.path.join(os.sep, "tmp", "backtest_results")  # results path used in your logs
DEFAULT_SYMBOLS = ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL", "DXY"]
DAYS = 30
REPORT_HEADER = ["time", "type", "entry", "exit", "pnl", "exit_time", "atr_at_entry"]
# -----------------------------------

def ensure_report_header(path=REPORT_PATH, header=REPORT_HEADER):
    """
    Ensure the CSV exists and has a clean header. If the file is corrupted, back it up and rewrite.
    """
    try:
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)
            return
        with open(path, "r", encoding="utf-8") as f:
            first = f.readline().strip()
        # normalize whitespace for comparison
        if not first or first.replace(" ", "") != ",".join(header):
            # backup old
            try:
                stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                backup = f"{path}.bak.{stamp}"
                os.rename(path, backup)
                print(f"Backed up existing report to {backup}")
            except Exception:
                # fallback: try copying or ignoring
                pass
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)
    except Exception as e:
        print("ensure_report_header failed:", e)

def load_bot_module(bot_filename):
    """
    Dynamically import the bot module by path. Returns module object.
    """
    if not os.path.exists(bot_filename):
        raise FileNotFoundError(f"Bot file not found: {bot_filename}")
    spec = importlib.util.spec_from_file_location("kyoto_bot", bot_filename)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

def safe_mkdir(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def normalize_and_append_rows(report_path, trades_list, start_ts):
    """
    trades_list: list of dicts or sequences
    Append normalized rows to report_path. Guarantees columns match REPORT_HEADER.
    """
    try:
        with open(report_path, "a", newline="", encoding="utf-8") as rf:
            writer = csv.writer(rf)
            for row in trades_list:
                if isinstance(row, dict):
                    time_v = row.get("time", start_ts)
                    type_v = row.get("type", "")
                    entry_v = row.get("entry", 0)
                    exit_v = row.get("exit", 0)
                    pnl_v = row.get("pnl", 0)
                    exit_time_v = row.get("exit_time", "")
                    atr_v = row.get("atr_at_entry", row.get("atr", 0))
                    writer.writerow([time_v, type_v, entry_v, exit_v, pnl_v, exit_time_v, atr_v])
                else:
                    # sequence-like: pad or trim to 7 values
                    try:
                        seq = list(row)
                        seq = (seq + [0]*7)[:7]
                        writer.writerow(seq)
                    except Exception:
                        writer.writerow([start_ts, "error", 0, 0, 0, 0, 0])
    except Exception as e:
        print("Failed to append rows to report:", e)

def main(argv):
    print("Starting safe 30-day backtests runner...")
    symbols = argv[1:] if len(argv) > 1 else DEFAULT_SYMBOLS

    safe_mkdir(BACKTEST_DIR)
    ensure_report_header(REPORT_PATH, REPORT_HEADER)

    # load bot module
    try:
        bot = load_bot_module(BOT_FILENAME)
        print(f"Loaded bot module from {BOT_FILENAME}")
    except Exception as e:
        print("Failed to load bot module:", e)
        traceback.print_exc()
        return 2

    # try to obtain v15 adapter if the bot exposes a loader
    v15 = None
    try:
        if hasattr(bot, "load_v15_module") and callable(bot.load_v15_module):
            v15 = bot.load_v15_module()
            print("v15 loaded:", getattr(v15, "__name__", v15))
        else:
            # Some versions call it load_v15 or return an object; try other names
            loader = getattr(bot, "load_v15", None) or getattr(bot, "load_v15_impl", None)
            if callable(loader):
                v15 = loader()
                print("v15 loaded (via alt loader):", getattr(v15, "__name__", v15))
            else:
                print("No v15 loader found in bot module; continuing with v15=None")
    except Exception as e:
        print("Exception while loading v15:", e)
        v15 = None

    for sym in symbols:
        log_path = os.path.join(BACKTEST_DIR, f"backtest_{sym}_30d.log")
        try:
            print(f"--- backtest start: {sym} ---")
            start_ts = datetime.utcnow().isoformat()

            try:
                # core call - many bot versions accept (v15, symbol=..., days=...)
                # we'll try common signatures
                ret = None
                if hasattr(bot, "run_backtest"):
                    try:
                        # prefer signature: run_backtest(v15, symbol=..., days=...)
                        ret = bot.run_backtest(v15, symbol=sym, days=DAYS)
                    except TypeError:
                        # fallback: run_backtest(symbol, days)
                        try:
                            ret = bot.run_backtest(symbol=sym, days=DAYS)
                        except TypeError:
                            # fallback: run_backtest(sym, DAYS)
                            try:
                                ret = bot.run_backtest(sym, DAYS)
                            except Exception as e:
                                raise
                else:
                    raise AttributeError("Bot module has no run_backtest function")
            except Exception as e_run:
                tb = traceback.format_exc()
                with open(log_path, "w", encoding="utf-8") as lf:
                    lf.write(f"Exception running run_backtest for {sym}:\n")
                    lf.write(tb)
                print(f"Backtest for {sym} raised exception. See {log_path}")
                # append placeholder to master CSV so analyzers don't fail
                normalize_and_append_rows(REPORT_PATH, [{"time": start_ts, "type": "error", "entry": 0, "exit": 0, "pnl": 0, "exit_time": "", "atr_at_entry": 0}], start_ts)
                continue

            with open(log_path, "w", encoding="utf-8") as lf:
                lf.write(f"Backtest returned: {repr(ret)}\n")

            # ret may be dict with 'trades_list' or legacy structure
            trades_list = []
            if isinstance(ret, dict):
                trades_list = ret.get("trades_list", []) or ret.get("trades", []) or []
            elif isinstance(ret, list):
                trades_list = ret
            else:
                trades_list = []

            # attempt to coerce single-row return into trades_list
            if not trades_list and isinstance(ret, dict):
                if "entry" in ret and "exit" in ret and "pnl" in ret:
                    trades_list = [{
                        "time": ret.get("time", start_ts),
                        "type": ret.get("type", ""),
                        "entry": ret.get("entry", 0),
                        "exit": ret.get("exit", 0),
                        "pnl": ret.get("pnl", 0),
                        "exit_time": ret.get("exit_time", ""),
                        "atr_at_entry": ret.get("atr_at_entry", ret.get("atr", 0))
                    }]

            if trades_list:
                normalize_and_append_rows(REPORT_PATH, trades_list, start_ts)
            else:
                # no trades recorded, append a 'none' placeholder row for observability
                normalize_and_append_rows(REPORT_PATH, [{"time": start_ts, "type": "none", "entry": 0, "exit": 0, "pnl": 0, "exit_time": "", "atr_at_entry": 0}], start_ts)

            print(f"Saved {log_path}")
        except Exception as outer:
            tb = traceback.format_exc()
            print(f"Outer exception for {sym}: {outer}")
            try:
                with open(log_path, "a", encoding="utf-8") as lf:
                    lf.write("Outer exception:\n")
                    lf.write(tb)
            except Exception:
                pass

    print("All backtests finished. Check", BACKTEST_DIR)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
