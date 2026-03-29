#!/usr/bin/env python3
"""
run_30d_no_xau.py
Run 30-day backtests for all symbols *except* XAUUSD, robustly handling many return formats.
Creates per-symbol logs in C:\tmp\backtest_results and appends normalized rows to KYOTO_V16_BACKTEST_REPORT.csv
"""
import os
import sys
import csv
import traceback
import importlib.util
from datetime import datetime

BOT_FILENAME = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
OUT_DIR = os.path.join(os.sep, "tmp", "backtest_results")
REPORT = "KYOTO_V16_BACKTEST_REPORT.csv"
# Exclude XAU on purpose
SYMBOLS = ["BTCUSD", "EURUSD", "USDJPY", "USOIL", "DXY"]
DAYS = 30
REPORT_HEADER = ["time", "type", "entry", "exit", "pnl", "exit_time", "atr_at_entry"]

os.makedirs(OUT_DIR, exist_ok=True)

def load_bot(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Bot file not found: {path}")
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def ensure_header():
    """Ensure report CSV exists with clean header (back up malformed if necessary)."""
    try:
        if not os.path.exists(REPORT):
            with open(REPORT, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(REPORT_HEADER)
            return
        # quick header check
        with open(REPORT, "r", encoding="utf-8") as f:
            first = f.readline().strip()
        expected = ",".join(REPORT_HEADER)
        if first.replace(" ", "") != expected:
            # backup old
            stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            bak = f"{REPORT}.bak.{stamp}"
            try:
                os.replace(REPORT, bak)
                print(f"Backed up old report to {bak}")
            except Exception:
                pass
            with open(REPORT, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(REPORT_HEADER)
    except Exception as e:
        print("ensure_header error:", e)

def coerce_to_trades_list(ret):
    """
    Turn the various possible return values from bot.run_backtest(...) into a list of trade-like dicts.
    - If ret is dict: prefer 'trades_list' or 'trades'; if single-trade encoded at top-level, convert to list.
    - If ret is list: return it (assume it's trade rows).
    - If ret is scalar/None: return empty list.
    """
    if ret is None:
        return []
    if isinstance(ret, list):
        return ret
    if isinstance(ret, dict):
        # try common keys
        t = ret.get("trades_list") or ret.get("trades")
        if isinstance(t, list):
            return t
        # single-trade pattern
        if all(k in ret for k in ("entry", "exit", "pnl")):
            return [{
                "time": ret.get("time", datetime.utcnow().isoformat()),
                "type": ret.get("type", ""),
                "entry": ret.get("entry", 0),
                "exit": ret.get("exit", 0),
                "pnl": ret.get("pnl", 0),
                "exit_time": ret.get("exit_time", ""),
                "atr_at_entry": ret.get("atr_at_entry", ret.get("atr", 0))
            }]
        # maybe ret itself is a dict-per-trade container with numeric keys? not expected -> return empty
        return []
    # scalar (int/float/str/other) -> no trades
    return []

def append_rows(trades):
    """
    Append normalized rows to the REPORT CSV. Accepts:
    - trades: iterable of dicts or seqs
    - single dict or single seq will be converted into a one-item list before writing.
    Always write columns matching REPORT_HEADER.
    """
    if trades is None:
        trades = []
    # coerce single dict/seq into list
    if isinstance(trades, dict):
        trades = [trades]
    else:
        # if it's a scalar (e.g., '0' or 0), guard
        if not hasattr(trades, "__iter__") or isinstance(trades, (str, bytes)):
            trades = [trades]

    with open(REPORT, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in trades:
            # dict-like
            if isinstance(row, dict):
                time_v = row.get("time", datetime.utcnow().isoformat())
                type_v = row.get("type", "")
                entry_v = row.get("entry", 0)
                exit_v = row.get("exit", 0)
                pnl_v = row.get("pnl", 0)
                exit_time_v = row.get("exit_time", "")
                atr_v = row.get("atr_at_entry", row.get("atr", 0))
                writer.writerow([time_v, type_v, entry_v, exit_v, pnl_v, exit_time_v, atr_v])
            else:
                # sequence-like: try to convert to list of values and pad/truncate
                try:
                    seq = list(row)
                    seq = (seq + [""]*7)[:7]
                    writer.writerow(seq)
                except Exception:
                    # fallback placeholder
                    writer.writerow([datetime.utcnow().isoformat(), "error", 0, 0, 0, "", 0])

def main():
    ensure_header()
    try:
        bot = load_bot(BOT_FILENAME)
    except Exception as e:
        print("Failed to load bot:", e)
        traceback.print_exc()
        return 2

    # optional v15 loader
    try:
        v15 = bot.load_v15_module() if hasattr(bot, "load_v15_module") else None
    except Exception:
        v15 = None

    for sym in SYMBOLS:
        log_path = os.path.join(OUT_DIR, f"backtest_{sym}_30d.log")
        print(f"Running {sym}...")
        try:
            # call the bot with safe keyword args (prefer v15 if available)
            if v15 is not None:
                ret = bot.run_backtest(v15, symbol=sym, days=DAYS)
            else:
                ret = bot.run_backtest(symbol=sym, days=DAYS)
        except Exception:
            # log the exception and append an error placeholder row
            tb = traceback.format_exc()
            with open(log_path, "w", encoding="utf-8") as lf:
                lf.write("Exception running run_backtest:\n")
                lf.write(tb)
            print(f"Backtest for {sym} failed; see {log_path}")
            append_rows([{"time": datetime.utcnow().isoformat(), "type": "error", "entry":0, "exit":0, "pnl":0, "exit_time":"", "atr_at_entry":0}])
            continue

        # Write per-symbol log
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write("Backtest returned:\n")
            lf.write(repr(ret) + "\n")

        # Normalize ret -> trades_list
        trades_list = coerce_to_trades_list(ret)

        if trades_list:
            append_rows(trades_list)
        else:
            # no trades found -> append a 'none' placeholder row so analyzers have at least one row
            append_rows([{"time": datetime.utcnow().isoformat(), "type":"none", "entry":0, "exit":0, "pnl":0, "exit_time":"", "atr_at_entry":0}])

        print(f"Saved {log_path}")

    print("All backtests finished. Check", OUT_DIR)
    return 0

if __name__ == "__main__":
    sys.exit(main())
