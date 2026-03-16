#!/usr/bin/env python3 """ Robust 30-day backtest runner (patched for varied bot return shapes) Creates/normalizes rows into KYOTO_V16_BACKTEST_REPORT.csv and writes per-symbol logs into /tmp/backtest_results

Usage: python run_30d_backtests_patched.py

This script intentionally defensive: it will not crash when the bot returns an int, a dict summary, or a list of trade dicts/tuples. It normalizes those into CSV rows with the header: time,type,entry,exit,pnl,exit_time,atr_at_entry

Make sure the bot file exists in the same folder and is named exactly: KYOTO_INFERNO_V16_fixed-5_upgraded.py (or edit BOT_PATH below).

""" from future import annotations import importlib.util import csv import os import sys import time import traceback from typing import Any, Dict, List, Iterable, Optional

---- CONFIG ---- 

BOT_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py" REPORT_PATH = "KYOTO_V16_BACKTEST_REPORT.csv" BACKTEST_DIR = os.path.join("", "tmp", "backtest_results") SYMBOLS = ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL", "DXY"] DAYS = 30 CSV_FIELDS = ["time", "type", "entry", "exit", "pnl", "exit_time", "atr_at_entry"]

os.makedirs(BACKTEST_DIR, exist_ok=True)

---- helpers ---- 

def safe_import_bot(path: str): spec = importlib.util.spec_from_file_location("kyoto_bot", path) if spec is None or spec.loader is None: raise ImportError(f"Cannot load bot at {path}") mod = importlib.util.module_from_spec(spec) spec.loader.exec_module(mod) return mod

def normalize_ret_to_trades_list(ret: Any, start_ts: int) -> List[Dict[str, Any]]: """Normalize various ret shapes into a list of trade dicts with CSV_FIELDS keys. Accepts: - list/tuple of dicts - dict with key 'trades_list' or 'trades' which is a list/tuple - dict describing a single trade (contains entry/exit/pnl) - scalar (int/float/None) -> returns [] """ out: List[Dict[str, Any]] = []

# If bot returns a dict, try to extract trades or treat as single trade if isinstance(ret, dict): candidate = None if "trades_list" in ret: candidate = ret.get("trades_list") elif "trades" in ret: candidate = ret.get("trades") else: candidate = None if isinstance(candidate, (list, tuple)): # list of items; normalize each for it in candidate: row = _make_row_from_candidate(it, start_ts) if row: out.append(row) elif isinstance(candidate, dict): row = _make_row_from_candidate(candidate, start_ts) if row: out.append(row) else: # fallback: maybe ret itself encodes a single trade row = _make_row_from_candidate(ret, start_ts) if row: out.append(row) elif isinstance(ret, (list, tuple)): for it in ret: row = _make_row_from_candidate(it, start_ts) if row: out.append(row) else: # scalar or None -> no trades out = [] return out 

def _make_row_from_candidate(it: Any, start_ts: int) -> Optional[Dict[str, Any]]: """Try to create a CSV row dict from many candidate shapes.""" if it is None: return None # If it's already a dict with keys if isinstance(it, dict): # prefer flattening known keys if any(k in it for k in ("entry", "exit", "pnl")): return { "time": int(it.get("time", start_ts)), "type": str(it.get("type", "")), "entry": it.get("entry", 0), "exit": it.get("exit", 0), "pnl": it.get("pnl", 0), "exit_time": it.get("exit_time", ""), "atr_at_entry": it.get("atr_at_entry", it.get("atr", 0)), } # else try to interpret as generic dict of values -> skip return None

# If it's a sequence (tuple/list) assume positional: (time, type, entry, exit, pnl, exit_time, atr) if isinstance(it, (list, tuple)): seq = list(it) # pad/trim to 7 while len(seq) < 7: seq.append(None) return { "time": int(seq[0]) if seq[0] is not None else start_ts, "type": str(seq[1]) if seq[1] is not None else "", "entry": seq[2] or 0, "exit": seq[3] or 0, "pnl": seq[4] or 0, "exit_time": seq[5] or "", "atr_at_entry": seq[6] or 0, } # scalars, ints etc -> no row return None 

def append_rows_to_report(path: str, rows: Iterable[Dict[str, Any]]): """Append rows to CSV report. Create header if missing. Rows must be dicts with CSV_FIELDS.""" init_header = not os.path.exists(path) # ensure dir exists d = os.path.dirname(path) or "." os.makedirs(d, exist_ok=True) with open(path, "a", newline="", encoding="utf-8") as f: writer = csv.DictWriter(f, fieldnames=CSV_FIELDS) if init_header: writer.writeheader() for r in rows: # sanitize by only keeping known fields and ensuring types clean = {k: r.get(k, "") for k in CSV_FIELDS} writer.writerow(clean)

---- main runner ---- 

def main(): if not os.path.exists(BOT_PATH): print(f"Bot file not found: {BOT_PATH}") sys.exit(1)

bot = safe_import_bot(BOT_PATH) # try to load v15 implementation if provided v15 = None try: if hasattr(bot, "load_v15_module"): v15 = bot.load_v15_module() except Exception: print("Warning: load_v15_module() failed:") traceback.print_exc() print("Starting safe 30-day backtests runner...") for sym in SYMBOLS: log_path = os.path.join(BACKTEST_DIR, f"backtest_{sym}_30d.log") try: # call run_backtest in bot start_ts = int(time.time()) print(f"--- backtest start: {sym}") try: ret = bot.run_backtest(v15, symbol=sym, days=DAYS) except TypeError: # older interfaces may take (v15, symbol, days) differently ret = bot.run_backtest(v15, sym, DAYS) # Normalize returned data into a list of trade rows trades_list = normalize_ret_to_trades_list(ret, start_ts) # Append to master report safely try: append_rows_to_report(REPORT_PATH, trades_list) except Exception as e: print(f"Failed to append rows to report: {e}") # attempt single placeholder row so report isn't left malformed try: append_rows_to_report(REPORT_PATH, [{"time": start_ts, "type": "error", "entry": 0, "exit": 0, "pnl": 0, "exit_time": "", "atr_at_entry": 0}]) except Exception: pass # save per-symbol log (store the return summary safely) with open(log_path, "w", encoding="utf-8") as lf: lf.write("RETURN_VALUE:\n") lf.write(str(ret) + "\n\n") lf.write("LOG_OUTPUT:\n") lf.write(f"Backtest for {sym} returned {len(trades_list)} trades\n") print(f"Saved {log_path}") except Exception as e: # On unexpected error, write traceback to per-symbol log and continue with open(log_path, "w", encoding="utf-8") as lf: lf.write("EXCEPTION:\n") lf.write(traceback.format_exc()) print(f"Backtest for {sym} raised exception. See {log_path}") print("All backtests finished. Check "+BACKTEST_DIR) 

if name == "main": main()

