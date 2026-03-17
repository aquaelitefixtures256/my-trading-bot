#!/usr/bin/env python3
# run_30d_no_xau.py  -> run 30-day backtests but exclude XAUUSD
import os, sys, csv, importlib.util, traceback
from datetime import datetime

BOT_FILENAME = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
OUT_DIR = os.path.join(os.sep, "tmp", "backtest_results")
REPORT = "KYOTO_V16_BACKTEST_REPORT.csv"
SYMBOLS = ["BTCUSD", "EURUSD", "USDJPY", "USOIL", "DXY"]  # XAU removed
DAYS = 30
REPORT_HEADER = ["time", "type", "entry", "exit", "pnl", "exit_time", "atr_at_entry"]

os.makedirs(OUT_DIR, exist_ok=True)

def load_bot(path):
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def ensure_header():
    if not os.path.exists(REPORT):
        with open(REPORT, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(REPORT_HEADER)

def append_rows(rows):
    with open(REPORT, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            if isinstance(r, dict):
                w.writerow([r.get("time",""), r.get("type",""), r.get("entry",0), r.get("exit",0), r.get("pnl",0), r.get("exit_time",""), r.get("atr_at_entry",0)])
            else:
                try:
                    seq = list(r)
                    seq = (seq + [0]*7)[:7]
                    w.writerow(seq)
                except Exception:
                    w.writerow([datetime.utcnow().isoformat(),"error",0,0,0,"",0])

def main():
    ensure_header()
    try:
        bot = load_bot(BOT_FILENAME)
    except Exception as e:
        print("Failed to load bot:", e)
        return 2
    # try to load v15
    try:
        v15 = bot.load_v15_module() if hasattr(bot, "load_v15_module") else None
    except Exception:
        v15 = None

    for sym in SYMBOLS:
        logp = os.path.join(OUT_DIR, f"backtest_{sym}_30d.log")
        try:
            print(f"Running {sym}...")
            if v15 is not None:
                ret = bot.run_backtest(v15, symbol=sym, days=DAYS)
            else:
                ret = bot.run_backtest(symbol=sym, days=DAYS)
        except Exception:
            with open(logp, "w", encoding="utf-8") as lf:
                lf.write(traceback.format_exc())
            print(f"Backtest for {sym} failed; wrote {logp}")
            append_rows([{"time": datetime.utcnow().isoformat(), "type": "error", "entry":0,"exit":0,"pnl":0,"exit_time":"","atr_at_entry":0}])
            continue

        with open(logp, "w", encoding="utf-8") as lf:
            lf.write("Returned:\n")
            lf.write(repr(ret) + "\n")
        # normalize returned trades_list if present
        trades_list = []
        if isinstance(ret, dict):
            trades_list = ret.get("trades_list", []) or ret.get("trades", []) or []
            if not trades_list and all(k in ret for k in ("entry","exit","pnl")):
                trades_list = [{"time": ret.get("time", datetime.utcnow().isoformat()), "type": ret.get("type",""), "entry":ret["entry"], "exit":ret["exit"], "pnl": ret["pnl"], "exit_time": ret.get("exit_time",""), "atr_at_entry": ret.get("atr_at_entry", ret.get("atr", 0))}]
        elif isinstance(ret, list):
            trades_list = ret
        if trades_list:
            append_rows(trades_list)
        else:
            append_rows([{"time": datetime.utcnow().isoformat(), "type":"none","entry":0,"exit":0,"pnl":0,"exit_time":"","atr_at_entry":0}])
        print(f"Saved {logp}")
    print("Done. See", OUT_DIR)
    return 0

if __name__ == "__main__":
    sys.exit(main())
