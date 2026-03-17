#!/usr/bin/env python3
# per_symbol_summary.py
# Summarize replay CSVs in debug_backtest_output/*.csv per symbol

import csv
import glob
import os
from statistics import mean, median, pstdev

REPLAY_GLOB = "debug_backtest_output/replay_trades_*.csv"
OUT_CSV = "per_symbol_summary.csv"

def parse_pnl(x):
    try:
        return float(x)
    except Exception:
        return None

def summarize_file(path):
    pnl_list = []
    wins = 0
    losses = 0
    buys = 0
    sells = 0
    durations = []
    # file format assumed: time,type,entry,exit,pnl,exit_time  (maybe header)
    with open(path, newline='', encoding='utf-8') as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row:
                continue
            if row[0].lower().startswith("time"):
                continue
            # try to defensively parse
            # common formats: time,type,entry,exit,pnl,exit_time
            # or time,type,entry,pnl,exit_time (different)
            try:
                typ = row[1].strip().lower()
            except Exception:
                typ = ""
            # find pnl value in row (search numeric)
            pnl = None
            for candidate in row[2:]:
                try:
                    val = float(candidate)
                    # take first numeric that looks like pnl — heuristic
                    pnl = val
                    break
                except Exception:
                    continue
            if pnl is None:
                # fallback: try last column
                pnl = parse_pnl(row[-2] if len(row) >= 2 else "")
            if pnl is None:
                continue
            pnl_list.append(pnl)
            if pnl > 0:
                wins += 1
            else:
                losses += 1
            if 'buy' in typ:
                buys += 1
            elif 'sell' in typ:
                sells += 1
    if not pnl_list:
        return None
    summary = {
        "file": os.path.basename(path),
        "symbol": os.path.basename(path).replace("replay_trades_","").replace(".csv",""),
        "trades": len(pnl_list),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(pnl_list),
        "net": sum(pnl_list),
        "avg": mean(pnl_list),
        "median": median(pnl_list),
        "std": pstdev(pnl_list) if len(pnl_list) > 1 else 0.0,
        "gross_win": sum(x for x in pnl_list if x>0),
        "gross_loss": sum(x for x in pnl_list if x<0),
        "buys": buys,
        "sells": sells
    }
    return summary

def main():
    files = sorted(glob.glob(REPLAY_GLOB))
    if not files:
        print("No replay files found:", REPLAY_GLOB)
        return 1
    rows = []
    for f in files:
        s = summarize_file(f)
        if s:
            rows.append(s)
            print(f"{s['symbol']}: trades={s['trades']}, net={s['net']:.3f}, win_rate={s['win_rate']:.2%}, gross_win={s['gross_win']:.3f}, gross_loss={s['gross_loss']:.3f}")
    # save CSV
    with open(OUT_CSV, "w", newline='', encoding='utf-8') as of:
        w = csv.writer(of)
        w.writerow(["symbol","trades","wins","losses","win_rate","net","avg","median","std","gross_win","gross_loss","buys","sells"])
        for r in rows:
            w.writerow([r["symbol"], r["trades"], r["wins"], r["losses"], f"{r['win_rate']:.6f}", f"{r['net']:.6f}", f"{r['avg']:.6f}", f"{r['median']:.6f}", f"{r['std']:.6f}", f"{r['gross_win']:.6f}", f"{r['gross_loss']:.6f}", r['buys'], r['sells']])
    print("Wrote", OUT_CSV)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
