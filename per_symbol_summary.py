#!/usr/bin/env python3
# per_symbol_summary_fixed.py
# Robust per-symbol summary of replay CSVs in debug_backtest_output/

import csv, glob, os
from statistics import mean, median, pstdev

REPLAY_GLOB = "debug_backtest_output/replay_trades_*.csv"
OUT_CSV = "per_symbol_summary.csv"
POSSIBLE_PNL_NAMES = {"pnl","pl","profit","p&l","pnl_usd","pnl_usd".lower()}

def try_float(s):
    try:
        return float(str(s).strip())
    except Exception:
        return None

def find_pnl_index(header):
    # header: list of column names
    if not header:
        return None
    # normalize header names
    hnorm = [c.strip().lower() for c in header]
    # direct matches
    for i,hn in enumerate(hnorm):
        if hn in POSSIBLE_PNL_NAMES or "pnl" == hn or hn.endswith("pnl") or "profit" in hn or "p&l" in hn:
            return i
    # common fallback: if header length >=5, assume pnl at index 4
    if len(header) >= 5:
        return 4
    return None

def summarize_file(path):
    pnls = []
    wins = losses = buys = sells = 0
    rows_read = 0
    header = None
    pnl_idx = None
    with open(path, newline='', encoding='utf-8') as f:
        rdr = csv.reader(f)
        # try to detect header from first row
        try:
            first = next(rdr)
        except StopIteration:
            return None
        rows_read += 1
        # if first row contains non-numeric in most columns, treat as header
        numeric_count = sum(1 for c in first if try_float(c) is not None)
        if numeric_count < max(1, len(first)//2):
            header = first
            pnl_idx = find_pnl_index(header)
        else:
            # no header; assume standard layout time,type,entry,exit,pnl,exit_time
            # treat the first row as data: reset iterator to include it
            pnl_idx = 4 if len(first) >= 5 else None
            # process first row as data
            # we will re-use logic below, so put it into a list to process
            data_rows = [first] + list(rdr)
            rows_read += sum(1 for _ in rdr)  # but we already consumed rdr into list; adjust below
            # easier: re-open file and stream with known pnl_idx
            f.seek(0)
            rdr = csv.reader(f)
        # process rows
        for row in rdr:
            if not row or all(not c.strip() for c in row):
                continue
            # skip header if header present
            if header is not None and row is header:
                continue
            rows_read += 1
            # find pnl value
            pnl = None
            if pnl_idx is not None and pnl_idx < len(row):
                pnl = try_float(row[pnl_idx])
            if pnl is None:
                # try common column names by searching row for numeric that looks like pnl:
                # prefer value that looks 'small' relative to price (heuristic) — but fallback to last numeric
                numeric_cols = [(i, try_float(c)) for i,c in enumerate(row) if try_float(c) is not None]
                if numeric_cols:
                    # prefer last numeric (often pnl placed near end)
                    pnl = numeric_cols[-1][1]
            if pnl is None:
                # cannot parse pnl for this row
                # skip but warn
                # print(f"WARNING: couldn't find pnl in {os.path.basename(path)} row: {row}")
                continue
            pnls.append(pnl)
            if pnl > 0: wins += 1
            else: losses += 1
            # type detection
            typ = row[1].strip().lower() if len(row) > 1 else ""
            if "buy" in typ: buys += 1
            elif "sell" in typ: sells += 1
    if not pnls:
        return None
    summary = {
        "file": os.path.basename(path),
        "symbol": os.path.basename(path).replace("replay_trades_","").replace(".csv",""),
        "trades": len(pnls),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(pnls),
        "net": sum(pnls),
        "avg": mean(pnls),
        "median": median(pnls),
        "std": pstdev(pnls) if len(pnls) > 1 else 0.0,
        "gross_win": sum(x for x in pnls if x>0),
        "gross_loss": sum(x for x in pnls if x<0),
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
        else:
            print("No PnL parsed for", f)
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
