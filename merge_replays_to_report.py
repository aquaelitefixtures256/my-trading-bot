#!/usr/bin/env python3
# merge_replays_to_report.py
# Merge replay CSVs (debug_backtest_output/replay_trades_*.csv) into KYOTO_V16_BACKTEST_REPORT.csv
# Normalizes to columns: time,type,entry,exit,pnl,exit_time,atr_at_entry

import csv
import glob
import os
from datetime import datetime

REPLAY_GLOB = "debug_backtest_output/replay_trades_*.csv"
REPORT = "KYOTO_V16_BACKTEST_REPORT.csv"
HEADER = ["time","type","entry","exit","pnl","exit_time","atr_at_entry"]

def ensure_header(path):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        return
    # if header malformed, replace it
    with open(path, "r", encoding="utf-8") as f:
        first = f.readline().strip()
    if first.replace(" ", "") != ",".join(HEADER):
        bak = f"{path}.bak.{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        try:
            os.rename(path, bak)
            print("Backed up existing report to", bak)
        except Exception:
            pass
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)

def normalize_row(row, fname=""):
    # Accept rows with headers or positional columns.
    # Possible input formats:
    # time,type,entry,exit,pnl,exit_time
    # time,type,entry,exit,pnl,exit_time,atr_at_entry
    # or rows of: time,type,entry,exit,pnl,exit_time (no atr)
    # We'll coerce everything into 7 columns.
    # If row is an empty line or short, return None.
    if not row or all((c is None or str(c).strip()=="" ) for c in row):
        return None
    # If dict-like (csv.DictReader used), convert to list
    if isinstance(row, dict):
        time_v = row.get("time") or row.get("time_idx") or ""
        type_v = row.get("type","")
        entry_v = row.get("entry",0)
        exit_v = row.get("exit",0)
        pnl_v = row.get("pnl", row.get("pl", 0))
        exit_time_v = row.get("exit_time","")
        atr_v = row.get("atr_at_entry", row.get("atr", 0))
        return [time_v,type_v,entry_v,exit_v,pnl_v,exit_time_v,atr_v]
    # list-like
    try:
        seq = list(row)
    except Exception:
        return None
    seq = [s for s in seq]
    # Some replay CSVs include header row; check that
    # If first element is "time" then skip (caller should skip headers)
    if str(seq[0]).lower().startswith("time"):
        return None
    # pad to 7
    seq = (seq + [""]*7)[:7]
    # Fix types: keep as-is but ensure numeric strings remain strings; analyzer will parse floats
    return seq

def main():
    ensure_header(REPORT)
    files = sorted(glob.glob(REPLAY_GLOB))
    if not files:
        print("No replay files found:", REPLAY_GLOB)
        return 1
    appended = 0
    with open(REPORT, "a", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        for f in files:
            print("Processing:", f)
            with open(f, "r", encoding="utf-8") as inf:
                rdr = csv.reader(inf)
                for row in rdr:
                    nr = normalize_row(row, f)
                    if nr:
                        writer.writerow(nr)
                        appended += 1
    print(f"Appended {appended} rows to {REPORT}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
