# xau_diagnostic.py
import csv, math, sys, statistics, pathlib

CSV_CANDIDATES = [
    "debug_backtest_output/replay_trades_XAUUSDm.csv",
    "debug_backtest_output/replay_trades_XAUUSD.csv",
    "debug_backtest_output/replay_trades_XAUUSD_m.csv",
    "replay_trades_XAUUSDm.csv",
    "replay_trades_XAUUSD.csv"
]

def find_csv():
    for p in CSV_CANDIDATES:
        pth = pathlib.Path(p)
        if pth.exists():
            return pth
    return None

p = find_csv()
if not p:
    print("ERROR: cannot find replay_trades_XAU CSV. Looked for:", CSV_CANDIDATES)
    sys.exit(1)

rows = []
with p.open('r', encoding='utf-8', errors='ignore') as f:
    # Try to detect delimiter and header
    first = f.readline()
    # rewind
    f.seek(0)
    csvr = csv.reader(f)
    header = next(csvr)
    header = [h.strip().lower().replace(" ", "_") for h in header]
    # normalize common names
    # allowed fields: time, type, entry, exit, pnl, exit_time, exit_reason
    for r in csvr:
        if not any(cell.strip() for cell in r):
            continue
        # pad if short
        while len(r) < len(header):
            r.append('')
        rec = dict(zip(header, r))
        rows.append(rec)

# Attempt to repair/interpret rows that may have misaligned columns
parsed = []
for rr in rows:
    # keep default values
    try:
        time_idx = None
        if 'time' in rr and rr['time'].strip() != '':
            time_idx = int(float(rr['time']))
    except Exception:
        time_idx = None
    typ = (rr.get('type') or rr.get('side') or '').strip().lower()
    # try entry and exit parsing from multiple possible keys
    def parse_price(keys):
        for k in keys:
            v = rr.get(k, '').strip()
            if v == '':
                continue
            try:
                return float(v)
            except Exception:
                # maybe field accidentally contains two numbers separated by comma; try to split
                parts = v.replace('"','').split(',')
                for p in parts:
                    try:
                        return float(p)
                    except:
                        pass
        return None
    entry = parse_price(['entry','price','open','entry_price'])
    exit_p = parse_price(['exit','exit_price','close','exitprice','exit_price'])
    pnl = None
    # try parse pnl if present
    try:
        pnl = float(rr.get('pnl','').strip()) if rr.get('pnl','').strip()!='' else None
    except Exception:
        pnl = None
    # if pnl missing but entry+exit present compute
    if pnl is None and entry is not None and exit_p is not None:
        if typ == 'sell':
            pnl = entry - exit_p
        else:
            pnl = exit_p - entry
    # exit_time
    exit_time = None
    try:
        exit_time = int(float(rr.get('exit_time', rr.get('exit time','') or rr.get('time','')).strip())) if rr.get('exit_time', '') else None
    except Exception:
        exit_time = None

    parsed.append({
        "time_idx": time_idx,
        "type": typ,
        "entry": entry,
        "exit": exit_p,
        "pnl": pnl,
        "exit_time": exit_time,
        "raw": rr
    })

# Filter invalid
valid = [r for r in parsed if (r['entry'] is not None and r['exit'] is not None and r['pnl'] is not None)]
invalid = [r for r in parsed if r not in valid]

print("CSV loaded:", p)
print("Total rows read:", len(parsed))
print("Valid trades:", len(valid), "Invalid/skipped rows:", len(invalid))
if len(invalid)>0:
    print("Sample invalid row raw data (1):", invalid[0]['raw'])

# Stats
pnls = [r['pnl'] for r in valid]
if pnls:
    total = sum(pnls)
    wins = [x for x in pnls if x>0]
    losses = [x for x in pnls if x<=0]
    print("\nPnL summary:")
    print(" Total PnL      : {:.6f}".format(total))
    print(" Trades         : {}".format(len(pnls)))
    print(" Wins           : {}  (avg {:.6f})".format(len(wins), (sum(wins)/len(wins) if wins else 0.0)))
    print(" Losses         : {}  (avg {:.6f})".format(len(losses), (sum(losses)/len(losses) if losses else 0.0)))
    print(" Win rate       : {:.2%}".format(len(wins)/len(pnls)))
    print(" Avg PnL/trade  : {:.6f}".format(total/len(pnls)))
    print(" Median PnL     : {:.6f}".format(statistics.median(pnls)))
    print(" Std dev PnL    : {:.6f}".format(statistics.pstdev(pnls)))
    print(" Largest wins   :")
    for x in sorted(wins, reverse=True)[:10]:
        print("  +{:.6f}".format(x))
    print(" Largest losses :")
    for x in sorted(losses)[:10]:
        print("  {:.6f}".format(x))
else:
    print("No valid PnL found in CSV to analyze.")

# Top 10 trades by abs(pnl)
srt = sorted(valid, key=lambda r: abs(r['pnl']), reverse=True)[:20]
print("\nTop trades by abs(PnL):")
for r in srt:
    print(" time_idx:", r['time_idx'], "type:", r['type'], "entry:", r['entry'], "exit:", r['exit'], "pnl:", r['pnl'], "exit_time:", r['exit_time'])

# Duration analysis
durations = []
for r in valid:
    if r['time_idx'] is not None and r['exit_time'] is not None:
        durations.append(r['exit_time'] - r['time_idx'])
if durations:
    print("\nDuration stats (bars): count {}, mean {:.2f}, p50 {:.1f}, p90 {:.1f}, max {}".format(len(durations), statistics.mean(durations), statistics.median(durations), sorted(durations)[int(len(durations)*0.9)], max(durations)))
else:
    print("\nNo duration info available.")

print("\nDiagnosis complete.")
