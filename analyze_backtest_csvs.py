# analyze_backtest_csvs.py
import csv
from pathlib import Path
import math

folder = Path("backtest_results")
files = sorted(folder.glob("backtest_*.csv"))
if not files:
    print("No CSVs found in", folder)
    raise SystemExit(1)

def read_trades(p):
    trades = []
    with p.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
            # try common field names
            try:
                pnl = float(r.get("pnl") or r.get("pnl_net") or 0.0)
            except:
                pnl = 0.0
            trades.append(pnl)
    return trades

for p in files:
    pnl_list = read_trades(p)
    if not pnl_list:
        print(p.name, "-> no trades recorded")
        continue
    gross_win = sum(x for x in pnl_list if x>0)
    gross_loss = -sum(x for x in pnl_list if x<0)
    profit_factor = gross_win / gross_loss if gross_loss>0 else float("inf")
    total = sum(pnl_list)
    trades = len(pnl_list)
    avg = total / trades
    wins = sum(1 for x in pnl_list if x>0)
    win_rate = wins / trades
    # equity curve & max drawdown
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for x in pnl_list:
        eq += x
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd: max_dd = dd
    expectancy = avg  # crude; keeps things simple per trade
    print(f"{p.name}: trades={trades}, net={total:.6f}, gross_win={gross_win:.6f}, gross_loss={gross_loss:.6f}, PF={profit_factor:.3f}, win_rate={win_rate:.3%}, avg_trade={avg:.6f}, max_dd={max_dd:.6f}, expectancy={expectancy:.6f}")
