# analyze_kyoto_report.py
import csv
from pathlib import Path

SRC = Path("KYOTO_V16_BACKTEST_REPORT.csv")

if not SRC.exists():
    print("ERROR: KYOTO_V16_BACKTEST_REPORT.csv not found in current folder.")
    raise SystemExit(1)

gross_win = 0.0
gross_loss = 0.0
trades = 0
wins = 0
skipped = 0

equity = 0.0
peak = 0.0
max_dd = 0.0

buy_count = 0
sell_count = 0

def to_float_safe(s):
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        # try replacing possible comma decimal separators (defensive)
        try:
            return float(s.replace(",", "."))
        except Exception:
            return None

with SRC.open(encoding="utf-8", errors="ignore") as f:
    reader = csv.DictReader(f)
    for r in reader:
        raw_pnl = r.get("pnl", "")
        pnl = to_float_safe(raw_pnl)
        if pnl is None:
            skipped += 1
            continue

        trades += 1
        if pnl > 0:
            gross_win += pnl
            wins += 1
        else:
            gross_loss += abs(pnl)

        # count buy/sell if available
        ttype = (r.get("type") or "").strip().lower()
        if ttype == "buy":
            buy_count += 1
        elif ttype == "sell":
            sell_count += 1

        # update equity curve and drawdown
        equity += pnl
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

# final metrics
profit_factor = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
win_rate = (wins / trades) if trades > 0 else 0.0
avg_trade = (gross_win - gross_loss) / trades if trades > 0 else 0.0
expectancy = avg_trade  # shorthand (avg PnL per trade)

print("=== KYOTO BACKTEST REPORT (aggregated) ===")
print("Source file:", SRC.resolve())
print()
print("Total rows processed:", trades + skipped)
print("Valid trades analysed:", trades)
print("Rows skipped (missing/invalid pnl):", skipped)
print()
print(f"Gross Win: {gross_win:.6f}")
print(f"Gross Loss: {gross_loss:.6f}")
print(f"Profit Factor: {profit_factor:.4f}")
print(f"Win Rate: {win_rate:.4%} ({wins}/{trades})")
print(f"Average PnL per trade: {avg_trade:.6f}")
print(f"Expectancy (per trade): {expectancy:.6f}")
print(f"Max Drawdown: {max_dd:.6f}")
print()
print(f"Buy trades: {buy_count}  Sell trades: {sell_count}")
