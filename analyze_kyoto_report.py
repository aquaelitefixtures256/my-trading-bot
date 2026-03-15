import csv

file = "KYOTO_V16_BACKTEST_REPORT.csv"

gross_win = 0
gross_loss = 0
trades = 0
wins = 0

equity = 0
peak = 0
max_dd = 0

with open(file) as f:
    reader = csv.DictReader(f)

    for r in reader:
        pnl = float(r["pnl"])
        trades += 1

        if pnl > 0:
            gross_win += pnl
            wins += 1
        else:
            gross_loss += abs(pnl)

        equity += pnl
        if equity > peak:
            peak = equity

        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

profit_factor = gross_win / gross_loss if gross_loss > 0 else 0
win_rate = wins / trades if trades > 0 else 0
avg_trade = (gross_win - gross_loss) / trades if trades > 0 else 0

print("Trades:", trades)
print("Gross Win:", gross_win)
print("Gross Loss:", gross_loss)
print("Profit Factor:", profit_factor)
print("Win Rate:", win_rate)
print("Average Trade:", avg_trade)
print("Max Drawdown:", max_dd)
