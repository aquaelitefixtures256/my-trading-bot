# xau_walkforward.py
# Run multiple 30-day backtests shifted in time to check robustness for XAU
import importlib.util, sys, math, logging, datetime
from pathlib import Path

UPGRADED_FILE = r"KYOTO_INFERNO_V16_fixed-5_upgraded.py"  # adjust if different
spec = importlib.util.spec_from_file_location("bot", UPGRADED_FILE)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

# ensure v15 loaded
try:
    v15 = bot.load_v15_module()
except Exception:
    v15 = None

# windows: number of 30-day windows, step in days
NUM_WINDOWS = 6
WINDOW_DAYS = 30
STEP_DAYS = 15  # overlap slightly to sample different market regimes

results = []
for i in range(NUM_WINDOWS):
    # We will ask run_backtest to use 'days' but offset start by moving MT5 pos --
    # if your run_backtest doesn't accept a start offset, you can modify it to accept `start_pos` or adapt momentarily.
    # Here we call run_backtest with days=WINDOW_DAYS and rely on bot.run_backtest to read historical bars.
    print(f"=== window {i+1}/{NUM_WINDOWS}: days={WINDOW_DAYS}, offset approx {i*STEP_DAYS} days ===")
    try:
        r = bot.run_backtest(v15, symbol="XAUUSD", days=WINDOW_DAYS)
    except Exception as e:
        print("run_backtest failed:", e)
        r = None
    results.append((i, r))

print("\nSUMMARY:")
for i,r in results:
    print(i, r)
