# test_fetch_large.py
import MetaTrader5 as mt5
import pprint, time
# paste the helper here if you didn't put it in the bot file
from KYOTO_INFERNO_V16_fixed_5_upgraded import fetch_m1_bars_mt5  # if you pasted helper into bot and its filename is exact

mt5.initialize()
sym = "XAUUSDm"
print("symbol_select:", mt5.symbol_select(sym, True))
print("Requesting up to 100000 M1 bars (this may take 10-30s)...")
bars = fetch_m1_bars_mt5(mt5, sym, minutes_needed=100000, chunk_size=20000)
print("Fetched bars count:", len(bars))
if bars:
    print("Sample first:", bars[:2])
    print("Sample last:", bars[-2:])
mt5.shutdown()
