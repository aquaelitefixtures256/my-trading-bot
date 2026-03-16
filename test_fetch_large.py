# test_fetch_large.py
# Self-contained diagnostic: fetch many M1 bars for XAUUSDm using a robust chunked helper.
import time
import MetaTrader5 as mt5
import pprint

def fetch_m1_bars_mt5(mt5_module, symbol, minutes_needed, chunk_size=10000, max_retries=4, wait_base=0.5):
    """
    Fetch up to `minutes_needed` M1 bars for `symbol` from MT5 in safe chunks.
    Returns list of bars ordered oldest -> newest (empty list if none).
    """
    try:
        mt5_module.symbol_select(symbol, True)
    except Exception:
        pass

    # quick probe
    try:
        probe = mt5_module.copy_rates_from_pos(symbol, mt5_module.TIMEFRAME_M1, 0, 1)
        if probe is None:
            time.sleep(0.6)
            probe = mt5_module.copy_rates_from_pos(symbol, mt5_module.TIMEFRAME_M1, 0, 1)
        if probe is None:
            return []
    except Exception:
        return []

    bars = []
    remaining = int(minutes_needed or 0)
    fetched_total = 0
    # request in chunks starting at increasing pos
    while remaining > 0:
        to_request = min(chunk_size, remaining)
        success = False
        for attempt in range(max_retries):
            try:
                rates = mt5_module.copy_rates_from_pos(symbol, mt5_module.TIMEFRAME_M1, fetched_total, to_request)
            except Exception:
                rates = None
            # <-- robust check to avoid "ambiguous truth value" for numpy arrays
            if rates is not None and len(rates) > 0:
                bars.extend(rates)
                got = len(rates)
                fetched_total += got
                remaining -= got
                success = True
                break
            else:
                time.sleep(wait_base * (2 ** attempt))
        if not success:
            # if large chunk is failing, reduce chunk and retry once
            if chunk_size > 2000:
                chunk_size = max(2000, chunk_size // 2)
                continue
            else:
                break

    # ensure ascending time order
    try:
        bars = sorted(bars, key=lambda r: int(r[0]))
    except Exception:
        pass
    if minutes_needed and len(bars) > minutes_needed:
        bars = bars[-minutes_needed:]
    return bars

def main():
    print("Initializing MT5...")
    ok = mt5.initialize()
    print("mt5.initialize() ->", ok, " last_error:", mt5.last_error())
    sym = "XAUUSDm"
    print("Selecting symbol:", sym, "->", mt5.symbol_select(sym, True))
    # adjust minutes_needed and chunk_size as you like; big numbers may take time
    minutes_needed = 100000   # ~69 days of 1-min bars (adjust if you want less)
    print(f"Requesting up to {minutes_needed} M1 bars (this may take a while)...")
    bars = fetch_m1_bars_mt5(mt5, sym, minutes_needed=minutes_needed, chunk_size=20000)
    print("Fetched bars count:", len(bars))
    if bars:
        print("Sample (first 2):")
        pprint.pprint(bars[:2])
        print("Sample (last 2):")
        pprint.pprint(bars[-2:])
    else:
        print("No bars returned. If 0, try: Market Watch -> Show All, open a 1m chart for XAUUSDm and scroll back, then rerun.")
    mt5.shutdown()
    print("MT5 shutdown.")

if __name__ == "__main__":
    main()
