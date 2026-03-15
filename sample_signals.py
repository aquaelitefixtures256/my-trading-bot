# sample_signals_fix.py
# Robust version: fetches recent MT5 bars in chunks and computes adapter signals historically.
import math, sys, time
from pathlib import Path
try:
    import MetaTrader5 as mt5
except Exception as e:
    print("ERROR: MetaTrader5 not importable:", e)
    sys.exit(1)

# USER-TUNEABLE
SYMBOLS = ["BTCUSD","EURUSD","USDJPY","XAUUSD","USOIL","DXY"]
TF = mt5.TIMEFRAME_M5
DAYS = 30
# target bars = DAYS * (24*60 / 5)
TARGET_BARS = DAYS * 24 * 12  # 30 days -> 8640
# Fetch in chunks of at most this many bars per MT5 call (safe default)
CHUNK = 10000

def resolve(sym):
    try:
        if mt5.symbol_select(sym, True):
            return sym
    except Exception:
        pass
    try:
        if mt5.symbol_select(sym + "m", True):
            return sym + "m"
    except Exception:
        pass
    return None

def fetch_recent_bars(sym, tf, total_needed):
    bars = []
    pos = 0
    # try a couple of times to fetch until exhausted or we collect total_needed
    while len(bars) < total_needed:
        to_request = min(CHUNK, total_needed - len(bars))
        try:
            batch = mt5.copy_rates_from_pos(sym, tf, pos, to_request)
        except Exception as e:
            print("  MT5 copy_rates_from_pos raised:", e)
            break
        if not batch or len(batch) == 0:
            # nothing more available
            break
        # extend and move offset
        bars.extend(list(batch))
        pos += len(batch)
        # small delay to avoid hammering MT5
        time.sleep(0.05)
        # safety cap (avoid infinite loops)
        if pos > 200000:
            break
    return bars

def compute_signals_from_bars(bars):
    n = len(bars)
    closes = [float(r[4]) for r in bars]
    highs = [float(r[2]) for r in bars]
    lows  = [float(r[3]) for r in bars]
    signals = []
    for i in range(n):
        price = closes[i]
        if i < 5:
            signals.append(0.0)
            continue
        period = min(20, i+1)
        sma = sum(closes[i-period+1:i+1]) / period
        tr_list = []
        start_j = max(1, i-50)
        for j in range(start_j, i+1):
            if j == 0: continue
            high = highs[j]; low = lows[j]; prev_close = closes[j-1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
        atr_period = min(14, len(tr_list))
        atr = sum(tr_list[-atr_period:]) / atr_period if atr_period > 0 else 0.0
        eps = 1e-9
        raw = (price - sma) / (atr + eps)
        s = math.tanh(raw / 2.0)
        if s>1.0: s = 1.0
        if s<-1.0: s = -1.0
        signals.append(float(s))
    return signals

def summarize(signals):
    import statistics
    if not signals: return None
    abs_s = sorted([abs(x) for x in signals])
    n = len(signals)
    mn = min(signals); mx = max(signals)
    mean = statistics.mean(signals)
    stdev = statistics.pstdev(signals) if n>1 else 0.0
    p50 = statistics.median(signals)
    def pctile(arr, p):
        if not arr: return 0.0
        idx = max(0, min(len(arr)-1, int(p*len(arr))-1))
        return arr[idx]
    p90 = pctile(abs_s, 0.90)
    p95 = pctile(abs_s, 0.95)
    p99 = pctile(abs_s, 0.99)
    counts = {}
    for t in (0.02, 0.04, 0.08, 0.18, 0.4, 0.6):
        counts[t] = sum(1 for x in abs_s if x >= t)
    return {"n":n,"min":mn,"max":mx,"mean":mean,"std":stdev,"median":p50,"abs_p90":p90,"abs_p95":p95,"abs_p99":p99,"counts":counts}

def run_one(sym):
    resolved = resolve(sym)
    print(f"\n--- {sym} -> resolved: {resolved}")
    if not resolved:
        print("  Could not resolve symbol; ensure Market Watch contains it.")
        return
    # try fetching in chunks
    print(f"  requesting up to {TARGET_BARS} bars (chunk={CHUNK}) for {resolved} ...")
    bars = fetch_recent_bars(resolved, TF, TARGET_BARS)
    if not bars or len(bars)==0:
        print("  MT5 returned no bars or empty. Try opening the chart, 'max bars in history', or smaller SAMPLE_BARS.")
        return
    print("  bars fetched:", len(bars))
    signals = compute_signals_from_bars(bars)
    stats = summarize(signals)
    if not stats:
        print("  no stats")
        return
    print(f"  stats: n={stats['n']} mean={stats['mean']:.6g} std={stats['std']:.6g} min={stats['min']:.6g} max={stats['max']:.6g}")
    print(f"  abs p90={stats['abs_p90']:.6g}, p95={stats['abs_p95']:.6g}, p99={stats['abs_p99']:.6g}")
    for t,c in stats["counts"].items():
        print(f"   count |signal|>={t}: {c} ({c/stats['n']:.2%})")
    suggested = stats["abs_p90"]
    print("  SUGGESTED threshold (90th pct of |signal|) =", suggested)

def main():
    print("Initializing MT5...")
    ok = mt5.initialize()
    print("mt5.initialize() ->", ok, "last_error:", mt5.last_error())
    for s in SYMBOLS:
        run_one(s)
    mt5.shutdown()
    print("\nDone.")

if __name__ == "__main__":
    main()
