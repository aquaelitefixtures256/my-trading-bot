# sample_signals.py
# Robust signal sampler for your KYOTO bot (fixed truth-check error).
# Fetches recent MT5 bars in chunks, computes the adapter signal historically,
# prints statistics and suggests per-symbol thresholds (90th pct of |signal|).
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
TARGET_BARS = DAYS * 24 * 12   # 30 days -> 8640 bars
CHUNK = 2000                   # safe chunk size for MT5 requests

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
    tries = 0
    # fetch until we have enough or nothing more
    while len(bars) < total_needed and tries < 20:
        to_request = min(CHUNK, total_needed - len(bars))
        try:
            batch = mt5.copy_rates_from_pos(sym, tf, pos, to_request)
        except Exception as e:
            print("  MT5 copy_rates_from_pos raised:", e)
            break
        # explicit checks to avoid numpy truth ambiguity
        if batch is None or (hasattr(batch, "__len__") and len(batch) == 0):
            # nothing more returned
            break
        # extend bars list (batch may be numpy structured array)
        try:
            bars.extend(list(batch))
        except Exception:
            # fallback: try iterating
            for b in batch:
                bars.append(b)
        pos += len(batch) if hasattr(batch, "__len__") else to_request
        tries += 1
        time.sleep(0.02)
        # safety cap
        if pos >= 200000:
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
            high = highs[j]; low = lows[j]; prev = closes[j-1]
            tr = max(high - low, abs(high - prev), abs(low - prev))
            tr_list.append(tr)
        atr_period = min(14, len(tr_list))
        atr = sum(tr_list[-atr_period:]) / atr_period if atr_period > 0 else 0.0
        eps = 1e-9
        raw = (price - sma) / (atr + eps)
        s = math.tanh(raw / 2.0)
        if s > 1.0: s = 1.0
        if s < -1.0: s = -1.0
        signals.append(float(s))
    return signals

def summarize(signals):
    import statistics
    if not signals:
        return None
    abs_s = sorted([abs(x) for x in signals])
    n = len(signals)
    mn = min(signals); mx = max(signals)
    mean = statistics.mean(signals)
    stdev = statistics.pstdev(signals) if n>1 else 0.0
    p50 = statistics.median(signals)
    def pctile(arr, p):
        if not arr:
            return 0.0
        idx = int(max(0, min(len(arr)-1, math.floor(p*len(arr)) - 1)))
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
    print(f"  requesting up to {TARGET_BARS} bars (chunk={CHUNK}) for {resolved} ...")
    bars = fetch_recent_bars(resolved, TF, TARGET_BARS)
    if bars is None or len(bars) == 0:
        print("  MT5 returned no bars or empty. Try opening the chart, increase 'Max bars in history', or reduce TARGET_BARS.")
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
        try:
            run_one(s)
        except Exception as e:
            print("  run_one raised:", e)
    mt5.shutdown()
    print("\nDone.")

if __name__ == "__main__":
    main()
