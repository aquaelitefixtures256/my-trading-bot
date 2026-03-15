# sample_signals.py
# Fetch recent MT5 bars and compute the deterministic adapter signal on historical bars.
# Prints summary stats and a suggested threshold (90th percentile of abs(signal)).
import math, sys
from pathlib import Path
import statistics
try:
    import MetaTrader5 as mt5
except Exception as e:
    print("ERROR: MetaTrader5 not importable:", e)
    sys.exit(1)

# CONFIG
SYMBOLS = ["BTCUSD","EURUSD","USDJPY","XAUUSD","USOIL","DXY"]   # will resolve to Exness 'm' suffix if needed
TIMEFRAME = mt5.TIMEFRAME_M5   # 5-minute bars
DAYS = 30
BARS_PER_DAY = 24 * 12         # 5-min bars per day
SAMPLE_BARS = 2000 * BARS_PER_DAY  # 30 days => 8640 bars

# smaller sample if you want quicker run (uncomment small)
# SAMPLE_BARS = 2000

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

def compute_signals_from_bars(bars):
    # bars is list/array of tuples as returned by mt5.copy_rates_from_pos
    n = len(bars)
    closes = [float(r[4]) for r in bars]
    highs = [float(r[2]) for r in bars]
    lows  = [float(r[3]) for r in bars]
    signals = []
    # For each bar index i compute sample similar to adapter:
    # SMA of last up to 20 bars, ATR from prior TRs (14), raw=(price - sma)/atr, signal = tanh(raw/2)
    for i in range(n):
        price = closes[i]
        # need at least some history
        if i < 5:
            signals.append(0.0)
            continue
        # SMA
        period = min(20, i+1)
        sma = sum(closes[i-period+1:i+1]) / period
        # ATR
        tr_list = []
        for j in range(max(1, i-50), i+1):  # compute TRs from some recent window for stability
            # ensure j>0 for prev close
            if j==0: continue
            high = highs[j]
            low = lows[j]
            prev_close = closes[j-1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
        atr_period = min(14, len(tr_list))
        atr = sum(tr_list[-atr_period:]) / atr_period if atr_period>0 else 0.0
        eps = 1e-9
        raw = (price - sma) / (atr + eps)
        s = math.tanh(raw / 2.0)
        # clamp
        if s>1.0: s = 1.0
        if s<-1.0: s = -1.0
        signals.append(float(s))
    return signals

def summarize(signals):
    abs_s = [abs(x) for x in signals]
    s_nonzero = [x for x in signals if x!=0.0]
    if not signals:
        return {}
    n = len(signals)
    mn = min(signals)
    mx = max(signals)
    mean = statistics.mean(signals)
    stdev = statistics.pstdev(signals) if n>1 else 0.0
    p50 = statistics.median(signals)
    p90 = sorted(abs_s)[int(0.90*len(abs_s))-1] if len(abs_s)>0 else 0.0
    p95 = sorted(abs_s)[int(0.95*len(abs_s))-1] if len(abs_s)>0 else 0.0
    p99 = sorted(abs_s)[int(0.99*len(abs_s))-1] if len(abs_s)>0 else 0.0
    # counts above typical thresholds
    counts = {}
    for t in [0.2, 0.4, 0.6, 0.8]:
        counts[t] = sum(1 for x in abs_s if x >= t)
    return {
        "n": n, "min": mn, "max": mx, "mean": mean, "std": stdev,
        "median": p50, "abs_p90": p90, "abs_p95": p95, "abs_p99": p99,
        "counts": counts
    }

def run():
    print("Initializing MT5...")
    ok = mt5.initialize()
    print("mt5.initialize() ->", ok, "last_error:", mt5.last_error())
    for s in SYMBOLS:
        resolved = resolve(s)
        print("\n---", s, "-> resolved:", resolved)
        if not resolved:
            print("  Could not resolve symbol; skipping.")
            continue
        print(f"  requesting last {SAMPLE_BARS} bars ({DAYS} days @5m) for {resolved} ...")
        bars = mt5.copy_rates_from_pos(resolved, TIMEFRAME, 0, SAMPLE_BARS)
        if not bars or len(bars)==0:
            print("  MT5 returned no bars or empty. Ensure Market Watch and MT5 terminal.")
            continue
        print("  bars fetched:", len(bars))
        signals = compute_signals_from_bars(bars)
        stats = summarize(signals)
        if not stats:
            print("  no stats")
            continue
        print(f"  stats: n={stats['n']} mean={stats['mean']:.6g} std={stats['std']:.6g} min={stats['min']:.6g} max={stats['max']:.6g}")
        print(f"  abs p90={stats['abs_p90']:.6g}, p95={stats['abs_p95']:.6g}, p99={stats['abs_p99']:.6g}")
        for t,c in stats["counts"].items():
            print(f"   count |signal|>={t}: {c} ({c/stats['n']:.2%})")
        # suggested threshold = abs p90
        suggested = stats["abs_p90"]
        print("  SUGGESTED threshold (90th pct of |signal|) =", suggested)
    mt5.shutdown()
    print("\nDone.")

if __name__ == "__main__":
    run()
