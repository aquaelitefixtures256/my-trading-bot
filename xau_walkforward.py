# xau_walkforward.py  (auto-detect + diagnostic)
import importlib.util, sys, traceback, time, statistics
from pathlib import Path

UPGRADED_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"  # adjust if needed
REQUESTED_WINDOW_DAYS = 30
REQUESTED_NUM_WINDOWS = 6
STEP_DAYS = 15
TF_MINUTES = 1  # M1

def safe_import_bot(path):
    spec = importlib.util.spec_from_file_location("bot_mod", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

def minutes(days): return int(days * 24 * 60)

def rates_to_bars(rates):
    bars=[]
    for r in rates:
        bars.append({
            'time': int(r[0]),
            'open': float(r[1]),
            'high': float(r[2]),
            'low': float(r[3]),
            'close': float(r[4]),
            'tick_volume': int(r[5]) if len(r)>5 else 0,
            'spread': int(r[6]) if len(r)>6 else 0,
            'real_volume': float(r[7]) if len(r)>7 else 0.0
        })
    return bars

def compute_stats(xs):
    if not xs:
        return {}
    s = sorted(xs)
    return {
        'n': len(xs),
        'mean': statistics.mean(xs),
        'stdev': statistics.pstdev(xs) if len(xs)>1 else 0.0,
        'p50': s[len(s)//2],
        'p90': s[int(len(s)*0.9)-1],
        'p95': s[int(len(s)*0.95)-1],
        'p99': s[int(len(s)*0.99)-1],
        'min': s[0],
        'max': s[-1],
    }

def sample_signal_distribution(v15, symbol_name, bars, sample_every=10):
    sigs=[]
    count = 0
    for i in range(0, len(bars), sample_every):
        price = bars[i]['close']
        try:
            if v15 and hasattr(v15, 'compute_signal'):
                s = v15.compute_signal(symbol_name, price, {})
            else:
                s = 0.0
        except Exception:
            s = 0.0
        sigs.append(float(s))
        count += 1
    return compute_stats(sigs), sigs

def local_backtest(v15, symbol, params, window_bars):
    # simple backtest: same rules as earlier (signal_thresh, simple timeout)
    signal_thresh = float(params.get("signal_thresh", 0.6))
    trades=[]
    position=None
    entry_idx=None
    wins=losses=0
    max_hold=int(params.get("max_hold", 60))
    for i, b in enumerate(window_bars):
        price = b['close']
        try:
            s = v15.compute_signal(symbol, price, {}) if v15 and hasattr(v15,'compute_signal') else 0.0
        except Exception:
            s = 0.0
        if position is None:
            if s >= signal_thresh:
                position='buy'; entry_idx=i; trades.append({'type':'buy','entry':price,'entry_idx':i})
            elif s <= -signal_thresh:
                position='sell'; entry_idx=i; trades.append({'type':'sell','entry':price,'entry_idx':i})
        else:
            held = i - trades[-1]['entry_idx']
            exit_now=False
            if held >= max_hold:
                exit_now=True
            else:
                # exit on opposite moderate signal
                if position=='buy' and s <= -0.2: exit_now=True
                if position=='sell' and s >= 0.2: exit_now=True
            if exit_now:
                t = trades[-1]; t['exit']=price; t['exit_idx']=i
                t['pnl'] = (t['exit']-t['entry']) if t['type']=='buy' else (t['entry']-t['exit'])
                if t['pnl']>0: wins+=1
                else: losses+=1
                position=None
    # close last if open
    if position is not None:
        t=trades[-1]; t['exit']=window_bars[-1]['close']; t['exit_idx']=len(window_bars)-1
        t['pnl']=(t['exit']-t['entry']) if t['type']=='buy' else (t['entry']-t['exit'])
        if t['pnl']>0: wins+=1
        else: losses+=1
    net = sum(t.get('pnl',0.0) for t in trades)
    return {'net':net,'trades':len(trades),'wins':wins,'losses':losses,'trades_list':trades}

def main():
    print("=== XAU walkforward diagnostic (auto-detect) ===")
    bot_path = Path(UPGRADED_FILE)
    if not bot_path.exists():
        print("Bot file not found:", UPGRADED_FILE); sys.exit(1)
    bot = safe_import_bot(str(bot_path))
    try:
        v15 = bot.load_v15_module()
        print("v15 loaded:", getattr(v15,'__name__',str(type(v15))))
    except Exception:
        v15=None; print("v15 not loaded or not present")
    # mt5 init
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        print("MT5 import failed:", e); sys.exit(1)
    if not mt5.initialize():
        print("mt5.init failed:", mt5.last_error()); sys.exit(1)
    # resolve symbol
    sym = "XAUUSD"
    candidate = bot.CONFIG.get("SYMBOL_MAP",{}).get(sym) if hasattr(bot,'CONFIG') else None
    candidates = []
    if candidate: candidates.append(candidate)
    candidates += [sym+"m", sym, "XAUUSDm", "XAUUSD"]
    # unique preserve order
    seen=set(); candidates=[c for c in candidates if not (c in seen or seen.add(c))]
    print("Candidates:", candidates)
    resolved=None
    for c in candidates:
        try:
            if mt5.symbol_info(c) is not None:
                resolved=c; break
        except Exception:
            pass
    if not resolved:
        print("Could not resolve symbol in MT5; add to Market Watch then retry."); mt5.shutdown(); sys.exit(1)
    print("Resolved:", resolved)
    # detect available bars by attempting to fetch a large number (MT5 will return what it has)
    max_probe = 200000
    print(f"Probing available bars (request up to {max_probe}) ...")
    rates = mt5.copy_rates_from_pos(resolved, mt5.TIMEFRAME_M1, 0, max_probe)
    if rates is None:
        print("No bars available via MT5 for", resolved, " — ensure Market Watch -> show all and chart is open.")
        mt5.shutdown(); sys.exit(1)
    available_bars = len(rates)
    print("Available M1 bars from MT5 for", resolved, "=", available_bars)
    # determine window size / count based on available_bars
    window_minutes = minutes(REQUESTED_WINDOW_DAYS)
    step_minutes = minutes(STEP_DAYS)
    max_possible_windows = max(1, (available_bars - window_minutes)//step_minutes + 1) if available_bars >= window_minutes else 0
    windows = min(REQUESTED_NUM_WINDOWS, max_possible_windows) if max_possible_windows>0 else 0
    if windows==0:
        print("Not enough bars for a single requested window (need", window_minutes, "bars).")
        # offer to shrink window_days
        possible_window_days = max(1, available_bars//1440)
        print("You can run a window of", possible_window_days, "day(s) instead. Re-run after editing REQUESTED_WINDOW_DAYS.")
        mt5.shutdown(); sys.exit(1)
    print(f"Will run {windows} windows (window_days={REQUESTED_WINDOW_DAYS}) using available data.")
    # run windows: we will slice the last available_bars so windows are up-to-last
    bars_all = rates_to_bars(rates)  # oldest->newest assumption from MT5
    # confirm order (if times decrease then reverse)
    if bars_all[0]['time'] > bars_all[-1]['time']:
        bars_all = list(reversed(bars_all))
    results=[]
    for w in range(windows):
        start_index = available_bars - (window_minutes + w*step_minutes)
        end_index = start_index + window_minutes
        if start_index < 0:
            print("Window", w+1, "start index negative -> skipping")
            results.append((w,None)); continue
        window_bars = bars_all[start_index:end_index]
        print("\n--- Window", w+1, f"bars {len(window_bars)} (start_idx={start_index}) ---")
        # sample signal distribution
        stats, sigs = sample_signal_distribution(v15, "XAUUSD", window_bars, sample_every=10)
        print("Signal sample stats:", stats)
        params = bot.CONFIG.get("BACKTEST_PARAMS", {}).get("XAUUSD", {}) if hasattr(bot,'CONFIG') else {}
        sig_t = float(params.get("signal_thresh", 0.6))
        # count signals above threshold in sampled signals
        if sigs:
            above = sum(1 for s in sigs if abs(s) >= sig_t)
            print(f"Sample points above |threshold|={sig_t}: {above}/{len(sigs)} ({above/len(sigs):.3%})")
        # run local fallback backtest
        res = local_backtest(v15, "XAUUSD", params, window_bars)
        print("Local backtest result:", res['net'], "trades:", res['trades'], "wins:", res['wins'])
        results.append((w, res))
    mt5.shutdown()
    print("\n=== SUMMARY ===")
    for w, r in results:
        print("Window", w+1, "->", r)
    # give actionable recommendation
    # if many windows had trades==0, suggest lowering threshold or sampling v15 config
    zero_windows = sum(1 for _,r in results if (r is None or r.get('trades',0)==0))
    if zero_windows >= len(results):
        print("\nDiagnosis: NONE of the windows produced trades. Likely reasons:")
        print("- signal_thresh is too high for current v15 signal magnitude")
        print("- v15.compute_signal may be returning very small values for XAU (model mismatch or expects different symbol name)")
        print("\nRecommendations (pick one):")
        print("1) Temporarily lower BACKTEST_PARAMS['XAUUSD']['signal_thresh'] to 0.6 or 0.4 and re-run.")
        print("2) Inspect v15.compute_signal by calling it directly on a few XAU prices (I can produce a tiny test script).")
        print("3) Backfill historical data so more windows are available, or reduce REQUESTED_WINDOW_DAYS to match available bars.")
    else:
        print("\nAt least one window produced trades — inspect trade metrics and proceed to tune thresholds/ATR.")
    print("Done.")

if __name__ == '__main__':
    main()
