# xau_walkforward.py
# Robust per-window walk-forward for XAU (fixed fetch-chunk behavior)
# Usage: python xau_walkforward.py
import importlib.util, sys, traceback, math, time
from pathlib import Path

UPGRADED_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"  # adjust if needed
NUM_WINDOWS = 6
WINDOW_DAYS = 30
STEP_DAYS = 15
TF_MINUTES = 1  # M1 timeframe
PER_WINDOW_FETCH = WINDOW_DAYS * 24 * 60  # bars per window

def safe_import_bot(path):
    spec = importlib.util.spec_from_file_location("bot_mod", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

def minutes(days):
    return int(days * 24 * 60)

def rates_to_bars(rates):
    bars = []
    for r in rates:
        bars.append({
            'time': int(r[0]),
            'open': float(r[1]),
            'high': float(r[2]),
            'low': float(r[3]),
            'close': float(r[4]),
            'tick_volume': int(r[5]) if len(r) > 5 else 0,
            'spread': int(r[6]) if len(r) > 6 else 0,
            'real_volume': float(r[7]) if len(r) > 7 else 0.0,
        })
    return bars

def detect_and_normalize_order(bars):
    # returns bars ordered oldest -> newest
    if not bars:
        return bars
    if bars[0]['time'] <= bars[-1]['time']:
        return bars
    return list(reversed(bars))

def fetch_window_mt5(mt5, symbol_candidate, start_pos, count):
    # ask MT5 for 'count' bars starting at position 'start_pos'
    try:
        rates = mt5.copy_rates_from_pos(symbol_candidate, mt5.TIMEFRAME_M1, int(start_pos), int(count))
    except Exception as e:
        return None, f"copy_rates_from_pos exception: {e}"
    if rates is None:
        return None, None
    bars = rates_to_bars(rates)
    bars = detect_and_normalize_order(bars)
    return bars, None

# a simple ATR computation used by local backtest
def compute_atr(bars, period=14):
    if not bars or len(bars) < period+1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        high = bars[i]['high']
        low = bars[i]['low']
        prev = bars[i-1]['close']
        tr = max(high-low, abs(high-prev), abs(low-prev))
        trs.append(tr)
    if len(trs) < period:
        period = len(trs)
    return sum(trs[-period:]) / period if period>0 else 0.0

def local_backtest(v15, symbol, params, window_bars):
    # Minimal deterministic backtest: mirrors your current logic (signal_thresh, ATR SL/TP, max_hold)
    trades = []
    position = None
    entry_price = None
    entry_idx = None
    wins = losses = 0
    signal_thresh = float(params.get("signal_thresh", 0.6))
    sl_atr_mult = float(params.get("sl_atr_mult", 1.2))
    tp_atr_mult = float(params.get("tp_atr_mult", 2.5))
    max_hold = int(params.get("max_hold", 60))
    max_loss_abs = params.get("max_loss_abs", None)
    if max_loss_abs is not None:
        max_loss_abs = float(max_loss_abs)

    for i, bar in enumerate(window_bars):
        price = float(bar['close'])
        # compute signal
        signal = None
        try:
            if v15 and hasattr(v15, "compute_signal"):
                signal = v15.compute_signal(symbol, price, {})
        except Exception:
            signal = None
        if signal is None:
            signal = 0.0

        if position is None:
            if signal >= signal_thresh:
                position = 'buy'
                entry_price = price; entry_idx = i
                atr = compute_atr(window_bars[max(0, i-50):i+1])
                sl_amt = atr*sl_atr_mult if atr>0 else None
                tp_amt = atr*tp_atr_mult if atr>0 else None
                sl_price = (entry_price - sl_amt) if sl_amt else None
                tp_price = (entry_price + tp_amt) if tp_amt else None
                if max_loss_abs is not None and sl_price is not None:
                    sl_price = max(sl_price, entry_price - max_loss_abs)
                trades.append({'type':'buy','entry':entry_price,'entry_idx':i,'sl_price':sl_price,'tp_price':tp_price})
            elif signal <= -signal_thresh:
                position = 'sell'
                entry_price = price; entry_idx = i
                atr = compute_atr(window_bars[max(0, i-50):i+1])
                sl_amt = atr*sl_atr_mult if atr>0 else None
                tp_amt = atr*tp_atr_mult if atr>0 else None
                sl_price = (entry_price + sl_amt) if sl_amt else None
                tp_price = (entry_price - tp_amt) if tp_amt else None
                if max_loss_abs is not None and sl_price is not None:
                    sl_price = min(sl_price, entry_price + max_loss_abs)
                trades.append({'type':'sell','entry':entry_price,'entry_idx':i,'sl_price':sl_price,'tp_price':tp_price})
        else:
            # check exit conditions
            last = trades[-1]
            held = i - last['entry_idx']
            last_price = price
            exit_now = False; exit_price = last_price; reason = 'timeout'
            if last['type']=='buy':
                if last.get('sl_price') is not None and last_price <= last['sl_price']:
                    exit_now=True; exit_price = last['sl_price']; reason='sl'
                elif last.get('tp_price') is not None and last_price >= last['tp_price']:
                    exit_now=True; exit_price = last['tp_price']; reason='tp'
            else:
                if last.get('sl_price') is not None and last_price >= last['sl_price']:
                    exit_now=True; exit_price = last['sl_price']; reason='sl'
                elif last.get('tp_price') is not None and last_price <= last['tp_price']:
                    exit_now=True; exit_price = last['tp_price']; reason='tp'
            if not exit_now:
                if held >= max_hold:
                    exit_now=True; exit_price = last_price; reason='max_hold'
                else:
                    # opposite signal forced exit
                    if last['type']=='buy' and signal <= -0.2:
                        exit_now=True; exit_price=last_price; reason='opp_signal'
                    if last['type']=='sell' and signal >= 0.2:
                        exit_now=True; exit_price=last_price; reason='opp_signal'
            if exit_now:
                last['exit_idx'] = i
                last['exit'] = float(exit_price)
                last['pnl'] = (last['exit']-last['entry']) if last['type']=='buy' else (last['entry']-last['exit'])
                last['exit_reason'] = reason
                if last['pnl']>0: wins+=1
                else: losses+=1
                position = None

    # close open
    if position is not None:
        last = trades[-1]
        last_price = float(window_bars[-1]['close'])
        last['exit_idx'] = len(window_bars)-1
        last['exit'] = last_price
        last['pnl'] = (last['exit']-last['entry']) if last['type']=='buy' else (last['entry']-last['exit'])
        last['exit_reason'] = 'eod'
        if last['pnl']>0: wins+=1
        else: losses+=1

    net = sum(t.get('pnl',0.0) for t in trades)
    return {'net': net, 'trades': len(trades), 'wins': wins, 'losses': losses, 'trades_list': trades}

def main():
    print("========== XAU Walkforward runner (fixed-chunk) ==========")
    bot_path = Path(UPGRADED_FILE)
    if not bot_path.exists():
        print("Bot file not found:", UPGRADED_FILE); sys.exit(1)
    bot = safe_import_bot(str(bot_path))
    try:
        v15 = bot.load_v15_module()
        print("v15 loaded:", getattr(v15, "__name__", str(type(v15))))
    except Exception:
        v15 = None
        print("v15 load failed or not present; continuing without it.")

    # try bot.run_backtest once (if it works fine we still use fallback per-window fetch below)
    try:
        print("Testing bot.run_backtest(...) call (one-day sample)")
        sample = bot.run_backtest(v15, symbol="XAUUSD", days=1)
        print("bot.run_backtest() returned:", sample)
    except Exception:
        print("bot.run_backtest() failed (OK) — will use MT5 fallback per-window.")
        traceback.print_exc()

    # mt5 init
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        print("MetaTrader5 import error:", e); sys.exit(1)

    print("Initializing MT5...")
    if not mt5.initialize():
        print("mt5.initialize() failed, last_error:", mt5.last_error()); sys.exit(1)
    print("mt5.initialize() -> True last_error:", mt5.last_error())

    # resolve candidate symbol names
    sym = "XAUUSD"
    symbol_map = bot.CONFIG.get("SYMBOL_MAP", {}) if hasattr(bot, "CONFIG") else {}
    candidates = []
    # from map
    mapped = symbol_map.get(sym)
    if mapped:
        candidates.append(mapped)
    # common variations
    candidates.extend([sym + "m", sym, "XAUUSDm", "XAUUSD"])
    # dedupe while preserving order
    seen = set(); unique = []
    for c in candidates:
        if not c: continue
        if c in seen: continue
        seen.add(c); unique.append(c)
    candidates = unique
    print("Symbol candidates for XAU:", candidates)

    # choose the first candidate that is present in MT5.symbols_get()
    resolved = None
    for c in candidates:
        try:
            si = mt5.symbol_info(c)
            if si is not None:
                resolved = c
                break
        except Exception:
            continue
    if resolved is None:
        print("Could not resolve any candidate symbol in MT5. Ensure symbol is added to Market Watch and the name is exact.")
        mt5.shutdown(); sys.exit(1)
    print("Resolved symbol for MT5:", resolved)

    window_minutes = minutes(WINDOW_DAYS)
    step_minutes = minutes(STEP_DAYS)

    results = []
    for w in range(NUM_WINDOWS):
        start_pos = w * step_minutes
        count = window_minutes
        print("="*8, f"Window {w+1}/{NUM_WINDOWS}", "="*8)
        print(f"Requesting window bars for {resolved}: start_pos={start_pos} count={count} ...")
        bars, err = fetch_window_mt5(mt5, resolved, start_pos, count)
        if err:
            print("Fetch error:", err)
            results.append((w, None))
            continue
        if not bars or len(bars) < count:
            print(f"Warning: got {0 if not bars else len(bars)} bars (expected {count}). Try increasing Market Watch retention or use smaller windows.")
            results.append((w, None))
            continue
        print(f"Window {w+1}: fetched {len(bars)} bars; running local backtest...")
        params = bot.CONFIG.get("BACKTEST_PARAMS", {}).get("XAUUSD", {})
        res = local_backtest(v15, "XAUUSD", params, bars)
        print("Result net=", res['net'], "trades=", res['trades'], "wins=", res['wins'], "losses=", res['losses'])
        results.append((w, res))
        time.sleep(0.2)

    mt5.shutdown()
    print("\nWALK-FORWARD SUMMARY:")
    for w, r in results:
        print("Window", w+1, "->", r)
    print("Done.")

if __name__ == "__main__":
    main()
