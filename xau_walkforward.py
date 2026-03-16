# xau_walkforward.py
# Robust walk-forward runner for XAU: tries bot.run_backtest() and falls back to a local backtester.
# Usage: python xau_walkforward.py

import importlib.util, sys, traceback, math, logging
from pathlib import Path

# CONFIG
UPGRADED_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"  # adjust if your filename differs
NUM_WINDOWS = 6
WINDOW_DAYS = 30
STEP_DAYS = 15
TF_MINUTES = 1  # M1 timeframe
MT5_CHUNK_SAFE_LIMIT = 200000  # safety cap for fetch size (prevent huge requests)

# ---- helpers ----
def safe_import_bot(path):
    spec = importlib.util.spec_from_file_location("bot_mod", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

def minutes(n_days):
    return n_days * 24 * 60

def print_banner(msg):
    print("\n" + ("="*10) + " " + msg + " " + ("="*10))

# Local ATR helper: expects bars list of dict-like with keys 'high','low','close'
def compute_atr(bars, period=14):
    if not bars or len(bars) < period+1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        high = float(bars[i]['high'])
        low = float(bars[i]['low'])
        prev_close = float(bars[i-1]['close'])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    # simple SMA ATR on last `period` elements of trs
    if len(trs) < period:
        period = len(trs)
    atr = sum(trs[-period:]) / period if period>0 else 0.0
    return float(atr)

# Convert mt5 rates (tuples) to dicts consistent with keys used here
def rates_to_bars(rates):
    # MT5 rates tuple format: (time, open, high, low, close, tick_volume, spread, real_volume)
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

# Simple local backtest that mirrors the bot's simple entry/exit logic
def local_backtest(mt5, v15_module, symbol, params, window_bars):
    # window_bars: list of bars dict (oldest -> newest)
    trades = []
    position = None
    entry_price = None
    entry_idx = None
    wins = 0
    losses = 0

    signal_thresh = float(params.get("signal_thresh", 0.6))
    sl_atr_mult = float(params.get("sl_atr_mult", 1.2))
    tp_atr_mult = float(params.get("tp_atr_mult", 2.5))
    max_hold = int(params.get("max_hold", 60))
    max_loss_abs = params.get("max_loss_abs", None)
    if max_loss_abs is not None:
        max_loss_abs = float(max_loss_abs)

    bars_len = len(window_bars)
    for i in range(bars_len):
        price = float(window_bars[i]['close'])
        signal = None
        # try v15 signal
        try:
            if v15_module and hasattr(v15_module, "compute_signal"):
                signal = v15_module.compute_signal(symbol, price, {})
        except Exception:
            signal = None
        if signal is None:
            # fallback conservative random-ish small signal if needed (but keep deterministic 0)
            signal = 0.0

        # entry logic
        if position is None:
            if signal >= signal_thresh:
                position = "buy"
                entry_price = price
                entry_idx = i
                # compute ATR at entry
                atr = compute_atr(window_bars[max(0, i-30):i+1])
                sl_amt = atr * sl_atr_mult if atr>0 else None
                tp_amt = atr * tp_atr_mult if atr>0 else None
                # compute SL/TP absolute price level
                sl_price = (entry_price - sl_amt) if sl_amt is not None else None
                tp_price = (entry_price + tp_amt) if tp_amt is not None else None
                # enforce absolute max loss cap (distance)
                if max_loss_abs is not None and sl_price is not None:
                    abs_cap = entry_price - max_loss_abs
                    # choose safer (closer to entry) price
                    sl_price = max(sl_price, abs_cap)
                trades.append({'type':'buy', 'entry': entry_price, 'entry_idx': entry_idx, 'sl_price': sl_price, 'tp_price': tp_price})
            elif signal <= -signal_thresh:
                position = "sell"
                entry_price = price
                entry_idx = i
                atr = compute_atr(window_bars[max(0, i-30):i+1])
                sl_amt = atr * sl_atr_mult if atr>0 else None
                tp_amt = atr * tp_atr_mult if atr>0 else None
                sl_price = (entry_price + sl_amt) if sl_amt is not None else None
                tp_price = (entry_price - tp_amt) if tp_amt is not None else None
                if max_loss_abs is not None and sl_price is not None:
                    abs_cap = entry_price + max_loss_abs
                    sl_price = min(sl_price, abs_cap)
                trades.append({'type':'sell', 'entry': entry_price, 'entry_idx': entry_idx, 'sl_price': sl_price, 'tp_price': tp_price})
        else:
            # position exists -> check exit conditions
            held = i - entry_idx
            cur_bar = window_bars[i]
            exit_now = False
            exit_reason = "timeout"
            exit_price = price
            # check TP/SL if available
            last_price = price
            # for buys
            last_entry = trades[-1]
            if position == 'buy':
                slp = last_entry.get('sl_price', None)
                tpp = last_entry.get('tp_price', None)
                # if sl or tp triggered by current close
                if slp is not None and last_price <= slp:
                    exit_now = True; exit_reason = "sl"
                    exit_price = slp
                elif tpp is not None and last_price >= tpp:
                    exit_now = True; exit_reason = "tp"
                    exit_price = tpp
            else:
                slp = last_entry.get('sl_price', None)
                tpp = last_entry.get('tp_price', None)
                if slp is not None and last_price >= slp:
                    exit_now = True; exit_reason = "sl"
                    exit_price = slp
                elif tpp is not None and last_price <= tpp:
                    exit_now = True; exit_reason = "tp"
                    exit_price = tpp
            # timeout / opposite signal
            if not exit_now:
                if held >= max_hold:
                    exit_now = True; exit_reason = "max_hold"
                    exit_price = last_price
                else:
                    # if opposite strong signal, exit
                    if position == 'buy' and signal <= -0.2:
                        exit_now = True; exit_reason = "opp_signal"; exit_price = last_price
                    elif position == 'sell' and signal >= 0.2:
                        exit_now = True; exit_reason = "opp_signal"; exit_price = last_price

            if exit_now:
                t = trades[-1]
                t['exit_idx'] = i
                t['exit'] = float(exit_price)
                t['pnl'] = (t['exit'] - t['entry']) if t['type']=='buy' else (t['entry'] - t['exit'])
                t['exit_reason'] = exit_reason
                # count
                if t['pnl'] > 0: wins += 1
                else: losses += 1
                position = None
                entry_price = None
                entry_idx = None

    # finalize: if open position at end, close at last bar
    if position is not None:
        t = trades[-1]
        last_price = float(window_bars[-1]['close'])
        t['exit_idx'] = bars_len-1
        t['exit'] = last_price
        t['pnl'] = (t['exit'] - t['entry']) if t['type']=='buy' else (t['entry'] - t['exit'])
        t['exit_reason'] = 'eod'
        if t['pnl']>0: wins+=1
        else: losses+=1

    net = sum(t.get('pnl',0.0) for t in trades)
    return {'net': net, 'trades': len(trades), 'wins': wins, 'losses': losses, 'trades_list': trades}

# ---- main ----
def main():
    print_banner("XAU Walkforward runner (robust)")
    # load bot
    bot_path = Path(UPGRADED_FILE)
    if not bot_path.exists():
        print("Bot file not found:", UPGRADED_FILE)
        sys.exit(1)

    bot = safe_import_bot(str(bot_path))
    # load v15
    try:
        v15 = bot.load_v15_module()
    except Exception:
        v15 = None

    # try to call official run_backtest first (safe wrapper)
    for w in range(NUM_WINDOWS):
        pass  # we'll attempt aggregated below

    print("Attempting to call bot.run_backtest() for a single sample to test compatibility...")
    try:
        # This may fail for reasons seen earlier; we run it once just to know
        test = bot.run_backtest(v15, symbol="XAUUSD", days=1)
        print("bot.run_backtest() seems callable (sample returned):", test)
    except Exception as e:
        print("bot.run_backtest() failed (OK, will use fallback). Error:")
        traceback.print_exc()

    # FALLBACK: local walk-forward using MT5 + v15 signals
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        print("Failed to import MetaTrader5 - required for fallback backtest:", e)
        sys.exit(1)

    print("Initializing MT5...")
    if not mt5.initialize():
        print("mt5.initialize() -> False, last_error:", mt5.last_error())
        sys.exit(1)
    else:
        print("mt5.initialize() -> True last_error:", mt5.last_error())

    # resolve symbol with bot symbol_map or append 'm' if Exness uses 'm' suffix
    watch = bot.CONFIG.get("WATCH_SYMBOLS", []) if hasattr(bot, "CONFIG") else []
    sym = "XAUUSD"
    # try mapping
    symbol_map = bot.CONFIG.get("SYMBOL_MAP", {}) if hasattr(bot, "CONFIG") else {}
    resolved = symbol_map.get(sym, sym + "m")  # prefer mapping, else append m
    print("Resolved XAU symbol for MT5:", resolved)

    # compute required total bars to fetch
    window_minutes = minutes(WINDOW_DAYS)
    step_minutes = minutes(STEP_DAYS)
    needed_minutes = window_minutes + (NUM_WINDOWS-1) * step_minutes
    if needed_minutes > MT5_CHUNK_SAFE_LIMIT:
        print("Requested many bars; capping to", MT5_CHUNK_SAFE_LIMIT)
        needed_minutes = MT5_CHUNK_SAFE_LIMIT

    print(f"Requesting {needed_minutes} minutes ({needed_minutes//1440} days approx) of M1 bars for {resolved} ...")
    rates = mt5.copy_rates_from_pos(resolved, mt5.TIMEFRAME_M1, 0, int(needed_minutes))
    if rates is None or len(rates) == 0:
        print("MT5 returned no bars. Ensure symbol is in Market Watch and MT5 terminal is running and that the symbol name is correct.")
        mt5.shutdown()
        sys.exit(1)

    bars = rates_to_bars(rates)  # oldest -> newest (assumption consistent with MT5)
    print("Fetched bars:", len(bars))

    params = bot.CONFIG.get("BACKTEST_PARAMS", {}).get("XAUUSD", {})
    print("Using BACKTEST_PARAMS for XAUUSD (fallback):", params)

    wf_results = []
    for i in range(NUM_WINDOWS):
        # slice window i: older windows have larger negative start index
        start = - (window_minutes + i * step_minutes)
        end = None if (i == 0) else - (i * step_minutes)
        if start == 0:
            window_bars = bars[:end] if end else bars[:]
        else:
            if end is None:
                window_bars = bars[start:]
            else:
                window_bars = bars[start:end]
        # safety: if window_bars shorter than window_minutes, skip
        if len(window_bars) < window_minutes:
            print(f"Window {i+1}: insufficient bars ({len(window_bars)} < {window_minutes}), skipping")
            wf_results.append((i, None))
            continue

        print_banner(f"Window {i+1}/{NUM_WINDOWS} (len={len(window_bars)} bars) -> running local backtest")
        res = local_backtest(mt5, v15, sym, params, window_bars)
        print("Result:", res['net'], "trades:", res['trades'], "wins:", res['wins'], "losses:", res['losses'])
        wf_results.append((i, res))

    mt5.shutdown()
    print_banner("WALK-FORWARD SUMMARY")
    for i, r in wf_results:
        print("Window", i+1, "->", r)
    print("Done.")

if __name__ == "__main__":
    main()
