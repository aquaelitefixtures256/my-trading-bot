# xau_param_sweep.py  (REPLACEMENT - robust)
# Sweeps XAU parameter grid using MT5 historical bars and your embedded v15 compute_signal.
# Writes xau_param_sweep_results.csv with results and prints top candidates.

import importlib.util, traceback, sys, time, csv, math
from pathlib import Path

MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
OUT_CSV = Path("xau_param_sweep_results.csv")

# grid (safe, conservative)
signal_thresh_candidates = [0.90, 0.92, 0.94, 0.96, 0.98, 0.99]
dxy_gate_candidates      = [0.12, 0.18, 0.22, 0.25, 0.30]
sl_mult_candidates       = [0.8, 1.0, 1.2, 1.5]
tp_mult_candidates       = [1.5, 2.0, 2.5, 3.0]
max_hold_candidates      = [30, 60, 90, 120]

combos = []
for s in signal_thresh_candidates:
    for d in dxy_gate_candidates:
        for sl in sl_mult_candidates:
            for tp in tp_mult_candidates:
                for mh in max_hold_candidates:
                    combos.append((s,d,sl,tp,mh))
print("Total combos:", len(combos))

# load bot and v15
def load_bot(path):
    spec = importlib.util.spec_from_file_location("bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

print("Loading bot...")
bot = load_bot(MODULE_PATH)
print("Bot loaded.")

v15 = None
if hasattr(bot, "load_v15_module"):
    try:
        v15 = bot.load_v15_module()
        # ensure adapter installed if available
        if hasattr(bot, "_install_v15_compute_signal_adapter"):
            v15 = bot._install_v15_compute_signal_adapter(v15)
        print("v15 ready:", v15)
    except Exception:
        print("v15 load failed:")
        traceback.print_exc()
        v15 = None

# MT5 helper
try:
    import MetaTrader5 as mt5
except Exception as e:
    print("MT5 import failed:", e); sys.exit(1)

def try_resolve_symbol(pref_list):
    # try pref variants first
    for s in pref_list:
        try:
            if mt5.symbol_select(s, True):
                return s
        except Exception:
            pass
    # otherwise search available symbols list for matching tokens
    try:
        sl = mt5.symbols_get()
        token_set = [t.lower() for t in pref_list]
        for sym in sl:
            n = sym.name.lower()
            for token in token_set:
                if token in n:
                    try:
                        if mt5.symbol_select(sym.name, True):
                            return sym.name
                    except Exception:
                        pass
        # last resort: return first symbol containing 'xau' or 'gold'
        for sym in sl:
            n = sym.name.lower()
            if "xau" in n or "gold" in n:
                try:
                    if mt5.symbol_select(sym.name, True):
                        return sym.name
                except Exception:
                    pass
    except Exception:
        pass
    return None

def resolve_xau():
    # common variants
    prefs = ["XAUUSD", "XAUUSDm", "XAU-USD", "XAUUSD.fx", "GOLD", "XAU"]
    return try_resolve_symbol(prefs)

def resolve_dxy():
    prefs = ["DXY", "DXYm", "USDX", "USDOLLAR", "DOLLAR_INDEX"]
    return try_resolve_symbol(prefs)

def fetch_bars(sym, tf, n):
    try:
        raw = mt5.copy_rates_from_pos(sym, tf, 0, n)
        if raw is None:
            return []
        return list(raw)
    except Exception:
        return []

def compute_signal(v15mod, symbol, price, recent):
    try:
        if v15mod and hasattr(v15mod, "compute_signal") and callable(v15mod.compute_signal):
            return float(v15mod.compute_signal(symbol, price, {"bars": recent}))
        if v15mod and hasattr(v15mod, "signal_to_side") and callable(v15mod.signal_to_side):
            return float(v15mod.signal_to_side(symbol, price))
    except Exception:
        # don't spam stack on inner loops
        return None
    return None

def compute_atr(recent):
    highs = [r["high"] for r in recent if "high" in r]
    lows  = [r["low"] for r in recent if "low" in r]
    closes= [r["close"] for r in recent if "close" in r]
    if len(closes) < 2:
        return 0.0
    tr = []
    for i in range(1, len(closes)):
        tr.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    p = min(14, len(tr))
    return sum(tr[-p:]) / p if p > 0 else 0.0

# Begin
print("Initializing MT5...")
ok = mt5.initialize()
print("mt5.initialize ->", ok, "last_error:", mt5.last_error())

# resolve XAU and DXY
xau_sym = resolve_xau()
dxy_sym = resolve_dxy()
if not xau_sym:
    print("Could not resolve XAU symbol in MT5. Attempting to list candidate symbols...")
    try:
        sl = mt5.symbols_get()
        for s in sl[:200]:
            print(" -", s.name)
    except Exception:
        pass
    print("Abort.")
    mt5.shutdown()
    sys.exit(1)

print("Resolved XAU as:", xau_sym)
print("Resolved DXY as:", dxy_sym or "(none)")

TF = mt5.TIMEFRAME_M1
TARGET_BARS = min(20000, 24*60*30)

print("Fetching XAU bars (this may take a moment)...")
xau_bars = fetch_bars(xau_sym, TF, TARGET_BARS)
if not xau_bars:
    print("No XAU bars returned. Abort.")
    mt5.shutdown()
    sys.exit(1)
n = len(xau_bars)
print("Fetched", n, "bars for", xau_sym)

dxy_bars = []
if dxy_sym:
    print("Fetching DXY bars...")
    dxy_bars = fetch_bars(dxy_sym, TF, n)
    print("Fetched", len(dxy_bars), "DXY bars")

results = []
start_time = time.time()
combo_count = 0

for s_thresh, d_th, sl_mult, tp_mult, mh in combos:
    combo_count += 1
    trades = []
    pos = None
    entry_idx = None
    entry_price = None
    wins = 0
    losses = 0
    gross_win = 0.0
    gross_loss = 0.0
    for i, b in enumerate(xau_bars):
        price = float(b[4])
        # prepare recent slice
        start = max(0, i - 60)
        recent_slice = []
        for r in xau_bars[start:i+1]:
            recent_slice.append({"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])})
        sig = compute_signal(v15, xau_sym, price, recent_slice)
        # compute dxy_signal if available
        dxy_sig = None
        if dxy_bars and len(dxy_bars) > i:
            startd = max(0, i-60)
            recent_dxy = []
            for r in dxy_bars[startd:i+1]:
                recent_dxy.append({"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])})
            try:
                dxy_sig = compute_signal(v15, dxy_sym, float(dxy_bars[i][4]), recent_dxy)
            except Exception:
                dxy_sig = None
        # ATR and SMA for gate and SL/TP
        atr = compute_atr(recent_slice)
        sma = sum([r["close"] for r in recent_slice[-20:]]) / max(1, len(recent_slice[-20:]))
        atr_frac = (atr / sma) if sma else 0.0

        # DXY gate logic
        dxy_block = False
        if d_th is not None and dxy_sig is not None:
            if sig is not None and sig > 0 and not (dxy_sig <= -abs(d_th)):
                dxy_block = True
            if sig is not None and sig < 0 and not (dxy_sig >= abs(d_th)):
                dxy_block = True

        # entry
        if pos is None and sig is not None and not dxy_block:
            if sig >= s_thresh:
                # open buy
                pos = "buy"; entry_idx = i; entry_price = price
                sl_amt = atr * sl_mult if atr>0 else None
                tp_amt = atr * tp_mult if atr>0 else None
                sl = price - sl_amt if sl_amt is not None else None
                tp = price + tp_amt if tp_amt is not None else None
                trades.append({"type":"buy","entry":price,"time_idx":i,"sl":sl,"tp":tp})
            elif sig <= -s_thresh:
                pos = "sell"; entry_idx = i; entry_price = price
                sl_amt = atr * sl_mult if atr>0 else None
                tp_amt = atr * tp_mult if atr>0 else None
                sl = price + sl_amt if sl_amt is not None else None
                tp = price - tp_amt if tp_amt is not None else None
                trades.append({"type":"sell","entry":price,"time_idx":i,"sl":sl,"tp":tp})
        # management: check SL/TP and max_hold
        if pos and trades:
            t = trades[-1]
            slp = t.get("sl")
            tpp = t.get("tp")
            # check SL
            if pos == "buy" and slp is not None and price <= slp:
                exit_price = price
                pnl = exit_price - t["entry"]
                if pnl > 0:
                    wins += 1; gross_win += pnl
                else:
                    losses += 1; gross_loss += abs(pnl)
                pos = None; entry_idx = None; entry_price = None
                continue
            if pos == "sell" and slp is not None and price >= slp:
                exit_price = price
                pnl = t["entry"] - exit_price
                if pnl > 0:
                    wins += 1; gross_win += pnl
                else:
                    losses += 1; gross_loss += abs(pnl)
                pos = None; entry_idx = None; entry_price = None
                continue
            # check TP
            if pos == "buy" and tpp is not None and price >= tpp:
                exit_price = price
                pnl = exit_price - t["entry"]
                wins += 1; gross_win += pnl
                pos = None; entry_idx = None; entry_price = None
                continue
            if pos == "sell" and tpp is not None and price <= tpp:
                exit_price = price
                pnl = t["entry"] - exit_price
                wins += 1; gross_win += pnl
                pos = None; entry_idx = None; entry_price = None
                continue
            # max_hold
            if entry_idx is not None and (i - entry_idx) >= mh:
                exit_price = price
                pnl = (exit_price - t["entry"]) if t["type"] == "buy" else (t["entry"] - exit_price)
                if pnl > 0:
                    wins += 1; gross_win += pnl
                else:
                    losses += 1; gross_loss += abs(pnl)
                pos = None; entry_idx = None; entry_price = None
                continue

    # finalize still open
    if pos and trades:
        t = trades[-1]
        exit_price = float(xau_bars[-1][4])
        pnl = (exit_price - t["entry"]) if t["type"] == "buy" else (t["entry"] - exit_price)
        if pnl > 0:
            wins += 1; gross_win += pnl
        else:
            losses += 1; gross_loss += abs(pnl)
    total_trades = len(trades)
    net = gross_win - gross_loss
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else float('inf')
    results.append({
        "signal": s_thresh,
        "dxy_thresh": d_th,
        "sl_mult": sl_mult,
        "tp_mult": tp_mult,
        "max_hold": mh,
        "trades": total_trades,
        "wins": wins,
        "losses": losses,
        "net": net,
        "gross_win": gross_win,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor
    })
    # progress
    if combo_count % 10 == 0:
        elapsed = time.time() - start_time
        print(f"Combo {combo_count}/{len(combos)} done. Last net={net:.2f}. elapsed {elapsed:.1f}s")

# sort and write CSV
results_sorted = sorted(results, key=lambda r: (-(r["net"]), -r["profit_factor"]))
with OUT_CSV.open("w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=list(results_sorted[0].keys()))
    writer.writeheader()
    for r in results_sorted:
        writer.writerow(r)

print("Sweep done. Top 10 candidates:")
for r in results_sorted[:10]:
    print(r)
print("Results saved to", OUT_CSV)

mt5.shutdown()
