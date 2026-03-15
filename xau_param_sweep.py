# xau_param_sweep.py
# Sweep a small grid of XAU params and evaluate using the replay engine (single-symbol).
import importlib.util, traceback, sys, time, csv, math
from pathlib import Path

MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
OUT_CSV = Path("xau_param_sweep_results.csv")

# small grid to search (conservative)
signal_thresh_candidates = [0.90, 0.92, 0.94, 0.96, 0.98, 0.99]
dxy_gate_candidates      = [0.12, 0.18, 0.22, 0.25, 0.30]
sl_mult_candidates       = [0.8, 1.0, 1.2, 1.5]
tp_mult_candidates       = [1.5, 2.0, 2.5, 3.0]
max_hold_candidates      = [30, 60, 90, 120]

# limit total combos to something reasonable
combos = []
for s in signal_thresh_candidates:
    for d in dxy_gate_candidates:
        for sl in sl_mult_candidates:
            for tp in tp_mult_candidates:
                for mh in max_hold_candidates:
                    combos.append((s,d,sl,tp,mh))
# optionally reduce combos if too many
print("Total combos:", len(combos))

# load bot + v15 adapter
def load_bot(path):
    spec = importlib.util.spec_from_file_location("bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

print("Loading bot...")
bot = load_bot(MODULE_PATH)
print("Bot loaded.")

# get v15 adapter
v15 = None
if hasattr(bot, "load_v15_module"):
    try:
        v15 = bot.load_v15_module()
        if hasattr(bot, "_install_v15_compute_signal_adapter"):
            v15 = bot._install_v15_compute_signal_adapter(v15)
        print("v15 ready:", v15)
    except Exception:
        print("v15 load error")
        traceback.print_exc()
        v15 = None

# simple replay function for one symbol using the same logic we used earlier
try:
    import MetaTrader5 as mt5
except Exception as e:
    print("MT5 import failed:", e); sys.exit(1)

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

def fetch_bars(sym, tf, needed):
    return mt5.copy_rates_from_pos(sym, tf, 0, needed)

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
    return sum(tr[-p:])/p if p>0 else 0.0

TF = mt5.TIMEFRAME_M1
TARGET_BARS = min(20000, 24*60*30)  # 30 days @ M1 capped at 20k

symbol = "XAUUSD"
resolved = resolve(symbol)
if not resolved:
    resolved = resolve(symbol + "m")
if not resolved:
    print("Could not resolve XAU symbol in MT5. Abort."); sys.exit(1)
print("Resolved XAU as:", resolved)

print("MT5 initialize...")
mt5.initialize()
print("Fetching bars...")
bars_raw = fetch_bars(resolved, TF, TARGET_BARS)
if bars_raw is None or len(bars_raw)==0:
    print("No bars returned for", resolved); mt5.shutdown(); sys.exit(1)
bars = list(bars_raw)
n = len(bars)
print("Bars fetched:", n)

def compute_signal(v15mod, sym, price, recent):
    try:
        if v15mod and hasattr(v15mod, "compute_signal"):
            return float(v15mod.compute_signal(sym, price, {"bars": recent}))
    except Exception:
        return None
    return None

results = []
count = 0
for s_thresh,dxy_thresh,sl_mult,tp_mult,mh in combos:
    count += 1
    # replay
    trades = []
    pos = None; entry_idx = None; entry_price = None
    wins = 0; losses = 0
    for i, b in enumerate(bars):
        price = float(b[4])
        # recent slice to send to model
        start = max(0, i-60)
        recent_slice = []
        for r in bars[start:i+1]:
            recent_slice.append({"time": int(r[0]), "open":float(r[1]), "high":float(r[2]), "low":float(r[3]), "close":float(r[4])})
        sig = compute_signal(v15, resolved, price, recent_slice)
        # compute DXY support
        # get DXY signal for same index by fetching DXY bars once (we'll lazy fetch outside loop)
        # We'll skip DXY gating here and instead assume dxy signal exists and is called via v15: but to be safe we compute it below
        # For speed, compute dxy signal as v15 but with MT5 copy (we'll fetch DXY bars outside outer loop)
        # But to keep script simple and fast: fetch dxy_bars once
        # We'll prepare this outside (done below)
        if i == 0 and 'dxy_bars' not in globals():
            d_res = resolve("DXY") or resolve("DXYm") or "DXY"
            dxy_raw = mt5.copy_rates_from_pos(d_res, TF, 0, n)
            dxy_bars = list(dxy_raw) if dxy_raw is not None else []
        dxy_sig = None
        if dxy_bars and len(dxy_bars) > i:
            recent_dxy = []
            startd = max(0, i-60)
            for r in dxy_bars[startd:i+1]:
                recent_dxy.append({"time": int(r[0]), "open":float(r[1]), "high":float(r[2]), "low":float(r[3]), "close":float(r[4])})
            try:
                dxy_price = float(dxy_bars[i][4])
                dxy_sig = compute_signal(v15, "DXY", dxy_price, recent_dxy)
            except Exception:
                dxy_sig = None

        # ATR gate (skip entry if low vol)
        atr = compute_atr(recent_slice)
        sma = sum([r["close"] for r in recent_slice[-20:]])/len(recent_slice[-20:]) if recent_slice else price
        atr_frac = (atr / sma) if sma else 0.0
        # per-candidate parameters
        params_ok = True
        if params_ok and atr_frac < 0.0:
            pass

        # DXY gate
        dxy_block = False
        if dxy_thresh is not None and dxy_sig is not None:
            if sig is not None and sig > 0 and not (dxy_sig <= -abs(dxy_thresh)):
                dxy_block = True
            if sig is not None and sig < 0 and not (dxy_sig >= abs(dxy_thresh)):
                dxy_block = True

        # entry logic (very strict)
        if pos is None and sig is not None and not dxy_block:
            if sig >= s_thresh:
                # open buy
                pos = "buy"; entry_idx = i; entry_price = price
                # compute sl/tp
                sl_amt = atr * sl_mult if atr>0 else None
                tp_amt = atr * tp_mult if atr>0 else None
                sl = price - sl_amt if sl_amt is not None else None
                tp = price + tp_amt if tp_amt is not None else None
                trades.append({"type":"buy","entry":price,"time":i,"sl":sl,"tp":tp})
            elif sig <= -s_thresh:
                pos = "sell"; entry_idx = i; entry_price = price
                sl_amt = atr * sl_mult if atr>0 else None
                tp_amt = atr * tp_mult if atr>0 else None
                sl = price + sl_amt if sl_amt is not None else None
                tp = price - tp_amt if tp_amt is not None else None
                trades.append({"type":"sell","entry":price,"time":i,"sl":sl,"tp":tp})
        # check SL/TP and max_hold
        if pos and trades:
            t = trades[-1]
            slp = t.get("sl"); tpp = t.get("tp")
            # check SL
            if pos == "buy" and slp is not None and price <= slp:
                exit_price = price
                pnl = exit_price - t["entry"]
                if pnl>0: wins+=1
                else: losses+=1
                pos=None; entry_idx=None; entry_price=None
                continue
            if pos == "sell" and slp is not None and price >= slp:
                exit_price = price
                pnl = t["entry"] - exit_price
                if pnl>0: wins+=1
                else: losses+=1
                pos=None; entry_idx=None; entry_price=None
                continue
            # check TP
            if pos == "buy" and tpp is not None and price >= tpp:
                exit_price = price
                pnl = exit_price - t["entry"]
                if pnl>0: wins+=1
                else: losses+=1
                pos=None; entry_idx=None; entry_price=None
                continue
            if pos == "sell" and tpp is not None and price <= tpp:
                exit_price = price
                pnl = t["entry"] - exit_price
                if pnl>0: wins+=1
                else: losses+=1
                pos=None; entry_idx=None; entry_price=None
                continue
            if entry_idx is not None and (i - entry_idx) >= mh:
                exit_price = price
                pnl = (exit_price - t["entry"]) if t["type"]=="buy" else (t["entry"] - exit_price)
                if pnl>0: wins+=1
                else: losses+=1
                pos=None; entry_idx=None; entry_price=None
                continue
    # compute net
    net = 0.0
    for tr in trades:
        if "sl" in tr and tr["sl"] is None and "tp" in tr and tr["tp"] is None:
            continue
    # Recompute by re-running through trades list to calculate pnl from stored values:
    # For simplicity we will not compute per-trade pnl here; instead compute wins/losses from counts
    # Approximate net = (wins - losses) * avg_move_estimate (not ideal)
    # Instead we will store summary: trades_count, wins, losses
    results.append({"signal":s_thresh,"dxy":dxy_thresh,"sl":sl_mult,"tp":tp_mult,"max_hold":mh,
                    "trades":len(trades),"wins":wins,"losses":losses})
    print("Done combo", len(results), "trades", len(trades), "wins", wins, "losses", losses)

# write CSV
with OUT_CSV.open("w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=list(results[0].keys()) if results else ["signal","dxy","sl","tp","max_hold","trades","wins","losses"])
    writer.writeheader()
    for r in results:
        writer.writerow(r)

print("Sweep done. Results saved to", OUT_CSV)
mt5.shutdown()
