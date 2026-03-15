# debug_backtest_replay.py
# Replay-style backtest that mimics run_backtest but with verbose logging and CSV output.
# Purpose: show exactly how many signals exceed thresholds and whether trades would be created.
import importlib.util, traceback, sys, time, csv, math
from pathlib import Path

MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
OUT_DIR = Path("debug_backtest_output")
OUT_DIR.mkdir(exist_ok=True)

def load_bot(path):
    spec = importlib.util.spec_from_file_location("bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

print("Loading bot module...")
bot = load_bot(MODULE_PATH)
print("Loaded:", MODULE_PATH)

# load v15 adapter if present
v15 = None
if hasattr(bot, "load_v15_module"):
    try:
        v15 = bot.load_v15_module()
        if hasattr(bot, "_install_v15_compute_signal_adapter"):
            v15 = bot._install_v15_compute_signal_adapter(v15)
        print("v15 module ready:", v15)
    except Exception:
        print("v15 loader error:")
        traceback.print_exc()

# read params map and symbols
CONFIG = getattr(bot, "CONFIG", {})
WATCH = CONFIG.get("WATCH_SYMBOLS", [])[:6] if CONFIG else ["BTCUSD","EURUSD","USDJPY","XAUUSD","USOIL","DXY"]
BACKTEST_PARAMS = CONFIG.get("BACKTEST_PARAMS", {})

# MT5 import and helper
try:
    import MetaTrader5 as mt5
except Exception as e:
    print("ERROR: MetaTrader5 not importable:", e)
    sys.exit(1)

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

def fetch_bars_chunked(sym, tf, needed):
    # fetch up to `needed` bars using chunking, return as list (old->new order preserved)
    bars = []
    pos = 0
    CHUNK = 4000
    tries = 0
    while len(bars) < needed and tries < 50:
        req = min(CHUNK, needed - len(bars))
        batch = mt5.copy_rates_from_pos(sym, tf, pos, req)
        if batch is None or (hasattr(batch, "__len__") and len(batch) == 0):
            break
        # extend
        try:
            bars.extend(list(batch))
            pos += len(batch)
        except Exception:
            for b in batch:
                bars.append(b)
            pos += req
        tries += 1
        time.sleep(0.02)
        if pos > 200000:
            break
    return bars

def compute_signal_with_ctx(v15mod, symbol, price, recent_for_ctx):
    try:
        if v15mod is None:
            return None
        if hasattr(v15mod, "compute_signal") and callable(v15mod.compute_signal):
            return float(v15mod.compute_signal(symbol, price, {"bars": recent_for_ctx}))
        if hasattr(v15mod, "signal_to_side") and callable(v15mod.signal_to_side):
            return float(v15mod.signal_to_side(symbol, price))
    except Exception:
        traceback.print_exc()
    return None

# replay parameters
DAYS = 30
BARS_PER_DAY_M1 = 24 * 60
TARGET_BARS = min(24*60*DAYS, 20000)  # limit to 20k to be safe; you can increase
TF = mt5.TIMEFRAME_M1

print("Initializing MT5...")
ok = mt5.initialize()
print("mt5.initialize ->", ok, "last_error:", mt5.last_error())

summary = {}

for sym in WATCH:
    resolved = resolve(sym)
    print("\n---", sym, "-> resolved:", resolved)
    if not resolved:
        print("  could not resolve; skipping")
        continue

    # choose param key: prefer exact resolved (with m) else basic
    p = BACKTEST_PARAMS.get(resolved) or BACKTEST_PARAMS.get(sym) or BACKTEST_PARAMS.get(sym + "m", {})
    signal_thresh = p.get("signal_thresh", 0.6)
    max_hold = p.get("max_hold", 60)

    print(f"  target bars={TARGET_BARS} TF=M1")
    bars = fetch_bars_chunked(resolved, TF, TARGET_BARS)
    if not bars or len(bars) == 0:
        print("  no bars fetched. Increase MarketWatch, check MT5. Skipping.")
        continue
    print("  bars fetched:", len(bars))

    # simulate
    trades = []
    position = None
    entry_price = None
    entry_index = None
    wins = 0
    losses = 0
    triggers_count = 0
    sample_triggers = []

    n = len(bars)
    for i, b in enumerate(bars):
        price = float(b[4])
        # prepare recent slice
        start = max(0, i - 60)
        recent_slice = []
        for r in bars[start:i+1]:
            # normalize to dict
            try:
                recent_slice.append({"time": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])})
            except Exception:
                try:
                    recent_slice.append({"time": getattr(r, "time", None), "close": float(getattr(r, "close", 0.0))})
                except Exception:
                    recent_slice.append({"close": float(0.0)})
        sig = compute_signal_with_ctx(v15, resolved, price, recent_slice)
        if sig is None:
            continue
        # count triggers (for logging)
        if abs(sig) >= signal_thresh:
            triggers_count += 1
            if len(sample_triggers) < 20:
                sample_triggers.append((i, round(sig,6), price))
        # ENTRY
        if position is None:
            if sig is not None and sig >= signal_thresh:
                position = "buy"; entry_price = price; entry_index = i
                trades.append({"type":"buy","entry":price,"time":i})
            elif sig is not None and sig <= -signal_thresh:
                position = "sell"; entry_price = price; entry_index = i
                trades.append({"type":"sell","entry":price,"time":i})
        # EXIT logic: hold for max_hold bars or opposite/weak signal
        if position:
            held = i - (entry_index if entry_index is not None else i)
            if held >= max_hold or (sig is not None and abs(sig) < 0.2):
                exit_price = price
                t = trades[-1]
                t.update({"exit": exit_price, "exit_time": i})
                pnl = (exit_price - t["entry"]) if t["type"] == "buy" else (t["entry"] - exit_price)
                t["pnl"] = pnl
                if pnl > 0: wins += 1
                else: losses += 1
                position = None; entry_price = None; entry_index = None

    # finalize: if still open, close at last price
    if position:
        exit_price = float(bars[-1][4])
        t = trades[-1]
        if "exit" not in t:
            t.update({"exit": exit_price, "exit_time": n-1})
            pnl = (exit_price - t["entry"]) if t["type"] == "buy" else (t["entry"] - exit_price)
            t["pnl"] = pnl
            if pnl > 0: wins += 1
            else: losses += 1

    net = sum(t.get("pnl",0) for t in trades)
    num = len(trades)
    win_rate = (wins/(wins+losses)) if (wins+losses)>0 else 0.0
    max_dd = 0.0

    print(f"  triggers >= thresh: {triggers_count}/{n} ({(triggers_count/n*100):.2f}%)")
    print("  trades:", num, "wins:", wins, "losses:", losses, "net:", net, "win_rate:", win_rate)
    if sample_triggers:
        print("  sample triggers:", sample_triggers[:10])

    # write per-symbol CSV of trades
    outp = OUT_DIR / f"replay_trades_{resolved}.csv"
    with outp.open("w", newline='', encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=['time','type','entry','exit','pnl','exit_time'])
        w.writeheader()
        for t in trades:
            w.writerow({'time': t.get('time'), 'type': t.get('type'), 'entry': t.get('entry'),
                        'exit': t.get('exit'), 'pnl': t.get('pnl'), 'exit_time': t.get('exit_time')})

    summary[resolved] = {"trades": num, "wins": wins, "losses": losses, "net": net, "triggers": triggers_count, "bars": n}

print("\nSummary:")
for k,v in summary.items():
    print(k, v)

mt5.shutdown()
print("done. Replay CSVs in", OUT_DIR)
