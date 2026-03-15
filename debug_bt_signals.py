# debug_bt_signals.py  (fixed)
import importlib.util, traceback, sys, math, time
try:
    import MetaTrader5 as mt5
except Exception as e:
    print("MT5 import failed:", e)
    sys.exit(1)

MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"

def load_bot(path):
    spec = importlib.util.spec_from_file_location("bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

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

def fetch_bars(sym, tf, n):
    try:
        bars = mt5.copy_rates_from_pos(sym, tf, 0, n)
        return bars
    except Exception as e:
        print("fetch_bars exception:", e)
        return None

def call_signal(v15, sym, price, recent_for_ctx):
    try:
        if v15 is None:
            return None
        if hasattr(v15, "compute_signal") and callable(v15.compute_signal):
            return float(v15.compute_signal(sym, price, {"bars": recent_for_ctx}))
        if hasattr(v15, "signal_to_side") and callable(v15.signal_to_side):
            return float(v15.signal_to_side(sym, price))
    except Exception:
        traceback.print_exc()
    return None

def normalize_recent_slice(bars, idx, lookback=60):
    start = max(0, idx - lookback + 1)
    slice_ = []
    for b in bars[start:idx+1]:
        # MT5 tuple: (time, open, high, low, close, ...)
        try:
            slice_.append({"time": int(b[0]), "open": float(b[1]), "high": float(b[2]), "low": float(b[3]), "close": float(b[4])})
            continue
        except Exception:
            pass
        # dict-like fallback
        try:
            slice_.append({"time": b.get("time"), "open": b.get("open"), "high": b.get("high"), "low": b.get("low"), "close": b.get("close")})
        except Exception:
            # last resort
            try:
                c = float(getattr(b, "close", 0.0))
            except Exception:
                c = 0.0
            slice_.append({"close": c})
    return slice_

def main():
    bot = load_bot(MODULE_PATH)
    print("Loaded bot:", MODULE_PATH)
    # load v15 via bot loader if exists
    v15 = None
    try:
        if hasattr(bot, "load_v15_module"):
            v15 = bot.load_v15_module()
            if hasattr(bot, "_install_v15_compute_signal_adapter"):
                v15 = bot._install_v15_compute_signal_adapter(v15)
            print("v15 module:", v15)
    except Exception:
        print("v15 loader failed:")
        traceback.print_exc()
        v15 = None

    # read thresholds
    try:
        params_map = bot.CONFIG.get("BACKTEST_PARAMS", {})
    except Exception:
        params_map = {}

    ok = mt5.initialize()
    print("MT5 initialize:", ok, "last_error:", mt5.last_error())

    symbols = list(bot.CONFIG.get("WATCH_SYMBOLS", []))[:6] if getattr(bot, "CONFIG", None) else ["BTCUSD","EURUSD","USDJPY","XAUUSD","USOIL","DXY"]
    print("Symbols to test:", symbols)

    TF = mt5.TIMEFRAME_M1
    FETCH_BARS = 2000  # safe number

    for s in symbols:
        resolved = resolve(s)
        print("\n---", s, "-> resolved:", resolved)
        if not resolved:
            print("  cannot resolve")
            continue
        bars = fetch_bars(resolved, TF, FETCH_BARS)
        # safe check to avoid numpy truth-value error
        if bars is None or (hasattr(bars, "__len__") and len(bars) == 0):
            print("  no bars returned for", resolved)
            continue
        print("  bars fetched:", len(bars))
        threshold = None
        # try multiple keys
        p = params_map.get(s) or params_map.get(s + "m") or {}
        threshold = p.get("signal_thresh", None)
        if threshold is None:
            threshold = 0.6
        count_triggers = 0
        sample_examples = []
        total = len(bars)
        for i in range(total):
            try:
                price = float(bars[i][4])
            except Exception:
                # fallback if tuple has attribute access
                try:
                    price = float(getattr(bars[i], "close", 0.0))
                except Exception:
                    price = 0.0
            recent = normalize_recent_slice(bars, i, lookback=60)
            sig = call_signal(v15, resolved, price, recent)
            if sig is None:
                continue
            if abs(sig) >= threshold:
                count_triggers += 1
                if len(sample_examples) < 20:
                    sample_examples.append((i, round(sig, 6), price))
        pct = count_triggers / total if total>0 else 0.0
        print(f"  threshold={threshold}, triggers={count_triggers}/{total} ({pct:.4%})")
        if sample_examples:
            print("  sample triggers (index, sig, price):")
            for e in sample_examples[:10]:
                print("   ", e)
        else:
            print("  (no signals exceed threshold)")

    mt5.shutdown()
    print("\nDone.")

if __name__ == "__main__":
    main()
