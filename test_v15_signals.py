# test_v15_signals.py
import importlib.util, sys, traceback, time
from datetime import datetime
try:
    import MetaTrader5 as mt5
except Exception as e:
    mt5 = None
    print("mt5 import failed:", e)

MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"

spec = importlib.util.spec_from_file_location("bot", MODULE_PATH)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

print("Loaded bot module:", MODULE_PATH)
# Try to load v15 module using your loader if it exists
v15 = None
try:
    if hasattr(bot, "load_v15_module"):
        print("Calling bot.load_v15_module()...")
        v15 = bot.load_v15_module()
        print("v15 module loaded:", v15)
    else:
        print("No load_v15_module() found in bot.")
except Exception:
    print("v15 loader raised exception:")
    traceback.print_exc()

# Ensure MT5 is running
if mt5 is None:
    print("MetaTrader5 python package not available in this env. Abort.")
    sys.exit(1)

init_ok = False
try:
    init_ok = mt5.initialize()
    print("mt5.initialize() ->", init_ok, "last_error:", mt5.last_error())
except Exception:
    print("mt5.initialize() raised exception:")
    traceback.print_exc()
    sys.exit(1)

symbols = list(getattr(bot, "CONFIG", {}).get("WATCH_SYMBOLS", []))[:6]
print("Symbols to test:", symbols)

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

for s in symbols:
    try:
        resolved = resolve(s)
        print("\n---", s, "-> resolved:", resolved)
        if not resolved:
            print("  Could not resolve symbol in MT5 MarketWatch. Add it.")
            continue
        tick = mt5.symbol_info_tick(resolved)
        if tick is None:
            print("  symbol_info_tick returned None for", resolved)
            continue
        price = getattr(tick, "last", None) or getattr(tick, "ask", None) or getattr(tick, "bid", None)
        print("  price:", price, "tick:", tick)
        # call compute_signal
        if v15 is None:
            print("  v15 module not loaded; skipping compute_signal")
            continue
        try:
            if hasattr(v15, "compute_signal") and callable(v15.compute_signal):
                sig = v15.compute_signal(resolved, float(price), {"source":"mt5_test"})
                print("  compute_signal returned:", sig, "type:", type(sig))
            elif hasattr(v15, "signal_to_side") and callable(v15.signal_to_side):
                sig = v15.signal_to_side(resolved, float(price))
                print("  signal_to_side returned:", sig, "type:", type(sig))
            else:
                print("  v15 module has no compute_signal/signal_to_side")
        except Exception:
            print("  compute_signal raised exception:")
            traceback.print_exc()
    except Exception:
        print("Error when testing symbol", s)
        traceback.print_exc()

try:
    mt5.shutdown()
    print("\nmt5.shutdown() done")
except Exception:
    pass

print("\nTest complete.")
