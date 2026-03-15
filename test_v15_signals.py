# test_v15_signals.py
# Recreated test script that loads your upgraded bot, loads v15,
# installs the compute_signal adapter if the bot exposes it, and
# prints compute_signal results for a short list of symbols using MT5 ticks.
#
# Usage: python test_v15_signals.py
#
import importlib.util
import traceback
import sys

MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"

def load_bot_module(path):
    spec = importlib.util.spec_from_file_location("bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    return bot

def safe_print(*a, **k):
    try:
        print(*a, **k)
    except Exception:
        # Ensure printing never crashes test
        sys.stdout.write(" ".join(map(str,a)) + "\n")

def main():
    try:
        bot = load_bot_module(MODULE_PATH)
    except Exception:
        safe_print("Failed to load bot module:", MODULE_PATH)
        traceback.print_exc()
        return

    safe_print("Loaded bot module:", MODULE_PATH)

    # Attempt to load v15 via the bot's loader if present
    v15 = None
    try:
        if hasattr(bot, "load_v15_module"):
            safe_print("Calling bot.load_v15_module()...")
            v15 = bot.load_v15_module()
            safe_print("v15 module loaded =", v15)
        else:
            safe_print("Bot has no load_v15_module()")
    except Exception:
        safe_print("v15 loader raised an exception:")
        traceback.print_exc()
        v15 = None

    # If the bot provides the adapter installer, use it to attach compute_signal
    try:
        if hasattr(bot, "_install_v15_compute_signal_adapter"):
            safe_print("Installing v15 compute_signal adapter via bot._install_v15_compute_signal_adapter(...)")
            try:
                v15 = bot._install_v15_compute_signal_adapter(v15)
                safe_print("Adapter installed (v15 now =)", type(v15), v15)
            except Exception:
                safe_print("Adapter installation raised an exception:")
                traceback.print_exc()
    except Exception:
        safe_print("Unexpected error while trying to install adapter:")
        traceback.print_exc()

    # Now run MT5 tests
    try:
        import MetaTrader5 as mt5
    except Exception:
        safe_print("MetaTrader5 python package not available in this environment.")
        return

    try:
        ok = mt5.initialize()
        safe_print("mt5.initialize() ->", ok, "last_error:", mt5.last_error())
    except Exception:
        safe_print("mt5.initialize() raised exception:")
        traceback.print_exc()
        return

    # Use WATCH_SYMBOLS from bot CONFIG if present, else fallback test list
    try:
        symbols = list(getattr(bot, "CONFIG", {}).get("WATCH_SYMBOLS", []))[:6]
        if not symbols:
            symbols = ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL", "DXY"]
    except Exception:
        symbols = ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL", "DXY"]

    safe_print("Symbols to test:", symbols)

    def resolve(sym):
        # try as-is then add 'm' suffix (Exness)
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
        safe_print("\n---", s, end=" ")
        try:
            resolved = resolve(s)
            safe_print("-> resolved:", resolved)
            if not resolved:
                safe_print("  Could not resolve symbol in MT5 MarketWatch. Add it.")
                continue

            tick = mt5.symbol_info_tick(resolved)
            if tick is None:
                safe_print("  symbol_info_tick returned None for", resolved)
                continue

            price = getattr(tick, "last", None) or getattr(tick, "ask", None) or getattr(tick, "bid", None)
            safe_print("  price:", price, "tick:", tick)

            # call compute_signal if possible
            if v15 is None:
                safe_print("  v15 module not loaded; skipping compute_signal")
                continue

            # if compute_signal exists, call it; otherwise try signal_to_side
            if hasattr(v15, "compute_signal") and callable(v15.compute_signal):
                try:
                    sig = v15.compute_signal(resolved, float(price), {"source":"mt5_test"})
                    safe_print("  compute_signal returned:", sig, "type:", type(sig))
                except Exception:
                    safe_print("  compute_signal raised exception:")
                    traceback.print_exc()
            elif hasattr(v15, "signal_to_side") and callable(v15.signal_to_side):
                try:
                    sig = v15.signal_to_side(resolved, float(price))
                    safe_print("  signal_to_side returned:", sig, "type:", type(sig))
                except Exception:
                    safe_print("  signal_to_side raised exception:")
                    traceback.print_exc()
            else:
                safe_print("  v15 module has no compute_signal/signal_to_side")
        except Exception:
            safe_print("Error when testing symbol", s)
            traceback.print_exc()

    try:
        mt5.shutdown()
        safe_print("\nmt5.shutdown() done")
    except Exception:
        pass

    safe_print("\nTest complete.")

if __name__ == "__main__":
    main()
