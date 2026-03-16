# mt5_full_debug.py
import MetaTrader5 as mt5
import platform, time, sys, traceback

CANDIDATES = ["XAUUSDm", "XAUUSD", "XAUUSD.i", "XAUUSD.m", "XAUUSD-USD", "XAU-USD"]  # extra guesses

def print_sep():
    print("="*60)

def probe_symbol(sym):
    try:
        si = mt5.symbol_info(sym)
        tick = mt5.symbol_info_tick(sym)
    except Exception as e:
        si = None; tick = None
    print(f"Symbol: {sym!r}")
    print("  symbol_info:", None if si is None else si._asdict() if hasattr(si,'_asdict') else repr(si))
    print("  tick:", tick)
    # try to fetch a tiny number of bars
    try:
        rates = mt5.copy_rates_from_pos(sym, mt5.TIMEFRAME_M1, 0, 10)
    except Exception as e:
        rates = None
        print("  copy_rates_from_pos raised:", e)
    print("  copy_rates_from_pos ->", None if rates is None else f"{len(rates)} bars")
    print()

def main():
    print("Python architecture:", platform.architecture())
    print("Python executable:", sys.executable)
    print_sep()
    try:
        ok = mt5.initialize()
    except Exception:
        ok = False
    print("mt5.initialize() ->", ok)
    try:
        le = mt5.last_error()
    except Exception:
        le = None
    print("mt5.last_error():", le)
    try:
        ti = mt5.terminal_info()
        print("mt5.terminal_info().server:", getattr(ti,'server',None))
        print("mt5.terminal_info().company:", getattr(ti,'company',None))
        print("mt5.terminal_info().version:", getattr(ti,'version',None))
    except Exception as e:
        print("terminal_info() error:", e)
    try:
        print("mt5.version():", mt5.version())
    except Exception as e:
        print("version() error:", e)
    print_sep()
    try:
        acc = mt5.account_info()
        print("account_info:", None if acc is None else acc._asdict())
    except Exception as e:
        print("account_info() error:", e)
    print_sep()
    # Show first 30 Market Watch symbols (if available)
    try:
        syms = mt5.symbols_get()
        print("Total symbols available via MT5.symbols_get():", len(syms))
        names = [s.name for s in syms[:60]]
        print("Sample symbols (first 60):", names)
    except Exception as e:
        print("symbols_get() error:", e)
    print_sep()
    # probe candidates
    for c in CANDIDATES:
        probe_symbol(c)
    # Also probe the exact symbol names that often appear in your bot
    extras = ["XAUUSD", "XAUUSDm", "XAUUSDm.s", "XAUUSD.m", "XAU-USD", "GOLD"]
    for c in extras:
        if c not in CANDIDATES:
            probe_symbol(c)
    print_sep()
    # Test a small request for any symbol currently present in symbols_get (first 5)
    try:
        for s in (syms[:5] if 'syms' in locals() and syms else []):
            name = s.name
            try:
                rates = mt5.copy_rates_from_pos(name, mt5.TIMEFRAME_M1, 0, 5)
            except Exception as e:
                rates = None
            print(f"Test fetch for {name}: {None if rates is None else len(rates)} bars")
    except Exception:
        pass
    print_sep()
    print("If copy_rates_from_pos returns None for the gold symbol, please follow GUI steps: Market Watch -> Show All -> open 1m chart for that symbol -> wait 30-60s -> re-run this script.")
    print("If you just added the symbol, give MT5 a minute to download history.")
    mt5.shutdown()
    print("mt5.shutdown() done.")

if __name__ == '__main__':
    main()
