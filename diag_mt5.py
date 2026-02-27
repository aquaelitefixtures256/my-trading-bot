# diag_mt5.py
import os, time, json
from datetime import datetime
try:
    import MetaTrader5 as mt5
    import pandas as pd
except Exception as e:
    print("ERROR: missing packages. Install: pip install MetaTrader5 pandas")
    raise

# adjust to match your env or fill values directly
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
MT5_PATH = os.getenv("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")

def try_init():
    ok = mt5.initialize(login=int(MT5_LOGIN) if MT5_LOGIN and MT5_LOGIN.isdigit() else None,
                       password=MT5_PASSWORD, server=MT5_SERVER)
    print("mt5.initialize() ->", ok)
    if not ok:
        print("mt5.last_error():", mt5.last_error())
    return ok

def list_symbols_sample(limit=50):
    try:
        syms = mt5.symbols_get()
        print("Total broker symbols available:", len(syms) if syms else 0)
        for s in (syms[:limit] if syms else []):
            print("  ", s.name)
    except Exception as e:
        print("list_symbols_sample failed:", e)

def check_symbol(symbol):
    print("\n--- CHECK", symbol, "---")
    # try variants
    variants = [symbol, symbol + "m", symbol + ".m", symbol + "-m", symbol.upper()+"m"]
    found = None
    for v in variants:
        si = mt5.symbol_info(v)
        if si is not None:
            found = v
            break
    print("Mapped broker symbol:", found)
    if not found:
        print("-> symbol not found on broker. Try discover list above to see exact name (suffixes/casing).")
        return
    si = mt5.symbol_info(found)
    print("visible:", si.visible, "trade_mode:", getattr(si, 'trade_mode', None))
    try:
        if not si.visible:
            mt5.symbol_select(found, True)
            time.sleep(0.5)
            si = mt5.symbol_info(found)
            print("selected -> visible now:", si.visible)
    except Exception as e:
        print("symbol_select error:", e)

    # get tick & last price
    tick = mt5.symbol_info_tick(found)
    print("tick:", tick)

    # get recent rates (H1)
    tf = mt5.TIMEFRAME_H1
    rates = mt5.copy_rates_from_pos(found, tf, 0, 120)
    if rates is None:
        print("No rates returned for", found)
        return
    df = pd.DataFrame(rates)
    print("rates rows:", len(df))
    if 'time' in df.columns:
        print("last rate time:", datetime.utcfromtimestamp(int(df['time'].iloc[-1])).isoformat())
    if 'close' in df.columns:
        print("last close:", df['close'].iloc[-1])
    else:
        print("no close column in rates")

if __name__ == "__main__":
    print("Running MT5 diagnostic")
    ok = try_init()
    if not ok:
        # try to start terminal if path exists (best-effort)
        if os.path.exists(MT5_PATH):
            print("Attempting to start MT5 terminal from", MT5_PATH)
            try:
                import subprocess
                subprocess.Popen([MT5_PATH])
                time.sleep(4)
                ok = try_init()
            except Exception as e:
                print("Failed to spawn MT5:", e)
    if not ok:
        print("MT5 not connected. Fix credentials, server, or start terminal.")
    else:
        list_symbols_sample(80)
        for s in ["XAUUSD", "XAUUSDm", "BTCUSD", "BTCUSDm", "USOIL", "USOILm", "USDJPY", "USDJPYm", "EURUSD", "EURUSDm"]:
            check_symbol(s)
    mt5.shutdown()
    print("Done.")
