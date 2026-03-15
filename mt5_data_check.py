# mt5_data_check.py
import MetaTrader5 as mt5
from datetime import datetime
symbols = ["BTCUSD", "EURUSD", "USDJPY", "XAUUSD", "USOIL", "DXY"]
print("Initializing MT5...")
ok = mt5.initialize()
print("MT5.initialize() ->", ok, " last_error:", mt5.last_error())
for s in symbols:
    # try as-is
    sel = mt5.symbol_select(s, True)
    sel_m = False
    if not sel:
        sel_m = mt5.symbol_select(s + "m", True)
    resolved = s if sel else (s + "m" if sel_m else None)
    print(f"Symbol: {s}  resolved -> {resolved}  selected? {bool(sel or sel_m)}")
    if resolved:
        try:
            rates = mt5.copy_rates_from_pos(resolved, mt5.TIMEFRAME_M1, 0, 10)
            print("  bars:", None if rates is None else len(rates))
            if rates is not None and len(rates)>0:
                print("  sample bar:", rates[0])
        except Exception as e:
            print("  error fetching rates for", resolved, ":", e)
mt5.shutdown()
print("MT5 shutdown")
