import MetaTrader5 as mt5

print("Initializing MT5...")
mt5.initialize()

symbol = "EURUSDm"

print("Requesting rates for:", symbol)
rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 50)

if rates is None:
    print("Rates: None")
    print("Count: 0")
else:
    print("Rates sample:", rates[:3])
    print("Count:", len(rates))

mt5.shutdown()
print("MT5 shutdown.")
