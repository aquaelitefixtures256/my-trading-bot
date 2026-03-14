import MetaTrader5 as mt5

print("Initializing MT5...")

if not mt5.initialize():
    print("MT5 initialization failed")
else:
    print("MT5 initialized")

tick = mt5.symbol_info_tick("BTCUSDm")
print("Tick result:", tick)

mt5.shutdown()
