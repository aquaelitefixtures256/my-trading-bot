import MetaTrader5 as mt5

print("Initializing:", mt5.initialize())

symbols = ["BTCUSD","EURUSD","USDJPY","XAUUSD","USOIL","DXY"]

for s in symbols:
    rates = mt5.copy_rates_from_pos(s, mt5.TIMEFRAME_M1, 0, 10)
    print(s, "bars:", None if rates is None else len(rates))
