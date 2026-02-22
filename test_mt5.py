# test_mt5.py
import MetaTrader5 as mt5
print("mt5 imported")
ok = mt5.initialize()
print("initialize ->", ok)
try:
    info = mt5.account_info()
    print("account_info ->", info)
except Exception as e:
    print("account_info error:", e)
print("last_error ->", mt5.last_error())
mt5.shutdown()
