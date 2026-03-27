import MetaTrader5 as mt5
import os

login = int(os.getenv("MT5_LOGIN"))
password = os.getenv("MT5_PASSWORD")
server = os.getenv("MT5_SERVER")

print("initialize:", mt5.initialize())
print("login:", mt5.login(login, password=password, server=server))
print("account:", mt5.account_info())
print("last_error:", mt5.last_error())
