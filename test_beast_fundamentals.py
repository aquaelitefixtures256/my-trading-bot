import importlib.util
import datetime
import os

print("=== VOID BEAST FUNDAMENTALS TEST ===")
print("Time:", datetime.datetime.now())
print()

# path to the bot file
bot_path = os.path.join(os.getcwd(), "voidx2.0.py")

# load module from file path
spec = importlib.util.spec_from_file_location("void_beast", bot_path)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

print("Bot module loaded successfully.")
print()

# test symbols
symbols = ["EURUSD", "USOIL", "USDJPY", "XAUUSD"]

for symbol in symbols:
    try:
        if hasattr(bot, "get_fundamental_score"):
            score = bot.get_fundamental_score(symbol)
            print(f"{symbol} fundamental score:", score)
        else:
            print(f"{symbol}: get_fundamental_score() not found in bot.")
    except Exception as e:
        print(f"{symbol} error:", e)

print()
print("=== TEST COMPLETE ===")
