import importlib
from datetime import datetime

print("=================================")
print("VOID BEAST LIVE MARKET SCAN")
print("=================================")
print("Time:", datetime.now())
print()

# import bot
bot = importlib.import_module("voidx2_0")

symbols = [
    "XAUUSD",
    "XAGUSD",
    "BTCUSD",
    "USOIL",
    "USDJPY",
    "EURUSD"
]

print("Scanning symbols...")
print()

for symbol in symbols:

    try:

        print("=================================")
        print("SYMBOL:", symbol)

        # technical score
        tech = bot.calculate_technical_score(symbol)

        # model score
        model = bot.get_model_score(symbol)

        # fundamentals
        fund = bot.fetch_fundamental_score(symbol)

        print("Technical Score :", round(tech, 4))
        print("Model Score     :", round(model, 4))
        print("Fundamental     :", round(fund, 4))

        total = (
            0.70 * tech +
            0.20 * model +
            0.10 * fund
        )

        print("TOTAL SCORE     :", round(total, 4))

        if total >= 0.18:
            print("TRADE SIGNAL → BUY")
        elif total <= -0.18:
            print("TRADE SIGNAL → SELL")
        else:
            print("NO TRADE ZONE")

        print()

    except Exception as e:
        print("ERROR:", e)
        print()

print("=================================")
print("SCAN COMPLETE")
print("=================================")
