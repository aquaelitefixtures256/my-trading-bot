import datetime

print("=== VOID BEAST FUNDAMENTALS FILE TEST ===")
print("Time:", datetime.datetime.now())
print()

symbols = ["EURUSD", "USDJPY", "XAUUSD", "BTCUSD", "USOIL"]

fund_file = "fundamentals_test.txt"

try:
    with open(fund_file, "r") as f:
        data = f.read()
except Exception as e:
    print("ERROR reading fundamentals file:", e)
    exit()

print("Fundamentals file loaded.\n")

for symbol in symbols:
    print("Checking:", symbol)

    if symbol in data:
        print("  ✔ News found for", symbol)

        if "BULLISH" in data:
            print("  Sentiment detected: BULLISH")

        if "BEARISH" in data:
            print("  Sentiment detected: BEARISH")

    else:
        print("  No news for this symbol")

    print()

print("=== TEST COMPLETE ===")
