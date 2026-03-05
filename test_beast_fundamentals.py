import importlib
import datetime

print("=== VOID BEAST FUNDAMENTALS TEST ===")
print("Time:", datetime.datetime.utcnow())
print()

# load the bot
bot = importlib.import_module("voidx2.0")

print("Bot module loaded:", bot.__name__)
print()

# -----------------------------------
# TEST NEWS MODULE
# -----------------------------------

print("---- Testing News Fetch ----")

try:
    news = bot.fetch_newsdata("gold OR oil OR usd")
    print("Articles found:", news.get("count"))

    for i, a in enumerate(news.get("articles", [])[:5], 1):
        print(f"{i}.", a.get("title"))

except Exception as e:
    print("News test error:", e)

print()

# -----------------------------------
# TEST ECONOMIC CALENDAR
# -----------------------------------

print("---- Testing Calendar ----")

try:
    events = bot.fetch_economic_calendar()

    if events:
        print("Calendar events:", len(events))
        print("Sample event:", events[0])
    else:
        print("No events returned")

except Exception as e:
    print("Calendar test error:", e)

print()

# -----------------------------------
# TEST FUNDAMENTAL SCORE
# -----------------------------------

print("---- Testing Fundamental Score ----")

symbols = ["EURUSD","XAUUSD","XAGUSD","BTCUSD"]

for s in symbols:
    try:
        score = bot.fetch_fundamental_score(s)
        print(s, "score =", score)
    except Exception as e:
        print(s, "error:", e)

print()
print("=== TEST COMPLETE ===")
