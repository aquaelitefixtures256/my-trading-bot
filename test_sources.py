import os
import requests
from datetime import datetime, timedelta
import json

MARKETAUX_KEY = os.getenv("MARKETAUX_KEY")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

print("MarketAux key present:", bool(MARKETAUX_KEY))
print("RapidAPI key present:", bool(RAPIDAPI_KEY))

today = datetime.utcnow().date()
frm = (today - timedelta(days=1)).strftime("%Y-%m-%d")
to = (today + timedelta(days=2)).strftime("%Y-%m-%d")

# -------------------------
# 1️⃣ Test MarketAux
# -------------------------
print("\n=== Testing MarketAux ===")

if MARKETAUX_KEY:
    try:
        url = "https://api.marketaux.com/v1/economic/calendar"
        params = {
            "api_token": MARKETAUX_KEY,
            "start_date": frm,
            "end_date": to
        }
        r = requests.get(url, params=params, timeout=10)
        print("HTTP:", r.status_code)
        data = r.json()
        events = data.get("data", [])
        print("Events found:", len(events))
        print(json.dumps(events[:3], indent=2))
    except Exception as e:
        print("MarketAux error:", e)
else:
    print("No MarketAux key set.")

# -------------------------
# 2️⃣ Test RapidAPI Ultimate Economic Calendar
# -------------------------
print("\n=== Testing RapidAPI Ultimate Economic Calendar ===")

if RAPIDAPI_KEY:
    try:
        url = "https://ultimate-economic-calendar.p.rapidapi.com/calendar"
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "ultimate-economic-calendar.p.rapidapi.com"
        }
        params = {
            "from": frm,
            "to": to
        }
        r = requests.get(url, headers=headers, params=params, timeout=10)
        print("HTTP:", r.status_code)
        try:
            data = r.json()
            print("Events sample:")
            print(json.dumps(data[:3], indent=2))
        except Exception:
            print("Could not parse JSON")
    except Exception as e:
        print("RapidAPI error:", e)
else:
    print("No RapidAPI key set.")

print("\nDone.")
