#!/usr/bin/env python3
import os, requests, json
from datetime import datetime, timedelta

NEWS_API_KEY = os.getenv("NEWS_API_KEY") or "your_news_api_key_here"
TE_KEY = os.getenv("TRADING_ECONOMICS_KEY") or "your_tradingeconomics_key_here"

def test_news():
    if not NEWS_API_KEY:
        print("No NEWS_API_KEY set in environment.")
        return
    q = "bitcoin OR oil OR gold OR iran"
    url = "https://newsapi.org/v2/everything"
    params = {"q": q, "language": "en", "pageSize": 5, "from": (datetime.utcnow()-timedelta(days=1)).isoformat()+"Z", "apiKey": NEWS_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        print("NewsAPI status:", r.status_code)
        js = r.json()
        print("Total results:", js.get("totalResults"))
        for a in js.get("articles", [])[:5]:
            print("-", a.get("publishedAt"), a.get("source",{}).get("name"), ":", a.get("title"))
    except Exception as e:
        print("NewsAPI fetch failed:", e)

def test_tradingeconomics():
    if not TE_KEY:
        print("No TRADING_ECONOMICS_KEY set.")
        return
    try:
        # sample: next 24h calendar events
        now = datetime.utcnow()
        d1 = (now - timedelta(hours=1)).strftime("%Y-%m-%d")
        d2 = (now + timedelta(hours=48)).strftime("%Y-%m-%d")
        url = f"https://api.tradingeconomics.com/calendar/country/all?c={TE_KEY}&d1={d1}&d2={d2}"
        r = requests.get(url, timeout=10)
        print("TradingEconomics status:", r.status_code)
        try:
            events = r.json()
            print("Sample events count:", len(events) if isinstance(events, list) else "unknown")
            for e in (events[:5] if isinstance(events, list) else []):
                print("-", e.get("date"), e.get("country"), e.get("importance"), e.get("event"))
        except Exception:
            print("No JSON or unexpected format")
    except Exception as e:
        print("TradingEconomics fetch failed:", e)

if __name__ == "__main__":
    test_news()
    print("-"*40)
    test_tradingeconomics()
