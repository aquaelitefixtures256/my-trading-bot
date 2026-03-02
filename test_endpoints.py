#!/usr/bin/env python3
# test_endpoints.py
# Usage: python test_endpoints.py
# Reads FINNHUB_KEY and ALPHAVANTAGE_KEY from environment variables.

import os, sys, json, requests, datetime, traceback

def safe_print_json(prefix, obj, max_chars=2000):
    try:
        s = json.dumps(obj, indent=2, default=str)
    except Exception:
        s = str(obj)
    print(prefix)
    print(s[:max_chars])
    if len(s) > max_chars:
        print("... (truncated)")

def test_finnhub(key):
    print("\n=== Finnhub economic calendar test ===")
    if not key:
        print("FINNHUB_KEY not set in environment. Set FINNHUB_KEY and re-run.")
        return
    try:
        today = datetime.datetime.utcnow().date()
        frm = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        to = (today + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        url = f"https://finnhub.io/api/v1/calendar/economic?from={frm}&to={to}&token={key}"
        r = requests.get(url, timeout=12)
        print("HTTP", r.status_code)
        try:
            j = r.json()
        except Exception:
            j = r.text
        safe_print_json("Finnhub response:", j)
    except Exception as e:
        print("Finnhub call failed:", repr(e))
        print(traceback.format_exc())

def test_alphavantage(key):
    print("\n=== Alpha Vantage crypto intraday test ===")
    if not key:
        print("ALPHAVANTAGE_KEY not set in environment. Set ALPHAVANTAGE_KEY and re-run.")
        return
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "DIGITAL_CURRENCY_INTRADAY",
            "symbol": "BTC",
            "market": "USD",
            "apikey": key
        }
        r = requests.get(url, params=params, timeout=15)
        print("HTTP", r.status_code)
        try:
            j = r.json()
        except Exception:
            j = r.text
        safe_print_json("Alpha Vantage response:", j)
    except Exception as e:
        print("Alpha Vantage call failed:", repr(e))
        print(traceback.format_exc())

def main():
    print("Reading keys from environment variables.")
    fh = os.getenv("FINNHUB_KEY")
    av = os.getenv("ALPHAVANTAGE_KEY")
    print("FINNHUB_KEY present:", bool(fh))
    print("ALPHAVANTAGE_KEY present:", bool(av))
    test_finnhub(fh)
    test_alphavantage(av)
    print("\nDone. Copy the printed output and paste it here if you want me to interpret it.")

if __name__ == "__main__":
    main()
