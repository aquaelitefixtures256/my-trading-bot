#!/usr/bin/env python3
# test_calendar_now.py
# Run: python test_calendar_now.py
# Requires MARKETAUX_KEY and RAPIDAPI_KEY in environment.

import os
import requests
import json
from datetime import datetime, timedelta, timezone

MARKETAUX_KEY = os.getenv("MARKETAUX_KEY")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

def iso_dates_window(days_before=1, days_after=2):
    today = datetime.utcnow().date()
    frm = (today - timedelta(days=days_before)).strftime("%Y-%m-%d")
    to = (today + timedelta(days=days_after)).strftime("%Y-%m-%d")
    return frm, to

def test_marketaux():
    print("\n=== MarketAux test ===")
    print("MARKETAUX_KEY present:", bool(MARKETAUX_KEY))
    if not MARKETAUX_KEY:
        print("SKIP MarketAux: MARKETAUX_KEY not set.")
        return
    frm, to = iso_dates_window()
    url = "https://api.marketaux.com/v1/economic/calendar"
    params = {"api_token": MARKETAUX_KEY, "start_date": frm, "end_date": to}
    try:
        r = requests.get(url, params=params, timeout=12)
        print("HTTP status:", r.status_code)
        try:
            j = r.json()
        except Exception:
            print("MarketAux: response not JSON")
            print(r.text[:1000])
            return
        # MarketAux commonly returns {"data": [...]} or similar
        events = j.get("data") or j.get("result") or j.get("events") or j
        if isinstance(events, dict):
            # try to locate list inside
            for k in ("data","result","events"):
                if k in events and isinstance(events[k], list):
                    events = events[k]; break
        count = len(events) if isinstance(events, list) else "unknown"
        print("Events count:", count)
        try:
            print("Sample (first 5):")
            print(json.dumps(events[:5], indent=2, default=str))
        except Exception:
            print("Could not pretty-print events")
    except Exception as e:
        print("MarketAux request failed:", repr(e))

def parse_rapid_event(e):
    # normalize one event dict from RapidAPI tradingview path
    out = {}
    out["date"] = e.get("date")
    out["indicator"] = e.get("indicator") or e.get("title") or ""
    out["country"] = (e.get("country") or "").upper()
    try:
        out["importance"] = int(e.get("importance")) if e.get("importance") is not None else None
    except Exception:
        out["importance"] = None
    out["actual"] = e.get("actual")
    out["forecast"] = e.get("forecast")
    out["previous"] = e.get("previous")
    out["id"] = e.get("id")
    return out

def test_rapidapi_tradingview(countries=None):
    print("\n=== RapidAPI Ultimate Economic Calendar (tradingview) test ===")
    print("RAPIDAPI_KEY present:", bool(RAPIDAPI_KEY))
    if not RAPIDAPI_KEY:
        print("SKIP RapidAPI: RAPIDAPI_KEY not set.")
        return
    frm, to = iso_dates_window()
    url = "https://ultimate-economic-calendar.p.rapidapi.com/economic-events/tradingview"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "ultimate-economic-calendar.p.rapidapi.com"
    }
    params = {"from": frm, "to": to}
    if countries:
        params["countries"] = countries
    try:
        r = requests.get(url, headers=headers, params=params, timeout=12)
        print("HTTP status:", r.status_code)
        try:
            j = r.json()
        except Exception:
            print("RapidAPI: response not JSON")
            print(r.text[:1000])
            return
        # provider returns {"result": [...], "status":"ok"} per your sample
        raw_list = j.get("result") or j.get("data") or j or []
        if isinstance(raw_list, dict):
            # attempt to find embedded list
            for k in ("result","data","events"):
                if k in raw_list and isinstance(raw_list[k], list):
                    raw_list = raw_list[k]; break
        count = len(raw_list) if isinstance(raw_list, list) else "unknown"
        print("Events count:", count)
        sample = []
        if isinstance(raw_list, list):
            for e in raw_list[:8]:
                sample.append(parse_rapid_event(e))
        print("Sample (first 8 normalized):")
        print(json.dumps(sample, indent=2, default=str))
    except Exception as e:
        print("RapidAPI request failed:", repr(e))

def main():
    print("Test started at (UTC):", datetime.now(timezone.utc).isoformat())
    test_marketaux()
    # test RapidAPI for US and DE (adjust countries string if you want other codes)
    test_rapidapi_tradingview(countries="US,DE")
    print("\nTest finished.")

if __name__ == "__main__":
    main()
