#!/usr/bin/env python3
# test_newsdata.py
import os, json, requests
from datetime import datetime, timedelta

KEY = os.getenv("NEWSDATA_KEY")
print("NEWSDATA_KEY present:", bool(KEY))

q = "bitcoin OR oil OR gold OR iran"
params = {"q": q, "language": "en", "page": 1, "page_size": 50, "apikey": KEY}
url = "https://newsdata.io/api/1/news"

try:
    r = requests.get(url, params=params, timeout=10)
    print("HTTP", r.status_code)
    try:
        j = r.json()
        print("Top-level keys:", list(j.keys()))
        # try common fields:
        for k in ("results", "articles", "data"):
            if k in j:
                print(f"Found '{k}' with {len(j[k]) if isinstance(j[k], list) else 'unknown'} items")
        print("Sample snippet:")
        if "results" in j and isinstance(j["results"], list):
            print(json.dumps(j["results"][:5], indent=2))
        elif "articles" in j and isinstance(j["articles"], list):
            print(json.dumps(j["articles"][:5], indent=2))
        elif isinstance(j, dict) and ("results" not in j and "articles" not in j):
            print("Full JSON top-level preview:", json.dumps(j, indent=2)[:2000])
        else:
            print("Response preview:", str(j)[:1000])
    except Exception:
        print("Response not JSON:", r.text[:2000])
except Exception as e:
    print("Request failed:", repr(e))
