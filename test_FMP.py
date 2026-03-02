import os
import requests
from datetime import datetime, timedelta
import json

FMP_KEY = os.getenv("FMP_KEY")

print("FMP key present:", bool(FMP_KEY))

today = datetime.utcnow().date()
frm = (today - timedelta(days=1)).strftime("%Y-%m-%d")
to = (today + timedelta(days=2)).strftime("%Y-%m-%d")

url = "https://site.financialmodelingprep.com/api/v3/economic_calendar"
params = {
    "from": frm,
    "to": to,
    "apikey": FMP_KEY
}

r = requests.get(url, params=params, timeout=10)

print("HTTP:", r.status_code)

try:
    data = r.json()
    print("Events count:", len(data) if isinstance(data, list) else "unknown")
    print(json.dumps(data[:5], indent=2))
except Exception:
    print("Failed to parse JSON")
