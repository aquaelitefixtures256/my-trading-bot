import os
import requests
from datetime import datetime

# ========= CONFIG =========
NEWSDATA_KEY = os.getenv("NEWSDATA_KEY")  # make sure this is set in your terminal
BASE_URL = "https://newsdata.io/api/1/news"

# ========= HELPER =========
def fetch_news(query="bitcoin OR gold OR oil", language="en"):
    if not NEWSDATA_KEY:
        raise ValueError("NEWSDATA_KEY not found in environment variables.")

    params = {
        "apikey": NEWSDATA_KEY,
        "q": query,
        "language": language,
    }

    response = requests.get(BASE_URL, params=params, timeout=15)
    print(f"HTTP Status Code: {response.status_code}")

    if response.status_code != 200:
        print("Error response:")
        print(response.text)
        return None

    return response.json()

# ========= TEST =========
if __name__ == "__main__":
    print("=== NEWS DATA FUNDAMENTALS TEST ===")
    print("Timestamp:", datetime.utcnow())
    print("")

    data = fetch_news()

    if not data:
        print("No data returned.")
        exit()

    articles = data.get("results", [])
    print(f"Articles returned: {len(articles)}")
    print("")

    for i, article in enumerate(articles[:5], 1):
        print(f"{i}. {article.get('title')}")
        print("   Source:", article.get("source_id"))
        print("   Date  :", article.get("pubDate"))
        print("")
