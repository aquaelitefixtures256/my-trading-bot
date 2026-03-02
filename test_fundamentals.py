#!/usr/bin/env python3
"""
test_fundamentals.py

Usage:
  - Put this file in the same directory as your bot file (default: Ultra_instinct0.0.py)
  - Ensure your env vars are set: FINNHUB_KEY, NEWSDATA_KEY, ALPHAVANTAGE_KEY
  - Run: python test_fundamentals.py
  - Or:  python test_fundamentals.py /full/path/to/YourBotFile.py
"""
import os, sys, importlib.util, json, traceback

BOT_PATH = sys.argv[1] if len(sys.argv) > 1 else "Ultra_instinct0.0.py"

def load_bot_module(path):
    spec = importlib.util.spec_from_file_location("user_bot_module", path)
    mod = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if loader is None:
        raise RuntimeError("Cannot load module from: " + path)
    loader.exec_module(mod)
    return mod

def safe_call(func, *a, **k):
    try:
        return func(*a, **k)
    except Exception as e:
        return {"__error__": True, "exc": repr(e), "trace": traceback.format_exc()}

def pretty_print(title, obj):
    print("="*60)
    print(title)
    print("-"*60)
    if obj is None:
        print("<no result>")
        return
    if isinstance(obj, (dict, list)):
        try:
            print(json.dumps(obj if len(str(obj)) < 10000 else (obj if isinstance(obj, dict) else obj[:10]), indent=2, default=str)[:10000])
        except Exception:
            print(str(obj)[:10000])
    else:
        print(str(obj))
    print()

def main():
    print("Loading bot module from:", BOT_PATH)
    if not os.path.exists(BOT_PATH):
        print("ERROR: bot file not found at path:", BOT_PATH)
        print("Place this test_fundamentals.py in the same folder as your bot or pass the bot filepath as an argument.")
        return
    mod = load_bot_module(BOT_PATH)
    # functions we expect
    want = ["fetch_fundamental_score", "fetch_newsdata", "fetch_finnhub_calendar", "fetch_alpha_vantage_crypto_intraday"]
    for w in want:
        if not hasattr(mod, w):
            print(f"WARNING: bot module does not expose function `{w}`. Found: {', '.join([n for n in dir(mod) if not n.startswith('_')])}")
    # 1) Test NewsData
    q = "bitcoin OR oil OR gold OR iran"
    print("\n[1] Testing NewsData (HEADLINES)")
    news = safe_call(getattr(mod, "fetch_newsdata", lambda *a, **k: {"count":0,"articles":[]}), q, 5)
    pretty_print("NewsData result (short)", news)
    # 2) Test Finnhub calendar
    print("\n[2] Testing Finnhub (ECON CALENDAR)")
    cal = safe_call(getattr(mod, "fetch_finnhub_calendar", lambda *a, **k: []), 1, 48)
    pretty_print("Finnhub calendar sample", cal if isinstance(cal, (list,dict)) else {"result": cal})
    # 3) Test AlphaVantage crypto intraday
    print("\n[3] Testing Alpha Vantage (CRYPTO INTRADAY for BTC)")
    av = safe_call(getattr(mod, "fetch_alpha_vantage_crypto_intraday", lambda *a, **k: {}), "BTC", "USD")
    pretty_print("AlphaVantage crypto intraday (keys / sample)", (list(av.keys()) if isinstance(av, dict) else str(av)) )
    # 4) Test composite fundamental score for common symbols
    print("\n[4] Testing fetch_fundamental_score (composite)")
    for s in ("BTCUSD", "EURUSD"):
        fs = safe_call(getattr(mod, "fetch_fundamental_score", lambda *a, **k: 0.0), s, 2)
        pretty_print(f"Fundamental score for {s}", fs)
    print("Done. If any step returned HTTP error codes (429/403/etc), check your provider dashboard and env vars.")
    print("Env variables used:")
    print("  FINNHUB_KEY:", bool(os.getenv("FINNHUB_KEY")))
    print("  NEWSDATA_KEY:", bool(os.getenv("NEWSDATA_KEY")))
    print("  ALPHAVANTAGE_KEY:", bool(os.getenv("ALPHAVANTAGE_KEY")))

if __name__ == '__main__':
    main()
