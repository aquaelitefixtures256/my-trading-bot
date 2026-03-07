import requests
import os

DASHBOARD_URL = os.getenv("DASHBOARD_URL")
DASHBOARD_KEY = os.getenv("DASHBOARD_KEY")

def push_trade_open(symbol, side, size, price):
    data = {"type":"trade_open", "symbol":symbol, "side":side, "size":size, "price":price}
    requests.post(f"{DASHBOARD_URL}/bot_event", json=data, headers={"Authorization": f"Bearer {DASHBOARD_KEY}"})

def push_trade_close(symbol, side, size, price):
    data = {"type":"trade_close", "symbol":symbol, "side":side, "size":size, "price":price}
    requests.post(f"{DASHBOARD_URL}/bot_event", json=data, headers={"Authorization": f"Bearer {DASHBOARD_KEY}"})

def push_log(level, message):
    data = {"type":"log", "level":level, "message":message}
    requests.post(f"{DASHBOARD_URL}/bot_event", json=data, headers={"Authorization": f"Bearer {DASHBOARD_KEY}"})

def push_analysis(symbol, tech_score, model_score, fund_score, total_score):
    data = {"type":"analysis", "symbol":symbol, "tech":tech_score, "model":model_score, "fund":fund_score, "total":total_score}
    requests.post(f"{DASHBOARD_URL}/bot_event", json=data, headers={"Authorization": f"Bearer {DASHBOARD_KEY}"})

def push_error(message):
    data = {"type":"error", "message":message}
    requests.post(f"{DASHBOARD_URL}/bot_event", json=data, headers={"Authorization": f"Bearer {DASHBOARD_KEY}"})
