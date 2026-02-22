# test_telegram.py
import os, requests
t = os.environ.get("TELEGRAM_BOT_TOKEN")
c = os.environ.get("TELEGRAM_CHAT_ID")
print("token present?", bool(t), "chat present?", bool(c))
if t and c:
    r = requests.post(f"https://api.telegram.org/bot{t}/sendMessage", json={"chat_id":c,"text":"EC2 test message"})
    print("status", r.status_code, r.text)
else:
    print("env vars missing")
