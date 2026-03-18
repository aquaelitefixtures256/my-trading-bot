# debug_sig_check.py
import importlib.util, pprint
BOT_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
spec = importlib.util.spec_from_file_location("kyoto_bot", BOT_FILE)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
print("Bot loaded:", BOT_FILE)
v15 = None
if hasattr(bot, "load_v15_module"):
    try:
        v15 = bot.load_v15_module()
        print("v15 loaded:", getattr(v15, "__name__", v15))
    except Exception as e:
        print("v15 load error:", e)
print("bot has compute_signal?:", hasattr(bot, "compute_signal"))
print("v15 has compute_signal?:", hasattr(v15, "compute_signal") if v15 else False)
# Try both names:
syms = ["BTCUSD", "BTCUSDm"]
for s in syms:
    try:
        price = 1.0
        ctx = {"bars": []}
        if v15 and hasattr(v15, "compute_signal"):
            try:
                out = v15.compute_signal(s, price, ctx)
                print("v15.compute_signal(", s, ") ->", out)
            except Exception as e:
                print("v15.compute_signal(", s, ") raised:", e)
        if hasattr(bot, "compute_signal"):
            try:
                out = bot.compute_signal(s, price, ctx)
                print("bot.compute_signal(", s, ") ->", out)
            except Exception as e:
                print("bot.compute_signal(", s, ") raised:", e)
    except Exception as e:
        print("outer error for symbol", s, e)
