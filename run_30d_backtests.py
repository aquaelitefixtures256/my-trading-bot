# run_30d_backtests.py
import importlib.util, sys, logging, io, traceback
from pathlib import Path

# === MODIFY THIS PATH IF NEEDED ===
UPGRADED_FILE = r"C:\Users\Administrator\OneDrive\Desktop\Muc_universe\KYOTO_INFERNO_V16_fixed-5_upgraded.py"

# load module from file path
spec = importlib.util.spec_from_file_location("upgraded_bot", UPGRADED_FILE)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

# set user-requested modes
bot.AUTO_EXECUTE = True
bot.CONFIG["DRY_RUN"] = False

# load embedded v15 module (if present)
try:
    v15 = bot.load_v15_module()
except Exception as e:
    print("Warning: load_v15_module() failed:", e)
    v15 = None

# pick 6 symbols (change this list if you want different symbols)
symbols = list(bot.CONFIG.get("WATCH_SYMBOLS", []))[:6]
print("Running 30-day backtests for:", symbols)

# capture logs per symbol and save to files
out_dir = Path("/tmp/backtest_results")
out_dir.mkdir(parents=True, exist_ok=True)

root_logger = logging.getLogger()
for sym in symbols:
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    h.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    h.setFormatter(fmt)

    # attach the handler to the bot logger and root logger (non-destructive)
    bot.logger.addHandler(h)
    root_logger.addHandler(h)
    try:
        print(f"--- backtest start: {sym} ---")
        ret = None
        try:
            ret = bot.run_backtest(v15, symbol=sym, days=30)
        except Exception as e:
            traceback.print_exc(file=buf)
        # fetch captured output
        log_output = buf.getvalue()
        # Save a file with raw logs and returned object
        p = out_dir / f"backtest_{sym}_30d.log"
        with p.open("w", encoding="utf-8") as f:
            f.write("RETURN_VALUE:\n")
            f.write(repr(ret) + "\n\n")
            f.write("LOG_OUTPUT:\n")
            f.write(log_output)
        print(f"Saved {p}")
    finally:
        # remove the capture handler cleanly
        try:
            bot.logger.removeHandler(h)
        except Exception:
            pass
        try:
            root_logger.removeHandler(h)
        except Exception:
            pass

print("All backtests finished. Check", out_dir)
