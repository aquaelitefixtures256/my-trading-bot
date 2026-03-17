#!/usr/bin/env python3
# run_single_xau_test.py
# Call your bot.run_backtest with a specific parameter override for XAU

import importlib.util, os, sys
from datetime import datetime

BOT_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"

def load_bot(path):
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def main():
    print("Loading bot:", BOT_FILE)
    bot = load_bot(BOT_FILE)

    # try to get v15 if available
    v15 = None
    if hasattr(bot, "load_v15_module"):
        try:
            v15 = bot.load_v15_module()
            print("Loaded v15:", getattr(v15,"__name__", v15))
        except Exception as e:
            print("v15 load failed:", e)

    params = {
        # these keys must match what your run_backtest expects; adapt if your bot uses other names
        "signal_thresh": 0.92,   # keep same signal thresh for now
        "atr_thresh": 0.0,       # keep zero here (we'll use atr_at_entry for SL)
        "max_hold": 30,
        # ATR-based SL/TP parameters we will pass in kwargs (bot must accept them or use them)
        "use_atr_sl": True,
        "sl_atr_mult": 1.2,   # SL = sl_atr_mult * ATR
        "tp_atr_mult": 2.5,   # TP = tp_atr_mult * ATR
        "require_dxy": False, # set True to test requiring DXY confirmation
    }

    print("Running run_backtest for XAUUSD with params:", params)
    start = datetime.utcnow().isoformat()
    try:
        # try a few signature variants
        if hasattr(bot, "run_backtest"):
            try:
                out = bot.run_backtest(v15, symbol="XAUUSD", days=30, params=params)
            except TypeError:
                try:
                    out = bot.run_backtest(symbol="XAUUSD", days=30, params=params)
                except TypeError:
                    out = bot.run_backtest("XAUUSD", 30, params)
        else:
            raise RuntimeError("Bot has no run_backtest function")
    except Exception as e:
        print("run_backtest failed:", e)
        raise

    print("Backtest returned:", out)
    print("Check debug_backtest_output/replay_trades_XAUUSDm.csv and debug_backtest_output/replay_trades_XAUUSDm.csv (or debug_backtest_output/replay_trades_XAUUSDm.*) for replay.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
