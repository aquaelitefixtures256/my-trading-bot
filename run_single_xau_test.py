#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust single-symbol XAU run_backtest runner.

Saves debug output to debug_backtest_output/run_single_xau_test.log and prints the best successful call.
Designed to tolerate different bot.run_backtest signatures.

Usage:
    python run_single_xau_test.py
"""

import importlib.util
import os
import sys
import traceback
from datetime import datetime

# ---------- Edit/confirm these if your filenames differ ----------
BOT_FILENAME = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
SYMBOL = "XAUUSD"
DAYS = 30
OUT_DIR = "debug_backtest_output"
LOG_FILE = os.path.join(OUT_DIR, "run_single_xau_test.log")
# ----------------------------------------------------------------

# PARAMS for single-XAU test — replaced as you requested
PARAMS = {
    "signal_thresh": 0.95,    # raise threshold to reduce noisy entries
    "atr_thresh": 0.0,
    "max_hold": 30,
    "use_atr_sl": True,
    "sl_atr_mult": 1.5,       # slightly wider SL (was 1.2)
    "tp_atr_mult": 2.5,       # keep TP multiplier
    "require_dxy": False      # leave off for now; try later
}

def safe_makedirs(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def load_bot(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Bot file not found: {path}")
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def try_run_backtest(bot, v15, symbol, days, params, logf):
    """
    Try several plausible run_backtest signatures until one succeeds.
    Returns tuple(success_bool, used_signature_str, return_value or exception)
    """
    attempts = [
        ("v15_symbol_days_params", lambda: bot.run_backtest(v15, symbol=symbol, days=days, params=params)),
        ("v15_symbol_days",        lambda: bot.run_backtest(v15, symbol=symbol, days=days)),
        ("symbol_days_params",     lambda: bot.run_backtest(symbol=symbol, days=days, params=params)),
        ("symbol_days",            lambda: bot.run_backtest(symbol=symbol, days=days)),
        ("positional_sym_days_params", lambda: bot.run_backtest(symbol, days, params)),
        ("positional_sym_days",    lambda: bot.run_backtest(symbol, days)),
        ("single_symbol",          lambda: bot.run_backtest(symbol)),
        ("no_args",                lambda: bot.run_backtest()),
    ]

    last_exc = None
    for name, fn in attempts:
        try:
            print(f"Trying signature: {name}", file=logf)
            sys.stdout.write(f"Trying signature: {name} ... ")
            sys.stdout.flush()
            result = fn()
            print("OK", file=logf)
            print("OK")
            return True, name, result
        except TypeError as e:
            last_exc = e
            tb = traceback.format_exc()
            print(f"TypeError for {name}: {e}", file=logf)
            # print short to stdout
            print("TypeError")
            continue
        except Exception as e:
            last_exc = e
            tb = traceback.format_exc()
            print(f"Exception for {name}: {e}\n{tb}", file=logf)
            print("Exception")
            continue

    return False, "all_attempts_failed", last_exc

def main():
    safe_makedirs(OUT_DIR)
    with open(LOG_FILE, "a", encoding="utf-8") as logf:
        start = datetime.utcnow().isoformat()
        print("="*40, file=logf)
        print(f"Run start: {start}", file=logf)
        print("="*40, file=logf)

        try:
            bot = load_bot(BOT_FILENAME)
            print(f"Loaded bot: {BOT_FILENAME}", file=logf)
            print(f"Loaded bot: {BOT_FILENAME}")
        except Exception as e:
            tb = traceback.format_exc()
            print("Failed to load bot module:", e, file=logf)
            print(tb, file=logf)
            print("Failed to load bot. See log:", LOG_FILE)
            return 2

        # try to load v15 adapter if available
        v15 = None
        try:
            loader = None
            if hasattr(bot, "load_v15_module") and callable(bot.load_v15_module):
                loader = bot.load_v15_module
            elif hasattr(bot, "load_v15") and callable(bot.load_v15):
                loader = bot.load_v15
            elif hasattr(bot, "load_v15_impl") and callable(bot.load_v15_impl):
                loader = bot.load_v15_impl

            if loader is not None:
                try:
                    v15 = loader()
                    print("Loaded v15 adapter:", getattr(v15, "__name__", repr(v15)), file=logf)
                    print("Loaded v15 adapter:", getattr(v15, "__name__", repr(v15)))
                except Exception as e:
                    print("v15 loader threw:", file=logf)
                    print(traceback.format_exc(), file=logf)
                    v15 = None
            else:
                print("No v15 loader found in bot module; continuing with v15=None", file=logf)
                print("No v15 loader found; continuing with v15=None")
        except Exception as e:
            print("Exception while attempting to load v15:", e, file=logf)
            print(traceback.format_exc(), file=logf)

        # finally try running backtest with robust signatures
        ok, sig, out = try_run_backtest(bot, v15, SYMBOL, DAYS, PARAMS, logf)

        print("\n=== RESULT ===", file=logf)
        print(f"success: {ok}", file=logf)
        print(f"signature_used: {sig}", file=logf)
        print(f"returned: {repr(out)}", file=logf)
        print("\nDetailed traceback / error (if any):", file=logf)
        if not ok:
            print(repr(out), file=logf)
            print(traceback.format_exc(), file=logf)

        # Also print to stdout summary
        print("\n=== run_single_xau_test summary ===")
        print("success:", ok)
        print("signature_used:", sig)
        print("returned:", repr(out))
        print("log file:", LOG_FILE)
        return 0

if __name__ == "__main__":
    sys.exit(main())
