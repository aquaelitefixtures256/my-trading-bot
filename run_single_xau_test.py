#!/usr/bin/env python3
# run_single_xau_test.py
# Robust runner for a single XAU backtest. Tries multiple call signatures and
# injects common global overrides if the bot doesn't accept a params kwarg.

import importlib.util
import inspect
import sys
import traceback
from datetime import datetime

BOT_FILE = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
SYMBOL = "XAUUSD"
DAYS = 30

# The params we want to test for XAU (tweak these numbers later)
PARAMS = {
    "signal_thresh": 0.92,
    "atr_thresh": 0.0,
    "max_hold": 30,
    "use_atr_sl": True,
    "sl_atr_mult": 1.2,
    "tp_atr_mult": 2.5,
    "require_dxy": False,
}

def load_bot(path):
    spec = importlib.util.spec_from_file_location("kyoto_bot", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def inject_globals(bot_module, symbol, params):
    """
    Try a few common global names the bot code might read for parameter overrides.
    We keep backups of existing values and return a dict to restore them afterwards.
    """
    keys_to_try = [
        "PARAMS", "PARAMS_OVERRIDE", "BACKTEST_PARAMS", "DEFAULT_PARAMS",
        "OVERRIDE_PARAMS", "SYMBOL_PARAMS", "SYMBOL_DEFAULTS", "GLOBAL_PARAMS"
    ]
    backup = {}
    for k in keys_to_try:
        if hasattr(bot_module, k):
            backup[k] = getattr(bot_module, k)
        try:
            # If the module expects per-symbol dict, set a mapping
            setattr(bot_module, k, {symbol: params})
        except Exception:
            try:
                setattr(bot_module, k, params)
            except Exception:
                pass
    # also set a dedicated per-symbol mapping name (common pattern)
    try:
        if not hasattr(bot_module, "SYMBOL_PARAMS_MAP"):
            bot_module.SYMBOL_PARAMS_MAP = {}
        backup["SYMBOL_PARAMS_MAP"] = dict(getattr(bot_module, "SYMBOL_PARAMS_MAP", {}))
        bot_module.SYMBOL_PARAMS_MAP[symbol] = params
    except Exception:
        pass
    return backup

def restore_globals(bot_module, backup):
    for k, v in backup.items():
        try:
            setattr(bot_module, k, v)
        except Exception:
            try:
                delattr(bot_module, k)
            except Exception:
                pass

def try_call_run_backtest(bot, v15, symbol, days, params):
    """
    Inspect bot.run_backtest signature and attempt a number of call patterns.
    Returns the (result, method_string, exception_if_any)
    """
    fn = getattr(bot, "run_backtest", None)
    if fn is None or not callable(fn):
        raise RuntimeError("Bot module has no callable run_backtest function")

    sig = None
    try:
        sig = inspect.signature(fn)
    except Exception:
        pass

    tried = []
    exceptions = []

    # helper to attempt a call
    def attempt(args=None, kwargs=None, desc=None):
        try:
            args = args or []
            kwargs = kwargs or {}
            tried.append((desc, args, kwargs))
            result = fn(*args, **kwargs)
            return ("ok", desc, result, None)
        except Exception as e:
            tb = traceback.format_exc()
            exceptions.append((desc, tb))
            return ("err", desc, None, tb)

    # Strategies ordered by likelihood (most conservative first)
    # 1) Named: v15, symbol, days, params
    if sig and ("v15" in sig.parameters or "v15_module" in sig.parameters or list(sig.parameters.keys())[:1] in [["v15"], ["v15_module"]]):
        res = attempt(args=[v15], kwargs={"symbol": symbol, "days": days, "params": params},
                      desc="v15 + symbol(days) + params")
        if res[0] == "ok":
            return res

    # 2) Named: v15, symbol, days (no params)
    if sig:
        res = attempt(args=[v15], kwargs={"symbol": symbol, "days": days},
                      desc="v15 + symbol + days (no params)")
        if res[0] == "ok":
            return res

    # 3) Named: symbol, days, params
    res = attempt(kwargs={"symbol": symbol, "days": days, "params": params},
                  desc="symbol + days + params (named)")
    if res[0] == "ok":
        return res

    # 4) Named: symbol, days (no params)
    res = attempt(kwargs={"symbol": symbol, "days": days}, desc="symbol + days (named)")
    if res[0] == "ok":
        return res

    # 5) Positionals: (v15, symbol, days)
    res = attempt(args=[v15, symbol, days], desc="positional (v15, symbol, days)")
    if res[0] == "ok":
        return res

    # 6) Positionals: (symbol, days)
    res = attempt(args=[symbol, days], desc="positional (symbol, days)")
    if res[0] == "ok":
        return res

    # 7) Extremely permissive: try calling with only v15
    res = attempt(args=[v15], desc="positional (v15 only)")
    if res[0] == "ok":
        return res

    # If nothing succeeded, return errors
    return ("all_failed", tried, exceptions, None)

def main():
    print("Loading bot:", BOT_FILE)
    bot = load_bot(BOT_FILE)

    # try to load v15 adapter if available (non-fatal)
    v15 = None
    try:
        if hasattr(bot, "load_v15_module"):
            try:
                v15 = bot.load_v15_module()
                print("Loaded v15:", getattr(v15, "__name__", v15))
            except Exception as e:
                print("v15 loader present but failed:", e)
        else:
            # try common alternative names
            for name in ("load_v15", "load_v15_impl"):
                loader = getattr(bot, name, None)
                if callable(loader):
                    try:
                        v15 = loader()
                        print(f"Loaded v15 via {name}:", getattr(v15, "__name__", v15))
                        break
                    except Exception as e:
                        print(f"{name} loader failed:", e)
    except Exception as e:
        print("v15 load attempt error:", e)

    print("Preparing parameter injection (non-destructive).")
    backup = inject_globals(bot, SYMBOL, PARAMS)

    print("Attempting to call run_backtest with robust strategy. Time:", datetime.utcnow().isoformat())
    result = try_call_run_backtest(bot, v15, SYMBOL, DAYS, PARAMS)

    # restore globals after attempt
    restore_globals(bot, backup)

    # Print results clearly
    if result[0] == "ok":
        desc = result[1]
        out = result[2]
        print("SUCCESS — run_backtest call succeeded using method:", desc)
        print("Returned value (repr):", repr(out))
        # friendly guidance where replay CSVs usually end up
        print("\nIf replay CSVs were produced, check the folder: debug_backtest_output/")
        return 0
    else:
        print("ALL ATTEMPTS FAILED. Summary of attempts and exceptions follows:\n")
        tried = result[1]
        excs = result[2]
        print("Tried call patterns (in order):")
        for t in tried:
            desc, args, kwargs = t
            print(" -", desc, "args_len=", len(args), "kwargs_keys=", list(kwargs.keys()))
        print("\nExceptions captured:")
        for name, tb in excs:
            print("=== attempt:", name, "===\n", tb)
        print("\nLast exception details printed above. Please paste them here so I can adapt the call pattern.")
        return 2

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
