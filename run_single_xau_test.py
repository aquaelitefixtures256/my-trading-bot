#!/usr/bin/env python3
"""
Robust runner for a single XAU backtest that forces a PARAMS dict into the bot module
so the bot uses your chosen parameters even if run_backtest doesn't accept a params kwarg.

Usage:
    python run_single_xau_test.py
You can edit the PARAMS block below (or pass --sym / --days via args if you like).
"""

import importlib.util
import os
import sys
import traceback
from datetime import datetime

BOT_FILENAME = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
SYM = "XAUUSD"
DAYS = 30

# ------------------ EDIT / VERIFY PARAMS HERE ------------------
PARAMS = {
    "signal_thresh": 0.95,
    "atr_thresh": 0.0,
    "max_hold": 20,
    "use_atr_sl": True,
    "sl_atr_mult": 1.8,
    "tp_atr_mult": 3.0,
    "require_dxy": True
}
# ----------------------------------------------------------------

OUT_DIR = "debug_backtest_output"
LOGFILE = os.path.join(OUT_DIR, "run_single_xau_test.log")
os.makedirs(OUT_DIR, exist_ok=True)

def load_module_from_path(path, name="kyoto_bot"):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def inject_params_into_module(mod, symbol, params):
    """
    Try a variety of plausible global names the bot might use to fetch params.
    Also add per-symbol mappings for both 'XAUUSD' and 'XAUUSDm' keys.
    """
    try:
        # simple names
        for nm in ("PARAMS", "params", "DEFAULT_PARAMS", "default_params", "GLOBAL_PARAMS", "global_params"):
            try:
                setattr(mod, nm, params)
            except Exception:
                pass

        # per-symbol maps
        try:
            ps = getattr(mod, "PARAMS_BY_SYMBOL", {}) or {}
        except Exception:
            ps = {}
        ps = dict(ps)  # copy
        # multiple symbol keys in case bot uses different forms
        for k in (symbol, symbol + "m", symbol + "M", symbol.replace("USD","")):
            ps[k] = params
        try:
            setattr(mod, "PARAMS_BY_SYMBOL", ps)
        except Exception:
            pass

        # common alternate names
        for nm in ("params_by_symbol", "per_symbol_params", "PER_SYMBOL_PARAMS", "PER_SYMBOL"):
            try:
                setattr(mod, nm, ps)
            except Exception:
                pass

        # provide a getter if bot expects a function
        try:
            if not hasattr(mod, "get_params_for"):
                setattr(mod, "get_params_for", lambda s: ps.get(s, params))
        except Exception:
            pass

        # also set attribute on v15 module placeholder if present (the script will attach v15)
    except Exception as e:
        # best-effort: don't fail
        print("inject_params_into_module error:", e)

def try_call_run_backtest(bot_mod, v15_mod, symbol, days, params):
    """
    Attempt several calling conventions and injection strategies until one succeeds.
    Return (success_bool, used_signature_str, return_value_or_exception)
    """
    attempts = []
    # 1) direct params kw
    attempts.append(("run_backtest(v15, symbol=..., days=..., params=...)", lambda: bot_mod.run_backtest(v15_mod, symbol=symbol, days=days, params=params)))

    # 2) v15, sym, days, params positional
    attempts.append(("run_backtest(v15, symbol, days, params)", lambda: bot_mod.run_backtest(v15_mod, symbol, days, params)))

    # 3) attempt run_backtest(symbol=..., days=..., params=...) (no v15)
    attempts.append(("run_backtest(symbol=..., days=..., params=...)", lambda: bot_mod.run_backtest(symbol=symbol, days=days, params=params)))

    # 4) attempt run_backtest(sym, days, params) (no v15)
    attempts.append(("run_backtest(sym, days, params)", lambda: bot_mod.run_backtest(symbol, days, params)))

    # 5) try calling after injecting params into bot_mod namespace; prefer v15 signature without params
    attempts.append(("run_backtest(v15, symbol=..., days=...) after injection", lambda: bot_mod.run_backtest(v15_mod, symbol=symbol, days=days)))

    # 6) fallback: run_backtest(symbol, days)
    attempts.append(("run_backtest(symbol, days) fallback", lambda: bot_mod.run_backtest(symbol, days)))

    last_exc = None
    for name, func in attempts:
        try:
            val = func()
            return True, name, val
        except TypeError as te:
            last_exc = te
            # print minimal debug and continue
            #print(f"TypeError for {name}: {te}")
            continue
        except Exception as e:
            # a runtime exception inside the bot; return that (we still succeeded in calling)
            return False, name, e
    return False, "all_attempts_failed", last_exc

def main():
    start_stamp = datetime.utcnow().isoformat()
    with open(LOGFILE, "w", encoding="utf-8") as lf:
        lf.write(f"run_single_xau_test started: {start_stamp}\n")
    try:
        bot = load_module_from_path(BOT_FILENAME, name="kyoto_bot")
    except Exception as e:
        tb = traceback.format_exc()
        print("Failed to load bot module:", e)
        with open(LOGFILE, "a", encoding="utf-8") as lf:
            lf.write("Failed to load bot:\n")
            lf.write(tb)
        return 2

    # load v15 if available
    v15 = None
    try:
        if hasattr(bot, "load_v15_module") and callable(bot.load_v15_module):
            try:
                v15 = bot.load_v15_module()
            except Exception:
                # some bots expose load_v15, some load_v15_module, try alternatives
                try:
                    v15 = getattr(bot, "load_v15")()
                except Exception:
                    v15 = None
        else:
            # attempt alt names
            for loader_name in ("load_v15", "load_v15_impl", "install_v15"):
                loader = getattr(bot, loader_name, None)
                if callable(loader):
                    try:
                        v15 = loader()
                        break
                    except Exception:
                        v15 = None
    except Exception:
        v15 = None

    # Ensure params injection BEFORE call (best-effort)
    inject_params_into_module(bot, SYM, PARAMS)
    if v15 is not None:
        try:
            inject_params_into_module(v15, SYM, PARAMS)
        except Exception:
            pass

    success, signature_used, result = try_call_run_backtest(bot, v15, SYM, DAYS, PARAMS)

    # log results
    with open(LOGFILE, "a", encoding="utf-8") as lf:
        lf.write(f"PARAMS used (script): {repr(PARAMS)}\n")
        lf.write(f"signature_attempted_result: {signature_used}\n")
        lf.write("returned:\n")
        try:
            lf.write(repr(result) + "\n")
        except Exception:
            lf.write("<<unrepr-able return>>\n")
        if not success:
            lf.write("NOTE: call failed or raised inside bot. See traceback below if exception.\n")
            if isinstance(result, Exception):
                lf.write("Exception (repr):\n")
                lf.write(repr(result) + "\n")
                lf.write("Traceback:\n")
                lf.write("".join(traceback.format_exception(None, result, result.__traceback__)))
    # Print summary to console for quick feedback
    print("Loaded bot:", BOT_FILENAME)
    print("PARAMS forced:", PARAMS)
    print("signature used:", signature_used)
    print("returned:", repr(result))
    print("Log written to:", LOGFILE)
    return 0

if __name__ == "__main__":
    sys.exit(main())
