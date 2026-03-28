#!/usr/bin/env python3
"""
KYOTO main-system BTCUSD live probe.

What it does:
- Loads your main KYOTO system file from disk
- Uses the system's own connect_mt5() and map_symbol_to_broker() helpers if available
- Sends exactly ONE tiny BTCUSD live order through the same main-system execution layer
- Prints the full response, including `raw`
- Does not touch your main bot file
"""

import os
import sys
import json
import traceback
import importlib.util
from pathlib import Path

SYMBOL = "BTCUSDm"
LOT = 0.01
DEVIATION = 30
MAGIC = 26012601
COMMENT = "KYOTO_MAIN_SYSTEM_BTCUSD_PROBE"

HERE = Path(__file__).resolve().parent
BOT_CANDIDATES = [
    HERE / "KYOTO_INFERNO_V18_TYPE_FIX.py",
    HERE / "KYOTO_INFERNO_V18.py",
    HERE / "KYOTO_INFERNO_V19.py",
    Path.cwd() / "KYOTO_INFERNO_V18_TYPE_FIX.py",
    Path.cwd() / "KYOTO_INFERNO_V18.py",
    Path.cwd() / "KYOTO_INFERNO_V19.py",
]


def locate_bot_file() -> Path:
    seen = set()
    for candidate in BOT_CANDIDATES:
        try:
            resolved = candidate.expanduser().resolve()
        except Exception:
            resolved = candidate.expanduser()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        if resolved.exists():
            return resolved
    raise FileNotFoundError(
        "Main bot file not found. Tried: "
        + ", ".join(str(p) for p in BOT_CANDIDATES)
    )


def load_bot_module():
    bot_path = locate_bot_file()
    spec = importlib.util.spec_from_file_location("kyoto_main_system", str(bot_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load main bot module spec from {bot_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["kyoto_main_system"] = mod
    spec.loader.exec_module(mod)
    return mod, bot_path


def env_get(name: str):
    v = os.getenv(name)
    return v if v not in ("", None) else None


def main() -> int:
    print("1) Loading main KYOTO system...")
    bot, bot_path = load_bot_module()
    print("Loaded:", bot_path.name)
    print("Path  :", bot_path)

    login = env_get("MT5_LOGIN")
    password = env_get("MT5_PASSWORD")
    server = env_get("MT5_SERVER")
    print("\n2) Environment")
    print("MT5_LOGIN   =", login)
    print("MT5_PASSWORD=", "***" if password else None)
    print("MT5_SERVER  =", server)

    if login is None or password is None or server is None:
        print("ERROR: Missing MT5_LOGIN / MT5_PASSWORD / MT5_SERVER.")
        return 2

    try:
        login_int = int(login)
    except Exception:
        print("ERROR: MT5_LOGIN is not an integer:", repr(login))
        return 3

    print("\n3) Connect using main system helper")
    if hasattr(bot, "connect_mt5") and callable(bot.connect_mt5):
        ok = bot.connect_mt5(login=login_int, password=password, server=server)
        print("bot.connect_mt5() =>", ok)
        if not ok:
            print("ERROR: connect_mt5 returned False")
            return 4
    else:
        print("ERROR: main system has no connect_mt5() helper.")
        return 5

    mapped_symbol = SYMBOL
    if hasattr(bot, "map_symbol_to_broker") and callable(bot.map_symbol_to_broker):
        try:
            mapped_symbol = bot.map_symbol_to_broker(SYMBOL)
        except Exception:
            mapped_symbol = SYMBOL
    print("\n4) Symbol mapping")
    print(f"{SYMBOL} -> {mapped_symbol}")

    mt5 = getattr(bot, "_mt5", None)
    if mt5 is None:
        try:
            import MetaTrader5 as mt5  # type: ignore
        except Exception as e:
            print("ERROR: Could not import MetaTrader5:", repr(e))
            return 6

    print("\n5) Symbol/tick check")
    try:
        mt5.symbol_select(mapped_symbol, True)
    except Exception:
        pass

    info = mt5.symbol_info(mapped_symbol)
    tick = mt5.symbol_info_tick(mapped_symbol)
    print("symbol_info =>", info)
    print("tick        =>", tick)

    if info is None or tick is None:
        print("ERROR: Symbol info/tick unavailable for", mapped_symbol)
        return 7

    price = tick.ask if getattr(tick, "ask", None) and tick.ask > 0 else getattr(tick, "bid", None)
    if price is None or float(price) <= 0:
        print("ERROR: Invalid price for", mapped_symbol, ":", price)
        return 8

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": mapped_symbol,
        "volume": LOT,
        "type": mt5.ORDER_TYPE_BUY,
        "price": float(price),
        "deviation": DEVIATION,
        "magic": MAGIC,
        "comment": COMMENT,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": getattr(info, "filling_mode", 0),
    }

    print("\n6) order_check()")
    try:
        check = mt5.order_check(request)
        print("order_check =>", check)
        print("last_error  =>", mt5.last_error())
    except Exception as e:
        print("order_check EXCEPTION =>", repr(e))
        print(traceback.format_exc())
        return 9

    print("\n7) LIVE ORDER PROBE THROUGH MAIN SYSTEM")
    try:
        if hasattr(bot, "UVXExecutionEngine"):
            engine = bot.UVXExecutionEngine()
            try:
                if hasattr(engine, "mode"):
                    engine.mode = "mt5"
            except Exception:
                pass
            result = engine.market_order(mapped_symbol, "buy", LOT)
        elif hasattr(bot, "order_wrapper") and callable(bot.order_wrapper):
            result = bot.order_wrapper(mt5, request)
        else:
            result = mt5.order_send(request)

        print("LIVE RESULT =>", result)
        try:
            print("JSON RESULT =>", json.dumps(result, default=str, indent=2))
        except Exception:
            pass
        print("last_error  =>", mt5.last_error())
    except Exception as e:
        print("LIVE PROBE EXCEPTION =>", repr(e))
        print(traceback.format_exc())
        return 10

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
