#!/usr/bin/env python3
"""
KYOTO main-system BTCUSD live probe.

Goal:
- Load the main bot file from the same folder when possible.
- Connect to MT5 using the bot's own helper if available.
- Fall back to direct MetaTrader5.initialize() if the helper is missing.
- Send exactly one tiny BTCUSD BUY through the bot's live order path.
- Print the order check and live result.
"""

import os
import sys
import json
import traceback
import importlib.util
from pathlib import Path

SYMBOL = "BTCUSD"
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
        "Main bot file not found. Tried: " + ", ".join(str(p) for p in BOT_CANDIDATES)
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


def resolve_mt5(bot):
    mt5 = getattr(bot, "_mt5", None)
    if mt5 is not None:
        return mt5, "bot._mt5"

    try:
        import MetaTrader5 as mt5  # type: ignore
        return mt5, "direct MetaTrader5 import"
    except Exception as e:
        raise RuntimeError(f"Could not import MetaTrader5: {e!r}")


def connect_mt5(bot, mt5, login_int: int, password: str, server: str):
    helpers = [
        "connect_mt5",
        "init_mt5",
        "mt5_connect",
        "setup_mt5",
        "start_mt5",
    ]
    for name in helpers:
        fn = getattr(bot, name, None)
        if callable(fn):
            try:
                if name == "connect_mt5":
                    ok = fn(login=login_int, password=password, server=server)
                elif name == "init_mt5":
                    ok = fn(login_int, password, server)
                else:
                    ok = fn(login_int, password, server)
                return bool(ok), f"bot.{name}()"
            except TypeError:
                try:
                    ok = fn()
                    return bool(ok), f"bot.{name}() no-arg"
                except Exception:
                    pass
            except Exception as e:
                raise RuntimeError(f"bot.{name}() failed: {e!r}")

    try:
        ok = mt5.initialize(login=login_int, password=password, server=server)
        return bool(ok), "MetaTrader5.initialize()"
    except Exception as e:
        raise RuntimeError(f"MetaTrader5.initialize() failed: {e!r}")


def calc_sl_tp(price: float, is_buy: bool):
    # Simple probe-level SL/TP so the order is well-formed.
    # BTCUSD usually needs wider stops than FX pairs.
    sl_dist = max(price * 0.004, 50.0)
    tp_dist = max(price * 0.006, 75.0)
    if is_buy:
        return price - sl_dist, price + tp_dist
    return price + sl_dist, price - tp_dist


def place_probe_trade(bot, mt5, mapped_symbol: str):
    try:
        mt5.symbol_select(mapped_symbol, True)
    except Exception:
        pass

    info = mt5.symbol_info(mapped_symbol)
    tick = mt5.symbol_info_tick(mapped_symbol)
    print("symbol_info =>", info)
    print("tick        =>", tick)

    if info is None or tick is None:
        raise RuntimeError(f"Symbol info/tick unavailable for {mapped_symbol}")

    price = tick.ask if getattr(tick, "ask", None) and tick.ask > 0 else getattr(tick, "bid", None)
    if price is None or float(price) <= 0:
        raise RuntimeError(f"Invalid price for {mapped_symbol}: {price!r}")

    sl, tp = calc_sl_tp(float(price), is_buy=True)

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": mapped_symbol,
        "volume": LOT,
        "type": mt5.ORDER_TYPE_BUY,
        "price": float(price),
        "sl": float(sl),
        "tp": float(tp),
        "deviation": DEVIATION,
        "magic": MAGIC,
        "comment": COMMENT,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": getattr(info, "filling_mode", 0),
    }

    print("\n6) order_check()")
    check = mt5.order_check(request)
    print("order_check =>", check)
    print("last_error  =>", mt5.last_error())

    print("\n7) LIVE ORDER PROBE THROUGH MAIN SYSTEM")
    result = None

    if callable(getattr(bot, "order_wrapper", None)):
        try:
            result = bot.order_wrapper(mt5, request)
            return result, "bot.order_wrapper"
        except Exception as e:
            print("bot.order_wrapper EXCEPTION =>", repr(e))
            print(traceback.format_exc())

    if callable(getattr(bot, "place_order_mt5", None)):
        try:
            result = bot.place_order_mt5(mt5, request)
            return result, "bot.place_order_mt5"
        except Exception as e:
            print("bot.place_order_mt5 EXCEPTION =>", repr(e))
            print(traceback.format_exc())

    if callable(getattr(bot, "UVXExecutionEngine", None)):
        try:
            engine = bot.UVXExecutionEngine()
            if hasattr(engine, "_mt5"):
                engine._mt5 = mt5
            if hasattr(engine, "mode"):
                engine.mode = "mt5"
            result = engine.market_order(mapped_symbol, "buy", LOT, sl=sl, tp=tp)
            return result, "bot.UVXExecutionEngine.market_order"
        except Exception as e:
            print("UVXExecutionEngine EXCEPTION =>", repr(e))
            print(traceback.format_exc())

    result = mt5.order_send(request)
    return result, "mt5.order_send"


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

    mt5, mt5_source = resolve_mt5(bot)
    print("\n3) MT5 source")
    print(mt5_source)

    print("\n4) Connect using bot helper or direct MT5 fallback")
    ok, how = connect_mt5(bot, mt5, login_int, password, server)
    print("connected =>", ok)
    print("method    =>", how)
    if not ok:
        print("ERROR: MT5 connection failed")
        return 4

    mapped_symbol = SYMBOL
    if callable(getattr(bot, "map_symbol_to_broker", None)):
        try:
            mapped_symbol = bot.map_symbol_to_broker(SYMBOL) or SYMBOL
        except Exception:
            mapped_symbol = SYMBOL
    print("\n5) Symbol mapping")
    print(f"{SYMBOL} -> {mapped_symbol}")

    try:
        result, method = place_probe_trade(bot, mt5, mapped_symbol)
        print("LIVE RESULT METHOD =>", method)
        print("LIVE RESULT        =>", result)
        try:
            print("JSON RESULT =>", json.dumps(result, default=str, indent=2))
        except Exception:
            pass
        print("last_error   =>", mt5.last_error())
    except Exception as e:
        print("LIVE PROBE EXCEPTION =>", repr(e))
        print(traceback.format_exc())
        return 10

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
