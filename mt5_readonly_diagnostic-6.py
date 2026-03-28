#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import logging
from typing import Any, Dict, Optional

try:
    import MetaTrader5 as mt5
except Exception as exc:
    print("ERROR: MetaTrader5 could not be imported.")
    print(exc)
    raise SystemExit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mt5_readonly_diagnostic")

SYMBOLS = ["BTCUSD", "USOIL", "EURUSD", "USDJPY", "XAUUSD"]

def mask(v: Optional[str]) -> str:
    if not v:
        return "<missing>"
    if len(v) <= 4:
        return "*" * len(v)
    return v[:2] + "***" + v[-2:]

def env() -> Dict[str, Optional[str]]:
    return {
        "MT5_LOGIN": os.getenv("MT5_LOGIN"),
        "MT5_PASSWORD": os.getenv("MT5_PASSWORD"),
        "MT5_SERVER": os.getenv("MT5_SERVER"),
        "MT5_PATH": os.getenv("MT5_PATH"),
    }

def log_env(e: Dict[str, Optional[str]]) -> None:
    logger.info("Environment:")
    logger.info("  MT5_LOGIN   = %s", "<set>" if e["MT5_LOGIN"] else "<missing>")
    logger.info("  MT5_PASSWORD= %s", "<set>" if e["MT5_PASSWORD"] else "<missing>")
    logger.info("  MT5_SERVER  = %s", mask(e["MT5_SERVER"]))
    logger.info("  MT5_PATH    = %s", e["MT5_PATH"] or "<not set>")

def init_mt5(e: Dict[str, Optional[str]]) -> bool:
    logger.info("Initializing MT5...")
    ok = mt5.initialize(path=e["MT5_PATH"]) if e["MT5_PATH"] else mt5.initialize()
    logger.info("mt5.initialize() -> %s", ok)
    if not ok:
        logger.error("last_error=%s", mt5.last_error())
    return bool(ok)

def login_mt5(e: Dict[str, Optional[str]]) -> bool:
    login = e["MT5_LOGIN"]
    password = e["MT5_PASSWORD"]
    server = e["MT5_SERVER"]
    if not login or not password or not server:
        logger.error("Missing login/password/server.")
        return False
    try:
        login_i = int(login)
    except ValueError:
        logger.error("MT5_LOGIN is not an integer.")
        return False
    ok = mt5.login(login=login_i, password=password, server=server)
    logger.info("mt5.login() -> %s", ok)
    if not ok:
        logger.error("last_error=%s", mt5.last_error())
    return bool(ok)

def show_terminal_account() -> None:
    logger.info("account_info() = %s", mt5.account_info())
    logger.info("terminal_info() = %s", mt5.terminal_info())
    logger.info("last_error() = %s", mt5.last_error())

def pick_filling(symbol_info: Any) -> int:
    ORDER_FILLING_FOK = getattr(mt5, "ORDER_FILLING_FOK", 0)
    ORDER_FILLING_IOC = getattr(mt5, "ORDER_FILLING_IOC", 1)
    ORDER_FILLING_RETURN = getattr(mt5, "ORDER_FILLING_RETURN", 2)
    fm = int(getattr(symbol_info, "filling_mode", ORDER_FILLING_RETURN) or ORDER_FILLING_RETURN)
    if fm in (ORDER_FILLING_FOK, ORDER_FILLING_IOC, ORDER_FILLING_RETURN):
        return fm
    return ORDER_FILLING_RETURN

def run_symbol_check(symbol: str) -> None:
    logger.info("=== %s ===", symbol)
    info = mt5.symbol_info(symbol)
    if info is None:
        logger.warning("%s: symbol_info() returned None", symbol)
        return
    sel = mt5.symbol_select(symbol, True)
    logger.info("%s: symbol_select(True) -> %s", symbol, sel)
    logger.info("%s: trade_mode=%s visible=%s digits=%s point=%s vol_min=%s vol_step=%s filling_mode=%s",
                symbol, getattr(info, "trade_mode", None), getattr(info, "visible", None),
                getattr(info, "digits", None), getattr(info, "point", None),
                getattr(info, "volume_min", None), getattr(info, "volume_step", None),
                getattr(info, "filling_mode", None))
    tick = mt5.symbol_info_tick(symbol)
    logger.info("%s: tick=%s", symbol, tick)
    if tick is None:
        logger.warning("%s: no tick available", symbol)
        return
    volume_min = float(getattr(info, "volume_min", 0.01) or 0.01)
    volume_step = float(getattr(info, "volume_step", 0.01) or 0.01)
    volume = max(volume_min, volume_step)
    price = float(getattr(tick, "ask", 0.0) or 0.0)
    if price <= 0:
        logger.warning("%s: invalid ask price", symbol)
        return
    request = {
        "action": getattr(mt5, "TRADE_ACTION_DEAL", 1),
        "symbol": symbol,
        "volume": volume,
        "type": getattr(mt5, "ORDER_TYPE_BUY", 0),
        "price": price,
        "deviation": 20,
        "type_filling": pick_filling(info),
        "type_time": getattr(mt5, "ORDER_TIME_GTC", 0),
        "comment": "readonly_mt5_diagnostic",
    }
    logger.info("%s: order_check request = %s", symbol, request)
    result = mt5.order_check(request)
    logger.info("%s: order_check result = %s", symbol, result)
    logger.info("%s: last_error() = %s", symbol, mt5.last_error())

def main() -> int:
    e = env()
    log_env(e)
    if not init_mt5(e):
        mt5.shutdown()
        return 1
    if not login_mt5(e):
        show_terminal_account()
        mt5.shutdown()
        return 2
    show_terminal_account()
    for s in SYMBOLS:
        run_symbol_check(s)
    mt5.shutdown()
    logger.info("Done. No live trades were sent.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
