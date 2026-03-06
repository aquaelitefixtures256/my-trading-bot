#!/usr/bin/env python3
"""
VOID Beast Master Controller
Professional orchestration layer that activates all Beast modules
and runs the trading engine continuously.

This controller:
• Runs every cycle
• Calls all protection systems
• Calls threshold gravity
• Calls risk scaling
• Calls regime filters
• Calls the trading engine
• Updates dashboard

Main bot file remains untouched.
"""

import time
import logging
import importlib
import subprocess

# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("VOID_BEAST_CONTROLLER")

# --------------------------------------------------
# MODULE LOADER
# --------------------------------------------------

modules = {}

module_names = [
    "beast_helpers",
    "beast_sentiment",
    "beast_scoring",
    "beast_threshold",
    "beast_risk",
    "beast_protection",
    "beast_dashboard",
    "beast_calendar",
    "beast_symbols",
    "beast_correlation",
    "beast_liquidity",
    "beast_monitor",
    "beast_execution_fix",
    "beast_regime",
    "beast_nfp"
]

def load_modules():
    for name in module_names:
        try:
            modules[name] = importlib.import_module(name)
            logger.info(f"Module loaded: {name}")
        except Exception as e:
            logger.warning(f"Module load failed: {name} -> {e}")

# --------------------------------------------------
# SAFETY SYSTEMS
# --------------------------------------------------

def run_calendar_protection():
    try:
        cal = modules.get("beast_calendar")
        if cal and hasattr(cal, "check_high_impact_news"):
            result = cal.check_high_impact_news()
            if result:
                logger.warning("High impact news detected → trading paused")
                return False
    except Exception as e:
        logger.warning(f"Calendar module error: {e}")

    return True


def run_liquidity_protection():
    try:
        mod = modules.get("beast_liquidity")
        if mod and hasattr(mod, "liquidity_guard"):
            mod.liquidity_guard()
    except Exception as e:
        logger.warning(f"Liquidity protection error: {e}")


def run_correlation_engine():
    try:
        mod = modules.get("beast_correlation")
        if mod and hasattr(mod, "update_correlation_matrix"):
            mod.update_correlation_matrix()
    except Exception as e:
        logger.warning(f"Correlation engine error: {e}")


def run_regime_engine():
    try:
        mod = modules.get("beast_regime")
        if mod and hasattr(mod, "detect_market_regime"):
            regime = mod.detect_market_regime()
            logger.info(f"Market regime: {regime}")
    except Exception as e:
        logger.warning(f"Regime engine error: {e}")


def run_threshold_engine():
    try:
        mod = modules.get("beast_threshold")
        if mod and hasattr(mod, "apply_gravity_and_volatility"):
            new_thr = mod.apply_gravity_and_volatility(0.18, 0.0)
            logger.info(f"Threshold gravity applied → {new_thr:.5f}")
    except Exception as e:
        logger.warning(f"Threshold engine error: {e}")


def run_risk_engine():
    try:
        mod = modules.get("beast_risk")
        if mod and hasattr(mod, "compute_dynamic_risk"):
            risk, mode = mod.compute_dynamic_risk(0,0,0)
            logger.info(f"Risk engine active → {risk} ({mode})")
    except Exception as e:
        logger.warning(f"Risk engine error: {e}")


def run_dashboard_update():
    try:
        dash = modules.get("beast_dashboard")
        if dash and hasattr(dash, "publish_cycle"):
            dash.publish_cycle({
                "controller":"active",
                "status":"running"
            })
    except Exception as e:
        logger.warning(f"Dashboard update error: {e}")

# --------------------------------------------------
# TRADING ENGINE CALL (UPDATED FOR VENV)
# --------------------------------------------------

def run_trading_engine():
    try:
        logger.info("Starting trading engine")
        subprocess.run(
            [r"C:\Users\Administrator\Desktop\Muc_universe\venv_quant\Scripts\python.exe",
             "void_beast_engine.py", "--loop", "--live"],
            check=False
        )
    except Exception as e:
        logger.error(f"Trading engine error: {e}")

# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

def main():

    logger.info("VOID Beast Master Controller starting")

    load_modules()

    while True:

        try:

            logger.info("---- NEW CYCLE START ----")

            # 1. Macro calendar protection
            if not run_calendar_protection():
                time.sleep(60)
                continue

            # 2. Liquidity protection
            run_liquidity_protection()

            # 3. Correlation engine
            run_correlation_engine()

            # 4. Regime detection
            run_regime_engine()

            # 5. Threshold gravity
            run_threshold_engine()

            # 6. Dynamic risk scaling
            run_risk_engine()

            # 7. Run trading engine
            run_trading_engine()

            # 8. Dashboard update
            run_dashboard_update()

            logger.info("Cycle complete")

        except Exception as e:
            logger.error(f"Controller error: {e}")

        # cycle delay
        time.sleep(60)


# --------------------------------------------------

if __name__ == "__main__":
    main()
