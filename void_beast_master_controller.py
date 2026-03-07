#!/usr/bin/env python3
"""
VOID Beast Master Controller — Institutional Edition

Advanced orchestration controller.

Features
--------
• Prevents adaptive parameter overwrite
• Stabilized threshold gravity with smoothing
• Flash crash circuit breakers
• Liquidity vacuum protection
• Portfolio volatility targeting
• Module health monitoring
• Engine watchdog protection
• Persistent system state
"""

import time
import logging
import importlib
import subprocess
import json
import os
import datetime

# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("VOID_BEAST_CONTROLLER")

# --------------------------------------------------
# MODULE REGISTRY
# --------------------------------------------------

modules = {}
module_health = {}

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

# --------------------------------------------------
# STATE FILES
# --------------------------------------------------

ADAPT_FILE = "adapt_state.json"
SYSTEM_FILE = "beast_system_state.json"

# --------------------------------------------------
# MODULE LOADER
# --------------------------------------------------

def load_modules():

    for name in module_names:

        try:

            modules[name] = importlib.import_module(name)

            module_health[name] = "ok"

            logger.info(f"Module loaded: {name}")

        except Exception as e:

            module_health[name] = "failed"

            logger.warning(f"Module load failed: {name} -> {e}")

# --------------------------------------------------
# STATE MANAGEMENT
# --------------------------------------------------

def load_json(path, default):

    if not os.path.exists(path):
        return default

    try:
        with open(path,"r") as f:
            return json.load(f)
    except:
        return default


def save_json(path, data):

    try:
        with open(path,"w") as f:
            json.dump(data,f,indent=2)
    except Exception as e:
        logger.warning(f"JSON save error: {e}")

# --------------------------------------------------
# REGIME DETECTION
# --------------------------------------------------

def detect_regime():

    mod = modules.get("beast_regime")

    try:

        if mod and hasattr(mod,"detect_market_regime"):

            regime = mod.detect_market_regime()

            logger.info(f"Market regime → {regime}")

            return regime

    except Exception as e:

        logger.warning(f"Regime detection error: {e}")

    return "normal"

# --------------------------------------------------
# THRESHOLD GRAVITY (SMOOTHED)
# --------------------------------------------------

def compute_threshold(prev_threshold):

    mod = modules.get("beast_threshold")

    try:

        if mod and hasattr(mod,"apply_gravity_and_volatility"):

            raw_thr = mod.apply_gravity_and_volatility(prev_threshold,0)

            smoothed = 0.7 * prev_threshold + 0.3 * raw_thr

            logger.info(f"Threshold smoothed → {smoothed:.5f}")

            return smoothed

    except Exception as e:

        logger.warning(f"Threshold computation error: {e}")

    return prev_threshold

# --------------------------------------------------
# FLASH CRASH DETECTOR
# --------------------------------------------------

def flash_crash_guard():

    mod = modules.get("beast_monitor")

    try:

        if mod and hasattr(mod,"detect_volatility_spike"):

            if mod.detect_volatility_spike():

                logger.warning("Flash crash protection triggered")

                return False

    except Exception as e:

        logger.warning(f"Flash crash monitor error: {e}")

    return True

# --------------------------------------------------
# LIQUIDITY GUARD
# --------------------------------------------------

def liquidity_guard():

    mod = modules.get("beast_liquidity")

    try:

        if mod and hasattr(mod,"liquidity_guard"):
            mod.liquidity_guard()

    except Exception as e:

        logger.warning(f"Liquidity protection error: {e}")

# --------------------------------------------------
# CORRELATION ENGINE
# --------------------------------------------------

def run_correlation():

    mod = modules.get("beast_correlation")

    try:

        if mod and hasattr(mod,"update_correlation_matrix"):
            mod.update_correlation_matrix()

    except Exception as e:

        logger.warning(f"Correlation engine error: {e}")

# --------------------------------------------------
# VOLATILITY TARGETING
# --------------------------------------------------

def compute_volatility_target(risk):

    mod = modules.get("beast_monitor")

    try:

        if mod and hasattr(mod,"estimate_market_volatility"):

            vol = mod.estimate_market_volatility()

            target_vol = 0.02

            if vol > 0:

                risk = risk * (target_vol / vol)

            logger.info(f"Volatility adjusted risk → {risk}")

            return risk

    except Exception as e:

        logger.warning(f"Volatility targeting error: {e}")

    return risk

# --------------------------------------------------
# ENGINE WATCHDOG
# --------------------------------------------------

def run_engine():

    start = time.time()

    try:

        subprocess.run([
            r"C:\Users\Administrator\Desktop\Muc_universe\venv_quant\Scripts\python.exe",
            "void_beast_engine.py",
            "--live"
        ])

    except Exception as e:

        logger.error(f"Engine error: {e}")

    runtime = time.time() - start

    if runtime > 120:

        logger.warning("Engine runtime unusually long")

# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------

def update_dashboard():

    dash = modules.get("beast_dashboard")

    try:

        if dash and hasattr(dash,"publish_cycle"):

            dash.publish_cycle({
                "controller":"active",
                "module_health":module_health
            })

    except Exception as e:

        logger.warning(f"Dashboard error: {e}")

# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

def main():

    logger.info("VOID Beast Controller starting")

    load_modules()

    while True:

        try:

            logger.info("------ NEW CYCLE ------")

            state = load_json(ADAPT_FILE,{"threshold":0.18,"risk":0.002})

            threshold = state["threshold"]
            risk = state["risk"]

            regime = detect_regime()

            if not flash_crash_guard():

                time.sleep(60)
                continue

            liquidity_guard()

            run_correlation()

            threshold = compute_threshold(threshold)

            risk = compute_volatility_target(risk)

            state["threshold"] = threshold
            state["risk"] = risk
            state["last_cycle"] = str(datetime.datetime.utcnow())

            save_json(ADAPT_FILE,state)

            logger.info(f"Active threshold → {threshold}")
            logger.info(f"Active risk → {risk}")

            run_engine()

            update_dashboard()

        except Exception as e:

            logger.error(f"Controller failure: {e}")

        time.sleep(60)


if __name__ == "__main__":
    main()
