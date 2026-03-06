import subprocess
import time
import logging
import importlib
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

BOT_FILE = "voidx2_0_final_beast_full-1.py"

MODULES = [
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

logging.info("VOID BEAST CONTROLLER INITIALIZING")

loaded_modules = {}

# Load all modules
for module_name in MODULES:

    try:

        module = importlib.import_module(module_name)

        loaded_modules[module_name] = module

        logging.info(f"{module_name} module active")

    except Exception as e:

        logging.warning(f"{module_name} failed to load: {e}")

# Confirm critical modules
if "beast_calendar" in loaded_modules:
    logging.info("Calendar module active")

if "beast_liquidity" in loaded_modules:
    logging.info("Liquidity module active")

if "beast_correlation" in loaded_modules:
    logging.info("Correlation engine active")

logging.info("All beast modules initialized")

# Start continuous engine loop
while True:

    try:

        logging.info("Launching VOID BEAST core engine")

        process = subprocess.run(
            [sys.executable, BOT_FILE, "--loop", "--live"],
            capture_output=False
        )

        logging.info("Core engine cycle finished")

    except Exception as e:

        logging.error(f"Controller crash: {e}")

    time.sleep(5)
