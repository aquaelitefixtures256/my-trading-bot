# run_kyoto_launcher.py
# Simple non-invasive launcher for KYOTO_INFERNO bot.
# Place this file beside your KYOTO_INFERNO_V16*.py file and run:
# python -u run_kyoto_launcher.py KYOTO_INFERNO_V16_fixed-5_upgraded.py

import sys
import os
import importlib.util
import logging
import time
import traceback

def configure_logging():
    import logging, sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)], force=True)
    # make sure root logger prints INFO
    logging.getLogger().setLevel(logging.INFO)

def import_module_from_path(path):
    mod_name = os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(mod_name, path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        return module, None
    except Exception as e:
        return None, traceback.format_exc()

def find_and_start(module):
    # Try common startup function names in order
    candidates = ["start_bot", "main", "run", "start"]
    for name in candidates:
        if hasattr(module, name):
            fn = getattr(module, name)
            try:
                logging.info("Calling %s() from %s", name, module.__name__)
                # call without args — most bots use start_bot() pattern
                fn()
                return True
            except TypeError:
                # try calling with no args anyway
                try:
                    fn()
                    return True
                except Exception:
                    logging.exception("Failed to call %s()", name)
                    return False
            except Exception:
                logging.exception("Exception while calling %s()", name)
                return False
    # If nothing found, warn
    logging.warning("No start function found in module. Look for start_bot()/main().")
    return False

def main():
    configure_logging()
    if len(sys.argv) < 2:
        print("Usage: python -u run_kyoto_launcher.py <path-to-your-bot-file.py>")
        sys.exit(1)
    path = sys.argv[1]
    if not os.path.exists(path):
        print(f"Bot file not found: {path}")
        sys.exit(1)
    logging.info("Importing bot module: %s", path)
    mod, err = import_module_from_path(path)
    if err:
        logging.error("Import failed. Traceback:\n%s", err)
        # Save to local log for easier debugging
        with open("kyoto_import_error.log", "w") as f:
            f.write(err)
        logging.error("Wrote traceback to kyoto_import_error.log")
        sys.exit(2)
    logging.info("Module imported successfully: %s", getattr(mod, "__name__", "<unknown>"))
    started = find_and_start(mod)
    if not started:
        logging.error("Could not find or run startup function inside the module.")
        # Print top-level hints: does module define start_bot?
        logging.info("Module attributes: %s", sorted([a for a in dir(mod) if not a.startswith('_')])[:50])

if __name__ == "__main__":
    main()
