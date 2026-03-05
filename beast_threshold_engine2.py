import os
import MetaTrader5 as mt5
import pandas as pd
import numpy as np

print("\nVOID BEAST THRESHOLD ENGINE START\n")

# ---------------------------------------------------
# USER SYMBOL LIST
# ---------------------------------------------------

TARGET_SYMBOLS = [
    "XAUUSDm",
    "XAGUSDm",
    "BTCUSDm",
    "USDJPYm",
    "USOILm",
    "EURUSDm"
]

TIMEFRAME = mt5.TIMEFRAME_M30
ATR_PERIOD = 14

# ---------------------------------------------------
# THRESHOLD SYSTEM
# ---------------------------------------------------

BASE_THRESHOLD = 0.18
MIN_THRESHOLD = 0.12
MAX_THRESHOLD = 0.30

GRAVITY = 0.25
ADAPT_SPEED = 0.015

# ---------------------------------------------------
# RISK ENGINE
# ---------------------------------------------------

BASE_RISK = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MID_RISK = 0.006
MAX_RISK = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "0.01"))

risk = BASE_RISK

# ---------------------------------------------------
# SIGNAL QUALITY FILTER
# ---------------------------------------------------

MAX_SPREAD = 50
VOL_SPIKE_MULT = 2.5
FLASH_CRASH_MOVE = 0.03

# ---------------------------------------------------
# MT5 INIT
# ---------------------------------------------------

if not mt5.initialize():
    print("MT5 failed to initialize")
    quit()

# ---------------------------------------------------
# DETECT SYMBOL SUFFIX
# ---------------------------------------------------

all_symbols = mt5.symbols_get()

def find_symbol(base):

    for s in all_symbols:
        if base in s.name:
            return s.name

    return None

# ---------------------------------------------------
# START THRESHOLD
# ---------------------------------------------------

current_threshold = float(os.getenv("CURRENT_THRESHOLD", BASE_THRESHOLD))

print("Starting Threshold:", current_threshold)

# ---------------------------------------------------
# PROCESS SYMBOLS
# ---------------------------------------------------

for base in TARGET_SYMBOLS:

    symbol = find_symbol(base)

    if symbol is None:
        print(base, "not found in broker")
        continue

    print("\nSymbol:", symbol)

    if not mt5.symbol_select(symbol, True):
        print("Failed selecting", symbol)
        continue

    info = mt5.symbol_info(symbol)

    if info is None:
        print("Symbol info missing")
        continue

    # ------------------------------------------
    # SPREAD FILTER
    # ------------------------------------------

    if info.spread > MAX_SPREAD:
        print("SQF BLOCK: spread too large")
        continue

    # ------------------------------------------
    # PRICE DATA
    # ------------------------------------------

    rates = mt5.copy_rates_from_pos(symbol, TIMEFRAME, 0, 200)

    if rates is None or len(rates) < 50:
        print("Price data unavailable")
        continue

    df = pd.DataFrame(rates)

    # ------------------------------------------
    # ATR
    # ------------------------------------------

    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift()),
            abs(df["low"] - df["close"].shift())
        )
    )

    df["atr"] = df["tr"].rolling(ATR_PERIOD).mean()

    atr_now = df["atr"].iloc[-1]
    atr_avg = df["atr"].mean()

    if np.isnan(atr_now):
        print("ATR unavailable")
        continue

    # ------------------------------------------
    # FLASH CRASH PROTECTION
    # ------------------------------------------

    move = abs(df["close"].iloc[-1] - df["close"].iloc[-2]) / df["close"].iloc[-2]

    if move > FLASH_CRASH_MOVE:
        print("FLASH CRASH PROTECTION TRIGGERED")
        continue

    # ------------------------------------------
    # VOLATILITY REGIME
    # ------------------------------------------

    adjustment = 0

    if atr_now > atr_avg * VOL_SPIKE_MULT:

        print("VOL SPIKE DETECTED")
        adjustment = 0.02

    elif atr_now < atr_avg * 0.7:

        print("LOW VOL REGIME")
        adjustment = -0.02

    # ------------------------------------------
    # THRESHOLD GRAVITY
    # ------------------------------------------

    gravity = (BASE_THRESHOLD - current_threshold) * GRAVITY

    change = gravity + adjustment

    change = max(-ADAPT_SPEED, min(ADAPT_SPEED, change))

    new_threshold = current_threshold + change

    new_threshold = max(MIN_THRESHOLD, min(MAX_THRESHOLD, new_threshold))

    print("Threshold before:", round(current_threshold,4))
    print("Threshold after:", round(new_threshold,4))

    current_threshold = new_threshold

    # ------------------------------------------
    # DYNAMIC RISK
    # ------------------------------------------

    strength = abs(new_threshold)

    if strength >= 0.25:

        risk = MAX_RISK
        mode = "MAX"

    elif strength >= 0.18:

        risk = MID_RISK
        mode = "MEDIUM"

    else:

        risk = BASE_RISK
        mode = "BASE"

    print("Risk Mode:", mode)
    print("Risk:", risk)

# ---------------------------------------------------
# EXPORT VARIABLES
# ---------------------------------------------------

os.environ["CURRENT_THRESHOLD"] = str(round(current_threshold,4))
os.environ["RISK_PER_TRADE_PCT"] = str(risk)

print("\nFINAL THRESHOLD:", os.environ["CURRENT_THRESHOLD"])
print("FINAL RISK:", os.environ["RISK_PER_TRADE_PCT"])

mt5.shutdown()
