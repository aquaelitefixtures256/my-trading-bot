import os
import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime

# ==========================================================
# SYMBOLS YOU TRADE
# ==========================================================

SYMBOLS = [
    "XAUUSD",
    "XAGUSD",
    "BTCUSD",
    "USDJPY",
    "USOIL",
    "EURUSD"
]

TIMEFRAME = mt5.TIMEFRAME_M30
ATR_PERIOD = 14

# ==========================================================
# THRESHOLD CONFIG
# ==========================================================

BASE_THRESHOLD = 0.18
MIN_THRESHOLD = 0.12
MAX_THRESHOLD = 0.30

GRAVITY = 0.20
ADAPT_SPEED = 0.02

LOW_VOL_MULT = -0.02
HIGH_VOL_MULT = 0.02

# ==========================================================
# RISK CONFIG
# ==========================================================

BASE_RISK = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MID_RISK = 0.006
MAX_RISK = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "0.01"))

# ==========================================================
# SIGNAL QUALITY FILTER LIMITS
# ==========================================================

MAX_SPREAD_MULTIPLIER = 2.5
MAX_VOL_SPIKE = 2.0
UNSTABLE_MOVE_PCT = 0.015

# ==========================================================
# START
# ==========================================================

print("\n===================================")
print(" VOID BEAST THRESHOLD ENGINE START ")
print("===================================\n")

if not mt5.initialize():
    print("MT5 initialization failed")
    quit()

current_threshold = float(os.getenv("CURRENT_THRESHOLD", BASE_THRESHOLD))

print("Starting Threshold:", current_threshold)
print("Base Threshold:", BASE_THRESHOLD)

# ==========================================================
# PROCESS EACH SYMBOL
# ==========================================================

for symbol in SYMBOLS:

    print("\n----------------------------------")
    print("Symbol:", symbol)

    # ensure symbol available
    if not mt5.symbol_select(symbol, True):
        print("Symbol selection failed")
        continue

    symbol_info = mt5.symbol_info(symbol)

    if symbol_info is None:
        print("Symbol info unavailable")
        continue

    # ======================================================
    # SPREAD CHECK
    # ======================================================

    spread = symbol_info.spread
    typical_spread = spread

    if spread > typical_spread * MAX_SPREAD_MULTIPLIER:
        print("SQF BLOCK: Spread spike detected")
        continue

    # ======================================================
    # GET PRICE DATA
    # ======================================================

    rates = mt5.copy_rates_from_pos(symbol, TIMEFRAME, 0, 200)

    if rates is None or len(rates) < 50:
        print("Failed to retrieve sufficient price data")
        continue

    df = pd.DataFrame(rates)

    # ======================================================
    # ATR CALCULATION
    # ======================================================

    df["high_low"] = df["high"] - df["low"]
    df["high_close"] = abs(df["high"] - df["close"].shift())
    df["low_close"] = abs(df["low"] - df["close"].shift())

    df["tr"] = df[["high_low", "high_close", "low_close"]].max(axis=1)
    df["atr"] = df["tr"].rolling(ATR_PERIOD).mean()

    current_atr = df["atr"].iloc[-1]
    avg_atr = df["atr"].mean()

    if pd.isna(current_atr):
        print("ATR unavailable")
        continue

    # ======================================================
    # VOLATILITY SPIKE FILTER
    # ======================================================

    if current_atr > avg_atr * MAX_VOL_SPIKE:
        print("SQF BLOCK: Volatility spike")
        continue

    # ======================================================
    # SIGNAL STABILITY CHECK
    # ======================================================

    last_close = df["close"].iloc[-1]
    prev_close = df["close"].iloc[-5]

    price_move = abs(last_close - prev_close) / prev_close

    if price_move > UNSTABLE_MOVE_PCT:
        print("SQF BLOCK: Unstable price move")
        continue

    # ======================================================
    # DETERMINE MARKET REGIME
    # ======================================================

    regime = "NORMAL"
    volatility_adjustment = 0

    if current_atr < avg_atr * 0.8:
        regime = "LOW VOL"
        volatility_adjustment = LOW_VOL_MULT

    elif current_atr > avg_atr * 1.2:
        regime = "HIGH VOL"
        volatility_adjustment = HIGH_VOL_MULT

    # ======================================================
    # GRAVITY SYSTEM
    # ======================================================

    gravity_pull = (BASE_THRESHOLD - current_threshold) * GRAVITY

    adjustment = gravity_pull + volatility_adjustment

    # limit adaptation speed
    adjustment = max(-ADAPT_SPEED, min(ADAPT_SPEED, adjustment))

    new_threshold = current_threshold + adjustment

    # clamp
    new_threshold = max(MIN_THRESHOLD, min(MAX_THRESHOLD, new_threshold))

    # ======================================================
    # DYNAMIC RISK ENGINE
    # ======================================================

    strength = abs(new_threshold)

    if strength >= 0.25:
        risk = MAX_RISK
        risk_mode = "MAX"

    elif strength >= 0.18:
        risk = MID_RISK
        risk_mode = "MEDIUM"

    else:
        risk = BASE_RISK
        risk_mode = "BASE"

    # ======================================================
    # OUTPUT
    # ======================================================

    print("ATR:", round(current_atr, 5))
    print("ATR avg:", round(avg_atr, 5))
    print("Regime:", regime)

    print("Threshold before:", round(current_threshold, 4))
    print("Adjustment:", round(adjustment, 4))
    print("Threshold after:", round(new_threshold, 4))

    print("Risk Mode:", risk_mode)
    print("Risk:", risk)

    current_threshold = new_threshold

# ==========================================================
# EXPORT ENVIRONMENT VARIABLES
# ==========================================================

os.environ["CURRENT_THRESHOLD"] = str(round(current_threshold, 4))
os.environ["RISK_PER_TRADE_PCT"] = str(risk)

print("\n===================================")
print(" FINAL THRESHOLD:", os.environ["CURRENT_THRESHOLD"])
print(" FINAL RISK:", os.environ["RISK_PER_TRADE_PCT"])
print("===================================\n")

mt5.shutdown()
