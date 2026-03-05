import os
import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime

# ==========================================================
# BEAST CONFIGURATION
# ==========================================================

SYMBOL = "XAUUSD, XAGUSD, BTCUSD, EURUSD, USDJPY, USOIL"
TIMEFRAME = mt5.TIMEFRAME_M30
ATR_PERIOD = 14

# Threshold configuration
BASE_THRESHOLD = 0.18
MIN_THRESHOLD = 0.12
MAX_THRESHOLD = 0.30

# Gravity pull strength
GRAVITY = 0.20

# Max movement per cycle (prevents fast jumps)
ADAPT_SPEED = 0.02

# Volatility regime multipliers
LOW_VOL_MULT = -0.02
HIGH_VOL_MULT = 0.02

# ==========================================================
# RISK CONFIGURATION
# ==========================================================

BASE_RISK = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MID_RISK  = 0.006
MAX_RISK  = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "0.01"))

# ==========================================================
# CONNECT TO MT5
# ==========================================================

if not mt5.initialize():
    print("MT5 initialization failed")
    quit()

# ==========================================================
# GET MARKET DATA
# ==========================================================

rates = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME, 0, 200)

if rates is None:
    print("Failed to retrieve price data")
    mt5.shutdown()
    quit()

df = pd.DataFrame(rates)

# ==========================================================
# CALCULATE ATR
# ==========================================================

df["high_low"] = df["high"] - df["low"]
df["high_close"] = abs(df["high"] - df["close"].shift())
df["low_close"] = abs(df["low"] - df["close"].shift())

df["tr"] = df[["high_low", "high_close", "low_close"]].max(axis=1)
df["atr"] = df["tr"].rolling(ATR_PERIOD).mean()

current_atr = df["atr"].iloc[-1]
avg_atr = df["atr"].mean()

# ==========================================================
# READ CURRENT THRESHOLD
# ==========================================================

current_threshold = float(os.getenv("CURRENT_THRESHOLD", BASE_THRESHOLD))

# ==========================================================
# DETERMINE VOLATILITY REGIME
# ==========================================================

volatility_adjustment = 0
regime = "NORMAL"

if current_atr < avg_atr * 0.8:
    regime = "LOW VOLATILITY"
    volatility_adjustment = LOW_VOL_MULT

elif current_atr > avg_atr * 1.2:
    regime = "HIGH VOLATILITY"
    volatility_adjustment = HIGH_VOL_MULT

# ==========================================================
# GRAVITY SYSTEM
# ==========================================================

gravity_pull = (BASE_THRESHOLD - current_threshold) * GRAVITY

# ==========================================================
# FINAL THRESHOLD ADJUSTMENT
# ==========================================================

adjustment = gravity_pull + volatility_adjustment

# Limit adaptation speed
if adjustment > ADAPT_SPEED:
    adjustment = ADAPT_SPEED

if adjustment < -ADAPT_SPEED:
    adjustment = -ADAPT_SPEED

new_threshold = current_threshold + adjustment

# Clamp within limits
new_threshold = max(MIN_THRESHOLD, min(MAX_THRESHOLD, new_threshold))

# ==========================================================
# DYNAMIC RISK SCALING ENGINE
# ==========================================================

"""
Risk increases when signal quality is high.

Signal quality proxy = threshold strength
"""

signal_strength = abs(new_threshold)

if signal_strength >= 0.25:
    risk = MAX_RISK
    risk_mode = "MAX RISK"

elif signal_strength >= 0.18:
    risk = MID_RISK
    risk_mode = "MEDIUM RISK"

else:
    risk = BASE_RISK
    risk_mode = "BASE RISK"

# ==========================================================
# EXPORT ENVIRONMENT VARIABLES
# ==========================================================

os.environ["CURRENT_THRESHOLD"] = str(round(new_threshold, 4))
os.environ["RISK_PER_TRADE_PCT"] = str(risk)

# ==========================================================
# OUTPUT
# ==========================================================

print("\n==============================")
print(" VOID BEAST THRESHOLD ENGINE ")
print("==============================")

print("Time:", datetime.now())
print("Symbol:", SYMBOL)

print("\nATR Current :", round(current_atr, 5))
print("ATR Average :", round(avg_atr, 5))

print("\nMarket Regime:", regime)

print("\nPrevious Threshold:", round(current_threshold, 4))
print("Adjustment:", round(adjustment, 4))
print("New Threshold:", round(new_threshold, 4))

print("\nRisk Mode:", risk_mode)
print("Risk Per Trade:", risk)

print("\nEnvironment variables updated:")
print("CURRENT_THRESHOLD =", os.environ["CURRENT_THRESHOLD"])
print("RISK_PER_TRADE_PCT =", os.environ["RISK_PER_TRADE_PCT"])

mt5.shutdown()
