import importlib
from datetime import datetime

print("=== VOID BEAST BRAIN TEST ===")
print("Time:", datetime.now())
print()

# import bot
bot = importlib.import_module("voidx2_0")

# fake scores to test calculation
tech_score = 0.6
model_score = 0.3
fundamental_score = 0.5

print("Testing intelligence stack...")
print()

print("TECHNICAL SCORE:", tech_score)
print("MODEL SCORE:", model_score)
print("FUNDAMENTAL SCORE:", fundamental_score)
print()

# simulate total score logic
W_TECH = 0.70
W_MODEL = 0.20
W_FUND = 0.10

total_score = (
    (W_TECH * tech_score) +
    (W_MODEL * model_score) +
    (W_FUND * fundamental_score)
)

print("TOTAL SCORE:", round(total_score, 4))
print()

if total_score > 0.18:
    print("Signal: BUY ZONE")
elif total_score < -0.18:
    print("Signal: SELL ZONE")
else:
    print("Signal: HOLD / NO TRADE")

print()
print("=== TEST COMPLETE ===")
