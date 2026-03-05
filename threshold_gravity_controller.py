import os
import time

BASE_THRESHOLD = 0.18
MAX_THRESHOLD = 0.30
MIN_THRESHOLD = 0.12

# gravity strength (how fast it pulls back)
GRAVITY = 0.02

print("VOID BEAST THRESHOLD GRAVITY CONTROLLER ACTIVE")

while True:

    try:
        current = float(os.getenv("CURRENT_THRESHOLD", BASE_THRESHOLD))
    except:
        current = BASE_THRESHOLD

    new_threshold = current

    # pull back if too high
    if current > BASE_THRESHOLD:
        new_threshold = current - GRAVITY

    # push up slightly if too low
    if current < BASE_THRESHOLD:
        new_threshold = current + GRAVITY

    # clamp limits
    new_threshold = max(MIN_THRESHOLD, min(MAX_THRESHOLD, new_threshold))

    os.environ["CURRENT_THRESHOLD"] = str(round(new_threshold, 3))

    print(
        f"Threshold gravity adjustment: {current:.3f} -> {new_threshold:.3f}"
    )

    time.sleep(60)
