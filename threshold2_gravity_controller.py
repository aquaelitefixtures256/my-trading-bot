import time

BASE = 0.18
MIN_T = 0.12
MAX_T = 0.30
GRAVITY = 0.01

FILE = "beast_threshold_state.txt"

print("VOID BEAST GRAVITY CONTROLLER ACTIVE")

while True:
    try:
        with open(FILE, "r") as f:
            current = float(f.read().strip())
    except:
        current = BASE

    new = current

    if current > BASE:
        new = current - GRAVITY
    elif current < BASE:
        new = current + GRAVITY

    new = max(MIN_T, min(MAX_T, new))

    with open(FILE, "w") as f:
        f.write(str(round(new,3)))

    print(f"Gravity pull: {current:.3f} -> {new:.3f}")

    time.sleep(60)
