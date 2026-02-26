import os

print("CONFIRM_AUTO =", repr(os.getenv("CONFIRM_AUTO")))
print("LIVE arg passed? ->", "--live" in __import__("sys").argv)
