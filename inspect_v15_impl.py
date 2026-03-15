# inspect_v15_impl.py
import importlib.util, inspect, pprint
MODULE_PATH = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"

spec = importlib.util.spec_from_file_location("bot", MODULE_PATH)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

print("Loaded bot module:", MODULE_PATH)
v15 = None
if hasattr(bot, "load_v15_module"):
    try:
        v15 = bot.load_v15_module()
        print("v15 module loaded:", v15)
    except Exception as e:
        print("v15 loader raised:", e)
        raise

if v15 is None:
    print("v15 is None — nothing more to inspect.")
    raise SystemExit(0)

print("\nModule file:", getattr(v15, "__file__", "(embedded)"))
print("\nTop-level attributes and callables in v15 module:\n")
names = sorted(dir(v15))
for n in names:
    if n.startswith("_"):
        continue
    obj = getattr(v15, n)
    if inspect.isfunction(obj) or inspect.ismethod(obj) or inspect.isbuiltin(obj):
        try:
            sig = str(inspect.signature(obj))
        except Exception:
            sig = "(signature unknown)"
        print(f"FUNCTION: {n}{sig}")
    else:
        # show non-callable names briefly
        print(f"ATTR: {n} (type={type(obj).__name__})")

# Also list any common aliases we might try automatically:
candidates = ["compute_signal","signal_to_side","predict","get_signal","signal","score","model_predict","infer"]
print("\nCandidate names to try calling (present? type):")
for c in candidates:
    print(c, "->", hasattr(v15, c), "type:", type(getattr(v15, c)) if hasattr(v15, c) else None)
