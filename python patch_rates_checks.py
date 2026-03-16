# patch_rates_checks.py
import io, re, shutil, sys, os

SRC = "KYOTO_INFERNO_V16_fixed-5_upgraded.py"
BAK = SRC + ".bak"

if not os.path.exists(SRC):
    print("Source file not found:", SRC); sys.exit(1)

# make backup
shutil.copy2(SRC, BAK)
print("Backup written to", BAK)

with io.open(SRC, "r", encoding="utf-8") as f:
    text = f.read()

patterns = [
    # if rates:  -> explicit numpy/sequence-safe check
    (re.compile(r'(^\s*)if\s+rates\s*:', re.M), r'\1if rates is not None and len(rates) > 0:'),
    # if not rates: -> explicit empty/None test
    (re.compile(r'(^\s*)if\s+not\s+rates\s*:', re.M), r'\1if rates is None or len(rates) == 0:'),
    # probe checks (copy_rates_from_pos -> probe could be array or None)
    (re.compile(r'(^\s*)if\s+probe\s*:', re.M), r'\1if probe is not None and len(probe) > 0:'),
    (re.compile(r'(^\s*)if\s+not\s+probe\s*:', re.M), r'\1if probe is None or len(probe) == 0:'),
    # generic: safe check for "if bars:" usages
    (re.compile(r'(^\s*)if\s+bars\s*:', re.M), r'\1if bars is not None and len(bars) > 0:'),
    (re.compile(r'(^\s*)if\s+not\s+bars\s*:', re.M), r'\1if bars is None or len(bars) == 0:'),
]

new_text = text
changes = 0
for pat, repl in patterns:
    new_text, n = pat.subn(repl, new_text)
    changes += n

if changes == 0:
    print("No pattern matches found — nothing changed.")
else:
    with io.open(SRC, "w", encoding="utf-8") as f:
        f.write(new_text)
    print(f"Applied {changes} replacements to {SRC}")

# show small preview of lines containing 'rates' or 'probe' or 'bars' for manual sanity check
lines = new_text.splitlines()
print("\nPreview (lines with 'rates' or 'probe' or 'bars'):")
for i, ln in enumerate(lines, start=1):
    if 'rates' in ln or 'probe' in ln or 'bars' in ln:
        print(f"{i:4d}: {ln.rstrip()}")
