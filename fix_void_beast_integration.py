#!/usr/bin/env python3
# fix_void_beast_integration.py
# Moves the VOID BEAST integration import block to the top of the merged file
# to avoid being accidentally placed inside an existing try/except or block.

import sys, py_compile
from pathlib import Path

ROOT = Path.cwd()
MERGED = ROOT / "voidx2_0_final_beast.py"

if not MERGED.exists():
    print("Error: merged file not found:", MERGED)
    sys.exit(2)

text = MERGED.read_text(encoding="utf-8")

start_marker = "# ==== BEGIN VOID BEAST INTEGRATION IMPORTS ===="
end_marker = "# ==== END VOID BEAST INTEGRATION IMPORTS ===="

s = text.find(start_marker)
e = text.find(end_marker)

if s == -1 or e == -1:
    print("Integration block markers not found in merged file. Nothing changed.")
    sys.exit(0)

# include end marker line
e_end = e + len(end_marker)

# Extract block (keep leading newline consistency)
block = text[s:e_end]

# Remove block from original location
new_text = text[:s] + text[e_end:]

# Clean up any accidental duplicate blank lines
# ensure block is followed by two newlines
if not block.endswith("\n\n"):
    block = block.rstrip() + "\n\n"

# Prepend block to top (after optional shebang or module docstring)
# If file starts with a shebang, preserve it on line 0
lines = new_text.splitlines(keepends=True)

insert_at = 0
if lines and lines[0].startswith("#!"):
    # keep shebang at top, insert after it
    insert_at = 1

# Build final content
final_lines = []
if insert_at == 1:
    final_lines.append(lines[0])
    final_lines.append("\n")  # ensure separation
    final_lines.append(block)
    final_lines.extend(lines[1:])
else:
    final_lines.append(block)
    final_lines.extend(lines)

final_text = "".join(final_lines)

# Write back
MERGED.write_text(final_text, encoding="utf-8")
print("Integration block moved to top of", MERGED.name)

# Run syntax check
try:
    py_compile.compile(str(MERGED), doraise=True)
    print("Syntax check PASSED for", MERGED.name)
    sys.exit(0)
except py_compile.PyCompileError as e:
    print("Syntax check FAILED after moving block. Error:")
    print(e)
    # leave the file as-is for inspection
    sys.exit(4)
