from pathlib import Path

file = Path("voidx2_0_final_beast_full_upgraded.py")

text = file.read_text()

# Replace tabs with 4 spaces
text = text.replace("\t", "    ")

file.write_text(text)

print("Tabs converted to spaces.")
