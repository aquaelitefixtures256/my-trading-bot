# upgrade_beast.py
# Run in the folder containing void_beast_engine.py
# Usage: python upgrade_beast.py
from pathlib import Path
import re, shutil, datetime, py_compile, json, sys

ROOT = Path.cwd()
SRC = ROOT / "void_beast_engine.py"
OUT = ROOT / "voidx2_0_final_beast_full_upgraded.py"
BACKUP = ROOT / f"void_beast_engine.autobackup.{int(datetime.datetime.utcnow().timestamp())}.py"

if not SRC.exists():
    print("ERROR: source file not found:", SRC)
    sys.exit(2)

# 1) create a backup copy
shutil.copy2(SRC, BACKUP)
print("Backup created:", BACKUP.name)

text = SRC.read_text(encoding="utf-8")

# 2) remove XAGUSD / XAGUSDm from symbol arrays
symbol_regex = re.compile(r"(?P<prefix>\b(?:SYMBOLS|SYMBOL_LIST|DEFAULT_SYMBOLS|symbols)\s*=\s*)\[(?P<inside>[^\]]*)\]", re.IGNORECASE)
m = symbol_regex.search(text)
modified_symbols = None
if m:
    inside = m.group("inside")
    items = re.findall(r"['\"]([^'\"]+)['\"]", inside)
    filtered = [s for s in items if s.upper().replace("M","") != "XAGUSD"]
    new_inside = ", ".join(f"'{s}'" for s in filtered)
    new_block = m.group("prefix") + "[" + new_inside + "]"
    text = text[:m.start()] + new_block + text[m.end():]
    modified_symbols = filtered

else:
    # inject override near top (safe fallback)
    insert_pos = 0
    override_code = (
        "\n# Injected symbol override: remove XAGUSDm (silver) from traded symbols\n"
        "TRADED_SYMBOLS = [s for s in globals().get('TRADED_SYMBOLS', globals().get('SYMBOLS', ['XAUUSD','BTCUSD','USOIL','USDJPY','EURUSD'])) "
        "if s.upper().replace('M','') != 'XAGUSD']\n"
        "globals()['TRADED_SYMBOLS'] = TRADED_SYMBOLS\n\n"
    )
    text = override_code + text

# 3) orchestration snippet (calls protection, calendar, liquidity, correlation, threshold gravity)
orchestration = r'''
    # --- BEGIN INJECTED ORCHESTRATION (ensure modules run each cycle) ---
    try:
        # Calendar / NFP protection
        if 'beast_calendar' in globals():
            try:
                events = globals().get('BEAST_CALENDAR_EVENTS', [])
                blocked, reason = beast_calendar.should_block_for_events(events)
                if blocked:
                    logger.info(f"Calendar block active: {reason}; skipping cycle")
                    return
            except Exception:
                pass
        # Signal Quality Filter (SQF)
        if 'beast_protection' in globals():
            try:
                spread = globals().get('CURRENT_SPREAD_POINTS', None)
                atr_now = globals().get('CURRENT_ATR', None)
                atr_avg = globals().get('ATR_AVG', None)
                recent_move = globals().get('RECENT_MOVE_PCT', None)
                ok, r = beast_protection.sqf_check(globals().get('CURRENT_SYMBOL','GENERIC'), spread, atr_now, atr_avg, recent_move)
                if not ok:
                    logger.info(f"SQF blocked: {r}")
                    return
            except Exception:
                pass
        # Liquidity / commodity regime
        if 'beast_liquidity' in globals():
            try:
                ok, r = beast_liquidity.commodity_regime_check(globals().get('CURRENT_SYMBOL','GENERIC'), globals().get('CURRENT_ATR',None), globals().get('ATR_AVG',None), globals().get('CURRENT_SPREAD_POINTS',None))
                if not ok:
                    logger.info(f"Liquidity block: {r}")
                    return
            except Exception:
                pass
        # Correlation check
        if 'beast_correlation' in globals():
            try:
                series_a = globals().get('RECENT_SERIES_A', [])
                series_b = globals().get('RECENT_SERIES_B', [])
                corr = beast_correlation.correlation_coefficient(series_a, series_b)
                if abs(corr) > 0.95:
                    logger.info('Correlation block: high correlation')
                    return
            except Exception:
                pass
        # Apply threshold gravity
        if 'beast_threshold' in globals():
            try:
                cur = beast_threshold.get_current_threshold()
                newt = beast_threshold.apply_gravity_and_volatility(cur, volatility_adj=0.0)
                globals()['CURRENT_THRESHOLD'] = newt
            except Exception:
                pass
    except Exception:
        pass
    # --- END INJECTED ORCHESTRATION ---
'''

# 4) insert orchestration into __void_beast_cycle if available, otherwise into the main while True loop
m_func = re.search(r"def\s+__void_beast_cycle\s*\(\)\s*:\s*\n", text)
if m_func:
    start = m_func.end()
    rest = text[start:start+500]
    doc_match = re.match(r"\s*(?:\"\"\".*?\"\"\"|'''.*?''')\s*\n", rest, re.DOTALL)
    insert_at = start + (doc_match.end() if doc_match else 0)
    orchestration_indented = "\n".join("    " + line for line in orchestration.splitlines()) + "\n"
    text = text[:insert_at] + orchestration_indented + text[insert_at:]
else:
    m_loop = re.search(r"while\s+True\s*:\s*\n", text)
    if m_loop:
        loop_body_start = m_loop.end()
        orchestration_indented = "\n".join("    " + line for line in orchestration.splitlines()) + "\n"
        text = text[:loop_body_start] + orchestration_indented + text[loop_body_start:]
    else:
        print("No injection point found (no __void_beast_cycle or while True). Aborting and restoring backup.")
        shutil.copy2(BACKUP, SRC)
        sys.exit(3)

# 5) write upgraded file (do not overwrite original)
OUT.write_text(text, encoding="utf-8")
print("Wrote upgraded file:", OUT.name)

# 6) syntax check
try:
    py_compile.compile(str(OUT), doraise=True)
    print("Syntax OK for", OUT.name)
    print("Modified symbols (if discovered):", modified_symbols)
    print("Upgraded file location:", OUT)
except py_compile.PyCompileError as e:
    print("Syntax error in upgraded file:", e)
    print("Restoring original from backup.")
    shutil.copy2(BACKUP, SRC)
    sys.exit(4)
