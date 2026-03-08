# apply_orchestration_to_runtime.py
# Usage: python apply_orchestration_to_runtime.py [target_file]
# Default target_file: voidx_beast.py
from pathlib import Path
import sys, re, shutil, datetime, py_compile, textwrap

ROOT = Path.cwd()
target = sys.argv[1] if len(sys.argv) > 1 else "voidx_beast.py"
SRC = ROOT / target
if not SRC.exists():
    print("ERROR: target not found:", SRC)
    sys.exit(2)

# backup
BACKUP = ROOT / f"{SRC.stem}.backup_before_orch.{int(datetime.datetime.utcnow().timestamp())}.py"
shutil.copy2(SRC, BACKUP)
print("Backup created:", BACKUP.name)

text = SRC.read_text(encoding="utf-8")

# normalize tabs -> 4 spaces
text = text.replace("\t", "    ")

# 1) Remove XAGUSD / XAGUSDm from symbol arrays or inject override
sym_regex = re.compile(r"(?P<prefix>\b(?:SYMBOLS|SYMBOL_LIST|DEFAULT_SYMBOLS|TRADED_SYMBOLS|symbols)\s*=\s*)\[(?P<inside>[^\]]*)\]", re.IGNORECASE)
m = sym_regex.search(text)
modified_symbols = None
if m:
    inside = m.group("inside")
    items = re.findall(r"['\"]([^'\"]+)['\"]", inside)
    filtered = [s for s in items if s.upper().replace("M","") != "XAGUSD"]
    new_inside = ", ".join(f"'{s}'" for s in filtered)
    new_block = m.group("prefix") + "[" + new_inside + "]"
    text = text[:m.start()] + new_block + text[m.end():]
    modified_symbols = filtered
    print("Symbols list adjusted in file.")
else:
    # safe override near top (after first blank line)
    m_blank = re.search(r"\n\n", text)
    pos = m_blank.end() if m_blank else 0
    override = ("\n# Injected symbol override: remove XAGUSDm (silver)\n"
                "TRADED_SYMBOLS = [s for s in globals().get('TRADED_SYMBOLS', globals().get('SYMBOLS', ['XAUUSD','BTCUSD','USOIL','USDJPY','EURUSD'])) if s.upper().replace('M','') != 'XAGUSD']\n"
                "globals()['TRADED_SYMBOLS'] = TRADED_SYMBOLS\n\n")
    text = text[:pos] + override + text[pos:]
    print("Symbol override injected near top.")

# 2) Orchestration block (compact and safe)
orch = textwrap.dedent("""\
# --- BEGIN INJECTED ORCHESTRATION (ensure modules run each cycle) ---
try:
    # safe imports
    try:
        import beast_threshold, beast_risk, beast_protection, beast_dashboard, beast_monitor, beast_correlation, beast_liquidity
    except Exception:
        pass

    # Calendar / high-impact protection
    try:
        if 'beast_calendar' in globals():
            try:
                events = globals().get('BEAST_CALENDAR_EVENTS', [])
                blocked, reason = beast_calendar.should_block_for_events(events)
                if blocked:
                    logger.info(f"Calendar block active: {reason}; skipping cycle")
                    return
            except Exception:
                pass
    except Exception:
        pass

    # Signal Quality Filter (SQF)
    try:
        if 'beast_protection' in globals():
            try:
                spread = globals().get('CURRENT_SPREAD_POINTS', None)
                atr_now = globals().get('CURRENT_ATR', None)
                atr_avg = globals().get('ATR_AVG', None)
                recent_move = globals().get('RECENT_MOVE_PCT', None)
                ok, reason = beast_protection.sqf_check(globals().get('CURRENT_SYMBOL','GENERIC'), spread, atr_now, atr_avg, recent_move)
                if not ok:
                    logger.info(f"SQF blocked: {reason}")
                    return
            except Exception:
                pass
    except Exception:
        pass

    # Liquidity / commodity regime
    try:
        if 'beast_liquidity' in globals():
            try:
                ok, reason = beast_liquidity.commodity_regime_check(globals().get('CURRENT_SYMBOL','GENERIC'), globals().get('CURRENT_ATR',None), globals().get('ATR_AVG',None), globals().get('CURRENT_SPREAD_POINTS',None))
                if not ok:
                    logger.info(f"Liquidity block: {reason}")
                    return
            except Exception:
                pass
    except Exception:
        pass

    # Correlation check
    try:
        if 'beast_correlation' in globals():
            try:
                a = globals().get('RECENT_SERIES_A', [])
                b = globals().get('RECENT_SERIES_B', [])
                corr = beast_correlation.correlation_coefficient(a, b)
                if abs(corr) > 0.95:
                    logger.info('Correlation block: high correlation')
                    return
            except Exception:
                pass
    except Exception:
        pass

    # Threshold gravity + winrate adj (non-blocking)
    try:
        try:
            import threshold_adapter, trade_stats, dashboard_integration as dbi
        except Exception:
            threshold_adapter = None; trade_stats = None; dbi = None
        try:
            cur = beast_threshold.get_current_threshold()
        except Exception:
            cur = 0.18
        adj = 0.0; winrate = 0.0; n = 0
        if threshold_adapter is not None:
            try:
                adj, winrate, n = threshold_adapter.compute_adaptive_adjustment()
            except Exception:
                adj, winrate, n = 0.0, 0.0, 0
        try:
            newt = beast_threshold.apply_gravity_and_volatility(cur, volatility_adj=float(adj or 0.0))
            globals()['CURRENT_THRESHOLD'] = newt
        except Exception:
            globals()['CURRENT_THRESHOLD'] = cur
        try:
            if dbi is not None:
                dbi.send_analysis('__GLOBAL__', float(cur or 0.0), 0.0, float(winrate or 0.0), float(globals().get('CURRENT_THRESHOLD', cur or 0.0)), meta={'n': n})
        except Exception:
            pass
    except Exception:
        pass

    # update watchdog timestamp
    try:
        import time as _t; globals()['LAST_CYCLE_TS'] = _t.time()
    except Exception:
        pass

except Exception:
    pass
# --- END INJECTED ORCHESTRATION ---
""")

# 3) Insert orchestration safely into def __void_beast_cycle() or first while True:
if "BEGIN INJECTED ORCHESTRATION" not in text:
    m_func = re.search(r"def\s+__void_beast_cycle\s*\(\)\s*:\s*\n", text)
    inserted = False
    if m_func:
        start = m_func.end()
        rest = text[start:start+360]
        m_line = re.search(r"\n([ \t]+)\S", rest)
        indent = m_line.group(1) if m_line else "    "
        orch_ind = "\n".join(indent + line if line.strip() else line for line in orch.splitlines()) + "\n"
        doc_match = re.match(r"\s*(?:\"\"\".*?\"\"\"|'''.*?''')\s*\n", rest, re.DOTALL)
        insert_at = start + (doc_match.end() if doc_match else 0)
        text = text[:insert_at] + orch_ind + text[insert_at:]
        inserted = True
        print("Orchestration injected into __void_beast_cycle()")
    else:
        m_loop = re.search(r"while\s+True\s*:\s*\n", text)
        if m_loop:
            loop_start = m_loop.end()
            rest = text[loop_start:loop_start+360]
            m_line = re.search(r"\n([ \t]+)\S", rest)
            indent = m_line.group(1) if m_line else "    "
            orch_ind = "\n".join(indent + line if line.strip() else line for line in orch.splitlines()) + "\n"
            text = text[:loop_start] + orch_ind + text[loop_start:]
            inserted = True
            print("Orchestration injected into first while True loop")
    if not inserted:
        print("WARNING: no injection point found; orchestration not inserted.")
else:
    print("Orchestration already present in file; skipping injection.")

# 4) Inject watchdog near top if not present
if "BEGIN ORCHESTRATION WATCHDOG" not in text:
    wd = textwrap.dedent("""
# --- BEGIN ORCHESTRATION WATCHDOG (self-healing) ---
import threading, time, os, sys
def _watchdog_thread(poll_interval=10, max_gap=120):
    try:
        while True:
            try:
                last = globals().get('LAST_CYCLE_TS', None)
                now = time.time()
                if last is None:
                    globals()['LAST_CYCLE_TS'] = now
                else:
                    if now - last > max_gap:
                        try:
                            p = sys.executable
                            args = [p] + sys.argv
                            try:
                                sys.stdout.flush(); sys.stderr.flush()
                            except Exception:
                                pass
                            os.execv(p, args)
                        except Exception:
                            pass
                time.sleep(poll_interval)
            except Exception:
                time.sleep(poll_interval)
    except Exception:
        pass
try:
    _wd = threading.Thread(target=_watchdog_thread, daemon=True)
    _wd.start()
except Exception:
    pass
# --- END ORCHESTRATION WATCHDOG ---
""")
    # insert after first blank line or after imports
    m_blank = re.search(r"\n\n", text)
    pos = m_blank.end() if m_blank else 0
    text = text[:pos] + wd + text[pos:]
    print("Watchdog injected near top.")

# 5) ensure helper imports at top (non-fatal)
helpers_import = "\n# injected helpers imports\ntry:\n    import dashboard_integration\n    import trade_stats\n    import threshold_adapter\nexcept Exception:\n    pass\n\n"
if text.lstrip().startswith('"""') or text.lstrip().startswith("'''"):
    m_doc = re.search(r'^(?:[ \t]*("""|\'\'\')(?:.|\n)*?\1\n)', text)
    insert_pos2 = m_doc.end() if m_doc else 0
    text = text[:insert_pos2] + helpers_import + text[insert_pos2:]
else:
    text = helpers_import + text

# 6) write upgraded file (do not overwrite original)
OUT = ROOT / f"{SRC.stem}_orchestrated.py"
OUT.write_text(text, encoding="utf-8")
print("Wrote upgraded file:", OUT.name)

# 7) syntax check
try:
    py_compile.compile(str(OUT), doraise=True)
    print("Syntax OK for", OUT.name)
    print("Run the upgraded file with (demo first):")
    print(f"python {OUT.name} --loop --live False")
    sys.exit(0)
except py_compile.PyCompileError as e:
    print("Syntax error in upgraded file:", e)
    print("The original file and backup are unchanged. Paste the error here and I will fix it.")
    sys.exit(3)
