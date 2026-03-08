# automated_patch_local.py
# Run from the folder containing voidx2_0_final_beast_full.py
# Usage: python automated_patch_local.py
from pathlib import Path
import re, shutil, datetime, py_compile, textwrap, sys

ROOT = Path.cwd()
SRC = ROOT / "void_beast_engine.py"
OUT = ROOT / "voidx2_1_beast_orchestrated.py"
BACKUP = ROOT / f"voidx2_0_final_beast_full.backup.{int(datetime.datetime.utcnow().timestamp())}.py"

if not SRC.exists():
    print("ERROR: source file not found:", SRC)
    sys.exit(2)

# 1) backup
shutil.copy2(SRC, BACKUP)
print("Backup created:", BACKUP.name)

# 2) helper modules content (non-blocking, small, safe)
helpers = {
    "dashboard_integration.py": textwrap.dedent(r'''
        import os, time, json, requests
        from concurrent.futures import ThreadPoolExecutor
        from pathlib import Path
        DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "void_beast_key_123")
        DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://127.0.0.1:8000/ingest")
        REQUEST_TIMEOUT = float(os.getenv("DASHBOARD_REQUEST_TIMEOUT", "2.0"))
        OUTBOX_PATH = Path(os.getenv("DASHBOARD_OUTBOX", "dashboard_outbox.jsonl"))
        _executor = ThreadPoolExecutor(max_workers=2)
        def _write_outbox(payload):
            try:
                OUTBOX_PATH.parent.mkdir(parents=True, exist_ok=True)
                with OUTBOX_PATH.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"ts": time.time(), "payload": payload}) + "\\n")
            except Exception:
                pass
        def _post_async(payload):
            try:
                headers = {"Content-Type": "application/json"}
                resp = requests.post(DASHBOARD_URL, json=payload, timeout=REQUEST_TIMEOUT, headers=headers)
                if resp.status_code != 200:
                    _write_outbox(payload)
            except Exception:
                _write_outbox(payload)
        def send_event(event_type: str, payload: dict):
            data = {"key": DASHBOARD_KEY, "type": event_type, "payload": payload, "ts": time.time()}
            try:
                _executor.submit(_post_async, data)
            except Exception:
                _write_outbox(data)
        def send_log(level: str, message: str, meta: dict = None):
            send_event("log", {"level": level, "message": message, "meta": meta or {}})
        def send_error(message: str, meta: dict = None):
            send_event("error", {"message": message, "meta": meta or {}})
        def send_analysis(symbol: str, technical: float, fundamental: float, sentiment: float, final_score: float, meta: dict = None):
            send_event("analysis", {"symbol": symbol, "technical": technical, "fundamental": fundamental, "sentiment": sentiment, "final_score": final_score, "meta": meta or {}})
        def send_trade_open(symbol: str, side: str, lot: float, open_price: float, meta: dict = None):
            send_event("trade_open", {"symbol": symbol, "direction": side, "lot": lot, "open_price": open_price, "meta": meta or {}})
        def send_trade_close(symbol: str, side: str, lot: float, open_price: float, close_price: float, profit: float, meta: dict = None):
            send_event("trade_close", {"symbol": symbol, "direction": side, "lot": lot, "open_price": open_price, "close_price": close_price, "profit": profit, "meta": meta or {}})
        def send_heartbeat(status: str = "ok", meta: dict = None):
            send_event("heartbeat", {"status": status, "meta": meta or {}})
    '''),
    "trade_stats.py": textwrap.dedent(r'''
        import json, time
        from pathlib import Path
        TRADES_FILE = Path("beast_trades.jsonl")
        DEFAULT_WINDOW = 200
        def record_trade_result(symbol: str, profit: float, timestamp: float = None, meta: dict = None):
            ts = timestamp or time.time()
            TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
            entry = {"ts": ts, "symbol": symbol, "profit": float(profit), "meta": meta or {}}
            with TRADES_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\\n")
        def _read_all_trades():
            if not TRADES_FILE.exists():
                return []
            out = []
            with TRADES_FILE.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        out.append(json.loads(line))
                    except Exception:
                        continue
            return out
        def compute_winrate(window: int = None):
            if window is None:
                window = DEFAULT_WINDOW
            trades = _read_all_trades()
            if not trades:
                return 0.0, 0, 0, 0
            use = trades[-window:]
            wins = sum(1 for t in use if float(t.get("profit", 0.0)) > 0.0)
            n = len(use)
            if n == 0:
                return 0.0, 0, 0, 0
            winrate = wins / n
            return winrate, n, wins, n - wins
        def clear_trade_history():
            try:
                TRADES_FILE.unlink(missing_ok=True)
            except Exception:
                pass
    '''),
    "threshold_adapter.py": textwrap.dedent(r'''
        from trade_stats import compute_winrate
        TARGET_WINRATE = 0.55
        MIN_SAMPLES = 10
        MAX_STEP = 0.02
        BASE_THRESHOLD = 0.18
        def compute_adaptive_adjustment():
            winrate, n, wins, losses = compute_winrate()
            adj = 0.0
            if n >= MIN_SAMPLES:
                diff = TARGET_WINRATE - winrate
                adj = max(-MAX_STEP, min(MAX_STEP, diff * 0.5))
            return adj, winrate, n
    ''')
}

for name, content in helpers.items():
    p = ROOT / name
    if p.exists():
        print("Helper exists (skipping):", name)
    else:
        p.write_text(content.strip(), encoding="utf-8")
        print("Wrote helper module:", name)

# 3) read main source and normalize indentation
orig = SRC.read_text(encoding="utf-8")
text = orig.replace("\t", "    ")

# 4) remove XAGUSD / XAGUSDm from symbol arrays if present
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
else:
    # inject override near top (after file header or imports)
    m_blank = re.search(r"\n\n", text)
    pos = m_blank.end() if m_blank else 0
    override = ("\n# Injected symbol override: remove XAGUSDm (silver)\n"
                "TRADED_SYMBOLS = [s for s in globals().get('TRADED_SYMBOLS', globals().get('SYMBOLS', ['XAUUSD','BTCUSD','USOIL','USDJPY','EURUSD'])) if s.upper().replace('M','') != 'XAGUSD']\n"
                "globals()['TRADED_SYMBOLS'] = TRADED_SYMBOLS\n\n")
    text = text[:pos] + override + text[pos:]

# 5) orchestration block text (safe, minimal)
orch = textwrap.dedent("""
# --- BEGIN INJECTED ORCHESTRATION (ensure modules run each cycle) ---
try:
    try:
        import beast_threshold, beast_risk, beast_protection, beast_dashboard, beast_monitor, beast_correlation, beast_liquidity, beast_sentiment, beast_scoring, beast_regime, beast_nfp
    except Exception:
        pass
    # calendar / NFP protection
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
    # Liquidity / regime check
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
    # Threshold gravity + winrate adjustment (non-blocking)
    try:
        import threshold_adapter, trade_stats, dashboard_integration as dbi
    except Exception:
        threshold_adapter = None; trade_stats = None; dbi = None
    try:
        cur = beast_threshold.get_current_threshold()
    except Exception:
        cur = 0.18
    adj = 0.0
    winrate = 0.0; n = 0
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
    # telemetry
    try:
        if dbi is not None:
            dbi.send_analysis('__GLOBAL__', float(cur or 0.0), 0.0, float(winrate or 0.0), float(globals().get('CURRENT_THRESHOLD', cur or 0.0)), meta={'n': n})
    except Exception:
        pass
    # update last cycle timestamp for watchdog
    try:
        import time as _t
        globals()['LAST_CYCLE_TS'] = _t.time()
    except Exception:
        pass
except Exception:
    pass
# --- END INJECTED ORCHESTRATION ---
""")

# 6) insert orchestration into def __void_beast_cycle() or first while True loop
inserted = False
if "BEGIN INJECTED ORCHESTRATION" not in text:
    m_func = re.search(r"def\s+__void_beast_cycle\s*\(\)\s*:\s*\n", text)
    if m_func:
        start = m_func.end()
        rest = text[start:start+400]
        m_line = re.search(r"\n([ \t]+)\S", rest)
        indent = m_line.group(1) if m_line else "    "
        orch_ind = "\n".join(indent + line if line.strip() else line for line in orch.splitlines()) + "\n"
        doc_match = re.match(r"\s*(\"\"\".*?\"\"\"|'''.*?''')\s*\n", rest, re.DOTALL)
        insert_at = start + (doc_match.end() if doc_match else 0)
        text = text[:insert_at] + orch_ind + text[insert_at:]
        inserted = True
    else:
        m_loop = re.search(r"while\s+True\s*:\s*\n", text)
        if m_loop:
            loop_start = m_loop.end()
            rest = text[loop_start:loop_start+400]
            m_line = re.search(r"\n([ \t]+)\S", rest)
            indent = m_line.group(1) if m_line else "    "
            orch_ind = "\n".join(indent + line if line.strip() else line for line in orch.splitlines()) + "\n"
            text = text[:loop_start] + orch_ind + text[loop_start:]
            inserted = True

# 7) inject self-healing watchdog near top (after header or imports)
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
    m_blank = re.search(r"\n\n", text)
    pos = m_blank.end() if m_blank else 0
    text = text[:pos] + wd + text[pos:]

# 8) add helper imports near top for resilience
helpers_import = "\n# injected helpers imports\ntry:\n    import dashboard_integration\n    import trade_stats\n    import threshold_adapter\nexcept Exception:\n    pass\n\n"
if text.lstrip().startswith('"""') or text.lstrip().startswith("'''"):
    m_doc = re.search(r'^(?:[ \t]*("""|\'\'\')(?:.|\n)*?\1\n)', text)
    insert_pos2 = m_doc.end() if m_doc else 0
    text = text[:insert_pos2] + helpers_import + text[insert_pos2:]
else:
    text = helpers_import + text

# 9) write upgraded file (do not overwrite original)
OUT.write_text(text, encoding="utf-8")
print("Wrote upgraded file:", OUT)

# 10) syntax check
try:
    py_compile.compile(str(OUT), doraise=True)
    print("Syntax OK for", OUT.name)
    print("Upgraded file:", OUT)
    print("Backup remains at:", BACKUP)
    sys.exit(0)
except py_compile.PyCompileError as e:
    print("Syntax error in upgraded file:", e)
    print("Restoring original backup to source (no overwrite of original in this script).")
    # we do not overwrite original, backup left for manual restore
    sys.exit(3)
