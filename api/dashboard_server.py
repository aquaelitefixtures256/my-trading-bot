# api/dashboard_server.py
import os
import sys
import time
import json
import sqlite3
import logging
import shlex
import subprocess
import threading
from typing import Dict, Any, Optional

import jwt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -----------------------------
# HARDCODED CONFIG (as requested)
# -----------------------------
# admin bypass token (frontend may post this)
ADMIN_BYPASS_TOKEN = "12345678"

# JWT secret used for issuing tokens
JWT_SECRET = "void_beast_9f3a"
JWT_ALGO = "HS256"

# Dashboard ingest key (what the bot must send when posting /ingest)
DASHBOARD_KEY = "ALT_BEAST_03MAR2026_9f3a"

# Database and frontend directory (relative to this repo)
DB_PATH = "dashboard.db"
FRONTEND_DIR = "frontend"

# Bot executable + script path (use your venv Python and bot script)
# NOTE: we provide the full quoted command string so shlex.split works on Windows.
_python_exe = r"C:\Users\Administrator\Desktop\Muc_universe\venv_quant\Scripts\python.exe"
_bot_script = r"C:\Users\Administrator\Desktop\Muc_universe\voidx_beast.py"
BOT_CMD = f'"{_python_exe}" "{_bot_script}"'

# Working directory for the bot process (where voidx_beast.py expects to run)
BOT_WORKDIR = r"C:\Users\Administrator\Desktop\Muc_universe"

# Admin user (for JWT subject)
ADMIN_USER = "admin"
ADMIN_PASS = "password"  # you can change this later if you want to use real login

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dashboard_server")

# --- FastAPI app ---
app = FastAPI(title="VOID Beast Dashboard")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# serve frontend static files if present
if os.path.isdir(FRONTEND_DIR):
    from fastapi.staticfiles import StaticFiles
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# --- DB (sqlite) ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()

# Create minimal schema if not exists, plus mt5_status table
cur.executescript("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    symbol TEXT,
    side TEXT,
    lots REAL,
    open_price REAL,
    close_price REAL,
    profit REAL,
    status TEXT,
    meta TEXT
);
CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    symbol TEXT,
    technical REAL,
    fundamental REAL,
    sentiment REAL,
    final_score REAL,
    meta TEXT
);
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    level TEXT,
    message TEXT,
    meta TEXT
);
CREATE TABLE IF NOT EXISTS mt5_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
""")
conn.commit()

# track last event timestamp (for showing "connected")
last_event_ts = 0

# --- websocket manager ---
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        data = json.dumps(message, default=str)
        to_remove = []
        for ws in list(self.active):
            try:
                await ws.send_text(data)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            self.disconnect(ws)

manager = ConnectionManager()

# --- Helpers to persist events ---
def persist_event(event_type: str, payload: Any):
    global last_event_ts
    ts = int(time.time())
    try:
        cur.execute("INSERT INTO events(ts,type,payload) VALUES (?,?,?)", (ts, event_type, json.dumps(payload)))
        conn.commit()
        last_event_ts = ts
    except Exception:
        logger.exception("failed to persist event")

    # specialized handling
    if event_type in ("trade_open", "trade_close"):
        try:
            p = payload
            cur.execute(
                "INSERT INTO trades(ts,symbol,side,lots,open_price,close_price,profit,status,meta) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    ts,
                    p.get("symbol"),
                    p.get("direction") or p.get("side"),
                    p.get("lot") or p.get("lots"),
                    p.get("open_price"),
                    p.get("close_price"),
                    p.get("profit"),
                    p.get("status") or ("closed" if event_type == "trade_close" else "open"),
                    json.dumps(p.get("meta", {}))
                )
            )
            conn.commit()
        except Exception:
            logger.exception("failed to persist trade")

    if event_type == "analysis":
        try:
            p = payload
            cur.execute(
                "INSERT INTO analyses(ts,symbol,technical,fundamental,sentiment,final_score,meta) VALUES (?,?,?,?,?,?,?)",
                (ts, p.get("symbol"), p.get("technical"), p.get("fundamental"), p.get("sentiment"), p.get("final_score"), json.dumps(p.get("meta", {})))
            )
            conn.commit()
        except Exception:
            logger.exception("failed to persist analysis")

    if event_type == "error":
        try:
            p = payload
            cur.execute(
                "INSERT INTO errors(ts,level,message,meta) VALUES (?,?,?,?)",
                (ts, p.get("level", "ERROR"), p.get("message"), json.dumps(p.get("meta", {})))
            )
            conn.commit()
        except Exception:
            logger.exception("failed to persist error")

    # mt5 status/balance/equity: store last known values
    if event_type in ("mt5_status", "balance", "equity"):
        try:
            key = event_type
            value = json.dumps(payload)
            cur.execute("INSERT INTO mt5_status(ts,key,value) VALUES (?,?,?)", (ts, key, value))
            conn.commit()
        except Exception:
            logger.exception("failed to persist mt5 status")

# --- Models ---
class IngestModel(BaseModel):
    key: str
    type: str
    payload: dict

class AuthModel(BaseModel):
    username: str
    password: str

# --- Simple JWT auth helpers ---
def create_access_token(subject: str, expires_in: int = 3600):
    payload = {"sub": subject, "exp": int(time.time()) + expires_in}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
    return token

def verify_token(token: str) -> Optional[str]:
    # Accept the bypass token or a valid JWT
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing token")
    if token == ADMIN_BYPASS_TOKEN:
        return ADMIN_USER
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        return data.get("sub")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="token expired")
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token")

async def get_current_user(authorization: str = None):
    # Expect "Bearer <token>"
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization")
    token = authorization.split(" ", 1)[1]
    user = verify_token(token)
    return user

# --- Bot process manager (start/stop) ---
class BotProcessManager:
    def __init__(self, cmd: str, workdir: str = "."):
        self.cmd = cmd
        self.workdir = workdir
        self.proc: Optional[subprocess.Popen] = None
        self._stdout_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None

    def is_running(self):
        return self.proc is not None and self.proc.poll() is None

    def _start_stream_reader(self, stream, label: str):
        def _reader():
            try:
                for line in iter(stream.readline, ""):
                    if not line:
                        break
                    ln = line.rstrip()
                    logger.info(f"[bot {label}] {ln}")
                    # persist & broadcast
                    try:
                        persist_event("log", {"level": "INFO", "message": ln, "source": "bot"})
                        # broadcast bot_log asynchronously
                        def _b():
                            try:
                                import asyncio
                                coro = manager.broadcast({"type":"bot_log","payload":{"message":ln},"ts":int(time.time())})
                                asyncio.run(coro)
                            except Exception:
                                pass
                        threading.Thread(target=_b, daemon=True).start()
                    except Exception:
                        logger.exception("failed handling bot output line")
            except Exception:
                pass
        t = threading.Thread(target=_reader, daemon=True)
        t.start()
        return t

    def start(self):
        if self.is_running():
            return False, "already running"

        # Use BOT_CMD override defined above (hardcoded)
        cmd_to_use = BOT_CMD

        try:
            parts = shlex.split(cmd_to_use)
        except Exception:
            parts = cmd_to_use.split()

        # Ensure python interpreter used if a .py script is present
        if parts and parts[0].lower().endswith(".py"):
            cmd_list = [sys.executable] + parts
        else:
            if any(p.lower().endswith(".py") for p in parts) and not parts[0].lower().endswith(("python", "python.exe")):
                cmd_list = [sys.executable] + parts
            else:
                cmd_list = parts

        logger.info(f"Starting bot: {cmd_list} in {self.workdir}")
        try:
            self.proc = subprocess.Popen(
                cmd_list,
                cwd=self.workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True
            )
        except FileNotFoundError as e:
            logger.exception("Bot start failed - file not found")
            return False, f"file not found: {e}"
        except Exception as e:
            logger.exception("Bot start failed")
            return False, str(e)

        # Attach readers
        try:
            if self.proc.stdout:
                self._stdout_thread = self._start_stream_reader(self.proc.stdout, "out")
            if self.proc.stderr:
                self._stderr_thread = self._start_stream_reader(self.proc.stderr, "err")
        except Exception:
            logger.exception("Failed to attach stdout/stderr readers")

        return True, "started"

    def stop(self):
        if not self.is_running():
            return False, "not running"
        logger.info("Terminating bot process")
        try:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        except Exception:
            logger.exception("Failed to stop process")
        finally:
            self.proc = None
        return True, "stopped"

    def get_info(self):
        return {"running": self.is_running(), "pid": self.proc.pid if self.proc else None}

# initialize manager with defaults (hardcoded)
bot_manager = BotProcessManager(BOT_CMD, BOT_WORKDIR)

# --- API endpoints ---
@app.post("/ingest")
async def ingest(item: IngestModel):
    # Bot connector authenticates with DASHBOARD_KEY
    if item.key != DASHBOARD_KEY:
        raise HTTPException(status_code=401, detail="invalid key")
    try:
        persist_event(item.type, item.payload)
    except Exception as e:
        logger.exception("persist failed")
        raise HTTPException(status_code=500, detail=str(e))

    # broadcast to websockets
    try:
        await manager.broadcast({"type": item.type, "payload": item.payload, "ts": int(time.time())})
    except Exception:
        logger.exception("broadcast failed")
    return JSONResponse({"status": "ok"})

@app.post("/auth/login")
async def login(creds: AuthModel):
    if creds.username != ADMIN_USER or creds.password != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="invalid credentials")
    token = create_access_token(creds.username, expires_in=60*60*8)  # 8 hours
    return {"access_token": token, "token_type": "bearer"}

@app.post("/control/start")
async def control_start(authorization: str = Depends(lambda request=None: (request := request) and request.headers.get("Authorization"))):
    # require valid user (JWT or bypass token)
    user = await get_current_user(authorization)
    ok, msg = bot_manager.start()
    # broadcast status
    try:
        await manager.broadcast({"type": "control", "payload": {"action": "start", "ok": ok, "msg": msg, "user": user}, "ts": int(time.time())})
    except Exception:
        logger.exception("broadcast failed")
    return {"ok": ok, "msg": msg}

@app.post("/control/stop")
async def control_stop(authorization: str = Depends(lambda request=None: (request := request) and request.headers.get("Authorization"))):
    user = await get_current_user(authorization)
    ok, msg = bot_manager.stop()
    try:
        await manager.broadcast({"type": "control", "payload": {"action": "stop", "ok": ok, "msg": msg, "user": user}, "ts": int(time.time())})
    except Exception:
        logger.exception("broadcast failed")
    return {"ok": ok, "msg": msg}

@app.get("/status")
async def status():
    # basic stats + bot status + last event
    cur.execute("SELECT COUNT(*) FROM events")
    total = cur.fetchone()[0]
    bot_info = bot_manager.get_info()
    last_event = last_event_ts
    # fetch latest mt5 status values
    cur.execute("SELECT key,value,ts FROM mt5_status ORDER BY id DESC")
    rows = cur.fetchall()
    mt5 = {}
    for r in rows:
        try:
            val = json.loads(r[1])
        except Exception:
            val = r[1]
        mt5[r[0]] = {"value": val, "ts": r[2]}
    return {"status": "ok", "events": total, "bot": bot_info, "last_event": last_event, "mt5": mt5}

@app.get("/events")
async def events(limit: int = 50):
    cur.execute("SELECT ts,type,payload FROM events ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    out = []
    for r in rows:
        try:
            payload = json.loads(r[2])
        except Exception:
            payload = r[2]
        out.append({"ts": r[0], "type": r[1], "payload": payload})
    return out

@app.get("/trades")
async def trades(limit: int = 100):
    cur.execute("SELECT ts,symbol,side,lots,open_price,close_price,profit,status,meta FROM trades ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    return [{"ts": r[0], "symbol": r[1], "side": r[2], "lots": r[3], "open_price": r[4], "close_price": r[5], "profit": r[6], "status": r[7], "meta": json.loads(r[8] or "{}")} for r in rows]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # keepalive - the client may send pings or messages
            try:
                _ = await websocket.receive_text()
            except WebSocketDisconnect:
                break
    finally:
        manager.disconnect(websocket)

@app.get("/")
async def index():
    # If frontend index exists, serve it; otherwise return a simple JSON health message
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return JSONResponse({"message": "Void Beast Dashboard API is running", "status": "ok"})
