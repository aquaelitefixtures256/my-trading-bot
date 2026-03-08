# api/dashboard_server.py
import os
import sys
import time
import json
import sqlite3
import logging
import shlex
import subprocess
from typing import Dict, Any

import jwt
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- Config (env vars) ---
DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "VOID_TEST_KEY")
DB_PATH = os.getenv("DASHBOARD_DB", "dashboard.db")
FRONTEND_DIR = os.getenv("FRONTEND_DIR", "frontend")
JWT_SECRET = os.getenv("JWT_SECRET", "supersecret_jwt_key_change_me!")
JWT_ALGO = "HS256"
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "password")  # replace with strong secret in production

# Determine a sensible default bot path (assumes bot is one level above void_beast_dashboard)
# __file__ -> .../void_beast_dashboard/api/dashboard_server.py
# parent x3 -> .../Muc_universe
_default_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_default_bot_path = os.path.join(_default_parent, "voidx_beast.py")

# BOT_CMD: by default run the bot with the same Python interpreter being used for the server
BOT_CMD = os.getenv("BOT_CMD", f"{sys.executable} \"{_default_bot_path}\"")
# BOT_WORKDIR: default to the Muc_universe parent (so relative imports/files in bot still work)
BOT_WORKDIR = os.getenv("BOT_WORKDIR", _default_parent)

# --- Logging ---
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

# Create minimal schema if not exists
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
        for ws in self.active:
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
    cur.execute("INSERT INTO events(ts,type,payload) VALUES (?,?,?)", (ts, event_type, json.dumps(payload)))
    conn.commit()
    last_event_ts = ts

    if event_type in ("trade_open", "trade_close"):
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

    if event_type == "analysis":
        p = payload
        cur.execute(
            "INSERT INTO analyses(ts,symbol,technical,fundamental,sentiment,final_score,meta) VALUES (?,?,?,?,?,?,?)",
            (ts, p.get("symbol"), p.get("technical"), p.get("fundamental"), p.get("sentiment"), p.get("final_score"), json.dumps(p.get("meta", {})))
        )
        conn.commit()

    if event_type == "error":
        p = payload
        cur.execute(
            "INSERT INTO errors(ts,level,message,meta) VALUES (?,?,?,?)",
            (ts, p.get("level", "ERROR"), p.get("message"), json.dumps(p.get("meta", {})))
        )
        conn.commit()

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

def verify_token(token: str):
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
        self.proc: subprocess.Popen | None = None

    def is_running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self):
        if self.is_running():
            return False, "already running"

        # Build command list; prefer explicit sys.executable if BOT_CMD doesn't include interpreter
        try:
            # If BOT_CMD starts with a Python executable reference, keep as-is; otherwise ensure correct interpreter
            # Example BOT_CMD default is like: "C:\...\python.exe \"C:\...\voidx_beast.py\""
            cmd_env = os.getenv("BOT_CMD")
            if cmd_env:
                cmd_to_use = cmd_env
            else:
                cmd_to_use = self.cmd

            # If the command does not reference a python executable and looks like a script path, prefix with sys.executable
            parts = shlex.split(cmd_to_use)
            if len(parts) > 0 and parts[0].lower().endswith((".py",)):
                # first token is a script path — prepend sys.executable
                cmd_list = [sys.executable] + parts
            else:
                cmd_list = shlex.split(cmd_to_use)
        except Exception:
            # fallback conservative split
            cmd_list = shlex.split(self.cmd)

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

            # Optionally start background readers for stdout/stderr so process doesn't block
            def _drain_stream(stream, label):
                try:
                    for line in stream:
                        logger.info(f"[bot {label}] {line.rstrip()}")
                        # Also forward as dashboard 'log' events
                        try:
                            persist_event("log", {"level": "INFO", "message": line.rstrip(), "source": "bot"})
                        except Exception:
                            pass
                except Exception:
                    pass

            # spawn readers
            try:
                import threading
                if self.proc.stdout:
                    t_out = threading.Thread(target=_drain_stream, args=(self.proc.stdout, "out"), daemon=True)
                    t_out.start()
                if self.proc.stderr:
                    t_err = threading.Thread(target=_drain_stream, args=(self.proc.stderr, "err"), daemon=True)
                    t_err.start()
            except Exception:
                logger.debug("Failed to attach stdout/stderr readers")

            return True, "started"
        except FileNotFoundError as e:
            logger.exception("Bot start failed - file not found")
            return False, f"file not found: {e}"
        except Exception as e:
            logger.exception("Bot start failed")
            return False, str(e)

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
        except Exception as e:
            logger.exception("Failed to stop process")
        finally:
            self.proc = None
        return True, "stopped"

    def get_info(self):
        return {"running": self.is_running(), "pid": self.proc.pid if self.proc else None}

# initialize manager with defaults (but it will respect BOT_CMD/BOT_WORKDIR env vars)
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
    user = await get_current_user(authorization)
    # start bot
    ok, msg = bot_manager.start()
    # broadcast status
    await manager.broadcast({"type": "control", "payload": {"action": "start", "ok": ok, "msg": msg, "user": user}, "ts": int(time.time())})
    return {"ok": ok, "msg": msg}

@app.post("/control/stop")
async def control_stop(authorization: str = Depends(lambda request=None: (request := request) and request.headers.get("Authorization"))):
    user = await get_current_user(authorization)
    ok, msg = bot_manager.stop()
    await manager.broadcast({"type": "control", "payload": {"action": "stop", "ok": ok, "msg": msg, "user": user}, "ts": int(time.time())})
    return {"ok": ok, "msg": msg}

@app.get("/status")
async def status():
    # basic stats + bot status + last event
    cur.execute("SELECT COUNT(*) FROM events")
    total = cur.fetchone()[0]
    bot_info = bot_manager.get_info()
    last_event = last_event_ts
    return {"status": "ok", "events": total, "bot": bot_info, "last_event": last_event}

@app.get("/events")
async def events(limit: int = 50):
    cur.execute("SELECT ts,type,payload FROM events ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    return [{"ts": r[0], "type": r[1], "payload": json.loads(r[2])} for r in rows]

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
            # keepalive - the client may send pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/")
async def index():
    # If frontend index exists, serve it; otherwise return a simple JSON health message
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return JSONResponse({"message": "Void Beast Dashboard API is running", "status": "ok"})
