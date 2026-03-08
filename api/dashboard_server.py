# api/dashboard_server.py
import os
import time
import json
import sqlite3
import logging
from typing import Dict, Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -------------------------
# CONFIG - adjust if needed
# -------------------------
# Secret key the bot will use to POST /ingest
# You can override by setting env var DASHBOARD_KEY if you prefer
DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "ALT_BEAST_03MAR2026_9f3a")

# JWT secret for admin login (optional; login not required for ingest)
JWT_SECRET = os.getenv("JWT_SECRET", "void_beast_9f3a")

# Admin bypass token (frontend currently can use this) - not used for ingest
ADMIN_BYPASS_TOKEN = os.getenv("ADMIN_BYPASS_TOKEN", "12345678")

# DB + frontend folder
DB_PATH = os.getenv("DASHBOARD_DB", "dashboard.db")
FRONTEND_DIR = os.getenv("FRONTEND_DIR", "frontend")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dashboard_server")

# -------------------------
# FASTAPI app + CORS
# -------------------------
app = FastAPI(title="VOID Beast Dashboard (passive ingest mode)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# serve frontend static files if present
if os.path.isdir(FRONTEND_DIR):
    from fastapi.staticfiles import StaticFiles
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# -------------------------
# SQLITE: simple schema
# -------------------------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()

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

# Last event timestamp (used to show "connected")
last_event_ts: int = 0

# -------------------------
# WebSocket manager
# -------------------------
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

# -------------------------
# Helpers: persist & broadcast
# -------------------------
def persist_event(event_type: str, payload: Any):
    """Persist the raw event and do small normalization for trades/analysis/errors/mt5."""
    global last_event_ts
    ts = int(time.time())
    try:
        cur.execute("INSERT INTO events(ts,type,payload) VALUES (?,?,?)", (ts, event_type, json.dumps(payload)))
        conn.commit()
        last_event_ts = ts
    except Exception:
        logger.exception("persist events failed")

    # Specialized handling
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
            logger.exception("persist trade failed")

    if event_type == "analysis":
        try:
            p = payload
            cur.execute(
                "INSERT INTO analyses(ts,symbol,technical,fundamental,sentiment,final_score,meta) VALUES (?,?,?,?,?,?,?)",
                (ts, p.get("symbol"), p.get("technical"), p.get("fundamental"), p.get("sentiment"), p.get("final_score"), json.dumps(p.get("meta", {})))
            )
            conn.commit()
        except Exception:
            logger.exception("persist analysis failed")

    if event_type == "error":
        try:
            p = payload
            cur.execute(
                "INSERT INTO errors(ts,level,message,meta) VALUES (?,?,?,?)",
                (ts, p.get("level", "ERROR"), p.get("message"), json.dumps(p.get("meta", {})))
            )
            conn.commit()
        except Exception:
            logger.exception("persist error failed")

    if event_type in ("mt5_status", "balance", "equity"):
        try:
            key = event_type
            value = json.dumps(payload)
            cur.execute("INSERT INTO mt5_status(ts,key,value) VALUES (?,?,?)", (ts, key, value))
            conn.commit()
        except Exception:
            logger.exception("persist mt5 status failed")

# -------------------------
# Pydantic models
# -------------------------
class IngestModel(BaseModel):
    key: str
    type: str
    payload: dict

# -------------------------
# API: ingest (bot -> dashboard)
# -------------------------
@app.post("/ingest")
async def ingest(item: IngestModel):
    """
    Bot posts events here:
    {
      "key": "<DASHBOARD_KEY>",
      "type": "log" | "trade_open" | "trade_close" | "analysis" | "error" | "mt5_status" | "balance" | "equity" | ...,
      "payload": {...}
    }
    """
    if item.key != DASHBOARD_KEY:
        raise HTTPException(status_code=401, detail="invalid key")
    try:
        persist_event(item.type, item.payload)
    except Exception as e:
        logger.exception("persist failed")
        raise HTTPException(status_code=500, detail=str(e))

    # broadcast to websockets (clients will receive in real-time)
    try:
        await manager.broadcast({"type": item.type, "payload": item.payload, "ts": int(time.time())})
    except Exception:
        logger.exception("broadcast failed")

    return JSONResponse({"status": "ok"})

# -------------------------
# Disabled control endpoints (we run passive ingest mode)
# -------------------------
@app.post("/control/start")
async def control_start():
    # Start/Stop are intentionally disabled in passive mode.
    return JSONResponse({"ok": False, "msg": "start/stop disabled in passive ingest mode; start your bot manually"})

@app.post("/control/stop")
async def control_stop():
    return JSONResponse({"ok": False, "msg": "start/stop disabled in passive ingest mode; stop your bot manually"})

# -------------------------
# Query endpoints used by frontend
# -------------------------
@app.get("/status")
async def status():
    """
    Returns simple dashboard status.
    bot running is derived from last_event_ts (if a recent event was received).
    """
    cur.execute("SELECT COUNT(*) FROM events")
    total = cur.fetchone()[0]
    # Consider bot "running" if last event was within 30 seconds
    running = False
    if last_event_ts and (time.time() - last_event_ts) < 30:
        running = True
    return {
        "status": "ok",
        "events": total,
        "bot": {"running": running, "last_event_ts": last_event_ts},
        "last_event": last_event_ts
    }

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

@app.get("/analysis")
async def analysis(limit: int = 200):
    cur.execute("SELECT ts,symbol,technical,fundamental,sentiment,final_score,meta FROM analyses ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    return [{"ts": r[0], "symbol": r[1], "technical": r[2], "fundamental": r[3], "sentiment": r[4], "final_score": r[5], "meta": json.loads(r[6] or "{}")} for r in rows]

@app.get("/mt5")
async def mt5_status():
    cur.execute("SELECT id,ts,key,value FROM mt5_status ORDER BY id DESC LIMIT 50")
    rows = cur.fetchall()
    out = {}
    for r in rows:
        k = r[2]
        try:
            v = json.loads(r[3])
        except Exception:
            v = r[3]
        # only keep latest per key
        if k not in out:
            out[k] = {"value": v, "ts": r[1]}
    return out

# -------------------------
# WebSocket endpoint (frontend connects here)
# -------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # keepalive: the client may send pings
            try:
                _ = await websocket.receive_text()
            except WebSocketDisconnect:
                break
    finally:
        manager.disconnect(websocket)

# -------------------------
# Serve frontend index or basic health
# -------------------------
@app.get("/")
async def index():
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return JSONResponse({"message": "Void Beast Dashboard (passive ingest) running", "status": "ok"})
