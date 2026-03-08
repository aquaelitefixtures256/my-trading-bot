// frontend/src/App.jsx
import React, { useEffect, useRef, useState } from "react";
import axios from "axios";
import { Line } from "react-chartjs-2";
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend } from "chart.js";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

// Use the backend host directly so dev (vite) and backend (uvicorn) connect reliably
const API_BASE = "http://127.0.0.1:8000";
const WS_URL = "ws://127.0.0.1:8000/ws";

export default function App() {
  const [connected, setConnected] = useState(false);
  const [tradesOpen, setTradesOpen] = useState([]);
  const [tradesClosed, setTradesClosed] = useState([]);
  const [logs, setLogs] = useState([]);
  const [analysis, setAnalysis] = useState({});
  const [botInfo, setBotInfo] = useState({ running: false, pid: null });
  // Pre-set token so UI skips login (you can remove this to enable login flow)
  const [token, setToken] = useState("admin-token");
  const wsRef = useRef(null);
  const [pnlSeries, setPnlSeries] = useState([]); // array of {ts, profit}
  const [ticker, setTicker] = useState("");
  const [loginBusy, setLoginBusy] = useState(false);

  useEffect(() => {
    connectWS();
    fetchStatus();
    const t = setInterval(fetchStatus, 8000);
    return () => {
      clearInterval(t);
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch (e) {}
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function connectWS() {
    try {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        setConnected(true);
        addLog("INFO", "WS connected");
      };

      ws.onclose = () => {
        setConnected(false);
        addLog("WARN", "WS disconnected — retrying");
        // attempt reconnect
        setTimeout(connectWS, 1500);
      };

      ws.onerror = (e) => {
        console.error("WS error", e);
      };

      ws.onmessage = (evt) => {
        try {
          const msg = JSON.parse(evt.data);
          handleEvent(msg);
        } catch (err) {
          console.error("Failed to parse WS message", err, evt.data);
        }
        // optional keepalive reply
        try {
          ws.send("pong");
        } catch (e) {}
      };
    } catch (e) {
      console.error(e);
      setConnected(false);
    }
  }

  function handleEvent(msg) {
    if (!msg || !msg.type) return;
    if (msg.type === "log") {
      addLog(msg.payload.level || "INFO", msg.payload.message || JSON.stringify(msg.payload));
    } else if (msg.type === "error") {
      addLog(msg.payload.level || "ERROR", msg.payload.message || JSON.stringify(msg.payload));
    } else if (msg.type === "trade_open") {
      const p = msg.payload || {};
      setTradesOpen((prev) => [p, ...prev].slice(0, 200));
      setTicker(`${p.symbol ?? ""} ${p.direction ?? p.side ?? ""} ${p.lot ?? ""}`);
    } else if (msg.type === "trade_close") {
      const p = msg.payload || {};
      setTradesClosed((prev) => [p, ...prev].slice(0, 400));
      setTradesOpen((prev) => prev.filter((t) => !(t.symbol === p.symbol && (t.open_price === p.close_price || t.lot === p.lot))));
      setPnlSeries((prev) => {
        const out = [...prev, { ts: Date.now(), profit: Number(p.profit ?? 0) }];
        return out.slice(-120);
      });
      setTicker(`${p.symbol ?? ""} closed PnL ${p.profit ?? 0}`);
    } else if (msg.type === "analysis") {
      const p = msg.payload || {};
      if (p.symbol) {
        setAnalysis((prev) => ({ ...prev, [p.symbol]: p }));
      }
    } else if (msg.type === "control") {
      addLog("CTL", JSON.stringify(msg.payload));
      fetchStatus();
    }
  }

  function addLog(level, message) {
    const entry = { level, message: String(message), ts: Date.now() };
    setLogs((prev) => [entry, ...prev].slice(0, 800));
  }

  async function fetchStatus() {
    try {
      const r = await axios.get(API_BASE + "/status");
      if (r?.data) {
        setBotInfo(r.data.bot || { running: false, pid: null });
        if (r.data.last_event && Date.now() / 1000 - r.data.last_event < 30) {
          setTicker("Connected to void_beast.py");
        }
      }
    } catch (e) {
      // quietly fail — server may restart
    }
  }

  async function doLogin(user, pass) {
    setLoginBusy(true);
    try {
      const r = await axios.post(API_BASE + "/auth/login", { username: user, password: pass });
      setToken(r.data.access_token);
      addLog("INFO", "Admin logged in");
    } catch (e) {
      addLog("ERROR", "Login failed");
      console.error(e);
    } finally {
      setLoginBusy(false);
    }
  }

  async function startBot() {
    if (!token) {
      addLog("ERROR", "Admin token required to start");
      return;
    }
    try {
      await axios.post(API_BASE + "/control/start", {}, { headers: { Authorization: "Bearer " + token } });
      addLog("INFO", "Start command sent");
      fetchStatus();
    } catch (e) {
      addLog("ERROR", "Start failed");
      console.error(e);
    }
  }

  async function stopBot() {
    if (!token) {
      addLog("ERROR", "Admin token required to stop");
      return;
    }
    try {
      await axios.post(API_BASE + "/control/stop", {}, { headers: { Authorization: "Bearer " + token } });
      addLog("INFO", "Stop command sent");
      fetchStatus();
    } catch (e) {
      addLog("ERROR", "Stop failed");
      console.error(e);
    }
  }

  // Chart data for PnL
  const pnlData = {
    labels: pnlSeries.map((p) => {
      const d = new Date(p.ts);
      return d.toLocaleTimeString();
    }),
    datasets: [
      {
        label: "PnL",
        data: pnlSeries.map((p) => p.profit),
        fill: true,
      },
    ],
  };

  // Use two fallback image paths: /void-bg.jpg (public root) and /assets/void-bg.jpg (public/assets/)
  const backgroundStyle = {
    minHeight: "100vh",
    backgroundImage: "url('/void-bg.jpg'), url('/assets/void-bg.jpg')",
    backgroundSize: "cover",
    backgroundPosition: "center",
    backgroundRepeat: "no-repeat",
  };

  return (
    <div className="min-h-screen p-6" style={backgroundStyle}>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <div className="header-logo text-beast-green text-2xl">VOID BEAST</div>
          <div className="ticker text-sm">{ticker}</div>
        </div>

        <div className="flex items-center gap-3">
          <div className="px-3 py-1 card small">
            Status:{" "}
            {connected ? <span className="text-green-400">Connected</span> : <span className="text-red-400">Disconnected</span>}
          </div>
          <div className="px-3 py-1 card small">Bot: {botInfo.running ? "Running" : "Stopped"}</div>
        </div>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* Main column */}
        <div className="col-span-2 space-y-4">
          {/* Trade Monitor */}
          <div className="card p-4">
            <h3 className="mb-2 font-bold">Trade Monitor</h3>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <h4 className="text-sm text-gray-300">Open Trades</h4>
                <ul className="text-sm mt-2">
                  {tradesOpen.length === 0 && <li className="text-gray-500">No open trades</li>}
                  {tradesOpen.map((t, i) => (
                    <li key={i} className="py-1 border-b border-white/5 flex justify-between">
                      <div>
                        <span className="font-semibold mr-2">{t.symbol}</span>
                        <span className="text-xs text-gray-400">{t.direction ?? t.side}</span>
                      </div>
                      <div className="text-sm text-gray-300">{t.lot ?? t.lots}</div>
                    </li>
                  ))}
                </ul>
              </div>

              <div>
                <h4 className="text-sm text-gray-300">Closed Trades</h4>
                <ul className="text-sm mt-2">
                  {tradesClosed.length === 0 && <li className="text-gray-500">No closed trades</li>}
                  {tradesClosed.map((t, i) => (
                    <li key={i} className="py-1 border-b border-white/5 flex justify-between">
                      <div>
                        <span className="font-semibold mr-2">{t.symbol}</span>
                      <span className="text-xs text-gray-400">closed</span>
                      </div>
                      <div className={`text-sm ${Number(t.profit) >= 0 ? "text-beast-green" : "text-red-400"}`}>{t.profit ?? 0}</div>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </div>

          {/* Analysis Panel */}
          <div className="card p-4">
            <h3 className="mb-2 font-bold">Analysis Panel</h3>
            <div className="grid grid-cols-3 gap-3">
              {Object.keys(analysis).length === 0 && <div className="col-span-3 text-sm text-gray-400">No analysis yet</div>}
              {Object.entries(analysis).map(([sym, a]) => (
                <div key={sym} className="p-3 card rounded">
                  <div className="font-bold">{sym}</div>
                  <div className="text-sm">Tech: {a.technical ?? "-"}</div>
                  <div className="text-sm">Fund: {a.fundamental ?? "-"}</div>
                  <div className="text-sm">Sent: {a.sentiment ?? "-"}</div>
                  <div className="text-sm mt-2">Score: {a.final_score ?? "-"}</div>
                </div>
              ))}
            </div>
          </div>

          {/* PnL Chart */}
          <div className="card p-4">
            <h3 className="mb-2 font-bold">PnL (recent closed trades)</h3>
            <div>
              <Line data={pnlData} />
            </div>
          </div>
        </div>

        {/* Right column */}
        <div className="space-y-4">
          {/* Risk Monitor */}
          <div className="card p-4">
            <h3 className="mb-2 font-bold">Risk Monitor</h3>
            <div className="text-sm">
              Dynamic risk: <strong>Auto</strong>
            </div>
            <div className="text-sm mt-2">Thresholds: 0.18 | Current exposure: 0.12</div>
            <div className="mt-3 h-2 bg-white/5 rounded">
              <div style={{ width: "36%" }} className="h-2 bg-beast-red rounded" />
            </div>
          </div>

          {/* Error Monitor */}
          <div className="card p-4">
            <h3 className="mb-2 font-bold">Error Monitor</h3>
            <ul className="text-sm max-h-40 overflow-auto">
              {logs.length === 0 && <li className="text-gray-400">No logs yet</li>}
              {logs.map((l, i) => (
                <li key={i} className="py-1">
                  <span className={l.level === "ERROR" ? "text-red-400" : "text-gray-300"}>[{l.level}]</span> {l.message}
                </li>
              ))}
            </ul>
          </div>

          {/* Controls */}
          <div className="card p-4">
            <h3 className="mb-2 font-bold">Controls</h3>
            <div className="flex gap-2 items-center">
              {!token ? (
                <LoginForm onLogin={doLogin} busy={loginBusy} />
              ) : (
                <>
                  <button onClick={startBot} className="px-3 py-2 bg-green-500 rounded">
                    Start
                  </button>
                  <button onClick={stopBot} className="px-3 py-2 bg-red-600 rounded">
                    Stop
                  </button>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      <footer className="mt-6 text-center small text-gray-400">VOID Beast Trading Command Center</footer>
    </div>
  );
}

/* Simple LoginForm component used above */
function LoginForm({ onLogin, busy }) {
  const [u, setU] = useState("admin");
  const [p, setP] = useState("password");

  return (
    <div className="flex gap-2 items-center">
      <input value={u} onChange={(e) => setU(e.target.value)} className="p-2 rounded bg-black/20 text-sm" placeholder="username" />
      <input value={p} onChange={(e) => setP(e.target.value)} type="password" className="p-2 rounded bg-black/20 text-sm" placeholder="password" />
      <button onClick={() => onLogin(u, p)} className="px-3 py-2 bg-beast-green rounded text-black font-semibold" disabled={busy}>
        {busy ? "..." : "Login"}
      </button>
    </div>
  );
  }
