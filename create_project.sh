# This script creates a scaffold for a log processing system with multiple components.

#!/usr/bin/env bash
set -e

ROOT="$PWD"

mkdir -p "$ROOT"/{log-generator,log-listener,log-collector,persistor-auth,persistor-payment,persistor-system,persistor-application,log-ui}
##########################
# Local - log-generator
##########################
cat > log-generator/app.py <<'PY'
import time, random, os, requests
from datetime import datetime

LISTENER_HOST = os.environ.get("LISTENER_HOST", "log-listener")
LISTENER_PORT = os.environ.get("LISTENER_PORT", "5001")
LISTENER_URL = f"http://{LISTENER_HOST}:{LISTENER_PORT}/logs"

TYPES = ["auth","payment","system","application"]
LEVELS = ["INFO","DEBUG","WARN","ERROR"]

def make_log():
    return {
        "id": random.randint(100000,999999),
        "type": random.choice(TYPES),
        "level": random.choice(LEVELS),
        "message": f"Auto-generated event {random.randint(1,9999)}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "meta": {"host": "local-generator", "pid": random.randint(1000,9999)}
    }

if __name__ == "__main__":
    print("Log generator started — sending to", LISTENER_URL)
    interval = float(os.environ.get("INTERVAL", "1.5"))
    while True:
        payload = make_log()
        try:
            r = requests.post(LISTENER_URL, json=payload, timeout=5)
            print("sent", payload["id"], payload["type"], "->", r.status_code)
        except Exception as e:
            print("send error:", e)
        time.sleep(interval)
PY

cat > log-generator/requirements.txt <<'RQ'
requests
RQ

cat > log-generator/Dockerfile <<'DF'
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
DF

##########################
# Local - log-listener
##########################
cat > log-listener/app.py <<'PY'
from flask import Flask, request, jsonify
import os, requests, time

app = Flask(__name__)
COLLECTOR_URL = os.environ.get("COLLECTOR_URL")  # e.g. http://EC2_PUBLIC/api/collect

if not COLLECTOR_URL:
    print("WARNING: COLLECTOR_URL env not set. Set COLLECTOR_URL when running this container.")

def forward(event):
    retries = int(os.environ.get("RETRIES", "3"))
    backoff = float(os.environ.get("BACKOFF", "1"))
    for i in range(retries):
        try:
            r = requests.post(COLLECTOR_URL, json=event, timeout=5)
            return r.status_code, r.text
        except Exception as e:
            time.sleep(backoff)
            backoff *= 2
    return None, "failed"

@app.route("/health")
def health():
    return {"status":"ok"}, 200

@app.route("/logs", methods=["POST"])
def receive():
    event = request.get_json()
    if not event:
        return {"error":"invalid"}, 400
    status, info = forward(event)
    if status:
        return {"forwarded": True, "status_code": status}, 200
    else:
        return {"forwarded": False, "error": info}, 502

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
PY

cat > log-listener/requirements.txt <<'RQ'
Flask
requests
RQ

cat > log-listener/Dockerfile <<'DF'
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 5001
CMD ["python", "app.py"]
DF

##########################
# Cloud - log-collector
##########################
cat > log-collector/requirements.txt <<'RQ'
Flask
requests
psycopg2-binary
RQ

cat > log-collector/app.py <<'PY'
import os, time, json, requests
from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

app = Flask(__name__)

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB = os.environ.get("POSTGRES_DB", "logsdb")
PG_USER = os.environ.get("POSTGRES_USER", "logs_user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "logs_pass")

PERSISTORS = {
    "auth": os.environ.get("PERSISTOR_AUTH", "persistor-auth"),
    "payment": os.environ.get("PERSISTOR_PAYMENT", "persistor-payment"),
    "system": os.environ.get("PERSISTOR_SYSTEM", "persistor-system"),
    "application": os.environ.get("PERSISTOR_APPLICATION", "persistor-application"),
}
PERSISTOR_PORT = int(os.environ.get("PERSISTOR_PORT", "6000"))

def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)

def wait_for_postgres(retries=15, sleep_sec=2):
    for i in range(retries):
        try:
            conn = get_conn()
            conn.close()
            return True
        except Exception as e:
            print("Waiting for Postgres...", e)
            time.sleep(sleep_sec)
    raise Exception("Postgres not available")

def init_db():
    wait_for_postgres()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        event_id INTEGER,
        type TEXT,
        level TEXT,
        message TEXT,
        timestamp TIMESTAMPTZ,
        meta JSONB
      );
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_log(event):
    conn = get_conn()
    cur = conn.cursor()
    ts = None
    try:
        ts = datetime.fromisoformat(event.get("timestamp").replace("Z","+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)
    cur.execute(
        "INSERT INTO logs (event_id, type, level, message, timestamp, meta) VALUES (%s,%s,%s,%s,%s,%s)",
        (event.get("id"), event.get("type"), event.get("level"),
         event.get("message"), ts, json.dumps(event.get("meta") or {}))
    )
    conn.commit()
    cur.close()
    conn.close()

def route_to_persistor(event):
    p = PERSISTORS.get(event.get("type"))
    if not p:
        return False, "unknown type"
    url = f"http://{p}:{PERSISTOR_PORT}/persist"
    try:
        r = requests.post(url, json=event, timeout=3)
        return (r.status_code == 200), r.text
    except Exception as e:
        return False, str(e)

@app.route("/health")
def health():
    return {"status": "ok"}, 200

@app.route("/collect", methods=["POST"])
def collect():
    event = request.get_json()
    if not event:
        return {"error":"invalid"}, 400
    insert_log(event)
    ok, info = route_to_persistor(event)
    return {"stored": True, "routed": ok, "info": info}, 200

@app.route("/analyze", methods=["GET"])
def analyze():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT type, COUNT(*) FROM logs GROUP BY type")
    rows = cur.fetchall()
    counts = {r[0]: r[1] for r in rows}
    cur.close(); conn.close()
    return jsonify({"counts": counts})

@app.route("/logs", methods=["GET"])
def logs():
    limit = int(request.args.get("limit", "50"))
    since = request.args.get("since")
    sql = "SELECT event_id, type, level, message, timestamp, meta FROM logs"
    params = []
    if since:
        sql += " WHERE timestamp >= %s"
        params.append(since)
    sql += " ORDER BY timestamp DESC LIMIT %s"
    params.append(limit)
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql, params)
    rows = cur.fetchall()
    result = []
    for r in rows:
        result.append({
            "id": r["event_id"],
            "type": r["type"],
            "level": r["level"],
            "message": r["message"],
            "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
            "meta": r["meta"]
        })
    cur.close(); conn.close()
    return jsonify({"logs": result})

if __name__ == "__main__":
    print("Starting collector, waiting for Postgres...")
    init_db()
    app.run(host="0.0.0.0", port=5002)
PY

cat > log-collector/Dockerfile <<'DF'
FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev make \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
ENV PYTHONUNBUFFERED=1
EXPOSE 5002
CMD ["python", "app.py"]
DF

##########################
# Persistors
##########################
for p in persistor-auth persistor-payment persistor-system persistor-application; do
  d="$ROOT/$p"
  cat > "$d/app.py" <<'PY'
from flask import Flask, request, jsonify
import os, json

app = Flask(__name__)
STORE = os.environ.get("STORE_FILE", "/data/logs.json")
os.makedirs(os.path.dirname(STORE), exist_ok=True)

@app.route("/persist", methods=["POST"])
def persist():
    event = request.get_json()
    if not event:
        return {"error": "invalid"}, 400
    try:
        with open(STORE, "a") as f:
            f.write(json.dumps(event) + "\n")
        return {"status": "ok"}, 200
    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/health")
def health():
    return {"status":"ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)
PY

  cat > "$d/requirements.txt" <<'RQ'
Flask
RQ

  cat > "$d/Dockerfile" <<'DF'
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
RUN mkdir -p /data
VOLUME /data
EXPOSE 6000
CMD ["python", "app.py"]
DF
done

##########################
# React UI (minimal files)
##########################
mkdir -p log-ui/src log-ui/public
cat > log-ui/package.json <<'PJ'
{
  "name": "log-ui",
  "version": "1.0.0",
  "private": true,
  "dependencies": {
    "axios": "^1.4.0",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "recharts": "^2.4.0",
    "react-scripts": "5.0.1"
  },
  "scripts": {
    "start": "react-scripts start",
    "build": "react-scripts build"
  }
}
PJ

cat > log-ui/public/index.html <<'HTML'
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Log Dashboard</title>
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
HTML

cat > log-ui/src/index.js <<'JS'
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './index.css';

const root = createRoot(document.getElementById('root'));
root.render(<App />);
JS

cat > log-ui/src/index.css <<'CSS'
body { margin: 0; font-family: Arial, Helvetica, sans-serif; }
button { cursor: pointer; }
CSS

cat > log-ui/src/App.js <<'JS'
import React, { useState, useEffect } from "react";
import axios from "axios";
import { PieChart, Pie, Cell, Tooltip, Legend } from "recharts";

const API = "/api";
const TYPE_COLORS = { auth: "#007bff", payment: "#28a745", system: "#ffc107", application: "#dc3545" };

function App() {
  const [logs, setLogs] = useState([]);
  const [counts, setCounts] = useState({});
  const [loading, setLoading] = useState(false);
  const [filterType, setFilterType] = useState("all");
  const [filterLevel, setFilterLevel] = useState("all");
  const [searchText, setSearchText] = useState("");
  const [timeRange, setTimeRange] = useState("all");

  const fetchData = async () => {
    setLoading(true);
    try {
      const [logsRes, countsRes] = await Promise.all([
        axios.get(`${API}/logs?limit=500`),
        axios.get(`${API}/analyze`)
      ]);
      setLogs(logsRes.data.logs || []);
      setCounts(countsRes.data.counts || {});
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, 30000);
    return () => clearInterval(id);
  }, []);

  const chartData = Object.entries(counts).map(([name, value]) => ({ name, value }));
  const filtered = logs.filter((l) => {
    const typeOk = filterType === "all" || l.type === filterType;
    const levelOk = filterLevel === "all" || l.level === filterLevel;
    const searchOk = !searchText || (l.message || "").toLowerCase().includes(searchText.toLowerCase());
    let timeOk = true;
    if (timeRange !== "all") {
      const t = new Date(l.timestamp).getTime();
      const now = Date.now();
      if (timeRange === "5m") timeOk = now - t <= 5 * 60 * 1000;
      else if (timeRange === "1h") timeOk = now - t <= 60 * 60 * 1000;
      else if (timeRange === "24h") timeOk = now - t <= 24 * 60 * 60 * 1000;
    }
    return typeOk && levelOk && searchOk && timeOk;
  });

  return (
    <div style={{ padding: 20 }}>
      <h2>📊 Log Dashboard</h2>
      <div style={{ marginBottom: 12 }}>
        <button onClick={fetchData} disabled={loading}>{loading ? "Refreshing..." : "🔄 Refresh"}</button>
        <select value={filterType} onChange={(e)=>setFilterType(e.target.value)} style={{ marginLeft: 8 }}>
          <option value="all">All Types</option><option value="auth">Auth</option><option value="payment">Payment</option><option value="system">System</option><option value="application">Application</option>
        </select>
        <select value={filterLevel} onChange={(e)=>setFilterLevel(e.target.value)} style={{ marginLeft: 8 }}>
          <option value="all">All Levels</option><option value="INFO">INFO</option><option value="DEBUG">DEBUG</option><option value="WARN">WARN</option><option value="ERROR">ERROR</option>
        </select>
        <select value={timeRange} onChange={(e)=>setTimeRange(e.target.value)} style={{ marginLeft: 8 }}>
          <option value="all">All time</option><option value="5m">Last 5 min</option><option value="1h">Last 1 hour</option><option value="24h">Last 24 hours</option>
        </select>
        <input placeholder="Search message..." value={searchText} onChange={e=>setSearchText(e.target.value)} style={{ marginLeft: 8, padding: 4 }} />
      </div>

      <div style={{ display: "flex", gap: 24 }}>
        <div style={{ width: 420 }}>
          <h4>Distribution</h4>
          <PieChart width={400} height={300}>
            <Pie data={chartData} dataKey="value" nameKey="name" outerRadius={120} label>
              {chartData.map((entry, idx)=> (<Cell key={idx} fill={TYPE_COLORS[entry.name] || "#888"} />))}
            </Pie>
            <Tooltip />
            <Legend />
          </PieChart>
        </div>

        <div style={{ flex: 1 }}>
          <h4>Logs ({filtered.length})</h4>
          <div style={{ maxHeight: 520, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr style={{ background: "#eee" }}><th style={{ padding: 8 }}>ID</th><th style={{ padding: 8 }}>Type</th><th style={{ padding: 8 }}>Level</th><th style={{ padding: 8 }}>Message</th><th style={{ padding: 8 }}>Timestamp</th></tr></thead>
              <tbody>
                {filtered.map((l,i)=>(
                  <tr key={i} style={{ color: TYPE_COLORS[l.type] || "#000" }}>
                    <td style={{ padding: 8 }}>{l.id}</td>
                    <td style={{ padding: 8 }}>{l.type}</td>
                    <td style={{ padding: 8 }}>{l.level}</td>
                    <td style={{ padding: 8 }}>{l.message}</td>
                    <td style={{ padding: 8 }}>{new Date(l.timestamp).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {filtered.length === 0 && <div style={{ color: "#666", marginTop: 8 }}>No logs match filters.</div>}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
JS

cat > log-ui/nginx.conf <<'NG'
server {
  listen 80;
  server_name _;

  root /usr/share/nginx/html;
  index index.html;

  location /api/ {
    proxy_pass http://log-collector:5002/;
    proxy_set_header Host $host;
    proxy_http_version 1.1;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  }

  location / {
    try_files $uri /index.html;
  }
}
NG

cat > log-ui/Dockerfile <<'DF'
FROM node:18-alpine as build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --silent
COPY public ./public
COPY src ./src
RUN npm run build

FROM nginx:stable-alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
DF

##########################
# Compose files
##########################
cat > docker-compose.local.yml <<'YML'
version: "3.8"
services:
  log-listener:
    build: ./log-listener
    ports:
      - "5001:5001"
    environment:
      - COLLECTOR_URL=${COLLECTOR_URL}
    restart: unless-stopped

  log-generator:
    build: ./log-generator
    depends_on:
      - log-listener
    environment:
      - LISTENER_HOST=log-listener
      - LISTENER_PORT=5001
      - INTERVAL=1.5
    restart: unless-stopped
YML

cat > docker-compose.cloud.yml <<'YML'
version: "3.8"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: logs_user
      POSTGRES_PASSWORD: logs_pass
      POSTGRES_DB: logsdb
    volumes:
      - pgdata:/var/lib/postgresql/data

  log-collector:
    build: ./log-collector
    environment:
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - POSTGRES_DB=logsdb
      - POSTGRES_USER=logs_user
      - POSTGRES_PASSWORD=logs_pass
      - PERSISTOR_AUTH=persistor-auth
      - PERSISTOR_PAYMENT=persistor-payment
      - PERSISTOR_SYSTEM=persistor-system
      - PERSISTOR_APPLICATION=persistor-application
      - PERSISTOR_PORT=6000
    depends_on:
      - postgres
    volumes:
      - collector-data:/data

  persistor-auth:
    build: ./persistor-auth
    environment:
      - STORE_FILE=/data/auth_logs.json
    volumes:
      - persistor-auth-data:/data

  persistor-payment:
    build: ./persistor-payment
    environment:
      - STORE_FILE=/data/payment_logs.json
    volumes:
      - persistor-payment-data:/data

  persistor-system:
    build: ./persistor-system
    environment:
      - STORE_FILE=/data/system_logs.json
    volumes:
      - persistor-system-data:/data

  persistor-application:
    build: ./persistor-application
    environment:
      - STORE_FILE=/data/application_logs.json
    volumes:
      - persistor-application-data:/data

  log-ui:
    build: ./log-ui
    ports:
      - "80:80"
    depends_on:
      - log-collector

volumes:
  pgdata:
  collector-data:
  persistor-auth-data:
  persistor-payment-data:
  persistor-system-data:
  persistor-application-data:
YML

echo "Project scaffold created in $ROOT"
