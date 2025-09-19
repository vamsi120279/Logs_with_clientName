#!/usr/bin/env python3
from flask import Flask, request
import os, psycopg2, requests
from psycopg2.extras import RealDictCursor
from datetime import datetime
import time
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# Postgres
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "logsdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "logs_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "logs_pass")

# Persistors
PERSISTORS = {
    "auth": os.environ.get("PERSISTOR_AUTH", "http://persistor-auth:6000"),
    "payment": os.environ.get("PERSISTOR_PAYMENT", "http://persistor-payment:6000"),
    "system": os.environ.get("PERSISTOR_SYSTEM", "http://persistor-system:6000"),
    "application": os.environ.get("PERSISTOR_APPLICATION", "http://persistor-application:6000"),
}

# Splunk HEC
SPLUNK_HEC = os.environ.get("SPLUNK_HEC", "http://splunk:8088/services/collector")
SPLUNK_TOKEN = os.environ.get("SPLUNK_TOKEN", "splunk-token")

# Prometheus
log_counter = Counter('logs_total', 'Total logs received', ['level', 'client', 'type'])

ALLOWED_LEVELS = ("ERROR", "WARNING", "INFO", "DEBUG")

def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def init_db():
    sql = """
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        event_id TEXT UNIQUE,
        level TEXT,
        message TEXT,
        client_name TEXT,
        type TEXT,
        timestamp TIMESTAMP WITH TIME ZONE
    );
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def normalize_level(l):
    if not l:
        return "INFO"
    lvl = l.strip().upper()
    return lvl if lvl in ALLOWED_LEVELS else "INFO"

def forward_to_splunk(event):
    headers = {"Authorization": f"Splunk {SPLUNK_TOKEN}"}
    try:
        requests.post(SPLUNK_HEC, json={"event": event}, headers=headers, timeout=3)
    except Exception as e:
        print("⚠️ Failed to send log to Splunk:", e)

@app.route("/health")
def health():
    return {"status":"ok"}, 200

@app.route("/collect", methods=["POST"])
def collect():
    event = request.get_json()
    if not event:
        return {"error":"invalid payload"}, 400

    event_id = event.get("event_id") or str(int(time.time() * 1000))
    level = normalize_level(event.get("level"))
    message = event.get("message", "")
    client_name = event.get("client_name", "unknown")
    log_type = event.get("type", "application")  # default
    ts = event.get("timestamp")

    try:
        timestamp = datetime.fromisoformat(ts) if ts else datetime.utcnow()
    except Exception:
        timestamp = datetime.utcnow()

    try:
        # Save to Postgres
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO logs (event_id, level, message, client_name, type, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, (event_id, level, message, client_name, log_type, timestamp))
        conn.commit()
        cur.close()
        conn.close()

        # Prometheus
        log_counter.labels(level=level, client=client_name, type=log_type).inc()

        # Forward to persistors
        persistor_url = PERSISTORS.get(log_type)
        if persistor_url:
            try:
                requests.post(f"{persistor_url}/store", json=event, timeout=3)
            except Exception as e:
                print(f"⚠️ Failed to forward to {log_type} persistor:", e)

        # Forward to Splunk
        forward_to_splunk(event)

        return {"status":"ok"}, 200
    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/logs", methods=["GET"])
def get_logs():
    limit = int(request.args.get("limit", "500"))
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT event_id, level, message, client_name, type, timestamp FROM logs ORDER BY timestamp DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    for r in rows:
        r['timestamp'] = r['timestamp'].isoformat()
    return {"logs": rows}, 200

@app.route("/analyze", methods=["GET"])
def analyze():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT level, COUNT(*) FROM logs GROUP BY level;")
    rows = cur.fetchall()
    stats = {r[0]: r[1] for r in rows}
    cur.close()
    conn.close()
    return {"counts": stats}, 200

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

if __name__ == "__main__":
    for i in range(10):
        try:
            init_db()
            break
        except Exception as e:
            print("Waiting for Postgres...", e)
            time.sleep(2)
    app.run(host="0.0.0.0", port=5002)
