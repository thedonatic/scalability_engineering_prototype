from flask import Flask, request, jsonify
import os
import threading
import time
import sqlite3
import requests
import random
import redis
import signal
import math
from redis.sentinel import Sentinel

app = Flask(__name__)

# Configuration
DB_FILE = os.environ.get("DB_FILE", "/data/kv.db")
MAX_IN_FLIGHT = int(os.environ.get("MAX_IN_FLIGHT", 32))
NODE_ID = os.environ.get("NODE_ID", f"node-{random.randint(1, 1e6)}")
NODE_ADDR = os.environ.get("NODE_ADDR", "http://localhost:5000")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REGISTRATION_TTL = 15

# State
in_flight = 0
lock = threading.Lock()
# sentinel = Sentinel([('sentinel', 26379)], socket_timeout=2)
# r = sentinel.master_for('master', password='password', decode_responses=True)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, ts REAL, request_id TEXT)"
    )
    return conn

def register(state="joining"):
    while True:
        try:
            r.hset(f"node:{NODE_ID}", mapping={"addr": NODE_ADDR, "state": state})
            r.expire(f"node:{NODE_ID}", REGISTRATION_TTL)
            print(f"Node {NODE_ID} registered with state '{state}' at {NODE_ADDR}")
        except Exception as e:
            print("Redis registration error:", e)
            time.sleep(REGISTRATION_TTL)
        time.sleep(REGISTRATION_TTL // 2)

def deregister(*a):
    try:
        r.delete(f"node:{NODE_ID}")
    except Exception as e:
        print("Redis deregistration error:", e)
    os._exit(0)

def get_all_local_keys():
    conn = get_db()
    cur = conn.execute("SELECT key FROM kv")
    keys = [row[0] for row in cur.fetchall()]
    conn.close()
    return keys

def internal_set_local(key, value, ts, req_id):
    conn = get_db()
    cur = conn.execute("SELECT ts, request_id FROM kv WHERE key = ?", (key,))
    row = cur.fetchone()
    if row:
        prev_ts, prev_req = row
        if prev_req == req_id:
            conn.close()
            return True
        if ts < prev_ts:
            conn.close()
            return False
    conn.execute(
        "REPLACE INTO kv (key, value, ts, request_id) VALUES (?, ?, ?, ?)",
        (key, value, ts, req_id),
    )
    conn.commit()
    conn.close()
    return True

def internal_get_local(key):
    conn = get_db()
    cur = conn.execute("SELECT value, ts, request_id FROM kv WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    if row:
        value, ts, req_id = row
        return {"value": value, "ts": ts, "request_id": req_id}
    return None

@app.route("/internal/set", methods=["POST"])
def internal_set():
    data = request.json
    key = data["key"]
    value = data["value"]
    ts = float(data["ts"])
    req_id = data.get("request_id")
    result = internal_set_local(key, value, ts, req_id)
    return jsonify({"result": "replicated" if result else "old_write_ignored"})

@app.route("/internal/get", methods=["GET"])
def internal_get():
    key = request.args["key"]
    val = internal_get_local(key)
    return jsonify({"key": key, "value": val})

@app.route("/internal/all_keys", methods=["GET"])
def internal_all_keys():
    keys = get_all_local_keys()
    return jsonify({"keys": keys})

def initial_sync():
    # Discover existing ready nodes
    node_keys = r.keys("node:*")
    peers = []
    for k in node_keys:
        node_info = r.hgetall(k)
        if node_info.get("state") == "ready":
            peers.append(node_info["addr"])
    if not peers:
        print("No ready peers found; nothing to sync (first node).")
        return True
    local_keys = set(get_all_local_keys())
    for peer in peers:
        if peer == NODE_ADDR:
            continue
        try:
            resp = requests.get(f"{peer}/internal/all_keys", timeout=10)
            peer_keys = set(resp.json().get("keys", []))
            missing = peer_keys - local_keys
            for key in missing:
                val_resp = requests.get(f"{peer}/internal/get", params={"key": key}, timeout=3)
                if val_resp.status_code == 200 and val_resp.json().get("value"):
                    v = val_resp.json()["value"]
                    internal_set_local(key, v["value"], v["ts"], v.get("request_id"))
        except Exception as e:
            print(f"Sync error from {peer}:", e)
    print("Initial sync complete.")
    return True

def post_sync_set_ready():
    try:
        r.hset(f"node:{NODE_ID}", "state", "ready")
    except Exception as e:
        print("Could not set node state to ready:", e)

def get_live_ready_nodes():
    node_keys = r.keys("node:*")
    result = []
    for k in node_keys:
        node_info = r.hgetall(k)
        if node_info.get("state") == "ready":
            result.append(node_info["addr"])
    return sorted(set(result))

def compute_quorums():
    nodes = get_live_ready_nodes()
    N = len(nodes)
    W = max(math.ceil(N / 2), 1)
    R = max(N - W + 1, 1)
    return nodes, N, W, R

def choose_nodes(client_id, n, nodes):
    random.seed(hash(client_id))
    return random.sample(nodes, min(n, len(nodes)))

def with_overload_protection(func):
    def wrapper(*args, **kwargs):
        global in_flight
        with lock:
            if in_flight >= MAX_IN_FLIGHT:
                return jsonify({"error": "overloaded"}), 503
            in_flight += 1
        try:
            return func(*args, **kwargs)
        finally:
            with lock:
                in_flight -= 1
    wrapper.__name__ = func.__name__
    return wrapper

@app.route("/set", methods=["POST"])
@with_overload_protection
def coordinator_set():
    data = request.json
    key = data["key"]
    value = data["value"]
    req_id = data.get("request_id", f"{key}-{random.randint(1,1e9)}")
    ts = float(data.get("ts", time.time()))
    client_id = request.headers.get("X-Client-ID", "anon")

    nodes, N, W, R = compute_quorums()
    chosen = choose_nodes(client_id, N, nodes)
    successes = 0
    errors = []
    for node_addr in chosen:
        if node_addr == NODE_ADDR:
            result = internal_set_local(key, value, ts, req_id)
            if result:
                successes += 1
        else:
            try:
                rnode = requests.post(
                    f"{node_addr}/internal/set", json={
                        "key": key, "value": value, "ts": ts, "request_id": req_id
                    }, timeout=1
                )
                if rnode.status_code == 200:
                    successes += 1
            except Exception as e:
                errors.append(f"{node_addr}: {e}")
    if successes >= W:
        return jsonify({"result": "ok", "successes": successes})
    else:
        return (
            jsonify({"result": "write_failed", "successes": successes, "errors": errors}),
            503,
        )

@app.route("/get", methods=["GET"])
@with_overload_protection
def coordinator_get():
    key = request.args["key"]
    client_id = request.headers.get("X-Client-ID", "anon")

    nodes, N, W, R = compute_quorums()
    chosen = choose_nodes(client_id, N, nodes)
    results = []
    for node_addr in chosen:
        if node_addr == NODE_ADDR:
            v = internal_get_local(key)
            if v:
                results.append(v)
        else:
            try:
                rnode = requests.get(
                    f"{node_addr}/internal/get", params={"key": key}, timeout=1
                )
                if rnode.status_code == 200:
                    resp = rnode.json()
                    v = resp.get("value")
                    if v:
                        results.append(v)
            except Exception:
                pass
    if not results:
        return jsonify({"result": "not_found"}), 404
    latest = max(results, key=lambda x: x["ts"])
    return jsonify({"key": key, "value": latest["value"], "ts": latest["ts"]})

if __name__ == "__main__":
    # Register as joining
    threading.Thread(target=register, args=("joining",), daemon=True).start()
    # Sync from ready peers
    initial_sync()
    # Mark as ready
    post_sync_set_ready()
    # Start normal registration (state "ready")
    threading.Thread(target=register, args=("ready",), daemon=True).start()
    print("Node started with ID:", NODE_ID, "at", NODE_ADDR)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, deregister)
    app.run(host="0.0.0.0", port=5000)