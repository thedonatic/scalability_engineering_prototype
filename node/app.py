from flask import Flask, request, jsonify
import os
import threading
import time
import sqlite3
import requests
import random
import signal
import math
import logging

app = Flask(__name__)

# Configuration
DB_FILE = os.environ.get("DB_FILE", "/data/kv.db")
MAX_IN_FLIGHT = int(os.environ.get("MAX_IN_FLIGHT", 32))
NODE_ID = os.environ.get("NODE_ID", f"node-{random.randint(1, 1e6)}")
NODE_HOST = os.environ.get("NODE_HOST", "localhost")
NODE_PORT = int(os.environ.get("NODE_PORT", 5000))
NODE_ADDR = os.environ.get("NODE_ADDR", f"http://{NODE_HOST}:{NODE_PORT}")
SEED_NODE = os.environ.get("SEED_NODE", None)
REGISTRATION_TTL = 15

# State
in_flight = 0
lock = threading.Lock()

# Gossip state
known_nodes = set([NODE_ADDR])
known_nodes_lock = threading.Lock()
node_states = {}  # addr -> "joining"/"ready"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s"
)
logger = logging.getLogger("node")

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, ts REAL, request_id TEXT)"
    )
    return conn

def gossip_thread():
    while True:
        time.sleep(2)
        with known_nodes_lock:
            peers = list(known_nodes - {NODE_ADDR})
        if not peers:
            continue
        peer = random.choice(peers)
        try:
            with known_nodes_lock:
                payload = {
                    "nodes": list(known_nodes),
                    "states": node_states
                }
            logger.info(f"Gossiping with peer: {peer}")
            resp = requests.post(f"{peer}/gossip", json=payload, timeout=1)
            if resp.status_code == 200:
                data = resp.json()
                their_nodes = set(data.get("nodes", []))
                their_states = data.get("states", {})
                with known_nodes_lock:
                    before = len(known_nodes)
                    known_nodes.update(their_nodes)
                    node_states.update(their_states)
                    after = len(known_nodes)
                if after > before:
                    logger.info(f"Discovered {after - before} new node(s) via gossip.")
                # Visualize cluster membership after gossip
                with known_nodes_lock:
                    cluster_view = ", ".join(sorted(known_nodes))
                logger.info(f"Current cluster: [{cluster_view}]")
        except Exception as e:
            logger.debug(f"Gossip with {peer} failed: {e}")

def join_cluster(seed_addr):
    if not seed_addr or seed_addr == NODE_ADDR:
        return
    try:
        resp = requests.get(f"{seed_addr}/nodes", timeout=2)
        if resp.status_code == 200:
            their_nodes = set(resp.json().get("nodes", []))
            their_states = resp.json().get("states", {})
            with known_nodes_lock:
                known_nodes.update(their_nodes)
                node_states.update(their_states)
                known_nodes.add(seed_addr)
    except Exception:
        pass

@app.route("/gossip", methods=["POST"])
def gossip():
    data = request.get_json()
    their_nodes = set(data.get("nodes", []))
    their_states = data.get("states", {})
    with known_nodes_lock:
        before = len(known_nodes)
        known_nodes.update(their_nodes)
        node_states.update(their_states)
        after = len(known_nodes)
        if after > before:
            logger.info(f"Gossip (incoming): Added {after - before} new node(s).")
        # Visualize cluster membership after gossip
        cluster_view = ", ".join(sorted(known_nodes))
        logger.info(f"Current cluster: [{cluster_view}]")
        # Respond with our view
        return jsonify({"status": "ok", "added": after - before, "nodes": list(known_nodes), "states": node_states})

@app.route("/nodes", methods=["GET"])
def get_nodes():
    with known_nodes_lock:
        return jsonify({"nodes": list(known_nodes), "states": node_states})

def set_state(state):
    with known_nodes_lock:
        node_states[NODE_ADDR] = state

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
    with known_nodes_lock:
        peers = [n for n in known_nodes if n != NODE_ADDR and node_states.get(n) == "ready"]
    if not peers:
        print("No ready peers found; nothing to sync (first node).")
        return True
    local_keys = set(get_all_local_keys())
    for peer in peers:
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

def get_live_ready_nodes():
    with known_nodes_lock:
        return sorted([n for n in known_nodes if node_states.get(n) == "ready"])

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

def deregister(*a):
    # No central registry, just exit
    os._exit(0)

if __name__ == "__main__":
    # Gossip join
    if SEED_NODE and SEED_NODE != NODE_ADDR:
        join_cluster(SEED_NODE)
    set_state("joining")
    threading.Thread(target=gossip_thread, daemon=True).start()
    # Sync from ready peers
    initial_sync()
    # Mark as ready
    set_state("ready")
    print("Node started with ID:", NODE_ID, "at", NODE_ADDR)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, deregister)
    app.run(host="0.0.0.0", port=NODE_PORT)
