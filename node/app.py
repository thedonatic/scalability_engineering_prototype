from flask import Flask, request, jsonify
import os
import threading
import time
import sqlite3
import requests
import random
import signal
import logging
import hashlib
import bisect

app = Flask(__name__)

DB_FILE = os.environ.get("DB_FILE", "/data/kv.db")
NODE_HOST = os.environ.get("NODE_HOST", "localhost")
NODE_PORT = int(os.environ.get("NODE_PORT", 5000))
NODE_ADDR = os.environ.get("NODE_ADDR", f"http://{NODE_HOST}:{NODE_PORT}")
SEED_NODE = os.environ.get("SEED_NODE", None)
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", 3))
NUM_VNODES = int(os.environ.get("NUM_VNODES", 16))

known_nodes = set([NODE_ADDR])
known_nodes_lock = threading.Lock()
node_states = {}
node_last_seen = {NODE_ADDR: time.time()}
dead_nodes = set()
dead_nodes_lock = threading.Lock()

MAX_IN_FLIGHT = int(os.environ.get("MAX_IN_FLIGHT", 8))
in_flight = 0
in_flight_lock = threading.Lock()
db_write_lock = threading.Lock()

DEAD_TIMEOUT = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s"
)
logger = logging.getLogger("node")

def set_state(state):
    with known_nodes_lock:
        node_states[NODE_ADDR] = state

def get_all_local_keys():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, ts REAL, request_id TEXT)")
    cur = conn.execute("SELECT key FROM kv")
    keys = [row[0] for row in cur.fetchall()]
    conn.close()
    return keys

def internal_set_local(key, value, ts, req_id):
    with db_write_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, ts REAL, request_id TEXT)")
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
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.execute("SELECT value, ts, request_id FROM kv WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    if row:
        value, ts, req_id = row
        return {"value": value, "ts": ts, "request_id": req_id}
    return None

def with_node_load_shedding(fn):
    def wrapper(*args, **kwargs):
        global in_flight
        with in_flight_lock:
            if in_flight >= MAX_IN_FLIGHT:
                return jsonify({"error": "node overloaded"}), 503
            in_flight += 1
        try:
            return fn(*args, **kwargs)
        finally:
            with in_flight_lock:
                in_flight -= 1
    wrapper.__name__ = fn.__name__
    return wrapper

@app.route("/internal/set", methods=["POST"])
@with_node_load_shedding
def internal_set():
    data = request.json
    key = data["key"]
    value = data["value"]
    ts = float(data["ts"])
    req_id = data.get("request_id")
    result = internal_set_local(key, value, ts, req_id)
    # logger.info("Replicated key %s with value %s on node %s", key, value, NODE_ADDR)
    return jsonify({"result": "replicated" if result else "old_write_ignored"})

@app.route("/internal/get", methods=["GET"])
@with_node_load_shedding
def internal_get():
    key = request.args["key"]
    value = internal_get_local(key)
    # logger.info("Read key %s with value %s on node %s", key, value, NODE_ADDR)
    return jsonify({"key": key, "value": value})

@app.route("/internal/get_many", methods=["POST"])
@with_node_load_shedding
def internal_get_many():
    keys = request.json.get("keys", [])
    result = {}
    for key in keys:
        val = internal_get_local(key)
        if val:
            result[key] = val
    return jsonify(result)

@app.route("/internal/all_keys", methods=["GET"])
def internal_all_keys():
    keys = get_all_local_keys()
    return jsonify({"keys": keys})

@app.route("/nodes", methods=["GET"])
def get_nodes():
    with known_nodes_lock, dead_nodes_lock:
        return jsonify({
            "nodes": list(known_nodes),
            "states": node_states,
            "dead_nodes": list(dead_nodes)
        })

@app.route("/gossip", methods=["POST"])
def gossip():
    data = request.get_json()
    their_nodes = set(data.get("nodes", []))
    their_states = data.get("states", {})
    their_dead = set(data.get("dead_nodes", []))
    membership_changed = False
    with known_nodes_lock, dead_nodes_lock:
        added_dead = their_dead - dead_nodes
        if added_dead:
            logger.info(f"Gossip: learned about {len(added_dead)} dead nodes.")
        dead_nodes.update(their_dead)
        for dn in dead_nodes:
            if dn in known_nodes:
                known_nodes.discard(dn)
                node_states.pop(dn, None)
        before_nodes = set(known_nodes)
        added = {n for n in their_nodes if n not in dead_nodes} - known_nodes
        known_nodes.update(added)
        for node, state in their_states.items():
            if node not in dead_nodes and node_states.get(node) != state:
                node_states[node] = state
                membership_changed = True
        after_nodes = set(known_nodes)
        if before_nodes != after_nodes or added_dead:
            membership_changed = True
        if membership_changed:
            logger.info(f"Gossip (incoming): membership changed.")
        if added:
            logger.info(f"Gossip (incoming): Added {len(added)} new node(s).")
        cluster_view = ", ".join(sorted(known_nodes))
        return jsonify({
            "status": "ok",
            "added": len(added),
            "nodes": list(known_nodes),
            "states": node_states,
            "dead_nodes": list(dead_nodes)
        })

def get_live_ready_nodes():
    with known_nodes_lock, dead_nodes_lock:
        return sorted([n for n in known_nodes if node_states.get(n) == "ready" and n not in dead_nodes])

def get_hash(val):
    return int(hashlib.sha1(val.encode()).hexdigest(), 16)

def get_hash_ring():
    nodes = get_live_ready_nodes()
    if not nodes:
        return [], []
    ring = []
    node_refs = []
    for n in nodes:
        for v in range(NUM_VNODES):
            label = f"{n}-vn{v}"
            h = get_hash(label)
            ring.append(h)
            node_refs.append(n)
    zipped = sorted(zip(ring, node_refs))
    if zipped:
        ring, node_refs = zip(*zipped)
        return list(ring), list(node_refs)
    return [], []

def get_owner_nodes(key, rf=REPLICATION_FACTOR):
    ring, node_refs = get_hash_ring()
    if not ring:
        return []
    key_hash = get_hash(key)
    idx = bisect.bisect(ring, key_hash)
    owners = []
    seen = set()
    for i in range(len(ring)):
        pos = (idx + i) % len(ring)
        node = node_refs[pos]
        if node not in seen:
            owners.append(node)
            seen.add(node)
            if len(owners) == rf:
                break
    return owners

def gossip_thread():
    session = requests.Session()
    while True:
        time.sleep(2)
        with known_nodes_lock, dead_nodes_lock:
            peers = list(known_nodes - {NODE_ADDR})
            gossip_payload = {
                "nodes": list(known_nodes),
                "states": node_states,
                "dead_nodes": list(dead_nodes)
            }
        if not peers:
            continue
        fanout = min(3, max(1, int(len(peers) ** 0.5)))
        selected_peers = random.sample(peers, fanout)
        for peer in selected_peers:
            try:
                resp = session.post(f"{peer}/gossip", json=gossip_payload, timeout=1)
                if resp.status_code == 200:
                    data = resp.json()
                    their_nodes = set(data.get("nodes", []))
                    their_states = data.get("states", {})
                    their_dead = set(data.get("dead_nodes", []))
                    membership_changed = False
                    before_nodes = set(known_nodes)
                    dead_nodes.update(their_dead)
                    for dn in dead_nodes:
                        known_nodes.discard(dn)
                        node_states.pop(dn, None)
                    added = {n for n in their_nodes if n not in dead_nodes} - known_nodes
                    known_nodes.update(added)
                    for node, state in their_states.items():
                        if node not in dead_nodes and node_states.get(node) != state:
                            node_states[node] = state
                            membership_changed = True
                    after_nodes = set(known_nodes)
                    if before_nodes != after_nodes or their_dead:
                        membership_changed = True
            except Exception as e:
                logger.debug(f"Gossip with {peer} failed: {e}")

def health_check_thread():
    while True:
        time.sleep(5)
        dead_now = []
        with known_nodes_lock, dead_nodes_lock:
            peers = list(known_nodes - {NODE_ADDR})
        for peer in peers:
            try:
                resp = requests.get(f"{peer}/status", timeout=2)
                if resp.status_code == 200:
                    node_last_seen[peer] = time.time()
            except Exception:
                if time.time() - node_last_seen.get(peer, 0) > DEAD_TIMEOUT:
                    logger.info(f"Health check: node {peer} considered dead.")
                    dead_now.append(peer)
        if dead_now:
            with known_nodes_lock, dead_nodes_lock:
                for dn in dead_now:
                    known_nodes.discard(dn)
                    node_states.pop(dn, None)
                    dead_nodes.add(dn)
                    node_last_seen.pop(dn, None)

@app.route("/status", methods=["GET"])
def status():
    with known_nodes_lock, dead_nodes_lock, in_flight_lock:
        return jsonify({
            "nodes": list(known_nodes),
            "node_states": node_states,
            "dead_nodes": list(dead_nodes),
            "in_flight": in_flight,
            "max_in_flight": MAX_IN_FLIGHT,
        })

def join_cluster(seed_addr):
    if not seed_addr or seed_addr == NODE_ADDR:
        return
    try:
        resp = requests.get(f"{seed_addr}/nodes", timeout=2)
        if resp.status_code == 200:
            resp_json = resp.json()
            their_nodes = set(resp_json.get("nodes", []))
            their_states = resp_json.get("states", {})
            their_dead = set(resp_json.get("dead_nodes", []))
            with known_nodes_lock, dead_nodes_lock:
                known_nodes.update(their_nodes)
                node_states.update(their_states)
                dead_nodes.update(their_dead)
                known_nodes.add(seed_addr)
                for dn in dead_nodes:
                    known_nodes.discard(dn)
                    node_states.pop(dn, None)
    except Exception:
        pass

def initial_sync_and_anti_entropy():
    with known_nodes_lock, dead_nodes_lock:
        peers = [n for n in known_nodes if n != NODE_ADDR and node_states.get(n) == "ready" and n not in dead_nodes]
    needed_keys = set()
    for peer in peers:
        try:
            resp = requests.get(f"{peer}/internal/all_keys", timeout=10)
            peer_keys = set(resp.json().get("keys", []))
            for key in peer_keys:
                if NODE_ADDR in get_owner_nodes(key):
                    needed_keys.add(key)
        except Exception as e:
            print(f"Sync error from {peer}:", e)
    local_keys = set(get_all_local_keys())
    missing = list(needed_keys - local_keys)
    for peer in peers:
        if not missing:
            break
        try:
            fetch = [k for k in missing if NODE_ADDR in get_owner_nodes(k)]
            if fetch:
                val_resp = requests.post(f"{peer}/internal/get_many", json={"keys": fetch}, timeout=10)
                values = val_resp.json()
                for key, v in values.items():
                    internal_set_local(key, v["value"], v["ts"], v.get("request_id"))
                missing = [k for k in missing if k not in values]
        except Exception as e:
            print(f"Fetch error from {peer}:", e)
    set_state("ready")

def anti_entropy_thread():
    session = requests.Session()
    while True:
        time.sleep(10)
        with known_nodes_lock, dead_nodes_lock:
            peers = [n for n in known_nodes if n != NODE_ADDR and node_states.get(n) == "ready" and n not in dead_nodes]
        if not peers:
            continue
        local_keys = set(get_all_local_keys())
        for peer in peers:
            try:
                resp = session.get(f"{peer}/internal/all_keys", timeout=5)
                peer_keys = set(resp.json().get("keys", []))
                for key in peer_keys:
                    owners = get_owner_nodes(key)
                    if NODE_ADDR in owners:
                        local_val = internal_get_local(key)
                        need = False
                        if not local_val:
                            need = True
                        else:
                            val_resp = session.get(f"{peer}/internal/get", params={"key": key}, timeout=3)
                            if val_resp.status_code == 200 and val_resp.json().get("value"):
                                remote_val = val_resp.json()["value"]
                                if remote_val["ts"] > local_val["ts"]:
                                    need = True
                        if need:
                            val_resp = session.get(f"{peer}/internal/get", params={"key": key}, timeout=3)
                            if val_resp.status_code == 200 and val_resp.json().get("value"):
                                v = val_resp.json()["value"]
                                internal_set_local(key, v["value"], v["ts"], v.get("request_id"))
            except Exception as e:
                logger.debug(f"Anti-entropy sync with {peer} failed: {e}")

def deregister(*a):
    os._exit(0)

if __name__ == "__main__":
    if SEED_NODE and SEED_NODE != NODE_ADDR:
        join_cluster(SEED_NODE)
    set_state("joining")
    threading.Thread(target=gossip_thread, daemon=True).start()
    threading.Thread(target=health_check_thread, daemon=True).start()
    threading.Thread(target=anti_entropy_thread, daemon=True).start()
    initial_sync_and_anti_entropy()
    set_state("ready")
    print("Node started at", NODE_ADDR)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, deregister)
    logFlask = logging.getLogger('werkzeug')
    logFlask.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=NODE_PORT)
