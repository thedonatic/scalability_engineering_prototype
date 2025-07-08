from flask import Flask, request, jsonify
import os
import threading
import time
import requests
import random
import hashlib
import bisect
import logging

app = Flask(__name__)

SEED_NODE = os.environ.get("SEED_NODE", "http://node:5000")
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", 3))
NUM_VNODES = int(os.environ.get("NUM_VNODES", 16))
RING_UPDATE_INTERVAL = int(os.environ.get("RING_UPDATE_INTERVAL", 2))
RING_STABLE_PERIOD = int(os.environ.get("RING_STABLE_PERIOD", 5))

IN_FLIGHT_LIMIT = int(os.environ.get("GATEWAY_IN_FLIGHT_LIMIT", 100))
in_flight = 0
in_flight_lock = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s"
)
logger = logging.getLogger("gateway")
known_nodes = set()
node_states = {}
last_ring_update = 0
lock = threading.Lock()

def get_hash(val):
    return int(hashlib.sha1(val.encode()).hexdigest(), 16)

def build_hash_ring():
    with lock:
        ready_nodes = sorted([n for n in known_nodes if node_states.get(n) == "ready"])
    if not ready_nodes:
        return [], []
    ring = []
    node_refs = []
    for n in ready_nodes:
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
    ring, node_refs = build_hash_ring()
    if not ring:
        logger.info(f"[owner_nodes] No ring built! Returning [].")
        return []
    key_hash = get_hash(key)
    idx = bisect.bisect(ring, key_hash)

    # Log the ring's contents
    ring_info = [
        f"{i}: hash={h}, node={node_refs[i]}"
        for i, h in enumerate(ring)
    ]
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

    # logger.info(f"[owner_nodes] Final owners for key '{key}': {owners}")
    return owners

def poll_nodes():
    global last_ring_update
    while True:
        try:
            resp = requests.get(f"{SEED_NODE}/nodes", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                nodes = set(data.get("nodes", []))
                states = data.get("states", {})
                with lock:
                    known_nodes.clear()
                    known_nodes.update(nodes)
                    node_states.clear()
                    node_states.update(states)
                    last_ring_update = time.time()
        except Exception:
            pass
        time.sleep(RING_UPDATE_INTERVAL)

def is_ring_stable():
    with lock:
        ready_nodes = [n for n in known_nodes if node_states.get(n) == "ready"]
    return (time.time() - last_ring_update) < RING_STABLE_PERIOD and len(ready_nodes) > 0

def retry_with_backoff(fn, max_retries=3, base_delay=0.05, jitter=0.05):
    for attempt in range(max_retries + 1):
        try:
            resp = fn()
            if resp is not None and resp.status_code == 200:
                return resp
            if resp is not None and resp.status_code == 503:
                # Overloaded, try again
                pass
        except Exception:
            pass
        delay = base_delay * (2 ** attempt) + random.uniform(0, jitter)
        time.sleep(delay)
    return None

def with_gateway_load_shedding(fn):
    def wrapper(*args, **kwargs):
        global in_flight
        with in_flight_lock:
            if in_flight >= IN_FLIGHT_LIMIT:
                return jsonify({"error": "gateway overloaded"}), 503
            in_flight += 1
        try:
            return fn(*args, **kwargs)
        finally:
            with in_flight_lock:
                in_flight -= 1
    wrapper.__name__ = fn.__name__
    return wrapper

@app.route("/set", methods=["POST"])
@with_gateway_load_shedding
def gateway_set():
    if not is_ring_stable():
        return jsonify({"error": "Cluster is not stable, try again soon."}), 503
    data = request.json
    key = data["key"]
    value = data["value"]
    req_id = data.get("request_id", f"{key}-{random.randint(1, 1_000_000_000)}")
    ts = float(data.get("ts", time.time()))
    owners = get_owner_nodes(key)
    if not owners:
        return jsonify({"error": "No ready nodes found"}), 503
    W = max(len(owners) // 2 + 1, 1)
    successes = 0
    errors = []
    for node_addr in owners:
        def do_post():
            return requests.post(
                f"{node_addr}/internal/set", json={
                    "key": key, "value": value, "ts": ts, "request_id": req_id
                }, timeout=1
            )
        resp = retry_with_backoff(do_post)
        if resp and resp.status_code == 200:
            successes += 1
        else:
            errors.append(f"{node_addr}: write failed or overload")
    if successes >= W:
        # logger.info(f"Write quorum reached for key={key} with W={W} out of {len(owners)} nodes.")
        return jsonify({"result": "ok", "successes": successes})
    else:
        logger.warning(f"Write quorum NOT reached for key={key} (successes={successes}, W={W}). Errors: {errors}")
        return (
            jsonify({"result": "write_failed", "successes": successes, "errors": errors}),
            503,
        )

@app.route("/get", methods=["GET"])
@with_gateway_load_shedding
def gateway_get():
    if not is_ring_stable():
        return jsonify({"error": "Cluster is not stable, try again soon."}), 503
    key = request.args["key"]
    owners = get_owner_nodes(key)
    if not owners:
        return jsonify({"error": "No ready nodes found"}), 503
    R = max(len(owners) // 2 + 1, 1)
    results = []
    for node_addr in owners:
        def do_get():
            return requests.get(
                f"{node_addr}/internal/get", params={"key": key}, timeout=1
            )
        resp = retry_with_backoff(do_get)
        if resp and resp.status_code == 200:
            v = resp.json().get("value")
            if v:
                results.append(v)
        if len(results) >= R:
            break
    if not results:
        return jsonify({"result": "not_found"}), 404
    latest = max(results, key=lambda x: x["ts"])
    if len(results) >= R:
        logger.info(f"Read quorum reached for key={key} with R={R} out of {len(owners)} nodes.")
    else:
        logger.warning(f"Read quorum NOT reached for key={key} (results={len(results)}, R={R})")
    return jsonify({"key": key, "value": latest["value"], "ts": latest["ts"]})

@app.route("/status", methods=["GET"])
def gateway_status():
    with lock:
        ready_nodes = [n for n in known_nodes if node_states.get(n) == "ready"]
    return jsonify({
        "ready_nodes": ready_nodes,
        "known_nodes": list(known_nodes),
        "node_states": node_states,
        "ring_stable": is_ring_stable(),
        "num_ready": len(ready_nodes),
        "gateway_inflight": in_flight,
        "gateway_inflight_limit": IN_FLIGHT_LIMIT
    })

@app.route("/ring", methods=["GET"])
def display_ring():
    ring, node_refs = build_hash_ring()
    if not ring:
        return jsonify({"error": "No ready nodes in the ring"}), 503
    return jsonify({
        "ring": ring,
        "node_refs": node_refs,
        "num_vnodes": NUM_VNODES,
        "replication_factor": REPLICATION_FACTOR
    })

if __name__ == "__main__":
    threading.Thread(target=poll_nodes, daemon=True).start()
    logFlask = logging.getLogger('werkzeug')
    logFlask.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=8000)
