from flask import Flask, request, jsonify
import os
import requests
import random
import time

SEED_NODE = os.environ.get("SEED_NODE", "http://node:5000")
NODE_CACHE = {"nodes": set(), "last_fetch": 0, "states": {}}
CACHE_TTL = 10  # seconds

# Statistics for not found after successful write
NOT_FOUND_AFTER_WRITE = {
    "total_writes": 0,
    "not_found_reads": 0
}

# Track written keys for checking after write
WRITTEN_KEYS = set()

app = Flask(__name__)

def fetch_nodes_from(address):
    try:
        resp = requests.get(f"{address}/nodes", timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            nodes = set(data.get("nodes", []))
            states = data.get("states", {})
            ready_nodes = {n for n in nodes if states.get(n) == "ready"}
            return ready_nodes, states
    except Exception as e:
        print(f"Gossip discovery error from {address}:", e)
    return set(), {}

def get_random_known_node():
    # Always include the seed node as a fallback
    candidates = list(NODE_CACHE["nodes"]) + [SEED_NODE]
    return random.choice(candidates)

def refresh_nodes():
    while True:
        address = get_random_known_node()
        ready_nodes, states = fetch_nodes_from(address)
        if ready_nodes:
            NODE_CACHE["nodes"].update(ready_nodes)
            NODE_CACHE["states"].update(states)
            NODE_CACHE["last_fetch"] = time.time()
            print(f"[LoadBalancer] Discovered {len(NODE_CACHE['nodes'])} ready node(s).")
        time.sleep(2)

def get_live_ready_nodes():
    now = time.time()
    if now - NODE_CACHE["last_fetch"] > CACHE_TTL:
        address = get_random_known_node()
        ready_nodes, states = fetch_nodes_from(address)
        if ready_nodes:
            NODE_CACHE["nodes"].update(ready_nodes)
            NODE_CACHE["states"].update(states)
            NODE_CACHE["last_fetch"] = now
            print(f"[LoadBalancer] Refreshed {len(NODE_CACHE['nodes'])} ready node(s).")
    # Only return nodes that are still marked as ready in the latest known states
    return [n for n in NODE_CACHE["nodes"] if NODE_CACHE["states"].get(n) == "ready"]

def shuffle_shard(client_id, n=5):
    nodes = get_live_ready_nodes()
    if not nodes:
        return []
    random.seed(hash(client_id))
    sample = random.sample(nodes, min(n, len(nodes)))
    return sample

@app.route("/set", methods=["POST"])
def proxy_set():
    client_id = request.headers.get("X-Client-ID", "anon")
    candidates = shuffle_shard(client_id, 5)
    if not candidates:
        return jsonify({"error": "no available nodes"}), 503
    node = random.choice(candidates)
    try:
        resp = requests.post(f"{node}/set", json=request.json, headers=request.headers, timeout=2)
        if resp.status_code == 200:
            # Track key for later not-found check
            key = request.json.get("key")
            if key:
                WRITTEN_KEYS.add(key)
                NOT_FOUND_AFTER_WRITE["total_writes"] += 1
        return (resp.content, resp.status_code, resp.headers.items())
    except Exception as e:
        print(f"[LoadBalancer] SET routing error: {e}")
        return jsonify({"error": "backend error", "detail": str(e)}), 503

@app.route("/get", methods=["GET"])
def proxy_get():
    client_id = request.headers.get("X-Client-ID", "anon")
    candidates = shuffle_shard(client_id, 5)
    if not candidates:
        return jsonify({"error": "no available nodes"}), 503
    node = random.choice(candidates)
    key = request.args.get("key")
    try:
        resp = requests.get(f"{node}/get", params=request.args, headers=request.headers, timeout=2)
        # If this is a read for a previously written key and not found, count it
        if key in WRITTEN_KEYS and resp.status_code == 404:
            NOT_FOUND_AFTER_WRITE["not_found_reads"] += 1
        return (resp.content, resp.status_code, resp.headers.items())
    except Exception as e:
        print(f"[LoadBalancer] GET routing error: {e}")
        return jsonify({"error": "backend error", "detail": str(e)}), 503

@app.route("/lb_report", methods=["GET"])
def lb_report():
    return jsonify({
        "total_writes": NOT_FOUND_AFTER_WRITE["total_writes"],
        "not_found_reads_after_write": NOT_FOUND_AFTER_WRITE["not_found_reads"]
    })

if __name__ == "__main__":
    import threading
    # Always start with the seed node in the cache
    NODE_CACHE["nodes"].add(SEED_NODE)
    print(f"[LoadBalancer] Starting, using seed node: {SEED_NODE}")
    threading.Thread(target=refresh_nodes, daemon=True).start()
    app.run(host="0.0.0.0", port=8000)
