from flask import Flask, request, jsonify
import os
import requests
import random
import redis
import time
from redis.sentinel import Sentinel

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
# sentinel = Sentinel([('sentinel', 26379)], socket_timeout=2)
# r = sentinel.master_for('master', password='password', decode_responses=True)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = Flask(__name__)

NODE_CACHE = {"nodes": [], "last_fetch": 0}
def get_live_ready_nodes():
    now = time.time()
    if now - NODE_CACHE["last_fetch"] > 1:
        try:
            node_keys = r.keys("node:*")
            NODE_CACHE["nodes"] = []
            for k in node_keys:
                node_info = r.hgetall(k)
                if node_info.get("state") == "ready":
                    NODE_CACHE["nodes"].append(node_info["addr"])
            NODE_CACHE["last_fetch"] = now
        except Exception as e:
            print("Redis discovery error:", e)
    return NODE_CACHE["nodes"]

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
        return (resp.content, resp.status_code, resp.headers.items())
    except Exception as e:
        return jsonify({"error": "backend error", "detail": str(e)}), 503

@app.route("/get", methods=["GET"])
def proxy_get():
    client_id = request.headers.get("X-Client-ID", "anon")
    candidates = shuffle_shard(client_id, 5)
    if not candidates:
        return jsonify({"error": "no available nodes"}), 503
    node = random.choice(candidates)
    try:
        resp = requests.get(f"{node}/get", params=request.args, headers=request.headers, timeout=2)
        return (resp.content, resp.status_code, resp.headers.items())
    except Exception as e:
        return jsonify({"error": "backend error", "detail": str(e)}), 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)