import threading
import requests
import random
import uuid
import time
import statistics

LB_URL = "http://localhost:8000"
NUM_CLIENTS = 20
NUM_REQUESTS = 20
MAX_RETRIES = 3

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.gw_overloads = 0
        self.node_overloads = 0
        self.not_found = 0
        self.unknown_error = 0
        self.success = 0
        self.failure = 0
        self.latencies = []
        self.retries = []

    def record(self, resp, latency, retries, error_type=None):
        with self.lock:
            if resp is not None and resp.status_code == 200:
                self.success += 1
            elif resp is not None and resp.status_code == 503:
                if error_type == "gateway":
                    self.gw_overloads += 1
                elif error_type == "node":
                    self.node_overloads += 1
                else:
                    self.unknown_error += 1
                self.failure += 1
            elif resp is not None and resp.status_code == 404:
                self.not_found += 1
                self.failure += 1
            else:
                self.unknown_error += 1
                self.failure += 1
            self.latencies.append(latency)
            self.retries.append(retries)

    def report(self):
        with self.lock:
            total = self.success + self.failure
            avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
            med_latency = statistics.median(self.latencies) if self.latencies else 0
            max_latency = max(self.latencies) if self.latencies else 0
            avg_retries = sum(self.retries) / len(self.retries) if self.retries else 0
            print(f"Total requests: {total}")
            print(f"  Success: {self.success}")
            print(f"  Failures: {self.failure}")
            print(f"    - Gateway overloads (503): {self.gw_overloads}")
            print(f"    - Node overloads (503): {self.node_overloads}")
            print(f"    - Not found (404): {self.not_found}")
            print(f"    - Unknown errors: {self.unknown_error}")
            print(f"Average latency: {avg_latency:.3f}s")
            print(f"Median latency: {med_latency:.3f}s")
            print(f"Max latency: {max_latency:.3f}s")
            print(f"Average retries per request: {avg_retries:.2f}")

stats = Stats()

def send_with_retries(request_func):
    attempt = 0
    while attempt <= MAX_RETRIES:
        start = time.time()
        try:
            resp = request_func()
            latency = time.time() - start
            error_type = None
            if resp is not None:
                if resp.status_code == 200:
                    stats.record(resp, latency, attempt)
                    return
                elif resp.status_code == 503:
                    body = resp.json()
                    if "gateway overloaded" in str(body).lower():
                        error_type = "gateway"
                    elif "node overloaded" in str(body).lower():
                        error_type = "node"
                    elif "write_failed" in str(body).lower() or "errors" in body:
                        error_type = "node"
                    else:
                        print("Unexpected 503 response:", body)
                        error_type = "unknown"
                    stats.record(resp, latency, attempt, error_type)
                    if error_type in ("gateway", "node"):
                        delay = 0.03 * (2 ** attempt) + random.uniform(0, 0.03)
                        time.sleep(delay)
                        attempt += 1
                        continue
                    else:
                        return
                elif resp.status_code == 404:
                    stats.record(resp, latency, attempt, "not_found")
                    return
                else:
                    stats.record(resp, latency, attempt, "unknown")
                    return
            else:
                stats.record(None, latency, attempt, "unknown")
        except Exception as e:
            print(e)
            latency = time.time() - start
            stats.record(None, latency, attempt, "unknown")
        delay = 0.03 * (2 ** attempt) + random.uniform(0, 0.03)
        time.sleep(delay)
        attempt += 1

def client_task(client_id):
    for i in range(NUM_REQUESTS):
        k = f"overload-key-{random.randint(1, 1000)}"
        v = f"overload-value-{random.randint(1, 1000)}"
        req_id = f"{client_id}-{uuid.uuid4()}"

        def set_request():
            return requests.post(
                f"{LB_URL}/set",
                json={"key": k, "value": v, "request_id": req_id},
                headers={"X-Client-ID": client_id},
                timeout=5
            )
        send_with_retries(set_request)
        time.sleep(random.uniform(0.01, 0.05))

        def get_request():
            return requests.get(
                f"{LB_URL}/get",
                params={"key": k},
                headers={"X-Client-ID": client_id},
                timeout=5
            )
        send_with_retries(get_request)
        time.sleep(0.01)

threads = []
for i in range(NUM_CLIENTS):
    t = threading.Thread(target=client_task, args=(f"client-{i}",))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

stats.report()