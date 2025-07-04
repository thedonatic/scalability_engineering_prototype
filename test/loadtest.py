import threading
import requests
import random
import uuid
import time

LB_URL = "http://localhost:8000"
NUM_CLIENTS = 20
NUM_REQUESTS = 50

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.success = 0
        self.failure = 0
        self.latencies = []
        self.retries = []
        self.overloads = 0  # Track overloads

    def record(self, success, latency, retries):
        with self.lock:
            if success:
                self.success += 1
            else:
                self.failure += 1
            self.latencies.append(latency)
            self.retries.append(retries)

    def record_overload(self):
        with self.lock:
            self.overloads += 1

    def report(self):
        with self.lock:
            total = self.success + self.failure
            avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
            avg_retries = sum(self.retries) / len(self.retries) if self.retries else 0
            print(f"Total requests: {total}")
            print(f"Successes: {self.success}")
            print(f"Failures: {self.failure}")
            print(f"Average latency: {avg_latency:.3f}s")
            print(f"Average retries per request: {avg_retries:.2f}")
            print(f"Total overloads (HTTP 503): {self.overloads}")

# Progress tracker
class Progress:
    def __init__(self, total):
        self.lock = threading.Lock()
        self.completed = 0
        self.total = total

    def increment(self):
        with self.lock:
            self.completed += 1

    def get(self):
        with self.lock:
            return self.completed

stats = Stats()
progress = Progress(NUM_CLIENTS * NUM_REQUESTS * 2)  # 2 requests per iteration (set/get)

def send_with_retries(request_func, max_retries=3, base_delay=0.05, jitter=0.05, stats=None):
    attempt = 0
    while attempt <= max_retries:
        start = time.time()
        try:
            resp = request_func()
            latency = time.time() - start
            success = resp is not None and resp.status_code != 503
            if success:
                if stats:
                    stats.record(True, latency, attempt)
                return resp
        except Exception:
            latency = time.time() - start
        delay = base_delay * (2 ** attempt) + random.uniform(0, jitter)
        time.sleep(delay)
        attempt += 1
    # If we reach here, all retries failed
    if stats:
        stats.record(False, latency, attempt)
    return None

def client_task(client_id):
    for _ in range(NUM_REQUESTS):
        k = f"key{random.randint(1,100)}"
        v = f"value{random.randint(1,100)}"
        def set_request():
            return requests.post(
                f"{LB_URL}/set",
                json={"key": k, "value": v, "request_id": f"{client_id}-{uuid.uuid4()}"},
                headers={"X-Client-ID": client_id},
                timeout=2
            )
        def get_request():
            return requests.get(
                f"{LB_URL}/get",
                params={"key": k},
                headers={"X-Client-ID": client_id},
                timeout=2
            )
        resp = send_with_retries(set_request, stats=stats)
        if resp is not None and resp.status_code == 503:
            # print(f"[{client_id}] SET overloaded")
            stats.record_overload()
        progress.increment()
        resp = send_with_retries(get_request, stats=stats)
        if resp is not None and resp.status_code == 503:
            # print(f"[{client_id}] GET overloaded")
            stats.record_overload()
        progress.increment()
        time.sleep(0.05)

def progress_reporter():
    total = progress.total
    last_print = -1
    while True:
        completed = progress.get()
        percent = (completed / total) * 100
        # Print every 10% or at the end
        if int(percent // 10) != last_print or completed == total:
            print(f"Progress: {completed}/{total} requests ({percent:.1f}%)")
            last_print = int(percent // 10)
        if completed >= total:
            break
        time.sleep(1)

threads = []
for i in range(NUM_CLIENTS):
    t = threading.Thread(target=client_task, args=(f"client-{i}",))
    t.start()
    threads.append(t)

start_time = time.time()
progress_thread = threading.Thread(target=progress_reporter)
progress_thread.start()

for t in threads:
    t.join()

progress_thread.join()
elapsed = time.time() - start_time
stats.report()
print(f"Total time elapsed: {elapsed:.2f} seconds")
print("Load test completed.")
