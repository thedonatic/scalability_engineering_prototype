import requests
import time
import random
import uuid

LB_URL = "http://localhost:8000"
# LB_URL = "http://load-balancer:8000"

def backoff(attempt, base=0.2, cap=2.0):
    sleep = min(cap, base * 2 ** attempt) * (random.random() + 0.5)
    time.sleep(sleep)

def set_value(key, value, client_id):
    request_id = f"{client_id}-{int(time.time()*1000)}"
    for attempt in range(5):
        try:
            r = requests.post(f"{LB_URL}/set", json={"key": key, "value": value, "request_id": request_id},
                              headers={"X-Client-ID": client_id})
            if r.status_code == 200:
                print("SET OK:", r.json())
                return
            elif r.status_code == 503:
                print(f"Overloaded SET ({client_id}), retrying...")
                backoff(attempt)
            else:
                print("SET error:", r.text)
                break
        except Exception as e:
            print("SET exception:", e)
            backoff(attempt)

def get_value(key, client_id):
    for attempt in range(5):
        try:
            r = requests.get(f"{LB_URL}/get", params={"key": key},
                             headers={"X-Client-ID": client_id})
            if r.status_code == 200:
                print("GET OK:", r.json())
                return
            elif r.status_code == 503:
                print(f" - Overloaded GET ({client_id}), retrying...")
                backoff(attempt)
            else:
                print("GET error:", r.text)
                break
        except Exception as e:
            print("GET exception:", e)
            backoff(attempt)

if __name__ == "__main__":
    client_id = f"client-{uuid.uuid4()}"
    k = f"key{random.randint(1,100)}"
    v = f"value{random.randint(1,100)}"
    set_value(k, v, client_id)
    get_value(k, client_id)
