import requests
import time

GATEWAY = "http://localhost:8000"
TEST_KEY = "consistency_key"
VALUE1 = "before-scale"
VALUE2 = "after-scale"
WAIT = 1

def set_value(value):
    r = requests.post(f"{GATEWAY}/set", json={"key": TEST_KEY, "value": value})
    return r.json()

def get_value():
    try:
        r = requests.get(f"{GATEWAY}/get", params={"key": TEST_KEY}, timeout=1)
        return r.json().get("value")
    except Exception:
        return None

def poll_reads(expected, n=5):
    print(f"Polling {n} reads; expected value: '{expected}'")
    stale = 0
    correct = 0
    for _ in range(n):
        val = get_value()
        print(f"  -> Read: {val}")
        if val == expected:
            correct += 1
        else:
            stale += 1
        time.sleep(0.25)
    return correct, stale

print(f"\n[1] Setting '{TEST_KEY}' to '{VALUE1}'...")
resp = set_value(VALUE1)
print("Set response:", resp)
time.sleep(WAIT)

print(f"\n[2] Checking all reads BEFORE scaling ('{VALUE1}'):")
correct, stale = poll_reads(VALUE1, n=10)
if stale == 0:
    print("[OK] No stale reads before scaling.")
else:
    print(f"[WARN] {stale} stale/missing reads before scaling.")

print("\n[3] *** SCALE YOUR CLUSTER NOW! ***")
print("   (Run 'docker-compose up --scale node=5' or '--scale node=2', etc.)")
print("Polling reads while you scale. Watch for changes...")
for i in range(8):
    correct, stale = poll_reads(VALUE1, n=4)
    if stale > 0:
        print(f"  [WARN] Detected {stale} stale/missing reads during scaling.")
    else:
        print(f"  [OK] Reads are consistent during scaling so far.")
    time.sleep(1.5)

input("\nPress Enter once scaling is complete and the cluster looks stable...")

print(f"\n[4] Setting '{TEST_KEY}' to '{VALUE2}' after scaling.")
resp = set_value(VALUE2)
print("Set response:", resp)
time.sleep(WAIT)

print(f"\n[5] Checking for stale reads after scaling and new value ('{VALUE2}'):")
for i in range(12):
    val = get_value()
    print(f"  -> Read: {val}   {'[STALE]' if val != VALUE2 else '[OK]'}")
    time.sleep(0.5)