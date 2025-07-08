import requests
import time

GATEWAY = "http://localhost:8000"

print("\nTesting cluster via gateway at", GATEWAY)

print("\nSET key1 = hello ...")
r = requests.post(f"{GATEWAY}/set", json={"key": "key1", "value": "hello"})
print("Set response:", r.json())

print("\nGET key1 ...")
r = requests.get(f"{GATEWAY}/get", params={"key": "key1"})
print("Get response:", r.json())

print("\nSET key1 = world ...")
r = requests.post(f"{GATEWAY}/set", json={"key": "key1", "value": "world"})
print("Set response:", r.json())

print("\nGET key1 ...")
r = requests.get(f"{GATEWAY}/get", params={"key": "key1"})
print("Get response:", r.json())

print("\nGET missing_key ...")
r = requests.get(f"{GATEWAY}/get", params={"key": "missing_key"})
print("Get response:", r.json())

print("\nGateway status:")
r = requests.get(f"{GATEWAY}/status")
print(r.json())