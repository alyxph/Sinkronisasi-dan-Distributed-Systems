import urllib.request
import json

# Get stats
try:
    r = urllib.request.urlopen("http://localhost:8089/stats/requests")
    d = json.loads(r.read())
    print("=== LOCUST STATS ===")
    for s in d.get("stats", []):
        print(f"  {s['method']} {s['name']}: {s['num_requests']} req, {s['num_failures']} fail")
except Exception as e:
    print(f"Stats error: {e}")

# Get failures
try:
    r = urllib.request.urlopen("http://localhost:8089/stats/failures")
    d = json.loads(r.read())
    print("\n=== FAILURES ===")
    for err in d.get("errors", []):
        print(f"  {err['method']} {err['name']}")
        print(f"    Error: {err['error'][:200]}")
        print(f"    Count: {err['occurrences']}")
except Exception as e:
    print(f"Failures error: {e}")

# Check if nodes are alive
print("\n=== NODE HEALTH ===")
for port in [7001, 7002, 7003]:
    try:
        r = urllib.request.urlopen(f"http://localhost:{port}/health", timeout=3)
        data = json.loads(r.read())
        print(f"  Port {port}: OK - {data}")
    except Exception as e:
        print(f"  Port {port}: DEAD - {e}")
