"""Quick smoke test for all endpoints via Docker."""
import asyncio
import aiohttp
import json
import sys

LOCK_URL = "http://localhost:7001"
QUEUE_URL = "http://localhost:7101"
CACHE_URL = "http://localhost:7201"

async def test_all():
    results = []
    async with aiohttp.ClientSession() as session:
        # ─── Health checks ───────────────────────────────────────
        for name, url in [("Lock", LOCK_URL), ("Queue", QUEUE_URL), ("Cache", CACHE_URL)]:
            async with session.get(f"{url}/health") as resp:
                data = await resp.json()
                ok = resp.status == 200 and data.get("status") == "ok"
                results.append((f"{name} health", ok, data))

        # ─── Lock: acquire + status + release ────────────────────
        async with session.post(f"{LOCK_URL}/lock/acquire", json={
            "lock": "test-resource", "client_id": "test-client", "mode": "exclusive"
        }) as resp:
            data = await resp.json()
            ok = data.get("status") == "granted"
            results.append(("Lock acquire", ok, data))

        async with session.get(f"{LOCK_URL}/lock/status", params={"lock": "test-resource"}) as resp:
            data = await resp.json()
            # Note: with round-robin LB, status may hit a different node than acquire.
            # Raft replicates state, so holders should eventually be consistent.
            ok = resp.status == 200 and "holders" in data
            results.append(("Lock status", ok, data))

        async with session.post(f"{LOCK_URL}/lock/release", json={
            "lock": "test-resource", "client_id": "test-client"
        }) as resp:
            data = await resp.json()
            ok = resp.status == 200
            results.append(("Lock release", ok, data))

        # ─── Queue: publish + stats + consume + ack ──────────────
        async with session.post(f"{QUEUE_URL}/queue/publish", json={
            "queue": "test-queue", "message": {"item": "laptop", "qty": 1}
        }) as resp:
            data = await resp.json()
            ok = data.get("status") == "ok" and "message_id" in data
            msg_id_from_publish = data.get("message_id", "")
            results.append(("Queue publish", ok, data))

        async with session.get(f"{QUEUE_URL}/queue/stats", params={"queue": "test-queue"}) as resp:
            data = await resp.json()
            ok = resp.status == 200 and "pending" in data
            results.append(("Queue stats", ok, data))

        async with session.post(f"{QUEUE_URL}/queue/consume", json={"queue": "test-queue"}) as resp:
            data = await resp.json()
            ok = data.get("status") == "ok" and "message" in data
            msg = data.get("message", {})
            msg_id = msg.get("id", "")
            results.append(("Queue consume", ok, data))

        async with session.post(f"{QUEUE_URL}/queue/ack", json={
            "queue": "test-queue", "message_id": msg_id
        }) as resp:
            data = await resp.json()
            ok = data.get("status") == "acked"
            results.append(("Queue ack", ok, data))

        # ─── Cache: put + get ────────────────────────────────────
        async with session.post(f"{CACHE_URL}/cache/put", json={
            "key": "test-key", "value": "test-value-123"
        }) as resp:
            data = await resp.json()
            ok = data.get("status") == "ok"
            results.append(("Cache put", ok, data))

        async with session.get(f"{CACHE_URL}/cache/get", params={"key": "test-key"}) as resp:
            data = await resp.json()
            ok = data.get("value") == "test-value-123"
            results.append(("Cache get", ok, data))

    # ─── Print results ───────────────────────────────────────
    all_ok = True
    for name, ok, data in results:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_ok = False
        print(f"  [{status}] {name}: {json.dumps(data)}")

    print()
    if all_ok:
        print("ALL TESTS PASSED!")
    else:
        print("SOME TESTS FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(test_all())
