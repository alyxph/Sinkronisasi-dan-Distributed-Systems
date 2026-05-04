"""
Integration tests for the Lock Manager API.

These tests start 3 in-process lock manager nodes and exercise the
full Raft-based locking workflow through HTTP requests.

Run with:
    pytest tests/integration/test_lock_api.py -v -s
"""
from __future__ import annotations

import asyncio
from typing import Dict, List

import aiohttp
import pytest
import pytest_asyncio

from src.nodes.lock_manager import DistributedLockManagerNode
from src.utils.config import NodeConfig, PeerInfo


PORTS = [17001, 17002, 17003]
PEERS: List[PeerInfo] = [
    PeerInfo(node_id=f"test-lock-{i+1}", url=f"http://127.0.0.1:{port}")
    for i, port in enumerate(PORTS)
]


def _make_config(index: int) -> NodeConfig:
    return NodeConfig(
        node_id=f"test-lock-{index+1}",
        role="lock_manager",
        host="127.0.0.1",
        port=PORTS[index],
        peers=list(PEERS),
        data_dir="./data",
        raft_election_timeout_ms=800,
        raft_heartbeat_ms=300,
        failure_detector_interval_ms=500,
        lock_request_timeout_ms=5000,
        deadlock_check_interval_ms=1000,
        queue_visibility_timeout_s=15,
        queue_requeue_interval_s=5,
        cache_capacity=100,
        log_level="WARNING",
    )


@pytest_asyncio.fixture
async def cluster():
    """Start a 3-node lock manager cluster and yield the base URLs."""
    nodes: List[DistributedLockManagerNode] = []
    for i in range(3):
        node = DistributedLockManagerNode(_make_config(i))
        await node.start()
        nodes.append(node)

    # Give Raft time to elect a leader
    await asyncio.sleep(4)

    yield [f"http://127.0.0.1:{port}" for port in PORTS]

    for node in nodes:
        await node.stop()


@pytest.mark.asyncio
async def test_acquire_and_release(cluster):
    """Acquire an exclusive lock and release it."""
    urls = cluster
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Acquire
        async with session.post(f"{urls[0]}/lock/acquire", json={
            "lock": "test-lock-1",
            "client_id": "c1",
            "mode": "exclusive",
        }) as resp:
            data = await resp.json()
            assert data.get("status") in ("granted", "waiting"), f"Unexpected: {data}"

        # Release
        async with session.post(f"{urls[0]}/lock/release", json={
            "lock": "test-lock-1",
            "client_id": "c1",
        }) as resp:
            data = await resp.json()
            assert resp.status == 200


@pytest.mark.asyncio
async def test_shared_locks_concurrent(cluster):
    """Two shared locks on the same resource should both be granted."""
    urls = cluster
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(f"{urls[0]}/lock/acquire", json={
            "lock": "shared-res", "client_id": "s1", "mode": "shared",
        }) as resp:
            d1 = await resp.json()

        async with session.post(f"{urls[0]}/lock/acquire", json={
            "lock": "shared-res", "client_id": "s2", "mode": "shared",
        }) as resp:
            d2 = await resp.json()

        # Both should be granted
        assert d1.get("status") == "granted"
        assert d2.get("status") == "granted"

        # Clean up
        await session.post(f"{urls[0]}/lock/release", json={"lock": "shared-res", "client_id": "s1"})
        await session.post(f"{urls[0]}/lock/release", json={"lock": "shared-res", "client_id": "s2"})


@pytest.mark.asyncio
async def test_health_endpoint(cluster):
    """All nodes should respond to /health."""
    urls = cluster
    async with aiohttp.ClientSession() as session:
        for url in urls:
            async with session.get(f"{url}/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "ok"
