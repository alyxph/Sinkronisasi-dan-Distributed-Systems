from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, Optional

import aiohttp

from src.utils.config import PeerInfo


class MessageBus:
    def __init__(self, peers: Iterable[PeerInfo], request_timeout_s: float = 2.0) -> None:
        self.peers: Dict[str, PeerInfo] = {peer.node_id: peer for peer in peers}
        self.blocked_peers = set()
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=request_timeout_s)
        )

    def set_peers(self, peers: Iterable[PeerInfo]) -> None:
        self.peers = {peer.node_id: peer for peer in peers}

    def block_peer(self, peer_id: str) -> None:
        self.blocked_peers.add(peer_id)

    def unblock_peer(self, peer_id: str) -> None:
        self.blocked_peers.discard(peer_id)

    def is_blocked(self, peer_id: str) -> bool:
        return peer_id in self.blocked_peers

    async def request_json(
        self, method: str, url: str, payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        try:
            async with self.session.request(method, url, json=payload) as resp:
                data = await resp.json(content_type=None)
                return {"ok": resp.status < 400, "status": resp.status, "data": data}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    async def send_json(
        self,
        peer_id: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
    ) -> Dict[str, Any]:
        if self.is_blocked(peer_id):
            return {"ok": False, "error": "partitioned"}
        peer = self.peers.get(peer_id)
        if not peer:
            return {"ok": False, "error": "unknown_peer"}
        url = peer.url.rstrip("/") + path
        return await self.request_json(method, url, payload)

    async def broadcast(
        self, path: str, payload: Optional[Dict[str, Any]] = None, method: str = "POST"
    ) -> Dict[str, Dict[str, Any]]:
        tasks = {
            peer_id: asyncio.create_task(self.send_json(peer_id, path, payload, method))
            for peer_id in self.peers
        }
        results: Dict[str, Dict[str, Any]] = {}
        for peer_id, task in tasks.items():
            results[peer_id] = await task
        return results

    async def close(self) -> None:
        await self.session.close()
