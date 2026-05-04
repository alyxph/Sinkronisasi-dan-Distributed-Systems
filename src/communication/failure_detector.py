from __future__ import annotations

import asyncio
import contextlib
from typing import Dict, Iterable, Optional

import aiohttp

from src.utils.config import PeerInfo


class FailureDetector:
    def __init__(
        self,
        peers: Iterable[PeerInfo],
        interval_ms: int = 1000,
        timeout_s: float = 1.0,
    ) -> None:
        self.peers = list(peers)
        self.interval_s = interval_ms / 1000.0
        self.timeout_s = timeout_s
        self.status: Dict[str, bool] = {peer.node_id: True for peer in self.peers}
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout_s)
        )

    def set_peers(self, peers: Iterable[PeerInfo]) -> None:
        self.peers = list(peers)
        for peer in self.peers:
            self.status.setdefault(peer.node_id, True)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._session.close()

    def is_alive(self, peer_id: str) -> bool:
        return self.status.get(peer_id, False)

    def snapshot(self) -> Dict[str, bool]:
        return dict(self.status)

    async def _check_peer(self, peer: PeerInfo) -> None:
        url = peer.url.rstrip("/") + "/health"
        try:
            async with self._session.get(url) as resp:
                self.status[peer.node_id] = resp.status == 200
        except Exception:
            self.status[peer.node_id] = False

    async def _loop(self) -> None:
        while self._running:
            tasks = [self._check_peer(peer) for peer in self.peers]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self.interval_s)


