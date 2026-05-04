from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

from aiohttp import web

from src.swagger.swagger_ui import setup_swagger

from src.communication.failure_detector import FailureDetector
from src.communication.message_passing import MessageBus
from src.utils.config import NodeConfig, PeerInfo
from src.utils.metrics import MetricsRegistry


class BaseNode:
    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.node_id = config.node_id
        self.role = config.role
        self.logger = logging.getLogger(f"{self.role}.{self.node_id}")
        self.peers = list(config.peers)
        self.peers_by_id = {peer.node_id: peer for peer in self.peers}
        self.metrics = MetricsRegistry()
        self.message_bus = MessageBus(self.peers)
        self.failure_detector = FailureDetector(
            self.peers, interval_ms=config.failure_detector_interval_ms
        )

        self.app = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._stop_event = asyncio.Event()

        self._setup_routes()
        setup_swagger(self.app, role=self.role)

    def _setup_routes(self) -> None:
        self.app.add_routes(
            [
                web.get("/health", self.handle_health),
                web.get("/metrics", self.handle_metrics),
                web.get("/admin/peers", self.handle_peers),
                web.post("/admin/partition", self.handle_partition),
                web.get("/admin/state", self.handle_state),
            ]
        )

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response(
            {"status": "ok", "node_id": self.node_id, "role": self.role}
        )

    async def handle_metrics(self, request: web.Request) -> web.Response:
        return web.Response(text=self.metrics.render_prometheus(), content_type="text/plain")

    async def handle_peers(self, request: web.Request) -> web.Response:
        status = self.failure_detector.snapshot()
        return web.json_response(
            {
                "peers": [
                    {"id": peer.node_id, "url": peer.url, "alive": status.get(peer.node_id)}
                    for peer in self.peers
                ]
            }
        )

    async def handle_partition(self, request: web.Request) -> web.Response:
        data = await request.json()
        for peer_id in data.get("block", []):
            self.message_bus.block_peer(peer_id)
        for peer_id in data.get("unblock", []):
            self.message_bus.unblock_peer(peer_id)
        return web.json_response({"blocked": list(self.message_bus.blocked_peers)})

    async def handle_state(self, request: web.Request) -> web.Response:
        return web.json_response({"node_id": self.node_id, "role": self.role})

    def get_peer(self, peer_id: str) -> Optional[PeerInfo]:
        return self.peers_by_id.get(peer_id)

    async def start(self) -> None:
        await self.failure_detector.start()
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.config.host, self.config.port)
        await self._site.start()

    async def stop(self) -> None:
        self._stop_event.set()
        await self.failure_detector.stop()
        await self.message_bus.close()
        if self._runner:
            await self._runner.cleanup()

    async def wait(self) -> None:
        await self._stop_event.wait()
