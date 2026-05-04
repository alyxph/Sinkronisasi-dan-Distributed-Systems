from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from aiohttp import web

from src.nodes.base_node import BaseNode


@dataclass
class CacheEntry:
    value: Any
    state: str  # 'M', 'E', 'S', 'I'
    updated_at: float


class LruCache:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.entries: Dict[str, CacheEntry] = {}
        self.lru = OrderedDict()

    def get(self, key: str) -> Optional[CacheEntry]:
        entry = self.entries.get(key)
        if not entry or entry.state == "I":
            return None
        self.lru.pop(key, None)
        self.lru[key] = True
        return entry

    def put(self, key: str, entry: CacheEntry) -> Optional[Tuple[str, CacheEntry]]:
        self.entries[key] = entry
        self.lru.pop(key, None)
        self.lru[key] = True
        if len(self.lru) > self.capacity:
            evicted_key, _ = self.lru.popitem(last=False)
            evicted_entry = self.entries.pop(evicted_key, None)
            if evicted_entry:
                return evicted_key, evicted_entry
        return None

    def invalidate(self, key: str) -> None:
        entry = self.entries.get(key)
        if entry:
            entry.state = "I"
            self.lru.pop(key, None)


class DistributedCacheNode(BaseNode):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.cache = LruCache(config.cache_capacity)
        # Dummy memory/database
        self.memory_db: Dict[str, Any] = {}

        self.app.add_routes(
            [
                web.get("/cache/get", self.handle_get),
                web.post("/cache/put", self.handle_put),
                web.get("/internal/cache/read", self.handle_internal_read),
                web.post("/internal/cache/invalidate", self.handle_internal_invalidate),
            ]
        )

    async def start(self) -> None:
        await super().start()

    async def stop(self) -> None:
        await super().stop()

    async def handle_get(self, request: web.Request) -> web.Response:
        key = request.query.get("key")
        if not key:
            return web.json_response({"error": "missing_key"}, status=400)

        # 1. Check local cache
        entry = self.cache.get(key)
        if entry:
            self.metrics.inc_counter("cache_hit_total")
            return web.json_response({"key": key, "value": entry.value, "state": entry.state})

        self.metrics.inc_counter("cache_miss_total")

        # 2. Check peers
        peer_results = await self.message_bus.broadcast(f"/internal/cache/read?key={key}", method="GET")
        
        peer_data = None
        for peer_id, resp in peer_results.items():
            if resp["ok"] and resp["data"].get("found"):
                peer_data = resp["data"]["value"]
                break

        if peer_data is not None:
            # We found it in a peer, so it's Shared
            cache_entry = CacheEntry(value=peer_data, state="S", updated_at=time.time())
            evicted = self.cache.put(key, cache_entry)
            if evicted:
                await self._writeback_if_needed(evicted)
            return web.json_response({"key": key, "value": peer_data, "state": "S"})

        # 3. Read from memory/database
        value = await self.fetch_from_memory(key)
        if value is None:
            return web.json_response({"error": "not_found"}, status=404)
            
        # Since we got it from memory and no other peer had it, state is Exclusive (E)
        cache_entry = CacheEntry(value=value, state="E", updated_at=time.time())
        evicted = self.cache.put(key, cache_entry)
        if evicted:
            await self._writeback_if_needed(evicted)
        return web.json_response({"key": key, "value": value, "state": "E"})

    async def handle_put(self, request: web.Request) -> web.Response:
        data = await request.json()
        key = data.get("key")
        if not key:
            return web.json_response({"error": "missing_key"}, status=400)
        value = data.get("value")

        # Invalidate others
        results = await self.message_bus.broadcast(
            "/internal/cache/invalidate", payload={"key": key}, method="POST"
        )
        failed = [peer_id for peer_id, res in results.items() if not res.get("ok")]
        if failed:
            self.logger.warning("Cache invalidate failed: key=%s peers=%s", key, failed)

        # Write-through for demo consistency
        await self.write_back_to_memory(key, value)

        # Write to local cache as Modified
        entry = CacheEntry(value=value, state="M", updated_at=time.time())
        evicted = self.cache.put(key, entry)
        if evicted:
            await self._writeback_if_needed(evicted)
            
        return web.json_response({"status": "ok", "key": key})

    async def handle_internal_read(self, request: web.Request) -> web.Response:
        key = request.query.get("key")
        entry = self.cache.get(key)
        
        if entry:
            if entry.state == "M":
                # Writeback before sharing
                await self.write_back_to_memory(key, entry.value)
                entry.state = "S"
            elif entry.state == "E":
                entry.state = "S"
            
            return web.json_response({"found": True, "value": entry.value, "state": entry.state})
            
        return web.json_response({"found": False})

    async def handle_internal_invalidate(self, request: web.Request) -> web.Response:
        data = await request.json()
        key = data.get("key")
        if key:
            self.cache.invalidate(key)
        return web.json_response({"status": "ok"})

    async def fetch_from_memory(self, key: str) -> Optional[Any]:
        await asyncio.sleep(0.01)  # Simulate DB latency
        # Simply return a dummy value if it was not explicitly put in memory_db
        return self.memory_db.get(key)

    async def write_back_to_memory(self, key: str, data: Any) -> None:
        await asyncio.sleep(0.01)  # Simulate DB latency
        self.memory_db[key] = data

    async def _writeback_if_needed(self, evicted: Tuple[str, CacheEntry]) -> None:
        key, entry = evicted
        if entry.state == "M":
            await self.write_back_to_memory(key, entry.value)
