from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from typing import Any, Dict, Optional

import aiofiles
from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.hashing import ConsistentHashRing


class DistributedQueueNode(BaseNode):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.ring = ConsistentHashRing([config.node_id] + [p.node_id for p in self.peers])
        self.queues: Dict[str, deque] = defaultdict(deque)
        self.processing: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        
        self.data_dir = config.data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.wal_path = os.path.join(self.data_dir, f"{self.node_id}_queue.wal")
        self.wal_lock = asyncio.Lock()

        self.visibility_timeout = config.queue_visibility_timeout_s
        self._requeue_task: Optional[asyncio.Task] = None

        self.app.add_routes(
            [
                web.post("/queue/publish", self.handle_publish),
                web.post("/queue/consume", self.handle_consume),
                web.post("/queue/ack", self.handle_ack),
                web.post("/internal/queue/enqueue", self.handle_internal_enqueue),
                web.post("/internal/queue/dequeue", self.handle_internal_dequeue),
                web.post("/internal/queue/ack", self.handle_internal_ack),
                web.get("/queue/stats", self.handle_stats),
            ]
        )

    async def start(self) -> None:
        await super().start()
        await self._recover_from_wal()
        self._requeue_task = asyncio.create_task(self._requeue_loop())

    async def stop(self) -> None:
        if self._requeue_task:
            self._requeue_task.cancel()
        await super().stop()

    async def _append_to_wal(self, log_entry: Dict[str, Any]) -> None:
        async with self.wal_lock:
            try:
                async with aiofiles.open(self.wal_path, mode='a') as f:
                    await f.write(json.dumps(log_entry) + '\n')
            except Exception as e:
                self.logger.error(f"Failed to append WAL: {e}")

    async def _recover_from_wal(self) -> None:
        if not os.path.exists(self.wal_path):
            return
        
        self.logger.info("Recovering queue from WAL...")
        temp_queues = defaultdict(dict)
        acked_ids = set()

        try:
            async with aiofiles.open(self.wal_path, mode='r') as f:
                async for line in f:
                    if not line.strip():
                        continue
                    entry = json.loads(line)
                    if entry['type'] == 'ENQUEUE':
                        msg = entry['payload']
                        temp_queues[msg['queue']][msg['id']] = msg
                    elif entry['type'] == 'ACK':
                        acked_ids.add(entry['msg_id'])

            recovered = 0
            for queue_name, messages in temp_queues.items():
                for msg_id, msg_data in messages.items():
                    if msg_id not in acked_ids:
                        self.queues[queue_name].append(msg_data)
                        recovered += 1

            self.logger.info(f"Recovered {recovered} messages from WAL.")
        except Exception as e:
            self.logger.error(f"Error recovering from WAL: {e}")

    async def handle_publish(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue")
        message = data.get("message")
        if not queue_name or message is None:
            return web.json_response({"error": "missing_fields"}, status=400)

        target_node = self.ring.get_node(queue_name)
        if target_node == self.node_id:
            msg_id = f"{self.node_id}-{time.time_ns()}"
            msg_data = {"id": msg_id, "queue": queue_name, "data": message, "ts": time.time()}
            
            await self._append_to_wal({"type": "ENQUEUE", "payload": msg_data})
            self.queues[queue_name].append(msg_data)
            
            self.metrics.inc_counter("queue_messages_published")
            return web.json_response({"status": "enqueued", "message_id": msg_id})

        res = await self.message_bus.send_json(
            target_node, "/internal/queue/enqueue", {"queue": queue_name, "message": message}
        )
        if res.get("ok"):
            return web.json_response(res["data"])
        return web.json_response({"error": "forward_failed", "details": res}, status=500)

    async def handle_consume(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue")
        if not queue_name:
            return web.json_response({"error": "missing_queue"}, status=400)

        target_node = self.ring.get_node(queue_name)
        if target_node == self.node_id:
            msg = await self._local_dequeue(queue_name)
            if not msg:
                return web.json_response({"error": "queue_empty"}, status=404)
            return web.json_response({"status": "ok", "message": msg})

        res = await self.message_bus.send_json(
            target_node, "/internal/queue/dequeue", {"queue": queue_name}
        )
        if res.get("ok"):
            return web.json_response(res["data"], status=res["status"])
        return web.json_response({"error": "forward_failed", "details": res}, status=500)

    async def handle_ack(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue")
        message_id = data.get("message_id")
        if not queue_name or not message_id:
            return web.json_response({"error": "missing_fields"}, status=400)

        target_node = self.ring.get_node(queue_name)
        if target_node == self.node_id:
            success = await self._local_ack(queue_name, message_id)
            if success:
                return web.json_response({"status": "acked"})
            return web.json_response({"error": "not_found_or_expired"}, status=404)

        res = await self.message_bus.send_json(
            target_node, "/internal/queue/ack", {"queue": queue_name, "message_id": message_id}
        )
        if res.get("ok"):
            return web.json_response(res["data"], status=res["status"])
        return web.json_response({"error": "forward_failed", "details": res}, status=500)

    async def handle_stats(self, request: web.Request) -> web.Response:
        queue_name = request.query.get("queue")
        if not queue_name:
            return web.json_response({"error": "missing_queue"}, status=400)

        target_node = self.ring.get_node(queue_name)
        if target_node == self.node_id:
            return web.json_response({
                "queue": queue_name,
                "pending": len(self.queues[queue_name]),
                "processing": len(self.processing[queue_name])
            })

        peer = self.message_bus.peers.get(target_node)
        if not peer:
            return web.json_response({"error": "peer_not_found"}, status=500)
        url = peer.url.rstrip("/") + f"/queue/stats?queue={queue_name}"
        res = await self.message_bus.request_json("GET", url)
        if res.get("ok"):
            return web.json_response(res["data"], status=res["status"])
        return web.json_response({"error": "forward_failed", "details": res}, status=500)

    # Internal handlers
    async def handle_internal_enqueue(self, request: web.Request) -> web.Response:
        return await self.handle_publish(request)

    async def handle_internal_dequeue(self, request: web.Request) -> web.Response:
        data = await request.json()
        queue_name = data.get("queue")
        msg = await self._local_dequeue(queue_name)
        if not msg:
            return web.json_response({"error": "queue_empty"}, status=404)
        return web.json_response({"status": "ok", "message": msg})

    async def handle_internal_ack(self, request: web.Request) -> web.Response:
        return await self.handle_ack(request)

    async def _local_dequeue(self, queue_name: str) -> Optional[Dict[str, Any]]:
        q = self.queues[queue_name]
        if not q:
            return None
        
        msg = q.popleft()
        msg_id = msg['id']
        msg['visibility_timeout'] = time.time() + self.visibility_timeout
        self.processing[queue_name][msg_id] = msg
        
        self.metrics.inc_counter("queue_messages_consumed")
        # Return normalized format matching OpenAPI spec (don't leak internal fields)
        return {
            "id": msg_id,
            "body": msg.get("data"),
            "ts": msg.get("ts", time.time()),
        }

    async def _local_ack(self, queue_name: str, message_id: str) -> bool:
        if message_id in self.processing[queue_name]:
            del self.processing[queue_name][message_id]
            await self._append_to_wal({"type": "ACK", "msg_id": message_id})
            return True
        return False

    async def _requeue_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.queue_requeue_interval_s)
                now = time.time()
                for queue_name, in_flight in list(self.processing.items()):
                    expired = []
                    for msg_id, msg in in_flight.items():
                        if now > msg.get("visibility_timeout", 0):
                            expired.append(msg_id)
                    
                    for msg_id in expired:
                        msg = self.processing[queue_name].pop(msg_id)
                        msg.pop("visibility_timeout", None)
                        self.queues[queue_name].append(msg)
                        self.logger.warning(f"Message {msg_id} visibility timeout, requeued")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.error(f"Error in requeue loop: {exc}")
