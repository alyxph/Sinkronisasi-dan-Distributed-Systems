from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiohttp import web

from src.consensus.raft import RaftNode
from src.nodes.base_node import BaseNode


@dataclass
class LockRequest:
    request_id: str
    lock_name: str
    client_id: str
    mode: str
    created_at: float
    ttl_ms: Optional[int] = None


@dataclass
class LockState:
    holders: Dict[str, str] = field(default_factory=dict)
    holder_expiry: Dict[str, Optional[float]] = field(default_factory=dict)
    queue: List[LockRequest] = field(default_factory=list)


class LockStateMachine:
    def __init__(self) -> None:
        self.locks: Dict[str, LockState] = {}
        self.requests: Dict[str, Dict[str, Any]] = {}
        self.held_by_client: Dict[str, set[str]] = {}

    def apply(self, command: Dict[str, Any]) -> Dict[str, Any]:
        op = command.get("op")
        if op == "acquire":
            return self._apply_acquire(command)
        if op == "release":
            return self._apply_release(command)
        if op == "abort":
            return self._apply_abort(command)
        return {"status": "error", "error": "unknown_op"}

    def _apply_acquire(self, command: Dict[str, Any]) -> Dict[str, Any]:
        request_id = command["request_id"]
        if request_id in self.requests:
            return self.requests[request_id]

        request = LockRequest(
            request_id=request_id,
            lock_name=command["lock"],
            client_id=command["client_id"],
            mode=command["mode"],
            created_at=command["created_at"],
            ttl_ms=command.get("ttl_ms"),
        )
        state = self.locks.setdefault(request.lock_name, LockState())

        if self._can_grant(state, request):
            self._grant_request(state, request)
            result = {
                "status": "granted",
                "request_id": request.request_id,
                "lock": request.lock_name,
                "client_id": request.client_id,
                "mode": request.mode,
            }
        else:
            state.queue.append(request)
            result = {
                "status": "waiting",
                "request_id": request.request_id,
                "lock": request.lock_name,
                "client_id": request.client_id,
                "mode": request.mode,
            }

        self.requests[request_id] = result
        return result

    def _apply_release(self, command: Dict[str, Any]) -> Dict[str, Any]:
        lock_name = command["lock"]
        client_id = command["client_id"]
        state = self.locks.get(lock_name)
        if not state or client_id not in state.holders:
            result = {"status": "not_held", "lock": lock_name, "client_id": client_id}
            return result

        state.holders.pop(client_id, None)
        state.holder_expiry.pop(client_id, None)
        self.held_by_client.get(client_id, set()).discard(lock_name)

        released = {"status": "released", "lock": lock_name, "client_id": client_id}
        self._grant_waiting(state)
        return released

    def _apply_abort(self, command: Dict[str, Any]) -> Dict[str, Any]:
        request_id = command["request_id"]
        for state in self.locks.values():
            for idx, request in enumerate(state.queue):
                if request.request_id == request_id:
                    state.queue.pop(idx)
                    result = {
                        "status": "aborted",
                        "request_id": request_id,
                        "lock": request.lock_name,
                        "client_id": request.client_id,
                        "reason": command.get("reason", "deadlock"),
                    }
                    self.requests[request_id] = result
                    return result
        return {"status": "not_found", "request_id": request_id}

    def _can_grant(self, state: LockState, request: LockRequest) -> bool:
        if not state.holders:
            if request.mode == "exclusive":
                return True
            if request.mode == "shared":
                return not any(q.mode == "exclusive" for q in state.queue)
        if request.mode == "exclusive":
            return False
        if request.mode == "shared":
            if any(mode == "exclusive" for mode in state.holders.values()):
                return False
            if any(q.mode == "exclusive" for q in state.queue):
                return False
            return True
        return False

    def _grant_request(self, state: LockState, request: LockRequest) -> None:
        state.holders[request.client_id] = request.mode
        if request.ttl_ms:
            state.holder_expiry[request.client_id] = request.created_at + request.ttl_ms / 1000.0
        else:
            state.holder_expiry[request.client_id] = None
        self.held_by_client.setdefault(request.client_id, set()).add(request.lock_name)

    def _grant_waiting(self, state: LockState) -> None:
        if not state.queue:
            return
        granted: List[LockRequest] = []
        if not state.holders:
            first = state.queue[0]
            if first.mode == "exclusive":
                granted.append(state.queue.pop(0))
            else:
                while state.queue and state.queue[0].mode == "shared":
                    granted.append(state.queue.pop(0))
        else:
            if all(mode == "shared" for mode in state.holders.values()):
                while state.queue and state.queue[0].mode == "shared":
                    granted.append(state.queue.pop(0))

        for request in granted:
            self._grant_request(state, request)
            self.requests[request.request_id] = {
                "status": "granted",
                "request_id": request.request_id,
                "lock": request.lock_name,
                "client_id": request.client_id,
                "mode": request.mode,
            }

    def get_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        return self.requests.get(request_id)

    def lock_snapshot(self, lock_name: str) -> Dict[str, Any]:
        state = self.locks.get(lock_name)
        if not state:
            return {"lock": lock_name, "holders": {}, "queue": []}
        return {
            "lock": lock_name,
            "holders": state.holders,
            "queue": [
                {
                    "request_id": req.request_id,
                    "client_id": req.client_id,
                    "mode": req.mode,
                    "created_at": req.created_at,
                }
                for req in state.queue
            ],
        }

    def find_deadlock_victim(self) -> Optional[str]:
        graph: Dict[str, set[str]] = {}
        for state in self.locks.values():
            holders = set(state.holders.keys())
            for req in state.queue:
                graph.setdefault(req.client_id, set()).update(holders)

        cycle = self._find_cycle(graph)
        if not cycle:
            return None

        candidates: List[LockRequest] = []
        for state in self.locks.values():
            for req in state.queue:
                if req.client_id in cycle:
                    candidates.append(req)
        if not candidates:
            return None
        candidates.sort(key=lambda req: req.created_at, reverse=True)
        return candidates[0].request_id

    def _find_cycle(self, graph: Dict[str, set[str]]) -> List[str]:
        visited = set()
        stack = []
        in_stack = set()
        parent: Dict[str, str] = {}

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            stack.append(node)
            in_stack.add(node)
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    parent[neighbor] = node
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in in_stack:
                    cycle = [neighbor]
                    cur = node
                    while cur != neighbor:
                        cycle.append(cur)
                        cur = parent.get(cur, neighbor)
                    return cycle
            stack.pop()
            in_stack.remove(node)
            return None

        for node in graph:
            if node not in visited:
                cycle = dfs(node)
                if cycle:
                    return cycle
        return []

    def expired_holders(self, now: float) -> List[Dict[str, str]]:
        expired = []
        for lock_name, state in self.locks.items():
            for client_id, expiry in list(state.holder_expiry.items()):
                if expiry is not None and expiry <= now:
                    expired.append({"lock": lock_name, "client_id": client_id})
        return expired


class DistributedLockManagerNode(BaseNode):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.lock_state = LockStateMachine()
        peer_ids = [peer.node_id for peer in self.peers if peer.node_id != self.node_id]
        self.raft = RaftNode(
            node_id=self.node_id,
            peers=peer_ids,
            message_bus=self.message_bus,
            apply_callback=self._apply_command,
            election_timeout_ms=config.raft_election_timeout_ms,
            heartbeat_ms=config.raft_heartbeat_ms,
        )
        self.pending_futures: Dict[str, asyncio.Future] = {}
        self.deadlock_task: Optional[asyncio.Task] = None
        self.expiry_task: Optional[asyncio.Task] = None

        self.app.add_routes(
            [
                web.post("/lock/acquire", self.handle_acquire),
                web.post("/lock/release", self.handle_release),
                web.get("/lock/status", self.handle_status),
                web.get("/lock/leader", self.handle_leader),
                web.post("/raft/request_vote", self.handle_request_vote),
                web.post("/raft/append_entries", self.handle_append_entries),
            ]
        )

    async def start(self) -> None:
        await super().start()
        await self.raft.start()
        self.deadlock_task = asyncio.create_task(self._deadlock_loop())
        self.expiry_task = asyncio.create_task(self._expiry_loop())

    async def stop(self) -> None:
        if self.deadlock_task:
            self.deadlock_task.cancel()
        if self.expiry_task:
            self.expiry_task.cancel()
        await self.raft.stop()
        await super().stop()

    async def handle_request_vote(self, request: web.Request) -> web.Response:
        data = await request.json()
        response = await self.raft.handle_request_vote(data)
        return web.json_response(response)

    async def handle_append_entries(self, request: web.Request) -> web.Response:
        data = await request.json()
        response = await self.raft.handle_append_entries(data)
        return web.json_response(response)

    async def handle_leader(self, request: web.Request) -> web.Response:
        return web.json_response({"leader_id": self.raft.leader_id})

    async def handle_acquire(self, request: web.Request) -> web.Response:
        data = await request.json()
        if not self.raft.is_leader():
            return await self._proxy_to_leader("/lock/acquire", data)

        request_id = data.get("request_id") or str(uuid.uuid4())
        command = {
            "op": "acquire",
            "request_id": request_id,
            "lock": data["lock"],
            "client_id": data["client_id"],
            "mode": data.get("mode", "exclusive"),
            "ttl_ms": data.get("ttl_ms"),
            "created_at": time.time(),
        }
        future = asyncio.get_event_loop().create_future()
        self.pending_futures[request_id] = future

        timeout_s = self.config.lock_request_timeout_ms / 1000.0
        success = await self.raft.append_entry(command, timeout_s=timeout_s)

        if not success:
            self.pending_futures.pop(request_id, None)
            return web.json_response({"error": "commit_failed"}, status=503)

        try:
            result = await asyncio.wait_for(future, timeout=timeout_s)
        except asyncio.TimeoutError:
            result = {"status": "timeout", "request_id": request_id}
        return web.json_response(result)

    async def handle_release(self, request: web.Request) -> web.Response:
        data = await request.json()
        if not self.raft.is_leader():
            return await self._proxy_to_leader("/lock/release", data)

        command = {
            "op": "release",
            "lock": data["lock"],
            "client_id": data["client_id"],
        }
        timeout_s = self.config.lock_request_timeout_ms / 1000.0
        success = await self.raft.append_entry(command, timeout_s=timeout_s)
        if not success:
            return web.json_response({"error": "commit_failed"}, status=503)
        return web.json_response({"status": "ok"})

    async def handle_status(self, request: web.Request) -> web.Response:
        request_id = request.query.get("request_id")
        lock_name = request.query.get("lock")
        if request_id:
            status = self.lock_state.get_status(request_id)
            return web.json_response(status or {"status": "unknown"})
        if lock_name:
            return web.json_response(self.lock_state.lock_snapshot(lock_name))
        return web.json_response({"error": "missing_query"}, status=400)

    async def _proxy_to_leader(self, path: str, payload: Dict[str, Any]) -> web.Response:
        leader_id = self.raft.leader_id

        # Wait briefly for leader election if none known yet
        if not leader_id:
            for _ in range(10):  # poll for up to ~2s
                await asyncio.sleep(0.2)
                leader_id = self.raft.leader_id
                if leader_id:
                    break
            if not leader_id:
                return web.json_response({"error": "leader_unknown"}, status=503)

        response = await self.message_bus.send_json(leader_id, path, payload)
        if response.get("ok"):
            return web.json_response(response.get("data", {}), status=response.get("status", 200))

        # Retry once with refreshed leader info (leader may have changed)
        leader_id = self.raft.leader_id
        if leader_id:
            response = await self.message_bus.send_json(leader_id, path, payload)
            if response.get("ok"):
                return web.json_response(response.get("data", {}), status=response.get("status", 200))

        return web.json_response({"error": "leader_unreachable"}, status=503)

    def _apply_command(self, command: Dict[str, Any]) -> None:
        result = self.lock_state.apply(command)
        request_id = result.get("request_id")
        if request_id and request_id in self.pending_futures:
            future = self.pending_futures.pop(request_id)
            if not future.done():
                future.set_result(result)

    async def _deadlock_loop(self) -> None:
        interval = self.config.deadlock_check_interval_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            if not self.raft.is_leader():
                continue
            victim = self.lock_state.find_deadlock_victim()
            if victim:
                command = {"op": "abort", "request_id": victim, "reason": "deadlock"}
                await self.raft.append_entry(command)

    async def _expiry_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if not self.raft.is_leader():
                continue
            now = time.time()
            expired = self.lock_state.expired_holders(now)
            for item in expired:
                command = {"op": "release", "lock": item["lock"], "client_id": item["client_id"]}
                await self.raft.append_entry(command)
