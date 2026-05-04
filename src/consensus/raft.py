from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from src.communication.message_passing import MessageBus


@dataclass
class LogEntry:
    term: int
    command: Dict[str, Any]


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        message_bus: MessageBus,
        apply_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        election_timeout_ms: int = 1500,
        heartbeat_ms: int = 500,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.message_bus = message_bus
        self.apply_callback = apply_callback
        self.election_timeout_ms = election_timeout_ms
        self.heartbeat_ms = heartbeat_ms

        self.state = "follower"
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = -1
        self.last_applied = -1
        self.next_index: Dict[str, int] = {peer_id: 0 for peer_id in peers}
        self.match_index: Dict[str, int] = {peer_id: -1 for peer_id in peers}
        self.leader_id: Optional[str] = None

        self._state_lock = asyncio.Lock()
        self._running = False
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat = time.monotonic()

    def is_leader(self) -> bool:
        return self.state == "leader"

    def majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def get_state(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_size": len(self.log),
            "commit_index": self.commit_index,
        }

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._election_task = asyncio.create_task(self._election_loop())

    async def stop(self) -> None:
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def handle_request_vote(self, data: Dict[str, Any]) -> Dict[str, Any]:
        async with self._state_lock:
            term = int(data.get("term", 0))
            candidate_id = data.get("candidate_id")
            last_log_index = int(data.get("last_log_index", -1))
            last_log_term = int(data.get("last_log_term", 0))

            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = "follower"

            can_vote = self.voted_for in (None, candidate_id)
            up_to_date = self._is_log_up_to_date(last_log_index, last_log_term)

            if can_vote and up_to_date:
                self.voted_for = candidate_id
                self._last_heartbeat = time.monotonic()
                return {"term": self.current_term, "vote_granted": True}

            return {"term": self.current_term, "vote_granted": False}

    async def handle_append_entries(self, data: Dict[str, Any]) -> Dict[str, Any]:
        async with self._state_lock:
            term = int(data.get("term", 0))
            leader_id = data.get("leader_id")
            prev_log_index = int(data.get("prev_log_index", -1))
            prev_log_term = int(data.get("prev_log_term", 0))
            entries = data.get("entries", [])
            leader_commit = int(data.get("leader_commit", -1))

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            self.leader_id = leader_id
            self._last_heartbeat = time.monotonic()
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
            self.state = "follower"

            if prev_log_index >= 0:
                if prev_log_index >= len(self.log):
                    return {"term": self.current_term, "success": False}
                if self.log[prev_log_index].term != prev_log_term:
                    return {"term": self.current_term, "success": False}

            for offset, entry_data in enumerate(entries):
                index = prev_log_index + 1 + offset
                entry = LogEntry(term=entry_data["term"], command=entry_data["command"])
                if index < len(self.log) and self.log[index].term != entry.term:
                    self.log = self.log[:index]
                if index == len(self.log):
                    self.log.append(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)

        await self._apply_committed()
        return {"term": self.current_term, "success": True}

    async def append_entry(self, command: Dict[str, Any], timeout_s: float = 2.0) -> bool:
        if not self.is_leader():
            return False
        async with self._state_lock:
            entry = LogEntry(term=self.current_term, command=command)
            self.log.append(entry)
            entry_index = len(self.log) - 1

        success = await self._replicate_log(timeout_s)
        if success:
            async with self._state_lock:
                if entry_index > self.commit_index:
                    self.commit_index = entry_index
            await self._apply_committed()
        return success

    async def _replicate_log(self, timeout_s: float) -> bool:
        tasks = [asyncio.create_task(self._replicate_to_peer(peer_id)) for peer_id in self.peers]
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=timeout_s
            )
        except asyncio.TimeoutError:
            # Even on timeout, check if completed tasks already give us majority
            results = []
            for task in tasks:
                if task.done() and not task.cancelled():
                    results.append(task.result())
                else:
                    task.cancel()

        successes = 1
        for result in results:
            if isinstance(result, Exception):
                continue
            if result:
                successes += 1
        if successes >= self.majority():
            await self._advance_commit_index()
            return True
        return False

    async def _replicate_to_peer(self, peer_id: str) -> bool:
        # Read log state without lock — safe because only leader mutates,
        # and we're taking a point-in-time snapshot for replication.
        next_index = self.next_index.get(peer_id, len(self.log))
        prev_index = next_index - 1
        prev_term = self.log[prev_index].term if 0 <= prev_index < len(self.log) else 0
        log_len = len(self.log)
        entries = [
            {"term": entry.term, "command": entry.command}
            for entry in self.log[next_index:log_len]
        ]
        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

        response = await self.message_bus.send_json(
            peer_id, "/raft/append_entries", payload
        )
        if not response.get("ok"):
            return False
        data = response.get("data", {})
        if not data.get("success"):
            self.next_index[peer_id] = max(0, next_index - 1)
            return False

        new_match = next_index + len(entries) - 1
        self.match_index[peer_id] = new_match
        self.next_index[peer_id] = next_index + len(entries)
        return True

    async def _advance_commit_index(self) -> None:
        async with self._state_lock:
            match_indexes = list(self.match_index.values()) + [len(self.log) - 1]
            match_indexes.sort(reverse=True)
            majority_index = match_indexes[self.majority() - 1]
            if majority_index > self.commit_index:
                if self.log[majority_index].term == self.current_term:
                    self.commit_index = majority_index
        await self._apply_committed()

    async def _apply_committed(self) -> None:
        while True:
            async with self._state_lock:
                if self.last_applied >= self.commit_index:
                    return
                self.last_applied += 1
                entry = self.log[self.last_applied]
            if self.apply_callback:
                result = self.apply_callback(entry.command)
                if asyncio.iscoroutine(result):
                    await result

    async def _election_loop(self) -> None:
        while self._running:
            timeout = random.uniform(
                self.election_timeout_ms / 1000.0,
                self.election_timeout_ms / 1000.0 * 2.0,
            )
            await asyncio.sleep(timeout)
            if self.is_leader():
                continue
            if time.monotonic() - self._last_heartbeat < timeout:
                continue
            await self._start_election()

    async def _start_election(self) -> None:
        async with self._state_lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self._last_heartbeat = time.monotonic()
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
            payload = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            }

        votes = 1
        tasks = [self.message_bus.send_json(peer_id, "/raft/request_vote", payload) for peer_id in self.peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                continue
            data = result.get("data", {})
            if data.get("vote_granted"):
                votes += 1

        if votes >= self.majority():
            await self._become_leader()

    async def _become_leader(self) -> None:
        async with self._state_lock:
            self.state = "leader"
            self.leader_id = self.node_id
            for peer_id in self.peers:
                self.next_index[peer_id] = len(self.log)
                self.match_index[peer_id] = -1
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while self._running and self.is_leader():
            await asyncio.gather(
                *[self._replicate_to_peer(peer_id) for peer_id in self.peers],
                return_exceptions=True,
            )
            await asyncio.sleep(self.heartbeat_ms / 1000.0)

    def _is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        if not self.log:
            return True
        local_last_index = len(self.log) - 1
        local_last_term = self.log[local_last_index].term
        if last_log_term != local_last_term:
            return last_log_term > local_last_term
        return last_log_index >= local_last_index
