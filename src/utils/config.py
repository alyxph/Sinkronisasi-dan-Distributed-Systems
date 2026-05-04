from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv


@dataclass(frozen=True)
class PeerInfo:
    node_id: str
    url: str


@dataclass
class NodeConfig:
    node_id: str
    role: str
    host: str
    port: int
    peers: List[PeerInfo]
    data_dir: str
    raft_election_timeout_ms: int
    raft_heartbeat_ms: int
    failure_detector_interval_ms: int
    lock_request_timeout_ms: int
    deadlock_check_interval_ms: int
    queue_visibility_timeout_s: int
    queue_requeue_interval_s: int
    cache_capacity: int
    log_level: str


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_peers(raw: str) -> List[PeerInfo]:
    if not raw:
        return []
    raw = raw.strip()
    peers: List[PeerInfo] = []
    if raw.startswith("["):
        data = json.loads(raw)
        for item in data:
            node_id = item.get("id") or item.get("node_id")
            url = item.get("url")
            if node_id and url:
                peers.append(PeerInfo(node_id=node_id, url=url))
        return peers

    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            node_id, url = part.split("=", 1)
            peers.append(PeerInfo(node_id=node_id.strip(), url=url.strip()))
        else:
            peers.append(PeerInfo(node_id=part, url=part))
    return peers


def load_config() -> NodeConfig:
    load_dotenv()
    return NodeConfig(
        node_id=os.getenv("NODE_ID", "node-1"),
        role=os.getenv("NODE_ROLE", "lock_manager"),
        host=os.getenv("HOST", "0.0.0.0"),
        port=_parse_int(os.getenv("PORT", "7001"), 7001),
        peers=parse_peers(os.getenv("PEERS", "")),
        data_dir=os.getenv("DATA_DIR", "./data"),
        raft_election_timeout_ms=_parse_int(
            os.getenv("RAFT_ELECTION_TIMEOUT_MS", "1500"), 1500
        ),
        raft_heartbeat_ms=_parse_int(os.getenv("RAFT_HEARTBEAT_MS", "500"), 500),
        failure_detector_interval_ms=_parse_int(
            os.getenv("FAILURE_DETECTOR_INTERVAL_MS", "1000"), 1000
        ),
        lock_request_timeout_ms=_parse_int(
            os.getenv("LOCK_REQUEST_TIMEOUT_MS", "5000"), 5000
        ),
        deadlock_check_interval_ms=_parse_int(
            os.getenv("DEADLOCK_CHECK_INTERVAL_MS", "2000"), 2000
        ),
        queue_visibility_timeout_s=_parse_int(
            os.getenv("QUEUE_VISIBILITY_TIMEOUT_S", "15"), 15
        ),
        queue_requeue_interval_s=_parse_int(
            os.getenv("QUEUE_REQUEUE_INTERVAL_S", "5"), 5
        ),
        cache_capacity=_parse_int(os.getenv("CACHE_CAPACITY", "1000"), 1000),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
