from __future__ import annotations

import bisect
import hashlib
from typing import List, Optional


class ConsistentHashRing:
    def __init__(self, nodes: List[str], replicas: int = 100) -> None:
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys: List[int] = []
        self.set_nodes(nodes)

    def set_nodes(self, nodes: List[str]) -> None:
        self.ring.clear()
        self.sorted_keys.clear()
        for node in nodes:
            for replica in range(self.replicas):
                key = self._hash(f"{node}:{replica}")
                self.ring[key] = node
                self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def _hash(self, value: str) -> int:
        return int(hashlib.sha1(value.encode("utf-8")).hexdigest(), 16)

    def get_node(self, key: str) -> Optional[str]:
        if not self.sorted_keys:
            return None
        hvalue = self._hash(key)
        index = bisect.bisect_right(self.sorted_keys, hvalue)
        if index == len(self.sorted_keys):
            index = 0
        return self.ring[self.sorted_keys[index]]

    def get_nodes(self, key: str, count: int) -> List[str]:
        if not self.sorted_keys or count <= 0:
            return []
        hvalue = self._hash(key)
        index = bisect.bisect_right(self.sorted_keys, hvalue)
        if index == len(self.sorted_keys):
            index = 0
        result = []
        seen = set()
        for offset in range(len(self.sorted_keys)):
            node = self.ring[self.sorted_keys[(index + offset) % len(self.sorted_keys)]]
            if node not in seen:
                seen.add(node)
                result.append(node)
            if len(result) >= count:
                break
        return result
