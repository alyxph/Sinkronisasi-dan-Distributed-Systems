from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class PbftMessage:
    view: int
    seq: int
    payload: Dict[str, Any]


class PbftNode:
    def __init__(self) -> None:
        self.view = 0

    def handle_message(self, message: PbftMessage) -> None:
        raise NotImplementedError("PBFT implementation is optional")
