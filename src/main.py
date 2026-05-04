from __future__ import annotations

import asyncio
import logging
import signal
import sys

from src.nodes.cache_node import DistributedCacheNode
from src.nodes.lock_manager import DistributedLockManagerNode
from src.nodes.queue_node import DistributedQueueNode
from src.utils.config import load_config


def setup_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress noisy aiohttp access logs (health checks every 1s × all peers)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


async def main() -> None:
    config = load_config()
    setup_logging(config.log_level)
    logger = logging.getLogger("main")

    if config.role == "lock_manager":
        node = DistributedLockManagerNode(config)
    elif config.role == "queue_node":
        node = DistributedQueueNode(config)
    elif config.role == "cache_node":
        node = DistributedCacheNode(config)
    else:
        raise ValueError(f"Unknown NODE_ROLE: {config.role}")

    await node.start()
    logger.info("Node %s (%s) started on %s:%s", config.node_id, config.role, config.host, config.port)

    stop_event = asyncio.Event()

    def _handle_stop(*_args) -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    # Unix signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_stop)
        except NotImplementedError:
            # Windows: fall back to signal.signal
            signal.signal(sig, _handle_stop)

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass

    logger.info("Stopping node %s ...", config.node_id)
    await node.stop()
    logger.info("Node %s stopped", config.node_id)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
