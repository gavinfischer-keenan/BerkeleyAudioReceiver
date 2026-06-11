"""
audio_store.py — Local in-process detection ring-buffer.

Provides a thread-safe store for recent detections per node.
This is the Python-side cache; the api-server maintains its own
TypeScript-side store for API/WebSocket consumers.
"""

import threading
import time
from collections import deque
from typing import Any

from logger import get_logger

log = get_logger("audio_store")

_MAX_PER_NODE = 500   # detections to keep per node
_MAX_GLOBAL = 2000    # global rolling window


class AudioStore:
    """Thread-safe in-memory store for recent audio detections."""

    def __init__(self, max_per_node: int = _MAX_PER_NODE, max_global: int = _MAX_GLOBAL):
        self._lock = threading.Lock()
        self._by_node: dict[str, deque] = {}
        self._global: deque = deque(maxlen=max_global)
        self._max_per_node = max_per_node

    def add(
        self,
        node_id: str,
        analyzer: str,
        detections: list[dict[str, Any]],
        node_meta: dict[str, Any],
        chunk_file: str | None = None,
    ) -> None:
        """Store a batch of detections from one analysis run."""
        ts = int(time.time() * 1000)

        records = []
        for det in detections:
            record = {
                "nodeId": node_id,
                "location": node_meta.get("location_obj", {}),
                "timestamp": ts,
                "analyzer": analyzer,
                "species": det.get("species", ""),
                "commonName": det.get("commonName", ""),
                "confidence": det.get("confidence", 0.0),
                "startTime": det.get("startTime", 0.0),
                "endTime": det.get("endTime", 0.0),
                "chunkFile": chunk_file,
            }
            records.append(record)

        with self._lock:
            if node_id not in self._by_node:
                self._by_node[node_id] = deque(maxlen=self._max_per_node)
            for rec in records:
                self._by_node[node_id].append(rec)
                self._global.append(rec)

        if records:
            log.debug("Stored detections", extra={
                "node": node_id,
                "analyzer": analyzer,
                "count": len(records),
            })

    def get_node(self, node_id: str, limit: int = 50) -> list[dict]:
        """Return recent detections for a specific node."""
        with self._lock:
            buf = self._by_node.get(node_id, deque())
            items = list(buf)
        return items[-limit:]

    def get_global(self, limit: int = 100) -> list[dict]:
        """Return most recent detections across all nodes."""
        with self._lock:
            items = list(self._global)
        return items[-limit:]

    def get_node_ids(self) -> list[str]:
        """Return list of node IDs that have received detections."""
        with self._lock:
            return list(self._by_node.keys())

    def clear_node(self, node_id: str) -> None:
        with self._lock:
            if node_id in self._by_node:
                self._by_node[node_id].clear()


# Singleton instance used across the service
store = AudioStore()
