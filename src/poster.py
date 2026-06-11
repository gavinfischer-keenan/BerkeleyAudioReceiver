"""
poster.py — HTTP client that POSTs detection results to the api-server ingest endpoint.

Implements retry logic with exponential backoff so transient network hiccups
(e.g. server restart) don't drop detections permanently.
"""

import time
import urllib.request
import urllib.error
import json
from typing import Any

from logger import get_logger

log = get_logger("poster")

_DEFAULT_RETRIES = 3
_BASE_BACKOFF_S = 1.0


def post_detection(
    ingest_url: str,
    node_id: str,
    analyzer: str,
    detections: list[dict[str, Any]],
    node_meta: dict[str, Any],
    retries: int = _DEFAULT_RETRIES,
) -> bool:
    """
    POST audio detection results to /api/ingest/audio-<node_id>.

    Payload shape:
        {
          "data": {
            "nodeId": "garden-east",
            "analyzer": "birdnet",
            "location": { "name": "East Garden", "lat": 37.87, "lng": -122.27 },
            "detections": [ { "species": "...", "commonName": "...", ... } ]
          },
          "metadata": { "source": "audio-receiver", "node": "garden-east" }
        }
    """
    service_name = f"audio-{node_id}"
    url = f"{ingest_url.rstrip('/')}/{service_name}"

    payload = {
        "data": {
            "nodeId": node_id,
            "analyzer": analyzer,
            "location": node_meta.get("location_obj", {}),
            "detections": detections,
            "timestamp": int(time.time() * 1000),
        },
        "metadata": {
            "source": "audio-receiver",
            "node": node_id,
            "analyzer": analyzer,
        },
    }

    body = json.dumps(payload).encode("utf-8")

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp_body = resp.read().decode()
                log.info("Posted detections", extra={
                    "node": node_id,
                    "analyzer": analyzer,
                    "count": len(detections),
                    "status": resp.status,
                })
                return True
        except urllib.error.URLError as exc:
            delay = _BASE_BACKOFF_S * (2 ** (attempt - 1))
            log.warning("Post failed, retrying", extra={
                "node": node_id,
                "attempt": attempt,
                "error": str(exc),
                "retry_in_s": delay,
            })
            if attempt < retries:
                time.sleep(delay)

    log.error("All post attempts failed", extra={"node": node_id, "analyzer": analyzer})
    return False


def post_node_status(
    ingest_url: str,
    node_id: str,
    status: str,  # "online" | "offline" | "degraded"
    detail: str = "",
) -> None:
    """Report node connection status to api-server."""
    url = f"{ingest_url.rstrip('/')}/audio-{node_id}-status"
    payload = {
        "data": {
            "nodeId": node_id,
            "status": status,
            "detail": detail,
            "timestamp": int(time.time() * 1000),
        },
        "metadata": {"source": "audio-receiver", "type": "status"},
    }
    body = json.dumps(payload).encode("utf-8")
    try:
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception as exc:
        log.debug("Status post failed (non-critical)", extra={"node": node_id, "error": str(exc)})
