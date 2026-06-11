"""MQTT bridge — publishes audio detections and status to the home intelligence bus.

Follows the standard Berkeley agent lifecycle:
  start()  → publishes online status (retained)
  stop()   → publishes offline status (retained)
  publish_detection() → publishes to home/events/bird-audio or bat-audio

Topic schema (matches architecture doc):
  home/events/bird-audio           — BirdNET detections
  home/events/bat-audio            — BatNET detections
  home/status/audio-receiver       — service heartbeat (retained)
  home/status/audio-receiver/{id}  — per-node status (retained)
  home/sensors/audio-levels/{id}   — per-node dB levels
"""
from __future__ import annotations

import json
import os
import time
import threading
from typing import Any

from logger import get_logger

log = get_logger("mqtt_bridge")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_ENABLED = os.getenv("MQTT_ENABLED", "false").lower() in ("true", "1", "yes")

TOPIC_EVENTS_BIRD = "home/events/bird-audio"
TOPIC_EVENTS_BAT = "home/events/bat-audio"
TOPIC_STATUS = "home/status/audio-receiver"
TOPIC_ALERTS = "home/alerts/audio"
TOPIC_SENSORS_LEVELS = "home/sensors/audio-levels"

_client = None
_lock = threading.Lock()


def _get_client():
    """Lazy-init MQTT client. Returns None if disabled or paho-mqtt missing."""
    global _client
    if _client is not None:
        return _client
    if not MQTT_ENABLED:
        return None
    try:
        import paho.mqtt.client as mqtt
        with _lock:
            if _client is not None:
                return _client
            _client = mqtt.Client(client_id="audio-receiver", protocol=mqtt.MQTTv311)
            # LWT marks agent offline if connection drops unexpectedly
            _client.will_set(
                TOPIC_STATUS,
                json.dumps({"status": "offline", "agent": "audio-receiver"}),
                qos=1, retain=True,
            )
            _client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            _client.loop_start()
            log.info("MQTT connected", extra={"broker": MQTT_BROKER, "port": MQTT_PORT})
            return _client
    except ImportError:
        log.warning("paho-mqtt not installed — MQTT disabled")
        return None
    except Exception as exc:
        log.warning("MQTT connect failed", extra={"error": str(exc)})
        return None


def start(active_nodes: list[str]) -> None:
    """Standard agent lifecycle: publish online status."""
    client = _get_client()
    if not client:
        return
    client.publish(TOPIC_STATUS, json.dumps({
        "status": "online",
        "agent": "audio-receiver",
        "active_nodes": active_nodes,
        "timestamp": int(time.time() * 1000),
    }), qos=1, retain=True)
    log.info("MQTT status: online", extra={"nodes": active_nodes})


def stop() -> None:
    """Standard agent lifecycle: publish offline status and disconnect."""
    client = _get_client()
    if not client:
        return
    client.publish(TOPIC_STATUS, json.dumps({
        "status": "offline",
        "agent": "audio-receiver",
        "timestamp": int(time.time() * 1000),
    }), qos=1, retain=True)
    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    log.info("MQTT disconnected")


def publish_detection(
    node_id: str,
    analyzer: str,
    detections: list[dict[str, Any]],
    node_meta: dict[str, Any],
) -> None:
    """Publish each detection to the MQTT events topic."""
    client = _get_client()
    if not client:
        return
    topic = TOPIC_EVENTS_BAT if analyzer == "batnet" else TOPIC_EVENTS_BIRD
    for det in detections:
        payload = {
            "node_id": node_id,
            "analyzer": analyzer,
            "species": det.get("species", ""),
            "common_name": det.get("commonName", ""),
            "confidence": det.get("confidence", 0.0),
            "start_time": det.get("startTime", 0.0),
            "end_time": det.get("endTime", 0.0),
            "location": node_meta.get("location_obj", {}),
            "timestamp": int(time.time() * 1000),
        }
        client.publish(topic, json.dumps(payload), qos=1)
    log.debug("MQTT detections published", extra={
        "node": node_id, "analyzer": analyzer, "count": len(detections),
    })


def publish_node_status(node_id: str, status: str, detail: str = "") -> None:
    """Publish per-node status (online/offline/degraded)."""
    client = _get_client()
    if not client:
        return
    client.publish(f"{TOPIC_STATUS}/{node_id}", json.dumps({
        "node_id": node_id,
        "status": status,
        "detail": detail,
        "timestamp": int(time.time() * 1000),
    }), qos=0, retain=True)


def publish_audio_level(node_id: str, db_level: float) -> None:
    """Publish per-node audio level telemetry."""
    client = _get_client()
    if not client:
        return
    client.publish(f"{TOPIC_SENSORS_LEVELS}/{node_id}", json.dumps({
        "node_id": node_id,
        "db_level": round(db_level, 1),
        "timestamp": int(time.time() * 1000),
    }), qos=0)
