"""message_publisher.py — Publishes BirdNET observation summaries to BerkeleyMessages.

Distinct from mqtt_bridge.py (which handles real-time alert/event MQTT):
  - Summaries are NOT alerts — they go to home/messages/birdnet/summary
  - BerkeleyMessages subscribes to home/messages/# and stores them in the inbox
  - These appear in the AI Agent Messages inbox, NOT the alarm panel

Payload schema (matches BerkeleyMessages MessageType.observation):
  message_id   : str  — unique ID for dedup
  source       : "birdnet"
  type         : "observation"
  subject      : str  — human-readable title
  body         : str  — multi-line Markdown summary
  priority     : "low"
  action_required: false
  tags         : list[str]  — species names for filtering
  timestamp    : ISO-8601 UTC
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from logger import get_logger

log = get_logger("message_publisher")

MQTT_BROKER    = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT      = int(os.getenv("MQTT_PORT", "1883"))
MQTT_ENABLED   = os.getenv("MQTT_ENABLED", "false").lower() in ("true", "1", "yes")

# Publish hourly summaries on this topic
TOPIC_SUMMARY  = "home/messages/birdnet/summary"
TOPIC_NOTABLE  = "home/messages/birdnet/notable"   # high-confidence rare species

# Summary interval in seconds (default: 1 hour)
SUMMARY_INTERVAL_SEC = int(os.getenv("BIRDNET_SUMMARY_INTERVAL_SEC", "3600"))

# Confidence threshold above which a detection is "notable"
NOTABLE_CONFIDENCE = float(os.getenv("BIRDNET_NOTABLE_CONFIDENCE", "0.92"))

_client = None
_lock   = threading.Lock()


def _get_client():
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
            c = mqtt.Client(client_id="birdnet-message-publisher", protocol=mqtt.MQTTv311)
            c.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            c.loop_start()
            _client = c
            log.info("message_publisher.mqtt_connected", extra={"broker": MQTT_BROKER})
            return _client
    except ImportError:
        log.warning("paho-mqtt not installed — message publishing disabled")
        return None
    except Exception as exc:
        log.warning("message_publisher.connect_failed", extra={"error": str(exc)})
        return None


def _publish(topic: str, payload: dict) -> None:
    client = _get_client()
    if not client:
        return
    client.publish(topic, json.dumps(payload, default=str), qos=1, retain=False)


class BirdNetMessageSummary:
    """Accumulates detections for the current window and publishes periodic summaries.

    Usage:
        summary = BirdNetMessageSummary()
        summary.start()                        # starts background timer thread
        summary.record(detection_list, node_id)  # called per audio chunk
        summary.stop()
    """

    def __init__(self) -> None:
        self._detections: list[dict] = []          # raw detection dicts this window
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._window_start: datetime = datetime.now(timezone.utc)

    def start(self) -> None:
        """Start the periodic summary publisher."""
        if not MQTT_ENABLED:
            log.info("message_publisher.disabled — set MQTT_ENABLED=true to enable summaries")
            return
        self._schedule_next()
        log.info("message_publisher.started",
                 extra={"interval_sec": SUMMARY_INTERVAL_SEC, "topic": TOPIC_SUMMARY})

    def stop(self) -> None:
        """Flush any buffered detections and stop the timer."""
        if self._timer:
            self._timer.cancel()
        self._publish_summary()

    def record(self, detections: list[Any], node_id: str) -> None:
        """Record a batch of Detection objects for inclusion in the next summary.

        Also immediately publishes a 'notable' message if any detection exceeds
        the NOTABLE_CONFIDENCE threshold — these warrant faster attention.
        """
        with self._lock:
            for d in detections:
                det_dict = {
                    "species":     getattr(d, "species", ""),
                    "common_name": getattr(d, "common_name", ""),
                    "confidence":  getattr(d, "confidence", 0.0),
                    "start_time":  getattr(d, "start_time", 0.0),
                    "end_time":    getattr(d, "end_time", 0.0),
                    "analyzer":    getattr(d, "analyzer", "birdnet"),
                    "node_id":     node_id,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
                self._detections.append(det_dict)

                # Publish notable detection immediately
                if det_dict["confidence"] >= NOTABLE_CONFIDENCE:
                    self._publish_notable(det_dict)

    # ── Private ────────────────────────────────────────────────────────────────

    def _schedule_next(self) -> None:
        self._timer = threading.Timer(SUMMARY_INTERVAL_SEC, self._on_timer)
        self._timer.daemon = True
        self._timer.start()

    def _on_timer(self) -> None:
        self._publish_summary()
        self._schedule_next()

    def _publish_summary(self) -> None:
        with self._lock:
            detections = list(self._detections)
            window_start = self._window_start
            self._detections.clear()
            self._window_start = datetime.now(timezone.utc)

        if not detections:
            log.debug("message_publisher.summary_skipped — no detections this window")
            return

        # Build counts
        species_counts: Counter[str] = Counter(d["species"] for d in detections)
        common_counts: dict[str, str] = {
            d["species"]: d["common_name"] for d in detections if d["common_name"]
        }
        best_confidence: dict[str, float] = {}
        for d in detections:
            sp = d["species"]
            if d["confidence"] > best_confidence.get(sp, 0.0):
                best_confidence[sp] = d["confidence"]

        node_ids = sorted({d["node_id"] for d in detections})
        total = len(detections)
        unique = len(species_counts)
        window_end = datetime.now(timezone.utc)
        duration_min = round((window_end - window_start).total_seconds() / 60)

        # Build Markdown body
        top_species = species_counts.most_common(10)
        rows = "\n".join(
            f"| {common_counts.get(sp, sp)} | *{sp}* | {cnt} | {best_confidence.get(sp, 0)*100:.0f}% |"
            for sp, cnt in top_species
        )
        body = (
            f"**{total} detections, {unique} species** over {duration_min} minutes "
            f"across {len(node_ids)} microphone node(s).\n\n"
            f"| Common Name | Scientific | Detections | Best Confidence |\n"
            f"|-------------|-----------|------------|----------------|\n"
            f"{rows}\n\n"
            f"_Nodes: {', '.join(node_ids)}_"
        )

        subject = (
            f"BirdNET: {total} detections, {unique} species "
            f"({window_start.strftime('%H:%M')}–{window_end.strftime('%H:%M')} "
            f"{window_start.strftime('%b %d')})"
        )

        payload = {
            "message_id":     str(uuid.uuid4()),
            "source":         "birdnet",
            "type":           "observation",
            "subject":        subject,
            "body":           body,
            "priority":       "low",
            "action_required": False,
            "action_type":    None,
            "action_data":    None,
            "tags":           list(species_counts.keys()),
            "timestamp":      datetime.now(timezone.utc).isoformat(),
        }
        _publish(TOPIC_SUMMARY, payload)
        log.info("message_publisher.summary_published",
                 extra={"detections": total, "species": unique, "topic": TOPIC_SUMMARY})

    def _publish_notable(self, detection: dict) -> None:
        """Immediately publish a high-confidence detection as a notable message."""
        common = detection["common_name"] or detection["species"]
        confidence_pct = detection["confidence"] * 100

        payload = {
            "message_id":     str(uuid.uuid4()),
            "source":         "birdnet",
            "type":           "observation",
            "subject":        f"Notable detection: {common} ({confidence_pct:.0f}% confidence)",
            "body":           (
                f"**{common}** (*{detection['species']}*) detected at "
                f"**{confidence_pct:.0f}% confidence** on node `{detection['node_id']}`.\n\n"
                f"This exceeds the notable threshold ({NOTABLE_CONFIDENCE*100:.0f}%). "
                f"Detection window: {detection['start_time']:.1f}s–{detection['end_time']:.1f}s."
            ),
            "priority":       "low",
            "action_required": False,
            "action_type":    None,
            "action_data":    None,
            "tags":           [detection["species"], "notable"],
            "timestamp":      detection["recorded_at"],
        }
        _publish(TOPIC_NOTABLE, payload)
        log.info("message_publisher.notable_published",
                 extra={"species": detection["species"], "confidence": detection["confidence"]})
