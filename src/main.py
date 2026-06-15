"""
main.py — Entry point for the audio-receiver service.

Loads microphones.yaml, starts one RtspNode thread per enabled node,
wires the AudioPipeline as the chunk consumer, and manages MQTT lifecycle.

Usage:
    python3 src/main.py [--config config/microphones.yaml]

Responds to SIGTERM / SIGINT with a graceful shutdown.
"""

import argparse
import signal
import sys
import time
import threading
from pathlib import Path
from typing import Any

import yaml  # PyYAML

from logger import get_logger
from rtsp_node import RtspNode
from audio_pipeline import AudioPipeline, _birdnet_summary
import mqtt_bridge

log = get_logger("main")

_DEFAULT_CONFIG = Path(__file__).parent.parent / "config" / "microphones.yaml"

# ── Config loader ────────────────────────────────────────────────────────────

def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Berkeley Audio Receiver")
    parser.add_argument(
        "--config",
        default=str(_DEFAULT_CONFIG),
        help="Path to microphones.yaml (default: config/microphones.yaml)",
    )
    args = parser.parse_args()

    log.info("Audio receiver starting", extra={"config": args.config})

    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        log.error("Config file not found", extra={"path": args.config})
        sys.exit(1)
    except yaml.YAMLError as exc:
        log.error("Config parse error", extra={"err": str(exc)})
        sys.exit(1)

    ingest_url = cfg.get("ingest_url", "http://localhost:5050/api/ingest")
    archive_cfg = cfg.get("archive", {})
    archive_enabled = archive_cfg.get("enabled", True)
    archive_base = archive_cfg.get("base_dir", "./data/audio") if archive_enabled else None
    archive_format = archive_cfg.get("format", "wav")

    # Build pipeline (shared across all nodes)
    pipeline = AudioPipeline(cfg)

    # Create one RtspNode per enabled node
    nodes_cfg: list[dict[str, Any]] = cfg.get("nodes", [])
    active_nodes = [n for n in nodes_cfg if n.get("enabled", True)]

    if not active_nodes:
        log.error("No enabled nodes found in config — check microphones.yaml")
        sys.exit(1)

    log.info("Nodes to start", extra={
        "count": len(active_nodes),
        "ids": [n["id"] for n in active_nodes],
    })

    node_threads: list[RtspNode] = []

    for node_cfg in active_nodes:
        node = RtspNode(
            node_config=node_cfg,
            on_chunk=pipeline.process,
            ingest_url=ingest_url,
            archive_base_dir=archive_base,
            archive_format=archive_format,
        )
        node_threads.append(node)

    # ── MQTT lifecycle: publish online status ─────────────────────────────
    mqtt_bridge.start([n.node_id for n in node_threads])

    # ── BirdNET message summary: start hourly publisher ─────────────────
    _birdnet_summary.start()

    # ── Graceful shutdown handler ─────────────────────────────────────────
    shutdown_event = threading.Event()

    def _shutdown(sig, frame):
        log.info("Shutdown signal received", extra={"signal": sig})
        for n in node_threads:
            n.stop()
        pipeline.shutdown()
        _birdnet_summary.stop()   # flush final partial window to Messages inbox
        mqtt_bridge.stop()
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # ── Start all node threads ────────────────────────────────────────────
    for node in node_threads:
        node.start()
        log.info("Node thread started", extra={"node": node.node_id})

    log.info("All nodes running — waiting for audio...")

    # Block main thread until shutdown
    shutdown_event.wait()

    # Give threads a moment to finish current chunk
    for node in node_threads:
        node.join(timeout=10)

    log.info("Audio receiver stopped cleanly")


if __name__ == "__main__":
    main()
