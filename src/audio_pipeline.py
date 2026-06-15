"""
audio_pipeline.py — Orchestrates the analysis pipeline for each WAV chunk.

Called by RtspNode.on_chunk every time a new audio segment is captured.
Dispatches to all configured analyzers in parallel via a thread pool,
archives interesting clips, POSTs results to the api-server ingest bus,
and publishes detections to MQTT for the home intelligence bus.

Dual communication:
  - HTTP POST → api-server (dashboard, Socket.IO consumers)
  - MQTT      → Mosquitto bus (cross-modal AI, Alexa, HomeKit)
"""

from __future__ import annotations

import concurrent.futures
import sys
import os
from typing import Any

# Make src/ importable when running directly
sys.path.insert(0, os.path.dirname(__file__))

from base_analyzer import Detection
from analyzers import get_analyzers
from audio_archiver import AudioArchiver
from audio_store import store as local_store
from poster import post_detection
import mqtt_bridge
import message_publisher
from logger import get_logger

log = get_logger("pipeline")

# BirdNET message summary — accumulates detections and publishes hourly to
# home/messages/birdnet/summary for the BerkeleyMessages AI inbox.
# Started/stopped alongside the pipeline by main.py.
_birdnet_summary = message_publisher.BirdNetMessageSummary()

# Conservative thread count — keeps a Pi responsive for other tasks.
# Increase on beefy home servers.
_MAX_WORKERS = 4


class AudioPipeline:
    """
    Thread-pool-backed pipeline shared across all RTSP node threads.

    Args:
        config: full parsed microphones.yaml dict
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._ingest_url: str = config.get("ingest_url", "http://localhost:5050/api/ingest")
        self._archiver = AudioArchiver(config.get("archive", {}))
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=_MAX_WORKERS,
            thread_name_prefix="pipeline",
        )
        log.info("Pipeline ready", extra={"ingest_url": self._ingest_url})

    def process(self, node_id: str, wav_path: str, node_cfg: dict[str, Any]) -> None:
        """
        Accept a WAV chunk from an RTSP node and submit it for async analysis.
        Returns immediately — analysis happens in the thread pool.
        """
        self._executor.submit(self._run, node_id, wav_path, node_cfg)

    def _run(self, node_id: str, wav_path: str, node_cfg: dict[str, Any]) -> None:
        """Run all configured analyzers on a single WAV chunk."""
        requested_analyzers: list[str] = node_cfg.get("analyzers", ["birdnet"])
        analyzers = get_analyzers(requested_analyzers, self._config)

        node_meta = {
            "location_obj": {
                "name":        node_cfg.get("label", node_id),
                "description": node_cfg.get("location", ""),
                "lat":         float(node_cfg.get("lat", 0)),
                "lng":         float(node_cfg.get("lng", 0)),
            }
        }

        for analyzer in analyzers:
            try:
                detections: list[Detection] = analyzer.run(wav_path, node_cfg)
            except Exception as exc:
                log.error("Analyzer raised unexpectedly", extra={
                    "node": node_id, "analyzer": analyzer.name, "err": str(exc),
                })
                continue

            if not detections:
                log.debug("No detections", extra={"node": node_id, "analyzer": analyzer.name})
                continue

            det_dicts = [d.to_dict() for d in detections]

            # 1. Store locally (in-process ring buffer — Python side)
            local_store.add(node_id, analyzer.name, det_dicts, node_meta, wav_path)

            # 2. Archive interesting clips to disk
            #    archiver decides which rule (if any) triggered the save
            self._archiver.evaluate(node_id, wav_path, detections, analyzer.name)

            # 3. POST to api-server ingest bus
            #    → stored in TypeScript AudioDetectionStore
            #    → broadcast via Socket.IO to all connected consumers
            post_detection(
                ingest_url=self._ingest_url,
                node_id=node_id,
                analyzer=analyzer.name,
                detections=det_dicts,
                node_meta=node_meta,
            )

            # 4. Publish to MQTT bus (if enabled)
            #    → consumed by cross-modal AI, Alexa, HomeKit, event store
            mqtt_bridge.publish_detection(
                node_id=node_id,
                analyzer=analyzer.name,
                detections=det_dicts,
                node_meta=node_meta,
            )

            # 5. Accumulate in BirdNET message summary (birdnet detections only)
            #    Hourly summary → home/messages/birdnet/summary → BerkeleyMessages inbox
            #    High-confidence detections → home/messages/birdnet/notable (immediate)
            if analyzer.name == "birdnet" and detections:
                _birdnet_summary.record(detections, node_id)

            log.info("Chunk processed", extra={
                "node":     node_id,
                "analyzer": analyzer.name,
                "hits":     len(detections),
                "top":      detections[0].common_name if detections else None,
            })

    def shutdown(self) -> None:
        """Graceful shutdown — waits for in-flight analysis jobs to finish."""
        log.info("Shutting down pipeline — waiting for in-flight jobs...")
        self._executor.shutdown(wait=True, cancel_futures=False)
        log.info("Pipeline shut down cleanly")
