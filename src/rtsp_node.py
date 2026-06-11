"""
rtsp_node.py — Per-node RTSP stream consumer.

Continuously connects to an RTSP stream via ffmpeg, captures fixed-length
WAV chunks, and delivers them to the audio pipeline for analysis.

Each node runs in its own thread. On stream failure, exponential backoff
is applied before reconnecting, so a dropped Wi-Fi node doesn't spam logs.
"""

import subprocess
import threading
import time
import tempfile
import os
import shutil
from pathlib import Path
from typing import Callable, Any

from logger import get_logger
from poster import post_node_status

log = get_logger("rtsp_node")

# Reconnect backoff: starts at 5s, doubles up to MAX
_MIN_BACKOFF_S = 5.0
_MAX_BACKOFF_S = 120.0

# How many consecutive ffmpeg failures before marking node "degraded"
_DEGRADED_THRESHOLD = 3


class RtspNode(threading.Thread):
    """
    A background thread that continuously reads RTSP audio and emits WAV chunks.

    Args:
        node_config:      dict from microphones.yaml for this node
        on_chunk:         callback(node_id, chunk_wav_path, node_config) called for each chunk
        ingest_url:       api-server ingest base URL for status reporting
        archive_base_dir: if set, archive WAV chunks here; otherwise use a temp dir
        archive_format:   "wav" or "flac"
    """

    def __init__(
        self,
        node_config: dict[str, Any],
        on_chunk: Callable[[str, str, dict[str, Any]], None],
        ingest_url: str,
        archive_base_dir: str | None = None,
        archive_format: str = "wav",
    ):
        super().__init__(name=f"node-{node_config['id']}", daemon=True)
        self.cfg = node_config
        self.node_id: str = node_config["id"]
        self.rtsp_url: str = node_config["rtsp_url"]
        self.sample_rate: int = node_config.get("sample_rate", 48000)
        self.chunk_secs: int = node_config.get("chunk_secs", 15)
        self.on_chunk = on_chunk
        self.ingest_url = ingest_url
        self.archive_base_dir = archive_base_dir
        self.archive_format = archive_format
        self._stop_event = threading.Event()
        self._failure_count = 0
        self._backoff = _MIN_BACKOFF_S

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        log.info("Node starting", extra={"node": self.node_id, "url": self.rtsp_url})
        post_node_status(self.ingest_url, self.node_id, "initializing")

        while not self._stop_event.is_set():
            success = self._capture_chunk()

            if success:
                self._failure_count = 0
                self._backoff = _MIN_BACKOFF_S
            else:
                self._failure_count += 1
                if self._failure_count >= _DEGRADED_THRESHOLD:
                    post_node_status(
                        self.ingest_url,
                        self.node_id,
                        "degraded",
                        f"Stream unavailable after {self._failure_count} attempts",
                    )
                log.warning("Stream failure, backing off", extra={
                    "node": self.node_id,
                    "failures": self._failure_count,
                    "backoff_s": self._backoff,
                })
                self._stop_event.wait(self._backoff)
                self._backoff = min(self._backoff * 2, _MAX_BACKOFF_S)

        log.info("Node stopped", extra={"node": self.node_id})
        post_node_status(self.ingest_url, self.node_id, "offline", "Service shutdown")

    def _capture_chunk(self) -> bool:
        """Capture one chunk from the RTSP stream. Returns True on success."""
        # Determine output path
        if self.archive_base_dir:
            from datetime import date
            today = date.today().isoformat()
            out_dir = Path(self.archive_base_dir) / self.node_id / today
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = int(time.time())
            out_path = str(out_dir / f"{self.node_id}_{ts}.{self.archive_format}")
            use_temp = False
        else:
            tmp_dir = tempfile.mkdtemp(prefix=f"audio_{self.node_id}_")
            out_path = os.path.join(tmp_dir, f"chunk.{self.archive_format}")
            use_temp = True

        cmd = self._build_ffmpeg_cmd(out_path)

        log.debug("Capturing chunk", extra={
            "node": self.node_id,
            "out": out_path,
            "duration_s": self.chunk_secs,
        })

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.chunk_secs + 30,  # generous timeout
            )
        except subprocess.TimeoutExpired:
            log.warning("ffmpeg timed out", extra={"node": self.node_id})
            if use_temp:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            return False
        except FileNotFoundError:
            log.error("ffmpeg not found — install with: sudo apt install ffmpeg")
            self._stop_event.wait(60)  # Don't hammer if ffmpeg is missing
            return False

        if result.returncode != 0:
            log.warning("ffmpeg error", extra={
                "node": self.node_id,
                "returncode": result.returncode,
                "stderr": result.stderr[-300:],
            })
            if use_temp:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            return False

        if not os.path.exists(out_path) or os.path.getsize(out_path) < 1000:
            log.warning("Output WAV empty or missing", extra={"node": self.node_id, "path": out_path})
            if use_temp:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            return False

        # Signal first successful capture
        if self._failure_count > 0 or self._backoff > _MIN_BACKOFF_S:
            post_node_status(self.ingest_url, self.node_id, "online", "Stream recovered")
        elif self._failure_count == 0:
            post_node_status(self.ingest_url, self.node_id, "online")

        # Dispatch chunk to pipeline
        try:
            self.on_chunk(self.node_id, out_path, self.cfg)
        except Exception as exc:
            log.error("Pipeline callback error", extra={"node": self.node_id, "err": str(exc)})

        # Clean up temp dir only if not archiving
        if use_temp:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        return True

    def _build_ffmpeg_cmd(self, out_path: str) -> list[str]:
        """Build the ffmpeg command to capture a fixed-duration chunk."""
        codec = "flac" if self.archive_format == "flac" else "pcm_s16le"
        fmt = "flac" if self.archive_format == "flac" else "wav"

        return [
            "ffmpeg",
            "-loglevel", "warning",
            "-rtsp_transport", "tcp",         # TCP for reliability on local LAN
            "-i", self.rtsp_url,
            "-t", str(self.chunk_secs),        # duration
            "-ar", str(self.sample_rate),      # resample to target rate
            "-ac", "1",                        # mono
            "-acodec", codec,
            "-f", fmt,
            "-y",                              # overwrite output
            out_path,
        ]
