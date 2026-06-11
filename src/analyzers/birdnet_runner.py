"""
analyzers/birdnet_runner.py — BirdNET-Analyzer implementation of BaseAnalyzer.

System dependency: BirdNET-Analyzer
  Source:  https://github.com/kahst/BirdNET-Analyzer
  Install: /opt/BirdNET-Analyzer (via setup.sh)
  Runtime: python3 (subprocess) — NOT imported as a library
  Models:  bundled inside /opt/BirdNET-Analyzer/checkpoints/

Binary invoked:
  python3 /opt/BirdNET-Analyzer/analyze.py
    --i      <wav_path>         input audio file
    --o      <output_dir>       directory for results CSV
    --lat    <float>            latitude  (enables seasonal filtering)
    --lon    <float>            longitude (enables range filtering)
    --week   <1-48>             ISO week number (seasonal filter)
    --min_conf <0.0-1.0>        detection threshold
    --rtype  csv                output format (csv is most stable)
    --threads 1                 conserve CPU on Pi/server
    --sensitivity 1.0           default sensitivity

Output: <stem>.BirdNET.results.csv
  Columns: Start (s), End (s), Scientific name, Common name, Confidence
"""

from __future__ import annotations

import subprocess
import csv
import datetime
from pathlib import Path
from typing import Any

# stdlib only — no extra pip packages needed for this runner
from base_analyzer import BaseAnalyzer, Detection
from logger import get_logger

log = get_logger("birdnet")


def _week_number() -> int:
    """Current ISO week clamped to BirdNET's 1–48 range."""
    return max(1, min(48, datetime.date.today().isocalendar()[1]))


class BirdNetAnalyzer(BaseAnalyzer):
    """BirdNET-Analyzer subprocess wrapper."""

    name = "birdnet"

    def is_available(self) -> bool:
        analyzer_path = self.cfg.get("analyzer_path", "/opt/BirdNET-Analyzer")
        script = Path(analyzer_path) / "analyze.py"
        if not script.exists():
            log.warning(
                "BirdNET-Analyzer not found — install via setup.sh",
                extra={"expected": str(script)},
            )
            return False
        return True

    def run(self, wav_path: str, node_cfg: dict[str, Any]) -> list[Detection]:
        analyzer_path = self.cfg.get("analyzer_path", "/opt/BirdNET-Analyzer")
        min_confidence = float(self.cfg.get("min_confidence", 0.70))
        lat = float(node_cfg.get("lat", 37.87))
        lng = float(node_cfg.get("lng", -122.27))

        wav_path = str(Path(wav_path).resolve())
        out_dir = str(Path(wav_path).parent)
        script = str(Path(analyzer_path) / "analyze.py")

        cmd = [
            "python3", script,
            "--i",          wav_path,
            "--o",          out_dir,
            "--lat",        str(lat),
            "--lon",        str(lng),
            "--week",       str(_week_number()),
            "--min_conf",   str(min_confidence),
            "--rtype",      "csv",
            "--threads",    "1",
            "--sensitivity", "1.0",
        ]

        log.debug("Running BirdNET", extra={"node": node_cfg.get("id"), "cmd": " ".join(cmd)})

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120,
            )
        except subprocess.TimeoutExpired:
            log.error("BirdNET timed out", extra={"wav": wav_path})
            return []
        except FileNotFoundError:
            log.error("python3 not in PATH — check runtime environment")
            return []

        if result.returncode != 0:
            log.warning("BirdNET non-zero exit", extra={
                "rc": result.returncode, "stderr": result.stderr[-400:],
            })

        stem = Path(wav_path).stem
        csv_path = Path(out_dir) / f"{stem}.BirdNET.results.csv"
        if not csv_path.exists():
            log.debug("No BirdNET output CSV — likely no detections", extra={"node": node_cfg.get("id")})
            return []

        return _parse_csv(str(csv_path), "birdnet")


def _parse_csv(csv_path: str, analyzer: str) -> list[Detection]:
    """Parse BirdNET results CSV → list[Detection]."""
    detections: list[Detection] = []
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    detections.append(Detection(
                        species=row.get("Scientific name", "").strip(),
                        common_name=row.get("Common name", "").strip(),
                        confidence=float(row.get("Confidence", 0)),
                        start_time=float(row.get("Start (s)", 0)),
                        end_time=float(row.get("End (s)", 0)),
                        analyzer=analyzer,
                    ))
                except (ValueError, KeyError) as exc:
                    log.warning("Skipping malformed BirdNET row", extra={"err": str(exc)})
    except Exception as exc:
        log.error("Failed to parse BirdNET CSV", extra={"path": csv_path, "err": str(exc)})

    log.info("BirdNET complete", extra={"count": len(detections), "csv": csv_path})
    return detections
