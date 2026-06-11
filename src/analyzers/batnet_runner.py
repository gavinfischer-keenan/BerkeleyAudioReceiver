"""
analyzers/batnet_runner.py — BatNET-Detector implementation of BaseAnalyzer.

System dependency: BatNET-Detector
  Source:  https://github.com/kahst/BatNET-Detector
  Install: /opt/BatNET-Detector (via setup.sh)
  Runtime: python3 (subprocess) — NOT imported as a library
  Models:  bundled inside /opt/BatNET-Detector/

CRITICAL HARDWARE NOTE:
  BatNET analyses echolocation calls in the ultrasonic range (≥20 kHz, peak
  40–120 kHz depending on species). Standard consumer microphones have a
  frequency response of ~20 Hz–20 kHz and CANNOT capture these calls.

  Hardware capable of ultrasonic recording:
    - AudioMoth (Open Acoustic Devices)    — https://www.openacousticdevices.info
    - Wildlife Acoustics SM4BAT            — https://www.wildlifeacoustics.com
    - Ultramic UM200K (USB, up to 200 kHz) — https://www.dodotronic.com
    - Pettersson M500-384 (professional)

  Required sample rate: ≥192,000 Hz (192 kHz)

  Nodes with sample_rate < 192000 in microphones.yaml are SILENTLY SKIPPED
  by is_available(). No error is raised — this is by design.

Binary invoked (actual CLI may differ by BatNET version — verify on install):
  python3 /opt/BatNET-Detector/batnet.py
    --input     <wav_path>
    --output    <output_dir>
    --threshold <0.0-1.0>
    --format    json

Output: <stem>.batnet.json
  Schema: { "detections": [{ "scientific_name", "common_name",
                              "confidence", "start_s", "end_s" }] }
"""

from __future__ import annotations

import subprocess
import json
from pathlib import Path
from typing import Any

from base_analyzer import BaseAnalyzer, Detection
from logger import get_logger

log = get_logger("batnet")

# Minimum sample rate for ultrasonic bat call capture
_MIN_ULTRASONIC_RATE = 192_000


class BatNetAnalyzer(BaseAnalyzer):
    """BatNET-Detector subprocess wrapper."""

    name = "batnet"

    def is_available(self) -> bool:
        """
        Returns False (and silently disables) if:
          - BatNET-Detector binary is not installed at configured path
        Note: per-node sample_rate check happens inside run(), not here,
        so that the analyzer stays registered but skips ineligible nodes.
        """
        analyzer_path = self.cfg.get("analyzer_path", "/opt/BatNET-Detector")
        script = Path(analyzer_path) / "batnet.py"
        if not script.exists():
            log.warning(
                "BatNET-Detector not found — install via setup.sh or manually",
                extra={"expected": str(script)},
            )
            return False
        return True

    def run(self, wav_path: str, node_cfg: dict[str, Any]) -> list[Detection]:
        node_rate = int(node_cfg.get("sample_rate", 48_000))
        required_rate = int(self.cfg.get("required_sample_rate", _MIN_ULTRASONIC_RATE))

        # Silently skip nodes whose mics cannot capture ultrasonic frequencies.
        # Standard 48 kHz mics will always hit this branch — that is correct.
        if node_rate < required_rate:
            log.debug(
                "Skipping BatNET — node sample rate below ultrasonic threshold",
                extra={
                    "node": node_cfg.get("id"),
                    "node_rate_hz": node_rate,
                    "required_hz": required_rate,
                    "tip": "Use AudioMoth/Ultramic UM200K and set sample_rate: 192000",
                },
            )
            return []

        analyzer_path = self.cfg.get("analyzer_path", "/opt/BatNET-Detector")
        min_confidence = float(self.cfg.get("min_confidence", 0.65))

        wav_path = str(Path(wav_path).resolve())
        out_dir = str(Path(wav_path).parent)
        script = str(Path(analyzer_path) / "batnet.py")

        cmd = [
            "python3", script,
            "--input",     wav_path,
            "--output",    out_dir,
            "--threshold", str(min_confidence),
            "--format",    "json",
        ]

        log.debug("Running BatNET", extra={"node": node_cfg.get("id")})

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120,
            )
        except subprocess.TimeoutExpired:
            log.error("BatNET timed out", extra={"wav": wav_path})
            return []
        except FileNotFoundError:
            log.error("python3 not in PATH")
            return []

        if result.returncode != 0:
            log.warning("BatNET non-zero exit", extra={
                "rc": result.returncode, "stderr": result.stderr[-400:],
            })

        stem = Path(wav_path).stem
        json_path = Path(out_dir) / f"{stem}.batnet.json"
        if not json_path.exists():
            log.debug("No BatNET output JSON — likely no detections", extra={"node": node_cfg.get("id")})
            return []

        return _parse_json(str(json_path))


def _parse_json(json_path: str) -> list[Detection]:
    """Parse BatNET results JSON → list[Detection]."""
    detections: list[Detection] = []
    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        for det in data.get("detections", []):
            try:
                detections.append(Detection(
                    species=det.get("scientific_name", "").strip(),
                    common_name=det.get("common_name", "Unknown Bat").strip(),
                    confidence=float(det.get("confidence", 0)),
                    start_time=float(det.get("start_s", 0)),
                    end_time=float(det.get("end_s", 0)),
                    analyzer="batnet",
                ))
            except (ValueError, KeyError) as exc:
                log.warning("Skipping malformed BatNET detection", extra={"err": str(exc)})
    except Exception as exc:
        log.error("Failed to parse BatNET JSON", extra={"path": json_path, "err": str(exc)})

    log.info("BatNET complete", extra={"count": len(detections), "json": json_path})
    return detections
