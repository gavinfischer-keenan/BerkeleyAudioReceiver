"""
audio_archiver.py — "Interesting" clip archiver with configurable rules.

What gets saved to disk is controlled entirely by the `archive.rules` section
of config/microphones.yaml. The definition of "interesting" can be changed at
any time by editing that YAML — no code changes needed.

Current built-in rule types:
  min_confidence    — any detection above this threshold
  analyzers         — restrict rule to specific analyzer(s) (empty = all)
  species_contains  — substring match on scientific OR common name

Planned rule types (add config key + handler in _eval_rule()):
  time_start/time_end   — only archive during specific hours
  node_ids              — only archive from specific microphone locations
  min_duration_secs     — only clips longer than N seconds

Storage layout:
  <base_dir>/<node_id>/<YYYY-MM-DD>/<node_id>_<epoch_ms>.wav

Clips are NEVER deleted by this service — retention is managed by setup.sh
cron or a future maintenance job. archive.retention_days in config is
informational for now (cron script reads it separately).
"""

from __future__ import annotations

import os
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

from base_analyzer import Detection
from logger import get_logger

log = get_logger("archiver")


class AudioArchiver:
    """
    Evaluates each batch of detections against configured rules and,
    when a rule matches, copies the source WAV chunk to the archive tree.
    """

    def __init__(self, archive_config: dict[str, Any]) -> None:
        self.enabled: bool = archive_config.get("enabled", True)
        self.base_dir: Path = Path(archive_config.get("base_dir", "./data/audio"))
        self.fmt: str = archive_config.get("format", "wav")
        self.rules: list[dict[str, Any]] = [
            r for r in archive_config.get("rules", [])
            if r.get("enabled", True)
        ]
        if self.enabled:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            log.info("Archiver ready", extra={
                "base_dir": str(self.base_dir),
                "active_rules": [r.get("name") for r in self.rules],
            })

    def evaluate(
        self,
        node_id: str,
        wav_path: str,
        detections: list[Detection],
        analyzer_name: str,
    ) -> str | None:
        """
        Test detections against all active rules. If any rule matches,
        copy the WAV chunk to the archive and return the archive path.
        Returns None if the clip is not archived.
        """
        if not self.enabled or not self.rules:
            return None

        for rule in self.rules:
            if self._eval_rule(rule, detections, analyzer_name):
                archive_path = self._archive(node_id, wav_path, rule.get("name", "unknown"))
                if archive_path:
                    log.info("Interesting clip archived", extra={
                        "node": node_id,
                        "rule": rule.get("name"),
                        "analyzer": analyzer_name,
                        "detections": len(detections),
                        "path": archive_path,
                    })
                return archive_path

        return None

    # ── Rule evaluation ───────────────────────────────────────────────────────

    def _eval_rule(
        self,
        rule: dict[str, Any],
        detections: list[Detection],
        analyzer_name: str,
    ) -> bool:
        """Return True if ANY detection in the batch matches this rule."""
        if not detections:
            return False

        # Restrict to specific analyzers if specified
        rule_analyzers: list[str] = rule.get("analyzers", [])

        for det in detections:
            # Analyzer filter (empty list = all analyzers pass)
            if rule_analyzers and det.analyzer not in rule_analyzers:
                continue

            # Confidence threshold
            min_conf = float(rule.get("min_confidence", 0.0))
            if det.confidence < min_conf:
                continue

            # Species substring match (OR across list)
            species_terms: list[str] = rule.get("species_contains", [])
            if species_terms:
                haystack = f"{det.species} {det.common_name}".lower()
                if not any(term.lower() in haystack for term in species_terms):
                    continue

            # ── Future rule types: add elif branches here ─────────────────
            # Time window:
            #   time_start = rule.get("time_start")  # "05:30"
            #   time_end   = rule.get("time_end")     # "07:30"
            #   if time_start and time_end:
            #       now_t = datetime.now().strftime("%H:%M")
            #       if not (time_start <= now_t <= time_end):
            #           continue
            # ──────────────────────────────────────────────────────────────

            # All filters passed — this detection is interesting
            return True

        return False

    # ── File operations ───────────────────────────────────────────────────────

    def _archive(self, node_id: str, wav_path: str, rule_name: str) -> str | None:
        """Copy wav_path into the archive tree. Returns destination path or None."""
        try:
            today = date.today().isoformat()
            epoch_ms = int(datetime.now().timestamp() * 1000)
            dest_dir = self.base_dir / node_id / today
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Filename: <node_id>_<epoch_ms>_<rule>.wav
            safe_rule = rule_name.replace(" ", "_")[:32]
            dest_name = f"{node_id}_{epoch_ms}_{safe_rule}.{self.fmt}"
            dest_path = dest_dir / dest_name

            shutil.copy2(wav_path, dest_path)
            return str(dest_path)

        except Exception as exc:
            log.error("Archive copy failed", extra={
                "node": node_id, "src": wav_path, "err": str(exc),
            })
            return None
