"""
base_analyzer.py — Abstract interface for all audio analyzers.

Adding a new analyzer (e.g. SoundscapeEcology, YAMNet, Merlin) requires:
  1. Create src/analyzers/<name>_runner.py
  2. Subclass BaseAnalyzer and implement run()
  3. Register a key under your new analyzer in config/microphones.yaml
  4. The dispatcher in audio_pipeline.py picks it up automatically.

No changes to main.py, rtsp_node.py, or audio_pipeline.py needed.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any


class Detection:
    """
    Normalized detection record produced by any analyzer.
    All fields are required so downstream consumers (poster, archiver, store)
    have a consistent schema regardless of which analyzer produced the result.
    """
    __slots__ = (
        "species", "common_name", "confidence",
        "start_time", "end_time", "analyzer",
    )

    def __init__(
        self,
        species: str,
        common_name: str,
        confidence: float,
        start_time: float,
        end_time: float,
        analyzer: str,
    ) -> None:
        self.species = species
        self.common_name = common_name
        self.confidence = round(float(confidence), 4)
        self.start_time = float(start_time)
        self.end_time = float(end_time)
        self.analyzer = analyzer

    def to_dict(self) -> dict[str, Any]:
        return {
            "species":    self.species,
            "commonName": self.common_name,
            "confidence": self.confidence,
            "startTime":  self.start_time,
            "endTime":    self.end_time,
            "analyzer":   self.analyzer,
        }

    def __repr__(self) -> str:
        return (
            f"Detection(analyzer={self.analyzer!r}, species={self.common_name!r}, "
            f"confidence={self.confidence:.2f}, t={self.start_time:.1f}–{self.end_time:.1f}s)"
        )


class BaseAnalyzer(ABC):
    """
    Abstract base for all audio analyzers.

    Subclasses must implement run() and can override is_available()
    to perform startup checks (binary present, model files loaded, etc.).
    """

    #: Unique machine key — must match the key in microphones.yaml analyzers list
    #: and the config section name (e.g. "birdnet", "batnet").
    name: str = ""

    def __init__(self, analyzer_config: dict[str, Any]) -> None:
        """
        Args:
            analyzer_config: the config block for this analyzer from microphones.yaml
                             e.g. {"analyzer_path": "/opt/BirdNET-Analyzer", "min_confidence": 0.70}
        """
        self.cfg = analyzer_config

    @abstractmethod
    def run(
        self,
        wav_path: str,
        node_cfg: dict[str, Any],
    ) -> list[Detection]:
        """
        Analyse a WAV chunk and return detected events.

        Args:
            wav_path:  Absolute path to the WAV file for this chunk.
            node_cfg:  The full node config dict (lat, lng, sample_rate, etc.)

        Returns:
            List of Detection objects. Return [] if nothing detected or
            if the analyzer is not applicable (e.g. sample rate too low).
        """
        ...

    def is_available(self) -> bool:
        """
        Optional startup check. Return False to silently disable this analyzer.
        Override in subclasses to test binary/model availability.
        """
        return True
