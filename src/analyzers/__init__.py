"""
analyzers/__init__.py — Analyzer registry / dispatcher.

Registers all known analyzers by name. The pipeline calls get_analyzers()
with the node's analyzer list and the full config, receiving back only the
instantiated analyzers that are actually available on this host.

To add a new analyzer:
  1. Create analyzers/<name>_runner.py with a class subclassing BaseAnalyzer
  2. Add it to _REGISTRY below
  3. Add a config block under the same key in microphones.yaml
  That's it — no other files need to change.
"""

from __future__ import annotations
from typing import Any

from base_analyzer import BaseAnalyzer
from analyzers.birdnet_runner import BirdNetAnalyzer
from analyzers.batnet_runner import BatNetAnalyzer
from logger import get_logger

log = get_logger("analyzers")

# ── Analyzer registry ─────────────────────────────────────────────────────────
# Maps the config key (as used in microphones.yaml `analyzers:` lists and
# top-level config blocks) → analyzer class.
#
# Add future analyzers here:
#   "yamnet":      YamNetAnalyzer,       # Google AudioSet classifier
#   "soundscape":  SoundscapeAnalyzer,   # Soundscape Ecology metrics
#   "merlin":      MerlinAnalyzer,       # Cornell Merlin (if CLI becomes available)
# ─────────────────────────────────────────────────────────────────────────────
_REGISTRY: dict[str, type[BaseAnalyzer]] = {
    "birdnet": BirdNetAnalyzer,
    "batnet":  BatNetAnalyzer,
}


def get_analyzers(
    requested: list[str],
    full_config: dict[str, Any],
) -> list[BaseAnalyzer]:
    """
    Instantiate and return analyzers that are both requested and available.

    Args:
        requested:    list of analyzer names from the node config
                      e.g. ["birdnet", "batnet"]
        full_config:  the full parsed microphones.yaml dict — each analyzer
                      reads its own config block by name key

    Returns:
        List of ready BaseAnalyzer instances, in the order requested.
        Analyzers not in the registry, or failing is_available(), are skipped
        with a warning — they do NOT raise.
    """
    result: list[BaseAnalyzer] = []
    for name in requested:
        cls = _REGISTRY.get(name)
        if cls is None:
            log.warning(
                "Unknown analyzer — add to _REGISTRY in analyzers/__init__.py",
                extra={"name": name},
            )
            continue

        analyzer_cfg = full_config.get(name, {})
        instance = cls(analyzer_cfg)

        if not instance.is_available():
            log.warning(
                "Analyzer not available on this host — skipping",
                extra={"name": name},
            )
            continue

        result.append(instance)
        log.info("Analyzer ready", extra={"name": name})

    return result
