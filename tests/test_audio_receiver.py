"""
Tests for the audio-receiver service components.
Run with:  pytest tests/ -v
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import json
import csv
import time
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock


# ── logger ───────────────────────────────────────────────────────────────────

def test_logger_returns_logger():
    from logger import get_logger
    log = get_logger("test")
    assert log is not None
    assert log.name == "test"


def test_logger_same_instance():
    from logger import get_logger
    a = get_logger("same")
    b = get_logger("same")
    assert a is b


# ── audio_store ───────────────────────────────────────────────────────────────

def test_store_add_and_retrieve():
    from audio_store import AudioStore
    s = AudioStore(max_per_node=10, max_global=50)
    meta = {"location_obj": {"name": "Test Garden", "lat": 37.0, "lng": -122.0}}
    dets = [{"species": "Melospiza melodia", "commonName": "Song Sparrow",
              "confidence": 0.91, "startTime": 0.0, "endTime": 3.0, "analyzer": "birdnet"}]
    s.add("node-a", "birdnet", dets, meta)
    results = s.get_node("node-a", limit=10)
    assert len(results) == 1
    assert results[0]["species"] == "Melospiza melodia"
    assert results[0]["nodeId"] == "node-a"


def test_store_max_per_node():
    from audio_store import AudioStore
    s = AudioStore(max_per_node=5, max_global=100)
    meta = {"location_obj": {}}
    for i in range(10):
        s.add("node-x", "birdnet",
              [{"species": f"S{i}", "commonName": f"C{i}", "confidence": 0.9,
                "startTime": 0.0, "endTime": 3.0}],
              meta)
    assert len(s.get_node("node-x", limit=100)) == 5


def test_store_global_aggregates_all_nodes():
    from audio_store import AudioStore
    s = AudioStore()
    meta = {"location_obj": {}}
    det = [{"species": "A", "commonName": "B", "confidence": 0.8, "startTime": 0, "endTime": 3}]
    s.add("node-1", "birdnet", det, meta)
    s.add("node-2", "birdnet", det, meta)
    global_results = s.get_global(limit=10)
    node_ids = {r["nodeId"] for r in global_results}
    assert "node-1" in node_ids
    assert "node-2" in node_ids


def test_store_get_node_ids():
    from audio_store import AudioStore
    s = AudioStore()
    meta = {"location_obj": {}}
    det = [{"species": "A", "commonName": "B", "confidence": 0.8, "startTime": 0, "endTime": 3}]
    s.add("alpha", "birdnet", det, meta)
    s.add("beta", "batnet", det, meta)
    ids = s.get_node_ids()
    assert "alpha" in ids
    assert "beta" in ids


def test_store_thread_safety():
    """Hammer the store from multiple threads — should not raise."""
    from audio_store import AudioStore
    s = AudioStore(max_per_node=50, max_global=200)
    meta = {"location_obj": {}}
    det = [{"species": "X", "commonName": "Y", "confidence": 0.75, "startTime": 0, "endTime": 3}]
    errors = []

    def writer(node_id):
        try:
            for _ in range(20):
                s.add(node_id, "birdnet", det, meta)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(f"n{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors


# ── birdnet_runner CSV parsing ────────────────────────────────────────────────

def test_birdnet_parse_csv(tmp_path):
    """Test CSV parsing logic directly without running BirdNET binary."""
    from birdnet_runner import _parse_csv

    csv_file = tmp_path / "chunk.BirdNET.results.csv"
    rows = [
        {"Start (s)": "0.0", "End (s)": "3.0",
         "Scientific name": "Melospiza melodia", "Common name": "Song Sparrow", "Confidence": "0.91"},
        {"Start (s)": "6.0", "End (s)": "9.0",
         "Scientific name": "Turdus migratorius", "Common name": "American Robin", "Confidence": "0.78"},
    ]
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    results = _parse_csv(str(csv_file))
    assert len(results) == 2
    assert results[0]["commonName"] == "Song Sparrow"
    assert results[0]["confidence"] == 0.91
    assert results[1]["species"] == "Turdus migratorius"
    assert results[0]["analyzer"] == "birdnet"


def test_birdnet_missing_csv(tmp_path):
    from birdnet_runner import _parse_csv
    results = _parse_csv(str(tmp_path / "nonexistent.csv"))
    assert results == []


# ── batnet_runner ─────────────────────────────────────────────────────────────

def test_batnet_skips_low_sample_rate(tmp_path):
    from batnet_runner import run_batnet
    results = run_batnet(
        wav_path=str(tmp_path / "chunk.wav"),
        analyzer_path="/opt/BatNET-Detector",
        node_sample_rate=48000,  # below 192000 threshold
        min_confidence=0.65,
    )
    assert results == []


def test_batnet_parse_json(tmp_path):
    from batnet_runner import _parse_json

    json_file = tmp_path / "chunk.batnet.json"
    data = {
        "detections": [
            {"scientific_name": "Eptesicus fuscus", "common_name": "Big Brown Bat",
             "confidence": 0.84, "start_s": 0.5, "end_s": 0.9},
        ]
    }
    json_file.write_text(json.dumps(data))
    results = _parse_json(str(json_file))
    assert len(results) == 1
    assert results[0]["commonName"] == "Big Brown Bat"
    assert results[0]["analyzer"] == "batnet"
    assert results[0]["confidence"] == 0.84


# ── poster ────────────────────────────────────────────────────────────────────

def test_poster_builds_correct_payload():
    """Mock urllib to verify the POST payload structure."""
    import urllib.request
    captured = {}

    class MockResponse:
        status = 200
        def read(self): return b'{"success": true}'
        def __enter__(self): return self
        def __exit__(self, *a): pass

    def mock_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["body"] = json.loads(req.data.decode())
        return MockResponse()

    from poster import post_detection
    with patch("urllib.request.urlopen", side_effect=mock_urlopen):
        ok = post_detection(
            ingest_url="http://localhost:5050/api/ingest",
            node_id="garden-east",
            analyzer="birdnet",
            detections=[{"species": "Melospiza melodia", "commonName": "Song Sparrow",
                         "confidence": 0.91, "startTime": 0, "endTime": 3}],
            node_meta={"location_obj": {"name": "East Garden", "lat": 37.87, "lng": -122.27}},
        )

    assert ok is True
    assert captured["url"] == "http://localhost:5050/api/ingest/audio-garden-east"
    payload = captured["body"]
    assert payload["data"]["nodeId"] == "garden-east"
    assert payload["data"]["analyzer"] == "birdnet"
    assert len(payload["data"]["detections"]) == 1


def test_poster_retries_on_failure():
    """Verify exponential backoff occurs."""
    import urllib.error
    call_count = [0]

    def mock_urlopen(req, timeout=None):
        call_count[0] += 1
        raise urllib.error.URLError("connection refused")

    from poster import post_detection
    with patch("urllib.request.urlopen", side_effect=mock_urlopen), \
         patch("time.sleep"):  # don't actually sleep in tests
        ok = post_detection(
            ingest_url="http://localhost:5050/api/ingest",
            node_id="test-node",
            analyzer="birdnet",
            detections=[],
            node_meta={},
            retries=3,
        )

    assert ok is False
    assert call_count[0] == 3  # tried all 3 times
