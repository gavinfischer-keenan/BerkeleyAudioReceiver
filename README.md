# Berkeley Audio Receiver

Headless Python service that connects to remote RTSP microphone streams,
runs BirdNET-Analyzer and BatNET-Detector on captured audio, archives
interesting clips to disk, and publishes detections to both the Berkeley
Dashboard (HTTP) and the home intelligence MQTT bus.

**Standalone repo** — extracted from BerkeleyHouse with MQTT integration added.

## Architecture

```
  Microphone Node (Pi Zero W / ESP32 LyraT)
      │  RTSP stream (TCP, local LAN)
      ▼
  BerkeleyAudioReceiver/src/main.py
      │
      ├── RtspNode threads (one per enabled node)
      │     └── ffmpeg → WAV chunks (15s)
      │
      └── AudioPipeline (thread pool)
            ├── BirdNetAnalyzer  → /opt/BirdNET-Analyzer/analyze.py
            ├── BatNetAnalyzer   → /opt/BatNET-Detector/batnet.py
            │
            ├── AudioArchiver    → ./data/audio/<node>/<date>/<clip>.wav
            │
            ├── HTTP POST        → localhost:5050/api/ingest/audio-<nodeId>
            │                       (Berkeley Dashboard, Socket.IO)
            │
            └── MQTT Publish     → home/events/bird-audio  (NEW)
                                 → home/events/bat-audio   (NEW)
                                 → home/status/audio-receiver (retained)
```

## MQTT Topics

| Topic | QoS | Retained | Description |
|-------|-----|----------|-------------|
| `home/events/bird-audio` | 1 | No | BirdNET detections |
| `home/events/bat-audio` | 1 | No | BatNET detections |
| `home/status/audio-receiver` | 1 | Yes | Service online/offline |
| `home/status/audio-receiver/{nodeId}` | 0 | Yes | Per-node online/offline |
| `home/sensors/audio-levels/{nodeId}` | 0 | No | Audio dB levels |

## Quick Start

```bash
# 1. Clone
git clone https://github.com/gavinfischer-keenan/BerkeleyAudioReceiver.git
cd BerkeleyAudioReceiver

# 2. Install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
nano .env                        # Set MQTT_ENABLED=true if Mosquitto is running
nano config/microphones.yaml     # Set RTSP URLs for your mics

# 4. Run
python3 src/main.py
```

## Configuration

All settings are in `config/microphones.yaml`. See file for detailed comments.

### MQTT (optional)

Set these in `.env` to enable MQTT publishing alongside the HTTP POST path:

```env
MQTT_ENABLED=true
MQTT_BROKER=localhost
MQTT_PORT=1883
```

When MQTT is disabled, the service operates identically to the original
embedded version — HTTP POST only.

## Adding a new analyzer

1. Create `src/analyzers/<name>_runner.py` implementing `BaseAnalyzer`
2. Add it to `_REGISTRY` in `src/analyzers/__init__.py`
3. Add a config block in `config/microphones.yaml`
4. Add the key to the `analyzers:` list on relevant nodes

No changes to `main.py`, `rtsp_node.py`, or `audio_pipeline.py` needed.
