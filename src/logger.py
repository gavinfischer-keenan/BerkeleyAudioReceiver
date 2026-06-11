"""
Structured logger for audio-receiver.
Uses Python's standard logging module with JSON-friendly formatting
so output integrates cleanly with PM2 and log aggregators.
"""

import logging
import sys
import json
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Emit log records as JSON lines for structured log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exc"] = self.formatException(record.exc_info)
        # Any extra fields passed via `extra=` dict
        for key, val in record.__dict__.items():
            if key not in (
                "msg", "args", "levelname", "levelno", "pathname", "filename",
                "module", "exc_info", "exc_text", "stack_info", "lineno",
                "funcName", "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "name", "message",
            ):
                log_obj[key] = val
        return json.dumps(log_obj)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with JSON output to stdout."""
    log = logging.getLogger(name)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
        log.propagate = False
    return log
