"""Shared, non-networked evaluation output helpers."""
import hashlib
import importlib.metadata
import json
import os
import platform
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_sha256(path):
    """Source fingerprints survive Git's Windows/Linux line-ending conversion.

    Raw evidence artifacts still use sha256 so their exact bytes remain checked.
    """
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def provenance(source_files, started, elapsed):
    versions = {}
    for package in ("numpy", "scikit-learn", "joblib", "scipy", "threadpoolctl", "motor", "pymongo", "aiomqtt", "paho-mqtt", "dnspython"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            pass
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "startedAt": started,
        "wallClockExecutionSeconds": round(elapsed, 4),
        "hardware": {
            "os": platform.platform(),
            "architecture": platform.machine(),
            "processor": os.environ.get("PROCESSOR_IDENTIFIER") or platform.processor() or "unavailable",
            "logicalCpuCount": os.cpu_count(),
        },
        "python": platform.python_version(),
        "packages": versions,
        "sourceHashNormalization": "CRLF converted to LF; evidence artifact hashes use raw bytes",
        "sourceSha256": {name: source_sha256(ROOT / name) for name in source_files},
    }
