import hashlib
import json
import os
from datetime import datetime
from typing import Any, Dict


def today_str() -> str:
    """
    Return today's date as YYYYMMDD.
    """
    return datetime.utcnow().strftime("%Y%m%d")


def version_tag(n: int) -> str:
    """
    Return a zero-padded version tag in the form vNN.
    """
    if n < 0:
        raise ValueError("Version number must be non-negative")
    return f"v{n:02d}"


def render_filename(env: str, ritm: str, version: int, date_str: str | None = None) -> str:
    """
    Render a filename following the pattern:
    BPDH_BPCreate_{ENV}_{YYYYMMDD}_{RITM}_{vNN}.csv
    """
    if not date_str:
        date_str = today_str()
    env_clean = env.strip().upper()
    ritm_clean = ritm.strip().upper().replace(" ", "")
    vtag = version_tag(version)
    return f"BPDH_BPCreate_{env_clean}_{date_str}_{ritm_clean}_{vtag}.csv"


def sha256_of_file(path: str) -> str:
    """
    Compute the SHA-256 hash of a file on disk.
    """
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def sha256_of_bytes(data: bytes) -> str:
    """
    Compute the SHA-256 hash of a bytes object.
    Useful for in-memory CSVs generated for download.
    """
    return hashlib.sha256(data).hexdigest()


def write_manifest(path: str, manifest: Dict[str, Any]) -> None:
    """
    Write a JSON manifest to disk with pretty formatting.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True, ensure_ascii=False)
