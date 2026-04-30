from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from py_backend.config import config


def _path(collection: str, key: str) -> Path:
    return config.data_dir / f"{collection}-{key}.json"


def write_json(collection: str, key: str, value: Any) -> None:
    _path(collection, key).write_text(json.dumps(value, indent=2), encoding="utf-8")


def read_json(collection: str, key: str, fallback: Any = None) -> Any:
    target = _path(collection, key)
    if not target.exists():
        return fallback
    return json.loads(target.read_text(encoding="utf-8"))


def list_store() -> list[str]:
    return sorted(path.name for path in config.data_dir.glob("*.json"))
