# pystele/core/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    storage_path: Path
    default_backend: str
    version: str

    @classmethod
    def load(cls) -> "Config":
        storage = os.environ.get("PYSTELE_STORAGE_PATH", ".pystele")
        backend = os.environ.get("PYSTELE_DEFAULT_BACKEND", "local")
        version = os.environ.get("PYSTELE_VERSION", "0.0.1")
        return cls(Path(storage), backend, version)

    def validate(self) -> None:
        if not self.default_backend:
            raise ValueError("default_backend must be set")
        if not self.version:
            raise ValueError("version must be set")
        if not isinstance(self.storage_path, Path):
            raise ValueError("storage_path must be a Path")
