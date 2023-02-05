from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class DatabaseConfig:
    """Connection information for the relational store."""

    dsn: str
    echo_sql: bool = False


@dataclass
class PipelinePaths:
    """Filesystem locations for pipeline assets."""

    raw_orders: Path


@dataclass
class PipelineConfig:
    """Top-level configuration container."""

    db: DatabaseConfig
    paths: PipelinePaths
    batch_size: int = 500

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        data: Dict[str, Any] = yaml.safe_load(path.read_text())
        db = DatabaseConfig(**data["db"])
        paths_data = data["paths"]
        paths = PipelinePaths(raw_orders=Path(paths_data["raw_orders"]))
        return cls(db=db, paths=paths, batch_size=data.get("batch_size", 500))
