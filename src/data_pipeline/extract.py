from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable


class CSVExtractor:
    """Streams records from the legacy CSV export."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def extract(self) -> Iterable[Dict[str, str]]:
        with self._path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield row
