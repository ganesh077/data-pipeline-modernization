from __future__ import annotations

import logging
from typing import List

from .config import PipelineConfig
from .extract import CSVExtractor
from .load import OrderLoader
from .models import OrderRecord
from .transform import OrderTransformer


class ModernizedPipeline:
    """End-to-end orchestration for the upgraded pipeline."""

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._extractor = CSVExtractor(config.paths.raw_orders)
        self._transformer = OrderTransformer()
        self._loader = OrderLoader(config.db.dsn, config.db.echo_sql)
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> int:
        buffer: List[OrderRecord] = []
        processed = 0
        for raw_row in self._extractor.extract():
            buffer.append(self._transformer.normalize(raw_row))
            if len(buffer) >= self._config.batch_size:
                processed += self._flush(buffer)
        if buffer:
            processed += self._flush(buffer)
        self._logger.info("Pipeline run complete: %s rows", processed)
        return processed

    def _flush(self, buffer: List[OrderRecord]) -> int:
        flushed = self._loader.load(buffer)
        buffer.clear()
        return flushed
