from __future__ import annotations

import argparse
import logging
from pathlib import Path

from data_pipeline.config import PipelineConfig
from data_pipeline.pipeline import ModernizedPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the modernized data pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/settings.yaml"),
        help="Path to the pipeline configuration file",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    config = PipelineConfig.from_yaml(args.config)
    pipeline = ModernizedPipeline(config)
    processed = pipeline.run()
    logging.info("Successfully processed %s rows", processed)


if __name__ == "__main__":
    main()
