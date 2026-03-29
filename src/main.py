import sys
from datetime import datetime, timezone

from helpers import (
    setup_logger,
    load_config,
    build_storage_options,
    setup_all_directories,
)

from pipeline_utils import (
    run_static_pipeline,
    run_incremental_pipeline,
)

logger = setup_logger("main")


def main():
    start_time = datetime.now(timezone.utc)
    logger.info("Pipeline started.")

    try:
        cfg = load_config("../pipeline.conf")
    except FileNotFoundError as e:
        logger.critical(f"Config file not found: {e}")
        sys.exit(1)

    storage_options = build_storage_options(cfg)

    try:
        setup_all_directories(cfg)
    except Exception as e:
        logger.error(f"Could not set up MinIO directories: {e}")

    static_results      = run_static_pipeline(cfg, storage_options)
    incremental_results = run_incremental_pipeline(cfg, storage_options)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    all_ok  = all([
        static_results.get("catalogs"),
        static_results.get("contributors"),
        incremental_results.get("events"),
    ])
    failed = [k for k, v in {**static_results, **incremental_results}.items() if not v]

    logger.info(f"Pipeline finished in {elapsed:.1f}s. failed={failed if failed else 'none'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()