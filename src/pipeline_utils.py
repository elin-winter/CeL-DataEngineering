from helpers import (
    setup_logger,
    build_delta_path,
)
from api_extract import (
    extract_events,
    extract_catalogs,
    extract_contributors,
)
from lakehouse_utils import (
    overwrite_delta,
    delete_insert_delta,
    log_table_info,
)

logger = setup_logger("pipeline_utils")


def run_static_pipeline(cfg, storage_options: dict) -> dict:
    results = {}

    df_catalogs = extract_catalogs(cfg)
    if df_catalogs is not None:
        path = build_delta_path(cfg, cfg["datalake"]["catalogs_dir"])
        overwrite_delta(df_catalogs, path, storage_options)
        log_table_info(path, storage_options)
        results["catalogs"] = True
    else:
        logger.error("Failed to extract catalogs.")
        results["catalogs"] = False

    df_contributors = extract_contributors(cfg)
    if df_contributors is not None:
        path = build_delta_path(cfg, cfg["datalake"]["contributors_dir"])
        overwrite_delta(df_contributors, path, storage_options)
        log_table_info(path, storage_options)
        results["contributors"] = True
    else:
        logger.error("Failed to extract contributors.")
        results["contributors"] = False

    return results


def run_incremental_pipeline(cfg, storage_options: dict) -> dict:
    df_events = extract_events(cfg)
    if df_events is None:
        logger.error("Failed to extract seismic events.")
        return {"events": False}

    path = build_delta_path(cfg, cfg["datalake"]["events_dir"])

    partition_combos = (
        df_events[["partition_year", "partition_month", "partition_day", "partition_hour"]]
        .drop_duplicates()
    )

    if len(partition_combos) == 1:
        row = partition_combos.iloc[0]
        predicate = (
            f"partition_year='{row['partition_year']}' AND "
            f"partition_month='{row['partition_month']}' AND "
            f"partition_day='{row['partition_day']}' AND "
            f"partition_hour='{row['partition_hour']}'"
        )
    else:
        clauses = [
            f"(partition_year='{r['partition_year']}' AND "
            f"partition_month='{r['partition_month']}' AND "
            f"partition_day='{r['partition_day']}' AND "
            f"partition_hour='{r['partition_hour']}')"
            for _, r in partition_combos.iterrows()
        ]
        predicate = " OR ".join(clauses)

    delete_insert_delta(
        df=df_events,
        path=path,
        storage_options=storage_options,
        partition_predicate=predicate,
        partition_cols=["partition_year", "partition_month", "partition_day", "partition_hour"],
    )

    log_table_info(path, storage_options)
    return {"events": True}