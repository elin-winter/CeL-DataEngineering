import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError

from helpers import setup_logger

logger = setup_logger("lakehouse_utils")


def table_exists(path: str, storage_options: dict) -> bool:
    try:
        DeltaTable(path, storage_options=storage_options)
        return True
    except TableNotFoundError:
        return False

def create_table_if_not_exists(
    path: str,
    df: pd.DataFrame,
    storage_options: dict,
    partition_cols: list[str] | None = None,
):
    if table_exists(path, storage_options):
        return

    logger.info(f"Creating table with config: {path}")

    schema = pa.Schema.from_pandas(df)

    DeltaTable.create(
        path,
        schema=schema,
        partition_by=partition_cols,
        configuration={
            "delta.deletedFileRetentionDuration": "interval 7 day",
            "delta.logRetentionDuration": "interval 7 day",
        },
        storage_options=storage_options,
    )

def read_delta(path: str, storage_options: dict) -> pd.DataFrame | None:
    if not table_exists(path, storage_options):
        logger.warning(f"Table not found: {path}")
        return None
    return DeltaTable(path, storage_options=storage_options).to_pandas()


def overwrite_delta(
    df: pd.DataFrame,
    path: str,
    storage_options: dict,
    partition_cols: list[str] | None = None,
) -> None:
    logger.info(f"Overwriting table: {path} ({len(df)} rows)")
    write_deltalake(
        path, df,
        mode="overwrite",
        storage_options=storage_options,
        partition_by=partition_cols,
        schema_mode="overwrite",
    )


def delete_insert_delta(
    df: pd.DataFrame,
    path: str,
    storage_options: dict,
    partition_predicate: str,
    partition_cols: list[str] | None = None,
) -> None:
    if table_exists(path, storage_options):
        dt = DeltaTable(path, storage_options=storage_options)
        logger.debug(f"Deleting partition: {partition_predicate}")
        dt.delete(predicate=partition_predicate)
        write_deltalake(
            path, df,
            mode="append",
            storage_options=storage_options,
            partition_by=partition_cols,
            schema_mode="merge",
        )
        logger.info(f"Inserted {len(df)} rows into {path}")
    else:
        logger.info(f"Table does not exist, creating: {path}")
        create_table_if_not_exists(path, df, storage_options, partition_cols)

        write_deltalake(
            path, df,
            mode="overwrite",
            storage_options=storage_options,
            partition_by=partition_cols,
            schema_mode="overwrite",
        )


def append_delta(
    df: pd.DataFrame,
    path: str,
    storage_options: dict,
    partition_cols: list[str] | None = None,
) -> None:

    if not table_exists(path, storage_options):
        create_table_if_not_exists(path, df, storage_options, partition_cols)

    write_deltalake(
        path,
        pa.Table.from_pandas(df),
        mode="append",
        storage_options=storage_options,
        partition_by=partition_cols,
        schema_mode="merge",
    )

    logger.info(f"Appended {len(df)} rows to {path}")


def log_table_info(path: str, storage_options: dict) -> None:
    if not table_exists(path, storage_options):
        logger.info(f"Table not yet created: {path}")
        return
    try:
        dt      = DeltaTable(path, storage_options=storage_options)
        meta    = dt.metadata()
        history = dt.history(limit=1)
        rows    = len(dt.to_pandas())
        last_op = history[0].get("operation", "?") if history else "?"
        logger.info(
            f"Table: {path} | version={dt.version()} rows={rows} "
            f"partitions={meta.partition_columns} last_op={last_op}"
        )
    except Exception as e:
        logger.error(f"Could not read table info for {path}: {e}")