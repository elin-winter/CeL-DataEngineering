import json
import logging
import configparser
import os
from datetime import datetime, timezone

import boto3
from botocore.client import Config

METADATA_PATH = "../metadata/metadata_ingestion.json"


def setup_logger(name: str = "pipeline", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(filename)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = setup_logger("helpers")



def load_config(config_path: str) -> configparser.ConfigParser:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    cfg = configparser.ConfigParser()
    cfg.read(config_path)
    return cfg


def build_storage_options(cfg: configparser.ConfigParser) -> dict:
    m = cfg["minio"]
    return {
        "AWS_ENDPOINT_URL":           m["endpoint_url"],
        "AWS_ACCESS_KEY_ID":          m["access_key_id"],
        "AWS_SECRET_ACCESS_KEY":      m["secret_access_key"],
        "AWS_ALLOW_HTTP":             m["allow_http"],
        "aws_conditional_put":        "etag",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def get_partition_values(dt: datetime | None = None) -> dict:
    if dt is None:
        dt = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "partition_year":  str(dt.year),
        "partition_month": f"{dt.month:02d}",
        "partition_day":   f"{dt.day:02d}",
        "partition_hour":  f"{dt.hour:02d}",
    }


def build_delta_path(cfg: configparser.ConfigParser, subdir: str) -> str:
    return (
        f"s3://{cfg['minio']['bucket_name']}"
        f"/{cfg['datalake']['bronze_layer']}"
        f"/{cfg['datalake']['api_name']}"
        f"/{subdir}"
    )


# ─────────────────────────────────────────────
# Ingestion metadata
# ─────────────────────────────────────────────

def read_ingestion_metadata(path: str = METADATA_PATH) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def update_ingestion_metadata(entity: str, last_value: int | float, path: str = METADATA_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    metadata = read_ingestion_metadata(path)
    if entity not in metadata:
        metadata[entity] = {}
    metadata[entity]["last_value"] = last_value
    with open(path, "w") as f:
        json.dump(metadata, f, indent=2)


# ─────────────────────────────────────────────
# MinIO directory management
# ─────────────────────────────────────────────

def _get_s3_client(cfg: configparser.ConfigParser):
    m = cfg["minio"]
    return boto3.client(
        "s3",
        endpoint_url=m["endpoint_url"],
        aws_access_key_id=m["access_key_id"],
        aws_secret_access_key=m["secret_access_key"],
        config=Config(signature_version="s3v4"),
        use_ssl=False,
        verify=False,
    )


def ensure_bucket_exists(cfg: configparser.ConfigParser) -> None:
    bucket   = cfg["minio"]["bucket_name"]
    client   = _get_s3_client(cfg)
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        logger.info(f"Bucket created: {bucket}")


def ensure_prefix_exists(
    cfg: configparser.ConfigParser,
    prefix: str,
) -> None:
    bucket = cfg["minio"]["bucket_name"]
    client = _get_s3_client(cfg)
    if not prefix.endswith("/"):
        prefix += "/"
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        client.put_object(Bucket=bucket, Key=prefix, Body=b"")
        logger.info(f"Directory created: s3://{bucket}/{prefix}")


def setup_all_directories(cfg: configparser.ConfigParser) -> None:
    ensure_bucket_exists(cfg)
    bronze = cfg["datalake"]["bronze_layer"]
    api    = cfg["datalake"]["api_name"]
    for subdir in [
        cfg["datalake"]["events_dir"],
        cfg["datalake"]["catalogs_dir"],
        cfg["datalake"]["contributors_dir"],
    ]:
        ensure_prefix_exists(cfg, f"{bronze}/{api}/{subdir}")


def clear_bucket(cfg):
    endpoint_url = cfg["minio"]["endpoint_url"]
    access_key   = cfg["minio"]["access_key_id"]
    secret_key   = cfg["minio"]["secret_access_key"]
    bucket_name  = cfg["minio"]["bucket_name"]

    s3 = boto3.resource(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    bucket = s3.Bucket(bucket_name)

    try:
        bucket.objects.all().delete()
    except Exception as e:
        logger.error(f"❌ Error cleaning bucket: {e}")
        raise