import configparser
from datetime import datetime, timezone

import requests
import pandas as pd

from helpers import get_partition_values, read_ingestion_metadata, update_ingestion_metadata, setup_logger

logger = setup_logger("api_extract")


import xml.etree.ElementTree as ET

def _get(base_url: str, endpoint: str, params: dict | None = None):
    url = f"{base_url}/{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")

        if "application/json" in content_type:
            return resp.json()

        if "xml" in content_type or resp.text.strip().startswith("<"):
            return ET.fromstring(resp.text)

        logger.error(f"Unknown response format from {url}")
        return None

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error {url}: {e}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error {url}: {e}")
    except requests.exceptions.Timeout:
        logger.error(f"Timeout {url}")
    except Exception as e:
        logger.error(f"Unexpected error {url}: {e}")

    return None

def extract_events(cfg: configparser.ConfigParser) -> pd.DataFrame | None:
    metadata   = read_ingestion_metadata()
    last_value = metadata.get("earthquakes", {}).get("last_value", 0)
    now        = datetime.now(timezone.utc)
    endtime    = now.strftime("%Y-%m-%dT%H:%M:%S")

    if last_value == 0:
        from datetime import timedelta
        lookback_h = int(cfg["events_params"]["lookback_hours"])
        starttime  = (now.replace(tzinfo=None) - timedelta(hours=lookback_h)).strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(f"First run, using lookback of {lookback_h}h: {starttime} to {endtime}")
    else:
        starttime = datetime.utcfromtimestamp(last_value / 1000).strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(f"Incremental run from last_value={last_value}: {starttime} to {endtime}")

    params = {
        "format":       cfg["events_params"]["format"],
        "starttime":    starttime,
        "endtime":      endtime,
        "minmagnitude": cfg["events_params"]["min_magnitude"],
        "orderby":      cfg["events_params"]["orderby"],
        "limit":        cfg["events_params"]["limit"],
    }

    raw = _get(cfg["api"]["base_url"], cfg["api"]["events_endpoint"], params)
    if raw is None:
        return None

    features = raw.get("features", [])
    if not features:
        logger.warning(f"No events returned for window {starttime} to {endtime}.")
        return None

    records = [
        {
            "id":       feat.get("id"),
            "geometry": str(feat.get("geometry")),
            **feat.get("properties", {}),
        }
        for feat in features
    ]

    df = pd.DataFrame(records)

    if "time" in df.columns:
        dt_col = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["partition_year"]  = dt_col.dt.year.astype(str)
        df["partition_month"] = dt_col.dt.month.map(lambda x: f"{x:02d}")
        df["partition_day"]   = dt_col.dt.day.map(lambda x: f"{x:02d}")
        df["partition_hour"]  = dt_col.dt.hour.map(lambda x: f"{x:02d}")

        new_last_value = int(df["time"].max())
        update_ingestion_metadata("earthquakes", new_last_value)
        logger.info(f"Metadata updated: last_value={new_last_value}")
    else:
        logger.warning("Field 'time' not found; using extraction timestamp for partitions.")
        for k, v in get_partition_values().items():
            df[k] = v

    df["extracted_at"] = now.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"Events extracted: {len(df)} rows.")

    null_cols = df.columns[df.isna().all()].tolist()
    if null_cols:
        logger.info(f"Dropping completely null columns: {null_cols}")
        df = df.drop(columns=null_cols)

    logger.info(f"Events extracted: {len(df)} rows.")
    return df


def extract_catalogs(cfg: configparser.ConfigParser) -> pd.DataFrame | None:
    raw = _get(cfg["api"]["base_url"], cfg["api"]["catalogs_endpoint"])
    if raw is None:
        return None

    catalogs = [elem.text for elem in raw.findall(".//Catalog")]

    if not catalogs:
        logger.warning("Empty response from /catalogs.")
        return None

    df = pd.DataFrame({"catalog_name": catalogs})

    logger.info(f"Catalogs extracted: {len(df)} rows.")
    return df


def extract_contributors(cfg: configparser.ConfigParser) -> pd.DataFrame | None:
    raw = _get(cfg["api"]["base_url"], cfg["api"]["contributors_endpoint"])
    if raw is None:
        return None

    contributors = [elem.text for elem in raw.findall(".//Contributor")]

    if not contributors:
        logger.warning("Empty response from /contributors.")
        return None

    df = pd.DataFrame({"contributor_name": contributors})
    df["extracted_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"Contributors extracted: {len(df)} rows.")
    return df