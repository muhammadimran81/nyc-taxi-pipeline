"""Extract step: download NYC TLC yellow-taxi trip files and the zone lookup."""
import logging
import os
import urllib.request

log = logging.getLogger(__name__)

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def _download(url: str, dest: str) -> str:
    """Download url to dest atomically. Skips the download if dest already exists."""
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        log.info("Already downloaded, skipping: %s", dest)
        return dest
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    tmp = dest + ".part"
    log.info("Downloading %s", url)
    urllib.request.urlretrieve(url, tmp)
    os.replace(tmp, dest)
    log.info("Saved %s (%.1f MB)", dest, os.path.getsize(dest) / 1e6)
    return dest


def download_month(month: str, data_dir: str) -> str:
    """month is 'YYYY-MM'. Returns the local path of the raw parquet file."""
    dest = os.path.join(data_dir, "raw", f"yellow_tripdata_{month}.parquet")
    return _download(TRIP_URL.format(month=month), dest)


def download_zones(data_dir: str) -> str:
    dest = os.path.join(data_dir, "raw", "taxi_zone_lookup.csv")
    return _download(ZONES_URL, dest)
