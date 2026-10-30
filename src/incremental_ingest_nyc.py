
import logging
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

from src.checkpoints import get_checkpoint, save_checkpoint
from src.ingest_nyc_bronze import (
    API_URL,
    DATABASE_URL,
    MAX_RETRIES,
    PAGE_SIZE,
    SOURCE_CITY,
    save_page,
)

CHECKPOINT_NAME = "incremental_updated_at"
OVERLAP = timedelta(minutes=5)
INITIAL_LOOKBACK = timedelta(hours=24)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_page(
    session: requests.Session,
    lower_bound: datetime,
    upper_bound: datetime,
    last_updated_at: str | None,
    last_key: str | None,
) -> list[dict]:
    """Fetch a page ordered by update timestamp and request ID."""
    lower = lower_bound.isoformat(timespec="milliseconds")
    upper = upper_bound.isoformat(timespec="milliseconds")

    where = (
        f":updated_at >= '{lower}' "
        f"AND :updated_at <= '{upper}'"
    )

    if last_updated_at is not None and last_key is not None:
        where += (
            f" AND (:updated_at > '{last_updated_at}' "
            f"OR (:updated_at = '{last_updated_at}' "
            f"AND unique_key > '{last_key}'))"
        )

    params = {
        "$select": (
            "unique_key,created_date,closed_date,agency,agency_name,"
            "complaint_type,descriptor,borough,incident_zip,status,"
            "latitude,longitude,:updated_at"
        ),
        "$where": where,
        "$order": ":updated_at ASC, unique_key ASC",
        "$limit": PAGE_SIZE,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                API_URL,
                params=params,
                timeout=(10, 90),
            )
            response.raise_for_status()
            records = response.json()

            if not isinstance(records, list):
                raise ValueError("API response was not a JSON list")

            return records

        except (requests.exceptions.RequestException, ValueError):
            if attempt == MAX_RETRIES:
                raise

            delay = 2**attempt
            logger.warning(
                "API request failed; retrying in %s seconds",
                delay,
            )
            time.sleep(delay)

    raise RuntimeError("API retries exhausted")


def run_incremental() -> int:
    total_inserted = 0
    latest_processed_timestamp = None
    last_updated_at = None
    last_key = None

    with psycopg.connect(DATABASE_URL) as connection:
        checkpoint = get_checkpoint(
            connection,
            SOURCE_CITY,
            CHECKPOINT_NAME,
        )

    upper_bound = datetime.now(timezone.utc)

    if checkpoint:
        previous_watermark = datetime.fromisoformat(checkpoint)

        if previous_watermark.tzinfo is None:
            raise ValueError(
                "Saved incremental checkpoint has no timezone"
            )

        lower_bound = previous_watermark - OVERLAP
        logger.info("Resuming from checkpoint: %s", checkpoint)
    else:
        lower_bound = upper_bound - INITIAL_LOOKBACK
        logger.info("No checkpoint found; initial lookback is 24 hours")

    logger.info(
        "Query window: %s through %s",
        lower_bound,
        upper_bound,
    )

    with requests.Session() as session:
        while True:
            records = fetch_page(
                session,
                lower_bound,
                upper_bound,
                last_updated_at,
                last_key,
            )

            if not records:
                break

            # Save each page before advancing the pagination cursor.
            with psycopg.connect(DATABASE_URL) as connection:
                inserted = save_page(connection, records)

            total_inserted += inserted

            last = records[-1]
            last_updated_at = last[":updated_at"]
            last_key = str(last["unique_key"])

            latest_processed_timestamp = last[":updated_at"]

            logger.info(
                "Fetched %s records; inserted %s new snapshots "
                "(%s inserted this run)",
                len(records),
                inserted,
                total_inserted,
            )

            if len(records) < PAGE_SIZE:
                break

    # Update the checkpoint only after every page succeeds.
    # Do not advance it to the current time when no records were fetched.
    if latest_processed_timestamp is not None:
        checkpoint_timestamp = datetime.fromisoformat(
            latest_processed_timestamp.replace("Z", "+00:00")
        )

        with psycopg.connect(DATABASE_URL) as connection:
            save_checkpoint(
                connection,
                SOURCE_CITY,
                CHECKPOINT_NAME,
                checkpoint_timestamp.isoformat(),
            )

        logger.info(
            "Saved checkpoint at latest processed source timestamp: %s",
            checkpoint_timestamp.isoformat(),
        )
    else:
        logger.info("No records fetched; checkpoint left unchanged.")

    logger.info(
        "Incremental run completed; new snapshots: %s",
        total_inserted,
    )

    return total_inserted


if __name__ == "__main__":
    run_incremental()
