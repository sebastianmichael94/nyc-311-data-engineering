
import argparse
import logging
import os
import time
from datetime import datetime, timedelta

import psycopg
import requests
from psycopg.types.json import Jsonb


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

INITIAL_START_DATE = "2026-01-01T00:00:00"

PAGE_SIZE = 1000
MAX_RETRIES = 4
REQUEST_TIMEOUT = 60
OVERLAP_MINUTES = 5

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

SOURCE_CITY = "Chicago"
CHECKPOINT_NAME = "incremental_updated_at"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Database setup
# ---------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS bronze.chicago_service_requests_raw (
    raw_record_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_city TEXT NOT NULL DEFAULT 'Chicago',
    source_request_id TEXT NOT NULL,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chicago_bronze_source_request
ON bronze.chicago_service_requests_raw (source_request_id);

CREATE INDEX IF NOT EXISTS idx_chicago_bronze_ingested_at
ON bronze.chicago_service_requests_raw (ingested_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_chicago_bronze_source_snapshot
ON bronze.chicago_service_requests_raw (
    source_request_id,
    source_updated_at
)
WHERE source_updated_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_chicago_bronze_latest_snapshot
ON bronze.chicago_service_requests_raw (
    source_request_id,
    source_updated_at DESC,
    raw_record_id DESC
);

CREATE TABLE IF NOT EXISTS ops.ingestion_checkpoints (
    source_city TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL,
    checkpoint_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_city, checkpoint_name)
);
"""


def setup_database(connection):
    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE_SQL)

    connection.commit()
    logger.info("Chicago Bronze table and indexes are ready.")


# ---------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------

def parse_source_timestamp(value):
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
        return parsed.replace(tzinfo=None)

    except ValueError:
        logger.warning("Invalid source timestamp: %s", value)
        return None


def get_source_updated_at(record):
    modified = parse_source_timestamp(
        record.get("last_modified_date")
    )

    if modified is not None:
        return modified

    return parse_source_timestamp(
        record.get("created_date")
    )


# ---------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------

def get_checkpoint(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT checkpoint_value
            FROM ops.ingestion_checkpoints
            WHERE source_city = %s
              AND checkpoint_name = %s;
            """,
            (SOURCE_CITY, CHECKPOINT_NAME),
        )

        result = cursor.fetchone()

    if result:
        checkpoint = parse_source_timestamp(result[0])
        if checkpoint is None:
            raise ValueError("Existing Chicago checkpoint is invalid.")

        logger.info("Loaded existing checkpoint: %s", checkpoint)
        return checkpoint

    # First checkpoint-enabled run:
    # initialize from the existing Chicago Bronze history.
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT MAX(source_updated_at)
            FROM bronze.chicago_service_requests_raw;
            """
        )

        result = cursor.fetchone()

    checkpoint = result[0] if result and result[0] else None

    if checkpoint is None:
        checkpoint = datetime.fromisoformat(INITIAL_START_DATE)
        logger.info(
            "No existing Bronze timestamps found. "
            "Using initial start date: %s",
            checkpoint,
        )
    else:
        logger.info(
            "Bootstrapping checkpoint from existing Chicago Bronze: %s",
            checkpoint,
        )

    return checkpoint


def save_checkpoint(connection, checkpoint):
    checkpoint_value = checkpoint.isoformat()

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.ingestion_checkpoints (
                source_city,
                checkpoint_name,
                checkpoint_value
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (source_city, checkpoint_name)
            DO UPDATE SET
                checkpoint_value = EXCLUDED.checkpoint_value,
                updated_at = NOW();
            """,
            (SOURCE_CITY, CHECKPOINT_NAME, checkpoint_value),
        )


# ---------------------------------------------------------
# API extraction - keyset pagination
# ---------------------------------------------------------


def fetch_page(
    session,
    page_size,
    start_timestamp,
    last_modified_date=None,
    last_request_id=None,
):
    conditions = [
        f"last_modified_date >= '{start_timestamp}'"
    ]

    if last_modified_date is not None and last_request_id is not None:
        cursor_timestamp = last_modified_date.isoformat()

        conditions.append(
            "("
            f"last_modified_date > '{cursor_timestamp}' "
            "OR "
            "("
            f"last_modified_date = '{cursor_timestamp}' "
            f"AND sr_number > '{last_request_id}'"
            ")"
            ")"
        )

    params = {
        "$select": "*",
        "$where": " AND ".join(conditions),
        "$order": "last_modified_date ASC, sr_number ASC",
        "$limit": page_size,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(
                    f"Retryable HTTP status: {response.status_code}"
                )

            response.raise_for_status()
            records = response.json()

            if not isinstance(records, list):
                raise ValueError("API response was not a JSON list.")

            return records

        except (requests.RequestException, ValueError):
            if attempt == MAX_RETRIES:
                logger.exception("API request failed.")
                raise

            wait_seconds = min(2 ** attempt, 30)

            logger.warning(
                "Request failed. Attempt %s/%s. Retrying in %s seconds.",
                attempt,
                MAX_RETRIES,
                wait_seconds,
            )

            time.sleep(wait_seconds)

    return []



# ---------------------------------------------------------
# Bronze loading
# ---------------------------------------------------------

INSERT_SQL = """
INSERT INTO bronze.chicago_service_requests_raw (
    source_city,
    source_request_id,
    source_updated_at,
    raw_payload
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (source_request_id, source_updated_at)
WHERE source_updated_at IS NOT NULL
DO NOTHING;
"""


def save_page(connection, records):
    inserted = 0
    skipped = 0

    with connection.cursor() as cursor:
        for record in records:
            request_id = record.get("sr_number")

            if not request_id:
                skipped += 1
                logger.warning("Skipping record without sr_number.")
                continue

            source_updated_at = get_source_updated_at(record)

            if source_updated_at is None:
                skipped += 1
                logger.warning(
                    "Skipping record %s without a valid timestamp.",
                    request_id,
                )
                continue

            cursor.execute(
                INSERT_SQL,
                (
                    SOURCE_CITY,
                    str(request_id),
                    source_updated_at,
                    Jsonb(record),
                ),
            )

            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

    connection.commit()

    return inserted, skipped


# ---------------------------------------------------------
# Ingestion pipeline
# ---------------------------------------------------------

def ingest(max_pages=None):
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0
    page_number = 0

    logger.info("Starting Chicago incremental Bronze ingestion.")

    with requests.Session() as session:
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "Chicago-311-Data-Pipeline/1.0",
        })

        with psycopg.connect(DATABASE_URL) as connection:
            setup_database(connection)

            previous_checkpoint = get_checkpoint(connection)

            start_timestamp = (
                previous_checkpoint
                - timedelta(minutes=OVERLAP_MINUTES)
            )

            logger.info("Previous checkpoint: %s", previous_checkpoint)
            logger.info(
                "Querying records modified since: %s",
                start_timestamp,
            )
            logger.info(
                "Pagination: keyset (last_modified_date, sr_number)"
            )

            last_modified_date = None
            last_request_id = None
            latest_timestamp = previous_checkpoint

            while True:
                if max_pages is not None and page_number >= max_pages:
                    logger.warning(
                        "Reached page limit: %s. "
                        "This is a partial run; checkpoint will NOT advance.",
                        max_pages,
                    )
                    return

                records = fetch_page(
                    session=session,
                    page_size=PAGE_SIZE,
                    start_timestamp=start_timestamp.isoformat(),
                    last_modified_date=last_modified_date,
                    last_request_id=last_request_id,
                )

                if not records:
                    logger.info("API returned no more records.")
                    break

                # Validate pagination fields before saving this page.
                last_record = records[-1]

                last_modified_date = last_record.get("last_modified_date")
                last_request_id = last_record.get("sr_number")

                if not last_modified_date or not last_request_id:
                    raise ValueError(
                        "Last API record is missing last_modified_date "
                        "or sr_number. Cannot safely continue pagination."
                    )

                last_modified_date = parse_source_timestamp(
                    last_modified_date
                )

                if last_modified_date is None:
                    raise ValueError(
                        "Last API record has an invalid last_modified_date."
                    )

                for record in records:
                    modified = parse_source_timestamp(
                        record.get("last_modified_date")
                    )

                    if modified is None:
                        raise ValueError(
                            "API returned a record without a valid "
                            "last_modified_date. Checkpoint cannot safely advance."
                        )

                    latest_timestamp = max(latest_timestamp, modified)

                inserted, skipped = save_page(connection, records)

                page_number += 1
                total_fetched += len(records)
                total_inserted += inserted
                total_skipped += skipped

                logger.info(
                    "Page %s | Fetched %s | Inserted %s | Skipped %s | "
                    "Last cursor: (%s, %s)",
                    page_number,
                    len(records),
                    inserted,
                    skipped,
                    last_modified_date,
                    last_request_id,
                )

                if len(records) < PAGE_SIZE:
                    logger.info("Reached final API page.")
                    break

                time.sleep(0.2)

            # Save checkpoint only after the complete API extraction succeeds.
            # Checkpoint and all page inserts are already committed at this point.
            if latest_timestamp > previous_checkpoint:
                with connection.transaction():
                    save_checkpoint(connection, latest_timestamp)

                logger.info(
                    "New Chicago ingestion checkpoint saved: %s",
                    latest_timestamp,
                )
            else:
                logger.info(
                    "No newer records found. Checkpoint remains: %s",
                    previous_checkpoint,
                )

    logger.info("=" * 55)
    logger.info("CHICAGO INCREMENTAL BRONZE INGESTION SUMMARY")
    logger.info("Pages processed: %s", page_number)
    logger.info("Records fetched: %s", total_fetched)
    logger.info("Records inserted: %s", total_inserted)
    logger.info("Records skipped: %s", total_skipped)
    logger.info("Final checkpoint: %s", latest_timestamp)
    logger.info("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Incrementally ingest Chicago 311 records into Bronze."
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limit pages for testing. Partial runs do not advance checkpoint.",
    )

    args = parser.parse_args()
    ingest(max_pages=args.max_pages)
