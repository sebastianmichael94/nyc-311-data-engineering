
import json
import logging
import os
import time
from datetime import date, datetime, timezone

import psycopg
import requests
from psycopg.types.json import Jsonb

from src.checkpoints import get_checkpoint, save_checkpoint

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
SOURCE_CITY = "NYC"
PAGE_SIZE = 1000
MAX_RETRIES = 4

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_page(
    session: requests.Session,
    start_date: date,
    end_date: date,
    last_created_date: str | None,
    last_key: str | None,
) -> list[dict]:
    """Fetch the next page using a compound pagination cursor."""
    where_clause = (
        f"created_date >= '{start_date.isoformat()}T00:00:00' "
        f"AND created_date < '{end_date.isoformat()}T00:00:00'"
    )

    if last_created_date is not None and last_key is not None:
        where_clause += (
            f" AND (created_date > '{last_created_date}' "
            f"OR (created_date = '{last_created_date}' "
            f"AND unique_key > '{last_key}'))"
        )

    params = {
        "$select": (
            "unique_key,created_date,closed_date,agency,agency_name,"
            "complaint_type,descriptor,borough,incident_zip,status,"
            "latitude,longitude,:updated_at"
        ),
        "$where": where_clause,
        "$order": "created_date ASC, unique_key ASC",
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

        except (requests.exceptions.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                logger.error("API request failed after %s attempts", attempt)
                raise

            wait_seconds = 2**attempt
            logger.warning(
                "API request failed (attempt %s/%s): %s; retrying in %ss",
                attempt,
                MAX_RETRIES,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise RuntimeError("API retries exhausted")


def save_page(
    connection: psycopg.Connection,
    records: list[dict],
) -> int:
    """Insert raw snapshots. Fail rather than silently skip malformed records."""
    inserted_count = 0

    with connection.cursor() as cursor:
        for record in records:
            request_id = record.get("unique_key")
            created_date = record.get("created_date")
            updated_at = record.get(":updated_at")

            if not request_id or not created_date or not updated_at:
                raise ValueError(
                    "API record is missing unique_key, created_date, "
                    "or :updated_at; refusing to silently skip it"
                )

            parsed_updated_at = datetime.fromisoformat(
                updated_at.replace("Z", "+00:00")
            )

            if parsed_updated_at.tzinfo is None:
                raise ValueError(
                    f"Updated timestamp has no timezone: {updated_at}"
                )

            parsed_updated_at = parsed_updated_at.astimezone(timezone.utc)

            cursor.execute(
                """
                INSERT INTO bronze.service_requests_raw (
                    source_city,
                    source_request_id,
                    source_updated_at,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (
                    source_city,
                    source_request_id,
                    source_updated_at
                )
                WHERE source_updated_at IS NOT NULL
                DO NOTHING
                RETURNING raw_record_id
                """,
                (
                    SOURCE_CITY,
                    str(request_id),
                    parsed_updated_at,
                    Jsonb(record),
                ),
            )

            if cursor.fetchone() is not None:
                inserted_count += 1

    return inserted_count


def ingest_date_range(start_date: date, end_date: date) -> int:
    """Ingest a date range with a durable, transactionally saved page cursor."""
    if start_date >= end_date:
        raise ValueError("start_date must be earlier than end_date")

    progress_name = (
        f"progress_{start_date.isoformat()}_{end_date.isoformat()}"
    )

    # Autocommit keeps each explicit transaction limited to one page.
    with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
        saved_progress = get_checkpoint(
            connection, SOURCE_CITY, progress_name
        )

    last_created_date = None
    last_key = None

    if saved_progress:
        progress = json.loads(saved_progress)

        if progress.get("complete") is True:
            logger.info("Date range already complete: %s to %s",
                        start_date, end_date)
            return 0

        last_created_date = progress.get("created_date")
        last_key = progress.get("unique_key")

        if bool(last_created_date) != bool(last_key):
            raise ValueError("Saved progress contains an incomplete cursor")

        if last_created_date:
            logger.info(
                "Resuming after created_date=%s, unique_key=%s",
                last_created_date,
                last_key,
            )

    total_inserted = 0

    with requests.Session() as session:
        with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
            while True:
                records = fetch_page(
                    session,
                    start_date,
                    end_date,
                    last_created_date,
                    last_key,
                )

                if not records:
                    # Mark completion only after the API confirms no more pages.
                    with connection.transaction():
                        save_checkpoint(
                            connection,
                            SOURCE_CITY,
                            progress_name,
                            json.dumps({"complete": True}),
                        )
                    break

                for record in records:
                    if not record.get("created_date") or not record.get("unique_key"):
                        raise ValueError(
                            "Page contains a record missing its pagination cursor"
                        )

                next_created_date = records[-1]["created_date"]
                next_key = str(records[-1]["unique_key"])

                if last_created_date is not None:
                    current_cursor = (last_created_date, last_key)
                    next_cursor = (next_created_date, next_key)

                    if next_cursor <= current_cursor:
                        raise RuntimeError(
                            f"Pagination cursor failed to advance: "
                            f"{current_cursor} -> {next_cursor}"
                        )

                # Records and their resume cursor commit or roll back together.
                with connection.transaction():
                    inserted = save_page(connection, records)

                    save_checkpoint(
                        connection,
                        SOURCE_CITY,
                        progress_name,
                        json.dumps({
                            "created_date": next_created_date,
                            "unique_key": next_key,
                        }),
                    )

                total_inserted += inserted
                last_created_date = next_created_date
                last_key = next_key

                logger.info(
                    "Page fetched=%s; inserted this run=%s; "
                    "inserted total this run=%s; last cursor=(%s, %s)",
                    len(records),
                    inserted,
                    total_inserted,
                    last_created_date,
                    last_key,
                )

    logger.info(
        "Date range complete: %s to %s; inserted this run=%s",
        start_date,
        end_date,
        total_inserted,
    )
    return total_inserted


def ingest_month(year: int, month: int) -> int:
    """Ingest one month and save its completion checkpoint."""
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")

    start_date = date(year, month, 1)

    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)

    checkpoint_name = f"backfill_{year:04d}-{month:02d}"

    with psycopg.connect(DATABASE_URL) as connection:
        existing = get_checkpoint(
            connection, SOURCE_CITY, checkpoint_name
        )

    if existing == end_date.isoformat():
        logger.info("Skipping completed month %s", checkpoint_name)
        return 0

    logger.info("Starting month %s", checkpoint_name)
    inserted = ingest_date_range(start_date, end_date)

    with psycopg.connect(DATABASE_URL) as connection:
        save_checkpoint(
            connection,
            SOURCE_CITY,
            checkpoint_name,
            end_date.isoformat(),
        )

    logger.info(
        "Month %s completed; new raw snapshots inserted=%s",
        checkpoint_name,
        inserted,
    )
    return inserted


if __name__ == "__main__":
    count = ingest_month(2026, 10)
    print(f"Monthly ingestion inserted: {count}")
