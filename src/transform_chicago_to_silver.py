
import logging
import os
from datetime import datetime

import psycopg
from psycopg.rows import tuple_row

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

SOURCE_CITY = "Chicago"
CHECKPOINT_NAME = "silver_transform_raw_id"
BATCH_SIZE = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def parse_timestamp(value):
    """Convert an API timestamp into a PostgreSQL-compatible naive timestamp."""
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def parse_number(value):
    """Convert coordinates into floats, returning None for invalid values."""
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def transform_record(record):
    """Map one Chicago Bronze snapshot into the shared Silver schema."""
    raw_record_id, request_id, source_updated_at, ingested_at, payload = record

    created_at = parse_timestamp(payload.get("created_date"))
    category = payload.get("sr_type")

    # Silver requires both created_at and category.
    if created_at is None or not category:
        return None

    closed_at = parse_timestamp(payload.get("closed_date"))

    # Prefer the Bronze source timestamp. Fall back to created_date
    # only if the source timestamp is unexpectedly missing.
    if source_updated_at is None:
        source_updated_at = parse_timestamp(
            payload.get("last_modified_date")
        )

    if source_updated_at is None:
        source_updated_at = created_at

    owner_department = payload.get("owner_department")

    return (
        SOURCE_CITY,
        request_id,
        created_at,
        closed_at,
        owner_department,
        owner_department,
        category,
        None,  # Chicago has no direct equivalent to NYC descriptor.
        payload.get("status"),
        payload.get("community_area"),
        payload.get("zip_code"),
        parse_number(payload.get("latitude")),
        parse_number(payload.get("longitude")),
        source_updated_at,
        ingested_at,
    )


UPSERT_SQL = """
INSERT INTO silver.service_requests (
    source_city,
    source_request_id,
    created_at,
    closed_at,
    agency,
    agency_name,
    category,
    description,
    status,
    area,
    postal_code,
    latitude,
    longitude,
    source_updated_at,
    ingested_at
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
ON CONFLICT (source_city, source_request_id)
DO UPDATE SET
    created_at = EXCLUDED.created_at,
    closed_at = EXCLUDED.closed_at,
    agency = EXCLUDED.agency,
    agency_name = EXCLUDED.agency_name,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    status = EXCLUDED.status,
    area = EXCLUDED.area,
    postal_code = EXCLUDED.postal_code,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    source_updated_at = EXCLUDED.source_updated_at,
    ingested_at = EXCLUDED.ingested_at
WHERE
    silver.service_requests.source_updated_at IS NULL
    OR (
        EXCLUDED.source_updated_at IS NOT NULL
        AND EXCLUDED.source_updated_at
            >= silver.service_requests.source_updated_at
    );
"""


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

    return int(result[0]) if result else 0


def save_checkpoint(cursor, raw_record_id):
    cursor.execute(
        """
        INSERT INTO ops.ingestion_checkpoints (
            source_city,
            checkpoint_name,
            checkpoint_value,
            updated_at
        )
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (source_city, checkpoint_name)
        DO UPDATE SET
            checkpoint_value = EXCLUDED.checkpoint_value,
            updated_at = NOW();
        """,
        (SOURCE_CITY, CHECKPOINT_NAME, str(raw_record_id)),
    )


def get_upper_watermark(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COALESCE(MAX(raw_record_id), 0)
            FROM bronze.chicago_service_requests_raw;
            """
        )
        return cursor.fetchone()[0]


def get_affected_request_ids(connection, lower_id, upper_id):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT source_request_id
            FROM bronze.chicago_service_requests_raw
            WHERE raw_record_id > %s
              AND raw_record_id <= %s
            ORDER BY source_request_id;
            """,
            (lower_id, upper_id),
        )
        return [row[0] for row in cursor.fetchall()]


def get_latest_snapshots(connection, request_ids):
    if not request_ids:
        return []

    with connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (source_request_id)
                raw_record_id,
                source_request_id,
                source_updated_at,
                ingested_at,
                raw_payload
            FROM bronze.chicago_service_requests_raw
            WHERE source_request_id = ANY(%s)
            ORDER BY
                source_request_id,
                source_updated_at DESC NULLS LAST,
                raw_record_id DESC;
            """,
            (request_ids,),
        )
        return cursor.fetchall()


def transform_batch(connection, lower_id, upper_id):
    request_ids = get_affected_request_ids(
        connection, lower_id, upper_id
    )

    if not request_ids:
        return 0, 0, 0

    latest_snapshots = get_latest_snapshots(connection, request_ids)

    transformed = 0
    skipped = 0
    upserted = 0

    with connection.cursor() as cursor:
        for record in latest_snapshots:
            silver_record = transform_record(record)

            if silver_record is None:
                skipped += 1
                continue

            cursor.execute(UPSERT_SQL, silver_record)

            if cursor.rowcount == 1:
                upserted += 1

            transformed += 1

    return len(request_ids), transformed, skipped, upserted


def run_transformation():
    logger.info("Starting Chicago Bronze-to-Silver transformation.")

    with psycopg.connect(DATABASE_URL) as connection:
        checkpoint = get_checkpoint(connection)
        upper_watermark = get_upper_watermark(connection)

        logger.info("Starting checkpoint: %s", checkpoint)
        logger.info("Upper watermark: %s", upper_watermark)

        if checkpoint >= upper_watermark:
            logger.info("No new Chicago Bronze records to process.")
            return

        total_affected = 0
        total_transformed = 0
        total_skipped = 0
        total_upserted = 0

        batch_number = 0

        for batch_start in range(
            checkpoint + 1,
            upper_watermark + 1,
            BATCH_SIZE,
        ):
            batch_end = min(
                batch_start + BATCH_SIZE - 1,
                upper_watermark,
            )

            batch_number += 1

            # Silver writes and checkpoint advancement commit together.
            with connection.transaction():
                affected, transformed, skipped, upserted = (
                    transform_batch(
                        connection,
                        batch_start - 1,
                        batch_end,
                    )
                )

                save_checkpoint(
                    connection.cursor(),
                    batch_end,
                )

            total_affected += affected
            total_transformed += transformed
            total_skipped += skipped
            total_upserted += upserted

            logger.info(
                "Batch %s | Bronze IDs %s-%s | "
                "Affected requests: %s | "
                "Transformed: %s | Upserted: %s | Skipped: %s",
                batch_number,
                batch_start,
                batch_end,
                affected,
                transformed,
                upserted,
                skipped,
            )

        logger.info("==============================================")
        logger.info("CHICAGO SILVER TRANSFORMATION SUMMARY")
        logger.info("Batches processed: %s", batch_number)
        logger.info("Affected requests: %s", total_affected)
        logger.info("Transformed records: %s", total_transformed)
        logger.info("Silver rows inserted/updated: %s", total_upserted)
        logger.info("Skipped records: %s", total_skipped)
        logger.info("Final checkpoint: %s", upper_watermark)
        logger.info("==============================================")


if __name__ == "__main__":
    run_transformation()
