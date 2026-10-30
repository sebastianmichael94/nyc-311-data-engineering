
import logging
import os

import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

BATCH_SIZE = 1000
CHECKPOINT_NAME = "silver_transform_raw_id"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO silver.service_requests (
    source_city, source_request_id, created_at, closed_at,
    agency, agency_name, category, description, status,
    area, postal_code, latitude, longitude, source_updated_at
)
VALUES (
    %(source_city)s, %(source_request_id)s, %(created_at)s,
    %(closed_at)s, %(agency)s, %(agency_name)s, %(category)s,
    %(description)s, %(status)s, %(area)s, %(postal_code)s,
    %(latitude)s, %(longitude)s, %(source_updated_at)s
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
    source_updated_at = EXCLUDED.source_updated_at
WHERE silver.service_requests.source_updated_at IS NULL
   OR EXCLUDED.source_updated_at >= silver.service_requests.source_updated_at;
"""


def parse_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def transform_record(city: str, payload: dict) -> dict | None:
    request_id = payload.get("unique_key")
    created_at = payload.get("created_date")
    category = payload.get("complaint_type")

    if not request_id or not created_at or not category:
        return None

    return {
        "source_city": city,
        "source_request_id": str(request_id),
        "created_at": created_at,
        "closed_at": payload.get("closed_date") or None,
        "agency": payload.get("agency") or None,
        "agency_name": payload.get("agency_name") or None,
        "category": category.strip(),
        "description": payload.get("descriptor") or None,
        "status": payload.get("status") or None,
        "area": payload.get("borough") or None,
        "postal_code": payload.get("incident_zip") or None,
        "latitude": parse_float(payload.get("latitude")),
        "longitude": parse_float(payload.get("longitude")),
        "source_updated_at": payload.get(":updated_at") or None,
    }


def main():
    records_read = 0
    transformed_count = 0
    rejected_count = 0

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            # Read the previous transformation watermark.
            cursor.execute(
                """
                SELECT checkpoint_value
                FROM ops.ingestion_checkpoints
                WHERE source_city = 'NYC'
                  AND checkpoint_name = %s
                """,
                (CHECKPOINT_NAME,),
            )
            result = cursor.fetchone()

            if result is None:
                raise RuntimeError(
                    "Transformation checkpoint missing. "
                    "Initialize it before running."
                )

            last_id = int(result[0])

            # Capture a stable upper bound for this run.
            cursor.execute("""
                SELECT COALESCE(MAX(raw_record_id), 0)
                FROM bronze.service_requests_raw
                WHERE source_city = 'NYC'
            """)
            upper_id = cursor.fetchone()[0]

            if upper_id <= last_id:
                print("No new Bronze records to transform.")
                return

            logger.info(
                "Transforming Bronze IDs %s through %s",
                last_id + 1,
                upper_id,
            )

            # Identify distinct requests touched since the last run.
            cursor.execute("""
                CREATE TEMP TABLE changed_requests (
                    source_city TEXT NOT NULL,
                    source_request_id TEXT NOT NULL,
                    PRIMARY KEY (source_city, source_request_id)
                ) ON COMMIT DROP
            """)

            cursor.execute(
                """
                INSERT INTO changed_requests
                SELECT DISTINCT source_city, source_request_id
                FROM bronze.service_requests_raw
                WHERE source_city = 'NYC'
                  AND raw_record_id > %s
                  AND raw_record_id <= %s
                ON CONFLICT DO NOTHING
                """,
                (last_id, upper_id),
            )

            cursor.execute("SELECT COUNT(*) FROM changed_requests")
            affected_count = cursor.fetchone()[0]

            logger.info("Distinct affected requests: %s", affected_count)

            
            # Process affected requests in batches using keyset pagination.
            
            # Process affected requests in batches using keyset pagination.
            last_request_id = ""

            while True:
                cursor.execute(
                    """
                    SELECT source_request_id
                    FROM changed_requests
                    WHERE source_request_id > %s
                    ORDER BY source_request_id
                    LIMIT %s
                    """,
                    (last_request_id, BATCH_SIZE),
                )

                request_ids = [
                    row[0] for row in cursor.fetchall()
                ]

                if not request_ids:
                    break

                last_request_id = request_ids[-1]

                # Retrieve the latest historical snapshot for each request.
                cursor.execute(
                    """
                    SELECT DISTINCT ON (
                        b.source_city, b.source_request_id
                    )
                        b.source_city,
                        b.raw_payload
                    FROM bronze.service_requests_raw b
                    WHERE b.source_city = 'NYC'
                      AND b.source_request_id = ANY(%s)
                    ORDER BY
                        b.source_city,
                        b.source_request_id,
                        b.source_updated_at DESC NULLS LAST,
                        b.raw_record_id DESC
                    """,
                    (request_ids,),
                )

                latest_records = cursor.fetchall()
                records_read += len(latest_records)

                rows = []

                for city, payload in latest_records:
                    row = transform_record(city, payload)

                    if row is None:
                        rejected_count += 1
                    else:
                        rows.append(row)

                if rows:
                    cursor.executemany(UPSERT_SQL, rows)
                    transformed_count += len(rows)

                logger.info(
                    "Affected requests processed: %s/%s",
                    records_read,
                    affected_count,
                )


            # Advance the checkpoint in the same transaction as the upserts.
            cursor.execute(
                """
                UPDATE ops.ingestion_checkpoints
                SET checkpoint_value = %s,
                    updated_at = NOW()
                WHERE source_city = 'NYC'
                  AND checkpoint_name = %s
                """,
                (str(upper_id), CHECKPOINT_NAME),
            )

        # Connection context commits only if everything succeeds.

    print(f"Upper watermark: {upper_id}")
    print(f"Distinct affected requests: {affected_count}")
    print(f"Latest snapshots read: {records_read}")
    print(f"Records transformed/upserted: {transformed_count}")
    print(f"Records skipped: {rejected_count}")


if __name__ == "__main__":
    main()
