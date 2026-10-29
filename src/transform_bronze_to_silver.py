
import logging
import os

import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

BATCH_SIZE = 1000

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
    """Map an NYC raw payload into the common Silver schema."""
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

    # One transaction: a failure rolls back this transformation run.
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor(name="latest_bronze_snapshots") as read_cursor:
            read_cursor.execute(
                """
                SELECT DISTINCT ON (source_city, source_request_id)
                    source_city,
                    raw_payload
                FROM bronze.service_requests_raw
                WHERE source_city = 'NYC'
                ORDER BY
                    source_city,
                    source_request_id,
                    source_updated_at DESC NULLS LAST,
                    raw_record_id DESC
                """
            )

            with connection.cursor() as write_cursor:
                while True:
                    batch = read_cursor.fetchmany(BATCH_SIZE)
                    if not batch:
                        break

                    rows = []
                    records_read += len(batch)

                    for city, payload in batch:
                        row = transform_record(city, payload)
                        if row is None:
                            rejected_count += 1
                        else:
                            rows.append(row)

                    if rows:
                        write_cursor.executemany(UPSERT_SQL, rows)
                        transformed_count += len(rows)

                    logger.info(
                        "Latest snapshots read=%s; transformed=%s; rejected=%s",
                        records_read,
                        transformed_count,
                        rejected_count,
                    )

    print(f"Latest snapshots read: {records_read}")
    print(f"Records transformed/upserted: {transformed_count}")
    print(f"Records skipped: {rejected_count}")


if __name__ == "__main__":
    main()
