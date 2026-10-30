
import requests
import psycopg

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
DATABASE_URL = (
    "postgresql://pipeline_user:local_dev_password"
    "@localhost:5433/nyc_311"
)

start = "2026-10-01T00:00:00"
end = "2026-10-02T00:00:00"
page_size = 1000

source_ids = set()
last_created_date = None
last_key = None

base_where = (
    f"created_date >= '{start}' "
    f"AND created_date < '{end}'"
)

with requests.Session() as session:
    while True:
        where = base_where

        if last_created_date is not None:
            where += (
                f" AND (created_date > '{last_created_date}' "
                f"OR (created_date = '{last_created_date}' "
                f"AND unique_key > '{last_key}'))"
            )

        response = session.get(
            API_URL,
            params={
                "$select": "unique_key,created_date",
                "$where": where,
                "$order": "created_date ASC, unique_key ASC",
                "$limit": page_size,
            },
            timeout=(10, 90),
        )
        response.raise_for_status()
        records = response.json()

        if not records:
            break

        for record in records:
            source_ids.add(str(record["unique_key"]))

        last_created_date = records[-1]["created_date"]
        last_key = str(records[-1]["unique_key"])

        print(f"Fetched source IDs so far: {len(source_ids)}")

        if len(records) < page_size:
            break

with psycopg.connect(DATABASE_URL) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT source_request_id
            FROM bronze.service_requests_raw
            WHERE source_city = 'NYC'
              AND raw_payload ->> 'created_date' >= %s
              AND raw_payload ->> 'created_date' < %s
            """,
            (start, end),
        )
        bronze_ids = {row[0] for row in cursor.fetchall()}

missing_ids = source_ids - bronze_ids
extra_ids = bronze_ids - source_ids

print("\n--- Comparison results ---")
print("Source IDs:", len(source_ids))
print("Bronze IDs:", len(bronze_ids))
print("Missing from Bronze:", len(missing_ids))
print("Extra in Bronze:", len(extra_ids))
print("Sample missing IDs:", list(missing_ids)[:10])
