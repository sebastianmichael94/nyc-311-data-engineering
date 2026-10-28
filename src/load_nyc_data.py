
from pathlib import Path

import pandas as pd
import psycopg


CSV_PATH = Path("data/processed/nyc_311_clean.csv")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "nyc_311",
    "user": "pipeline_user",
    "password": "local_dev_password",
}

INSERT_SQL = """
    INSERT INTO nyc_311_requests (
        unique_key,
        created_date,
        closed_date,
        agency,
        agency_name,
        complaint_type,
        descriptor,
        borough,
        incident_zip,
        status
    )
    VALUES %s
    ON CONFLICT (unique_key) DO NOTHING;
"""


def load_data() -> pd.DataFrame:
    return pd.read_csv(CSV_PATH)



def load_into_postgres(df: pd.DataFrame) -> int:
    columns = [
        "unique_key",
        "created_date",
        "closed_date",
        "agency",
        "agency_name",
        "complaint_type",
        "descriptor",
        "borough",
        "incident_zip",
        "status",
    ]

    df = df[columns].copy()
    df = df.astype(object).where(pd.notna(df), None)
    rows = list(df.itertuples(index=False, name=None))

    insert_sql = """
        INSERT INTO nyc_311_requests (
            unique_key, created_date, closed_date, agency, agency_name,
            complaint_type, descriptor, borough, incident_zip, status
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
       ON CONFLICT (unique_key)
    DO UPDATE SET
    created_date = EXCLUDED.created_date,
    closed_date = EXCLUDED.closed_date,
    agency = EXCLUDED.agency,
    agency_name = EXCLUDED.agency_name,
    complaint_type = EXCLUDED.complaint_type,
    descriptor = EXCLUDED.descriptor,
    borough = EXCLUDED.borough,
    incident_zip = EXCLUDED.incident_zip,
    status = EXCLUDED.status;
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
            inserted_count = cur.rowcount

    return inserted_count


def main() -> None:
    df = load_data()
    inserted_count = load_into_postgres(df)

    print(f"CSV rows: {len(df)}")
    print(f"New rows inserted: {inserted_count}")


if __name__ == "__main__":
    main()
