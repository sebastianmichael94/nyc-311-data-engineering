import csv
import os
from pathlib import Path

import psycopg


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

REPORTS = {
    "monthly_complaint_trends.csv": """
        SELECT
            DATE_TRUNC('month', created_at)::date AS month,
            COUNT(*) AS total_complaints,
            COUNT(*) FILTER (WHERE closed_at IS NOT NULL) AS closed_complaints,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE closed_at IS NOT NULL)
                / NULLIF(COUNT(*), 0), 2
            ) AS closure_rate_pct
        FROM silver.service_requests
        WHERE source_city = 'NYC'
          AND created_at >= TIMESTAMP '2026-01-01'
          AND created_at < TIMESTAMP '2027-01-01'
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month;
    """,

    "complaints_by_borough_category.csv": """
        SELECT
            COALESCE(NULLIF(TRIM(area), ''), 'Unknown') AS borough,
            COALESCE(NULLIF(TRIM(category), ''), 'Unknown') AS category,
            COUNT(*) AS total_complaints,
            COUNT(*) FILTER (WHERE closed_at IS NOT NULL) AS closed_complaints
        FROM silver.service_requests
        WHERE source_city = 'NYC'
          AND created_at >= TIMESTAMP '2026-01-01'
          AND created_at < TIMESTAMP '2027-01-01'
        GROUP BY
            COALESCE(NULLIF(TRIM(area), ''), 'Unknown'),
            COALESCE(NULLIF(TRIM(category), ''), 'Unknown')
        ORDER BY total_complaints DESC, borough, category;
    """,

    "closure_time_by_agency_category.csv": """
        SELECT
            COALESCE(NULLIF(TRIM(agency), ''), 'Unknown') AS agency,
            COALESCE(NULLIF(TRIM(category), ''), 'Unknown') AS category,
            COUNT(*) AS closed_complaints,
            ROUND(
                (AVG(EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600))::numeric,
                2
            ) AS avg_closure_hours,
            ROUND(
                (PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600
                ))::numeric,
                2
            ) AS median_closure_hours
        FROM silver.service_requests
        WHERE source_city = 'NYC'
          AND created_at >= TIMESTAMP '2026-01-01'
          AND created_at < TIMESTAMP '2027-01-01'
          AND closed_at IS NOT NULL
          AND closed_at >= created_at
        GROUP BY
            COALESCE(NULLIF(TRIM(agency), ''), 'Unknown'),
            COALESCE(NULLIF(TRIM(category), ''), 'Unknown')
        ORDER BY closed_complaints DESC, agency, category;
    """,
}


def export_report(connection, filename, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        headers = [column.name for column in cursor.description]
        rows = cursor.fetchall()

    output_path = OUTPUT_DIR / filename

    with output_path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Exported {len(rows):,} rows -> {output_path}")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(DATABASE_URL) as connection:
        for filename, query in REPORTS.items():
            export_report(connection, filename, query)

    print("All three analytics reports exported successfully.")


if __name__ == "__main__":
    main()
