
import csv
import os
from pathlib import Path

import psycopg


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)

OUTPUT_DIR = (
    Path(__file__).resolve().parent.parent
    / "output"
    / "cross_city_analytics"
)


# ---------------------------------------------------------
# Cross-city analytics queries
# All queries use the shared Silver table.
# ---------------------------------------------------------

REPORTS = {

    # Question 1:
    # What are the top 10 complaint categories in each city?
    "cross_city_top_categories.csv": """
        WITH category_counts AS (
            SELECT
                source_city,
                COALESCE(NULLIF(TRIM(category), ''), 'Unknown') AS category,
                COUNT(*) AS total_requests,
                COUNT(*) FILTER (
                    WHERE closed_at IS NOT NULL
                ) AS closed_requests
            FROM silver.service_requests
            GROUP BY
                source_city,
                COALESCE(NULLIF(TRIM(category), ''), 'Unknown')
        ),
        ranked_categories AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY source_city
                    ORDER BY total_requests DESC
                ) AS category_rank
            FROM category_counts
        )
        SELECT
            source_city,
            category_rank,
            category,
            total_requests,
            closed_requests
        FROM ranked_categories
        WHERE category_rank <= 10
        ORDER BY source_city, category_rank;
    """,

    # Question 2:
    # How does monthly request volume compare between cities?
    "cross_city_monthly_trends.csv": """
        SELECT
            source_city,
            DATE_TRUNC('month', created_at)::date AS month,
            COUNT(*) AS total_requests,
            COUNT(*) FILTER (
                WHERE closed_at IS NOT NULL
            ) AS closed_requests
        FROM silver.service_requests
        GROUP BY
            source_city,
            DATE_TRUNC('month', created_at)
        ORDER BY month, source_city;
    """,

    # Question 3:
    # Which city has the highest request closure rate?
    "cross_city_closure_rates.csv": """
        SELECT
            source_city,
            COUNT(*) AS total_requests,
            COUNT(*) FILTER (
                WHERE closed_at IS NOT NULL
            ) AS closed_requests,
            COUNT(*) FILTER (
                WHERE closed_at IS NULL
            ) AS open_requests,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE closed_at IS NOT NULL
                ) / NULLIF(COUNT(*), 0),
                2
            ) AS closure_rate_pct
        FROM silver.service_requests
        GROUP BY source_city
        ORDER BY closure_rate_pct DESC;
    """,

    # Question 4:
    # How do average and median resolution times compare
    # between cities for each complaint category?
    "cross_city_resolution_times.csv": """
        SELECT
            source_city,
            COALESCE(NULLIF(TRIM(category), ''), 'Unknown') AS category,
            COUNT(*) AS closed_requests,
            ROUND(
                (
                    AVG(
                        EXTRACT(EPOCH FROM (closed_at - created_at))
                        / 3600
                    )
                )::numeric,
                2
            ) AS avg_resolution_hours,
            ROUND(
                (
                    PERCENTILE_CONT(0.5) WITHIN GROUP (
                        ORDER BY
                            EXTRACT(EPOCH FROM (closed_at - created_at))
                            / 3600
                    )
                )::numeric,
                2
            ) AS median_resolution_hours
        FROM silver.service_requests
        WHERE closed_at IS NOT NULL
          AND closed_at >= created_at
        GROUP BY
            source_city,
            COALESCE(NULLIF(TRIM(category), ''), 'Unknown')
        HAVING COUNT(*) >= 30
        ORDER BY category, source_city;
    """,

    # Question 5:
    # How does request volume vary by weekday in each city?
    "cross_city_weekday_patterns.csv": """
        SELECT
            source_city,
            EXTRACT(ISODOW FROM created_at)::integer AS weekday_number,
            TO_CHAR(created_at, 'FMDay') AS weekday,
            COUNT(*) AS total_requests,
            COUNT(*) FILTER (
                WHERE closed_at IS NOT NULL
            ) AS closed_requests
        FROM silver.service_requests
        GROUP BY
            source_city,
            EXTRACT(ISODOW FROM created_at),
            TO_CHAR(created_at, 'FMDay')
        ORDER BY weekday_number, source_city;
    """,
}


# ---------------------------------------------------------
# CSV export
# ---------------------------------------------------------

def export_report(connection, filename, query):
    with connection.cursor() as cursor:
        cursor.execute(query)

        headers = [
            column.name
            for column in cursor.description
        ]

        rows = cursor.fetchall()

    output_path = OUTPUT_DIR / filename

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(
        f"Exported {len(rows):,} rows -> {output_path}"
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Starting cross-city analytics export...")
    print(f"Reports to export: {len(REPORTS)}")

    with psycopg.connect(DATABASE_URL) as connection:
        for filename, query in REPORTS.items():
            export_report(
                connection,
                filename,
                query,
            )

    print(
        "\nAll five cross-city analytics reports "
        "exported successfully."
    )


if __name__ == "__main__":
    main()
