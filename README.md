# NYC 311 Data Pipeline

A Python and PostgreSQL data pipeline that ingests NYC 311 service requests, stores raw source snapshots, transforms them into a reusable analytics schema, and exports SQL-based reports to CSV.

## Architecture

- **Bronze:** Stores raw API payloads as JSONB, with source identifiers, update timestamps, and ingestion timestamps.
- **Silver:** Transforms source fields into a common service-request schema designed to support additional cities in the future.
- **Checkpoints:** Tracks ingestion progress to support resumable historical backfills.
- **Analytics:** Runs SQL aggregations against Silver and exports results as CSV files.

## Technology Stack

- Python
- PostgreSQL
- Docker Compose
- Requests
- Psycopg
- CSV / SQL analytics

## Data Scope

The current historical backfill covers NYC 311 requests created from January 1, 2026, through the latest available records retrieved during the October 2026 run.

The Silver layer contains the latest available snapshot per NYC request ID from the ingested Bronze data.

## Analytics Reports

Run:

```powershell
python .\src\export_analytics.py
```

The script creates the `output/` directory and exports:

1. `monthly_complaint_trends.csv` — monthly complaint volume, closed-request counts, and closure rates.
2. `complaints_by_borough_category.csv` — complaint counts grouped by borough and complaint category.
3. `closure_time_by_agency_category.csv` — closed-request counts and average/median closure durations in hours, excluding missing or invalid closure timestamps.

Generated CSV files are local outputs and are excluded from Git.

## Local Setup

1. Start PostgreSQL using Docker Compose:

   ```powershell
   docker compose up -d
   ```

2. Activate the virtual environment and install dependencies:

   ```powershell
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

3. Configure `DATABASE_URL` if your PostgreSQL connection differs from the local default.

4. Run the ingestion and transformation scripts as needed, then export the reports:

   ```powershell
   python .\src\export_analytics.py
   ```

## Current Validation

The 2026 Silver dataset contains 3,280,620 requests. The monthly report totals match that count. The generated reports contain 10 monthly rows, 961 borough/category groups, and 196 agency/category closure-time groups.

## Future Improvements

- Incremental ingestion for newly created and updated requests.
- Automated tests for transformations and analytics.
- Additional city adapters using the common Silver schema.
- Scheduled pipeline execution and operational monitoring.
