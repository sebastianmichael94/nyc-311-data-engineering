# NYC & Chicago 311 Data Engineering Pipeline

An end-to-end data engineering project that ingests 311 service requests from New York City and Chicago, stores source data in PostgreSQL, transforms both datasets into a shared data model, and generates cross-city analytics reports exported as CSV files.

## Architecture

```text
       NYC Open Data API          Chicago Data Portal API
               |                           |
               v                           v
       STAGE 1: NYC                STAGE 2: CHICAGO
       Data Ingestion              Data Ingestion
               |                           |
               v                           v
         NYC Bronze                 Chicago Bronze
               |                           |
               v                           v
       NYC Bronze → Silver    Chicago Bronze → Silver
               |                           |
               +-------------+-------------+
                             |
                             v
                    SHARED SILVER MODEL
                  silver.service_requests
                             |
                             v
                  STAGE 3: COMBINED PIPELINE
                     Cross-City Analytics
                             |
                             v
                       SQL Queries
                             |
                             v
                       CSV Reports
```

The orchestrator (`src/run_pipeline.py`) executes the following stages:

1. NYC incremental Bronze ingestion
2. Chicago incremental Bronze ingestion
3. NYC Bronze-to-Silver transformation
4. Chicago Bronze-to-Silver transformation
5. Cross-city analytics and CSV export

## Tech Stack

- **Language:** Python
- **Database:** PostgreSQL
- **Database Access:** Psycopg
- **Containerization:** Docker, Docker Compose
- **Data Sources:** NYC Open Data API, Chicago Data Portal API
- **Data Formats:** JSON, JSONB, CSV
- **Query Language:** SQL

## Data Pipeline

### Bronze Layer

- Stores raw API responses as JSONB.
- Maintains source request IDs, update timestamps, and ingestion timestamps.
- Preserves historical snapshots of updated service requests.
- Uses separate source-specific tables for NYC and Chicago.

### Silver Layer

- Transforms both source datasets into a shared PostgreSQL table: `silver.service_requests`.
- Standardizes source-specific fields into a common schema.
- Maintains one latest record per city and request ID.
- Uses upserts to apply newer source updates.

### Incremental Processing

- Timestamp-based checkpoints for incremental API ingestion.
- Keyset pagination for reliable API extraction.
- Overlap windows to reduce the risk of missing updates.
- Database constraints to prevent duplicate snapshots.
- Incremental Bronze-to-Silver transformations.
- Checkpoint-based recovery and idempotent pipeline reruns.
- Fail-fast orchestration to prevent downstream stages from running after a failure.

## Cross-City Analytics

SQL analytics queries run against the shared Silver model and export results to CSV files under `output/cross_city_analytics/`.

| CSV Report                        | Analysis                                                 |
| --------------------------------- | -------------------------------------------------------- |
| `cross_city_top_categories.csv`   | Top request categories by city                           |
| `cross_city_monthly_trends.csv`   | Monthly request volume by city                           |
| `cross_city_closure_rates.csv`    | Open and closed requests and closure rates               |
| `cross_city_resolution_times.csv` | Average and median resolution times by city and category |
| `cross_city_weekday_patterns.csv` | Request volume by weekday and city                       |

The project also generates three NYC-specific analytics reports:

- `monthly_complaint_trends.csv`
- `complaints_by_borough_category.csv`
- `closure_time_by_agency_category.csv`

All analytics query results are saved as CSV files for further analysis.

## Data Validation

Successful pipeline validation produced the following Silver-table results:

| City      | Unique Requests |
| --------- | --------------: |
| NYC       |       3,627,407 |
| Chicago   |       1,797,332 |
| **Total** |   **5,424,739** |

Validation confirmed:

- One Silver record per unique source request ID in each city.
- No missing request IDs.
- No missing creation timestamps.
- No missing categories.

## Project Structure

```text
retail-data-pipeline/
│
├── src/
│   ├── run_pipeline.py
│   ├── incremental_ingest_nyc.py
│   ├── ingest_chicago_bronze.py
│   ├── transform_bronze_to_silver.py
│   ├── transform_chicago_to_silver.py
│   ├── export_analytics.py
│   └── export_cross_city_analytics.py
│
├── sql/
├── data/
├── output/
│   ├── cross_city_analytics/
│   └── NYC analytics CSV reports
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## Setup & Execution

### Prerequisites

- Python 3.10+
- Docker Desktop with Docker Compose
- Git

### 1. Clone the repository

```bash
git clone https://github.com/sebastianmichael94/nyc-311-data-engineering.git
cd nyc-311-data-engineering
```

### 2. Create and activate a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Start PostgreSQL

```bash
docker compose up -d
```

Configure the database connection using the `DATABASE_URL` environment variable:

```powershell
$env:DATABASE_URL = "postgresql://pipeline_user:<your-password>@localhost:5433/nyc_311"
```

### 5. Run the complete pipeline

```powershell
python src/run_pipeline.py
```

### 6. Run cross-city analytics independently

```powershell
python src/export_cross_city_analytics.py
```

Generated CSV reports are saved under `output/cross_city_analytics/`.

## Data Sources

- [NYC 311 Service Requests](https://data.cityofnewyork.us/resource/erm2-nwe9.json)
- [Chicago 311 Service Requests](https://data.cityofchicago.org/resource/v6vf-nfxy.json)

**Note:** NYC and Chicago use different source category taxonomies. Category comparisons are descriptive and do not imply that categories are directly equivalent across cities. Closure rates reflect the current state recorded in the source data.
