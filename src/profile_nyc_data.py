
from pathlib import Path

import pandas as pd

input_path = Path("data/raw/nyc_311.csv")
df = pd.read_csv(input_path)

# 1. Check whether each request ID is unique.
print("--- Duplicate request IDs ---")
print(df["unique_key"].duplicated().sum())

# 2. Parse the date columns.
created = pd.to_datetime(df["created_date"], errors="coerce")
closed = pd.to_datetime(df["closed_date"], errors="coerce")

print("\n--- Date quality ---")
print(f"Invalid created dates: {created.isna().sum()}")
print(f"Missing or invalid closed dates: {closed.isna().sum()}")

# 3. Check for impossible date relationships.
invalid_duration = (
    created.notna()
    & closed.notna()
    & (closed < created)
)
print(f"Closed before created: {invalid_duration.sum()}")

# 4. Compare request status with whether a close date exists.
print("\n--- Status vs. close date ---")
print(pd.crosstab(
    df["status"],
    df["closed_date"].isna(),
    rownames=["status"],
    colnames=["close_date_missing"],
))

# 5. Inspect the most common categories.
print("\n--- Top 10 complaint types ---")
print(df["complaint_type"].value_counts().head(10))

print("\n--- Request statuses ---")
print(df["status"].value_counts())
