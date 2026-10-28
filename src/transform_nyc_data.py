
from pathlib import Path

import pandas as pd

RAW_PATH = Path("data/raw/nyc_311.csv")
PROCESSED_PATH = Path("data/processed/nyc_311_clean.csv")
REJECTED_PATH = Path("data/processed/nyc_311_rejected.csv")


def load_data() -> pd.DataFrame:
    """Load the raw NYC 311 CSV."""
    return pd.read_csv(RAW_PATH)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize text fields, parse dates, and normalize ZIP codes."""
    df = df.copy()

    # Remember the original date values so we can distinguish
    # a genuinely missing close date from an invalid one.
    raw_created = df["created_date"].copy()
    raw_closed = df["closed_date"].copy()

    # Standardize text fields.
    text_columns = [
        "agency",
        "agency_name",
        "complaint_type",
        "descriptor",
        "borough",
        "status",
    ]

    for column in text_columns:
        df[column] = df[column].str.strip()

    # Parse dates. Invalid date strings become NaT.
    df["created_date"] = pd.to_datetime(
        raw_created, errors="coerce"
    )
    df["closed_date"] = pd.to_datetime(
        raw_closed, errors="coerce"
    )

    # Track invalid, non-empty date strings separately.
    df["_invalid_created_date"] = (
        raw_created.notna()
        & raw_created.astype("string").str.strip().ne("")
        & df["created_date"].isna()
    )

    df["_invalid_closed_date"] = (
        raw_closed.notna()
        & raw_closed.astype("string").str.strip().ne("")
        & df["closed_date"].isna()
    )

    # ZIP codes are identifiers, not numeric measurements.
    df["incident_zip"] = (
        pd.to_numeric(df["incident_zip"], errors="coerce")
        .astype("Int64")
        .astype("string")
    )

    return df


def validate_data(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate valid records from rejected records with reasons."""
    df = df.copy()

    # Keep a list of rejection reasons for every record.
    reasons = [[] for _ in range(len(df))]

    def reject(mask: pd.Series, reason: str) -> None:
        """Add a reason to every row matching the mask."""
        for position, matches in enumerate(mask.fillna(False).tolist()):
            if matches:
                reasons[position].append(reason)

    # 1. Request ID must exist.
    reject(
        df["unique_key"].isna(),
        "Missing unique_key",
    )

    # 2. Keep the first occurrence of each non-missing ID.
    duplicate_ids = (
        df["unique_key"].notna()
        & df["unique_key"].duplicated(keep="first")
    )
    reject(duplicate_ids, "Duplicate unique_key")

    # 3. Creation date must exist and parse successfully.
    reject(
        df["created_date"].isna(),
        "Missing or invalid created_date",
    )

    # 4. A supplied close date must parse successfully.
    # A genuinely missing close date is allowed.
    reject(
        df["_invalid_closed_date"],
        "Invalid closed_date",
    )

    # 5. A request cannot close before it was created.
    invalid_order = (
        df["created_date"].notna()
        & df["closed_date"].notna()
        & (df["closed_date"] < df["created_date"])
    )
    reject(invalid_order, "closed_date before created_date")

    # 6. Complaint type must not be missing or blank.
    invalid_complaint = (
        df["complaint_type"].isna()
        | df["complaint_type"].eq("")
    )
    reject(invalid_complaint, "Missing complaint_type")

    # Build the rejected dataset and retain all applicable reasons.
    rejected_mask = pd.Series(
        [bool(row_reasons) for row_reasons in reasons],
        index=df.index,
    )

    rejected_df = df.loc[rejected_mask].copy()
    rejected_df["rejection_reason"] = [
        "; ".join(row_reasons)
        for row_reasons, is_rejected
        in zip(reasons, rejected_mask.tolist())
        if is_rejected
    ]

    valid_df = df.loc[~rejected_mask].copy()

    # Remove internal helper columns from both output datasets.
    helper_columns = [
        "_invalid_created_date",
        "_invalid_closed_date",
    ]

    valid_df = valid_df.drop(columns=helper_columns)
    rejected_df = rejected_df.drop(columns=helper_columns)

    return valid_df, rejected_df


def main() -> None:
    """Run the complete transformation and validation pipeline."""
    raw_df = load_data()
    transformed_df = transform_data(raw_df)

    valid_df, rejected_df = validate_data(transformed_df)

    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)

    valid_df.to_csv(PROCESSED_PATH, index=False)
    rejected_df.to_csv(REJECTED_PATH, index=False)

    print("--- Pipeline summary ---")
    print(f"Input rows: {len(raw_df)}")
    print(f"Valid rows: {len(valid_df)}")
    print(f"Rejected rows: {len(rejected_df)}")
    print(f"Clean data saved to: {PROCESSED_PATH}")
    print(f"Rejected data saved to: {REJECTED_PATH}")

    if not rejected_df.empty:
        print("\n--- Rejection reasons ---")
        print(rejected_df["rejection_reason"].value_counts().to_string())

    # Confirm that no records disappeared during processing.
    assert len(valid_df) + len(rejected_df) == len(raw_df)


if __name__ == "__main__":
    main()
