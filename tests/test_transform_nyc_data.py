
import pandas as pd
import pytest

from src.transform_nyc_data import transform_data, validate_data


def make_record(**overrides):
    """Create a valid sample NYC 311 record."""
    record = {
        "unique_key": 1001,
        "created_date": "2026-10-20T10:00:00",
        "closed_date": "2026-10-20T12:00:00",
        "agency": "NYPD",
        "agency_name": "Police Department",
        "complaint_type": "Noise",
        "descriptor": "Loud music",
        "borough": "BROOKLYN",
        "incident_zip": 11211.0,
        "status": "Closed",
    }
    record.update(overrides)
    return record


def run_validation(records):
    """Transform records, then validate them."""
    df = pd.DataFrame(records)
    transformed_df = transform_data(df)
    return validate_data(transformed_df)


def test_valid_record_passes():
    valid, rejected = run_validation([make_record()])

    assert len(valid) == 1
    assert len(rejected) == 0


def test_missing_closed_date_is_allowed():
    valid, rejected = run_validation([
        make_record(closed_date=None, status="In Progress")
    ])

    assert len(valid) == 1
    assert len(rejected) == 0
    assert pd.isna(valid.iloc[0]["closed_date"])


def test_missing_unique_key_is_rejected():
    valid, rejected = run_validation([
        make_record(unique_key=None)
    ])

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Missing unique_key" in rejected.iloc[0]["rejection_reason"]


def test_duplicate_unique_key_rejects_later_record():
    valid, rejected = run_validation([
        make_record(),
        make_record(complaint_type="Parking"),
    ])

    assert len(valid) == 1
    assert len(rejected) == 1
    assert rejected.iloc[0]["complaint_type"] == "Parking"
    assert "Duplicate unique_key" in rejected.iloc[0]["rejection_reason"]


@pytest.mark.parametrize(
    "created_date",
    [None, "not-a-date"],
)
def test_missing_or_invalid_created_date_is_rejected(created_date):
    valid, rejected = run_validation([
        make_record(created_date=created_date)
    ])

    assert len(valid) == 0
    assert len(rejected) == 1
    assert (
        "Missing or invalid created_date"
        in rejected.iloc[0]["rejection_reason"]
    )


def test_invalid_closed_date_is_rejected():
    valid, rejected = run_validation([
        make_record(closed_date="not-a-date")
    ])

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Invalid closed_date" in rejected.iloc[0]["rejection_reason"]


def test_closed_before_created_is_rejected():
    valid, rejected = run_validation([
        make_record(
            created_date="2026-10-20T12:00:00",
            closed_date="2026-10-20T10:00:00",
        )
    ])

    assert len(valid) == 0
    assert len(rejected) == 1
    assert (
        "closed_date before created_date"
        in rejected.iloc[0]["rejection_reason"]
    )


@pytest.mark.parametrize(
    "complaint_type",
    [None, "   "],
)
def test_missing_or_blank_complaint_type_is_rejected(complaint_type):
    valid, rejected = run_validation([
        make_record(complaint_type=complaint_type)
    ])

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "Missing complaint_type" in rejected.iloc[0]["rejection_reason"]


def test_rejected_records_preserve_reasons():
    valid, rejected = run_validation([
        make_record(
            created_date="2026-10-20T12:00:00",
            closed_date="2026-10-20T10:00:00",
        )
    ])

    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == (
        "closed_date before created_date"
    )


def test_valid_and_rejected_counts_reconcile():
    records = [
        make_record(),
        make_record(
            unique_key=1002,
            closed_date=None,
            status="Open",
        ),
        make_record(
            unique_key=1003,
            created_date="2026-10-20T12:00:00",
            closed_date="2026-10-20T10:00:00",
        ),
    ]

    valid, rejected = run_validation(records)

    assert len(valid) + len(rejected) == len(records)
    assert len(valid) == 2
    assert len(rejected) == 1
