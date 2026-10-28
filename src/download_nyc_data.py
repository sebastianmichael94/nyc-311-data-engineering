
from pathlib import Path

import requests

url = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

params = {
    "$select": (
        "unique_key,created_date,closed_date,agency,"
        "agency_name,complaint_type,descriptor,borough,"
        "incident_zip,status"
    ),
    "$limit": 50000,
    "$order": "created_date DESC",
}

output_path = Path("data/raw/nyc_311.csv")


def main():
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    # Create the output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save the original CSV response without transforming it
    output_path.write_bytes(response.content)

    print("NYC 311 data downloaded successfully.")
    print(f"HTTP status: {response.status_code}")
    print(f"File size: {output_path.stat().st_size:,} bytes")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
