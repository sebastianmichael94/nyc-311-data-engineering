
import requests

url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

params = {
    "$select": (
        "unique_key,created_date,closed_date,agency,agency_name,"
        "complaint_type,descriptor,borough,incident_zip,status,:updated_at"
    ),
    "$where": (
        "created_date >= '2026-10-01T00:00:00' "
        "AND created_date < '2026-10-02T00:00:00'"
    ),
    "$order": "unique_key ASC",
    "$limit": 5,
}

try:
    response = requests.get(url, params=params, timeout=(10, 90))
    print("HTTP status:", response.status_code)
    response.raise_for_status()

    records = response.json()
    print("Records returned:", len(records))
    print("First record:", records[0] if records else "No records found")

except requests.exceptions.Timeout:
    print("The date-filtered API query timed out.")
except requests.exceptions.RequestException as exc:
    print("API request failed:", exc)
