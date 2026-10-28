
import json
from pathlib import Path

import requests

# 1. Fetch data from the API
url = "https://jsonplaceholder.typicode.com/users"

response = requests.get(url, timeout=10)
response.raise_for_status()

users = response.json()

print("Number of users fetched:", len(users))

# 2. Define where the raw data should be saved
output_path = Path("data/raw/users.json")

# 3. Create the directory if it doesn't already exist
output_path.parent.mkdir(parents=True, exist_ok=True)

# 4. Save the raw API response as JSON
with output_path.open("w", encoding="utf-8") as file:
    json.dump(users, file, indent=4, ensure_ascii=False)

print(f"Raw API data saved to: {output_path}")
