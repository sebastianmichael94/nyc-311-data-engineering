
import json
from pathlib import Path

import pandas as pd

input_path = Path("data/raw/users.json")
output_path = Path("data/processed/customers.csv")
rejected_path = Path("data/processed/rejected_customers.json")


def transform_user(user):
    """Flatten and normalize one API user record."""
    return {
        "customer_id": user.get("id"),
        "name": user.get("name", "").strip(),
        "email": user.get("email", "").strip().lower(),
        "city": user.get("address", {}).get("city", "").strip(),
        "company_name": user.get("company", {}).get("name", "").strip(),
    }


def validate_user(user):
    """Return a list of validation errors for one transformed record."""
    errors = []

    if user["customer_id"] is None:
        errors.append("Missing customer_id")

    if not user["name"]:
        errors.append("Missing name")

    if not user["email"] or "@" not in user["email"]:
        errors.append("Missing or invalid email")

    if not user["city"]:
        errors.append("Missing city")

    if not user["company_name"]:
        errors.append("Missing company_name")

    return errors


def main():
    with input_path.open("r", encoding="utf-8") as file:
        raw_users = json.load(file)

    valid_users = []
    rejected_users = []
    seen_ids = set()

    for raw_user in raw_users:
        user = transform_user(raw_user)
        errors = validate_user(user)

        # Check for duplicate IDs
        customer_id = user["customer_id"]
        if customer_id is not None and customer_id in seen_ids:
            errors.append("Duplicate customer_id")
        elif customer_id is not None:
            seen_ids.add(customer_id)

        if errors:
            rejected_users.append({
                "record": user,
                "errors": errors,
            })
        else:
            valid_users.append(user)

    # Save valid records
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(valid_users)
    df.to_csv(output_path, index=False)

    # Save rejected records and their reasons
    with rejected_path.open("w", encoding="utf-8") as file:
        json.dump(rejected_users, file, indent=4)

    print(f"Records received: {len(raw_users)}")
    print(f"Records accepted: {len(valid_users)}")
    print(f"Records rejected: {len(rejected_users)}")
    print(f"Accepted data: {output_path}")
    print(f"Rejected data: {rejected_path}")


if __name__ == "__main__":
    main()
