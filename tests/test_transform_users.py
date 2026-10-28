
from src.transform_users import transform_user, validate_user


def test_transform_user_flattens_nested_json():
    raw_user = {
        "id": 1,
        "name": "  Leanne Graham  ",
        "email": "  SINCERE@EXAMPLE.COM ",
        "address": {"city": " Gwenborough "},
        "company": {"name": " Romaguera-Crona "},
    }

    result = transform_user(raw_user)

    assert result == {
        "customer_id": 1,
        "name": "Leanne Graham",
        "email": "sincere@example.com",
        "city": "Gwenborough",
        "company_name": "Romaguera-Crona",
    }


def test_valid_user_has_no_validation_errors():
    user = {
        "customer_id": 1,
        "name": "Leanne Graham",
        "email": "leanne@example.com",
        "city": "Gwenborough",
        "company_name": "Romaguera-Crona",
    }

    assert validate_user(user) == []


def test_missing_email_is_rejected():
    user = {
        "customer_id": 1,
        "name": "Leanne Graham",
        "email": "",
        "city": "Gwenborough",
        "company_name": "Romaguera-Crona",
    }

    errors = validate_user(user)

    assert "Missing or invalid email" in errors


def test_invalid_email_is_rejected():
    user = {
        "customer_id": 1,
        "name": "Leanne Graham",
        "email": "not-an-email",
        "city": "Gwenborough",
        "company_name": "Romaguera-Crona",
    }

    errors = validate_user(user)

    assert "Missing or invalid email" in errors


def test_missing_nested_address_does_not_crash():
    raw_user = {
        "id": 1,
        "name": "Leanne Graham",
        "email": "leanne@example.com",
        "company": {"name": "Romaguera-Crona"},
    }

    result = transform_user(raw_user)

    assert result["city"] == ""
    assert "Missing city" in validate_user(result)
