
import os

import psycopg


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline_user:local_dev_password@localhost:5433/nyc_311",
)


def get_checkpoint(
    connection: psycopg.Connection,
    source_city: str,
    checkpoint_name: str,
) -> str | None:
    """Return the saved checkpoint value, or None if it doesn't exist."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT checkpoint_value
            FROM ops.ingestion_checkpoints
            WHERE source_city = %s
              AND checkpoint_name = %s
            """,
            (source_city, checkpoint_name),
        )
        row = cursor.fetchone()

    return row[0] if row else None


def save_checkpoint(
    connection: psycopg.Connection,
    source_city: str,
    checkpoint_name: str,
    checkpoint_value: str,
) -> None:
    """Insert or update a checkpoint."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ops.ingestion_checkpoints (
                source_city,
                checkpoint_name,
                checkpoint_value,
                updated_at
            )
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (source_city, checkpoint_name)
            DO UPDATE SET
                checkpoint_value = EXCLUDED.checkpoint_value,
                updated_at = NOW()
            """,
            (source_city, checkpoint_name, checkpoint_value),
        )


if __name__ == "__main__":
    # Temporary smoke test. The connection context commits on success.
    with psycopg.connect(DATABASE_URL) as connection:
        save_checkpoint(
            connection,
            "NYC",
            "test_checkpoint",
            "2026-10-01",
        )

        saved_value = get_checkpoint(
            connection,
            "NYC",
            "test_checkpoint",
        )

        print("Saved checkpoint:", saved_value)
