import logging
from datetime import date

from src.ingest_nyc_bronze import ingest_month

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

START_YEAR = 2026
START_MONTH = 1


def backfill() -> None:
    today = date.today()
    year, month = START_YEAR, START_MONTH

    while (year, month) <= (today.year, today.month):
        logging.info("Backfill progress: %04d-%02d", year, month)

        try:
            ingest_month(year, month)
        except Exception:
            logging.exception(
                "Backfill stopped at %04d-%02d. "
                "Fix the error and rerun to resume.",
                year,
                month,
            )
            raise

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    logging.info("Historical backfill finished.")


if __name__ == "__main__":
    backfill()
