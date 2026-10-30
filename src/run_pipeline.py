import logging
import os
import subprocess
import sys
import time
from pathlib import Path


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

NYC_INGEST_CANDIDATES = [
    SRC_DIR / "incremental_ingest_nyc.py",
]


PIPELINE_STAGES = [
    (
        "NYC Bronze Ingestion",
        NYC_INGEST_CANDIDATES,
    ),
    (
        "Chicago Bronze Ingestion",
        [SRC_DIR / "ingest_chicago_bronze.py"],
    ),
    (
        "NYC Silver Transformation",
        [SRC_DIR / "transform_bronze_to_silver.py"],
    ),
    (
        "Chicago Silver Transformation",
        [SRC_DIR / "transform_chicago_to_silver.py"],
    ),
    (
        "Cross-City Analytics Export",
        [SRC_DIR / "export_cross_city_analytics.py"],
    ),
]



# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("pipeline")


# ---------------------------------------------------------
# Stage resolution and validation
# ---------------------------------------------------------

def resolve_stage_script(stage_name, candidates):
    existing = [path for path in candidates if path.is_file()]

    if not existing:
        expected = ", ".join(str(path) for path in candidates)
        raise FileNotFoundError(
            f"Could not find script for {stage_name}. "
            f"Expected one of: {expected}"
        )

    if len(existing) > 1:
        raise RuntimeError(
            f"Multiple scripts found for {stage_name}: {existing}. "
            "Configure the intended script explicitly."
        )

    return existing[0]


def validate_pipeline():
    resolved_stages = []

    for stage_name, candidates in PIPELINE_STAGES:
        script_path = resolve_stage_script(stage_name, candidates)
        resolved_stages.append((stage_name, script_path))

    logger.info("All pipeline scripts validated.")

    for stage_number, (stage_name, script_path) in enumerate(
        resolved_stages,
        start=1,
    ):
        logger.info(
            "Stage %s: %s -> %s",
            stage_number,
            stage_name,
            script_path.name,
        )

    return resolved_stages


# ---------------------------------------------------------
# Stage execution
# ---------------------------------------------------------

def run_stage(stage_number, total_stages, stage_name, script_path):
    logger.info("=" * 65)
    logger.info(
        "STARTING STAGE %s/%s: %s",
        stage_number,
        total_stages,
        stage_name,
    )
    logger.info("Script: %s", script_path.name)
    logger.info("=" * 65)

    start_time = time.perf_counter()

    # Make the project root available to Python imports
    # in every subprocess.
    env = os.environ.copy()

    existing_pythonpath = env.get("PYTHONPATH", "")

    env["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        if not existing_pythonpath
        else str(PROJECT_ROOT) + os.pathsep + existing_pythonpath
    )

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            env=env,
            check=False,
        )

    except OSError:
        logger.exception(
            "Could not start stage: %s",
            stage_name,
        )
        return False

    duration = time.perf_counter() - start_time

    if result.returncode != 0:
        logger.error(
            "STAGE FAILED: %s | Exit code: %s | Duration: %.2f seconds",
            stage_name,
            result.returncode,
            duration,
        )
        return False

    logger.info(
        "STAGE COMPLETED: %s | Duration: %.2f seconds",
        stage_name,
        duration,
    )

    return True


# ---------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------

def run_pipeline():
    pipeline_start = time.perf_counter()

    logger.info("#" * 65)
    logger.info("STARTING MULTI-SOURCE 311 DATA PIPELINE")
    logger.info("Project directory: %s", PROJECT_ROOT)
    logger.info("Python interpreter: %s", sys.executable)
    logger.info("#" * 65)

    try:
        stages = validate_pipeline()
        total_stages = len(stages)

        for stage_number, (stage_name, script_path) in enumerate(
            stages,
            start=1,
        ):
            success = run_stage(
                stage_number,
                total_stages,
                stage_name,
                script_path,
            )

            if not success:
                logger.error(
                    "PIPELINE STOPPED. Remaining stages were not executed."
                )
                return 1

        total_duration = time.perf_counter() - pipeline_start

        logger.info("#" * 65)
        logger.info("ALL PIPELINE STAGES COMPLETED SUCCESSFULLY")
        logger.info("Total stages: %s", total_stages)
        logger.info(
            "Total execution time: %.2f seconds",
            total_duration,
        )
        logger.info("#" * 65)

        return 0

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        return 130

    except Exception:
        logger.exception("Pipeline failed.")
        return 1


if __name__ == "__main__":
    sys.exit(run_pipeline())
