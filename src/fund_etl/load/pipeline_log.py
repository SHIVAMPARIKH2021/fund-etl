# fund_etl/load/pipeline_log.py
import psycopg

from fund_etl.config import settings
from fund_etl.sql.queries import insert_pipeline_execution_log, update_pipeline_execution_log


def start_pipeline_execution(year: int, quarter: int, created_by: str = "fund-etl") -> int:
    """
    Initializes or updates the pipeline run to IN_PROGRESS.
    Upserts using uq_pipeline_year_quarter constraint.
    Returns the log_id.
    """
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(insert_pipeline_execution_log, {"year": year, "quarter": quarter, "created_by": created_by})
            log_id = cur.fetchone()[0]
        conn.commit()
    return log_id


def mark_pipeline_success(log_id: int, total_records: int) -> None:
    """Marks the execution record as COMPLETED with total rows ingested."""
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(update_pipeline_execution_log,
                        {
                            "status":"COMPLETED",
                            "error_message":None,
                            "log_id": log_id,
                            "total_records": total_records
                        }
                    )
        conn.commit()


def mark_pipeline_failed(log_id: int, error_message: str, partial_records: int = 0) -> None:
    """Marks the execution record as FAILED and captures exception trace."""
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                update_pipeline_execution_log,
                {
                    "log_id": log_id,
                    "status": "FAILED",
                    "error_message": error_message[:2000],  # keep bounded for DB column
                    "total_records": partial_records,
                },
            )
        conn.commit()