# fund_etl/load/pipeline_log.py
from datetime import datetime
import psycopg
from fund_etl.config import settings


def start_pipeline_execution(year: int, quarter: int, created_by: str = "python-fund-etl") -> int:
    """
    Initializes or updates the pipeline run to IN_PROGRESS.
    Upserts using uq_pipeline_year_quarter constraint.
    Returns the log_id.
    """
    sql = """
        INSERT INTO sec_financials.pipeline_execution_log (
            filing_year,
            filing_quarter,
            ingestion_status,
            ingestion_records,
            ingestion_started_at,
            ingestion_completed_at,
            ingestion_error,
            created_by,
            created_at,
            updated_at
        ) VALUES (
            %(year)s, %(quarter)s, 'IN_PROGRESS', 0, CURRENT_TIMESTAMP, NULL, NULL,
            %(created_by)s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        ON CONFLICT (filing_year, filing_quarter) DO UPDATE SET
            ingestion_status = 'IN_PROGRESS',
            ingestion_records = 0,
            ingestion_started_at = CURRENT_TIMESTAMP,
            ingestion_completed_at = NULL,
            ingestion_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        RETURNING log_id;
    """
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"year": year, "quarter": quarter, "created_by": created_by})
            log_id = cur.fetchone()[0]
        conn.commit()
    return log_id


def mark_pipeline_success(log_id: int, total_records: int) -> None:
    """Marks the execution record as COMPLETED with total rows ingested."""
    sql = """
        UPDATE sec_financials.pipeline_execution_log
        SET ingestion_status = 'COMPLETED',
            ingestion_records = %(total_records)s,
            ingestion_completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE log_id = %(log_id)s;
    """
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"log_id": log_id, "total_records": total_records})
        conn.commit()


def mark_pipeline_failed(log_id: int, error_message: str, partial_records: int = 0) -> None:
    """Marks the execution record as FAILED and captures exception trace."""
    sql = """
        UPDATE sec_financials.pipeline_execution_log
        SET ingestion_status = 'FAILED',
            ingestion_records = %(partial_records)s,
            ingestion_error = %(error_message)s,
            ingestion_completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE log_id = %(log_id)s;
    """
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "log_id": log_id,
                    "error_message": error_message[:2000],  # keep bounded for DB column
                    "partial_records": partial_records,
                },
            )
        conn.commit()