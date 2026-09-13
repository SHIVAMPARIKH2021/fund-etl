import io
import logging
from collections.abc import Sequence
import polars as pl
import psycopg

from fund_etl.config import settings

logger = logging.getLogger(__name__)


def bulk_copy_dataframe(
    df: pl.DataFrame,
    table_name: str,
    target_columns: Sequence[str],
    conflict_columns: Sequence[str] | None = None,
) -> int:
    """Streams a Polars DataFrame into Postgres using psycopg COPY protocol in CSV format."""
    if df.is_empty():
        return 0

    # Ensure all target columns exist; fill missing with null
    for col in target_columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    ordered_df = df.select(target_columns)

    # Serialize using RFC 4180 CSV standard with TAB delimiter
    buffer = io.BytesIO()
    ordered_df.write_csv(
        buffer,
        separator="\t",
        include_header=False,
        null_value="",  # CSV mode treats unquoted empty fields as NULL
    )
    buffer.seek(0)

    col_clause = ", ".join(f'"{col}"' for col in target_columns)
    # Use FORMAT csv with explicit QUOTE and NULL settings
    copy_options = "WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"', ESCAPE '\"', NULL '')"

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            if conflict_columns:
                temp_table = f"temp_{table_name.split('.')[-1]}"
                cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP;")

                sql_copy = f"COPY {temp_table} ({col_clause}) FROM STDIN {copy_options}"
                with cur.copy(sql_copy) as copy_op:
                    while chunk := buffer.read(65536):
                        copy_op.write(chunk)

                conflict_clause = ", ".join(f'"{c}"' for c in conflict_columns)
                sql_insert = f"""
                    INSERT INTO {table_name} ({col_clause})
                    SELECT {col_clause} FROM {temp_table}
                    ON CONFLICT ({conflict_clause}) DO NOTHING;
                """
                cur.execute(sql_insert)
            else:
                sql_copy = f"COPY {table_name} ({col_clause}) FROM STDIN {copy_options}"
                with cur.copy(sql_copy) as copy_op:
                    while chunk := buffer.read(65536):
                        copy_op.write(chunk)

        conn.commit()

    return len(ordered_df)