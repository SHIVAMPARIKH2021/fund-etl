import io
import logging
from collections.abc import Sequence
from typing import Any, LiteralString, Union

import polars as pl
import psycopg
from psycopg import sql

from fund_etl.config import settings
from fund_etl.sql.queries import (
    copy_from_std_in,
    copy_quarter_taxonomy_tags,
    create_temporary_sec_financial_table,
    sql_insert,
    create_temporary_tag_bridge_table,
    copy_temporary_tag_bridge_table
)
import pandera as pa

logger = logging.getLogger(__name__)


def bulk_copy_dataframe(
    df: pl.DataFrame,
    table_name: str,
    target_columns: Sequence[str],
    conflict_columns: Sequence[str] | None = None,
    extra_columns: dict[str, Any] | None = None,
    schema: pa.DataFrameSchema | None = None,
) -> int:
    """Streams a Polars DataFrame into Postgres using psycopg COPY protocol in CSV format."""
    if df.is_empty():
        return 0

    # Optional Pandera validation step before transformation & copy
    if schema is not None:
        try:
            schema.validate(df)
        except pa.errors.SchemaError as exc:
            logger.error("Data validation failed for table %s: %s", table_name, exc)
            raise

    # Inject static columns (e.g., quarter_id)
    if extra_columns:
        for col_name, col_value in extra_columns.items():
            df = df.with_columns(pl.lit(col_value).alias(col_name))

    # Ensure all target columns exist; fill missing with null
    for col in target_columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    ordered_df = df.select(target_columns)

    buffer = io.BytesIO()
    ordered_df.write_csv(
        buffer,
        separator="\t",
        include_header=False,
        null_value="",
    )
    buffer.seek(0)

    col_identifiers = [sql.Identifier(col) for col in target_columns]
    col_clause = sql.SQL(", ").join(col_identifiers)
    copy_options = "WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"', ESCAPE '\"', NULL '')"

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            if conflict_columns:
                raw_table_name = table_name.split(".")[-1]
                temp_table = f"temp_{raw_table_name}"
                create_temp_table = sql.SQL(create_temporary_sec_financial_table)
                query = create_temp_table.format(
                    sql.Identifier(temp_table),
                    sql.Identifier(*(table_name.split(".")))
                )

                cur.execute(query)

                sql_copy = copy_from_std_in_sql(copy_from_std_in, temp_table, col_clause, copy_options)
                with cur.copy(sql_copy) as copy_op:
                    while chunk := buffer.read(65536):
                        copy_op.write(chunk)

                conflict_identifiers = [sql.Identifier(c) for c in conflict_columns]
                conflict_clause = sql.SQL(", ").join(conflict_identifiers)
                insert_query = sql.SQL(sql_insert).format(
                    table_name=sql.Identifier(*(table_name.split("."))),
                    col_clause=col_clause,
                    temp_table=sql.Identifier(temp_table),
                    conflict_clause=conflict_clause,
                )
                cur.execute(insert_query)
            else:
                sql_copy = copy_from_std_in_sql(copy_from_std_in, table_name, col_clause, copy_options)
                with cur.copy(sql_copy) as copy_op:
                    while chunk := buffer.read(65536):
                        copy_op.write(chunk)

        conn.commit()

    return len(ordered_df)


def link_quarter_taxonomy_tags(tag_df: pl.DataFrame, quarter_id: int) -> int:
    """Populates the quarter_taxonomy_tags bridge table."""
    if tag_df.is_empty():
        return 0

    unique_tags = tag_df.select(["tag", "version"]).unique()

    buffer = io.BytesIO()
    unique_tags.write_csv(buffer, separator="\t", include_header=False, null_value="")
    buffer.seek(0)

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(create_temporary_tag_bridge_table)

            with cur.copy(copy_temporary_tag_bridge_table) as copy:
                while chunk := buffer.read(65536):
                    copy.write(chunk)

            cur.execute(copy_quarter_taxonomy_tags, {"quarter_id": quarter_id})
        conn.commit()

    return len(unique_tags)


def copy_from_std_in_sql(
    sql_template: LiteralString,
    target_table: str,
    col_clause: Union[str, sql.Composed],
    copy_options: str,
) -> sql.Composed:
    """Builds a COPY sql_template string handling schema-qualified table names properly."""
    table_ident = sql.Identifier(*target_table.split("."))

    return sql.SQL(sql_template).format(
        temp_table=table_ident,
        col_clause=col_clause if isinstance(col_clause, sql.Composed) else sql.SQL(col_clause),
        copy_options=sql.SQL(copy_options),
    )