"""Tests for fund_etl.load.load."""

import io

import pandera.polars as pa
import polars as pl
import pytest
from psycopg import sql

from fund_etl.load.load import (
    bulk_copy_dataframe,
    copy_from_std_in_sql,
    link_quarter_taxonomy_tags,
)


# ---------------------------------------------------------------------------
# copy_from_std_in_sql (pure SQL builder)
# ---------------------------------------------------------------------------
def test_copy_from_std_in_sql_builds_composed_query():
    col_clause = sql.SQL(", ").join([sql.Identifier(c) for c in ["a", "b"]])
    composed = copy_from_std_in_sql(
        "COPY {temp_table} ({col_clause}) FROM STDIN {copy_options}",
        "sec_financials.submissions",
        col_clause,
        "WITH (FORMAT csv)",
    )
    assert isinstance(composed, sql.Composed)
    text = composed.as_string(None)
    assert "COPY" in text
    assert "FROM STDIN" in text
    assert "sec_financials" in text and "submissions" in text


def test_copy_from_std_in_sql_accepts_plain_string_col_clause():
    composed = copy_from_std_in_sql(
        "COPY {temp_table} ({col_clause}) FROM STDIN {copy_options}",
        "my_table",
        "a, b",
        "WITH (FORMAT csv)",
    )
    assert isinstance(composed, sql.Composed)
    assert "COPY" in composed.as_string(None)


def test_copy_from_std_in_sql_splits_schema_qualified_name():
    composed = copy_from_std_in_sql(
        "COPY {temp_table} ({col_clause}) FROM STDIN {copy_options}",
        "schema.sub.table",
        "a",
        "",
    )
    text = composed.as_string(None)
    assert "schema" in text and "sub" in text and "table" in text


# ---------------------------------------------------------------------------
# bulk_copy_dataframe
# ---------------------------------------------------------------------------
def test_bulk_copy_empty_dataframe_returns_zero(db):
    df = pl.DataFrame({"adsh": []})
    result = bulk_copy_dataframe(df, "sec_financials.tbl", ["adsh"])
    assert result == 0
    assert db.conn.commits == 0
    assert db.cursor.executed == []


def test_bulk_copy_without_conflict_columns(db):
    df = pl.DataFrame({"adsh": ["x"], "val": [1]})
    result = bulk_copy_dataframe(df, "sec_financials.tbl", ["adsh", "val"])
    assert result == 1
    assert db.conn.commits == 1
    # A COPY was streamed (bytes written) but no INSERT ... temp table.
    assert len(db.cursor.copied_chunks) > 0
    assert all("INSERT INTO" not in q for q, _ in db.cursor.executed)


def test_bulk_copy_with_conflict_columns_uses_temp_table(db):
    df = pl.DataFrame({"adsh": ["x"], "val": [1]})
    result = bulk_copy_dataframe(
        df, "sec_financials.submissions", ["adsh", "val"], conflict_columns=["adsh"]
    )
    assert result == 1
    assert db.conn.commits == 1
    # Temp table create + INSERT from temp table both executed.
    assert any("CREATE TEMP TABLE" in q for q, _ in db.cursor.executed)
    assert any("INSERT INTO" in q for q, _ in db.cursor.executed)
    assert len(db.cursor.copied_chunks) > 0


def test_bulk_copy_injects_extra_columns(db):
    df = pl.DataFrame({"adsh": ["x"]})
    result = bulk_copy_dataframe(
        df,
        "sec_financials.submissions",
        ["adsh", "quarter_id"],
        extra_columns={"quarter_id": 7},
    )
    assert result == 1
    # The copied CSV payload must contain the injected literal value.
    payload = b"".join(db.cursor.copied_chunks).decode()
    assert payload.strip().endswith("7")


def test_bulk_copy_fills_missing_target_columns_with_null(db):
    df = pl.DataFrame({"adsh": ["x"]})
    # "missing" is not in df; loader must add it as a null column.
    result = bulk_copy_dataframe(df, "sec_financials.tbl", ["adsh", "missing"])
    assert result == 1
    assert len(db.cursor.copied_chunks) > 0


def test_bulk_copy_schema_validation_success(db):
    schema = pa.DataFrameSchema(
        {"adsh": pa.Column(pl.String, checks=pa.Check.str_length(20, 20), nullable=False)}
    )
    df = pl.DataFrame({"adsh": ["0" * 20]})
    result = bulk_copy_dataframe(df, "sec_financials.tbl", ["adsh"], schema=schema)
    assert result == 1


def test_bulk_copy_schema_validation_failure_raises(db):
    schema = pa.DataFrameSchema(
        {"adsh": pa.Column(pl.String, checks=pa.Check.str_length(20, 20), nullable=False)}
    )
    df = pl.DataFrame({"adsh": ["short"]})
    with pytest.raises(pa.errors.SchemaError):
        bulk_copy_dataframe(df, "sec_financials.tbl", ["adsh"], schema=schema)
    # Validation failure happens before any DB work.
    assert db.conn.commits == 0


def test_bulk_copy_writes_tab_separated_payload(db):
    df = pl.DataFrame({"a": ["1"], "b": ["2"]})
    bulk_copy_dataframe(df, "sec_financials.tbl", ["a", "b"])
    payload = b"".join(db.cursor.copied_chunks).decode()
    assert "\t" in payload
    assert payload.strip() == "1\t2"


# ---------------------------------------------------------------------------
# link_quarter_taxonomy_tags
# ---------------------------------------------------------------------------
def test_link_quarter_taxonomy_tags_empty_returns_zero(db):
    df = pl.DataFrame({"tag": [], "version": []})
    result = link_quarter_taxonomy_tags(df, quarter_id=1)
    assert result == 0
    assert db.conn.commits == 0


def test_link_quarter_taxonomy_tags_dedupes_and_copies(db):
    df = pl.DataFrame(
        {
            "tag": ["Assets", "Assets", "Liabilities"],
            "version": ["us-gaap/2025", "us-gaap/2025", "us-gaap/2025"],
        }
    )
    result = link_quarter_taxonomy_tags(df, quarter_id=42)
    assert result == 2  # unique (tag, version) pairs
    assert db.conn.commits == 1
    assert any("quarter_taxonomy_tags" in q for q, _ in db.cursor.executed)
    # The quarter_id is passed through to the bridge insert.
    _, params = db.cursor.executed[-1]
    assert params == {"quarter_id": 42}
    assert len(db.cursor.copied_chunks) > 0
