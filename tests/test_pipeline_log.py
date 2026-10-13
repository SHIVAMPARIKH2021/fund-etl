"""Tests for fund_etl.load.pipeline_log."""

from fund_etl.load.pipeline_log import (
    mark_pipeline_failed,
    mark_pipeline_success,
    start_pipeline_execution,
)


def test_start_pipeline_execution_returns_log_id(db):
    db.cursor._fetchone_result = (123,)
    log_id = start_pipeline_execution(2026, 2)
    assert log_id == 123
    assert db.conn.commits == 1
    query, params = db.cursor.executed[0]
    assert "INSERT INTO" in query
    assert params == {"year": 2026, "quarter": 2, "created_by": "fund-etl"}


def test_start_pipeline_execution_custom_created_by(db):
    db.cursor._fetchone_result = (5,)
    log_id = start_pipeline_execution(2025, 4, created_by="tester")
    assert log_id == 5
    _, params = db.cursor.executed[0]
    assert params["created_by"] == "tester"


def test_mark_pipeline_success(db):
    mark_pipeline_success(log_id=9, total_records=1000)
    assert db.conn.commits == 1
    query, params = db.cursor.executed[0]
    assert "UPDATE" in query
    assert params["status"] == "COMPLETED"
    assert params["log_id"] == 9
    assert params["total_records"] == 1000
    assert params["error_message"] is None


def test_mark_pipeline_failed(db):
    mark_pipeline_failed(log_id=3, error_message="boom", partial_records=42)
    assert db.conn.commits == 1
    query, params = db.cursor.executed[0]
    assert "UPDATE" in query
    assert params["status"] == "FAILED"
    assert params["log_id"] == 3
    assert params["error_message"] == "boom"
    assert params["total_records"] == 42


def test_mark_pipeline_failed_truncates_long_error(db):
    long_error = "x" * 5000
    mark_pipeline_failed(log_id=1, error_message=long_error)
    _, params = db.cursor.executed[0]
    # Error message is bounded to the DB column size (2000 chars).
    assert len(params["error_message"]) == 2000


def test_mark_pipeline_failed_default_partial_records(db):
    mark_pipeline_failed(log_id=1, error_message="err")
    _, params = db.cursor.executed[0]
    assert params["total_records"] == 0
