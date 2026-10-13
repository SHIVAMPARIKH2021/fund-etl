"""Tests for the top-level main.py orchestration helpers."""

import argparse
from pathlib import Path

import psycopg
import pytest

import main


# ---------------------------------------------------------------------------
# str_to_bool
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value", [True, False])
def test_str_to_bool_passthrough_bool(value):
    assert main.str_to_bool(value) is value


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "t", "yes", "y", "1"])
def test_str_to_bool_truthy_strings(value):
    assert main.str_to_bool(value) is True


@pytest.mark.parametrize("value", ["false", "False", "FALSE", "f", "no", "n", "0"])
def test_str_to_bool_falsy_strings(value):
    assert main.str_to_bool(value) is False


def test_str_to_bool_invalid_raises():
    with pytest.raises(argparse.ArgumentTypeError):
        main.str_to_bool("maybe")


# ---------------------------------------------------------------------------
# resolve_quarter_id
# ---------------------------------------------------------------------------
def test_resolve_quarter_id_returns_fetched_id(db):
    db.cursor._fetchone_result = (77,)
    assert main.resolve_quarter_id(2026, 2) == 77
    assert db.conn.commits == 1
    query, params = db.cursor.executed[0]
    assert "quarter_config" in query
    assert params == {"year": 2026, "quarter": 2, "updated_by": "fund-etl"}


# ---------------------------------------------------------------------------
# copy_metadata_and_clean_directory
# ---------------------------------------------------------------------------
def test_copy_metadata_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        main.copy_metadata_and_clean_directory(tmp_path / "nope", tmp_path / "dest")


def test_copy_metadata_copies_json_and_htm(tmp_path):
    source = tmp_path / "raw"
    dest = tmp_path / "processed"
    quarter = source / "2026q2"
    quarter.mkdir(parents=True)
    (quarter / "rr1-metadata.json").write_text("{}")
    (quarter / "readme.htm").write_text("<html></html>")
    # A non-metadata file that should NOT be copied.
    (quarter / "sub.tsv").write_text("data")

    main.copy_metadata_and_clean_directory(source, dest)

    copied = dest / "2026q2"
    assert (copied / "rr1-metadata.json").exists()
    assert (copied / "readme.htm").exists()
    assert not (copied / "sub.tsv").exists()
    # Source directory is cleaned up.
    assert not quarter.exists()


def test_copy_metadata_creates_dest_and_removes_source(tmp_path):
    source = tmp_path / "raw"
    dest = tmp_path / "processed"
    quarter = source / "2025q1"
    quarter.mkdir(parents=True)
    (quarter / "readme.htm").write_text("x")

    main.copy_metadata_and_clean_directory(source, dest)

    assert dest.is_dir()
    assert (dest / "2025q1" / "readme.htm").exists()
    assert not quarter.exists()


# ---------------------------------------------------------------------------
# check_job_execution
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("quarter", [0, 5, -1])
def test_check_job_invalid_quarter(quarter):
    assert main.check_job_execution(2026, quarter) is False


def test_check_job_invalid_year():
    assert main.check_job_execution(1999, 2) is False


def test_check_job_db_error_returns_false(db, monkeypatch):
    def _raise(*_a, **_k):
        raise psycopg.Error("connection failed")

    monkeypatch.setattr(psycopg, "connect", _raise)
    assert main.check_job_execution(2026, 2) is False


def test_check_job_db_not_completed_returns_false(db):
    db.cursor._fetchone_result = ("IN_PROGRESS",)
    assert main.check_job_execution(2026, 2) is False


def test_check_job_db_no_row_returns_false(db):
    db.cursor._fetchone_result = None
    assert main.check_job_execution(2026, 2) is False


def test_check_job_completed_but_missing_dir_returns_false(db, monkeypatch, tmp_path):
    db.cursor._fetchone_result = ("COMPLETED",)
    monkeypatch.setattr(main, "PROCESSED_DATA_DIR", tmp_path)
    assert main.check_job_execution(2026, 2) is False


def test_check_job_completed_missing_artifacts_returns_false(db, monkeypatch, tmp_path):
    db.cursor._fetchone_result = ("COMPLETED",)
    target = tmp_path / "2026q2"
    target.mkdir()
    monkeypatch.setattr(main, "PROCESSED_DATA_DIR", tmp_path)
    assert main.check_job_execution(2026, 2) is False


def test_check_job_completed_empty_artifacts_returns_false(db, monkeypatch, tmp_path):
    db.cursor._fetchone_result = ("COMPLETED",)
    target = tmp_path / "2026q2"
    target.mkdir()
    (target / "readme.htm").write_text("")  # empty file
    (target / "rr1-metadata.json").write_text("{}")
    monkeypatch.setattr(main, "PROCESSED_DATA_DIR", tmp_path)
    assert main.check_job_execution(2026, 2) is False


def test_check_job_completed_with_artifacts_returns_true(db, monkeypatch, tmp_path):
    db.cursor._fetchone_result = ("COMPLETED",)
    target = tmp_path / "2026q2"
    target.mkdir()
    (target / "readme.htm").write_text("<html></html>")
    (target / "rr1-metadata.json").write_text("{}")
    monkeypatch.setattr(main, "PROCESSED_DATA_DIR", tmp_path)
    assert main.check_job_execution(2026, 2) is True


def test_check_job_oserror_returns_false(db, monkeypatch, tmp_path):
    db.cursor._fetchone_result = ("COMPLETED",)
    target = tmp_path / "2026q2"
    target.mkdir()
    (target / "readme.htm").write_text("x")
    (target / "rr1-metadata.json").write_text("{}")
    monkeypatch.setattr(main, "PROCESSED_DATA_DIR", tmp_path)

    original_is_file = Path.is_file

    def _boom(self):
        if self.name in ("readme.htm", "rr1-metadata.json"):
            raise OSError("disk error")
        return original_is_file(self)

    monkeypatch.setattr(Path, "is_file", _boom)
    assert main.check_job_execution(2026, 2) is False
