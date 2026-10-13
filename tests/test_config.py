"""Tests for fund_etl.config."""

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from fund_etl import config
from fund_etl.config import (
    BASE_DIR,
    DATA_DIR,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
    Settings,
    settings,
)

VALID_DB = "postgresql://postgres:postgres@localhost:5432/funds"
VALID_UA = "PetProjectFundETL admin@example.com"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Ensure the real env vars don't leak into validation tests."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)


def test_path_constants_are_consistent():
    assert BASE_DIR.name == "fund-etl"
    assert DATA_DIR == BASE_DIR / "data"
    assert RAW_DATA_DIR == DATA_DIR / "raw"
    assert PROCESSED_DATA_DIR == DATA_DIR / "processed"
    assert all(isinstance(p, Path) for p in (BASE_DIR, DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR))


def test_singleton_settings_loaded_from_env_file():
    # The module-level singleton is built from the repo .env at import time.
    assert isinstance(settings, Settings)
    assert settings.database_url is not None
    assert settings.sec_user_agent  # non-empty
    assert settings.batch_size == 250_000
    assert settings.log_level == "INFO"


def test_settings_optional_defaults():
    s = Settings(_env_file=None, database_url=VALID_DB, sec_user_agent=VALID_UA)
    assert s.sec_base_url.startswith("https://www.sec.gov/files/dera")
    assert s.ticker_url.endswith("company_tickers_mf.json")
    assert s.batch_size == 250_000
    assert s.log_level == "INFO"


def test_settings_reads_from_environment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", VALID_DB)
    monkeypatch.setenv("SEC_USER_AGENT", VALID_UA)
    s = Settings(_env_file=None)
    assert str(s.database_url).startswith("postgresql://")
    assert s.sec_user_agent == VALID_UA


def test_settings_missing_database_url_raises():
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None, sec_user_agent=VALID_UA)
    assert "database_url" in str(exc.value)


def test_settings_missing_user_agent_raises():
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None, database_url=VALID_DB)
    assert "sec_user_agent" in str(exc.value)


def test_settings_user_agent_too_short_raises():
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None, database_url=VALID_DB, sec_user_agent="short")
    assert "sec_user_agent" in str(exc.value)


def test_settings_invalid_database_url_raises():
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None, database_url="not-a-dsn", sec_user_agent=VALID_UA)
    assert "database_url" in str(exc.value)


def test_settings_extra_env_vars_are_ignored(monkeypatch):
    monkeypatch.setenv("SOME_UNRELATED_VAR", "ignored")
    s = Settings(_env_file=None, database_url=VALID_DB, sec_user_agent=VALID_UA)
    assert not hasattr(s, "some_unrelated_var")


def test_env_file_points_inside_repo():
    # model_config should reference the repo-local .env file.
    env_file = Settings.model_config.get("env_file")
    assert env_file is not None
    assert str(env_file).endswith(".env")
    assert os.path.isabs(str(env_file))
