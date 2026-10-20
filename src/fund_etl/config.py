import os
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus
from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Base directory pointing to repo root: fund-etl/
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"


class Settings(BaseSettings):
    """
    Centralized pipeline settings.
    Automatically reads from environment variables, falls back to .env,
    and fails loudly at boot if required parameters are missing or invalid.
    """
    # Discrete database configuration fields
    db_username: Optional[str] = Field("app_admin", description="Database username")
    db_password: Optional[str] = Field(None, description="Database password")
    db_host: Optional[str] = Field("localhost", description="Database host")
    db_port: Optional[int] = Field(5432, description="Database port")
    db_name: Optional[str] = Field("funds", description="Database name")
    db_schema: Optional[str] = Field("sec_financials", description="Default search schema")

    # Dynamic or direct connection URL
    database_url: Optional[PostgresDsn] = Field(
        None,
        description="Postgres connection string. Auto-assembled if not explicitly provided."
    )

    # Required SEC parameters
    sec_user_agent: str = Field(
        ...,
        min_length=10,
        description="User-Agent required by SEC (e.g., 'AppName admin@domain.com')"
    )

    # Optional defaults
    sec_base_url: str = "https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets"
    ticker_url: str = "https://www.sec.gov/files/company_tickers_mf.json"
    batch_size: int = 250_000
    log_level: str = "INFO"

    # Pydantic v2 Settings configuration
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Reads WINDOWS_HOST injected by Doppler (fallback to RABBITMQ_HOST or localhost)
    rabbitmq_host: str = Field(
        default="localhost",
        validation_alias="WINDOWS_HOST"
    )
    rabbitmq_port: int = Field(default=5672, validation_alias="RABBITMQ_PORT")

    @field_validator("database_url", mode="before")
    @classmethod
    def assemble_db_connection(cls, v: Optional[str], info) -> str:
        # If full database_url is already explicitly set in environment or .env, use it
        if v:
            return str(v)

        data = info.data
        user = data.get("db_username") or os.getenv("DB_USERNAME", "app_admin")
        password = data.get("db_password") or os.getenv("DB_PASSWORD", "")
        host = data.get("db_host") or os.getenv("DB_HOST", "localhost")
        port = data.get("db_port") or os.getenv("DB_PORT", 5432)
        name = data.get("db_name") or os.getenv("DB_NAME", "funds")
        schema = data.get("db_schema") or os.getenv("DB_SCHEMA", "sec_financials")

        if not password:
            raise ValueError(
                "Cannot assemble database_url: 'DB_PASSWORD' (or 'db_password') must be provided."
            )

        encoded_password = quote_plus(password)
        return f"postgresql://{user}:{encoded_password}@{host}:{port}/{name}"


# Singleton instance used across modules
settings = Settings()